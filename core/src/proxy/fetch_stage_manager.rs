use {
    crate::{
        bam_dependencies::BamConnectionState,
        proxy::{HeartbeatEvent, ProxyError},
    },
    crossbeam_channel::{select, tick, Receiver, RecvError, Sender},
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{self, Protocol},
    },
    solana_perf::packet::PacketBatch,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU8, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// How often to check for heartbeat timeouts
const HEARTBEAT_CHECK_INTERVAL: Duration = Duration::from_millis(1500); // Empirically determined from load testing

/// How long to delay before switching to relayer TPU after first heartbeat
const RELAYER_TPU_ENABLE_DELAY: Duration = Duration::from_secs(60);

/// How often to log metrics
const METRICS_INTERVAL: Duration = Duration::from_secs(1);

/// Manages switching between the validator's tpu ports and that of the proxy's.
/// Switch-overs are triggered by late and missed heartbeats.
pub struct FetchStageManager {
    t_hdl: JoinHandle<()>,
}

impl FetchStageManager {
    pub fn new(
        cluster_info: Arc<ClusterInfo>,
        relayer_heartbeat_rx: Receiver<HeartbeatEvent>,
        packet_intercept_rx: Receiver<PacketBatch>,
        // Intercepted packets get piped through here.
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
        bam_tpu_info: Arc<RwLock<Option<(SocketAddr, SocketAddr)>>>,
    ) -> Self {
        let t_hdl = Self::start(
            cluster_info,
            relayer_heartbeat_rx,
            packet_intercept_rx,
            packet_tx,
            exit,
            bam_enabled,
            my_fallback_contact_info,
            bam_tpu_info,
        );

        Self { t_hdl }
    }

    fn start(
        cluster_info: Arc<ClusterInfo>,
        relayer_heartbeat_rx: Receiver<HeartbeatEvent>,
        packet_intercept_rx: Receiver<PacketBatch>,
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
        bam_tpu_info: Arc<RwLock<Option<(SocketAddr, SocketAddr)>>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("fetch-stage-manager".into())
            .spawn(move || {
                // Save original TPU info
                let original_tpu_info = (
                    my_fallback_contact_info.tpu(Protocol::UDP).unwrap(),
                    my_fallback_contact_info
                        .tpu_forwards(Protocol::UDP)
                        .unwrap(),
                );

                // Initialize the 'brain of the operation'
                let mut brain = FetchStageBrain::new(
                    packet_tx,
                    bam_enabled.clone(),
                    TpuAddresses {
                        tpu_addr: original_tpu_info.0,
                        tpu_forward_addr: original_tpu_info.1,
                    },
                    bam_tpu_info,
                    cluster_info,
                    HEARTBEAT_CHECK_INTERVAL,
                    RELAYER_TPU_ENABLE_DELAY,
                );

                // Setup ticks for periodic evaluation and metrics
                let heartbeat_tick = tick(HEARTBEAT_CHECK_INTERVAL);
                let metrics_tick = tick(METRICS_INTERVAL);

                // Run the semi-eternal loop
                while !exit.load(Ordering::Relaxed) {
                    let all_good = select! {
                        recv(packet_intercept_rx) -> pkt => brain.handle_packet_batch(pkt),
                        recv(heartbeat_tick) -> _ => brain.handle_evaluation_tick(),
                        recv(relayer_heartbeat_rx) -> tpu_info => brain.handle_relayer_message(tpu_info),
                        recv(metrics_tick) -> _ => brain.handle_metrics_tick(),
                    };
                    if !all_good {
                        break;
                    }
                }
            })
            .unwrap()
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_hdl.join()
    }
}

/// Metrics collected by FetchStageManager
struct FetchStageMetrics {
    packets_forwarded: u64,
    heartbeats_received: u64,
}

/// TPU addresses container
#[derive(Clone, Copy)]
struct TpuAddresses {
    /// Standard TPU address for public traffic
    tpu_addr: SocketAddr,

    /// TPU forward address for other validators
    tpu_forward_addr: SocketAddr,
}

/// Information about a relayer that is heartbeating
struct HeartbeatingRelayerInfo {
    /// TPU addresses received from relayer heartbeats
    tpu_addresses: TpuAddresses,

    /// When the first heartbeat was received
    first_heartbeat: Instant,

    /// When the last heartbeat was received
    last_heartbeat: Instant,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
struct TpuState {
    tpu_type: TpuConnectionType,
    addr: SocketAddr,
    fwd_addr: SocketAddr,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
enum TpuConnectionType {
    Original,
    Relayer,
    Bam,
}

struct FetchStageBrain {
    /// Which Tpu is being used
    current_tpu_state: TpuState,

    /// Channel to forward packets to FetchStage
    packet_tx: Sender<PacketBatch>,

    /// Whether BAM is enabled and connected
    bam_enabled: Arc<AtomicU8>,

    /// Fallback TPU addresses
    original_tpu_info: TpuAddresses,

    /// Relayer heartbeat tracking
    relayer_info: Option<HeartbeatingRelayerInfo>,

    /// BAM TPU addresses
    bam_tpu_info: Arc<RwLock<Option<(SocketAddr, SocketAddr)>>>,

    /// ClusterInfo to update TPU addresses
    cluster_info: Arc<ClusterInfo>,

    /// Metrics collected
    metrics: FetchStageMetrics,

    /// Heartbeat check interval
    heartbeat_check_interval: Duration,

    /// Relayer TPU enable delay
    relayer_tpu_enable_delay: Duration,
}

impl FetchStageBrain {
    fn new(
        packet_tx: Sender<PacketBatch>,
        bam_enabled: Arc<AtomicU8>,
        original_tpu_info: TpuAddresses,
        bam_tpu_info: Arc<RwLock<Option<(SocketAddr, SocketAddr)>>>,
        cluster_info: Arc<ClusterInfo>,
        heartbeat_check_interval: Duration,
        relayer_tpu_enable_delay: Duration,
    ) -> Self {
        Self {
            packet_tx,
            bam_enabled,
            current_tpu_state: TpuState {
                tpu_type: TpuConnectionType::Original,
                addr: original_tpu_info.tpu_addr,
                fwd_addr: original_tpu_info.tpu_forward_addr,
            },
            metrics: FetchStageMetrics {
                packets_forwarded: 0,
                heartbeats_received: 0,
            },
            original_tpu_info,
            relayer_info: None,
            bam_tpu_info,
            cluster_info,
            heartbeat_check_interval,
            relayer_tpu_enable_delay,
        }
    }

    fn is_bam_connected(&self) -> bool {
        BamConnectionState::from_u8(self.bam_enabled.load(Ordering::Relaxed))
            == BamConnectionState::Connected
    }

    fn get_next_tpu_state(&self) -> TpuState {
        if self.is_bam_connected() && self.bam_tpu_info.read().unwrap().is_some() {
            return TpuState {
                tpu_type: TpuConnectionType::Bam,
                addr: self.bam_tpu_info.read().unwrap().unwrap().0,
                fwd_addr: self.bam_tpu_info.read().unwrap().unwrap().1,
            };
        }

        if self.relayer_info.as_ref().map_or(false, |info| {
            let now = Instant::now();
            now.duration_since(info.last_heartbeat) < self.heartbeat_check_interval
                && now.duration_since(info.first_heartbeat) > self.relayer_tpu_enable_delay
        }) {
            return TpuState {
                tpu_type: TpuConnectionType::Relayer,
                addr: self
                    .relayer_info
                    .as_ref()
                    .unwrap()
                    .tpu_addresses
                    .tpu_addr,
                fwd_addr: self
                    .relayer_info
                    .as_ref()
                    .unwrap()
                    .tpu_addresses
                    .tpu_forward_addr,
            };
        }

        TpuState {
            tpu_type: TpuConnectionType::Original,
            addr: self.original_tpu_info.tpu_addr,
            fwd_addr: self.original_tpu_info.tpu_forward_addr,
        }
    }

    /// Evaluate the current state and make transitions as needed; returns false if we should shut down
    fn handle_evaluation_tick(&mut self) -> bool {
        // Increment the state machine using latest gathered data
        let prev_state = self.current_tpu_state;
        self.current_tpu_state = self.get_next_tpu_state();

        // Reset relayer info if we switched away from it;
        if prev_state.tpu_type == TpuConnectionType::Relayer
            && self.current_tpu_state.tpu_type != TpuConnectionType::Relayer
        {
            self.relayer_info = None;
        }

        // Update gossip if the state changed
        if prev_state != self.current_tpu_state {
            info!(
                "Switching TPU state from {:?} to {:?}",
                prev_state, self.current_tpu_state
            );
            self.update_gossip_based_on_state()
        } else {
            true
        }
    }

    /// Log metrics and reset counters; returns false if we should shut down
    fn handle_metrics_tick(&mut self) -> bool {
        datapoint_info!(
            "relayer-heartbeat",
            (
                "fetch_stage_packets_forwarded",
                self.metrics.packets_forwarded,
                i64
            ),
            ("heartbeats_received", self.metrics.heartbeats_received, i64),
        );
        self.metrics.packets_forwarded = 0;
        self.metrics.heartbeats_received = 0;
        true
    }

    /// Process a relayer heartbeat message; returns false if we should shut down
    fn handle_relayer_message(
        &mut self,
        tpu_info: Result<(SocketAddr, SocketAddr), RecvError>,
    ) -> bool {
        let Ok((tpu_addr, tpu_forward_addr)) = tpu_info else {
            warn!("relayer heartbeat receiver disconnected, shutting down");
            return false;
        };

        self.metrics.heartbeats_received += 1;
        if let Some(relayer_info) = self.relayer_info.as_mut() {
            relayer_info.last_heartbeat = Instant::now();
            relayer_info.tpu_addresses.tpu_addr = tpu_addr;
            relayer_info.tpu_addresses.tpu_forward_addr = tpu_forward_addr;
        } else {
            self.relayer_info = Some(HeartbeatingRelayerInfo {
                tpu_addresses: TpuAddresses {
                    tpu_addr,
                    tpu_forward_addr,
                },
                first_heartbeat: Instant::now(),
                last_heartbeat: Instant::now(),
            });
        }
        true
    }

    /// Process a batch of packets from FetchStage; returns false if we should shut down
    fn handle_packet_batch(&mut self, pkt: Result<PacketBatch, RecvError>) -> bool {
        match pkt {
            Ok(pkt) => {
                // Only forward packets when fetch stage is "connected"
                if self.should_forward_packets() {
                    if self.packet_tx.send(pkt).is_err() {
                        error!("{:?}", ProxyError::PacketForwardError);
                        return false;
                    }
                    self.metrics.packets_forwarded += 1;
                }
                true
            }
            Err(_) => {
                warn!("packet intercept receiver disconnected, shutting down");
                return false;
            }
        }
    }

    /// Determine if packets should be forwarded to FetchStage
    fn should_forward_packets(&self) -> bool {
        matches!(self.current_tpu_state.tpu_type, TpuConnectionType::Original)
    }

    /// Update gossip TPU addresses based on current state
    fn update_gossip_based_on_state(&self) -> bool {
        self.set_tpu_addresses(self.current_tpu_state.addr, self.current_tpu_state.fwd_addr)
            .is_ok()
    }

    /// Set the TPU addresses in gossip
    fn set_tpu_addresses(
        &self,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) -> Result<(), contact_info::Error> {
        info!(
            "Updating TPU addresses to {}, {}",
            tpu_address, tpu_forward_address
        );
        self.cluster_info.set_tpu_quic(tpu_address)?;
        self.cluster_info
            .set_tpu_forwards_quic(tpu_forward_address)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*, solana_gossip::contact_info::ContactInfo, solana_keypair::Keypair,
        solana_net_utils::SocketAddrSpace, solana_perf::packet::BytesPacket, solana_signer::Signer,
        solana_time_utils::timestamp,
    };

    fn new_test_cluster_info() -> Arc<ClusterInfo> {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        Arc::new(ClusterInfo::new(
            contact_info,
            keypair,
            SocketAddrSpace::Unspecified,
        ))
    }

    fn check_brain(
        brain: &FetchStageBrain,
        expected_tpu_type: TpuConnectionType,
        expected_addr: &SocketAddr,
        expected_fwd_addr: &SocketAddr,
    ) {
        assert_eq!(brain.current_tpu_state.tpu_type, expected_tpu_type);
        assert_eq!(brain.current_tpu_state.addr, *expected_addr);
        assert_eq!(brain.current_tpu_state.fwd_addr, *expected_fwd_addr);
    }

    fn check_cluster_info(
        cluster_info: &ClusterInfo,
        expected_addr: &SocketAddr,
        expected_fwd_addr: &SocketAddr,
    ) {
        assert_eq!(
            cluster_info.my_contact_info().tpu(Protocol::QUIC).unwrap(),
            *expected_addr
        );
        assert_eq!(
            cluster_info
                .my_contact_info()
                .tpu_forwards(Protocol::QUIC)
                .unwrap(),
            *expected_fwd_addr
        );
    }

    fn check_sending_packet(
        brain: &mut FetchStageBrain,
        packet_rx: &Receiver<PacketBatch>,
        should_send: bool,
    ) {
        let pkt = PacketBatch::Single(BytesPacket::empty());
        assert!(brain.handle_packet_batch(Ok(pkt.clone())));
        if should_send {
            let received_pkt = packet_rx.recv().unwrap();
            assert_eq!(received_pkt, pkt);
        } else {
            assert!(packet_rx.try_recv().is_err());
        }
    }

    struct TestContext {
        cluster_info: Arc<ClusterInfo>,
        bam_enabled: Arc<AtomicU8>,
        original_tpu_info: TpuAddresses,
        bam_tpu_info: Arc<RwLock<Option<(SocketAddr, SocketAddr)>>>,
        heartbeat_check_interval: Duration,
        relayer_tpu_enable_delay: Duration,
        _packet_tx: Sender<PacketBatch>,
        packet_rx: Receiver<PacketBatch>,
        brain: FetchStageBrain,
    }

    fn setup_test() -> TestContext {
        let cluster_info = new_test_cluster_info();
        let bam_enabled = Arc::new(AtomicU8::new(BamConnectionState::Disconnected as u8));
        let original_tpu_info = TpuAddresses {
            tpu_addr: cluster_info.my_contact_info().tpu(Protocol::QUIC).unwrap(),
            tpu_forward_addr: cluster_info
                .my_contact_info()
                .tpu_forwards(Protocol::QUIC)
                .unwrap(),
        };
        let bam_tpu_info = Arc::new(RwLock::new(None));
        let heartbeat_check_interval = Duration::from_secs(1);
        let relayer_tpu_enable_delay = Duration::from_secs(1);
        let (packet_tx, packet_rx) = crossbeam_channel::unbounded();

        let brain = FetchStageBrain::new(
            packet_tx.clone(),
            bam_enabled.clone(),
            original_tpu_info,
            bam_tpu_info.clone(),
            cluster_info.clone(),
            heartbeat_check_interval,
            relayer_tpu_enable_delay,
        );

        TestContext {
            cluster_info,
            bam_enabled: bam_enabled,
            original_tpu_info,
            bam_tpu_info: bam_tpu_info,
            heartbeat_check_interval,
            relayer_tpu_enable_delay,
            _packet_tx: packet_tx,
            packet_rx,
            brain,
        }
    }

    #[test]
    fn test_original_to_relayer_and_back_switch() {
        let TestContext {
            cluster_info,
            bam_enabled: _,
            original_tpu_info,
            bam_tpu_info: _,
            heartbeat_check_interval,
            relayer_tpu_enable_delay,
            _packet_tx,
            packet_rx,
            mut brain,
        } = setup_test();

        // Initially should be original and packets should be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Original,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_cluster_info(
            &cluster_info,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, true);

        // Simulate relayer heartbeat
        let relayer_tpu_addr: SocketAddr = "127.0.0.1:6000".parse().unwrap();
        let relayer_tpu_fwd_addr: SocketAddr = "127.0.0.1:6001".parse().unwrap();
        let relayer_heartbeat = Ok((relayer_tpu_addr, relayer_tpu_fwd_addr));
        assert!(brain.handle_relayer_message(relayer_heartbeat));

        // Should not switch yet (however we should start tracking it and packets should still be forwarded
        assert!(brain.handle_evaluation_tick());
        assert!(brain.relayer_info.is_some());
        check_brain(
            &brain,
            TpuConnectionType::Original,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_cluster_info(
            &cluster_info,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, true);

        // Wait enough time to exceed relayer_tpu_enable_delay
        std::thread::sleep(relayer_tpu_enable_delay.saturating_mul(2));
        assert!(brain.handle_relayer_message(relayer_heartbeat));
        assert!(brain.handle_evaluation_tick());

        // Should have switched to relayer and packets should NOT be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Relayer,
            &relayer_tpu_addr,
            &relayer_tpu_fwd_addr,
        );
        check_cluster_info(&cluster_info, &relayer_tpu_addr, &relayer_tpu_fwd_addr);
        check_sending_packet(&mut brain, &packet_rx, false);

        // Simulate relayer heartbeat timeout by waiting longer than heartbeat_check_interval
        std::thread::sleep(heartbeat_check_interval.saturating_mul(2));
        assert!(brain.handle_evaluation_tick());
        // Should have switched back to original and packets should be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Original,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_cluster_info(
            &cluster_info,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, true);
    }

    #[test]
    fn test_original_to_bam_and_back_switch() {
        let TestContext {
            cluster_info,
            bam_enabled,
            original_tpu_info,
            bam_tpu_info,
            heartbeat_check_interval: _,
            relayer_tpu_enable_delay: _,
            _packet_tx,
            packet_rx,
            mut brain,
        } = setup_test();

        // Initially should be original and packets should be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Original,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_cluster_info(
            &cluster_info,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, true);

        // Enable BAM and set BAM TPU info
        bam_enabled.store(BamConnectionState::Connected as u8, Ordering::Relaxed);
        let bam_tpu_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let bam_tpu_fwd_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        *bam_tpu_info.write().unwrap() = Some((bam_tpu_addr, bam_tpu_fwd_addr));
        assert!(brain.handle_evaluation_tick());

        // Should have switched to BAM and packets should NOT be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Bam,
            &bam_tpu_addr,
            &bam_tpu_fwd_addr,
        );
        check_cluster_info(&cluster_info, &bam_tpu_addr, &bam_tpu_fwd_addr);
        check_sending_packet(&mut brain, &packet_rx, false);

        // Disable BAM
        bam_enabled.store(BamConnectionState::Disconnected as u8, Ordering::Relaxed);
        assert!(brain.handle_evaluation_tick());

        // Should have switched back to original and packets should be forwarded
        check_brain(
            &brain,
            TpuConnectionType::Original,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_cluster_info(
            &cluster_info,
            &original_tpu_info.tpu_addr,
            &original_tpu_info.tpu_forward_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, true);
    }
}
