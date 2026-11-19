use {
    crate::{bam_dependencies::BamConnectionState, proxy::HeartbeatEvent},
    arc_swap::ArcSwap,
    crossbeam_channel::{Receiver, RecvError, TrySendError, select, tick},
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{self, Protocol},
    },
    solana_perf::packet::PacketBatch,
    solana_streamer::streamer::ChannelSend,
    std::{
        fmt,
        net::SocketAddr,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

/// Maximum time allowed between relayer heartbeats before considering the relayer timed out
const MAX_TIME_BETWEEN_RELAYER_HEARTBEATS: Duration = Duration::from_millis(1500); // Empirically determined from load testing

/// How often to re-evaluate TPU state
const STATE_MACHINE_TICK: Duration = Duration::from_millis(solana_clock::DEFAULT_MS_PER_SLOT);

/// How long to delay before switching to relayer TPU after first heartbeat
const RELAYER_TPU_ENABLE_DELAY: Duration = Duration::from_secs(60);

/// How often to log metrics
const METRICS_INTERVAL: Duration = Duration::from_secs(1);
const TPU_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// Sends TPU packets to sigverify while this validator advertises its own TPU, and drops them
/// while a relayer or BAM TPU is advertised. FetchStageManager decides which.
#[derive(Clone)]
pub struct TpuPacketSender {
    sender: agave_wake_channel::Sender<PacketBatch>,
    forward: Arc<AtomicBool>,
    packets_forwarded: Arc<AtomicU64>,
}

impl TpuPacketSender {
    pub fn new(sender: agave_wake_channel::Sender<PacketBatch>) -> Self {
        Self {
            sender,
            forward: Arc::new(AtomicBool::new(true)),
            packets_forwarded: Arc::new(AtomicU64::new(0)),
        }
    }
}

impl ChannelSend<PacketBatch> for TpuPacketSender {
    fn try_send(&self, batch: PacketBatch) -> Result<(), TrySendError<PacketBatch>> {
        if !self.forward.load(Ordering::Relaxed) {
            return Ok(());
        }
        let num_packets = batch.len() as u64;
        self.sender.try_send(batch)?;
        self.packets_forwarded
            .fetch_add(num_packets, Ordering::Relaxed);
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.sender.is_empty()
    }

    fn len(&self) -> usize {
        self.sender.len()
    }
}

/// Manages switching between the validator's tpu ports and that of the proxy's.
/// Switch-overs are triggered by late and missed heartbeats.
pub struct FetchStageManager {
    t_hdl: JoinHandle<()>,
}

impl FetchStageManager {
    pub fn new(
        // ClusterInfo is used to switch between advertising the proxy's TPU ports and that of this validator's.
        cluster_info: Arc<ClusterInfo>,
        relayer_heartbeat_rx: Receiver<HeartbeatEvent>,
        tpu_packet_sender: TpuPacketSender,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
        bam_tpu_info: Arc<ArcSwap<Option<(SocketAddr, SocketAddr)>>>,
    ) -> Self {
        let t_hdl = Self::start(
            cluster_info,
            relayer_heartbeat_rx,
            tpu_packet_sender,
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
        tpu_packet_sender: TpuPacketSender,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
        bam_tpu_info: Arc<ArcSwap<Option<(SocketAddr, SocketAddr)>>>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("fetch-stage-manager".into())
            .spawn(move || {
                // Save original TPU info.
                let original_tpu_info = (
                    my_fallback_contact_info.tpu(Protocol::QUIC).unwrap(),
                    my_fallback_contact_info
                        .tpu_forwards(Protocol::QUIC)
                        .unwrap(),
                );

                // Initialize the 'brain of the operation'
                let mut tpu_state_machine = FetchStageTpuStateMachine::new(
                    tpu_packet_sender,
                    bam_enabled.clone(),
                    TpuAddresses {
                        tpu_addr: original_tpu_info.0,
                        tpu_forward_addr: original_tpu_info.1,
                    },
                    bam_tpu_info,
                    cluster_info,
                    MAX_TIME_BETWEEN_RELAYER_HEARTBEATS,
                    RELAYER_TPU_ENABLE_DELAY,
                );

                // Setup ticks for periodic evaluation and metrics
                let state_machine_tick = tick(STATE_MACHINE_TICK);
                let metrics_tick = tick(METRICS_INTERVAL);

                // Run the semi-eternal loop
                while !exit.load(Ordering::Relaxed) {
                    let all_good = select! {
                        recv(state_machine_tick) -> _ => {
                            tpu_state_machine.state_machine_tick();
                            true
                        },
                        recv(relayer_heartbeat_rx) -> tpu_info => tpu_state_machine.handle_relayer_message(tpu_info),
                        recv(metrics_tick) -> _ => {
                            tpu_state_machine.handle_metrics_tick();
                            true
                        },
                    };
                    if !all_good {
                        datapoint_warn!("fetch_stage_manager-shutdown", ("stop", 1, i64));
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

impl fmt::Display for TpuConnectionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TpuConnectionType::Original => "Original",
            TpuConnectionType::Relayer => "Relayer",
            TpuConnectionType::Bam => "Bam",
        };
        write!(f, "{s}")
    }
}

struct FetchStageTpuStateMachine {
    /// Which Tpu is being used
    current_tpu_state: TpuState,

    /// Forwards TPU packets to sigverify while the original TPU is advertised
    tpu_packet_sender: TpuPacketSender,

    /// Whether BAM is enabled and connected
    bam_enabled: Arc<AtomicU8>,

    /// Fallback TPU addresses
    original_tpu_info: TpuAddresses,

    /// Relayer heartbeat tracking
    relayer_info: Option<HeartbeatingRelayerInfo>,

    /// BAM TPU addresses
    bam_tpu_info: Arc<ArcSwap<Option<(SocketAddr, SocketAddr)>>>,

    /// ClusterInfo to update TPU addresses
    cluster_info: Arc<ClusterInfo>,

    /// Heartbeats received since the last metrics report
    heartbeats_received: u64,

    /// How often to check for relayer heartbeat timeouts
    max_time_between_relayer_heartbeats: Duration,

    /// Relayer TPU enable delay
    relayer_tpu_enable_delay: Duration,

    /// Last TPU report time
    last_tpu_report: Instant,
}

impl FetchStageTpuStateMachine {
    fn new(
        tpu_packet_sender: TpuPacketSender,
        bam_enabled: Arc<AtomicU8>,
        original_tpu_info: TpuAddresses,
        bam_tpu_info: Arc<ArcSwap<Option<(SocketAddr, SocketAddr)>>>,
        cluster_info: Arc<ClusterInfo>,
        max_time_between_relayer_heartbeats: Duration,
        relayer_tpu_enable_delay: Duration,
    ) -> Self {
        Self {
            tpu_packet_sender,
            bam_enabled,
            current_tpu_state: TpuState {
                tpu_type: TpuConnectionType::Original,
                addr: original_tpu_info.tpu_addr,
                fwd_addr: original_tpu_info.tpu_forward_addr,
            },
            heartbeats_received: 0,
            original_tpu_info,
            relayer_info: None,
            bam_tpu_info,
            cluster_info,
            max_time_between_relayer_heartbeats,
            relayer_tpu_enable_delay,
            last_tpu_report: Instant::now() - TPU_REPORT_INTERVAL,
        }
    }

    fn is_bam_connected(&self) -> bool {
        BamConnectionState::from_u8(self.bam_enabled.load(Ordering::Acquire))
            == BamConnectionState::Connected
    }

    // NOTE: For BAM and Relayer sockets we add +6.
    // Those systems still conform to legacy pre v4.X agave offsetting scheme, so the TPU port is actually 6 higher than the one returned from them.
    fn get_next_tpu_state(&self) -> TpuState {
        // BAM has the highest priority.
        // We'll always advertise BAM TPU even if relayer-url is set.
        if self.is_bam_connected()
            && let Some((addr, fwd_addr)) = self.bam_tpu_info.load().as_ref()
        {
            return TpuState {
                tpu_type: TpuConnectionType::Bam,
                addr: SocketAddr::new(addr.ip(), addr.port().saturating_add(6)),
                fwd_addr: SocketAddr::new(fwd_addr.ip(), fwd_addr.port().saturating_add(6)),
            };
        }

        // Relayer has second priority
        if self.relayer_info.as_ref().is_some_and(|info| {
            let now = Instant::now();
            now.duration_since(info.last_heartbeat) < self.max_time_between_relayer_heartbeats
                && now.duration_since(info.first_heartbeat) > self.relayer_tpu_enable_delay
        }) {
            let relayer_addr = self.relayer_info.as_ref().unwrap().tpu_addresses.tpu_addr;
            let relayer_fwd_addr = self
                .relayer_info
                .as_ref()
                .unwrap()
                .tpu_addresses
                .tpu_forward_addr;
            return TpuState {
                tpu_type: TpuConnectionType::Relayer,
                addr: SocketAddr::new(relayer_addr.ip(), relayer_addr.port().saturating_add(6)),
                fwd_addr: SocketAddr::new(
                    relayer_fwd_addr.ip(),
                    relayer_fwd_addr.port().saturating_add(6),
                ),
            };
        }

        // Default to original TPU
        TpuState {
            tpu_type: TpuConnectionType::Original,
            addr: self.original_tpu_info.tpu_addr,
            fwd_addr: self.original_tpu_info.tpu_forward_addr,
        }
    }

    /// Evaluate the current state and make transitions as needed
    fn state_machine_tick(&mut self) -> bool {
        // Increment the state machine using latest gathered data
        let prev_state = self.current_tpu_state;
        self.current_tpu_state = self.get_next_tpu_state();

        let state_changed = prev_state != self.current_tpu_state;
        let report_due = Instant::now().duration_since(self.last_tpu_report) >= TPU_REPORT_INTERVAL;

        let mut succeeded = true;

        // Update gossip if the state changed
        if state_changed {
            info!(
                "Switching TPU state from {:?} to {:?}",
                prev_state, self.current_tpu_state
            );
            if !self.gossip_current_tpu_state() {
                datapoint_error!("tpu_state_machine_tick-gossip_error", ("error", 1, i64),);

                // Revert state if gossiping new state failed.
                self.current_tpu_state = prev_state;
                let _ = self.gossip_current_tpu_state();
                succeeded = false;
            }
            if succeeded
                && prev_state.tpu_type == TpuConnectionType::Relayer
                && self.current_tpu_state.tpu_type != TpuConnectionType::Relayer
            {
                self.relayer_info = None;
            }
            self.tpu_packet_sender.forward.store(
                self.current_tpu_state.tpu_type == TpuConnectionType::Original,
                Ordering::Relaxed,
            );
        }

        if state_changed || report_due {
            datapoint_info!(
                "tpu_state_machine_tick",
                (
                    "current_tpu",
                    self.current_tpu_state.tpu_type.to_string(),
                    String
                ),
                ("previous_tpu", prev_state.tpu_type.to_string(), String)
            );
            self.last_tpu_report = Instant::now();
        }

        succeeded
    }

    /// Log metrics and reset counters
    fn handle_metrics_tick(&mut self) {
        datapoint_info!(
            "relayer-heartbeat",
            (
                "fetch_stage_packets_forwarded",
                self.tpu_packet_sender
                    .packets_forwarded
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            ("heartbeats_received", self.heartbeats_received, i64),
        );
        self.heartbeats_received = 0;
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

        let now = Instant::now();
        self.heartbeats_received += 1;
        if let Some(relayer_info) = self.relayer_info.as_mut() {
            relayer_info.last_heartbeat = now;
            relayer_info.tpu_addresses.tpu_addr = tpu_addr;
            relayer_info.tpu_addresses.tpu_forward_addr = tpu_forward_addr;
        } else {
            self.relayer_info = Some(HeartbeatingRelayerInfo {
                tpu_addresses: TpuAddresses {
                    tpu_addr,
                    tpu_forward_addr,
                },
                first_heartbeat: now,
                last_heartbeat: now,
            });
        }
        true
    }

    /// Update gossip TPU addresses based on current state
    fn gossip_current_tpu_state(&self) -> bool {
        info!(
            "Gossiping TPU addresses to {}, {}",
            self.current_tpu_state.addr, self.current_tpu_state.fwd_addr
        );

        if let Err(e) = self.cluster_info.set_tpu_quic(self.current_tpu_state.addr) {
            error!("Failed to set TPU QUIC address: {e:?}");
            return false;
        }
        if let Err(e) = self
            .cluster_info
            .set_tpu_forwards_quic(self.current_tpu_state.fwd_addr)
        {
            error!("Failed to set TPU FWD QUIC address: {e:?}");
            return false;
        }

        true
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
        brain: &FetchStageTpuStateMachine,
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
        brain: &mut FetchStageTpuStateMachine,
        packet_rx: &agave_wake_channel::Receiver<PacketBatch>,
        should_send: bool,
    ) {
        let forwarded = || {
            brain
                .tpu_packet_sender
                .packets_forwarded
                .load(Ordering::Relaxed)
        };
        let packets_forwarded = forwarded();
        let pkt = PacketBatch::from(vec![BytesPacket::empty(); 3]);
        assert!(brain.tpu_packet_sender.try_send(pkt.clone()).is_ok());
        if should_send {
            assert_eq!(packet_rx.try_recv().unwrap(), pkt);
            assert_eq!(forwarded(), packets_forwarded + 3);
        } else {
            assert!(packet_rx.try_recv().is_err());
            assert_eq!(forwarded(), packets_forwarded);
        }
    }

    struct TestContext {
        cluster_info: Arc<ClusterInfo>,
        bam_enabled: Arc<AtomicU8>,
        original_tpu_info: TpuAddresses,
        bam_tpu_info: Arc<ArcSwap<Option<(SocketAddr, SocketAddr)>>>,
        heartbeat_check_interval: Duration,
        relayer_tpu_enable_delay: Duration,
        _packet_tx: agave_wake_channel::Sender<PacketBatch>,
        packet_rx: agave_wake_channel::Receiver<PacketBatch>,
        brain: FetchStageTpuStateMachine,
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
        let bam_tpu_info = Arc::new(ArcSwap::new(Arc::new(None)));
        let heartbeat_check_interval = Duration::from_secs(1);
        let relayer_tpu_enable_delay = Duration::from_secs(1);
        let (packet_tx, packet_rx) = agave_wake_channel::unbounded();

        let brain = FetchStageTpuStateMachine::new(
            TpuPacketSender::new(packet_tx.clone()),
            bam_enabled.clone(),
            original_tpu_info,
            bam_tpu_info.clone(),
            cluster_info.clone(),
            heartbeat_check_interval,
            relayer_tpu_enable_delay,
        );

        TestContext {
            cluster_info,
            bam_enabled,
            original_tpu_info,
            bam_tpu_info,
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
        assert!(brain.state_machine_tick());
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
        thread::sleep(relayer_tpu_enable_delay.saturating_mul(2));
        assert!(brain.handle_relayer_message(relayer_heartbeat));
        assert!(brain.state_machine_tick());

        // Should have switched to relayer and packets should NOT be forwarded
        let expected_relayer_addr =
            SocketAddr::new(relayer_tpu_addr.ip(), relayer_tpu_addr.port() + 6);
        let expected_relayer_fwd_addr =
            SocketAddr::new(relayer_tpu_fwd_addr.ip(), relayer_tpu_fwd_addr.port() + 6);
        check_brain(
            &brain,
            TpuConnectionType::Relayer,
            &expected_relayer_addr,
            &expected_relayer_fwd_addr,
        );
        check_cluster_info(
            &cluster_info,
            &expected_relayer_addr,
            &expected_relayer_fwd_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, false);

        // Simulate relayer heartbeat timeout by waiting longer than heartbeat_check_interval
        thread::sleep(heartbeat_check_interval.saturating_mul(2));
        assert!(brain.state_machine_tick());
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
        let bam_tpu_addr: SocketAddr = "127.0.0.1:65529".parse().unwrap();
        let bam_tpu_fwd_addr: SocketAddr = "127.0.0.1:65529".parse().unwrap();
        bam_tpu_info.store(Arc::new(Some((bam_tpu_addr, bam_tpu_fwd_addr))));
        assert!(brain.state_machine_tick());

        // Should have switched to BAM and packets should NOT be forwarded
        let expected_bam_addr = SocketAddr::new(bam_tpu_addr.ip(), bam_tpu_addr.port() + 6);
        let expected_bam_fwd_addr =
            SocketAddr::new(bam_tpu_fwd_addr.ip(), bam_tpu_fwd_addr.port() + 6);
        check_brain(
            &brain,
            TpuConnectionType::Bam,
            &expected_bam_addr,
            &expected_bam_fwd_addr,
        );
        check_cluster_info(&cluster_info, &expected_bam_addr, &expected_bam_fwd_addr);
        check_sending_packet(&mut brain, &packet_rx, false);

        // Disable BAM
        bam_enabled.store(BamConnectionState::Disconnected as u8, Ordering::Relaxed);
        assert!(brain.state_machine_tick());

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
    fn test_bam_to_relayer_and_back_switch() {
        let TestContext {
            cluster_info,
            bam_enabled,
            original_tpu_info: _,
            bam_tpu_info,
            heartbeat_check_interval: _,
            relayer_tpu_enable_delay,
            _packet_tx,
            packet_rx,
            mut brain,
        } = setup_test();

        // Enable BAM and set BAM TPU info
        bam_enabled.store(BamConnectionState::Connected as u8, Ordering::Relaxed);
        let bam_tpu_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let bam_tpu_fwd_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        bam_tpu_info.store(Arc::new(Some((bam_tpu_addr, bam_tpu_fwd_addr))));
        assert!(brain.state_machine_tick());

        // Should be BAM and packets should NOT be forwarded
        let expected_bam_addr = SocketAddr::new(bam_tpu_addr.ip(), bam_tpu_addr.port() + 6);
        let expected_bam_fwd_addr =
            SocketAddr::new(bam_tpu_fwd_addr.ip(), bam_tpu_fwd_addr.port() + 6);
        check_brain(
            &brain,
            TpuConnectionType::Bam,
            &expected_bam_addr,
            &expected_bam_fwd_addr,
        );
        check_cluster_info(&cluster_info, &expected_bam_addr, &expected_bam_fwd_addr);
        check_sending_packet(&mut brain, &packet_rx, false);

        // Simulate relayer heartbeat
        let relayer_tpu_addr: SocketAddr = "127.0.0.1:7002".parse().unwrap();
        let relayer_tpu_fwd_addr: SocketAddr = "127.0.0.1:7003".parse().unwrap();
        let relayer_heartbeat = Ok((relayer_tpu_addr, relayer_tpu_fwd_addr));
        assert!(brain.handle_relayer_message(relayer_heartbeat));

        // Wait long enough; should still not switch since BAM is connected
        std::thread::sleep(relayer_tpu_enable_delay.saturating_mul(2));
        assert!(brain.handle_relayer_message(relayer_heartbeat));
        assert!(brain.state_machine_tick());
        check_brain(
            &brain,
            TpuConnectionType::Bam,
            &expected_bam_addr,
            &expected_bam_fwd_addr,
        );
        check_cluster_info(&cluster_info, &expected_bam_addr, &expected_bam_fwd_addr);
        check_sending_packet(&mut brain, &packet_rx, false);

        // Disable BAM
        bam_enabled.store(BamConnectionState::Disconnected as u8, Ordering::Relaxed);
        assert!(brain.state_machine_tick());

        // Should have switched to relayer and packets should NOT be forwarded
        let expected_relayer_addr =
            SocketAddr::new(relayer_tpu_addr.ip(), relayer_tpu_addr.port() + 6);
        let expected_relayer_fwd_addr =
            SocketAddr::new(relayer_tpu_fwd_addr.ip(), relayer_tpu_fwd_addr.port() + 6);
        check_brain(
            &brain,
            TpuConnectionType::Relayer,
            &expected_relayer_addr,
            &expected_relayer_fwd_addr,
        );
        check_cluster_info(
            &cluster_info,
            &expected_relayer_addr,
            &expected_relayer_fwd_addr,
        );
        check_sending_packet(&mut brain, &packet_rx, false);
    }

    #[test]
    fn test_bam_gossip_failure_reverts_state() {
        use std::{net::SocketAddr, str::FromStr};

        let TestContext {
            bam_enabled,
            bam_tpu_info,
            packet_rx,
            mut brain,
            ..
        } = setup_test();
        let bad_bam_tpu_info = Arc::new(Some((
            SocketAddr::from_str("0.0.0.0:0").unwrap(),
            SocketAddr::from_str("0.0.0.0:0").unwrap(),
        )));

        // A failed switch away from Original keeps forwarding packets.
        bam_enabled.store(BamConnectionState::Connected as u8, Ordering::Relaxed);
        bam_tpu_info.store(bad_bam_tpu_info.clone());
        assert!(!brain.state_machine_tick());
        check_sending_packet(&mut brain, &packet_rx, true);

        // Enable BAM and set a valid BAM TPU info
        bam_enabled.store(BamConnectionState::Connected as u8, Ordering::Relaxed);
        let bam_tpu_addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let bam_tpu_fwd_addr: SocketAddr = "127.0.0.1:7001".parse().unwrap();
        bam_tpu_info.store(Arc::new(Some((bam_tpu_addr, bam_tpu_fwd_addr))));
        assert!(brain.state_machine_tick());
        assert_eq!(brain.current_tpu_state.tpu_type, TpuConnectionType::Bam);

        // Now set a bad BAM TPU info that will cause gossip to fail
        bam_tpu_info.store(bad_bam_tpu_info);

        // The state machine should attempt to switch, fail gossip, and revert to previous state
        let prev_state = brain.current_tpu_state;
        let result = brain.state_machine_tick();
        assert!(!result, "Should return false on gossip failure");
        assert_eq!(
            brain.current_tpu_state, prev_state,
            "State should revert to previous on gossip failure"
        );
    }
}
