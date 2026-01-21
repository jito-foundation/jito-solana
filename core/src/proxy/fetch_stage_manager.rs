use {
    crate::{
        bam_dependencies::BamConnectionState,
        proxy::{HeartbeatEvent, ProxyError},
    },
    crossbeam_channel::{select, tick, Receiver, Sender},
    solana_client::connection_cache::Protocol,
    solana_gossip::{cluster_info::ClusterInfo, contact_info},
    solana_perf::packet::PacketBatch,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU8, Ordering},
            Arc,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(1500); // Empirically determined from load testing
const DISCONNECT_DELAY: Duration = Duration::from_secs(60);
const METRICS_CADENCE: Duration = Duration::from_secs(1);

struct FetchStageState {
    fetch_connected: bool,
    heartbeat_received: bool,
    pending_disconnect: bool,
}

impl FetchStageState {
    fn new() -> Self {
        Self {
            fetch_connected: true,
            heartbeat_received: false,
            pending_disconnect: false,
        }
    }

    fn reset_to_bam_state(&mut self) {
        self.fetch_connected = false;
        self.heartbeat_received = false;
        self.pending_disconnect = true;
    }

    fn switch_to_connected_mode(&mut self) {
        self.fetch_connected = true;
        self.pending_disconnect = false;
    }

    fn switch_to_disconnected_mode(&mut self) {
        self.fetch_connected = false;
        self.pending_disconnect = false;
    }

    fn set_to_pending_disconnect(&mut self) {
        self.pending_disconnect = true;
    }

    fn needs_fallback_reconnect(&self) -> bool {
        !self.heartbeat_received && (!self.fetch_connected || self.pending_disconnect)
    }

    fn should_start_pending_disconnect(&self) -> bool {
        self.fetch_connected && !self.pending_disconnect
    }

    fn should_disconnect_to_relayer(&self, pending_disconnect_ts: &std::time::Instant) -> bool {
        self.fetch_connected
            && self.pending_disconnect
            && pending_disconnect_ts.elapsed() > DISCONNECT_DELAY
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
        // Channel that heartbeats are received from. Entirely responsible for triggering switch-overs.
        heartbeat_rx: Receiver<HeartbeatEvent>,
        // Channel that packets from FetchStage are intercepted from.
        packet_intercept_rx: Receiver<PacketBatch>,
        // Intercepted packets get piped through here.
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
    ) -> Self {
        let t_hdl = Self::start(
            cluster_info,
            heartbeat_rx,
            packet_intercept_rx,
            packet_tx,
            exit,
            bam_enabled,
            my_fallback_contact_info,
        );

        Self { t_hdl }
    }

    /// Disconnect fetch behaviour
    /// Starts connected
    /// When connected and a packet is received, forward it
    /// When disconnected, packet is dropped
    /// When receiving heartbeat while connected and not pending disconnect
    ///      Sets pending_disconnect to true and records time
    /// When receiving heartbeat while connected, and pending for > DISCONNECT_DELAY_SEC
    ///      Sets fetch_connected to false, pending_disconnect to false
    ///      Advertises TPU ports sent in heartbeat
    /// When tick is received without heartbeat_received
    ///      Sets fetch_connected to true, pending_disconnect to false
    ///      Advertises saved contact info
    fn start(
        cluster_info: Arc<ClusterInfo>,
        heartbeat_rx: Receiver<HeartbeatEvent>,
        packet_intercept_rx: Receiver<PacketBatch>,
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        my_fallback_contact_info: contact_info::ContactInfo,
    ) -> JoinHandle<()> {
        Builder::new().name("fetch-stage-manager".into()).spawn(move || {
            // Save validator's original TPU addresses for fallback

            let mut state = FetchStageState::new();

            let mut pending_disconnect_ts = Instant::now();

            let heartbeat_tick = tick(HEARTBEAT_TIMEOUT);
            let metrics_tick = tick(METRICS_CADENCE);
            let mut packets_forwarded = 0;
            let mut heartbeats_received = 0;
            while !exit.load(Ordering::Relaxed) {
                // BAM override: When BAM is enabled, bypass all normal operation
                if BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
                    == BamConnectionState::Connected
                {
                    state.reset_to_bam_state();
                    // Drain any queued packets to prevent buildup
                    while packet_intercept_rx.try_recv().is_ok() {}
                    std::thread::sleep(Duration::from_millis(100));
                    continue;
                }

                select! {
                    recv(packet_intercept_rx) -> pkt => {
                        match pkt {
                            Ok(pkt) => {
                                // Only forward packets when fetch stage is "connected"
                                if state.fetch_connected {
                                    if packet_tx.send(pkt).is_err() {
                                        error!("{:?}", ProxyError::PacketForwardError);
                                        return;
                                    }
                                    packets_forwarded += 1;
                                }
                                // When fetch_connected=false, packets are dropped (not forwarded)
                            }
                            Err(_) => {
                                warn!("packet intercept receiver disconnected, shutting down");
                                return;
                            }
                        }
                    }
                    recv(heartbeat_tick) -> _ => {
                        if exit.load(Ordering::Relaxed) {
                            break;
                        }
                        // If no heartbeat received and we're in a state that needs fallback
                        if state.needs_fallback_reconnect() {
                            if BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
                                == BamConnectionState::Connected
                            {
                                state.reset_to_bam_state();
                                continue;
                            }
                            warn!("heartbeat late, reconnecting fetch stage");
                            // Switch to "connected" mode (forward packets) and use validator's TPU
                            state.switch_to_connected_mode();

                            // Set TPU addresses back to validator's original addresses
                            // yes, using UDP here is extremely confusing for the validator
                            // since the entire network is running QUIC. However, it's correct.
                            if let Err(e) = Self::set_tpu_addresses(&cluster_info, my_fallback_contact_info.tpu(Protocol::UDP).unwrap(), my_fallback_contact_info.tpu_forwards(Protocol::UDP).unwrap()) {
                                error!("error setting tpu or tpu_fwd to ({:?}, {:?}), error: {:?}", my_fallback_contact_info.tpu(Protocol::UDP).unwrap(), my_fallback_contact_info.tpu_forwards(Protocol::UDP).unwrap(), e);
                            }
                            heartbeats_received = 0;
                        }
                        // Reset heartbeat flag for next timeout cycle
                        state.heartbeat_received = false;
                    }
                    recv(heartbeat_rx) -> tpu_info => {
                        if let Ok((tpu_addr, tpu_forward_addr)) = tpu_info {
                            heartbeats_received += 1;
                            state.heartbeat_received = true;
                            if state.should_start_pending_disconnect() {
                                info!("received heartbeat while fetch stage connected, pending disconnect after delay");
                                pending_disconnect_ts = Instant::now();
                                state.set_to_pending_disconnect();
                            }
                            if state.should_disconnect_to_relayer(&pending_disconnect_ts) {
                                if BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
                                    == BamConnectionState::Connected
                                {
                                    state.reset_to_bam_state();
                                    continue;
                                }
                                info!("disconnecting fetch stage");
                                state.switch_to_disconnected_mode();
                                if let Err(e) = Self::set_tpu_addresses(&cluster_info, tpu_addr, tpu_forward_addr) {
                                    error!("error setting tpu or tpu_fwd to ({tpu_addr:?}, {tpu_forward_addr:?}), error: {e:?}");
                                }
                            }
                        } else {
                            {
                                warn!("relayer heartbeat receiver disconnected, shutting down");
                                return;
                            }
                        }
                    }
                    recv(metrics_tick) -> _ => {
                        datapoint_info!(
                            "relayer-heartbeat",
                            ("fetch_stage_packets_forwarded", packets_forwarded, i64),
                            ("heartbeats_received", heartbeats_received, i64),
                        );

                    }
                }
            }
        }).unwrap()
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) -> Result<(), contact_info::Error> {
        cluster_info.set_tpu_quic(tpu_address)?;
        cluster_info.set_tpu_forwards_quic(tpu_forward_address)?;
        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_hdl.join()
    }
}
