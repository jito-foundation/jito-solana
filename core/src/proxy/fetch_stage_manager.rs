use {
    crate::proxy::{relayer_stage::RelayerConfig, HeartbeatEvent, ProxyError, ProxyResult},
    crossbeam_channel::{select, tick, Receiver, Sender},
    solana_client::connection_cache::Protocol,
    solana_gossip::{cluster_info::ClusterInfo, contact_info},
    solana_perf::packet::PacketBatch,
    std::{
        net::SocketAddr,
        ops::Add,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const DISCONNECT_DELAY: Duration = Duration::from_secs(60);
const METRICS_CADENCE: Duration = Duration::from_secs(1);

/// Manages switching between the validator's tpu ports and that of the proxy's.
/// Switch-overs are triggered by late and missed heartbeats.    
pub struct FetchStageManager {
    t_hdl: JoinHandle<()>,
}

impl FetchStageManager {
    pub fn new(
        // Relayer config used to update the heartbeat interval
        relayer_config: Arc<Mutex<RelayerConfig>>,
        // ClusterInfo is used to switch between advertising the proxy's TPU ports and that of this validator's.
        cluster_info: Arc<ClusterInfo>,
        // Channel that heartbeats are received from. Entirely responsible for triggering switch-overs.
        heartbeat_rx: Receiver<HeartbeatEvent>,
        // Channel that packets from FetchStage are intercepted from.
        packet_intercept_rx: Receiver<PacketBatch>,
        // Intercepted packets get piped through here.
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let t_hdl = Builder::new()
            .name("solFetchStageMgr".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    if let Err(e) = Self::start(
                        &relayer_config,
                        &cluster_info,
                        &heartbeat_rx,
                        &packet_intercept_rx,
                        &packet_tx,
                        &exit,
                    ) {
                        error!("FetchStageManager errored on {e:?}, restarting now.");
                    }
                }
            })
            .unwrap();

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
        global_relayer_config: &Arc<Mutex<RelayerConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        heartbeat_rx: &Receiver<HeartbeatEvent>,
        packet_intercept_rx: &Receiver<PacketBatch>,
        packet_tx: &Sender<PacketBatch>,
        exit: &Arc<AtomicBool>,
    ) -> ProxyResult<()> {
        // Contact info to gossip to the network if no heartbeats are received from relayer

        let my_fallback_contact_info = cluster_info.my_contact_info();
        let local_relayer_config = global_relayer_config.lock().unwrap().clone();

        let mut fetch_connected = true;
        let mut heartbeat_received = false;
        let mut pending_disconnect = false;

        let mut pending_disconnect_ts = Instant::now();

        let relayer_config_tick = tick(Duration::from_secs(1));
        // Add buffer to `DEFAULT_RELAYER_EXPECTED_HEARTBEAT_INTERVAL_MS`
        let heartbeat_tick = tick(
            local_relayer_config
                .expected_heartbeat_interval
                .add(Duration::from_secs(1)),
        );
        let metrics_tick = tick(METRICS_CADENCE);
        let mut packets_forwarded = 0;
        let mut heartbeats_received = 0;
        loop {
            select! {
                recv(packet_intercept_rx) -> pkt => {
                    match pkt {
                        Ok(pkt) => {
                            if fetch_connected {
                                packet_tx.send(pkt).map_err(|_e| ProxyError::PacketForwardError)?;
                                packets_forwarded += 1;
                            }
                        }
                        Err(_) => {
                            warn!("packet intercept receiver disconnected, shutting down");
                            return Err(ProxyError::HeartbeatChannelError);
                        }
                    }
                }
                recv(heartbeat_tick) -> _ => {
                    if exit.load(Ordering::Relaxed) {
                        return Ok(());
                    }
                    if !heartbeat_received && (!fetch_connected || pending_disconnect) {
                        warn!("heartbeat late, reconnecting fetch stage");
                        fetch_connected = true;
                        pending_disconnect = false;

                        // unwrap safe here bc contact_info.tpu(Protocol::QUIC) and contact_info.tpu_forwards(Protocol::QUIC)
                        // are checked on startup
                        if let Err(e) = Self::set_tpu_addresses(cluster_info, my_fallback_contact_info.tpu(Protocol::QUIC).unwrap(), my_fallback_contact_info.tpu_forwards(Protocol::QUIC).unwrap()) {
                            error!("error setting tpu or tpu_fwd to ({:?}, {:?}), error: {:?}", my_fallback_contact_info.tpu(Protocol::QUIC).unwrap(), my_fallback_contact_info.tpu_forwards(Protocol::QUIC).unwrap(), e);
                        }
                        heartbeats_received = 0;
                    }
                    heartbeat_received = false;
                }
                recv(heartbeat_rx) -> tpu_info => {
                    if let Ok((tpu_addr, tpu_forward_addr)) = tpu_info {
                        heartbeats_received += 1;
                        heartbeat_received = true;
                        if fetch_connected && !pending_disconnect {
                            info!("received heartbeat while fetch stage connected, pending disconnect after delay");
                            pending_disconnect_ts = Instant::now();
                            pending_disconnect = true;
                        }
                        if fetch_connected && pending_disconnect && pending_disconnect_ts.elapsed() > DISCONNECT_DELAY {
                            info!("disconnecting fetch stage");
                            fetch_connected = false;
                            pending_disconnect = false;
                            if let Err(e) = Self::set_tpu_addresses(cluster_info, tpu_addr, tpu_forward_addr) {
                                error!("error setting tpu or tpu_fwd to ({:?}, {:?}), error: {:?}", tpu_addr, tpu_forward_addr, e);
                            }
                        }
                    } else {
                        {
                            warn!("relayer heartbeat receiver disconnected, shutting down");
                            return Err(ProxyError::HeartbeatChannelError);
                        }
                    }
                }
                recv(relayer_config_tick) -> _ => {
                    if local_relayer_config != global_relayer_config.lock().unwrap().clone() {
                        return Err(ProxyError::RelayerConfigChanged);
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
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) -> Result<(), contact_info::Error> {
        cluster_info.set_tpu(tpu_address)?;
        cluster_info.set_tpu_forwards(tpu_forward_address)?;
        Ok(())
    }

    pub fn join(self) -> thread::Result<()> {
        self.t_hdl.join()
    }
}
