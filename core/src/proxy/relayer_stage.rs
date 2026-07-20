//! Maintains a connection to the Relayer.
//!
//! The external Relayer is responsible for the following:
//! - Acts as a TPU proxy.
//! - Sends transactions to the validator.
//! - Does not bundles to avoid DOS vector.
//! - When validator connects, it changes its TPU and TPU forward address to the relayer.
//! - Expected to send heartbeat to validator as watchdog. If watchdog times out, the validator
//!   disconnects and reverts the TPU and TPU forward settings.

use {
    crate::{
        proto_packet_to_packet,
        proxy::{
            HeartbeatEvent, ProxyError,
            auth::{AuthInterceptor, auth_client_from_endpoint, maybe_refresh_auth_tokens},
            endpoint_from_url,
        },
    },
    arc_swap::ArcSwap,
    crossbeam_channel::Sender,
    jito_protos::proto::{
        auth::{Token, auth_service_client::AuthServiceClient},
        relayer::{self, relayer_client::RelayerClient},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_perf::packet::{BytesPacket, PacketBatch},
    solana_signer::Signer,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        ops::AddAssign,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::time::{interval, sleep, timeout},
    tonic::{
        Streaming,
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;

#[derive(Default)]
struct RelayerStageStats {
    num_empty_messages: u64,
    num_packets: u64,
    num_heartbeats: u64,
}

impl RelayerStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "relayer_stage-stats",
            ("num_empty_messages", self.num_empty_messages, i64),
            ("num_packets", self.num_packets, i64),
            ("num_heartbeats", self.num_heartbeats, i64),
        );
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RelayerConfig {
    /// Relayer URL
    pub relayer_url: String,
    /// Interval at which heartbeats are expected.
    pub expected_heartbeat_interval: Duration,
    /// The max tolerable age of the last heartbeat.
    pub oldest_allowed_heartbeat: Duration,
}

pub struct RelayerStage {
    t_hdls: Vec<JoinHandle<()>>,
}

impl RelayerStage {
    pub fn new(
        relayer_config: Arc<ArcSwap<RelayerConfig>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that server-sent heartbeats are piped through.
        heartbeat_tx: Sender<HeartbeatEvent>,
        // Channel that non-trusted streamed packets are piped through.
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread = Builder::new()
            .name("relayer-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();

                rt.block_on(Self::start(
                    relayer_config,
                    cluster_info,
                    heartbeat_tx,
                    packet_tx,
                    exit,
                ));
            })
            .unwrap();

        Self {
            t_hdls: vec![thread],
        }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn start(
        relayer_config: Arc<ArcSwap<RelayerConfig>>,
        cluster_info: Arc<ClusterInfo>,
        heartbeat_tx: Sender<HeartbeatEvent>,
        packet_tx: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) {
        const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_S);
        const CONNECTION_BACKOFF: Duration = Duration::from_secs(CONNECTION_BACKOFF_S);

        let mut error_count: u64 = 0;

        while !exit.load(Ordering::Relaxed) {
            // Wait until a valid config is supplied (either initially or by admin rpc)
            // Use if!/else here to avoid extra CONNECTION_BACKOFF wait on successful termination
            let local_relayer_config = relayer_config.load();
            if !Self::is_valid_relayer_config(&local_relayer_config) {
                sleep(CONNECTION_BACKOFF).await;
            } else if let Err(e) = Self::connect_auth_and_stream(
                &local_relayer_config,
                &relayer_config,
                &cluster_info,
                &heartbeat_tx,
                &packet_tx,
                &exit,
                &CONNECTION_TIMEOUT,
            )
            .await
            {
                match e {
                    // This error is frequent on hot spares, and the parsed string does not work
                    // with datapoints (incorrect escaping).
                    ProxyError::AuthenticationPermissionDenied => {
                        warn!(
                            "relayer permission denied. not on leader schedule. ignore if \
                             hot-spare."
                        )
                    }
                    e => {
                        error_count += 1;
                        datapoint_warn!(
                            "relayer_stage-proxy_error",
                            ("count", error_count, i64),
                            ("error", e.to_string(), String),
                        );
                    }
                }
                sleep(CONNECTION_BACKOFF).await;
            }
        }
    }

    async fn connect_auth_and_stream(
        local_relayer_config: &RelayerConfig,
        global_relayer_config: &Arc<ArcSwap<RelayerConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        packet_tx: &Sender<PacketBatch>,
        exit: &Arc<AtomicBool>,
        connection_timeout: &Duration,
    ) -> crate::proxy::Result<()> {
        // Get a copy of configs here in case they have changed at runtime
        let keypair = cluster_info.keypair().clone();
        let backend_endpoint = Self::get_endpoint(&local_relayer_config.relayer_url)?;

        debug!("connecting to auth: {}", local_relayer_config.relayer_url);
        let (auth_client, access_token, refresh_token) =
            auth_client_from_endpoint(&backend_endpoint, connection_timeout, keypair.as_ref())
                .await?;

        datapoint_info!(
            "relayer_stage-tokens_generated",
            ("url", local_relayer_config.relayer_url, String),
            ("count", 1, i64),
        );

        debug!(
            "connecting to relayer: {}",
            local_relayer_config.relayer_url
        );
        let relayer_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::RelayerConnectionTimeout)?
            .map_err(|err| {
                ProxyError::RelayerConnectionError(
                    crate::proxy::sanitize_status_message_for_influx(&err.to_string()),
                )
            })?;
        let relayer_client = RelayerClient::with_interceptor(
            relayer_channel,
            AuthInterceptor::new(access_token.clone()),
        );

        Self::start_consuming_relayer_packets(
            relayer_client,
            heartbeat_tx,
            packet_tx,
            local_relayer_config,
            global_relayer_config,
            exit,
            auth_client,
            access_token,
            refresh_token,
            keypair,
            cluster_info,
            connection_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_consuming_relayer_packets(
        mut client: RelayerClient<InterceptedService<Channel, AuthInterceptor>>,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &RelayerConfig, // local copy of config with current connections
        global_config: &Arc<ArcSwap<RelayerConfig>>, // guarded reference for detecting run-time updates
        exit: &Arc<AtomicBool>,
        auth_client: AuthServiceClient<Channel>,
        access_token: Arc<ArcSwap<Token>>,
        refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        connection_timeout: &Duration,
    ) -> crate::proxy::Result<()> {
        let heartbeat_event: HeartbeatEvent = {
            let tpu_config = timeout(
                *connection_timeout,
                client.get_tpu_configs(relayer::GetTpuConfigsRequest {}),
            )
            .await
            .map_err(|_| ProxyError::MethodTimeout("relayer_get_tpu_configs".to_string()))?
            .map_err(|status| ProxyError::MethodError {
                code: status.code(),
                message: crate::proxy::sanitize_status_message_for_influx(status.message()),
            })?
            .into_inner();

            let tpu_addr = tpu_config
                .tpu
                .ok_or_else(|| ProxyError::MissingTpuSocket("tpu".to_string()))?;
            let tpu_forward_addr = tpu_config
                .tpu_forward
                .ok_or_else(|| ProxyError::MissingTpuSocket("tpu_fwd".to_string()))?;

            let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
            let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

            let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
            let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);
            (tpu_socket, tpu_forward_socket)
        };

        let packet_stream = timeout(
            *connection_timeout,
            client.subscribe_packets(relayer::SubscribePacketsRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("relayer_subscribe_packets".to_string()))?
        .map_err(|status| ProxyError::MethodError {
            code: status.code(),
            message: crate::proxy::sanitize_status_message_for_influx(status.message()),
        })?
        .into_inner();

        Self::consume_packet_stream(
            heartbeat_event,
            heartbeat_tx,
            packet_stream,
            packet_tx,
            local_config,
            global_config,
            exit,
            auth_client,
            access_token,
            refresh_token,
            keypair,
            cluster_info,
            connection_timeout,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_packet_stream(
        heartbeat_event: HeartbeatEvent,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        mut packet_stream: Streaming<relayer::SubscribePacketsResponse>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &RelayerConfig, // local copy of config with current connections
        global_config: &Arc<ArcSwap<RelayerConfig>>, // guarded reference for detecting run-time updates
        exit: &Arc<AtomicBool>,
        mut auth_client: AuthServiceClient<Channel>,
        access_token: Arc<ArcSwap<Token>>,
        mut refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        connection_timeout: &Duration,
    ) -> crate::proxy::Result<()> {
        const METRICS_TICK: Duration = Duration::from_secs(1);
        let refresh_within_s: u64 = METRICS_TICK.as_secs().saturating_mul(3).saturating_div(2);

        let mut relayer_stats = RelayerStageStats::default();
        let mut metrics_and_auth_tick = interval(METRICS_TICK);

        let mut num_full_refreshes: u64 = 1;
        let mut num_refresh_access_token: u64 = 0;

        let mut heartbeat_check_interval = interval(local_config.expected_heartbeat_interval);
        let mut last_heartbeat_ts = Instant::now();

        info!("connected to packet stream");

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg
                        .map_err(ProxyError::from)?
                        .ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_relayer_packets(resp, heartbeat_event, heartbeat_tx, &mut last_heartbeat_ts, packet_tx, &mut relayer_stats)?;
                }
                _ = heartbeat_check_interval.tick() => {
                    if last_heartbeat_ts.elapsed() > local_config.oldest_allowed_heartbeat {
                        return Err(ProxyError::HeartbeatExpired);
                    }
                }
                _ = metrics_and_auth_tick.tick() => {
                    relayer_stats.report();
                    relayer_stats = RelayerStageStats::default();

                    if cluster_info.id() != keypair.pubkey() {
                        return Err(ProxyError::AuthenticationConnectionError("validator identity changed".to_string()));
                    }

                    if global_config.load().as_ref() != local_config {
                        return Err(ProxyError::AuthenticationConnectionError("relayer config changed".to_string()));
                    }

                    let (maybe_new_access, maybe_new_refresh) = maybe_refresh_auth_tokens(&mut auth_client,
                        &access_token,
                        &refresh_token,
                        cluster_info,
                        connection_timeout,
                        refresh_within_s,
                    ).await?;

                    if let Some(new_token) = maybe_new_access {
                        num_refresh_access_token += 1;
                        datapoint_info!(
                            "relayer_stage-refresh_access_token",
                            ("url", &local_config.relayer_url, String),
                            ("count", num_refresh_access_token, i64),
                        );

                        access_token.store(Arc::new(new_token));
                    }
                    if let Some(new_token) = maybe_new_refresh {
                        num_full_refreshes += 1;
                        datapoint_info!(
                            "relayer_stage-tokens_generated",
                            ("url", &local_config.relayer_url, String),
                            ("count", num_full_refreshes, i64),
                        );
                        refresh_token = new_token;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_relayer_packets(
        subscribe_packets_resp: relayer::SubscribePacketsResponse,
        heartbeat_event: HeartbeatEvent,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        last_heartbeat_ts: &mut Instant,
        packet_tx: &Sender<PacketBatch>,
        relayer_stats: &mut RelayerStageStats,
    ) -> crate::proxy::Result<()> {
        match subscribe_packets_resp.msg {
            None => {
                relayer_stats.num_empty_messages.add_assign(1);
            }
            Some(relayer::subscribe_packets_response::Msg::Batch(proto_batch)) => {
                if proto_batch.packets.is_empty() {
                    relayer_stats.num_empty_messages.add_assign(1);
                    return Ok(());
                }

                let packet_batch = PacketBatch::from(
                    proto_batch
                        .packets
                        .into_iter()
                        .map(proto_packet_to_packet)
                        .collect::<Vec<BytesPacket>>(),
                );

                relayer_stats
                    .num_packets
                    .add_assign(packet_batch.len() as u64);

                packet_tx
                    .send(packet_batch)
                    .map_err(|_| ProxyError::PacketForwardError)?;
            }
            Some(relayer::subscribe_packets_response::Msg::Heartbeat(_)) => {
                relayer_stats.num_heartbeats.add_assign(1);

                *last_heartbeat_ts = Instant::now();
                heartbeat_tx
                    .send(heartbeat_event)
                    .map_err(|_| ProxyError::HeartbeatChannelError)?;
            }
        }
        Ok(())
    }

    pub fn is_valid_relayer_config(config: &RelayerConfig) -> bool {
        if config.relayer_url.is_empty() {
            return false;
        }
        if config.oldest_allowed_heartbeat.is_zero() {
            error!("can't connect to relayer. oldest allowed heartbeat must be greater than 0.");
            return false;
        }
        if config.expected_heartbeat_interval.is_zero() {
            error!("can't connect to relayer. expected heartbeat interval must be greater than 0.");
            return false;
        }
        if let Err(e) = Self::get_endpoint(&config.relayer_url) {
            error!("can't connect to relayer. error creating relayer endpoint - {e}");
            return false;
        }
        true
    }

    fn get_endpoint(relayer_url: &str) -> crate::proxy::Result<Endpoint> {
        endpoint_from_url(
            relayer_url,
            || {
                ProxyError::RelayerConnectionError(format!(
                    "invalid relayer url value: {relayer_url}"
                ))
            },
            || {
                ProxyError::RelayerConnectionError(
                    "failed to set tls_config for relayer service".to_string(),
                )
            },
        )
    }
}
