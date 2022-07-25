//! The `relayer_stage` maintains connections to relayers and block engines.
//!
//! Relayer:
//! - acts as a TPU proxy.
//! - sends transactions to the validator
//! - do not support bundles to avoid DOS vector.
//! - when validator connects, it changes its TPU and TPU forward address to the relayer.
//! - expected to send heartbeat to validator as watchdog. if watchdog times out, the validator
//!   disconnects and reverts the TPU and TPU forward settings
//!
//! Block Engines:
//! - acts as a system that sends high profit bundles and transactions to a validator.
//! - sends transactions and bundles to the validator.
//! - when validator connects, it doesn't touch the TPU and TPU forward addresses.
//! - expected to send heartbeat to the validator as a watchdog. if watchdog times out, the validator
//!   disconnects and reconnects.
//!
//! If the block engine and relayer address are the same or only a block engine address is provided,
//! it also serves the same functionality as a relayer.

use {
    crate::{
        backoff::BackoffStrategy, bundle::PacketBundle, proto_packet_to_packet,
        sigverify::SigverifyTracerPacketStats,
    },
    crossbeam_channel::{select, tick, unbounded, Receiver, Sender},
    jito_protos::proto::{
        block_engine::{self, block_engine_validator_client::BlockEngineValidatorClient},
        relayer::{self, relayer_client::RelayerClient},
    },
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_metrics::datapoint_info,
    solana_perf::packet::PacketBatch,
    solana_sdk::{hash::hash, signer::Signer},
    std::{
        cell::RefCell,
        fs::File,
        io::Read,
        net::{AddrParseError, IpAddr, Ipv4Addr, SocketAddr},
        rc::Rc,
        result,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::time::{interval, sleep},
    tonic::{
        codegen::InterceptedService,
        metadata::MetadataValue,
        service::Interceptor,
        transport::{Certificate, Channel, ClientTlsConfig, Endpoint},
        Code, Status, Streaming,
    },
    uuid::Uuid,
};

type Result<T> = result::Result<T, RelayerStageError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);

const HEARTBEAT_TIMEOUT_MS: Duration = Duration::from_millis(1500); // Empirically determined from load testing
const DISCONNECT_DELAY_SEC: Duration = Duration::from_secs(60);
const METRICS_CADENCE_SEC: Duration = Duration::from_secs(1);

#[derive(Clone, Debug, Default)]
pub struct RelayerAndBlockEngineConfig {
    pub relayer_address: String,
    pub trust_relayer_packets: bool,
    pub block_engine_address: String,
    pub trust_block_engine_packets: bool,
}

/// Intercepts requests and adds the necessary headers for auth.
#[derive(Clone)]
pub struct AuthInterceptor {
    cluster_info: Arc<ClusterInfo>,
    token: Rc<RefCell<String>>,
}

impl AuthInterceptor {
    pub(crate) fn new(cluster_info: Arc<ClusterInfo>, token: Rc<RefCell<String>>) -> Self {
        AuthInterceptor {
            cluster_info,
            token,
        }
    }

    pub(crate) fn should_retry(
        status: &Status,
        token: Rc<RefCell<String>>,
        max_retries: usize,
        n_retries: usize,
    ) -> bool {
        if max_retries == n_retries {
            return false;
        }

        let mut token = token.borrow_mut();
        if let Some(new_token) = Self::maybe_new_auth_token(status, &token) {
            *token = new_token;
            true
        } else {
            false
        }
    }

    /// Checks to see if the server returned a token to be signed and if it does not equal the current
    /// token then the new token is returned and authentication can be retried.
    fn maybe_new_auth_token(status: &Status, current_token: &str) -> Option<String> {
        if status.code() != Code::Unauthenticated {
            return None;
        }

        let msg = status.message().split_whitespace().collect::<Vec<&str>>();
        if msg.len() != 2 {
            return None;
        }

        if msg[0] != "token:" {
            return None;
        }

        if msg[1] != current_token {
            Some(msg[1].to_string())
        } else {
            None
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> result::Result<tonic::Request<()>, Status> {
        // Prefix with pubkey and hash it in order to ensure BlockEngine doesn't have us sign a malicious transaction.
        let token = format!(
            "{}-{}",
            self.cluster_info.keypair().pubkey(),
            self.token.take(),
        );
        let hashed_token = hash(token.as_bytes());

        request.metadata_mut().append_bin(
            "public-key-bin",
            MetadataValue::from_bytes(&self.cluster_info.keypair().pubkey().to_bytes()),
        );
        request.metadata_mut().append_bin(
            "message-bin",
            MetadataValue::from_bytes(hashed_token.to_bytes().as_slice()),
        );
        request.metadata_mut().append_bin(
            "signature-bin",
            MetadataValue::from_bytes(
                self.cluster_info
                    .keypair()
                    .sign_message(hashed_token.to_bytes().as_slice())
                    .as_ref(),
            ),
        );

        Ok(request)
    }
}

#[derive(Error, Debug)]
pub enum RelayerStageError {
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
    #[error("stream disconnected")]
    GrpcStreamDisconnected,
    #[error("heartbeat error")]
    HeartbeatChannelError,
    #[error("heartbeat expired")]
    HeartbeatExpired,
    #[error("error forwarding packet to banking stage")]
    PacketForwardError,
    #[error("missing tpu config: {0:?}")]
    MissingTpuSocket(String),
    #[error("invalid socket address: {0:?}")]
    InvalidSocketAddress(#[from] AddrParseError),
    #[error("shutdown")]
    Shutdown,
}

pub struct RelayerAndBlockEngineStage {
    _heartbeat_sender: Sender<HeartbeatEvent>,
    relayer_threads: Vec<JoinHandle<()>>,
    heartbeat_thread: JoinHandle<()>,
}

impl RelayerAndBlockEngineStage {
    /// verified_packet_sender is the same channel verified packets are sent on.
    /// packet_sender goes through the find sender stake and rest of the TPU
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        relayer_config: RelayerAndBlockEngineConfig,
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        bundle_sender: Sender<Vec<PacketBundle>>,
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let (tpu_proxy_heartbeat_sender, tpu_proxy_heartbeat_receiver) = unbounded();

        let RelayerAndBlockEngineConfig {
            relayer_address,
            trust_relayer_packets,
            block_engine_address,
            trust_block_engine_packets,
        } = relayer_config;

        let proxy_threads = Self::spawn_relayer_threads(
            relayer_address,
            trust_relayer_packets,
            block_engine_address,
            trust_block_engine_packets,
            cluster_info,
            verified_packet_sender,
            packet_sender.clone(),
            tpu_proxy_heartbeat_sender.clone(),
            bundle_sender,
            exit.clone(),
        );

        // This thread is responsible for connecting and disconnecting the fetch stage to prevent
        // circumventing TPU proxy.
        let heartbeat_thread = Self::heartbeat_thread(
            packet_intercept_receiver,
            packet_sender,
            tpu_proxy_heartbeat_receiver,
            cluster_info,
            exit,
        );

        Self {
            // if no validator interface address provided, sender side of the channel gets dropped
            // in heartbeat thread and causes packet forwarding to error. this reference
            // on self, prevents it from being dropped
            _heartbeat_sender: tpu_proxy_heartbeat_sender,
            relayer_threads: proxy_threads,
            heartbeat_thread,
        }
    }

    fn spawn_relayer_threads(
        relayer_address: String,
        trust_relayer_packets: bool,
        block_engine_address: String,
        trust_block_engine_packets: bool,
        cluster_info: &Arc<ClusterInfo>,
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        packet_sender: Sender<PacketBatch>,
        tpu_proxy_heartbeat_sender: Sender<HeartbeatEvent>,
        bundle_sender: Sender<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        if relayer_address == block_engine_address || relayer_address.is_empty() {
            // block engine is also a relayer
            // the block engine can send packets and bundles
            // the block engine heartbeat controls the TPU address
            vec![Self::start_block_engine_thread(
                block_engine_address,
                trust_block_engine_packets,
                packet_sender,
                verified_packet_sender,
                Some(tpu_proxy_heartbeat_sender),
                bundle_sender,
                cluster_info.clone(),
                exit,
            )]
        } else {
            // the relayer acts as the TPU proxy and the block engine sends bundles
            // both the relayer and block engine can send packets
            // only the relayer heartbeats and controls the TPU address
            // only the block engine supports bundles
            vec![
                Self::start_block_engine_thread(
                    block_engine_address,
                    trust_block_engine_packets,
                    packet_sender.clone(),
                    verified_packet_sender.clone(),
                    None, // connected to a relayer, the relayer will heartbeat to tpu
                    bundle_sender,
                    cluster_info.clone(),
                    exit.clone(),
                ),
                Self::start_relayer_thread(
                    relayer_address,
                    trust_relayer_packets,
                    packet_sender,
                    verified_packet_sender,
                    Some(tpu_proxy_heartbeat_sender),
                    cluster_info.clone(),
                    exit,
                ),
            ]
        }
    }

    /// Connects to the block engine.
    /// If tpu_proxy_heartbeat_sender is some, the block engine is also the relayer and will advertise
    /// the block engine's IP address.
    fn start_block_engine_thread(
        address: String,
        trust_block_engine_packets: bool,
        packet_sender: Sender<PacketBatch>, // if !trust_block_engine_packets, use this sender for packets
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: Option<Sender<HeartbeatEvent>>,
        bundle_sender: Sender<Vec<PacketBundle>>,
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-block-engine-thread".into())
            .spawn(move || {
                let endpoint = Endpoint::from_shared(address.clone());

                if !address.contains("http") || endpoint.is_err() {
                    error!("missing or malformed mev proxy address provided, exiting mev loop [address={}]", address);
                    datapoint_info!("block-engine-error", ("bad_proxy_addr", 1, i64));
                    return;
                }

                let mut endpoint = endpoint.unwrap();
                if address.as_str().contains("https") {
                    let mut buf = Vec::new();
                    File::open("/etc/ssl/certs/jito_ca.pem")
                        .unwrap()
                        .read_to_end(&mut buf)
                        .unwrap();
                    endpoint = endpoint
                        .tls_config(
                            ClientTlsConfig::new()
                                .domain_name("jito.wtf")
                                .ca_certificate(Certificate::from_pem(buf)),
                        )
                        .unwrap();
                }

                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let mut backoff = BackoffStrategy::new();

                    loop {
                        match endpoint.connect().await {
                            Ok(channel) => {
                                let token = Rc::new(RefCell::new(String::default()));
                                match Self::start_block_engine_stream(
                                    BlockEngineValidatorClientWrapper {
                                        inner: BlockEngineValidatorClient::with_interceptor(channel, AuthInterceptor::new(cluster_info.clone(), token.clone())),
                                        token,
                                        max_retries: 4,
                                    },
                                    &trust_block_engine_packets,
                                    &packet_sender,
                                    &verified_packet_sender,
                                    &tpu_proxy_heartbeat_sender,
                                    &bundle_sender,
                                    &exit,
                                    &mut backoff,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error in block engine stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to block engine: {:?}", e);
                            }
                        }

                        sleep(Duration::from_millis(backoff.next_wait())).await;
                    }
                });
            })
            .unwrap()
    }

    async fn start_block_engine_stream(
        mut client_wrapper: BlockEngineValidatorClientWrapper,
        trust_block_engine_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        exit: &Arc<AtomicBool>,
        backoff: &mut BackoffStrategy,
    ) -> Result<()> {
        let maybe_heartbeat_event: Option<HeartbeatEvent> = if tpu_proxy_heartbeat_sender.is_some()
        {
            let tpu_config = client_wrapper
                .get_tpu_configs(block_engine::GetTpuConfigsRequest {})
                .await?;
            let tpu_addr = tpu_config
                .tpu
                .ok_or_else(|| RelayerStageError::MissingTpuSocket("tpu".into()))?;
            let tpu_forward_addr = tpu_config
                .tpu_forward
                .ok_or_else(|| RelayerStageError::MissingTpuSocket("tpu_fwd".into()))?;

            let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
            let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

            let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
            let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);
            Some((tpu_socket, tpu_forward_socket))
        } else {
            None
        };

        let subscribe_packets_stream = client_wrapper
            .subscribe_packets(block_engine::SubscribePacketsRequest {})
            .await?;
        let subscribe_bundles_stream = client_wrapper
            .subscribe_bundles(block_engine::SubscribeBundlesRequest {})
            .await?;

        backoff.reset();

        Self::stream_block_engine_bundles_and_packets(
            maybe_heartbeat_event,
            subscribe_packets_stream,
            subscribe_bundles_stream,
            trust_block_engine_packets,
            packet_sender,
            verified_packet_sender,
            bundle_sender,
            tpu_proxy_heartbeat_sender,
            exit,
        )
        .await
    }

    async fn start_relayer_stream(
        mut client_wrapper: RelayerClientWrapper,
        trust_relayer_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        exit: &Arc<AtomicBool>,
        backoff: &mut BackoffStrategy,
    ) -> Result<()> {
        let heartbeat_event: HeartbeatEvent = {
            let tpu_config = client_wrapper
                .get_tpu_configs(relayer::GetTpuConfigsRequest {})
                .await?;
            let tpu_addr = tpu_config
                .tpu
                .ok_or_else(|| RelayerStageError::MissingTpuSocket("tpu".into()))?;
            let tpu_forward_addr = tpu_config
                .tpu_forward
                .ok_or_else(|| RelayerStageError::MissingTpuSocket("tpu_fwd".into()))?;

            let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
            let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

            let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
            let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);
            (tpu_socket, tpu_forward_socket)
        };

        let subscribe_packets_stream = client_wrapper
            .subscribe_packets(relayer::SubscribePacketsRequest {})
            .await?;

        // assume it's all good here
        backoff.reset();

        Self::stream_relayer_packets(
            heartbeat_event,
            subscribe_packets_stream,
            trust_relayer_packets,
            packet_sender,
            verified_packet_sender,
            tpu_proxy_heartbeat_sender,
            exit,
        )
        .await
    }

    async fn stream_relayer_packets(
        heartbeat_event: HeartbeatEvent,
        mut packet_stream: Streaming<relayer::SubscribePacketsResponse>,
        trust_relayer_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        exit: &Arc<AtomicBool>,
    ) -> Result<()> {
        info!("relayer starting bundle and packet stream");

        let mut heartbeat_check_tick = interval(Duration::from_millis(500));
        let mut last_heartbeat = Instant::now();

        loop {
            if exit.load(Ordering::Relaxed) {
                return Err(RelayerStageError::Shutdown);
            }

            tokio::select! {
                maybe_packets = packet_stream.message() => {
                    Self::handle_relayer_packets(maybe_packets, &heartbeat_event, tpu_proxy_heartbeat_sender, trust_relayer_packets, packet_sender, verified_packet_sender, &mut last_heartbeat)?;
                }
                _ = heartbeat_check_tick.tick() => {
                    if last_heartbeat.elapsed() > Duration::from_millis(1_500) {
                        return Err(RelayerStageError::HeartbeatExpired);
                    }
                }
            }
        }
    }

    async fn stream_block_engine_bundles_and_packets(
        maybe_heartbeat_event: Option<HeartbeatEvent>,
        mut packet_stream: Streaming<block_engine::SubscribePacketsResponse>,
        mut bundle_stream: Streaming<block_engine::SubscribeBundlesResponse>,
        trust_block_engine_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        exit: &Arc<AtomicBool>,
    ) -> Result<()> {
        let mut heartbeat_check_tick = interval(Duration::from_millis(500));
        let mut last_heartbeat = Instant::now();

        info!("block engine starting bundle and packet stream");
        loop {
            if exit.load(Ordering::Relaxed) {
                return Err(RelayerStageError::Shutdown);
            }

            tokio::select! {
                maybe_packets = packet_stream.message() => {
                    Self::handle_block_engine_packets(maybe_packets, &maybe_heartbeat_event, tpu_proxy_heartbeat_sender, trust_block_engine_packets, packet_sender, verified_packet_sender, &mut last_heartbeat)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_sender)?;
                }
                _ = heartbeat_check_tick.tick() => {
                    if last_heartbeat.elapsed() > Duration::from_millis(1_500) {
                        return Err(RelayerStageError::HeartbeatExpired);
                    }
                }
            }
        }
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: result::Result<
            Option<block_engine::SubscribeBundlesResponse>,
            Status,
        >,
        bundle_sender: &Sender<Vec<PacketBundle>>,
    ) -> Result<()> {
        let bundles_response =
            maybe_bundles_response?.ok_or(RelayerStageError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle {
                    batch: PacketBatch::new(
                        bundle
                            .bundle?
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect(),
                    ),
                    uuid: Uuid::from_str(&bundle.uuid).ok()?,
                })
            })
            .collect();
        bundle_sender
            .send(bundles)
            .map_err(|_| RelayerStageError::PacketForwardError)
    }

    fn handle_relayer_packets(
        maybe_packets_response: result::Result<Option<relayer::SubscribePacketsResponse>, Status>,
        maybe_heartbeat_event: &HeartbeatEvent,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        trust_relayer_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        last_heartbeat: &mut Instant,
    ) -> Result<()> {
        let packets = maybe_packets_response?.ok_or(RelayerStageError::GrpcStreamDisconnected)?;
        match packets.msg {
            None => return Err(RelayerStageError::GrpcStreamDisconnected),
            Some(relayer::subscribe_packets_response::Msg::Batch(proto_batch)) => {
                let packet_batch = PacketBatch::new(
                    proto_batch
                        .packets
                        .into_iter()
                        .map(proto_packet_to_packet)
                        .collect(),
                );

                if *trust_relayer_packets {
                    verified_packet_sender
                        .send((vec![packet_batch], None))
                        .map_err(|_| RelayerStageError::PacketForwardError)?;
                } else {
                    packet_sender
                        .send(packet_batch)
                        .map_err(|_| RelayerStageError::PacketForwardError)?;
                }
            }
            Some(relayer::subscribe_packets_response::Msg::Heartbeat(_)) => {
                *last_heartbeat = Instant::now();
                if let Some(tpu_proxy_heartbeat_sender) = tpu_proxy_heartbeat_sender {
                    tpu_proxy_heartbeat_sender
                        .send(*maybe_heartbeat_event)
                        .map_err(|_| RelayerStageError::HeartbeatChannelError)?;
                }
            }
        }
        Ok(())
    }

    fn handle_block_engine_packets(
        maybe_packets_response: result::Result<
            Option<block_engine::SubscribePacketsResponse>,
            Status,
        >,
        maybe_heartbeat_event: &Option<HeartbeatEvent>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        trust_block_engine_packets: &bool,
        packet_sender: &Sender<PacketBatch>,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        last_heartbeat: &mut Instant,
    ) -> Result<()> {
        let packets = maybe_packets_response?.ok_or(RelayerStageError::GrpcStreamDisconnected)?;
        match packets.msg {
            None => return Err(RelayerStageError::GrpcStreamDisconnected),
            Some(block_engine::subscribe_packets_response::Msg::Batch(proto_batch)) => {
                let packet_batch = PacketBatch::new(
                    proto_batch
                        .packets
                        .into_iter()
                        .map(proto_packet_to_packet)
                        .collect(),
                );
                if *trust_block_engine_packets {
                    verified_packet_sender
                        .send((vec![packet_batch], None))
                        .map_err(|_| RelayerStageError::PacketForwardError)?;
                } else {
                    packet_sender
                        .send(packet_batch)
                        .map_err(|_| RelayerStageError::PacketForwardError)?;
                }
            }
            Some(block_engine::subscribe_packets_response::Msg::Heartbeat(_)) => {
                *last_heartbeat = Instant::now();
                if let Some(tpu_proxy_heartbeat_sender) = tpu_proxy_heartbeat_sender {
                    tpu_proxy_heartbeat_sender
                        .send((*maybe_heartbeat_event).unwrap())
                        .map_err(|_| RelayerStageError::HeartbeatChannelError)?;
                }
            }
        }
        Ok(())
    }

    fn start_relayer_thread(
        address: String,
        trust_relayer_packets: bool,
        packet_sender: Sender<PacketBatch>, // if !trust_relayer_packets, use this sender for packets
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: Option<Sender<HeartbeatEvent>>,
        cluster_info: Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-relayer-thread".into())
            .spawn(move || {
                if !address.contains("http") {
                    info!("malformed or missing mev proxy address provided, exiting mev loop");
                    datapoint_info!("relayer-connection-error", ("bad_proxy_addr", 1, i64));
                    return;
                }
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let mut backoff = BackoffStrategy::new();

                    loop {
                        let endpoint = Endpoint::from_shared(address.clone()).unwrap();
                        match endpoint.connect().await {
                            Ok(channel) => {
                                let token = Rc::new(RefCell::new(String::default()));
                                let client_wrapper = RelayerClientWrapper {
                                    inner: RelayerClient::with_interceptor(
                                        channel,
                                        AuthInterceptor::new(cluster_info.clone(), token.clone()),
                                    ),
                                    token,
                                    max_retries: 4,
                                };
                                match Self::start_relayer_stream(
                                    client_wrapper,
                                    &trust_relayer_packets,
                                    &packet_sender,
                                    &verified_packet_sender,
                                    &tpu_proxy_heartbeat_sender,
                                    &exit,
                                    &mut backoff,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error in relayer stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to relayer: {:?}", e);
                            }
                        }
                        sleep(Duration::from_millis(backoff.next_wait())).await;
                    }
                });
            })
            .unwrap()
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
    fn heartbeat_thread(
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
        tpu_proxy_heartbeat_receiver: Receiver<HeartbeatEvent>,
        cluster_info: &Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let cluster_info = cluster_info.clone();
        Builder::new()
            .name("heartbeat_thread".into())
            .spawn(move || {
                let saved_contact_info = cluster_info.my_contact_info();
                let mut fetch_connected = true;
                let mut heartbeat_received = false;
                let mut pending_disconnect = false;
                // initialized to avoid compiler errors but overwritten before read
                let mut pending_disconnect_ts = Instant::now();
                let heartbeat_tick = tick(HEARTBEAT_TIMEOUT_MS);
                let metrics_tick = tick(METRICS_CADENCE_SEC);
                let mut packets_forwarded = 0;
                let mut heartbeats_received = 0;
                loop {
                    select! {
                        recv(packet_intercept_receiver) -> pkt => {
                            match pkt {
                                Ok(pkt) => {
                                    if fetch_connected {
                                        if packet_sender.send(pkt).is_err() {
                                            error!("{:?}", RelayerStageError::PacketForwardError);
                                            return;
                                        }
                                        packets_forwarded += 1;
                                    }
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
                            if !heartbeat_received && (!fetch_connected || pending_disconnect) {
                                warn!("heartbeat late, reconnecting fetch stage");
                                fetch_connected = true;
                                pending_disconnect = false;
                                Self::set_tpu_addresses(&cluster_info, saved_contact_info.tpu, saved_contact_info.tpu_forwards);
                                heartbeats_received = 0;
                            }
                            heartbeat_received = false;
                        }
                        recv(tpu_proxy_heartbeat_receiver) -> tpu_info => {
                            if let Ok((tpu_addr, tpu_forward_addr)) = tpu_info {
                                heartbeats_received += 1;
                                heartbeat_received = true;
                                if fetch_connected && !pending_disconnect {
                                    info!("received heartbeat while fetch stage connected, pending disconnect after delay");
                                    pending_disconnect_ts = Instant::now();
                                    pending_disconnect = true;
                                }
                                if fetch_connected && pending_disconnect && pending_disconnect_ts.elapsed() > DISCONNECT_DELAY_SEC {
                                    info!("disconnecting fetch stage");
                                    fetch_connected = false;
                                    pending_disconnect = false;
                                    Self::set_tpu_addresses(&cluster_info, tpu_addr, tpu_forward_addr);
                                }
                            } else {
                                // see comment on heartbeat_sender clone in new()
                                unreachable!();
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
    ) {
        let mut new_contact_info = cluster_info.my_contact_info();
        new_contact_info.tpu = tpu_address;
        new_contact_info.tpu_forwards = tpu_forward_address;
        cluster_info.set_my_contact_info(new_contact_info);
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.relayer_threads {
            t.join()?;
        }
        self.heartbeat_thread.join()?;
        Ok(())
    }
}

pub struct RelayerClientWrapper {
    inner: RelayerClient<InterceptedService<Channel, AuthInterceptor>>,
    token: Rc<RefCell<String>>,
    max_retries: usize,
}

impl RelayerClientWrapper {
    async fn get_tpu_configs(
        &mut self,
        req: relayer::GetTpuConfigsRequest,
    ) -> Result<relayer::GetTpuConfigsResponse> {
        let mut n_retries = 0;
        loop {
            return match self.inner.get_tpu_configs(req.clone()).await {
                Ok(resp) => Ok(resp.into_inner()),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status.into())
                }
            };
        }
    }

    async fn subscribe_packets(
        &mut self,
        req: relayer::SubscribePacketsRequest,
    ) -> Result<Streaming<relayer::SubscribePacketsResponse>> {
        let mut n_retries = 0;
        loop {
            return match self.inner.subscribe_packets(req.clone()).await {
                Ok(resp) => Ok(resp.into_inner()),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status.into())
                }
            };
        }
    }
}

pub struct BlockEngineValidatorClientWrapper {
    inner: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
    token: Rc<RefCell<String>>,
    max_retries: usize,
}

impl BlockEngineValidatorClientWrapper {
    async fn get_tpu_configs(
        &mut self,
        req: block_engine::GetTpuConfigsRequest,
    ) -> Result<block_engine::GetTpuConfigsResponse> {
        let mut n_retries = 0;
        loop {
            return match self.inner.get_tpu_configs(req.clone()).await {
                Ok(resp) => Ok(resp.into_inner()),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status.into())
                }
            };
        }
    }

    async fn subscribe_packets(
        &mut self,
        req: block_engine::SubscribePacketsRequest,
    ) -> Result<Streaming<block_engine::SubscribePacketsResponse>> {
        let mut n_retries = 0;
        loop {
            return match self.inner.subscribe_packets(req.clone()).await {
                Ok(resp) => Ok(resp.into_inner()),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status.into())
                }
            };
        }
    }

    async fn subscribe_bundles(
        &mut self,
        req: block_engine::SubscribeBundlesRequest,
    ) -> Result<Streaming<block_engine::SubscribeBundlesResponse>> {
        let mut n_retries = 0;
        loop {
            return match self.inner.subscribe_bundles(req.clone()).await {
                Ok(resp) => Ok(resp.into_inner()),
                Err(status) => {
                    if AuthInterceptor::should_retry(
                        &status,
                        self.token.clone(),
                        self.max_retries,
                        n_retries,
                    ) {
                        n_retries += 1;
                        continue;
                    }
                    Err(status.into())
                }
            };
        }
    }
}
