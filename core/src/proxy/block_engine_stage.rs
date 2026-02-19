//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.

use {
    crate::{
        bam_dependencies::BamConnectionState,
        banking_trace::BankingPacketSender,
        packet_bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{generate_auth_tokens, maybe_refresh_auth_tokens, AuthInterceptor},
            ProxyError,
        },
    },
    ahash::HashMapExt,
    arc_swap::ArcSwap,
    crossbeam_channel::Sender,
    itertools::Itertools,
    jito_protos::proto::{
        auth::{auth_service_client::AuthServiceClient, Token},
        block_engine::{
            self, block_engine_validator_client::BlockEngineValidatorClient,
            BlockBuilderFeeInfoRequest, BlockEngineEndpoint, GetBlockEngineEndpointRequest,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_perf::packet::{BytesPacket, PacketBatch},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::hash_map::Entry,
        net::{SocketAddr, ToSocketAddrs},
        ops::AddAssign,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU8, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::{
        task,
        time::{interval, sleep, timeout},
    },
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Streaming,
    },
};

const CONNECTION_TIMEOUT_S: u64 = 10;
const CONNECTION_BACKOFF_S: u64 = 5;

#[derive(Default)]
struct BlockEngineStageStats {
    num_bundles: u64,
    num_bundle_packets: u64,
    num_packets: u64,
    num_empty_packets: u64,
}

impl BlockEngineStageStats {
    pub(crate) fn report(&self) {
        datapoint_info!(
            "block_engine_stage-stats",
            ("num_bundles", self.num_bundles, i64),
            ("num_bundle_packets", self.num_bundle_packets, i64),
            ("num_packets", self.num_packets, i64),
            ("num_empty_packets", self.num_empty_packets, i64)
        );
    }
}

#[derive(Clone, Default)]
pub struct BlockBuilderFeeInfo {
    pub block_builder: Pubkey,
    pub block_builder_commission: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockEngineConfig {
    /// Block Engine URL
    pub block_engine_url: String,

    /// Disables Block Engine auto-configuration. This stops the validator client from using the most performant Block Engine region. Values provided to `--block-engine-url` will be used as-is.
    pub disable_block_engine_autoconfig: bool,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

pub struct BlockEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}
#[derive(Error, Debug)]
enum ProbeError {
    #[error(transparent)]
    BuildEndpoint(#[from] ProxyError),

    #[error("gRPC connect timeout")]
    ConnectTimeout,

    #[error("gRPC connect error: {0}")]
    Connect(#[from] tonic::transport::Error),

    #[error("gRPC request error: {0}")]
    Request(#[from] tonic::Status),

    #[error("no successful probe samples")]
    NoSuccessfulSamples,
}

impl BlockEngineStage {
    const CONNECTION_TIMEOUT: Duration = Duration::from_secs(CONNECTION_TIMEOUT_S);
    const CONNECTION_BACKOFF: Duration = Duration::from_secs(CONNECTION_BACKOFF_S);
    pub fn new(
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that non-trusted packets get piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted packets get piped through.
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicU8>,
    ) -> Self {
        let block_builder_fee_info = block_builder_fee_info.clone();

        let thread = Builder::new()
            .name("block-engine-stage".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(Self::start(
                    block_engine_config,
                    cluster_info,
                    bundle_tx,
                    packet_tx,
                    banking_packet_sender,
                    exit,
                    block_builder_fee_info,
                    shredstream_receiver_address,
                    bam_enabled,
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
        block_engine_config: Arc<Mutex<BlockEngineConfig>>,
        cluster_info: Arc<ClusterInfo>,
        bundle_tx: Sender<Vec<PacketBundle>>,
        packet_tx: Sender<PacketBatch>,
        banking_packet_sender: BankingPacketSender,
        exit: Arc<AtomicBool>,
        block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicU8>,
    ) {
        let mut error_count: u64 = 0;

        while !exit.load(Ordering::Relaxed) {
            // Wait until a valid config is supplied (either initially or by admin rpc)
            // Use if!/else here to avoid extra CONNECTION_BACKOFF wait on successful termination
            let local_block_engine_config =
                task::block_in_place(|| block_engine_config.lock().unwrap().clone());
            if !Self::is_valid_block_engine_config(&local_block_engine_config) {
                shredstream_receiver_address.store(Arc::new(None));
                sleep(Self::CONNECTION_BACKOFF).await;
                continue;
            }

            if let Err(e) = Self::connect_auth_and_stream_maybe_autoconfig(
                &block_engine_config,
                &cluster_info,
                &bundle_tx,
                &packet_tx,
                &banking_packet_sender,
                &exit,
                &block_builder_fee_info,
                &shredstream_receiver_address,
                &local_block_engine_config,
                &bam_enabled,
            )
            .await
            {
                match e {
                    // This error is frequent on hot spares, and the parsed string does not work
                    // with datapoints (incorrect escaping).
                    ProxyError::AuthenticationPermissionDenied => warn!(
                        "block engine permission denied. not on leader schedule. ignore if \
                         hot-spare."
                    ),
                    ProxyError::BamEnabled => {}
                    e => {
                        error_count += 1;
                        datapoint_warn!(
                            "block_engine_stage-proxy_error",
                            ("count", error_count, i64),
                            ("error", e.to_string(), String),
                        );
                    }
                }
                sleep(Self::CONNECTION_BACKOFF).await;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream_maybe_autoconfig(
        block_engine_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        local_block_engine_config: &BlockEngineConfig,
        bam_enabled: &Arc<AtomicU8>,
    ) -> crate::proxy::Result<()> {
        if BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
            == BamConnectionState::Connected
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
            return Ok(());
        }

        let endpoint = Self::get_endpoint(&local_block_engine_config.block_engine_url)?;
        if !local_block_engine_config.disable_block_engine_autoconfig {
            datapoint_info!(
                "block_engine_stage-connect",
                "type" => "autoconfig",
                ("count", 1, i64),
            );
            return Self::connect_auth_and_stream_autoconfig(
                endpoint,
                local_block_engine_config,
                block_engine_config,
                cluster_info,
                bundle_tx,
                packet_tx,
                banking_packet_sender,
                exit,
                block_builder_fee_info,
                shredstream_receiver_address,
                bam_enabled,
            )
            .await
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err));
        }

        let mut backend_endpoint = endpoint.clone();
        if let Some((global_url, maybe_shredstream_socket)) = Self::get_global_endpoint(&endpoint)
            .await
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?
        {
            if maybe_shredstream_socket.is_some() {
                // no else branch needed since we'll still send to shred_receiver_addresses
                shredstream_receiver_address.store(Arc::new(maybe_shredstream_socket));
            }
            backend_endpoint = Self::get_endpoint(global_url.as_str())?;
        }

        datapoint_info!(
            "block_engine_stage-connect",
            "type" => "direct",
            ("count", 1, i64),
        );
        Self::connect_auth_and_stream(
            &backend_endpoint,
            local_block_engine_config,
            block_engine_config,
            cluster_info,
            bundle_tx,
            packet_tx,
            banking_packet_sender,
            exit,
            block_builder_fee_info,
            &Self::CONNECTION_TIMEOUT,
            bam_enabled,
        )
        .await
        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))
        .inspect(|_| {
            datapoint_info!(
                "block_engine_stage-connect",
                "type" => "closed_connection",
                ("url", endpoint.uri().to_string(), String),
                ("count", 1, i64),
            )
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream_autoconfig(
        endpoint: Endpoint,
        local_block_engine_config: &BlockEngineConfig,
        global_block_engine_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: &Arc<AtomicU8>,
    ) -> crate::proxy::Result<()> {
        let candidates = Self::get_ranked_endpoints(&endpoint)
            .await
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;

        // try connecting to best block engine
        let mut attempted = false;
        let mut backend_endpoint = endpoint.clone();
        let endpoint_count = candidates.len();
        for (block_engine_url, (maybe_shredstream_socket, latency_us)) in candidates
            .into_iter()
            .sorted_unstable_by_key(|(_endpoint, (_shredstream_socket, latency_us))| *latency_us)
        {
            if block_engine_url != local_block_engine_config.block_engine_url {
                info!(
                    "Selected best Block Engine url: {block_engine_url}, Shredstream socket: \
                     {maybe_shredstream_socket:?}, rtt: ({:?})",
                    Duration::from_micros(latency_us)
                );
                backend_endpoint = Self::get_endpoint(block_engine_url.as_str())?;
            }
            if let Some(shredstream_socket) = maybe_shredstream_socket {
                shredstream_receiver_address.store(Arc::new(Some(shredstream_socket)));
            }
            attempted = true;
            let connect_start = Instant::now();
            match Self::connect_auth_and_stream(
                &backend_endpoint,
                local_block_engine_config,
                global_block_engine_config,
                cluster_info,
                bundle_tx,
                packet_tx,
                banking_packet_sender,
                exit,
                block_builder_fee_info,
                &Self::CONNECTION_TIMEOUT,
                bam_enabled,
            )
            .await
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err))
            {
                Ok(()) => {
                    datapoint_info!(
                        "block_engine_stage-connect",
                        "type" => "closed_connection",
                        ("url", backend_endpoint.uri().to_string(), String),
                        ("count", 1, i64),
                    );
                    return Ok(());
                }
                Err(e) => {
                    // log each connection error
                    match &e {
                        // This error is frequent on hot spares, and the parsed string does not work
                        // with datapoints (incorrect escaping).
                        ProxyError::AuthenticationPermissionDenied => warn!(
                            "block engine permission denied. not on leader schedule. ignore if \
                             hot-spare."
                        ),
                        ProxyError::BamEnabled => return Ok(()),
                        other => {
                            datapoint_warn!(
                                "block_engine_stage-autoconfig_error",
                                "type" => "proxy_err",
                                ("url", block_engine_url, String),
                                ("count", 1, i64),
                                ("error", other.to_string(), String),
                            );
                        }
                    }

                    if connect_start.elapsed() > Self::CONNECTION_TIMEOUT * 3 {
                        return Err(e); // run a new round of probes and connect to new best
                    }
                    // Otherwise, try next endpoint without delay; caller handles backoff on overall failure
                }
            }
        }
        if !attempted {
            return Err(ProxyError::BlockEngineEndpointError(
                "autoconfig failed: no endpoints available after gRPC RTT ranking".to_string(),
            ));
        }
        Err(ProxyError::BlockEngineEndpointError(format!(
            "autoconfig failed: all {endpoint_count} candidate endpoints failed to connect",
        )))
    }

    fn map_bam_enabled(bam_enabled: &Arc<AtomicU8>, err: ProxyError) -> ProxyError {
        match BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed)) {
            BamConnectionState::Disconnected => err,
            BamConnectionState::Connecting | BamConnectionState::Connected => {
                ProxyError::BamEnabled
            }
        }
    }

    /// Discover candidate endpoints either ranked via gRPC RTT probe or using global fallback.
    /// Use u64::MAX for latency value to indicate global fallback (no RTT probe data).
    async fn get_ranked_endpoints(
        backend_endpoint: &Endpoint,
    ) -> crate::proxy::Result<
        ahash::HashMap<
            String, /* block engine url */
            (
                Option<SocketAddr>, /* shredstream receiver, fallible when DNS can't resolve */
                u64,                /* latency us */
            ),
        >,
    > {
        let mut endpoint_discovery = BlockEngineValidatorClient::connect(backend_endpoint.clone())
            .await
            .map_err(ProxyError::BlockEngineConnectionError)?;
        let endpoints = endpoint_discovery
            .get_block_engine_endpoints(GetBlockEngineEndpointRequest {})
            .await
            .map_err(ProxyError::BlockEngineRequestError)?
            .into_inner();
        datapoint_info!(
            "block_engine_stage-autoconfig",
            ("regioned_count", endpoints.regioned_endpoints.len(), i64),
            ("count", 1, i64),
        );
        let endpoint_latencies =
            Self::probe_and_rank_endpoints(&endpoints.regioned_endpoints).await;
        if endpoint_latencies.is_empty() {
            let Some(global) = endpoints.global_endpoint else {
                return Err(ProxyError::BlockEngineEndpointError(
                    "Block engine configuration failed: no reachable endpoints found".to_owned(),
                ));
            };

            let ss_res = global
                .shredstream_receiver_address
                .to_socket_addrs()
                .inspect_err(|e| {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "shredstream_resolve",
                        ("address", global.shredstream_receiver_address, String),
                        ("count", 1, i64),
                        ("err", e.to_string(), String),
                    );
                })
                .ok()
                .and_then(|mut shredstream_sockets| shredstream_sockets.next());

            return Ok(ahash::HashMap::from_iter([(
                global.block_engine_url,
                (ss_res, u64::MAX),
            )]));
        }

        Ok(endpoint_latencies)
    }

    async fn get_global_endpoint(
        backend_endpoint: &Endpoint,
    ) -> crate::proxy::Result<Option<(String, Option<SocketAddr>)>> {
        let mut endpoint_discovery = BlockEngineValidatorClient::connect(backend_endpoint.clone())
            .await
            .map_err(ProxyError::BlockEngineConnectionError)?;
        let endpoints = endpoint_discovery
            .get_block_engine_endpoints(GetBlockEngineEndpointRequest {})
            .await
            .map_err(ProxyError::BlockEngineRequestError)?
            .into_inner();

        let Some(global) = endpoints.global_endpoint else {
            return Err(ProxyError::BlockEngineEndpointError(
                "Block engine configuration failed: no global endpoint found".to_owned(),
            ));
        };

        datapoint_info!(
            "block_engine_stage-connect",
            "type" => "direct_global",
            ("count", 1, i64),
        );

        let ss_res = global
            .shredstream_receiver_address
            .to_socket_addrs()
            .inspect_err(|e| {
                datapoint_warn!(
                    "block_engine_stage-autoconfig_error",
                    "type" => "shredstream_resolve",
                    ("address", global.shredstream_receiver_address, String),
                    ("count", 1, i64),
                    ("err", e.to_string(), String),
                );
            })
            .ok()
            .and_then(|mut shredstream_sockets| shredstream_sockets.next());

        Ok(Some((global.block_engine_url, ss_res)))
    }

    #[allow(clippy::too_many_arguments)]
    async fn connect_auth_and_stream(
        backend_endpoint: &Endpoint,
        local_block_engine_config: &BlockEngineConfig,
        global_block_engine_config: &Arc<Mutex<BlockEngineConfig>>,
        cluster_info: &Arc<ClusterInfo>,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        connection_timeout: &Duration,
        bam_enabled: &Arc<AtomicU8>,
    ) -> crate::proxy::Result<()> {
        // Get a copy of configs here in case they have changed at runtime
        let keypair = cluster_info.keypair().clone();

        debug!("connecting to auth: {}", backend_endpoint.uri());
        let auth_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
            .map_err(|e| ProxyError::AuthenticationConnectionError(e.to_string()))
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;

        let mut auth_client = AuthServiceClient::new(auth_channel);

        debug!("generating authentication token");
        let (access_token, refresh_token) = timeout(
            *connection_timeout,
            generate_auth_tokens(&mut auth_client, &keypair),
        )
        .await
        .map_err(|_| ProxyError::AuthenticationTimeout)
        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))??;

        let backend_url = backend_endpoint.uri().to_string();
        datapoint_info!(
            "block_engine_stage-tokens_generated",
            ("url", backend_url, String),
            ("count", 1, i64),
        );

        debug!("connecting to block engine: {}", backend_endpoint.uri());
        let block_engine_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::BlockEngineConnectionTimeout)?
            .map_err(ProxyError::BlockEngineConnectionError)
            .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;

        let access_token = Arc::new(Mutex::new(access_token));
        let block_engine_client = BlockEngineValidatorClient::with_interceptor(
            block_engine_channel,
            AuthInterceptor::new(access_token.clone()),
        );

        datapoint_info!(
            "block_engine_stage-connected",
            ("url", backend_url, String),
            ("count", 1, i64),
        );

        Self::start_consuming_block_engine_bundles_and_packets(
            bundle_tx,
            block_engine_client,
            packet_tx,
            local_block_engine_config,
            global_block_engine_config,
            banking_packet_sender,
            exit,
            block_builder_fee_info,
            auth_client,
            access_token,
            refresh_token,
            connection_timeout,
            keypair,
            cluster_info,
            &backend_url,
            bam_enabled,
        )
        .await
    }

    /// Build an Endpoint from the URL provided
    fn get_endpoint(block_engine_url: &str) -> Result<Endpoint, ProxyError> {
        let mut backend_endpoint = Endpoint::from_shared(block_engine_url.to_owned())
            .map_err(|_| {
                ProxyError::BlockEngineEndpointError(format!(
                    "invalid block engine url value: {block_engine_url}",
                ))
            })?
            .tcp_keepalive(Some(Duration::from_secs(60)));
        if block_engine_url.starts_with("https") {
            backend_endpoint = backend_endpoint
                .tls_config(tonic::transport::ClientTlsConfig::new())
                .map_err(|_| {
                    ProxyError::BlockEngineEndpointError(format!(
                        "failed to set tls_config for block engine: {block_engine_url}",
                    ))
                })?;
        }
        Ok(backend_endpoint)
    }

    async fn probe_grpc_rtt_us(block_engine_url: &str) -> Result<u64, ProbeError> {
        const PROBE_COUNT: usize = 3;

        // Connect once and probe multiple times so we're not ranking on handshake costs.
        let endpoint = Self::get_endpoint(block_engine_url)?;
        let channel = timeout(Self::CONNECTION_TIMEOUT, endpoint.connect())
            .await
            .map_err(|_| ProbeError::ConnectTimeout)??;

        let mut client = BlockEngineValidatorClient::new(channel);

        let mut best_us: u64 = u64::MAX;
        let mut any_success = false;
        for sample in 0..PROBE_COUNT {
            let start = Instant::now();
            let res = timeout(
                Self::CONNECTION_TIMEOUT,
                client.get_block_engine_endpoints(GetBlockEngineEndpointRequest {}),
            )
            .await;
            match res {
                Ok(Ok(_resp)) => {
                    let elapsed_us = start.elapsed().as_micros() as u64;
                    any_success = true;
                    best_us = best_us.min(elapsed_us);
                    datapoint_info!(
                        "block_engine_stage-autoconfig_ping",
                        "method" => "grpc",
                        ("endpoint", block_engine_url, String),
                        ("latency_us", elapsed_us, i64),
                        ("sample", sample, i64),
                    );
                }
                Ok(Err(status)) => {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "probe_request",
                        ("url", block_engine_url, String),
                        ("count", 1, i64),
                        ("err", status.to_string(), String),
                    );
                }
                Err(_elapsed) => {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "probe_timeout",
                        ("url", block_engine_url, String),
                        ("count", 1, i64),
                        ("err", "timeout", String),
                    );
                }
            }
        }

        if any_success {
            Ok(best_us)
        } else {
            Err(ProbeError::NoSuccessfulSamples)
        }
    }

    /// Probe all candidate endpoints concurrently, aggregate best RTT per endpoint.
    async fn probe_and_rank_endpoints(
        endpoints: &[BlockEngineEndpoint],
    ) -> ahash::HashMap<
        String, /* block engine url */
        (
            Option<SocketAddr>, /* shredstream receiver, fallable when DNS can't resolve */
            u64,                /* latency us */
        ),
    > {
        let mut agg_endpoints: ahash::HashMap<
            String, /* block engine url */
            (
                Option<SocketAddr>, /* shredstream receiver, fallable when DNS can't resolve */
                u64,                /* latency us */
            ),
        > = ahash::HashMap::with_capacity(endpoints.len());
        let mut best_endpoint_url = String::new();
        let mut best_endpoint_rtt_us = u64::MAX;

        let tasks = endpoints
            .iter()
            .cloned()
            .map(|endpoint| {
                task::spawn(async move {
                    let rtt_res = Self::probe_grpc_rtt_us(&endpoint.block_engine_url).await;
                    (endpoint, rtt_res)
                })
            })
            .collect_vec();

        for join_res in futures::future::join_all(tasks).await {
            let (endpoint, rtt_res) = match join_res {
                Ok(v) => v,
                Err(e) => {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "probe_join",
                        ("count", 1, i64),
                        ("err", e.to_string(), String),
                    );
                    continue;
                }
            };

            let rtt_us = match rtt_res {
                Ok(v) => v,
                Err(e) => {
                    datapoint_warn!(
                        "block_engine_stage-autoconfig_error",
                        "type" => "probe",
                        ("url", endpoint.block_engine_url.as_str(), String),
                        ("count", 1, i64),
                        ("err", e.to_string(), String),
                    );
                    continue;
                }
            };

            if rtt_us <= best_endpoint_rtt_us {
                best_endpoint_rtt_us = rtt_us;
                best_endpoint_url = endpoint.block_engine_url.clone();
            }

            match agg_endpoints.entry(endpoint.block_engine_url.clone()) {
                Entry::Occupied(mut ent) => {
                    let (_shredstream_socket, best_rtt_us) = ent.get_mut();
                    if rtt_us <= *best_rtt_us {
                        *best_rtt_us = rtt_us;
                    }
                }
                Entry::Vacant(entry) => {
                    let maybe_shredstream_socket = endpoint
                        .shredstream_receiver_address
                        .to_socket_addrs()
                        .inspect_err(|e| {
                            datapoint_warn!(
                                "block_engine_stage-autoconfig_error",
                                "type" => "shredstream_resolve",
                                ("address", endpoint.block_engine_url.as_str(), String),
                                ("count", 1, i64),
                                ("err", e.to_string(), String),
                            );
                        })
                        .ok()
                        .and_then(|mut shredstream_sockets| shredstream_sockets.next());
                    entry.insert((maybe_shredstream_socket, rtt_us));
                }
            };
        }

        datapoint_info!(
            "block_engine_stage-autoconfig",
            ("endpoints_count", agg_endpoints.len(), i64),
            ("best_endpoint_url", best_endpoint_url.as_str(), String),
            ("best_endpoint_latency_us", best_endpoint_rtt_us, i64),
            ("count", 1, i64),
        );

        agg_endpoints
    }

    #[allow(clippy::too_many_arguments)]
    async fn start_consuming_block_engine_bundles_and_packets(
        bundle_tx: &Sender<Vec<PacketBundle>>,
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &BlockEngineConfig, // local copy of config with current connections
        global_config: &Arc<Mutex<BlockEngineConfig>>, // guarded reference for detecting run-time updates
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        refresh_token: Token,
        connection_timeout: &Duration,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        block_engine_url: &str,
        bam_enabled: &Arc<AtomicU8>,
    ) -> crate::proxy::Result<()> {
        let subscribe_packets_stream = timeout(
            *connection_timeout,
            client.subscribe_packets(block_engine::SubscribePacketsRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("block_engine_subscribe_packets".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))
        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?
        .into_inner();

        let subscribe_bundles_stream = timeout(
            *connection_timeout,
            client.subscribe_bundles(block_engine::SubscribeBundlesRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("subscribe_bundles".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))
        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?
        .into_inner();

        let block_builder_info = timeout(
            *connection_timeout,
            client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))
        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?
        .into_inner();
        {
            let block_builder_pubkey =
                Pubkey::from_str(&block_builder_info.pubkey).unwrap_or_else(|_| {
                    datapoint_warn!(
                        "block_engine_stage-block_builder_pubkey_parse_error",
                        ("url", &block_engine_url, String),
                        ("pubkey", &block_builder_info.pubkey, String),
                        ("count", 1, i64),
                    );
                    block_builder_fee_info.load().block_builder
                });
            block_builder_fee_info.store(Arc::new(BlockBuilderFeeInfo {
                block_builder: block_builder_pubkey,
                block_builder_commission: block_builder_info.commission,
            }));
        }

        Self::consume_bundle_and_packet_stream(
            client,
            (subscribe_bundles_stream, subscribe_packets_stream),
            bundle_tx,
            packet_tx,
            local_config,
            global_config,
            banking_packet_sender,
            exit,
            block_builder_fee_info,
            auth_client,
            access_token,
            refresh_token,
            keypair,
            cluster_info,
            connection_timeout,
            block_engine_url,
            bam_enabled,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn consume_bundle_and_packet_stream(
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        (mut bundle_stream, mut packet_stream): (
            Streaming<block_engine::SubscribeBundlesResponse>,
            Streaming<block_engine::SubscribePacketsResponse>,
        ),
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        local_config: &BlockEngineConfig, // local copy of config with current connections
        global_config: &Arc<Mutex<BlockEngineConfig>>, // guarded reference for detecting run-time updates
        banking_packet_sender: &BankingPacketSender,
        exit: &Arc<AtomicBool>,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        mut auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        mut refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        connection_timeout: &Duration,
        block_engine_url: &str,
        bam_enabled: &Arc<AtomicU8>,
    ) -> crate::proxy::Result<()> {
        const METRICS_TICK: Duration = Duration::from_secs(1);
        const MAINTENANCE_TICK: Duration = Duration::from_secs(10 * 60);
        let refresh_within_s: u64 = METRICS_TICK.as_secs().saturating_mul(3).saturating_div(2);

        let mut num_full_refreshes: u64 = 1;
        let mut num_refresh_access_token: u64 = 0;
        let mut block_engine_stats = BlockEngineStageStats::default();
        let mut metrics_and_auth_tick = interval(METRICS_TICK);
        let mut maintenance_tick = interval(MAINTENANCE_TICK);

        info!("connected to packet and bundle stream");

        while !exit.load(Ordering::Relaxed) {
            if BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
                == BamConnectionState::Connected
            {
                info!("bam enabled, exiting block engine stage");
                return Ok(());
            }

            tokio::select! {
                maybe_packet = packet_stream.message() => {
                    let resp = maybe_packet
                        .map_err(ProxyError::GrpcError)?
                        .ok_or(ProxyError::GrpcStreamDisconnected)
                        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;
                    Self::handle_block_engine_packets(resp, packet_tx, banking_packet_sender, local_config.trust_packets, &mut block_engine_stats)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    let resp = maybe_bundles
                        .map_err(ProxyError::GrpcError)?
                        .ok_or(ProxyError::GrpcStreamDisconnected)
                        .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;
                    Self::handle_block_engine_bundles(resp, bundle_tx, &mut block_engine_stats)?;
                }
                _ = metrics_and_auth_tick.tick() => {
                    block_engine_stats.report();
                    block_engine_stats = BlockEngineStageStats::default();

                    if cluster_info.id() != keypair.pubkey() {
                        return Err(ProxyError::AuthenticationConnectionError("validator identity changed".to_string()));
                    }

                    if !global_config.lock().unwrap().eq(local_config) {
                        return Err(ProxyError::BlockEngineConfigChanged);
                    }

                    let (maybe_new_access, maybe_new_refresh) = maybe_refresh_auth_tokens(&mut auth_client,
                        &access_token,
                        &refresh_token,
                        cluster_info,
                        connection_timeout,
                        refresh_within_s,
                    ).await
                    .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?;

                    if let Some(new_token) = maybe_new_access {
                        num_refresh_access_token += 1;
                        datapoint_info!(
                            "block_engine_stage-refresh_access_token",
                            ("url", &block_engine_url, String),
                            ("count", num_refresh_access_token, i64),
                        );

                         *access_token.lock().unwrap() = new_token;
                    }
                    if let Some(new_token) = maybe_new_refresh {
                        num_full_refreshes += 1;
                        datapoint_info!(
                            "block_engine_stage-tokens_generated",
                            ("url", &block_engine_url, String),
                            ("count", num_full_refreshes, i64),
                        );
                        refresh_token = new_token;
                    }
                }
                _ = maintenance_tick.tick() => {
                    let block_builder_info = timeout(
                        *connection_timeout,
                        client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest{})
                    )
                    .await
                    .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
                    .map_err(|e| ProxyError::MethodError(e.to_string()))
                    .map_err(|err| Self::map_bam_enabled(bam_enabled, err))?
                    .into_inner();
                    let block_builder_pubkey =
                        Pubkey::from_str(&block_builder_info.pubkey).unwrap_or_else(|_| {
                            datapoint_warn!(
                                "block_engine_stage-block_builder_pubkey_parse_error",
                                ("url", &block_engine_url, String),
                                ("pubkey", &block_builder_info.pubkey, String),
                                ("count", 1, i64),
                            );
                            block_builder_fee_info.load().block_builder
                        });
                    block_builder_fee_info.store(Arc::new(BlockBuilderFeeInfo {
                        block_builder: block_builder_pubkey,
                        block_builder_commission: block_builder_info.commission,
                    }));
                }
            }
        }

        Ok(())
    }

    fn handle_block_engine_bundles(
        bundles_response: block_engine::SubscribeBundlesResponse,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        let mut bundle_packets = 0u64;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                let packet_batch = PacketBatch::from(
                    bundle
                        .bundle?
                        .packets
                        .into_iter()
                        .map(proto_packet_to_packet)
                        .collect::<Vec<BytesPacket>>(),
                );
                bundle_packets += packet_batch.len() as u64;
                Some(PacketBundle::new(packet_batch, bundle.uuid))
            })
            .collect();
        block_engine_stats
            .num_bundles
            .add_assign(bundles.len() as u64);
        block_engine_stats
            .num_bundle_packets
            .add_assign(bundle_packets);

        // NOTE: bundles are sanitized in bundle_sanitizer module
        bundle_sender
            .send(bundles)
            .map_err(|_| ProxyError::PacketForwardError)
    }

    fn handle_block_engine_packets(
        resp: block_engine::SubscribePacketsResponse,
        packet_tx: &Sender<PacketBatch>,
        banking_packet_sender: &BankingPacketSender,
        trust_packets: bool,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        if let Some(batch) = resp.batch {
            if batch.packets.is_empty() {
                block_engine_stats.num_empty_packets.add_assign(1);
                return Ok(());
            }

            let packet_batch = PacketBatch::from(
                batch
                    .packets
                    .into_iter()
                    .map(proto_packet_to_packet)
                    .collect::<Vec<BytesPacket>>(),
            );

            block_engine_stats
                .num_packets
                .add_assign(packet_batch.len() as u64);

            if trust_packets {
                banking_packet_sender
                    .send(Arc::new(vec![packet_batch]))
                    .map_err(|_| ProxyError::PacketForwardError)?;
            } else {
                packet_tx
                    .send(packet_batch)
                    .map_err(|_| ProxyError::PacketForwardError)?;
            }
        } else {
            block_engine_stats.num_empty_packets.add_assign(1);
        }

        Ok(())
    }

    pub fn is_valid_block_engine_config(config: &BlockEngineConfig) -> bool {
        if config.block_engine_url.is_empty() {
            warn!("can't connect to block_engine. missing block_engine_url.");
            return false;
        }
        if let Err(e) = Endpoint::from_str(&config.block_engine_url) {
            error!("can't connect to block engine. error creating block engine endpoint - {e}");
            return false;
        }
        true
    }
}
