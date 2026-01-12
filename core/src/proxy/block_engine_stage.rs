//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.

use {
    crate::{
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
            atomic::{AtomicBool, Ordering},
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
        transport::{Channel, Endpoint, Uri},
        Status, Streaming,
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
enum PingError<'a> {
    #[error("Failed to send ping: {0}")]
    CommandFailure(#[from] std::io::Error),

    #[error("Ping command exited with non-zero status: {1:?} for host: {0}")]
    NonZeroExit(&'a str, Option<i32>),

    #[error("No valid RTT found in ping output")]
    NoRttFound,

    #[error("Failed to parse RTT: {0}")]
    ParseFloatError(#[from] std::num::ParseFloatError),
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicBool>,
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
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: Arc<AtomicBool>,
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        local_block_engine_config: &BlockEngineConfig,
        bam_enabled: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        if bam_enabled.load(Ordering::Relaxed) {
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
            .await;
        }

        if let Some((_best_url, (best_socket, _best_latency_us))) =
            Self::get_ranked_endpoints(&endpoint)
                .await?
                .into_iter()
                .min_by_key(|(_url, (_socket, latency_us))| *latency_us)
        {
            if best_socket.is_some() {
                // no else branch needed since we'll still send to shred_receiver_address
                shredstream_receiver_address.store(Arc::new(best_socket));
            }
        }

        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        datapoint_info!(
            "block_engine_stage-connect",
            "type" => "direct",
            ("count", 1, i64),
        );
        Self::connect_auth_and_stream(
            &endpoint,
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        shredstream_receiver_address: &Arc<ArcSwap<Option<SocketAddr>>>,
        bam_enabled: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        let candidates = Self::get_ranked_endpoints(&endpoint).await?;

        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        // try connecting to best block engine
        let mut attempted = false;
        let mut backend_endpoint = endpoint.clone();
        let endpoint_count = candidates.len();
        for (block_engine_url, (maybe_shredstream_socket, latency_us)) in candidates
            .into_iter()
            .sorted_unstable_by_key(|(_endpoint, (_shredstream_socket, latency_us))| *latency_us)
        {
            if bam_enabled.load(Ordering::Relaxed) {
                return Ok(());
            }
            if block_engine_url != local_block_engine_config.block_engine_url {
                info!(
                    "Selected best Block Engine url: {block_engine_url}, Shredstream socket: \
                     {maybe_shredstream_socket:?}, ping: ({:?})",
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
                        other => {
                            datapoint_warn!(
                                "block_engine_stage-autoconfig_error",
                                ("url", block_engine_url, String),
                                ("count", 1, i64),
                                ("error", other.to_string(), String),
                            );
                        }
                    }

                    if connect_start.elapsed() > Self::CONNECTION_TIMEOUT * 3 {
                        return Err(e); // run a new round of pings and connect to new best
                    }
                    // Otherwise, try next endpoint without delay; caller handles backoff on overall failure
                }
            }
        }
        if !attempted {
            return Err(ProxyError::BlockEngineEndpointError(
                "autoconfig failed: no endpoints available after ping ranking".to_string(),
            ));
        }
        Err(ProxyError::BlockEngineEndpointError(format!(
            "autoconfig failed: all {endpoint_count} candidate endpoints failed to connect",
        )))
    }

    /// Discover candidate endpoints either ranked via ping or using global fallback.
    /// Use u64::MAX for latency value to indicate global fallback (no ping data).
    async fn get_ranked_endpoints(
        backend_endpoint: &Endpoint,
    ) -> crate::proxy::Result<
        ahash::HashMap<
            String, /* block engine url */
            (
                Option<SocketAddr>, /* shredstream receiver, fallable when DNS can't resolve */
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
        let endpoint_latencies = Self::ping_and_rank_endpoints(&endpoints.regioned_endpoints).await;
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        connection_timeout: &Duration,
        bam_enabled: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        // Get a copy of configs here in case they have changed at runtime
        let keypair = cluster_info.keypair().clone();

        debug!("connecting to auth: {}", backend_endpoint.uri());
        let auth_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::AuthenticationConnectionTimeout)?
            .map_err(|e| ProxyError::AuthenticationConnectionError(e.to_string()))?;

        let mut auth_client = AuthServiceClient::new(auth_channel);

        debug!("generating authentication token");
        let (access_token, refresh_token) = timeout(
            *connection_timeout,
            generate_auth_tokens(&mut auth_client, &keypair),
        )
        .await
        .map_err(|_| ProxyError::AuthenticationTimeout)??;

        let backend_url = backend_endpoint.uri().to_string();
        datapoint_info!(
            "block_engine_stage-tokens_generated",
            ("url", backend_url, String),
            ("count", 1, i64),
        );

        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        debug!("connecting to block engine: {}", backend_endpoint.uri());
        let block_engine_channel = timeout(*connection_timeout, backend_endpoint.connect())
            .await
            .map_err(|_| ProxyError::BlockEngineConnectionTimeout)?
            .map_err(ProxyError::BlockEngineConnectionError)?;

        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

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

    /// Runs a single `ping -c 1 <ip>` command and returns the RTT in microseconds, or an error.
    async fn ping(host: &str) -> Result<u64, PingError> {
        let output = tokio::process::Command::new("ping")
            .arg("-c")
            .arg("1") // ping once
            .arg("-w")
            .arg("2") // don't wait more than 2 secs for a response
            .arg(host)
            .output()
            .await?; // can produce std::io::Error -> PingError::CommandFailure

        if !output.status.success() {
            warn!(
                "Ping error to host: {host}. Stdout: {}, Stderr:{}, return code: {:?}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
                output.status.code()
            );
            return Err(PingError::NonZeroExit(host, output.status.code()));
        }

        // Example line to parse: `64 bytes from 8.8.8.8: icmp_seq=1 ttl=57 time=12.3 ms`
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            let Some(rtt_str) = line
                .find("time=")
                .map(|index| &line[index + "time=".len()..])
                .and_then(|rtt_str| rtt_str.find(" ms").map(|index| &rtt_str[..index]))
            else {
                continue;
            };

            let rtt = rtt_str.parse::<f64>()?; // might return ParseFloatError
            return Ok((rtt * 1000.0).round() as u64);
        }

        Err(PingError::NoRttFound)
    }

    /// Ping all candidate endpoints concurrently, aggregate best RTT per endpoint
    async fn ping_and_rank_endpoints(
        endpoints: &[BlockEngineEndpoint],
    ) -> ahash::HashMap<
        String, /* block engine url */
        (
            Option<SocketAddr>, /* shredstream receiver, fallable when DNS can't resolve */
            u64,                /* latency us */
        ),
    > {
        const PING_COUNT: usize = 3;

        let endpoints_to_ping = endpoints
            .iter()
            .flat_map(|endpoint| std::iter::repeat_n(endpoint, PING_COUNT)) // send multiple pings to each destination to get the best time
            .filter_map(|endpoint| {
                let uri = endpoint
                    .block_engine_url
                    .parse::<Uri>()
                    .inspect_err(|e| {
                        warn!(
                            "Failed to parse URI: {}, Error: {e}",
                            endpoint.block_engine_url
                        )
                    })
                    .ok()?;
                let _ = uri.host()?;
                Some((endpoint, uri))
            })
            .collect_vec();
        let ping_res = futures::future::join_all(
            endpoints_to_ping
                .iter()
                .map(|(_endpoint, uri)| Self::ping(uri.host().unwrap())), // unwrap checked in filter_map above
        )
        .await;

        let mut agg_endpoints: ahash::HashMap<
            String, /* block engine url */
            (
                Option<SocketAddr>, /* shredstream receiver, fallable when DNS can't resolve */
                u64,                /* latency us */
            ),
        > = ahash::HashMap::with_capacity(endpoints.len());
        let mut best_endpoint = (None, u64::MAX);
        ping_res.iter().zip(endpoints_to_ping.iter()).for_each(
            |(maybe_ping_res, (endpoint, _uri))| {
                let Ok(latency_us) = maybe_ping_res else {
                    return;
                };
                if *latency_us <= best_endpoint.1 {
                    best_endpoint = (Some(endpoint), *latency_us);
                };
                datapoint_info!(
                    "block_engine_stage-autoconfig_ping",
                    ("endpoint", endpoint.block_engine_url, String),
                    ("latency_us", *latency_us, i64),
                );
                match agg_endpoints.entry(endpoint.block_engine_url.clone()) {
                    Entry::Occupied(mut ent) => {
                        let (_shredstream_socket, best_ping_us) = ent.get_mut();
                        if latency_us <= best_ping_us {
                            *best_ping_us = *latency_us;
                        }
                    }
                    Entry::Vacant(entry) => {
                        let maybe_shredstream_socket = endpoint
                            .shredstream_receiver_address
                            .to_socket_addrs()
                            .inspect_err(|e| {
                                warn!(
                                    "Failed to resolve shredstream address {}, error: {e}",
                                    endpoint.shredstream_receiver_address
                                )
                            })
                            .ok()
                            .and_then(|mut shredstream_sockets| shredstream_sockets.next());
                        entry.insert((maybe_shredstream_socket, *latency_us));
                    }
                };
            },
        );

        datapoint_info!(
            "block_engine_stage-autoconfig",
            ("endpoints_count", agg_endpoints.len(), i64),
            (
                "best_endpoint_url",
                best_endpoint
                    .0
                    .map(|x| x.block_engine_url.as_str())
                    .unwrap_or_default(),
                String
            ),
            ("best_endpoint_latency_us", best_endpoint.1, i64),
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        refresh_token: Token,
        connection_timeout: &Duration,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        block_engine_url: &str,
        bam_enabled: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        if bam_enabled.load(Ordering::Relaxed) {
            return Ok(());
        }

        let subscribe_packets_stream = timeout(
            *connection_timeout,
            client.subscribe_packets(block_engine::SubscribePacketsRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("block_engine_subscribe_packets".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        let subscribe_bundles_stream = timeout(
            *connection_timeout,
            client.subscribe_bundles(block_engine::SubscribeBundlesRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("subscribe_bundles".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        let block_builder_info = timeout(
            *connection_timeout,
            client.get_block_builder_fee_info(BlockBuilderFeeInfoRequest {}),
        )
        .await
        .map_err(|_| ProxyError::MethodTimeout("get_block_builder_fee_info".to_string()))?
        .map_err(|e| ProxyError::MethodError(e.to_string()))?
        .into_inner();

        {
            let block_builder_fee_info = block_builder_fee_info.clone();
            task::spawn_blocking(move || {
                let mut bb_fee = block_builder_fee_info.lock().unwrap();
                bb_fee.block_builder_commission = block_builder_info.commission;
                if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                    bb_fee.block_builder = pk
                }
            })
            .await
            .unwrap();
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
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        mut auth_client: AuthServiceClient<Channel>,
        access_token: Arc<Mutex<Token>>,
        mut refresh_token: Token,
        keypair: Arc<Keypair>,
        cluster_info: &Arc<ClusterInfo>,
        connection_timeout: &Duration,
        block_engine_url: &str,
        bam_enabled: &Arc<AtomicBool>,
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
            if bam_enabled.load(Ordering::Relaxed) {
                info!("bam enabled, exiting block engine stage");
                return Ok(());
            }

            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_block_engine_packets(resp, packet_tx, banking_packet_sender, local_config.trust_packets, &mut block_engine_stats)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_tx, &mut block_engine_stats)?;
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
                    ).await?;

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
                    .map_err(|e| ProxyError::MethodError(e.to_string()))?
                    .into_inner();

                    let block_builder_fee_info = block_builder_fee_info.clone();
                    task::spawn_blocking(move || {
                        let mut bb_fee = block_builder_fee_info.lock().unwrap();
                        bb_fee.block_builder_commission = block_builder_info.commission;
                        if let Ok(pk) = Pubkey::from_str(&block_builder_info.pubkey) {
                            bb_fee.block_builder = pk
                        }
                    })
                    .await
                    .unwrap();
                }
            }
        }

        Ok(())
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: Result<Option<block_engine::SubscribeBundlesResponse>, Status>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
        block_engine_stats: &mut BlockEngineStageStats,
    ) -> crate::proxy::Result<()> {
        let bundles_response = maybe_bundles_response?.ok_or(ProxyError::GrpcStreamDisconnected)?;
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle::new(
                    PacketBatch::from(
                        bundle
                            .bundle?
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect::<Vec<BytesPacket>>(),
                    ),
                    bundle.uuid,
                ))
            })
            .collect();
        block_engine_stats
            .num_bundles
            .add_assign(bundles.len() as u64);
        block_engine_stats.num_bundle_packets.add_assign(
            bundles
                .iter()
                .map(|bundle| bundle.batch().len() as u64)
                .sum::<u64>(),
        );

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
