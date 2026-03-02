// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::{v0_to_versioned_proto, BamOutboundMessage},
    jito_protos::proto::{
        bam_api::{
            bam_node_api_client::BamNodeApiClient, scheduler_message_v0::Msg,
            scheduler_response::VersionedMsg, scheduler_response_v0::Resp, AuthChallengeRequest,
            ConfigRequest, ConfigResponse, SchedulerMessage, SchedulerMessageV0, SchedulerResponse,
            SchedulerResponseV0,
        },
        bam_types::{AtomicTxnBatch, AuthProof, Pong, ValidatorHeartBeat},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_signer::Signer,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering::Relaxed},
            Arc, Mutex,
        },
        time::{Duration, Instant, SystemTime},
    },
    thiserror::Error,
    tokio::{
        sync::mpsc,
        time::{interval, timeout},
    },
    tokio_stream::wrappers::ReceiverStream,
};

pub struct BamConnection {
    config: Arc<Mutex<Option<ConfigResponse>>>,
    connection_task: tokio::task::JoinHandle<()>,
    is_healthy: Arc<AtomicBool>,
    url: String,
    exit: Arc<AtomicBool>,
}

const AUTH_LABEL: &[u8] = b"X_OFF_CHAIN_JITO_BAM_V1\0";
const CONNECTION_TIMEOUT: Duration = std::time::Duration::from_secs(5);
const NETWORK_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const OUTBOUND_CHANNEL_CAPACITY: usize = 100_000;
const VALIDATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const METRICS_AND_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(25);
const REFRESH_CONFIG_INTERVAL: Duration = Duration::from_secs(1);
const OUTBOUND_TICK_INTERVAL: Duration = Duration::from_millis(1);
const MAX_WAITING_RESULTS: usize = 24;
const WAIT_SLEEP_DURATION: Duration = Duration::from_millis(10);
const CHILD_TASK_SHUTDOWN_GRACE: Duration = Duration::from_millis(100);
const CONNECTION_TASK_DROP_GRACE: Duration = Duration::from_millis(250);
pub const MAX_DURATION_BETWEEN_NODE_HEARTBEATS: Duration = Duration::from_secs(6); // 3x the nodes heartbeat interval
pub const WAIT_TO_RECONNECT_DURATION: Duration = Duration::from_secs(1);

impl BamConnection {
    /// Try to initialize a connection to the BAM Node; if it is not possible to connect, it will return an error.
    pub async fn try_init(
        url: String,
        cluster_info: Arc<ClusterInfo>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        outbound_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
    ) -> Result<Self, TryInitError> {
        // Create connection and inbound and outbound streams
        let backend_endpoint = tonic::transport::Endpoint::from_shared(url.clone())?
            .connect_timeout(CONNECTION_TIMEOUT)
            .timeout(NETWORK_REQUEST_TIMEOUT);
        let channel = timeout(CONNECTION_TIMEOUT, backend_endpoint.connect()).await??;
        let mut validator_client = BamNodeApiClient::new(channel);
        let (outbound_sender, outbound_receiver_internal) =
            mpsc::channel(OUTBOUND_CHANNEL_CAPACITY);
        let outbound_stream = tonic::Request::new(ReceiverStream::new(outbound_receiver_internal));
        let inbound_stream = timeout(
            NETWORK_REQUEST_TIMEOUT,
            validator_client.init_scheduler_stream(outbound_stream),
        )
        .await?
        .map_err(|e| {
            error!("Failed to start scheduler stream: {e:?}");
            TryInitError::StreamStartError(e)
        })?
        .into_inner();

        // Create data structures for the connection task
        let metrics = Arc::new(BamConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(false));
        let config = Arc::new(Mutex::new(None));
        let exit = Arc::new(AtomicBool::new(false));

        // Start the connection task
        let connection_task = tokio::spawn(Self::connection_task(
            exit.clone(),
            inbound_stream,
            outbound_sender,
            validator_client,
            config.clone(),
            batch_sender,
            cluster_info,
            metrics.clone(),
            is_healthy.clone(),
            outbound_receiver,
        ));

        Ok(Self {
            config,
            connection_task,
            is_healthy,
            url,
            exit,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn connection_task(
        exit: Arc<AtomicBool>,
        mut inbound_stream: tonic::Streaming<SchedulerResponse>,
        outbound_sender: mpsc::Sender<SchedulerMessage>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<BamConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
        outbound_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
    ) {
        let mut last_heartbeat = None;
        let mut heartbeat_interval = interval(VALIDATOR_HEARTBEAT_INTERVAL);
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut metrics_and_health_check_interval = interval(METRICS_AND_HEALTH_CHECK_INTERVAL);
        metrics_and_health_check_interval
            .set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Create auth proof
        let Some(auth_proof) = Self::prepare_auth_proof(&mut validator_client, cluster_info).await
        else {
            error!("Failed to prepare auth response");
            return;
        };

        // Send it as first message
        let start_message = SchedulerMessageV0 {
            msg: Some(Msg::AuthProof(auth_proof)),
        };
        if outbound_sender
            .send(v0_to_versioned_proto(start_message))
            .await
            .inspect_err(|_| {
                error!("Failed to send initial auth proof message");
            })
            .is_err()
        {
            error!("Outbound sender channel closed before sending initial auth proof message");
            return;
        }

        let mut builder_config_task = tokio::spawn(Self::refresh_config_task(
            exit.clone(),
            config.clone(),
            validator_client.clone(),
            metrics.clone(),
        ));

        let mut outbound_task = tokio::spawn(Self::outbound_task(
            exit.clone(),
            outbound_sender.clone(),
            outbound_receiver,
            metrics.clone(),
        ));

        while !exit.load(Relaxed) {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    let _ = outbound_sender.try_send(v0_to_versioned_proto(SchedulerMessageV0 {
                        msg: Some(Msg::HeartBeat(ValidatorHeartBeat {
                            time_sent_microseconds: u64::try_from(SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_micros()).unwrap_or_default(),
                        })),
                    }));
                    metrics.heartbeat_sent.fetch_add(1, Relaxed);
                }
                _ = metrics_and_health_check_interval.tick() => {
                    let is_healthy_now = last_heartbeat.is_some_and(|t: Instant| t.elapsed() < MAX_DURATION_BETWEEN_NODE_HEARTBEATS);
                    is_healthy.store(is_healthy_now, Relaxed);
                    if !is_healthy_now {
                        metrics
                            .unhealthy_connection_count
                            .fetch_add(1, Relaxed);
                    }

                    metrics.report();
                }
                inbound = inbound_stream.message() => {
                    let inbound = match inbound {
                        Ok(Some(msg)) => msg,
                        Ok(None) => {
                            error!("Inbound stream closed");
                            break;
                        }
                        Err(e) => {
                            error!("Failed to receive message from inbound stream: {e:?}");
                            break;
                        }
                    };

                    let Some(VersionedMsg::V0(inbound)) = inbound.versioned_msg else {
                        error!("Received unsupported versioned message: {inbound:?}");
                        break;
                    };

                    match inbound {
                        SchedulerResponseV0 { resp: Some(Resp::HeartBeat(_)), .. } => {
                            last_heartbeat = Some(std::time::Instant::now());
                            metrics.heartbeat_received.fetch_add(1, Relaxed);
                        }
                        SchedulerResponseV0 { resp: Some(Resp::MultipleAtomicTxnBatch(batches)), .. } => {
                            for batch in batches.batches {
                                metrics.bundle_received.fetch_add(1, Relaxed);
                                let _ = batch_sender.try_send(batch).inspect_err(|_| {
                                    metrics.bundle_forward_to_scheduler_fail.fetch_add(1, Relaxed);
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
        is_healthy.store(false, Relaxed);
        tokio::join!(
            Self::wait_or_abort_child("refresh_config", &mut builder_config_task),
            Self::wait_or_abort_child("outbound", &mut outbound_task),
        );
    }

    async fn wait_or_abort_child(task_name: &str, task: &mut tokio::task::JoinHandle<()>) {
        match timeout(CHILD_TASK_SHUTDOWN_GRACE, &mut *task).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                if !e.is_cancelled() {
                    warn!("BAM task {task_name} error: {e:?}");
                }
            }
            Err(_) => {
                warn!(
                    "BAM task {task_name} did not exit within {CHILD_TASK_SHUTDOWN_GRACE:?}; \
                     aborting"
                );
                task.abort();
                let _ = task.await;
            }
        }
    }

    fn send_batch_results(
        outbound_sender: &mut mpsc::Sender<SchedulerMessage>,
        results: Vec<jito_protos::proto::bam_types::AtomicTxnBatchResult>,
        metrics: &BamConnectionMetrics,
    ) {
        if !results.is_empty() {
            let outbound = SchedulerMessageV0 {
                msg: Some(Msg::MultipleAtomicTxnBatchResult(
                    jito_protos::proto::bam_types::MultipleAtomicTxnBatchResult { results },
                )),
            };
            if outbound_sender
                .try_send(v0_to_versioned_proto(outbound))
                .is_err()
            {
                metrics.outbound_fail.fetch_add(1, Relaxed);
            } else {
                metrics.outbound_sent.fetch_add(1, Relaxed);
            }
        }
    }

    async fn refresh_config_task(
        exit: Arc<AtomicBool>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        metrics: Arc<BamConnectionMetrics>,
    ) {
        let mut interval = interval(REFRESH_CONFIG_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        while !exit.load(Relaxed) {
            tokio::select! {
                _ = interval.tick() => {
                    let request = tonic::Request::new(ConfigRequest {});
                    match timeout(
                        NETWORK_REQUEST_TIMEOUT,
                        validator_client.get_builder_config(request),
                    )
                    .await
                    {
                        Ok(Ok(response)) => {
                            let resp_config = response.into_inner();
                            *config.lock().unwrap() = Some(resp_config);
                            metrics.builder_config_received.fetch_add(1, Relaxed);
                        }
                        Ok(Err(e)) => {
                            error!("Failed to get config: {e:?}");
                        }
                        Err(_) => error!("Timed out getting config"),
                    }
                }
            }
        }
    }

    async fn outbound_task(
        exit: Arc<AtomicBool>,
        mut outbound_sender: mpsc::Sender<SchedulerMessage>,
        outbound_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
        metrics: Arc<BamConnectionMetrics>,
    ) {
        let mut outbound_tick_interval = interval(OUTBOUND_TICK_INTERVAL);
        outbound_tick_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Burst);

        let mut waiting_results = Vec::new();

        while !exit.load(Relaxed) {
            tokio::select! {
                _ = outbound_tick_interval.tick() => {
                    while let Ok(outbound) = outbound_receiver.try_recv() {
                        match outbound {
                            BamOutboundMessage::LeaderState(leader_state) => {
                                metrics.leaderstate_sent.fetch_add(1, Relaxed);
                                let outbound = SchedulerMessageV0 {
                                    msg: Some(Msg::LeaderState(leader_state)),
                                };
                                if outbound_sender.try_send(v0_to_versioned_proto(outbound)).is_err() {
                                    metrics.outbound_fail.fetch_add(1, Relaxed);
                                } else {
                                    metrics.outbound_sent.fetch_add(1, Relaxed);
                                }
                            }
                            BamOutboundMessage::AtomicTxnBatchResult(result) => {
                                metrics.bundleresult_sent.fetch_add(1, Relaxed);
                                waiting_results.push(result);
                                if waiting_results.len() >= MAX_WAITING_RESULTS {
                                    Self::send_batch_results(&mut outbound_sender, std::mem::take(&mut waiting_results), metrics.as_ref());
                                }
                            }
                            BamOutboundMessage::Ping(id) => {
                                metrics.ping_received.fetch_add(1, Relaxed);
                                metrics.last_ping_id_received.store(id, Relaxed);
                                let pong_message_outbound = SchedulerMessageV0 {
                                    msg: Some(Msg::Pong(Pong { id })),
                                };
                                if outbound_sender.try_send(v0_to_versioned_proto(pong_message_outbound)).is_err() {
                                    metrics.outbound_pong_send_fail.fetch_add(1, Relaxed);
                                } else {
                                    metrics.outbound_pong_sent.fetch_add(1, Relaxed);
                                    metrics.last_ping_id_sent.store(id, Relaxed);
                                }
                            }
                            _ => {}
                        }
                    }
                    Self::send_batch_results(&mut outbound_sender, std::mem::take(&mut waiting_results), metrics.as_ref());
                }
            }
        }
    }

    fn sign_message(keypair: &Keypair, message: &[u8]) -> Option<String> {
        let slot_signature = keypair.try_sign_message(message).ok()?;
        let slot_signature = slot_signature.to_string();
        Some(slot_signature)
    }

    pub fn is_healthy(&self) -> bool {
        self.is_healthy.load(Relaxed)
    }

    pub fn wait_until_healthy_and_config_received(
        &self,
        duration: std::time::Duration,
        exit: &AtomicBool,
    ) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < duration && !exit.load(Relaxed) {
            if self.is_healthy() && self.get_latest_config().is_some() {
                return true;
            }
            std::thread::sleep(WAIT_SLEEP_DURATION);
        }
        false
    }

    pub fn get_latest_config(&self) -> Option<ConfigResponse> {
        if !self.is_healthy() {
            return None;
        }
        self.config.lock().unwrap().clone()
    }

    pub fn url(&self) -> &str {
        &self.url
    }

    /// Bytes that must be signed/verified
    pub fn labeled_bytes(challenge: &[u8]) -> Vec<u8> {
        let mut v = Vec::with_capacity(AUTH_LABEL.len() + challenge.len());
        v.extend_from_slice(AUTH_LABEL);
        v.extend_from_slice(challenge);
        v
    }

    async fn prepare_auth_proof(
        validator_client: &mut BamNodeApiClient<tonic::transport::channel::Channel>,
        cluster_info: Arc<ClusterInfo>,
    ) -> Option<AuthProof> {
        let request = tonic::Request::new(AuthChallengeRequest {});
        let resp = match timeout(
            NETWORK_REQUEST_TIMEOUT,
            validator_client.get_auth_challenge(request),
        )
        .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                error!("Failed to get auth challenge: {e:?}");
                return None;
            }
            Err(_) => {
                error!("Timed out getting auth challenge");
                return None;
            }
        };

        let resp = resp.into_inner();
        let challenge_to_sign = resp.challenge_to_sign;
        let challenge_bytes = challenge_to_sign.as_bytes();
        let to_sign = Self::labeled_bytes(challenge_bytes);

        let signature = Self::sign_message(cluster_info.keypair().as_ref(), &to_sign)?;

        Some(AuthProof {
            challenge_to_sign,
            validator_pubkey: cluster_info.keypair().pubkey().to_string(),
            signature,
        })
    }
}

impl Drop for BamConnection {
    fn drop(&mut self) {
        self.is_healthy.store(false, Relaxed);
        self.exit.store(true, Relaxed);
        std::thread::sleep(CONNECTION_TASK_DROP_GRACE);
        if !self.connection_task.is_finished() {
            self.connection_task.abort();
        }
    }
}

#[derive(Default)]
struct BamConnectionMetrics {
    bundle_received: AtomicU64,
    bundle_forward_to_scheduler_fail: AtomicU64,
    heartbeat_received: AtomicU64,
    builder_config_received: AtomicU64,

    unhealthy_connection_count: AtomicU64,

    leaderstate_sent: AtomicU64,
    bundleresult_sent: AtomicU64,
    heartbeat_sent: AtomicU64,
    outbound_sent: AtomicU64,
    outbound_fail: AtomicU64,

    ping_received: AtomicU64,
    last_ping_id_received: AtomicU32,
    outbound_pong_sent: AtomicU64,
    last_ping_id_sent: AtomicU32,
    outbound_pong_send_fail: AtomicU64,
}

impl BamConnectionMetrics {
    fn has_data(&self) -> bool {
        self.bundle_received.load(Relaxed) > 0
            || self.bundle_forward_to_scheduler_fail.load(Relaxed) > 0
            || self.heartbeat_received.load(Relaxed) > 0
            || self.builder_config_received.load(Relaxed) > 0
            || self.unhealthy_connection_count.load(Relaxed) > 0
            || self.leaderstate_sent.load(Relaxed) > 0
            || self.bundleresult_sent.load(Relaxed) > 0
            || self.heartbeat_sent.load(Relaxed) > 0
            || self.outbound_sent.load(Relaxed) > 0
            || self.outbound_fail.load(Relaxed) > 0
            || self.ping_received.load(Relaxed) > 0
            || self.outbound_pong_send_fail.load(Relaxed) > 0
            || self.outbound_pong_sent.load(Relaxed) > 0
    }

    pub fn report(&self) {
        if !self.has_data() {
            return;
        }
        datapoint_info!(
            "bam_connection-metrics",
            (
                "bundle_received",
                self.bundle_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "bundle_forward_to_scheduler_fail",
                self.bundle_forward_to_scheduler_fail.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "heartbeat_received",
                self.heartbeat_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "builder_config_received",
                self.builder_config_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "unhealthy_connection_count",
                self.unhealthy_connection_count.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "leaderstate_sent",
                self.leaderstate_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "bundleresult_sent",
                self.bundleresult_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "heartbeat_sent",
                self.heartbeat_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "outbound_sent",
                self.outbound_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "outbound_fail",
                self.outbound_fail.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "ping_received",
                self.ping_received.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "last_ping_id_received",
                self.last_ping_id_received.load(Relaxed),
                i64
            ),
            (
                "outbound_pong_sent_count",
                self.outbound_pong_sent.swap(0, Relaxed) as i64,
                i64
            ),
            (
                "last_ping_id_sent",
                self.last_ping_id_sent.load(Relaxed),
                i64
            ),
            (
                "outbound_pong_send_fail_count",
                self.outbound_pong_send_fail.swap(0, Relaxed) as i64,
                i64
            )
        );
    }
}

#[derive(Error, Debug)]
pub enum TryInitError {
    #[error("Currently in leader slot")]
    MidLeaderSlotError,
    #[error("Failed to connect to endpoint: {0}")]
    EndpointConnectError(#[from] tonic::transport::Error),
    #[error("Connection attempt timed out: {0}")]
    ConnectionTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Failed to start stream: {0}")]
    StreamStartError(#[from] tonic::Status),
}
