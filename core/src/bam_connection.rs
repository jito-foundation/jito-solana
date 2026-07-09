// Maintains a connection to the BAM Node and handles sending and receiving messages
// Keeps track of last received heartbeat 'behind the scenes' and will mark itself as unhealthy if no heartbeat is received

use {
    crate::bam_dependencies::{BamOutboundMessage, v0_to_versioned_proto},
    jito_protos::proto::{
        bam_api::{
            AuthChallengeRequest, ConfigRequest, ConfigResponse, SchedulerMessage,
            SchedulerMessageV0, SchedulerResponse, SchedulerResponseV0,
            bam_node_api_client::BamNodeApiClient, scheduler_message_v0::Msg,
            scheduler_response::VersionedMsg, scheduler_response_v0::Resp,
        },
        bam_types::{
            AtomicTxnBatch, AuthProof, MultipleAtomicTxnBatchResult, Pong, ValidatorHeartBeat,
        },
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_signer::Signer,
    std::{
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
        },
        time::{Duration, Instant, SystemTime},
    },
    thiserror::Error,
    tokio::{
        sync::{mpsc, oneshot},
        time::{interval, timeout},
    },
    tokio_stream::wrappers::ReceiverStream,
    tonic::transport::{ClientTlsConfig, Endpoint},
};

pub struct BamConnection {
    config: Arc<Mutex<Option<ConfigResponse>>>,
    connection_task: tokio::task::JoinHandle<mpsc::Receiver<BamOutboundMessage>>,
    is_healthy: Arc<AtomicBool>,
    url: String,
    connection_exit: Arc<AtomicBool>,
}

const AUTH_LABEL: &[u8] = b"X_OFF_CHAIN_JITO_BAM_V1\0";
const CONNECTION_TIMEOUT: Duration = std::time::Duration::from_secs(5);
const NETWORK_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
const OUTBOUND_CHANNEL_CAPACITY: usize = 100_000;
const MAX_OUTBOUND_RESULT_BATCH_SIZE: usize = 24;
const VALIDATOR_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const METRICS_AND_HEALTH_CHECK_INTERVAL: Duration = Duration::from_millis(25);
const REFRESH_CONFIG_INTERVAL: Duration = Duration::from_secs(1);
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
        outbound_receiver: &mut Option<mpsc::Receiver<BamOutboundMessage>>,
    ) -> Result<Self, TryInitError> {
        // Create connection and inbound and outbound streams
        let backend_endpoint = Self::endpoint_from_url(&url)?
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

        let outbound_receiver = outbound_receiver
            .take()
            .expect("BAM outbound receiver already in use");

        // Create data structures for the connection task
        let metrics = Arc::new(BamConnectionMetrics::default());
        let is_healthy = Arc::new(AtomicBool::new(false));
        let config = Arc::new(Mutex::new(None));
        let connection_exit = Arc::new(AtomicBool::new(false));

        // Start the connection task
        let connection_task = tokio::spawn(Self::connection_task(
            connection_exit.clone(),
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
            connection_exit,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn connection_task(
        connection_exit: Arc<AtomicBool>,
        mut inbound_stream: tonic::Streaming<SchedulerResponse>,
        outbound_sender: mpsc::Sender<SchedulerMessage>,
        mut validator_client: BamNodeApiClient<tonic::transport::channel::Channel>,
        config: Arc<Mutex<Option<ConfigResponse>>>,
        batch_sender: crossbeam_channel::Sender<AtomicTxnBatch>,
        cluster_info: Arc<ClusterInfo>,
        metrics: Arc<BamConnectionMetrics>,
        is_healthy: Arc<AtomicBool>,
        mut outbound_receiver: mpsc::Receiver<BamOutboundMessage>,
    ) -> mpsc::Receiver<BamOutboundMessage> {
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
            return outbound_receiver;
        };

        // Send it as first message
        let start_message = SchedulerMessageV0 {
            msg: Some(Msg::AuthProof(auth_proof)),
        };
        if outbound_sender
            .send(v0_to_versioned_proto(start_message))
            .await
            .is_err()
        {
            error!("Outbound sender channel closed before sending initial auth proof message");
            return outbound_receiver;
        }

        let mut post_auth_client = Some(validator_client);
        let mut refresh_config_exit_sender: Option<oneshot::Sender<()>> = None;
        let mut refresh_config_task = None;

        while !connection_exit.load(Relaxed) {
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

                    // The first successful inbound scheduler message proves the auth'd stream was accepted.
                    if let Some(mut validator_client) = post_auth_client.take() {
                        let config = config.clone();
                        let metrics = metrics.clone();
                        let (exit_sender, mut exit_receiver) = oneshot::channel();
                        refresh_config_exit_sender = Some(exit_sender);
                        refresh_config_task = Some(tokio::spawn(async move {
                            let mut interval = interval(REFRESH_CONFIG_INTERVAL);
                            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                            loop {
                                let request = tonic::Request::new(ConfigRequest {});
                                let result = tokio::select! {
                                    biased;
                                    result = async {
                                        interval.tick().await;
                                        timeout(
                                            NETWORK_REQUEST_TIMEOUT,
                                            validator_client.get_builder_config(request),
                                        )
                                        .await
                                    } => result,
                                    _ = &mut exit_receiver => break,
                                };
                                match result {
                                    Ok(Ok(response)) => {
                                        *config.lock().unwrap() = Some(response.into_inner());
                                        metrics.builder_config_received.fetch_add(1, Relaxed);
                                    }
                                    Ok(Err(e)) => error!("Failed to get config: {e:?}"),
                                    Err(_) => error!("Timed out getting config"),
                                }
                            }
                        }));
                    }

                    match inbound {
                        SchedulerResponseV0 { resp: Some(Resp::HeartBeat(_)), .. } => {
                            last_heartbeat = Some(Instant::now());
                            metrics.heartbeat_received.fetch_add(1, Relaxed);
                        }
                        SchedulerResponseV0 { resp: Some(Resp::MultipleAtomicTxnBatch(batches)), .. } => {
                            for batch in batches.batches {
                                metrics.bundle_received.fetch_add(1, Relaxed);
                                if batch_sender.try_send(batch).is_err() {
                                    metrics.bundle_forward_to_scheduler_fail.fetch_add(1, Relaxed);
                                }
                            }
                        }
                        SchedulerResponseV0 { resp: Some(Resp::Ping(ping)), .. } => {
                            let outbound = SchedulerMessageV0 {
                                msg: Some(Msg::Pong(Pong { id: ping.id })),
                            };
                            if outbound_sender.try_send(v0_to_versioned_proto(outbound)).is_err() {
                                metrics.outbound_fail.fetch_add(1, Relaxed);
                            } else {
                                metrics.outbound_sent.fetch_add(1, Relaxed);
                            }
                        }
                        _ => {}
                    }
                }
                outbound = outbound_receiver.recv(), if post_auth_client.is_none() => {
                    let Some(outbound) = outbound else {
                        error!("BAM outbound channel closed");
                        break;
                    };
                    let leader_state_to_send = match outbound {
                        BamOutboundMessage::LeaderState(leader_state) => Some(leader_state),
                        BamOutboundMessage::AtomicTxnBatchResult(result) => {
                            let mut results = Vec::with_capacity(MAX_OUTBOUND_RESULT_BATCH_SIZE);
                            results.push(result);
                            let mut leader_state_to_send = None;

                            while results.len() < MAX_OUTBOUND_RESULT_BATCH_SIZE {
                                match outbound_receiver.try_recv() {
                                    Ok(BamOutboundMessage::AtomicTxnBatchResult(result)) => {
                                        results.push(result);
                                    }
                                    Ok(BamOutboundMessage::LeaderState(leader_state)) => {
                                        leader_state_to_send = Some(leader_state);
                                        break;
                                    }
                                    Err(_) => break,
                                }
                            }

                            metrics.bundleresult_sent.fetch_add(results.len() as u64, Relaxed);
                            let outbound = SchedulerMessageV0 {
                                msg: Some(Msg::MultipleAtomicTxnBatchResult(
                                    MultipleAtomicTxnBatchResult { results },
                                )),
                            };
                            if outbound_sender.try_send(v0_to_versioned_proto(outbound)).is_err() {
                                metrics.outbound_fail.fetch_add(1, Relaxed);
                            } else {
                                metrics.outbound_sent.fetch_add(1, Relaxed);
                            }
                            leader_state_to_send
                        }
                    };

                    if let Some(leader_state) = leader_state_to_send {
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

                }
            }
        }
        connection_exit.store(true, Relaxed);
        is_healthy.store(false, Relaxed);

        drop(refresh_config_exit_sender.take());

        if let Some(refresh_config_task) = refresh_config_task.as_mut() {
            match timeout(CHILD_TASK_SHUTDOWN_GRACE, &mut *refresh_config_task).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if !e.is_cancelled() {
                        warn!("BAM task refresh_config error: {e:?}");
                    }
                }
                Err(_) => {
                    warn!(
                        "BAM task refresh_config did not exit within \
                         {CHILD_TASK_SHUTDOWN_GRACE:?}; aborting"
                    );
                    refresh_config_task.abort();
                    let _ = refresh_config_task.await;
                }
            }
        }

        outbound_receiver
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
            if self.connection_task.is_finished() {
                return false;
            }
            if self.get_latest_config().is_some() {
                return true;
            }
            std::thread::sleep(WAIT_SLEEP_DURATION);
        }
        false
    }

    pub async fn shutdown(mut self) -> mpsc::Receiver<BamOutboundMessage> {
        self.is_healthy.store(false, Relaxed);
        self.connection_exit.store(true, Relaxed);

        let shutdown_start = Instant::now();
        let outbound_receiver = (&mut self.connection_task)
            .await
            .expect("BAM connection task should return outbound receiver");
        let shutdown_duration = shutdown_start.elapsed();
        if shutdown_duration > CONNECTION_TASK_DROP_GRACE {
            info!("BAM connection task shutdown took {shutdown_duration:?}");
        }
        outbound_receiver
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

        let signature = cluster_info
            .keypair()
            .try_sign_message(&to_sign)
            .ok()?
            .to_string();

        Some(AuthProof {
            challenge_to_sign,
            validator_pubkey: cluster_info.keypair().pubkey().to_string(),
            signature,
        })
    }

    fn endpoint_from_url(url: &str) -> Result<Endpoint, TryInitError> {
        let mut endpoint =
            Endpoint::from_shared(url.to_owned())?.tcp_keepalive(Some(Duration::from_secs(60)));
        if url.starts_with("https") {
            endpoint = endpoint.tls_config(ClientTlsConfig::new().with_enabled_roots())?;
        }
        Ok(endpoint)
    }
}

impl Drop for BamConnection {
    fn drop(&mut self) {
        self.is_healthy.store(false, Relaxed);
        self.connection_exit.store(true, Relaxed);
        if self.connection_task.is_finished() {
            return;
        }
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
    }

    fn report(&self) {
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
            )
        );
    }
}

#[derive(Error, Debug)]
pub enum TryInitError {
    #[error("Failed to connect to endpoint: {0}")]
    EndpointConnectError(#[from] tonic::transport::Error),
    #[error("Connection attempt timed out: {0}")]
    ConnectionTimeout(#[from] tokio::time::error::Elapsed),
    #[error("Failed to start stream: {0}")]
    StreamStartError(#[from] tonic::Status),
}
