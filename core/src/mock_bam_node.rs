use {
    crossbeam_channel::unbounded,
    jito_protos::proto::{
        bam_api::{
            bam_node_api_server::{BamNodeApi, BamNodeApiServer},
            scheduler_message::VersionedMsg,
            scheduler_message_v0::Msg,
            scheduler_response::VersionedMsg as ResponseVersionedMsg,
            scheduler_response_v0::Resp,
            AuthChallengeRequest, AuthChallengeResponse, ConfigRequest, ConfigResponse,
            SchedulerMessage, SchedulerResponse, SchedulerResponseV0,
        },
        bam_types::{
            AtomicTxnBatch, BamConfig, BlockEngineBuilderConfig, BuilderHeartBeat,
            MultipleAtomicTxnBatch, Packet, Socket,
        },
    },
    solana_keypair::Keypair,
    solana_net_utils::sockets::bind_to,
    solana_perf::packet::PacketBatch,
    solana_streamer::{
        nonblocking::simple_qos::SimpleQosConfig,
        quic::{spawn_simple_qos_server, QuicStreamerConfig},
        streamer::StakedNodes,
    },
    std::{
        collections::VecDeque,
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, SystemTime},
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tokio_util::sync::CancellationToken,
    tonic::{Request, Response, Status, Streaming},
};

const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);
const GRPC_CHANNEL_SIZE: usize = 1000;
const DEFAULT_QUEUE_SIZE: usize = 10_000;

pub struct MockBamNodeConfig {
    pub grpc_addr: SocketAddr,
    pub tpu_addr: SocketAddr,
    pub tpu_fwd_addr: SocketAddr,
    pub builder_pubkey: String,
    pub builder_commission: u32,
    pub prio_fee_recipient: String,
    pub commission_bps: u32,
    pub heartbeat_interval: Duration,
}

impl Default for MockBamNodeConfig {
    fn default() -> Self {
        Self {
            grpc_addr: "127.0.0.1:0".parse().unwrap(),
            tpu_addr: "127.0.0.1:0".parse().unwrap(),
            tpu_fwd_addr: "127.0.0.1:0".parse().unwrap(),
            builder_pubkey: "11111111111111111111111111111111".to_string(),
            builder_commission: 10,
            prio_fee_recipient: "22222222222222222222222222222222".to_string(),
            commission_bps: 100,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }
}

pub struct TransactionQueue {
    queue: Mutex<VecDeque<PacketBatch>>,
    max_size: usize,
}

impl TransactionQueue {
    pub fn new(max_size: usize) -> Self {
        Self {
            queue: Mutex::new(VecDeque::with_capacity(max_size)),
            max_size,
        }
    }

    pub fn push(&self, batch: PacketBatch) -> bool {
        let mut queue = self.queue.lock().unwrap();
        if queue.len() >= self.max_size {
            return false;
        }
        queue.push_back(batch);
        true
    }

    pub fn drain(&self, count: usize) -> Vec<PacketBatch> {
        let mut queue = self.queue.lock().unwrap();
        let drain_count = count.min(queue.len());
        queue.drain(..drain_count).collect()
    }

    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.lock().unwrap().is_empty()
    }
}

struct MockBamNodeState {
    tpu_addr: SocketAddr,
    tpu_fwd_addr: SocketAddr,
    builder_pubkey: String,
    builder_commission: u32,
    prio_fee_recipient: String,
    commission_bps: u32,
    heartbeat_interval: Duration,
    auth_challenges: Mutex<Vec<String>>,
    auth_proofs_received: AtomicU64,
    send_heartbeats: AtomicBool,
    transaction_queue: Arc<TransactionQueue>,
    next_seq_id: AtomicU64,
}

impl MockBamNodeState {
    fn generate_challenge(&self) -> String {
        let challenge = format!(
            "challenge-{}-{}",
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            rand::random::<u64>()
        );
        self.auth_challenges.lock().unwrap().push(challenge.clone());
        challenge
    }

    fn verify_auth_proof(&self, challenge: &str, _pubkey: &str, _signature: &str) -> bool {
        let mut challenges = self.auth_challenges.lock().unwrap();
        if let Some(pos) = challenges.iter().position(|c| c == challenge) {
            challenges.remove(pos);
            self.auth_proofs_received.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn build_config_response(&self) -> ConfigResponse {
        ConfigResponse {
            block_engine_config: Some(BlockEngineBuilderConfig {
                builder_pubkey: self.builder_pubkey.clone(),
                builder_commission: self.builder_commission,
            }),
            bam_config: Some(BamConfig {
                prio_fee_recipient_pubkey: self.prio_fee_recipient.clone(),
                commission_bps: self.commission_bps,
                tpu_sock: Some(Socket {
                    ip: self.tpu_addr.ip().to_string(),
                    port: self.tpu_addr.port() as u32,
                }),
                tpu_fwd_sock: Some(Socket {
                    ip: self.tpu_fwd_addr.ip().to_string(),
                    port: self.tpu_fwd_addr.port() as u32,
                }),
            }),
        }
    }

    fn drain_and_build_batches(&self, max_schedule_slot: u64) -> Option<MultipleAtomicTxnBatch> {
        let packet_batches = self.transaction_queue.drain(10);
        if packet_batches.is_empty() {
            return None;
        }

        let batches: Vec<AtomicTxnBatch> = packet_batches
            .into_iter()
            .flat_map(|batch| {
                batch
                    .iter()
                    .filter_map(|packet| {
                        let data = packet.data(..)?;
                        let seq_id = self.next_seq_id.fetch_add(1, Ordering::Relaxed) as u32;
                        Some(AtomicTxnBatch {
                            seq_id,
                            max_schedule_slot,
                            packets: vec![Packet {
                                data: data.to_vec(),
                                meta: Some(jito_protos::proto::bam_types::Meta {
                                    size: data.len() as u64,
                                    flags: Some(jito_protos::proto::bam_types::PacketFlags {
                                        simple_vote_tx: false,
                                        revert_on_error: false,
                                    }),
                                }),
                            }],
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();

        if batches.is_empty() {
            return None;
        }

        Some(MultipleAtomicTxnBatch { batches })
    }
}

struct MockBamNodeService(Arc<MockBamNodeState>);

#[tonic::async_trait]
impl BamNodeApi for MockBamNodeService {
    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<AuthChallengeResponse>, Status> {
        Ok(Response::new(AuthChallengeResponse {
            challenge_to_sign: self.0.generate_challenge(),
        }))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        Ok(Response::new(self.0.build_config_response()))
    }

    type InitSchedulerStreamStream = ReceiverStream<Result<SchedulerResponse, Status>>;

    async fn init_scheduler_stream(
        &self,
        request: Request<Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (outbound_tx, outbound_rx) = mpsc::channel(GRPC_CHANNEL_SIZE);

        let state = Arc::clone(&self.0);
        let heartbeat_interval = self.0.heartbeat_interval;

        tokio::spawn(async move {
            let mut heartbeat_ticker = tokio::time::interval(heartbeat_interval);
            heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            let mut batch_check_ticker = tokio::time::interval(Duration::from_millis(10));
            batch_check_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    msg = inbound.message() => {
                        match msg {
                            Ok(Some(scheduler_msg)) => {
                                handle_scheduler_message(&state, scheduler_msg);
                            }
                            Ok(None) | Err(_) => break,
                        }
                    }
                    _ = heartbeat_ticker.tick() => {
                        if state.send_heartbeats.load(Ordering::Relaxed) && outbound_tx.send(Ok(heartbeat_response())).await.is_err() {
                            break;
                        }
                    }
                    _ = batch_check_ticker.tick() => {
                        if let Some(batches) = state.drain_and_build_batches(0) {
                            if outbound_tx.send(Ok(batch_response(batches))).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(outbound_rx)))
    }
}

fn handle_scheduler_message(state: &MockBamNodeState, msg: SchedulerMessage) {
    let Some(VersionedMsg::V0(v0)) = msg.versioned_msg else {
        return;
    };

    if let Some(Msg::AuthProof(proof)) = v0.msg {
        state.verify_auth_proof(
            &proof.challenge_to_sign,
            &proof.validator_pubkey,
            &proof.signature,
        );
    }
}

fn heartbeat_response() -> SchedulerResponse {
    SchedulerResponse {
        versioned_msg: Some(ResponseVersionedMsg::V0(SchedulerResponseV0 {
            resp: Some(Resp::HeartBeat(BuilderHeartBeat {
                time_sent_microseconds: SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
            })),
        })),
    }
}

fn batch_response(batches: MultipleAtomicTxnBatch) -> SchedulerResponse {
    SchedulerResponse {
        versioned_msg: Some(ResponseVersionedMsg::V0(SchedulerResponseV0 {
            resp: Some(Resp::MultipleAtomicTxnBatch(batches)),
        })),
    }
}

pub struct MockBamNode {
    state: Arc<MockBamNodeState>,
    grpc_addr: SocketAddr,
    cancel: CancellationToken,
    grpc_handle: Option<tokio::task::JoinHandle<()>>,
    packet_receiver_handle: Option<tokio::task::JoinHandle<()>>,
}

impl MockBamNode {
    pub async fn start(config: MockBamNodeConfig) -> std::io::Result<Self> {
        let cancel = CancellationToken::new();
        let transaction_queue = Arc::new(TransactionQueue::new(DEFAULT_QUEUE_SIZE));

        let tpu_socket = bind_to(config.tpu_addr.ip(), config.tpu_addr.port())?;
        let tpu_fwd_socket = bind_to(config.tpu_fwd_addr.ip(), config.tpu_fwd_addr.port())?;
        let tpu_addr = tpu_socket.local_addr()?;
        let tpu_fwd_addr = tpu_fwd_socket.local_addr()?;

        let grpc_listener = tokio::net::TcpListener::bind(config.grpc_addr).await?;
        let grpc_addr = grpc_listener.local_addr()?;

        let state = Arc::new(MockBamNodeState {
            tpu_addr,
            tpu_fwd_addr,
            builder_pubkey: config.builder_pubkey,
            builder_commission: config.builder_commission,
            prio_fee_recipient: config.prio_fee_recipient,
            commission_bps: config.commission_bps,
            heartbeat_interval: config.heartbeat_interval,
            auth_challenges: Mutex::new(Vec::new()),
            auth_proofs_received: AtomicU64::new(0),
            send_heartbeats: AtomicBool::new(true),
            transaction_queue: transaction_queue.clone(),
            next_seq_id: AtomicU64::new(0),
        });

        let (packet_sender, packet_receiver) = unbounded();
        let staked_nodes = Arc::new(RwLock::new(StakedNodes::default()));
        let keypair = Keypair::new();

        let _tpu_server = spawn_simple_qos_server(
            "mockTpu",
            "mock_tpu",
            vec![tpu_socket],
            &keypair,
            packet_sender.clone(),
            staked_nodes.clone(),
            QuicStreamerConfig::default(),
            SimpleQosConfig::default(),
            cancel.clone(),
        )
        .map_err(|e| std::io::Error::other(format!("Failed to spawn TPU server: {e:?}")))?;

        let _tpu_fwd_server = spawn_simple_qos_server(
            "mockTpuFwd",
            "mock_tpu_fwd",
            vec![tpu_fwd_socket],
            &keypair,
            packet_sender,
            staked_nodes,
            QuicStreamerConfig::default(),
            SimpleQosConfig::default(),
            cancel.clone(),
        )
        .map_err(|e| std::io::Error::other(format!("Failed to spawn TPU FWD server: {e:?}")))?;

        let queue = transaction_queue.clone();
        let receiver_cancel = cancel.clone();
        let packet_receiver_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = receiver_cancel.cancelled() => break,
                    result = tokio::task::spawn_blocking({
                        let packet_receiver = packet_receiver.clone();
                        move || packet_receiver.recv_timeout(Duration::from_millis(100))
                    }) => {
                        if let Ok(Ok(batch)) = result {
                            queue.push(batch);
                        }
                    }
                }
            }
        });

        let grpc_state = Arc::clone(&state);
        let grpc_cancel = cancel.clone();
        let grpc_handle = tokio::spawn(async move {
            tokio::select! {
                _ = grpc_cancel.cancelled() => {}
                result = tonic::transport::Server::builder()
                    .add_service(BamNodeApiServer::new(MockBamNodeService(grpc_state)))
                    .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(grpc_listener)) => {
                    if let Err(e) = result {
                        log::error!("gRPC server error: {e:?}");
                    }
                }
            }
        });

        Ok(Self {
            state,
            grpc_addr,
            cancel,
            grpc_handle: Some(grpc_handle),
            packet_receiver_handle: Some(packet_receiver_handle),
        })
    }

    pub fn grpc_addr(&self) -> SocketAddr {
        self.grpc_addr
    }
    pub fn grpc_url(&self) -> String {
        format!("http://{}", self.grpc_addr)
    }
    pub fn tpu_addr(&self) -> SocketAddr {
        self.state.tpu_addr
    }
    pub fn tpu_fwd_addr(&self) -> SocketAddr {
        self.state.tpu_fwd_addr
    }
    pub fn transaction_queue(&self) -> &Arc<TransactionQueue> {
        &self.state.transaction_queue
    }

    pub fn set_send_heartbeats(&self, send: bool) {
        self.state.send_heartbeats.store(send, Ordering::Relaxed);
    }

    pub fn auth_proofs_received(&self) -> u64 {
        self.state.auth_proofs_received.load(Ordering::Relaxed)
    }

    pub async fn shutdown(&mut self) {
        self.cancel.cancel();
        if let Some(handle) = self.grpc_handle.take() {
            let _ = handle.await;
        }
        if let Some(handle) = self.packet_receiver_handle.take() {
            let _ = handle.await;
        }
    }
}

impl Drop for MockBamNode {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_bam_node_starts() {
        let node = MockBamNode::start(MockBamNodeConfig::default())
            .await
            .expect("should start");
        assert_ne!(node.grpc_addr().port(), 0);
        assert_ne!(node.tpu_addr().port(), 0);
        assert_ne!(node.tpu_fwd_addr().port(), 0);
    }

    #[tokio::test]
    async fn test_transaction_queue_fifo() {
        let queue = TransactionQueue::new(100);
        assert!(queue.push(PacketBatch::from(vec![])));
        assert!(queue.push(PacketBatch::from(vec![])));
        assert_eq!(queue.len(), 2);

        let drained = queue.drain(1);
        assert_eq!(drained.len(), 1);
        assert_eq!(queue.len(), 1);

        queue.drain(10);
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn test_grpc_get_config() {
        use jito_protos::proto::bam_api::bam_node_api_client::BamNodeApiClient;

        let node = MockBamNode::start(MockBamNodeConfig::default())
            .await
            .expect("should start");
        let mut client = BamNodeApiClient::connect(node.grpc_url())
            .await
            .expect("should connect");

        let response = client
            .get_builder_config(ConfigRequest {})
            .await
            .expect("should get config");
        let config = response.into_inner();

        assert!(config.bam_config.is_some());
        assert!(config.block_engine_config.is_some());

        let bam_config = config.bam_config.unwrap();
        assert_eq!(
            bam_config.tpu_sock.unwrap().port,
            node.tpu_addr().port() as u32
        );
    }

    #[tokio::test]
    async fn test_grpc_auth_challenge() {
        use jito_protos::proto::bam_api::bam_node_api_client::BamNodeApiClient;

        let node = MockBamNode::start(MockBamNodeConfig::default())
            .await
            .expect("should start");
        let mut client = BamNodeApiClient::connect(node.grpc_url())
            .await
            .expect("should connect");

        let response = client
            .get_auth_challenge(AuthChallengeRequest {})
            .await
            .expect("should get challenge");
        let challenge = response.into_inner().challenge_to_sign;

        assert!(challenge.starts_with("challenge-"));
    }
}
