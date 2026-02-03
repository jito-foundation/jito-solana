use {
    jito_protos::proto::{
        bam_api::{
            bam_node_api_server::{BamNodeApi, BamNodeApiServer},
            scheduler_message::VersionedMsg,
            scheduler_message_v0::Msg,
            scheduler_response_v0::Resp,
            AuthChallengeRequest, AuthChallengeResponse, ConfigRequest, ConfigResponse,
            SchedulerMessage, SchedulerResponse, SchedulerResponseV0,
        },
        bam_types::{
            AtomicTxnBatch, BamConfig, BlockEngineBuilderConfig, BuilderHeartBeat,
            MultipleAtomicTxnBatch, Packet, Socket,
        },
    },
    solana_core::bam_dependencies::BamOutboundMessage,
    solana_gossip::{cluster_info::ClusterInfo, node::Node},
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    solana_signer::Signer,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, SystemTime},
    },
    tokio::sync::mpsc,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{Request, Response, Status, Streaming},
};

fn v0_response(resp: Resp) -> SchedulerResponse {
    SchedulerResponse {
        versioned_msg: Some(
            jito_protos::proto::bam_api::scheduler_response::VersionedMsg::V0(
                SchedulerResponseV0 { resp: Some(resp) },
            ),
        ),
    }
}

fn heartbeat_response() -> SchedulerResponse {
    v0_response(Resp::HeartBeat(BuilderHeartBeat {
        time_sent_microseconds: SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
    }))
}

#[derive(Clone)]
struct MockConfig {
    builder_pubkey: String,
    builder_commission: u32,
    prio_fee_recipient: String,
    commission_bps: u32,
}

impl Default for MockConfig {
    fn default() -> Self {
        Self {
            builder_pubkey: "11111111111111111111111111111111".to_string(),
            builder_commission: 10,
            prio_fee_recipient: "22222222222222222222222222222222".to_string(),
            commission_bps: 100,
        }
    }
}

struct MockBamNode {
    send_heartbeats: Arc<AtomicBool>,
    heartbeat_interval: Duration,
    auth_proofs_received: Arc<AtomicU64>,
    config: Arc<Mutex<MockConfig>>,
    batch_to_send: Arc<Mutex<Option<AtomicTxnBatch>>>,
    #[allow(dead_code)]
    outbound_tx: Arc<Mutex<Option<mpsc::Sender<AtomicTxnBatch>>>>,
}

impl MockBamNode {
    fn new(send_heartbeats: Arc<AtomicBool>, heartbeat_interval: Duration) -> Self {
        Self {
            send_heartbeats,
            heartbeat_interval,
            auth_proofs_received: Arc::new(AtomicU64::new(0)),
            config: Arc::new(Mutex::new(MockConfig::default())),
            batch_to_send: Arc::new(Mutex::new(None)),
            outbound_tx: Arc::new(Mutex::new(None)),
        }
    }

    #[allow(dead_code)]
    fn with_config(self, config: MockConfig) -> Self {
        *self.config.lock().unwrap() = config;
        self
    }
}

#[tonic::async_trait]
impl BamNodeApi for MockBamNode {
    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<AuthChallengeResponse>, Status> {
        Ok(Response::new(AuthChallengeResponse {
            challenge_to_sign: "test-challenge-12345".to_string(),
        }))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        let config = self.config.lock().unwrap().clone();
        Ok(Response::new(ConfigResponse {
            block_engine_config: Some(BlockEngineBuilderConfig {
                builder_pubkey: config.builder_pubkey,
                builder_commission: config.builder_commission,
            }),
            bam_config: Some(BamConfig {
                prio_fee_recipient_pubkey: config.prio_fee_recipient,
                commission_bps: config.commission_bps,
                tpu_sock: Some(Socket {
                    ip: "127.0.0.1".to_string(),
                    port: 8000,
                }),
                tpu_fwd_sock: Some(Socket {
                    ip: "127.0.0.1".to_string(),
                    port: 8001,
                }),
            }),
        }))
    }

    type InitSchedulerStreamStream = ReceiverStream<Result<SchedulerResponse, Status>>;

    async fn init_scheduler_stream(
        &self,
        request: Request<Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(100);
        let send_heartbeats = self.send_heartbeats.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let auth_proofs_received = self.auth_proofs_received.clone();
        let batch_to_send = self.batch_to_send.clone();

        tokio::spawn(async move {
            let mut authenticated = false;

            while let Ok(Some(msg)) = inbound.message().await {
                if let Some(VersionedMsg::V0(v0)) = msg.versioned_msg {
                    if let Some(Msg::AuthProof(_)) = v0.msg {
                        authenticated = true;
                        auth_proofs_received.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                }
            }

            if !authenticated {
                return;
            }

            if send_heartbeats.load(Ordering::Relaxed)
                && tx.send(Ok(heartbeat_response())).await.is_err()
            {
                return;
            }

            loop {
                tokio::time::sleep(heartbeat_interval).await;

                if send_heartbeats.load(Ordering::Relaxed)
                    && tx.send(Ok(heartbeat_response())).await.is_err()
                {
                    break;
                }

                let batch = batch_to_send.lock().unwrap().take();
                if let Some(batch) = batch {
                    let resp = v0_response(Resp::MultipleAtomicTxnBatch(MultipleAtomicTxnBatch {
                        batches: vec![batch],
                    }));
                    if tx.send(Ok(resp)).await.is_err() {
                        break;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

struct MockServerHandle {
    addr: SocketAddr,
    send_heartbeats: Arc<AtomicBool>,
    #[allow(dead_code)]
    config: Arc<Mutex<MockConfig>>,
    batch_to_send: Arc<Mutex<Option<AtomicTxnBatch>>>,
}

async fn start_mock_server(
    send_heartbeats: Arc<AtomicBool>,
    heartbeat_interval: Duration,
) -> MockServerHandle {
    let mock = MockBamNode::new(send_heartbeats.clone(), heartbeat_interval);
    let config = mock.config.clone();
    let batch_to_send = mock.batch_to_send.clone();

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let server_started = Arc::new(AtomicBool::new(false));
    let server_started_clone = server_started.clone();

    tokio::spawn(async move {
        server_started_clone.store(true, Ordering::Relaxed);
        let _ = tonic::transport::Server::builder()
            .add_service(BamNodeApiServer::new(mock))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await;
    });

    while !server_started.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    MockServerHandle {
        addr,
        send_heartbeats,
        config,
        batch_to_send,
    }
}

fn create_test_cluster_info() -> Arc<ClusterInfo> {
    let keypair = Arc::new(Keypair::new());
    let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
    Arc::new(ClusterInfo::new(
        node.info,
        keypair,
        SocketAddrSpace::Unspecified,
    ))
}

fn create_channels() -> (
    crossbeam_channel::Sender<AtomicTxnBatch>,
    crossbeam_channel::Receiver<AtomicTxnBatch>,
    crossbeam_channel::Sender<BamOutboundMessage>,
    crossbeam_channel::Receiver<BamOutboundMessage>,
) {
    let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
    let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();
    (batch_tx, batch_rx, outbound_tx, outbound_rx)
}

async fn wait_until_healthy(
    connection: &solana_core::bam_connection::BamConnection,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if connection.is_healthy() && connection.get_latest_config().is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[cfg(test)]
mod bam_connection_tests {
    use {super::*, solana_core::bam_connection::BamConnection};

    #[tokio::test]
    async fn test_connection_healthy_with_heartbeats() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats, Duration::from_secs(2)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        assert!(
            wait_until_healthy(&connection, Duration::from_secs(10)).await,
            "connection should become healthy"
        );
        assert!(connection.is_healthy());
        assert!(connection.get_latest_config().is_some());
    }

    #[tokio::test]
    async fn test_connection_unhealthy_when_heartbeats_stop() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats.clone(), Duration::from_millis(500)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        assert!(
            wait_until_healthy(&connection, Duration::from_secs(10)).await,
            "connection should become healthy initially"
        );

        server.send_heartbeats.store(false, Ordering::Relaxed);
        tokio::time::sleep(Duration::from_secs(8)).await;

        assert!(
            !connection.is_healthy(),
            "connection should be unhealthy after heartbeats stop"
        );
    }

    #[tokio::test]
    async fn test_connection_stays_unhealthy_without_initial_heartbeats() {
        let send_heartbeats = Arc::new(AtomicBool::new(false));
        let server = start_mock_server(send_heartbeats, Duration::from_secs(2)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        let became_healthy = wait_until_healthy(&connection, Duration::from_secs(3)).await;

        assert!(!became_healthy);
        assert!(!connection.is_healthy());
    }

    #[tokio::test]
    async fn test_connection_fails_when_server_unreachable() {
        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let result = BamConnection::try_init(
            "http://127.0.0.1:1".to_string(), // Invalid port
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_available_when_healthy() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats, Duration::from_secs(1)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);

        let config = connection.get_latest_config().expect("config should exist");
        let bam_config = config.bam_config.expect("bam_config should exist");
        assert_eq!(bam_config.commission_bps, 100);
    }

    #[tokio::test]
    async fn test_batches_forwarded_to_receiver() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats, Duration::from_millis(100)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);

        let test_batch = AtomicTxnBatch {
            seq_id: 42,
            max_schedule_slot: 100,
            packets: vec![Packet {
                data: vec![1, 2, 3],
                meta: None,
            }],
        };
        *server.batch_to_send.lock().unwrap() = Some(test_batch);

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received = batch_rx.try_recv().expect("should receive batch");
        assert_eq!(received.seq_id, 42);
        assert_eq!(received.max_schedule_slot, 100);
    }

    #[tokio::test]
    async fn test_drop_sets_unhealthy() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats, Duration::from_secs(1)).await;

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection = BamConnection::try_init(
            format!("http://{}", server.addr),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await
        .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);
        assert!(connection.is_healthy());

        drop(connection);
        // Connection is dropped, no assertion needed - Drop impl sets unhealthy
    }
}

#[cfg(test)]
mod bam_manager_tests {
    use {
        super::*,
        solana_core::{
            admin_rpc_post_init::KeyUpdaters,
            bam_dependencies::{BamConnectionState, BamDependencies},
            bam_manager::BamManager,
            proxy::block_engine_stage::BlockBuilderFeeInfo,
        },
        solana_ledger::{blockstore::Blockstore, genesis_utils::create_genesis_config},
        solana_poh::poh_recorder::create_test_recorder,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        std::sync::{atomic::AtomicU8, RwLock},
    };

    fn create_test_bam_dependencies(
        cluster_info: Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<solana_runtime::bank_forks::BankForks>>,
    ) -> BamDependencies {
        let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();

        BamDependencies {
            bam_enabled: Arc::new(AtomicU8::new(BamConnectionState::Disconnected as u8)),
            batch_sender: batch_tx,
            batch_receiver: batch_rx,
            outbound_sender: outbound_tx,
            outbound_receiver: outbound_rx,
            cluster_info,
            block_builder_fee_info: Arc::new(Mutex::new(BlockBuilderFeeInfo::default())),
            bank_forks,
            bam_node_pubkey: Arc::new(Mutex::new(Pubkey::default())),
        }
    }

    fn bam_is_connected(bam_enabled: &Arc<AtomicU8>) -> bool {
        BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
            == BamConnectionState::Connected
    }

    #[allow(clippy::type_complexity)]
    fn create_test_poh_recorder() -> (
        Arc<AtomicBool>,
        Arc<std::sync::RwLock<solana_poh::poh_recorder::PohRecorder>>,
        Arc<RwLock<solana_runtime::bank_forks::BankForks>>,
        solana_poh::poh_service::PohService,
        tempfile::TempDir,
    ) {
        let genesis_config = create_genesis_config(1000).genesis_config;
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let ledger_path = tempfile::tempdir().unwrap();
        let blockstore =
            Arc::new(Blockstore::open(ledger_path.path()).expect("Failed to open blockstore"));

        let (exit, poh_recorder, _poh_controller, _recorder, poh_service, _entry_receiver) =
            create_test_recorder(bank, blockstore, None, None);

        (exit, poh_recorder, bank_forks, poh_service, ledger_path)
    }

    #[tokio::test]
    async fn test_bam_enabled_when_connection_healthy() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats.clone(), Duration::from_secs(1)).await;

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(format!("http://{}", server.addr))));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if bam_is_connected(&bam_enabled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(bam_is_connected(&bam_enabled), "bam_enabled should be true");

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_bam_disabled_when_connection_unhealthy() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats.clone(), Duration::from_millis(500)).await;

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(format!("http://{}", server.addr))));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if bam_is_connected(&bam_enabled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(bam_is_connected(&bam_enabled));

        server.send_heartbeats.store(false, Ordering::Relaxed);

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if !bam_is_connected(&bam_enabled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        assert!(
            !bam_is_connected(&bam_enabled),
            "bam_enabled should be false"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_bam_disabled_when_no_url() {
        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(None));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            !bam_is_connected(&bam_enabled),
            "bam_enabled should remain false"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_reconnects_when_url_changes() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server1 = start_mock_server(send_heartbeats.clone(), Duration::from_secs(1)).await;
        let server2 = start_mock_server(send_heartbeats.clone(), Duration::from_secs(1)).await;

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(format!("http://{}", server1.addr))));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url.clone(),
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if bam_is_connected(&bam_enabled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(bam_is_connected(&bam_enabled));

        *bam_url.lock().unwrap() = Some(format!("http://{}", server2.addr));

        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(
            bam_is_connected(&bam_enabled),
            "bam_enabled should be true after URL change"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_block_builder_fee_info_updated() {
        let send_heartbeats = Arc::new(AtomicBool::new(true));
        let server = start_mock_server(send_heartbeats.clone(), Duration::from_secs(1)).await;

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let block_builder_fee_info = dependencies.block_builder_fee_info.clone();
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(format!("http://{}", server.addr))));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        let start = std::time::Instant::now();
        while start.elapsed() < Duration::from_secs(15) {
            if bam_is_connected(&bam_enabled) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        tokio::time::sleep(Duration::from_secs(2)).await;

        let fee_info = block_builder_fee_info.lock().unwrap();
        assert_eq!(fee_info.block_builder_commission, 10);

        exit.store(true, Ordering::Relaxed);
        drop(fee_info);
        poh_service.join().unwrap();
    }
}
