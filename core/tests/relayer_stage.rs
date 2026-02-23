use {
    crossbeam_channel::unbounded,
    jito_protos::proto::{
        auth::{
            auth_service_server::{AuthService, AuthServiceServer},
            GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
            GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse,
            Token,
        },
        relayer::{
            relayer_server::{Relayer, RelayerServer},
            GetTpuConfigsRequest, GetTpuConfigsResponse, SubscribePacketsRequest,
            SubscribePacketsResponse,
        },
        shared::Socket,
    },
    prost_types::Timestamp,
    solana_core::proxy::relayer_stage::{RelayerConfig, RelayerStage},
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    solana_signer::Signer,
    solana_time_utils::timestamp,
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::{Duration, Instant},
    },
    tokio::sync::watch,
    tokio_stream::wrappers::ReceiverStream,
    tonic::{Request, Response, Status},
};

fn mock_token() -> Token {
    Token {
        value: "token".into(),
        expires_at_utc: Some(Timestamp {
            seconds: i64::MAX / 2,
            nanos: 0,
        }),
    }
}

struct MockAuthService;

#[tonic::async_trait]
impl AuthService for MockAuthService {
    async fn generate_auth_challenge(
        &self,
        _: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        Ok(Response::new(GenerateAuthChallengeResponse {
            challenge: "c".into(),
        }))
    }
    async fn generate_auth_tokens(
        &self,
        _: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(mock_token()),
            refresh_token: Some(mock_token()),
        }))
    }
    async fn refresh_access_token(
        &self,
        _: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(mock_token()),
        }))
    }
}

struct MockRelayer(Arc<AtomicU64>);

#[tonic::async_trait]
impl Relayer for MockRelayer {
    async fn get_tpu_configs(
        &self,
        _: Request<GetTpuConfigsRequest>,
    ) -> Result<Response<GetTpuConfigsResponse>, Status> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(Response::new(GetTpuConfigsResponse {
            tpu: Some(Socket {
                ip: "127.0.0.1".into(),
                port: 9000,
            }),
            tpu_forward: Some(Socket {
                ip: "127.0.0.1".into(),
                port: 9001,
            }),
        }))
    }

    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;

    async fn subscribe_packets(
        &self,
        _: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            std::future::pending::<()>().await;
            drop(tx);
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

async fn start_mock_relayer() -> (SocketAddr, Arc<AtomicU64>) {
    let connection_count = Arc::new(AtomicU64::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn({
        let count = connection_count.clone();
        async move {
            tonic::transport::Server::builder()
                .add_service(AuthServiceServer::new(MockAuthService))
                .add_service(RelayerServer::new(MockRelayer(count)))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
                .await
                .ok();
        }
    });

    (addr, connection_count)
}

fn make_cluster_info() -> Arc<ClusterInfo> {
    let keypair = Arc::new(Keypair::new());
    Arc::new(ClusterInfo::new(
        ContactInfo::new_localhost(&keypair.pubkey(), timestamp()),
        keypair,
        SocketAddrSpace::Unspecified,
    ))
}

fn valid_config(addr: SocketAddr) -> RelayerConfig {
    RelayerConfig {
        relayer_url: format!("http://{addr}"),
        expected_heartbeat_interval: Duration::from_millis(1000),
        oldest_allowed_heartbeat: Duration::from_secs(30),
    }
}

fn invalid_config() -> RelayerConfig {
    RelayerConfig {
        relayer_url: "not a url".to_string(),
        expected_heartbeat_interval: Duration::from_millis(1000),
        oldest_allowed_heartbeat: Duration::from_secs(30),
    }
}

async fn wait_for_count_at_least(counter: &AtomicU64, target: u64) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if counter.load(Ordering::SeqCst) >= target {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("expected connection count >= {target}, got {}", counter.load(Ordering::SeqCst));
}

async fn wait_for_stage_exit(stage: RelayerStage, timeout: Duration) {
    let handle = tokio::task::spawn_blocking(move || stage.join());
    let join_result = tokio::time::timeout(timeout, handle)
        .await
        .expect("relayer stage did not exit in time");
    let _ = join_result.expect("relayer stage join failed");
}

#[tokio::test]
async fn test_relayer_stage_connects_on_config_update() {
    let (addr, connection_count) = start_mock_relayer().await;
    let cluster_info = make_cluster_info();

    let (config_tx, config_rx) = watch::channel(None);
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    // No connection with empty config
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(connection_count.load(Ordering::SeqCst), 0);

    // Send valid config -> should connect
    config_tx.send(Some(valid_config(addr))).unwrap();
    wait_for_count_at_least(&connection_count, 1).await;

    config_tx.send(None).unwrap();
    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_relayer_stage_ignores_invalid_then_connects() {
    let (addr, connection_count) = start_mock_relayer().await;
    let cluster_info = make_cluster_info();

    let (config_tx, config_rx) = watch::channel(Some(invalid_config()));
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(connection_count.load(Ordering::SeqCst), 0);

    config_tx.send(Some(valid_config(addr))).unwrap();
    wait_for_count_at_least(&connection_count, 1).await;

    config_tx.send(None).unwrap();
    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_relayer_stage_reconnects_on_config_change() {
    let (addr, connection_count) = start_mock_relayer().await;
    let cluster_info = make_cluster_info();

    let (config_tx, config_rx) = watch::channel(Some(valid_config(addr)));
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    wait_for_count_at_least(&connection_count, 1).await;
    let initial_count = connection_count.load(Ordering::SeqCst);

    // Update config to trigger reconnect.
    let mut updated = valid_config(addr);
    updated.expected_heartbeat_interval = Duration::from_millis(1200);
    updated.oldest_allowed_heartbeat = Duration::from_secs(35);
    config_tx.send(Some(updated)).unwrap();

    wait_for_count_at_least(&connection_count, initial_count + 1).await;

    config_tx.send(None).unwrap();
    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_relayer_stage_stops_on_config_clear() {
    let (addr, connection_count) = start_mock_relayer().await;
    let cluster_info = make_cluster_info();

    let (config_tx, config_rx) = watch::channel(Some(valid_config(addr)));
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    wait_for_count_at_least(&connection_count, 1).await;
    let count_before_clear = connection_count.load(Ordering::SeqCst);

    config_tx.send(None).unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let count_after_clear = connection_count.load(Ordering::SeqCst);
    assert_eq!(count_after_clear, count_before_clear);

    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_relayer_stage_exit_while_idle() {
    let cluster_info = make_cluster_info();

    let (_config_tx, config_rx) = watch::channel(None);
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}

#[tokio::test]
async fn test_relayer_stage_exit_while_invalid() {
    let cluster_info = make_cluster_info();

    let (_config_tx, config_rx) = watch::channel(Some(invalid_config()));
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    let stage = RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    exit.store(true, Ordering::Relaxed);
    wait_for_stage_exit(stage, Duration::from_secs(2)).await;
}
