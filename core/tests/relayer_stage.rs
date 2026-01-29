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
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc,
        },
        time::Duration,
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
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::test]
async fn test_relayer_stage_connects_on_config_update() {
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

    let keypair = Arc::new(Keypair::new());
    let cluster_info = Arc::new(ClusterInfo::new(
        ContactInfo::new_localhost(&keypair.pubkey(), timestamp()),
        keypair,
        SocketAddrSpace::Unspecified,
    ));

    let (config_tx, config_rx) = watch::channel(RelayerConfig::default());
    let (heartbeat_tx, _) = unbounded();
    let (packet_tx, _) = unbounded();
    let exit = Arc::new(AtomicBool::new(false));

    RelayerStage::new(
        config_rx,
        cluster_info,
        heartbeat_tx,
        packet_tx,
        exit.clone(),
    );

    // No connection with empty config
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(connection_count.load(Ordering::SeqCst), 0);

    // Send valid config → should connect
    config_tx
        .send(RelayerConfig {
            relayer_url: format!("http://{addr}"),
            expected_heartbeat_interval: Duration::from_millis(500),
            oldest_allowed_heartbeat: Duration::from_millis(2000),
        })
        .unwrap();

    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(connection_count.load(Ordering::SeqCst) > 0);

    exit.store(true, Ordering::Relaxed);
}
