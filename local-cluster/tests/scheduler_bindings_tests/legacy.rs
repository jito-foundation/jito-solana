//! Legacy bundles enter through authenticated Block Engine gRPC, never RPC or shared queues.
use {
    super::RunningScheduler,
    jito_protos::proto::{
        auth::{
            GenerateAuthChallengeRequest, GenerateAuthChallengeResponse, GenerateAuthTokensRequest,
            GenerateAuthTokensResponse, RefreshAccessTokenRequest, RefreshAccessTokenResponse,
            Role, Token,
            auth_service_server::{AuthService, AuthServiceServer},
        },
        block_engine::{
            BlockBuilderFeeInfoRequest, BlockBuilderFeeInfoResponse, BlockEngineEndpoint,
            GetBlockEngineEndpointRequest, GetBlockEngineEndpointResponse, SubscribeBundlesRequest,
            SubscribeBundlesResponse, SubscribePacketsRequest, SubscribePacketsResponse,
            block_engine_validator_server::{BlockEngineValidator, BlockEngineValidatorServer},
        },
        bundle::{Bundle, BundleUuid},
        packet,
    },
    serial_test::serial,
    solana_commitment_config::CommitmentConfig,
    solana_core::{
        proxy::block_engine_stage::BlockEngineConfig,
        tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
        validator::ValidatorConfig,
    },
    solana_keypair::Keypair,
    solana_local_cluster::{
        cluster::Cluster,
        integration_tests::{DEFAULT_NODE_STAKE, RUST_LOG_FILTER},
        local_cluster::{ClusterConfig, DEFAULT_MINT_LAMPORTS, LocalCluster},
        validator_configs::make_identical_validator_configs,
    },
    solana_net_utils::{SocketAddrSpace, sockets::bind_to_localhost_unique},
    solana_program_binaries::{jito_tip_distribution, jito_tip_payment},
    solana_pubkey::Pubkey,
    solana_rent::Rent,
    solana_rpc_client::rpc_client::RpcClient,
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{
        collections::HashSet,
        net::UdpSocket,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        thread::sleep,
        time::{Duration, Instant, SystemTime},
    },
    tokio::{runtime::Runtime, sync::mpsc},
    tokio_stream::wrappers::{ReceiverStream, TcpListenerStream},
    tonic::{Request, Response, Status},
};

type BundleStreamSender = mpsc::Sender<Result<SubscribeBundlesResponse, Status>>;
type PacketStreamSender = mpsc::Sender<Result<SubscribePacketsResponse, Status>>;

#[derive(Clone)]
struct AuthenticatedBlockEngine {
    identity: Pubkey,
    endpoint: BlockEngineEndpoint,
    challenges: Arc<Mutex<HashSet<String>>>,
    access_token: String,
    refresh_token: String,
    authenticated: Arc<AtomicUsize>,
    bundles: Arc<Mutex<Option<BundleStreamSender>>>,
    packets: Arc<Mutex<Option<PacketStreamSender>>>,
}

impl AuthenticatedBlockEngine {
    fn token(&self, value: &str) -> Token {
        let mut token = Token {
            value: value.to_owned(),
            ..Token::default()
        };
        token.expires_at_utc.get_or_insert_default().seconds = i64::try_from(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_add(3600),
        )
        .unwrap();
        token
    }

    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        let expected = format!("Bearer {}", self.access_token);
        if self.authenticated.load(Ordering::Acquire) == 0
            || request
                .metadata()
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                != Some(expected.as_str())
        {
            return Err(Status::unauthenticated(
                "missing or invalid validator access token",
            ));
        }
        Ok(())
    }

    fn wait_subscribed(&self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if self.bundles.lock().unwrap().is_some() && self.packets.lock().unwrap().is_some() {
                assert!(self.authenticated.load(Ordering::Acquire) > 0);
                return;
            }
            assert!(
                Instant::now() < deadline,
                "validator did not authenticate and subscribe to both Block Engine streams"
            );
            sleep(Duration::from_millis(10));
        }
    }

    fn send_bundle(&self, bundle: BundleUuid) {
        self.bundles
            .lock()
            .unwrap()
            .as_ref()
            .expect("authenticated bundle subscription")
            .try_send(Ok(SubscribeBundlesResponse {
                bundles: vec![bundle],
            }))
            .expect("Block Engine bundle stream disconnected or backpressured");
    }
}

#[tonic::async_trait]
impl AuthService for AuthenticatedBlockEngine {
    async fn generate_auth_challenge(
        &self,
        request: Request<GenerateAuthChallengeRequest>,
    ) -> Result<Response<GenerateAuthChallengeResponse>, Status> {
        let request = request.into_inner();
        if request.role != Role::Validator as i32
            || request.pubkey.as_slice() != self.identity.as_ref()
        {
            return Err(Status::unauthenticated(
                "unexpected validator identity or role",
            ));
        }
        let challenge = Keypair::new().pubkey().to_string();
        self.challenges
            .lock()
            .unwrap()
            .insert(format!("{}-{challenge}", self.identity));
        Ok(Response::new(GenerateAuthChallengeResponse { challenge }))
    }

    async fn generate_auth_tokens(
        &self,
        request: Request<GenerateAuthTokensRequest>,
    ) -> Result<Response<GenerateAuthTokensResponse>, Status> {
        let request = request.into_inner();
        let signature = Signature::try_from(request.signed_challenge.as_slice())
            .map_err(|_| Status::unauthenticated("invalid signature encoding"))?;
        if request.client_pubkey.as_slice() != self.identity.as_ref()
            || !self.challenges.lock().unwrap().remove(&request.challenge)
            || !signature.verify(self.identity.as_ref(), request.challenge.as_bytes())
        {
            return Err(Status::unauthenticated(
                "invalid validator challenge signature",
            ));
        }
        self.authenticated.fetch_add(1, Ordering::Release);
        Ok(Response::new(GenerateAuthTokensResponse {
            access_token: Some(self.token(&self.access_token)),
            refresh_token: Some(self.token(&self.refresh_token)),
        }))
    }

    async fn refresh_access_token(
        &self,
        request: Request<RefreshAccessTokenRequest>,
    ) -> Result<Response<RefreshAccessTokenResponse>, Status> {
        if request.into_inner().refresh_token != self.refresh_token {
            return Err(Status::unauthenticated("invalid refresh token"));
        }
        Ok(Response::new(RefreshAccessTokenResponse {
            access_token: Some(self.token(&self.access_token)),
        }))
    }
}

#[tonic::async_trait]
impl BlockEngineValidator for AuthenticatedBlockEngine {
    type SubscribePacketsStream = ReceiverStream<Result<SubscribePacketsResponse, Status>>;
    type SubscribeBundlesStream = ReceiverStream<Result<SubscribeBundlesResponse, Status>>;

    async fn subscribe_packets(
        &self,
        request: Request<SubscribePacketsRequest>,
    ) -> Result<Response<Self::SubscribePacketsStream>, Status> {
        self.authorize(&request)?;
        let (sender, receiver) = mpsc::channel(16);
        // Retain the sender so the real validator's packet stream remains open.
        *self.packets.lock().unwrap() = Some(sender);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn subscribe_bundles(
        &self,
        request: Request<SubscribeBundlesRequest>,
    ) -> Result<Response<Self::SubscribeBundlesStream>, Status> {
        self.authorize(&request)?;
        let (sender, receiver) = mpsc::channel(16);
        *self.bundles.lock().unwrap() = Some(sender);
        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn get_block_builder_fee_info(
        &self,
        request: Request<BlockBuilderFeeInfoRequest>,
    ) -> Result<Response<BlockBuilderFeeInfoResponse>, Status> {
        self.authorize(&request)?;
        Ok(Response::new(BlockBuilderFeeInfoResponse {
            pubkey: self.identity.to_string(),
            commission: 0,
        }))
    }

    async fn get_block_engine_endpoints(
        &self,
        _request: Request<GetBlockEngineEndpointRequest>,
    ) -> Result<Response<GetBlockEngineEndpointResponse>, Status> {
        // Direct-global mode still discovers the global URL and shred destination
        // before authenticating; only regional latency selection is disabled.
        Ok(Response::new(GetBlockEngineEndpointResponse {
            global_endpoint: Some(self.endpoint.clone()),
            regioned_endpoints: vec![self.endpoint.clone()],
        }))
    }
}

struct BlockEngineServer {
    url: String,
    service: AuthenticatedBlockEngine,
    server_task: tokio::task::JoinHandle<()>,
    _shred_receiver: UdpSocket,
    _runtime: Runtime,
}

impl BlockEngineServer {
    fn start(identity: Pubkey) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener =
            runtime.block_on(async { tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap() });
        let url = format!("http://{}", listener.local_addr().unwrap());
        let shred_receiver = bind_to_localhost_unique().unwrap();
        let service = AuthenticatedBlockEngine {
            identity,
            endpoint: BlockEngineEndpoint {
                block_engine_url: url.clone(),
                shredstream_receiver_address: shred_receiver.local_addr().unwrap().to_string(),
            },
            challenges: Arc::new(Mutex::new(HashSet::new())),
            access_token: Keypair::new().pubkey().to_string(),
            refresh_token: Keypair::new().pubkey().to_string(),
            authenticated: Arc::new(AtomicUsize::new(0)),
            bundles: Arc::new(Mutex::new(None)),
            packets: Arc::new(Mutex::new(None)),
        };
        let node = service.clone();
        let server_task = runtime.spawn(async move {
            tonic::transport::Server::builder()
                .add_service(AuthServiceServer::new(node.clone()))
                .add_service(BlockEngineValidatorServer::new(node))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        Self {
            url,
            service,
            server_task,
            _shred_receiver: shred_receiver,
            _runtime: runtime,
        }
    }
}

impl Drop for BlockEngineServer {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

fn send_and_confirm_bundle(
    server: &BlockEngineServer,
    rpc: &RpcClient,
    payer: &Keypair,
    tip_account: &Pubkey,
    phase: &str,
) {
    let intermediate = Keypair::new();
    let recipient = Pubkey::new_unique();
    let blockhash = rpc.get_latest_blockhash().unwrap();
    // Transaction 2 cannot execute before transaction 1 creates and funds its payer.
    // Transaction 3 pays a real tip account initialized by the validator's TipManager.
    let transactions = [
        system_transaction::transfer(payer, &intermediate.pubkey(), 3_000_000, blockhash),
        system_transaction::transfer(&intermediate, &recipient, 1_000_000, blockhash),
        system_transaction::transfer(payer, tip_account, 10_000, blockhash),
    ];
    let intermediate_fee = rpc.get_fee_for_message(&transactions[1].message).unwrap();
    let signatures = transactions
        .iter()
        .map(|tx| tx.signatures[0])
        .collect::<Vec<_>>();
    let bundle = BundleUuid {
        uuid: Keypair::new().pubkey().to_string(),
        bundle: Some(Bundle {
            header: None,
            packets: transactions
                .iter()
                .map(|transaction| {
                    let data = wincode::serialize(transaction).unwrap();
                    packet::Packet {
                        meta: Some(packet::Meta {
                            size: u64::try_from(data.len()).unwrap(),
                            ..packet::Meta::default()
                        }),
                        data: data.into(),
                    }
                })
                .collect(),
        }),
    };
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut last_send = Instant::now() - Duration::from_secs(1);
    loop {
        // Reoffer the identical signed bundle during the session-loss handover, as the
        // internal BundleStage may not resume until the missing heartbeat is detected.
        // None of these transactions is ever submitted through RPC.
        if last_send.elapsed() >= Duration::from_millis(250) {
            server.service.send_bundle(bundle.clone());
            last_send = Instant::now();
        }
        let statuses = rpc.get_signature_statuses(&signatures).unwrap().value;
        if statuses.iter().all(|status| {
            status
                .as_ref()
                .is_some_and(|status| status.satisfies_commitment(CommitmentConfig::confirmed()))
        }) {
            let slots = statuses
                .iter()
                .map(|status| {
                    let status = status.as_ref().unwrap();
                    assert_eq!(
                        status.err, None,
                        "{phase}: bundle transaction failed: {status:?}"
                    );
                    status.slot
                })
                .collect::<Vec<_>>();
            assert!(
                slots.iter().all(|slot| *slot == slots[0]),
                "{phase}: bundle split across slots: {slots:?}"
            );
            assert_eq!(
                rpc.get_balance(&recipient).unwrap(),
                1_000_000,
                "{phase}: recipient balance"
            );
            assert_eq!(
                rpc.get_balance(&intermediate.pubkey()).unwrap(),
                2_000_000_u64.checked_sub(intermediate_fee).unwrap(),
                "{phase}: dependent transaction payer balance"
            );
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{phase}: legacy bundle {} not confirmed; signatures={signatures:?}, \
             statuses={statuses:?}",
            bundle.uuid
        );
        sleep(Duration::from_millis(50));
    }
}

#[test]
#[serial]
fn test_jito_scheduler_bindings_legacy_bundle_and_fallback() {
    agave_logger::setup_with_default(RUST_LOG_FILTER);
    let validator_config = ValidatorConfig {
        jito_scheduler_bindings: true,
        ..ValidatorConfig::default_for_test()
    };
    let block_engine_config = validator_config.block_engine_config.clone();
    let cluster = LocalCluster::new(
        &mut ClusterConfig {
            node_stakes: vec![DEFAULT_NODE_STAKE],
            mint_lamports: DEFAULT_MINT_LAMPORTS,
            validator_configs: make_identical_validator_configs(&validator_config, 1),
            ticks_per_slot: 16,
            rent: Rent::default(),
            ..ClusterConfig::default()
        },
        SocketAddrSpace::Unspecified,
    );
    let identity = *cluster.entry_point_info.pubkey();
    let validator = &cluster.validators[&identity];
    // Use LocalCluster's real genesis programs, rent and validator/vote identities.
    assert!(cluster.genesis_config.rent.minimum_balance(0) > 0);
    let tips = TipManager::new(TipManagerConfig {
        tip_payment_program_id: jito_tip_payment::id(),
        tip_distribution_program_id: jito_tip_distribution::id(),
        tip_distribution_account_config: TipDistributionAccountConfig {
            merkle_root_upload_authority: identity,
            vote_account: validator.info.voting_keypair.pubkey(),
            commission_bps: 10,
        },
    });
    let rpc = cluster
        .build_rpc_client_with_commitment(&identity, CommitmentConfig::confirmed())
        .unwrap();
    assert_eq!(
        rpc.get_minimum_balance_for_rent_exemption(0).unwrap(),
        Rent::default().minimum_balance(0),
        "validator must enforce normal rent for real tip-program initialization"
    );
    for program in [
        tips.tip_payment_program_id(),
        tips.tip_distribution_program_id(),
    ] {
        assert!(
            rpc.get_account(&program).unwrap().executable,
            "tip program {program} missing"
        );
    }
    let server = BlockEngineServer::start(identity);
    let ipc_path = validator.info.ledger_path.join("scheduler_bindings.ipc");
    let mut external = RunningScheduler::attach(&ipc_path);
    block_engine_config.store(Arc::new(BlockEngineConfig {
        block_engine_url: server.url.clone(),
        disable_block_engine_autoconfig: true,
        trust_packets: false,
    }));
    server.service.wait_subscribed();
    let tip_account = *tips.get_tip_accounts().iter().next().unwrap();
    send_and_confirm_bundle(
        &server,
        &rpc,
        &cluster.funding_keypair,
        &tip_account,
        "external",
    );
    for (address, owner) in [
        (
            tips.tip_payment_config_pubkey(),
            tips.tip_payment_program_id(),
        ),
        (
            tips.tip_distribution_config_pubkey(),
            tips.tip_distribution_program_id(),
        ),
        (tip_account, tips.tip_payment_program_id()),
    ] {
        assert_eq!(
            rpc.get_account(&address).unwrap().owner,
            owner,
            "real tip account {address}"
        );
    }
    let stats = external.stop();
    assert!(
        stats.received_transactions >= 3,
        "legacy ingress was not scheduled: {stats:?}"
    );
    assert!(
        stats.completed_batches > 0,
        "legacy bundle did not complete externally: {stats:?}"
    );
    // The client has joined and cannot execute another transaction. Send a new legacy
    // bundle through the same authenticated network ingress; only internal fallback can commit it.
    send_and_confirm_bundle(
        &server,
        &rpc,
        &cluster.funding_keypair,
        &tip_account,
        "fallback",
    );
    assert!(server.service.authenticated.load(Ordering::Acquire) > 0);
}
