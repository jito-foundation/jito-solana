use {
    agave_scheduler_bindings::{LEADER_READY, SharableTransactionBatchRegion, processed_codes},
    agave_scheduling_utils::handshake::{ClientSession, client},
    arc_swap::ArcSwap,
    crossbeam_channel::{Receiver, Sender, unbounded},
    jito_protos::proto::{
        bam_api::{
            AuthChallengeRequest, AuthChallengeResponse, ConfigRequest, ConfigResponse,
            SchedulerMessage, SchedulerResponse, SchedulerResponseV0,
            bam_node_api_server::{BamNodeApi, BamNodeApiServer},
            scheduler_message::VersionedMsg,
            scheduler_message_v0::Msg,
            scheduler_response_v0::Resp,
        },
        bam_types::{
            AtomicTxnBatch, AtomicTxnBatchResult, AuthProof, BamConfig, BlockEngineBuilderConfig,
            BuilderHeartBeat, LeaderState, Meta, MultipleAtomicTxnBatch, Packet, PacketFlags,
            Socket, atomic_txn_batch_result,
        },
    },
    jito_scheduler::{SchedulerError, SchedulerStats},
    jito_scheduler_bindings::{HEARTBEAT_ID, JitoExecutionResponse, JitoResponseRegion},
    serial_test::serial,
    solana_commitment_config::CommitmentConfig,
    solana_core::{bam_connection::BamConnection, validator::ValidatorConfig},
    solana_keypair::Keypair,
    solana_local_cluster::{
        cluster::Cluster,
        integration_tests::{DEFAULT_NODE_STAKE, RUST_LOG_FILTER},
        local_cluster::{ClusterConfig, DEFAULT_MINT_LAMPORTS, LocalCluster},
        validator_configs::make_identical_validator_configs,
    },
    solana_net_utils::SocketAddrSpace,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{
        collections::HashSet,
        path::Path,
        str::FromStr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        thread::{JoinHandle, sleep},
        time::{Duration, Instant, SystemTime},
    },
    tokio::{runtime::Runtime, sync::mpsc},
    tokio_stream::wrappers::{ReceiverStream, TcpListenerStream},
    tonic::{Request, Response, Status, Streaming},
};

pub(super) struct RunningScheduler {
    exit: Arc<AtomicBool>,
    thread: Option<JoinHandle<Result<SchedulerStats, SchedulerError>>>,
}

impl RunningScheduler {
    pub(super) fn attach(path: &Path) -> Self {
        Self::attach_inner(path, None)
    }

    fn attach_with_bam(path: &Path, bam_url: &ArcSwap<Option<String>>, url: String) -> Self {
        Self::attach_inner(path, Some((bam_url, url)))
    }

    fn attach_inner(path: &Path, bam: Option<(&ArcSwap<Option<String>>, String)>) -> Self {
        let mut logon = jito_scheduler::client_logon(2, 2);
        logon.allocator_size = 64 * 1024 * 1024;
        let mut session = client::connect(path, logon, Duration::from_secs(10)).unwrap();
        // Progress is published after the validator installs this session's workers
        // and pauses internal scheduling. Only then allow BAM to authenticate a stream.
        Self::wait_ready(&mut session, false);
        if let Some((bam_url, url)) = bam {
            bam_url.store(Arc::new(Some(url)));
            Self::wait_ready(&mut session, true);
        }
        let exit = Arc::new(AtomicBool::new(false));
        let client_exit = exit.clone();
        let thread = std::thread::spawn(move || {
            jito_scheduler::run(
                session,
                client_exit,
                jito_scheduler::SchedulerConfig::default(),
            )
        });
        Self {
            exit,
            thread: Some(thread),
        }
    }

    fn wait_ready(session: &mut ClientSession, bam: bool) {
        let deadline = Instant::now() + Duration::from_secs(20);
        let mut last_heartbeat = Instant::now() - Duration::from_secs(1);
        loop {
            let jito = session.jito.as_mut().unwrap();
            // This helper owns the actual client queues until run() starts. Keep its
            // liveness lease while observing progress; no transaction result is injected.
            if last_heartbeat.elapsed() >= Duration::from_millis(500) {
                jito.completion
                    .try_write(JitoExecutionResponse {
                        id: HEARTBEAT_ID,
                        batch: SharableTransactionBatchRegion {
                            num_transactions: 0,
                            transactions_offset: 0,
                        },
                        processed_code: processed_codes::PROCESSED,
                        execution_slot: 0,
                        bank_id: 0,
                        responses: JitoResponseRegion::default(),
                    })
                    .unwrap();
                last_heartbeat = Instant::now();
            }
            if jito.progress.try_read().is_some_and(|message| {
                message.progress.leader_state == LEADER_READY
                    && message.bank_id != u64::MAX
                    && (!bam || (message.bam_connected != 0 && message.atomic_batches_enabled != 0))
            }) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "external session did not acquire a ready bank (BAM required: {bam})"
            );
            sleep(Duration::from_millis(10));
        }
    }

    pub(super) fn stop(&mut self) -> SchedulerStats {
        self.exit.store(true, Ordering::Release);
        self.thread.take().unwrap().join().unwrap().unwrap()
    }
}

impl Drop for RunningScheduler {
    fn drop(&mut self) {
        self.exit.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn v0_response(resp: Resp) -> SchedulerResponse {
    SchedulerResponse {
        versioned_msg: Some(
            jito_protos::proto::bam_api::scheduler_response::VersionedMsg::V0(
                SchedulerResponseV0 { resp: Some(resp) },
            ),
        ),
    }
}

#[derive(Clone)]
struct AuthenticatedBam {
    identity: Pubkey,
    challenges: Arc<Mutex<HashSet<String>>>,
    authenticated: Arc<AtomicUsize>,
    batch: Arc<Mutex<Option<AtomicTxnBatch>>>,
    leaders: Sender<LeaderState>,
    results: Sender<AtomicTxnBatchResult>,
}

impl AuthenticatedBam {
    fn verify(&self, proof: AuthProof) -> Result<(), Status> {
        if !self
            .challenges
            .lock()
            .unwrap()
            .remove(&proof.challenge_to_sign)
        {
            return Err(Status::unauthenticated("unknown or already used challenge"));
        }
        let identity = Pubkey::from_str(&proof.validator_pubkey)
            .map_err(|_| Status::unauthenticated("invalid validator identity"))?;
        let signature = Signature::from_str(&proof.signature)
            .map_err(|_| Status::unauthenticated("invalid signature encoding"))?;
        if identity != self.identity
            || !signature.verify(
                identity.as_ref(),
                &BamConnection::labeled_bytes(proof.challenge_to_sign.as_bytes()),
            )
        {
            return Err(Status::unauthenticated("invalid validator signature"));
        }
        self.authenticated.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[tonic::async_trait]
impl BamNodeApi for AuthenticatedBam {
    async fn get_auth_challenge(
        &self,
        _request: Request<AuthChallengeRequest>,
    ) -> Result<Response<AuthChallengeResponse>, Status> {
        let challenge_to_sign = Keypair::new().pubkey().to_string();
        self.challenges
            .lock()
            .unwrap()
            .insert(challenge_to_sign.clone());
        Ok(Response::new(AuthChallengeResponse { challenge_to_sign }))
    }

    async fn get_builder_config(
        &self,
        _request: Request<ConfigRequest>,
    ) -> Result<Response<ConfigResponse>, Status> {
        Ok(Response::new(ConfigResponse {
            block_engine_config: Some(BlockEngineBuilderConfig {
                builder_pubkey: self.identity.to_string(),
                builder_commission: 0,
            }),
            bam_config: Some(BamConfig {
                prio_fee_recipient_pubkey: self.identity.to_string(),
                commission_bps: 0,
                tpu_sock: Some(Socket {
                    ip: "127.0.0.1".into(),
                    port: 8000,
                }),
                tpu_fwd_sock: Some(Socket {
                    ip: "127.0.0.1".into(),
                    port: 8001,
                }),
                shred_socks: vec![],
            }),
        }))
    }

    type InitSchedulerStreamStream = ReceiverStream<Result<SchedulerResponse, Status>>;

    async fn init_scheduler_stream(
        &self,
        request: Request<Streaming<SchedulerMessage>>,
    ) -> Result<Response<Self::InitSchedulerStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (sender, receiver) = mpsc::channel(100);
        let service = self.clone();
        tokio::spawn(async move {
            let proof = match inbound.message().await {
                Ok(Some(SchedulerMessage {
                    versioned_msg: Some(VersionedMsg::V0(message)),
                })) => match message.msg {
                    Some(Msg::AuthProof(proof)) => proof,
                    _ => {
                        let _ = sender
                            .send(Err(Status::unauthenticated("expected auth proof")))
                            .await;
                        return;
                    }
                },
                _ => return,
            };
            if let Err(error) = service.verify(proof) {
                let _ = sender.send(Err(error)).await;
                return;
            }
            let mut interval = tokio::time::interval(Duration::from_millis(25));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    message = inbound.message() => {
                        let Ok(Some(SchedulerMessage { versioned_msg: Some(VersionedMsg::V0(message)) })) = message else { return; };
                        match message.msg {
                            Some(Msg::LeaderState(state)) => { let _ = service.leaders.send(state); }
                            Some(Msg::MultipleAtomicTxnBatchResult(results)) => {
                                for result in results.results { let _ = service.results.send(result); }
                            }
                            _ => {}
                        }
                    }
                    _ = interval.tick() => {
                        let timestamp = u64::try_from(SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()).unwrap();
                        if sender.send(Ok(v0_response(Resp::HeartBeat(BuilderHeartBeat { time_sent_microseconds: timestamp })))).await.is_err() { return; }
                        let batch = service.batch.lock().unwrap().take();
                        if let Some(batch) = batch
                            && sender.send(Ok(v0_response(Resp::MultipleAtomicTxnBatch(MultipleAtomicTxnBatch { batches: vec![batch] })))).await.is_err() { return; }
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

struct BamServer {
    url: String,
    service: AuthenticatedBam,
    leaders: Receiver<LeaderState>,
    results: Receiver<AtomicTxnBatchResult>,
    server_task: tokio::task::JoinHandle<()>,
    _runtime: Runtime,
}

impl BamServer {
    fn start(identity: Pubkey) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let (leader_sender, leaders) = unbounded();
        let (result_sender, results) = unbounded();
        let service = AuthenticatedBam {
            identity,
            challenges: Arc::new(Mutex::new(HashSet::new())),
            authenticated: Arc::new(AtomicUsize::new(0)),
            batch: Arc::new(Mutex::new(None)),
            leaders: leader_sender,
            results: result_sender,
        };
        let node = service.clone();
        let (url, server_task) = runtime.block_on(async move {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server_task = tokio::spawn(async move {
                tonic::transport::Server::builder()
                    .add_service(BamNodeApiServer::new(node))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            (url, server_task)
        });
        Self {
            url,
            service,
            leaders,
            results,
            server_task,
            _runtime: runtime,
        }
    }
}

impl Drop for BamServer {
    fn drop(&mut self) {
        self.server_task.abort();
    }
}

/// Signed BAM network ingress must cross the bridge, real external client and real
/// Bank execution before its committed reply and RPC confirmation are observed.
#[test]
#[serial]
fn test_jito_scheduler_bindings_authenticated_bam_commit() {
    agave_logger::setup_with_default(RUST_LOG_FILTER);
    let validator_config = ValidatorConfig {
        jito_scheduler_bindings: true,
        ..ValidatorConfig::default_for_test()
    };
    let bam_url = validator_config.bam_url.clone();
    let cluster = LocalCluster::new(
        &mut ClusterConfig {
            node_stakes: vec![DEFAULT_NODE_STAKE],
            mint_lamports: DEFAULT_MINT_LAMPORTS,
            validator_configs: make_identical_validator_configs(&validator_config, 1),
            ticks_per_slot: 16,
            ..ClusterConfig::default()
        },
        SocketAddrSpace::Unspecified,
    );
    let identity = *cluster.entry_point_info.pubkey();
    let server = BamServer::start(identity);
    let ipc_path = cluster.validators[&identity]
        .info
        .ledger_path
        .join("scheduler_bindings.ipc");
    let mut external = RunningScheduler::attach_with_bam(&ipc_path, &bam_url, server.url.clone());
    // The shared progress above confirms Connected and atomic readiness. Observe a
    // fresh leader announcement on the authenticated stream before choosing its slot limit.
    while server.leaders.try_recv().is_ok() {}
    let leader = server
        .leaders
        .recv_timeout(Duration::from_secs(10))
        .unwrap();
    assert!(leader.slot_cu_budget_remaining > 0);
    assert_eq!(server.service.authenticated.load(Ordering::Relaxed), 1);
    let rpc = cluster
        .build_rpc_client_with_commitment(&identity, CommitmentConfig::confirmed())
        .unwrap();
    let recipient = Pubkey::new_unique();
    let amount = 1_000_000;
    let payer_balance = rpc.get_balance(&cluster.funding_keypair.pubkey()).unwrap();
    let transaction = system_transaction::transfer(
        &cluster.funding_keypair,
        &recipient,
        amount,
        rpc.get_latest_blockhash().unwrap(),
    );
    let signature = transaction.signatures[0];
    let fee = rpc.get_fee_for_message(&transaction.message).unwrap();
    let data = wincode::serialize(&transaction).unwrap();
    let seq_id = 73;
    *server.service.batch.lock().unwrap() = Some(AtomicTxnBatch {
        seq_id,
        max_schedule_slot: leader.slot + 128,
        packets: vec![Packet {
            meta: Some(Meta {
                size: u64::try_from(data.len()).unwrap(),
                flags: Some(PacketFlags {
                    simple_vote_tx: false,
                    revert_on_error: true,
                }),
            }),
            data: data.into(),
        }],
    });
    let result = server
        .results
        .recv_timeout(Duration::from_secs(20))
        .unwrap();
    assert_eq!(result.seq_id, seq_id);
    let Some(atomic_txn_batch_result::Result::Committed(committed)) = result.result else {
        panic!("BAM atomic transfer was not committed: {result:?}");
    };
    assert_eq!(committed.transaction_results.len(), 1);
    let metadata = &committed.transaction_results[0];
    assert!(metadata.execution_success);
    assert!(metadata.cus_consumed > 0);
    assert_eq!(
        metadata.feepayer_balance_lamports,
        payer_balance - amount - fee
    );
    // Never submit through RPC: this signature has only one ingress, the BAM stream.
    let deadline = Instant::now() + Duration::from_secs(45);
    loop {
        if let Some(result) = rpc
            .get_signature_status_with_commitment(&signature, CommitmentConfig::confirmed())
            .unwrap()
        {
            result.unwrap();
            assert_eq!(rpc.get_balance(&recipient).unwrap(), amount);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "BAM transaction {signature} was not confirmed"
        );
        sleep(Duration::from_millis(50));
    }
    let stats = external.stop();
    assert!(
        stats.received_transactions > 0
            && stats.submitted_batches > 0
            && stats.completed_batches > 0
    );
    assert_eq!(server.service.authenticated.load(Ordering::Relaxed), 1);
}
