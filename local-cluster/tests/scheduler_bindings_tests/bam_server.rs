use {
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
            BuilderHeartBeat, LeaderState, MultipleAtomicTxnBatch, Socket,
        },
    },
    solana_core::bam_connection::BamConnection,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    solana_signer::Signer,
    std::{
        collections::HashSet,
        str::FromStr,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime},
    },
    tokio::{runtime::Runtime, sync::mpsc},
    tokio_stream::wrappers::{ReceiverStream, TcpListenerStream},
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

#[derive(Clone)]
pub(super) struct AuthenticatedBam {
    identity: Pubkey,
    challenges: Arc<Mutex<HashSet<String>>>,
    pub(super) authenticated: Arc<AtomicUsize>,
    pub(super) batch: Arc<Mutex<Option<AtomicTxnBatch>>>,
    pub(super) leaders: Sender<LeaderState>,
    pub(super) results: Sender<AtomicTxnBatchResult>,
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

pub(super) struct BamServer {
    pub(super) url: String,
    pub(super) service: AuthenticatedBam,
    pub(super) leaders: Receiver<LeaderState>,
    pub(super) results: Receiver<AtomicTxnBatchResult>,
    server_task: tokio::task::JoinHandle<()>,
    _runtime: Runtime,
}

impl BamServer {
    pub(super) fn start(identity: Pubkey) -> Self {
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
