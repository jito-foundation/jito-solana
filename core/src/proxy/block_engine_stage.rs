//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - Acts as a system that sends high profit bundles and transactions to a validator.
//! - Sends transactions and bundles to the validator.

use {
    crate::{
        backoff::BackoffStrategy,
        packet_bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{token_manager::auth_tokens_update_loop, AuthInterceptor},
            ProxyError,
        },
        sigverify::SigverifyTracerPacketStats,
    },
    crossbeam_channel::Sender,
    jito_protos::proto::{
        auth::Token,
        block_engine::{self, block_engine_validator_client::BlockEngineValidatorClient},
    },
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    std::{
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    tokio::{runtime::Runtime, time::sleep},
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Status, Streaming,
    },
    uuid::Uuid,
};

pub struct BlockEngineStage {
    t_hdls: Vec<JoinHandle<()>>,
}

#[derive(Clone, Debug)]
pub struct BlockEngineConfig {
    /// Address to the external auth-service responsible for generating access tokens.
    pub auth_service_endpoint: Endpoint,

    /// Primary backend endpoint.
    pub backend_endpoint: Endpoint,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

impl BlockEngineStage {
    pub fn new(
        block_engine_config: BlockEngineConfig,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that non-trusted packets get piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted packets get piped through.
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let BlockEngineConfig {
            auth_service_endpoint,
            backend_endpoint,
            trust_packets,
        } = block_engine_config;

        let mut t_hdls = vec![];
        let access_token = Arc::new(Mutex::new(Token::default()));
        let rt = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap(),
        );

        {
            let access_token = access_token.clone();
            let rt = rt.clone();
            let exit = exit.clone();

            t_hdls.push(
                Builder::new()
                    .name("block-engine-auth-update-thread".into())
                    .spawn(move || {
                        rt.block_on(async move {
                            auth_tokens_update_loop(
                                auth_service_endpoint,
                                access_token,
                                cluster_info,
                                Duration::from_secs(1),
                                exit,
                            )
                            .await
                        });
                    })
                    .unwrap(),
            );
        }

        t_hdls.push(Self::start(
            rt,
            access_token,
            backend_endpoint,
            bundle_tx,
            packet_tx,
            trust_packets,
            verified_packet_tx,
            exit,
        ));

        Self { t_hdls }
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn start(
        rt: Arc<Runtime>,
        access_token: Arc<Mutex<Token>>,
        block_engine_endpoint: Endpoint,
        bundle_tx: Sender<Vec<PacketBundle>>,
        packet_tx: Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-block-engine-thread".into())
            .spawn(move || {
                rt.block_on(async move {
                    while access_token.lock().unwrap().value.is_empty() {
                        if exit.load(Ordering::Relaxed) {
                            return;
                        }

                        sleep(Duration::from_millis(500)).await;
                    }

                    let mut backoff = BackoffStrategy::new();
                    while !exit.load(Ordering::Relaxed) {
                        match block_engine_endpoint.connect().await {
                            Ok(channel) => {
                                match Self::start_consuming_block_engine_bundles_and_packets(
                                    &mut backoff,
                                    &bundle_tx,
                                    BlockEngineValidatorClient::with_interceptor(
                                        channel,
                                        AuthInterceptor::new(access_token.clone()),
                                    ),
                                    &packet_tx,
                                    trust_packets,
                                    &verified_packet_tx,
                                    &exit,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error consuming block-engine stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to block-engine: {:?}", e);
                            }
                        }

                        sleep(Duration::from_millis(backoff.next_wait())).await;
                    }
                });
            })
            .unwrap()
    }

    async fn start_consuming_block_engine_bundles_and_packets(
        backoff: &mut BackoffStrategy,
        bundle_tx: &Sender<Vec<PacketBundle>>,
        mut client: BlockEngineValidatorClient<InterceptedService<Channel, AuthInterceptor>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        let subscribe_packets_stream = client
            .subscribe_packets(block_engine::SubscribePacketsRequest {})
            .await?
            .into_inner();
        let subscribe_bundles_stream = client
            .subscribe_bundles(block_engine::SubscribeBundlesRequest {})
            .await?
            .into_inner();

        backoff.reset();

        Self::consume_bundle_and_packet_stream(
            (subscribe_bundles_stream, subscribe_packets_stream),
            bundle_tx,
            packet_tx,
            trust_packets,
            verified_packet_tx,
            exit,
        )
        .await
    }

    async fn consume_bundle_and_packet_stream(
        (mut bundle_stream, mut packet_stream): (
            Streaming<block_engine::SubscribeBundlesResponse>,
            Streaming<block_engine::SubscribePacketsResponse>,
        ),
        bundle_tx: &Sender<Vec<PacketBundle>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        info!("Starting bundle and packet stream consumer.");

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_block_engine_packets(resp, packet_tx, verified_packet_tx, trust_packets)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_tx)?;
                }
            }
        }

        Ok(())
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: Result<Option<block_engine::SubscribeBundlesResponse>, Status>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
    ) -> crate::proxy::Result<()> {
        let bundles_response = maybe_bundles_response?.ok_or(ProxyError::GrpcStreamDisconnected)?;
        info!("received block-engine bundles");
        let bundles: Vec<PacketBundle> = bundles_response
            .bundles
            .into_iter()
            .filter_map(|bundle| {
                Some(PacketBundle {
                    batch: PacketBatch::new(
                        bundle
                            .bundle?
                            .packets
                            .into_iter()
                            .map(proto_packet_to_packet)
                            .collect(),
                    ),
                    uuid: Uuid::from_str(&bundle.uuid).ok()?,
                })
            })
            .collect();
        bundle_sender
            .send(bundles)
            .map_err(|_| ProxyError::PacketForwardError)
    }

    fn handle_block_engine_packets(
        resp: block_engine::SubscribePacketsResponse,
        packet_tx: &Sender<PacketBatch>,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
    ) -> crate::proxy::Result<()> {
        if let Some(batch) = resp.batch {
            info!("received block-engine packets");
            let packet_batch = PacketBatch::new(
                batch
                    .packets
                    .into_iter()
                    .map(proto_packet_to_packet)
                    .collect(),
            );
            if trust_packets {
                verified_packet_tx
                    .send((vec![packet_batch], None))
                    .map_err(|_| ProxyError::PacketForwardError)?;
            } else {
                packet_tx
                    .send(packet_batch)
                    .map_err(|_| ProxyError::PacketForwardError)?;
            }
        } else {
            warn!("received empty packet batch");
        }

        Ok(())
    }
}
