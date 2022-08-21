//! Maintains a connection to the Block Engine.
//!
//! The Block Engine is responsible for the following:
//! - acts as a system that sends high profit bundles and transactions to a validator.
//! - sends transactions and bundles to the validator.
//! - when validator connects, it doesn't touch the TPU and TPU forward addresses.
//! - expected to send heartbeat to the validator as a watchdog. if watchdog times out, the validator
//!   disconnects and reconnects.

use {
    crate::{
        backoff::BackoffStrategy,
        bundle::PacketBundle,
        proto_packet_to_packet,
        proxy::{
            auth::{token_manager::auth_tokens_update_loop, AuthInterceptor},
            BackendConfig, HeartbeatEvent, ProxyError,
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
        net::{IpAddr, Ipv4Addr, SocketAddr},
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    tokio::time::{interval, sleep},
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

impl BlockEngineStage {
    pub fn new(
        backend_config: BackendConfig,
        // Channel that bundles get piped through.
        bundle_tx: Sender<Vec<PacketBundle>>,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Pass in Some sender to get notified on heartbeats received from the block-engine.
        maybe_heartbeat_tx: Option<Sender<HeartbeatEvent>>,
        // Channel that non-trusted packets get piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted packets get piped through.
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let BackendConfig {
            auth_service_endpoint,
            backend_endpoint,
            trust_packets,
        } = backend_config;

        let mut t_hdls = vec![];
        let access_token = Arc::new(Mutex::new(Token::default()));

        {
            let access_token = access_token.clone();
            let exit = exit.clone();
            t_hdls.push(
                Builder::new()
                    .name("block-engine-auth-update-thread".into())
                    .spawn(move || {
                        auth_tokens_update_loop(
                            auth_service_endpoint,
                            access_token,
                            cluster_info,
                            Duration::from_secs(1),
                            exit,
                        )
                    })
                    .unwrap(),
            );
        }
        t_hdls.push(Self::start(
            access_token,
            backend_endpoint,
            bundle_tx,
            maybe_heartbeat_tx,
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
        access_token: Arc<Mutex<Token>>,
        block_engine_endpoint: Endpoint,
        bundle_tx: Sender<Vec<PacketBundle>>,
        maybe_heartbeat_tx: Option<Sender<HeartbeatEvent>>,
        packet_tx: Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-block-engine-thread".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    while access_token.lock().unwrap().value.is_empty() {
                        sleep(Duration::from_millis(500)).await;
                    }

                    let mut backoff = BackoffStrategy::new();
                    loop {
                        match block_engine_endpoint.connect().await {
                            Ok(channel) => {
                                match Self::start_consuming_block_engine_bundles_and_packets(
                                    &mut backoff,
                                    &bundle_tx,
                                    BlockEngineValidatorClient::with_interceptor(
                                        channel,
                                        AuthInterceptor::new(access_token.clone()),
                                    ),
                                    &maybe_heartbeat_tx,
                                    &packet_tx,
                                    trust_packets,
                                    &verified_packet_tx,
                                    &exit,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error in block engine stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to block engine: {:?}", e);
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
        maybe_heartbeat_tx: &Option<Sender<HeartbeatEvent>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        let maybe_heartbeat_event: Option<HeartbeatEvent> = if maybe_heartbeat_tx.is_some() {
            let tpu_config = client
                .get_tpu_configs(block_engine::GetTpuConfigsRequest {})
                .await?
                .into_inner();
            let tpu_addr = tpu_config
                .tpu
                .ok_or_else(|| ProxyError::MissingTpuSocket("tpu".into()))?;
            let tpu_forward_addr = tpu_config
                .tpu_forward
                .ok_or_else(|| ProxyError::MissingTpuSocket("tpu_fwd".into()))?;

            let tpu_ip = IpAddr::from(tpu_addr.ip.parse::<Ipv4Addr>()?);
            let tpu_forward_ip = IpAddr::from(tpu_forward_addr.ip.parse::<Ipv4Addr>()?);

            let tpu_socket = SocketAddr::new(tpu_ip, tpu_addr.port as u16);
            let tpu_forward_socket = SocketAddr::new(tpu_forward_ip, tpu_forward_addr.port as u16);
            Some((tpu_socket, tpu_forward_socket))
        } else {
            None
        };

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
            maybe_heartbeat_event,
            maybe_heartbeat_tx,
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
        maybe_heartbeat_event: Option<HeartbeatEvent>,
        maybe_heartbeat_tx: &Option<Sender<HeartbeatEvent>>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        let mut heartbeat_check_tick = interval(Duration::from_millis(500));
        let mut last_heartbeat_ts = Instant::now();

        info!("block engine starting bundle and packet stream");
        loop {
            if exit.load(Ordering::Relaxed) {
                return Err(ProxyError::Shutdown);
            }

            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_block_engine_packets(resp, &mut last_heartbeat_ts, &maybe_heartbeat_event, maybe_heartbeat_tx, packet_tx,  verified_packet_tx, trust_packets)?;
                }
                maybe_bundles = bundle_stream.message() => {
                    Self::handle_block_engine_maybe_bundles(maybe_bundles, bundle_tx)?;
                }
                _ = heartbeat_check_tick.tick() => {
                    if last_heartbeat_ts.elapsed() > Duration::from_millis(1_500) {
                        return Err(ProxyError::HeartbeatExpired);
                    }
                }
            }
        }
    }

    fn handle_block_engine_maybe_bundles(
        maybe_bundles_response: Result<Option<block_engine::SubscribeBundlesResponse>, Status>,
        bundle_sender: &Sender<Vec<PacketBundle>>,
    ) -> crate::proxy::Result<()> {
        let bundles_response = maybe_bundles_response?.ok_or(ProxyError::GrpcStreamDisconnected)?;
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
        subscribe_packets_resp: block_engine::SubscribePacketsResponse,
        last_heartbeat_ts: &mut Instant,
        maybe_heartbeat_event: &Option<HeartbeatEvent>,
        maybe_heartbeat_tx: &Option<Sender<HeartbeatEvent>>,
        packet_tx: &Sender<PacketBatch>,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
    ) -> crate::proxy::Result<()> {
        match subscribe_packets_resp.msg {
            // TODO: rethink this error
            None => return Err(ProxyError::GrpcStreamDisconnected),
            Some(block_engine::subscribe_packets_response::Msg::Batch(proto_batch)) => {
                let packet_batch = PacketBatch::new(
                    proto_batch
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
            }
            Some(block_engine::subscribe_packets_response::Msg::Heartbeat(_)) => {
                *last_heartbeat_ts = Instant::now();
                if let Some(heartbeat_tx) = maybe_heartbeat_tx {
                    heartbeat_tx
                        .send((*maybe_heartbeat_event).unwrap())
                        .map_err(|_| ProxyError::HeartbeatChannelError)?;
                }
            }
        }
        Ok(())
    }
}
