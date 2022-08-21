//! Maintains a connection to the Relayer.
//!
//! The external Relayer is responsible for the following:
//! - Acts as a TPU proxy.
//! - Sends transactions to the validator.
//! - Does not bundles to avoid DOS vector.
//! - When validator connects, it changes its TPU and TPU forward address to the relayer.
//! - Expected to send heartbeat to validator as watchdog. If watchdog times out, the validator
//!   disconnects and reverts the TPU and TPU forward settings.

use {
    crate::{
        backoff::BackoffStrategy,
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
        relayer::{self, relayer_client::RelayerClient},
    },
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_perf::packet::PacketBatch,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
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
        Streaming,
    },
};

pub struct RelayerStage {
    t_hdls: Vec<JoinHandle<()>>,
}

impl RelayerStage {
    pub fn new(
        backend_config: BackendConfig,
        // The keypair stored here is used to sign auth challenges.
        cluster_info: Arc<ClusterInfo>,
        // Channel that server-sent heartbeats are piped through.
        heartbeat_tx: Sender<HeartbeatEvent>,
        // Channel that non-trusted streamed packets are piped through.
        packet_tx: Sender<PacketBatch>,
        // Channel that trusted streamed packets are piped through.
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
                    .name("relayer-auth-update-thread".into())
                    .spawn(move || {
                        auth_tokens_update_loop(
                            auth_service_endpoint,
                            access_token,
                            cluster_info.clone(),
                            Duration::from_secs(1),
                            exit,
                        )
                    })
                    .unwrap(),
            );
        }

        t_hdls.push(Self::start(
            access_token,
            heartbeat_tx,
            packet_tx,
            backend_endpoint,
            verified_packet_tx,
            trust_packets,
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

    fn start(
        access_token: Arc<Mutex<Token>>,
        heartbeat_tx: Sender<HeartbeatEvent>,
        packet_tx: Sender<PacketBatch>,
        relayer_endpoint: Endpoint,
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-relayer-thread".into())
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
                        match relayer_endpoint.connect().await {
                            Ok(channel) => {
                                match Self::start_consuming_relayer_packets(
                                    &mut backoff,
                                    RelayerClient::with_interceptor(
                                        channel,
                                        AuthInterceptor::new(access_token.clone()),
                                    ),
                                    &heartbeat_tx,
                                    &packet_tx,
                                    &verified_packet_tx,
                                    trust_packets,
                                    &exit,
                                )
                                .await
                                {
                                    Ok(_) => {}
                                    Err(e) => {
                                        error!("error in relayer stream: {:?}", e);
                                    }
                                }
                            }
                            Err(e) => {
                                error!("error connecting to relayer: {:?}", e);
                            }
                        }
                        sleep(Duration::from_millis(backoff.next_wait())).await;
                    }
                });
            })
            .unwrap()
    }

    async fn start_consuming_relayer_packets(
        backoff: &mut BackoffStrategy,
        mut client: RelayerClient<InterceptedService<Channel, AuthInterceptor>>,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        packet_tx: &Sender<PacketBatch>,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        let heartbeat_event: HeartbeatEvent = {
            let tpu_config = client
                .get_tpu_configs(relayer::GetTpuConfigsRequest {})
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
            (tpu_socket, tpu_forward_socket)
        };

        let packet_stream = client
            .subscribe_packets(relayer::SubscribePacketsRequest {})
            .await?
            .into_inner();

        // assume it's all good here
        backoff.reset();

        Self::consume_packet_stream(
            heartbeat_event,
            heartbeat_tx,
            packet_stream,
            packet_tx,
            trust_packets,
            verified_packet_tx,
            exit,
        )
        .await
    }

    async fn consume_packet_stream(
        heartbeat_event: HeartbeatEvent,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        mut packet_stream: Streaming<relayer::SubscribePacketsResponse>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        info!("relayer starting bundle and packet stream");

        let mut heartbeat_check_tick = interval(Duration::from_millis(500));
        let mut last_heartbeat_ts = Instant::now();

        loop {
            if exit.load(Ordering::Relaxed) {
                // TODO: Is this an error?
                return Err(ProxyError::Shutdown);
            }

            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_relayer_packets(resp, heartbeat_event, heartbeat_tx, &mut last_heartbeat_ts, packet_tx, trust_packets, verified_packet_tx)?;
                }
                _ = heartbeat_check_tick.tick() => {
                    if last_heartbeat_ts.elapsed() > Duration::from_millis(1_500) {
                        return Err(ProxyError::HeartbeatExpired);
                    }
                }
            }
        }
    }

    fn handle_relayer_packets(
        subscribe_packets_resp: relayer::SubscribePacketsResponse,
        heartbeat_event: HeartbeatEvent,
        heartbeat_tx: &Sender<HeartbeatEvent>,
        last_heartbeat_ts: &mut Instant,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
    ) -> crate::proxy::Result<()> {
        match subscribe_packets_resp.msg {
            // TODO: This error doesn't make sense.
            None => return Err(ProxyError::GrpcStreamDisconnected),
            Some(relayer::subscribe_packets_response::Msg::Batch(proto_batch)) => {
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
            Some(relayer::subscribe_packets_response::Msg::Heartbeat(_)) => {
                *last_heartbeat_ts = Instant::now();

                heartbeat_tx
                    .send(heartbeat_event)
                    .map_err(|_| ProxyError::HeartbeatChannelError)?;
            }
        }
        Ok(())
    }
}
