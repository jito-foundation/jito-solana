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
            HeartbeatEvent, ProxyError,
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
    tokio::{
        runtime::Runtime,
        time::{interval, sleep},
    },
    tonic::{
        codegen::InterceptedService,
        transport::{Channel, Endpoint},
        Streaming,
    },
};

pub struct RelayerStage {
    t_hdls: Vec<JoinHandle<()>>,
}

#[derive(Clone, Debug)]
pub struct RelayerConfig {
    /// Address to the external auth-service responsible for generating access tokens.
    pub auth_service_endpoint: Endpoint,

    /// Primary backend endpoint.
    pub backend_endpoint: Endpoint,

    /// Interval at which heartbeats are expected.
    pub expected_heartbeat_interval: Duration,

    /// The max tolerable age of the last heartbeat.
    pub oldest_allowed_heartbeat: Duration,

    /// If set then it will be assumed the backend verified packets so signature verification will be bypassed in the validator.
    pub trust_packets: bool,
}

impl RelayerStage {
    pub fn new(
        relayer_config: RelayerConfig,
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
        let RelayerConfig {
            auth_service_endpoint,
            backend_endpoint,
            expected_heartbeat_interval,
            oldest_allowed_heartbeat,
            trust_packets,
        } = relayer_config;

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
            let exit = exit.clone();
            let rt = rt.clone();

            t_hdls.push(
                Builder::new()
                    .name("relayer-auth-update-thread".into())
                    .spawn(move || {
                        rt.block_on(async move {
                            auth_tokens_update_loop(
                                auth_service_endpoint,
                                access_token,
                                cluster_info.clone(),
                                Duration::from_secs(1),
                                exit,
                            )
                            .await
                        })
                    })
                    .unwrap(),
            );
        }

        t_hdls.push(Self::start(
            rt,
            access_token,
            heartbeat_tx,
            expected_heartbeat_interval,
            oldest_allowed_heartbeat,
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

    #[allow(clippy::too_many_arguments)]
    fn start(
        rt: Arc<Runtime>,
        access_token: Arc<Mutex<Token>>,
        heartbeat_tx: Sender<HeartbeatEvent>,
        expected_heartbeat_interval: Duration,
        oldest_allowed_heartbeat: Duration,
        packet_tx: Sender<PacketBatch>,
        relayer_endpoint: Endpoint,
        verified_packet_tx: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        trust_packets: bool,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("jito-relayer-thread".into())
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
                        match relayer_endpoint.connect().await {
                            Ok(channel) => {
                                match Self::start_consuming_relayer_packets(
                                    &mut backoff,
                                    RelayerClient::with_interceptor(
                                        channel,
                                        AuthInterceptor::new(access_token.clone()),
                                    ),
                                    &heartbeat_tx,
                                    expected_heartbeat_interval,
                                    oldest_allowed_heartbeat,
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
        expected_heartbeat_interval: Duration,
        oldest_allowed_heartbeat: Duration,
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
            expected_heartbeat_interval,
            oldest_allowed_heartbeat,
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
        expected_heartbeat_interval: Duration,
        oldest_allowed_heartbeat: Duration,
        mut packet_stream: Streaming<relayer::SubscribePacketsResponse>,
        packet_tx: &Sender<PacketBatch>,
        trust_packets: bool,
        verified_packet_tx: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        exit: &Arc<AtomicBool>,
    ) -> crate::proxy::Result<()> {
        info!("relayer starting bundle and packet stream");

        let mut heartbeat_check_interval = interval(expected_heartbeat_interval);
        let mut last_heartbeat_ts = Instant::now();

        while !exit.load(Ordering::Relaxed) {
            tokio::select! {
                maybe_msg = packet_stream.message() => {
                    let resp = maybe_msg?.ok_or(ProxyError::GrpcStreamDisconnected)?;
                    Self::handle_relayer_packets(resp, heartbeat_event, heartbeat_tx, &mut last_heartbeat_ts, packet_tx, trust_packets, verified_packet_tx)?;
                }
                _ = heartbeat_check_interval.tick() => {
                    if last_heartbeat_ts.elapsed() > oldest_allowed_heartbeat {
                        return Err(ProxyError::HeartbeatExpired);
                    }
                }
            }
        }

        Ok(())
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
            None => {
                warn!("Received an empty subscribe_packets msg.");
            }
            Some(relayer::subscribe_packets_response::Msg::Batch(proto_batch)) => {
                // info!("packet batch received");

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
                // debug!("heartbeat received");

                *last_heartbeat_ts = Instant::now();
                heartbeat_tx
                    .send(heartbeat_event)
                    .map_err(|_| ProxyError::HeartbeatChannelError)?;
            }
        }
        Ok(())
    }
}
