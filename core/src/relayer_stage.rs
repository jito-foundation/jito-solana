//! The `relayer_stage` maintains connections to relayers and block engines.
//!
//! Relayer:
//! - acts as a TPU proxy.
//! - sends transactions to the validator
//! - do not support bundles to avoid DOS vector.
//! - when validator connects, it changes its TPU and TPU forward address to the relayer.
//! - expected to send heartbeat to validator as watchdog. if watchdog times out, the validator
//!   disconnects and reverts the TPU and TPU forward settings
//!
//! Block Engines:
//! - acts as a system that sends high profit bundles and transactions to a validator.
//! - sends transactions and bundles to the validator.
//! - when validator connects, it doesn't touch the TPU and TPU forward addresses.
//! - expected to send heartbeat to the validator as a watchdog. if watchdog times out, the validator
//!   disconnects and reconnects.
//!
//! If the block engine and relayer address are the same or only a block engine address is provided,
//! it also serves the same functionality as a relayer.

use {
    crate::{
        backoff::BackoffStrategy,
        blocking_proxy_client::{AuthInterceptor, BlockingProxyClient, ProxyError},
        bundle::PacketBundle,
        proto_packet_to_packet,
        sigverify::SigverifyTracerPacketStats,
    },
    crossbeam_channel::{select, tick, unbounded, Receiver, RecvError, Sender},
    jito_protos::proto::validator_interface::{
        packet_stream_msg::Msg, PacketStreamMsg, SubscribeBundlesResponse,
    },
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_metrics::datapoint_info,
    solana_perf::packet::PacketBatch,
    solana_sdk::{signature::Signature, signer::Signer},
    std::{
        net::SocketAddr,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tokio::time::Instant,
    tonic::Status,
    uuid::Uuid,
};

pub struct RelayerStage {
    t_hdls: Vec<JoinHandle<()>>,
}

#[derive(Error, Debug)]
pub enum RelayerStageError {
    #[error("proxy error: {0}")]
    ProxyError(#[from] ProxyError),
    #[error("grpc error: {0}")]
    GrpcError(#[from] Status),
    #[error("stream disconnected")]
    GrpcStreamDisconnected,
    #[error("bad packet message")]
    BadMessage,
    #[error("error sending message to another part of the system")]
    ChannelError,
    #[error("backend sent disconnection through heartbeat")]
    HeartbeatError,
    #[error("missed heartbeat, but failed to forward disconnect message")]
    HeartbeatChannelError,
    #[error("error forwarding packet to banking stage")]
    PacketForwardError,
    #[error("relayer configuration is invalid error: {0:?}")]
    InvalidRelayerConfig(String),
}

type Result<T> = std::result::Result<T, RelayerStageError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);
type SubscribePacketsResult = std::result::Result<Option<PacketStreamMsg>, Status>;

const HEARTBEAT_TIMEOUT_MS: Duration = Duration::from_millis(1500); // Empirically determined from load testing
const DISCONNECT_DELAY_SEC: Duration = Duration::from_secs(60);
const METRICS_CADENCE_SEC: Duration = Duration::from_secs(1);
const METRICS_NAME: &str = "mev_stage";

impl RelayerStage {
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        relayer_address: String,
        block_engine_address: String,
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        bundle_sender: Sender<Vec<PacketBundle>>,
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let msg = b"Let's get this money!".to_vec();
        let keypair = cluster_info.keypair();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthInterceptor::new(msg, sig, pubkey);

        let (tpu_proxy_heartbeat_sender, tpu_proxy_heartbeat_receiver) = unbounded();

        let mut t_hdls = vec![];

        t_hdls.extend(Self::spawn_relayer_threads(
            relayer_address,
            block_engine_address,
            interceptor,
            verified_packet_sender,
            // if no validator interface address provided, sender side of the channel gets dropped
            // in heartbeat thread and causes packet forwarding to error. this clone, along with the
            // reference on self, prevents it from being dropped
            tpu_proxy_heartbeat_sender,
            bundle_sender,
            exit.clone(),
        ));

        t_hdls.push(Self::heartbeat_thread(
            packet_intercept_receiver,
            packet_sender,
            tpu_proxy_heartbeat_receiver,
            cluster_info,
            exit,
        ));

        info!("started mev stage");

        Self { t_hdls }
    }

    fn spawn_relayer_threads(
        relayer_address: String,
        block_engine_address: String,
        interceptor: AuthInterceptor,
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: Sender<HeartbeatEvent>,
        bundle_sender: Sender<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        if relayer_address == block_engine_address || relayer_address.is_empty() {
            // block engine is also a relayer
            // the block engine can send packets and bundles
            // the block engine heartbeat controls the TPU address
            vec![Self::start_proxy_thread(
                block_engine_address,
                interceptor,
                verified_packet_sender,
                Some(tpu_proxy_heartbeat_sender),
                Some(bundle_sender),
                exit,
            )]
        } else {
            // the relayer acts as the TPU proxy and the block engine sends bundles
            // both the relayer and block engine can send packets
            // only the relayer heartbeats and controls the TPU address
            // only the block engine supports bundles
            vec![
                (relayer_address, Some(tpu_proxy_heartbeat_sender), None),
                (block_engine_address, None, Some(bundle_sender)),
            ]
            .into_iter()
            .map(
                |(proxy_addr, maybe_heartbeat_sender, maybe_bundle_sender)| {
                    Self::start_proxy_thread(
                        proxy_addr,
                        interceptor.clone(),
                        verified_packet_sender.clone(),
                        maybe_heartbeat_sender,
                        maybe_bundle_sender,
                        exit.clone(),
                    )
                },
            )
            .collect::<Vec<JoinHandle<()>>>()
        }
    }

    fn start_proxy_thread(
        address: String,
        mut interceptor: AuthInterceptor,
        verified_packet_sender: Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: Option<Sender<HeartbeatEvent>>,
        bundle_sender: Option<Sender<Vec<PacketBundle>>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("proxy_thread".into())
            .spawn(move || {
                if !address.contains("http") {
                    error!("missing or malformed mev proxy address provided, exiting mev loop [address={}]", address);
                    datapoint_info!(METRICS_NAME, ("bad_proxy_addr", 1, i64));
                    return;
                }

                let mut backoff = BackoffStrategy::new();

                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    let mut client = match BlockingProxyClient::new(address.clone(), interceptor.clone()) {
                        Ok(client) => client,
                        Err(e) => {
                            error!("error connecting to proxy: {:?}", e);
                            datapoint_info!(
                                METRICS_NAME,
                                ("proxy_connection_error", 1, i64),
                                ("addr", address, String)
                            );

                            thread::sleep(Duration::from_millis(backoff.next_wait()));
                            continue;
                        }
                    };

                    if let Err(e) = Self::start_stream(
                        &mut client,
                        &verified_packet_sender,
                        &bundle_sender,
                        &tpu_proxy_heartbeat_sender,
                        &mut backoff,
                        &exit,
                    ) {
                        if let Some(token) = BlockingProxyClient::maybe_retryable_auth(&e, interceptor.get_msg()) {
                            interceptor.set_msg(token.as_bytes().to_vec());

                            info!("retrying auth with new token");
                            datapoint_info!(
                                METRICS_NAME,
                                ("retryable_auth", 1, i64),
                                ("addr", address, String)
                            );
                        } else {
                            error!("error starting proxy stream: {:?}", e);
                            datapoint_info!(
                                METRICS_NAME,
                                ("proxy_stream_error", 1, i64),
                                ("addr", address, String)
                            );

                            thread::sleep(Duration::from_millis(backoff.next_wait()));
                        }
                    }
                }
            })
            .unwrap()
    }

    /// This thread is responsible for connecting and disconnecting the fetch stage to prevent
    /// circumventing TPU proxy.
    ///
    /// Disconnect fetch behaviour
    /// Starts connected
    /// When connected and a packet is received, forward it
    /// When disconnected, packet is dropped
    /// When receiving heartbeat while connected and not pending disconnect
    ///      Sets pending_disconnect to true and records time
    /// When receiving heartbeat while connected, and pending for > DISCONNECT_DELAY_SEC
    ///      Sets fetch_connected to false, pending_disconnect to false
    ///      Advertises TPU ports sent in heartbeat
    /// When tick is received without heartbeat_received
    ///      Sets fetch_connected to true, pending_disconnect to false
    ///      Advertises saved contact info
    fn heartbeat_thread(
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
        tpu_proxy_heartbeat_receiver: Receiver<HeartbeatEvent>,
        cluster_info: &Arc<ClusterInfo>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let cluster_info = cluster_info.clone();
        thread::Builder::new()
            .name("heartbeat_thread".into())
            .spawn(move || {
                let saved_contact_info = cluster_info.my_contact_info();
                let mut fetch_connected = true;
                let mut heartbeat_received = false;
                let mut pending_disconnect = false;
                // initialized to avoid compiler errors but overwritten before read
                let mut pending_disconnect_ts = Instant::now();
                let heartbeat_tick = tick(HEARTBEAT_TIMEOUT_MS);
                let metrics_tick = tick(METRICS_CADENCE_SEC);
                let mut packets_forwarded = 0;
                let mut heartbeats_received = 0;
                loop {
                    select! {
                        recv(packet_intercept_receiver) -> pkt => {
                            match pkt {
                                Ok(pkt) => {
                                    if fetch_connected {
                                        if packet_sender.send(pkt).is_err() {
                                            error!("{:?}", RelayerStageError::PacketForwardError);
                                            return;
                                        }
                                        packets_forwarded += 1;
                                    }
                                }
                                Err(_) => {
                                    warn!("packet intercept receiver disconnected, shutting down");
                                    return;
                                }
                            }
                        }
                        recv(heartbeat_tick) -> _ => {
                            if exit.load(Ordering::Relaxed) {
                                break;
                            }
                            if !heartbeat_received && (!fetch_connected || pending_disconnect) {
                                warn!("heartbeat late, reconnecting fetch stage");
                                fetch_connected = true;
                                pending_disconnect = false;
                                Self::set_tpu_addresses(&cluster_info, saved_contact_info.tpu, saved_contact_info.tpu_forwards);
                                heartbeats_received = 0;
                            }
                            heartbeat_received = false;
                        }
                        recv(tpu_proxy_heartbeat_receiver) -> tpu_info => {
                            if let Ok((tpu_addr, tpu_forward_addr)) = tpu_info {
                                heartbeats_received += 1;
                                heartbeat_received = true;
                                if fetch_connected && !pending_disconnect {
                                    info!("received heartbeat while fetch stage connected, pending disconnect after delay");
                                    pending_disconnect_ts = Instant::now();
                                    pending_disconnect = true;
                                }
                                if fetch_connected && pending_disconnect && pending_disconnect_ts.elapsed() > DISCONNECT_DELAY_SEC {
                                    info!("disconnecting fetch stage");
                                    fetch_connected = false;
                                    pending_disconnect = false;
                                    Self::set_tpu_addresses(&cluster_info, tpu_addr, tpu_forward_addr);
                                }
                            } else {
                                // see comment on heartbeat_sender clone in new()
                                unreachable!();
                            }
                        }
                        recv(metrics_tick) -> _ => {
                            datapoint_info!(
                                METRICS_NAME,
                                ("fetch_stage_packets_forwarded", packets_forwarded, i64),
                                ("heartbeats_received", heartbeats_received, i64),
                            );
                        }
                    }
                }
            }).unwrap()
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) {
        let mut new_contact_info = cluster_info.my_contact_info();
        new_contact_info.tpu = tpu_address;
        new_contact_info.tpu_forwards = tpu_forward_address;
        cluster_info.set_my_contact_info(new_contact_info);
    }

    fn handle_packet(
        msg: std::result::Result<SubscribePacketsResult, RecvError>,
        packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        tpu: &SocketAddr,
        tpu_fwd: &SocketAddr,
    ) -> Result<(usize, usize, bool)> {
        let mut is_heartbeat = false;
        let mut batches_received = 0;
        let mut packets_received = 0;
        if let Ok(msg) = msg {
            let msg = msg?
                .ok_or_else(|| {
                    datapoint_info!(METRICS_NAME, ("grpc_stream_disconnected", 1, i64));
                    RelayerStageError::GrpcStreamDisconnected
                })?
                .msg
                .ok_or_else(|| {
                    datapoint_info!(METRICS_NAME, ("bad_message", 1, i64));
                    RelayerStageError::BadMessage
                })?;
            match msg {
                Msg::BatchList(batch_wrapper) => {
                    batches_received += batch_wrapper.batch_list.len();
                    let packet_batches = batch_wrapper
                        .batch_list
                        .into_iter()
                        .map(|batch| {
                            packets_received += batch.packets.len();
                            PacketBatch::new(
                                batch
                                    .packets
                                    .into_iter()
                                    .map(proto_packet_to_packet)
                                    .collect(),
                            )
                        })
                        .collect();
                    packet_sender.send((packet_batches, None)).map_err(|_| {
                        datapoint_info!(METRICS_NAME, ("proxy_packet_forward_failed", 1, i64));
                        RelayerStageError::ChannelError
                    })?;
                }
                // The boolean value of heartbeat is meaningless but needs to be a protobuf type
                Msg::Heartbeat(_) => {
                    // always sends because tpu_proxy has its own fail-safe and can't assume
                    // state
                    if let Some(tpu_proxy_heartbeat_sender) = tpu_proxy_heartbeat_sender {
                        tpu_proxy_heartbeat_sender
                            .send((*tpu, *tpu_fwd))
                            .map_err(|_| {
                                datapoint_info!(METRICS_NAME, ("heartbeat_channel_error", 1, i64));
                                RelayerStageError::HeartbeatChannelError
                            })?;
                    }
                    is_heartbeat = true;
                }
            }
        } else {
            return Err(RelayerStageError::ChannelError);
        }
        Ok((batches_received, packets_received, is_heartbeat))
    }

    fn handle_bundle(
        msg: std::result::Result<
            std::result::Result<Option<SubscribeBundlesResponse>, Status>,
            RecvError,
        >,
        bundle_sender: &Option<Sender<Vec<PacketBundle>>>,
    ) -> Result<()> {
        match msg {
            Ok(msg) => {
                let response = msg?.ok_or(RelayerStageError::GrpcStreamDisconnected)?;
                let bundles = response
                    .bundles
                    .into_iter()
                    .map(|b| {
                        let batch = PacketBatch::new(
                            b.packets.into_iter().map(proto_packet_to_packet).collect(),
                        );
                        // TODO (LB): copy over UUID from Bundle
                        PacketBundle {
                            batch,
                            uuid: Uuid::new_v4(),
                        }
                    })
                    .collect();
                if let Some(bundle_sender) = bundle_sender {
                    if let Err(e) = bundle_sender.send(bundles) {
                        error!("error forwarding bundle: {:?}", e);
                    }
                }
            }
            Err(_) => return Err(RelayerStageError::ChannelError),
        }
        Ok(())
    }

    fn stream_from_proxy(
        client: &mut BlockingProxyClient,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        bundle_sender: &Option<Sender<Vec<PacketBundle>>>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
        backoff: &mut BackoffStrategy,
        exit: &Arc<AtomicBool>,
    ) -> Result<()> {
        let packet_receiver = client.start_bi_directional_packet_stream()?;

        // conditionally create an actual bundle receiver if there's a channel to send on
        let (_stubbed_sender, mut bundle_receiver) = unbounded();

        if bundle_sender.is_some() {
            bundle_receiver = client.subscribe_bundles()?;
        }

        let mut heartbeat_received = false;
        let mut received_first_heartbeat = false;
        let heartbeat_tick = tick(HEARTBEAT_TIMEOUT_MS);

        let metrics_tick = tick(METRICS_CADENCE_SEC);
        let mut total_msg_received = 0;
        let mut total_batches_received = 0;
        let mut total_packets_received = 0;

        loop {
            select! {
                recv(heartbeat_tick) -> _ => {
                    if exit.load(Ordering::Relaxed) {
                        return Err(RelayerStageError::HeartbeatError);
                    }

                    if received_first_heartbeat {
                        backoff.reset();
                    } else {
                        warn!("waiting for first heartbeat");
                    }
                    // Only disconnect if previously received heartbeat and then missed
                    if !heartbeat_received && received_first_heartbeat {
                        warn!("heartbeat late, disconnecting");
                        datapoint_info!(
                            METRICS_NAME,
                            ("proxy_stream_disconnect", 1, i64)
                        );
                        return Err(RelayerStageError::HeartbeatError);
                    }
                    heartbeat_received = false;
                }
                recv(packet_receiver) -> msg => {
                    let (batches_received, packets_received, is_heartbeat) =
                        Self::handle_packet(msg, verified_packet_sender, tpu_proxy_heartbeat_sender, &tpu, &tpu_fwd)?;
                    heartbeat_received |= is_heartbeat;
                    if is_heartbeat && !received_first_heartbeat {
                        received_first_heartbeat = true;
                    }
                    total_msg_received += 1;
                    total_batches_received += batches_received;
                    total_packets_received += packets_received;
                }
                recv(bundle_receiver) -> msg => {
                    Self::handle_bundle(msg, bundle_sender)?;
                }
                recv(metrics_tick) -> _ => {
                    datapoint_info!(
                        METRICS_NAME,
                        ("msg_received", total_msg_received, i64),
                        ("batches_received", total_batches_received, i64),
                        ("packets_received", total_packets_received, i64),
                    );
                }
            }
        }
    }

    fn start_stream(
        client: &mut BlockingProxyClient,
        verified_packet_sender: &Sender<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>,
        bundle_sender: &Option<Sender<Vec<PacketBundle>>>,
        tpu_proxy_heartbeat_sender: &Option<Sender<HeartbeatEvent>>,
        backoff: &mut BackoffStrategy,
        exit: &Arc<AtomicBool>,
    ) -> Result<()> {
        let (tpu, tpu_fwd) = client.fetch_tpu_config()?;

        Self::stream_from_proxy(
            client,
            verified_packet_sender,
            bundle_sender,
            tpu_proxy_heartbeat_sender,
            tpu,
            tpu_fwd,
            backoff,
            exit,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.t_hdls {
            t.join()?;
        }
        Ok(())
    }
}
