//! The `mev_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.
//! It notifies the tpu_proxy_advertiser on connect/disconnect.

use {
    crate::{
        backoff::{self, BackoffStrategy},
        blocking_proxy_client::{AuthenticationInjector, BlockingProxyClient, ProxyError},
        bundle::Bundle,
        proto::validator_interface::{
            subscribe_packets_response::Msg, SubscribeBundlesResponse, SubscribePacketsResponse,
        },
        proto_packet_to_packet,
    },
    crossbeam_channel::{select, tick, unbounded, Receiver, RecvError, Sender},
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    solana_metrics::datapoint_info,
    solana_perf::packet::PacketBatch,
    solana_sdk::{signature::Signature, signer::Signer},
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{self, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
    tokio::time::Instant,
    tonic::Status,
};

pub struct MevStage {
    _heartbeat_sender: Sender<HeartbeatEvent>,
    proxy_thread: JoinHandle<()>,
    heartbeat_thread: JoinHandle<()>,
}

#[derive(Error, Debug)]
pub enum MevStageError {
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
}

type Result<T> = std::result::Result<T, MevStageError>;
type HeartbeatEvent = (SocketAddr, SocketAddr);
type SubscribePacketsResult = std::result::Result<Option<SubscribePacketsResponse>, Status>;

const HEARTBEAT_TIMEOUT_MS: Duration = Duration::from_millis(1500); // Empirically determined from load testing
const DISCONNECT_DELAY_SEC: Duration = Duration::from_secs(60);
const METRICS_CADENCE_SEC: Duration = Duration::from_secs(1);
const METRICS_NAME: &str = "mev_stage";

impl MevStage {
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        validator_interface_address: String,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        bundle_sender: Sender<Vec<Bundle>>,
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
    ) -> Self {
        let msg = b"Let's get this money!".to_vec();
        let keypair = cluster_info.keypair();
        let sig: Signature = keypair.sign_message(msg.as_slice());
        let pubkey = keypair.pubkey();
        let interceptor = AuthenticationInjector::new(msg, sig, pubkey);

        let (heartbeat_sender, heartbeat_receiver) = unbounded();

        let proxy_thread = Self::spawn_proxy_thread(
            validator_interface_address,
            interceptor,
            verified_packet_sender,
            // if no validator interface address provided, sender side of the channel gets dropped
            // in heartbeat thread and causes packet forwarding to error. this clone, along with the
            // reference on self, prevents it from being dropped
            heartbeat_sender.clone(),
            bundle_sender,
        );

        // This thread is responsible for connecting and disconnecting the fetch stage to prevent
        // circumventing TPU proxy.
        let heartbeat_thread = Self::heartbeat_thread(
            packet_intercept_receiver,
            packet_sender,
            heartbeat_receiver,
            cluster_info,
        );

        info!("started mev stage");

        Self {
            _heartbeat_sender: heartbeat_sender,
            proxy_thread,
            heartbeat_thread,
        }
    }

    fn spawn_proxy_thread(
        validator_interface_address: String,
        interceptor: AuthenticationInjector,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        heartbeat_sender: Sender<HeartbeatEvent>,
        bundle_sender: Sender<Vec<Bundle>>,
    ) -> JoinHandle<()> {
        thread::Builder::new()
            .name("proxy_thread".into())
            .spawn(move || {
                if !validator_interface_address.contains("http") {
                    info!("malformed or missing mev proxy address provided, exiting mev loop");
                    datapoint_info!(METRICS_NAME, ("bad_proxy_addr", 1, i64));
                    return;
                }

                let mut backoff = BackoffStrategy::new();

                loop {
                    if let Err(e) = Self::connect_and_stream(
                        validator_interface_address.clone(),
                        &interceptor,
                        &heartbeat_sender,
                        &verified_packet_sender,
                        &mut backoff,
                        &bundle_sender,
                    ) {
                        error!("spawn_proxy_thread error [error: {:?}]", e);
                        datapoint_info!(METRICS_NAME, ("proxy_connection_error", 1, i64));
                        thread::sleep(Duration::from_millis(backoff.next_wait()));
                    }
                }
            })
            .unwrap()
    }

    // Disconnect fetch behaviour
    // Starts connected
    // When connected and a packet is received, forward it
    // When disconnected, packet is dropped
    // When receiving heartbeat while connected and not pending disconnect
    //      Sets pending_disconnect to true and records time
    // When receiving heartbeat while connected, and pending for > DISCONNECT_DELAY_SEC
    //      Sets fetch_connected to false, pending_disconnect to false
    //      Advertises TPU ports sent in heartbeat
    // When tick is received without heartbeat_received
    //      Sets fetch_connected to true, pending_disconnect to false
    //      Advertises saved contact info
    fn heartbeat_thread(
        packet_intercept_receiver: Receiver<PacketBatch>,
        packet_sender: Sender<PacketBatch>,
        heartbeat_receiver: Receiver<HeartbeatEvent>,
        cluster_info: &Arc<ClusterInfo>,
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
                                            error!("{:?}", MevStageError::PacketForwardError);
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
                            if !heartbeat_received && (!fetch_connected || pending_disconnect) {
                                warn!("heartbeat late, reconnecting fetch stage");
                                fetch_connected = true;
                                pending_disconnect = false;
                                Self::set_tpu_addresses(&cluster_info, saved_contact_info.tpu, saved_contact_info.tpu_forwards);
                                heartbeats_received = 0;
                            }
                            heartbeat_received = false;
                        }
                        recv(heartbeat_receiver) -> tpu_info => {
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
        packet_sender: &Sender<Vec<PacketBatch>>,
        heartbeat_sender: &Sender<HeartbeatEvent>,
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
                    MevStageError::GrpcStreamDisconnected
                })?
                .msg
                .ok_or_else(|| {
                    datapoint_info!(METRICS_NAME, ("bad_message", 1, i64));
                    MevStageError::BadMessage
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
                    packet_sender.send(packet_batches).map_err(|_| {
                        datapoint_info!(METRICS_NAME, ("proxy_packet_forward_failed", 1, i64));
                        MevStageError::ChannelError
                    })?;
                }
                // The boolean value of heartbeat is meaningless but needs to be a protobuf type
                Msg::Heartbeat(_) => {
                    // always sends because tpu_proxy has its own fail-safe and can't assume
                    // state
                    heartbeat_sender.send((*tpu, *tpu_fwd)).map_err(|_| {
                        datapoint_info!(METRICS_NAME, ("heartbeat_channel_error", 1, i64));
                        MevStageError::HeartbeatChannelError
                    })?;
                    is_heartbeat = true;
                }
            }
        } else {
            return Err(MevStageError::ChannelError);
        }
        Ok((batches_received, packets_received, is_heartbeat))
    }

    fn handle_bundle(
        msg: std::result::Result<
            std::result::Result<Option<SubscribeBundlesResponse>, Status>,
            RecvError,
        >,
        bundle_sender: &Sender<Vec<Bundle>>,
    ) -> Result<()> {
        match msg {
            Ok(msg) => {
                let response = msg?.ok_or(MevStageError::GrpcStreamDisconnected)?;
                let bundles = response
                    .bundles
                    .into_iter()
                    .map(|b| {
                        let batch = PacketBatch::new(
                            b.packets.into_iter().map(proto_packet_to_packet).collect(),
                        );
                        Bundle { batch }
                    })
                    .collect();
                bundle_sender
                    .send(bundles)
                    .map_err(|_| MevStageError::ChannelError)?;
            }
            Err(_) => return Err(MevStageError::ChannelError),
        }
        Ok(())
    }

    fn stream_from_proxy(
        mut client: BlockingProxyClient,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        tpu: SocketAddr,
        tpu_fwd: SocketAddr,
        verified_packet_sender: &Sender<Vec<PacketBatch>>,
        backoff: &mut backoff::BackoffStrategy,
        bundle_sender: &Sender<Vec<Bundle>>,
    ) -> Result<()> {
        let packet_receiver = client.subscribe_packets()?;
        let bundle_receiver = client.subscribe_bundles()?;

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
                        return Err(MevStageError::HeartbeatError);
                    }
                    heartbeat_received = false;
                }
                recv(packet_receiver) -> msg => {
                    let (batches_received, packets_received, is_heartbeat) =
                        Self::handle_packet(msg, verified_packet_sender, heartbeat_sender, &tpu, &tpu_fwd)?;
                    heartbeat_received |= is_heartbeat;
                    if is_heartbeat && !received_first_heartbeat {
                        received_first_heartbeat = true;
                    }
                    total_msg_received += 1;
                    total_batches_received += batches_received;
                    total_packets_received += packets_received;
                }
                recv(bundle_receiver) -> msg => {
                    let _ = Self::handle_bundle(msg, bundle_sender)?;
                }
                recv(packet_receiver) -> msg => {
                    let (batches_received, packets_received, is_heartbeat) =
                        Self::handle_packet(msg, verified_packet_sender, heartbeat_sender, &tpu, &tpu_fwd)?;
                    heartbeat_received |= is_heartbeat;
                    if is_heartbeat && !received_first_heartbeat {
                        received_first_heartbeat = true;
                    }
                    total_msg_received += 1;
                    total_batches_received += batches_received;
                    total_packets_received += packets_received;
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

    fn connect_and_stream(
        validator_interface_address: String,
        auth_interceptor: &AuthenticationInjector,
        heartbeat_sender: &Sender<HeartbeatEvent>,
        verified_packet_sender: &Sender<Vec<PacketBatch>>,
        backoff: &mut BackoffStrategy,
        bundle_sender: &Sender<Vec<Bundle>>,
    ) -> Result<()> {
        let mut client = BlockingProxyClient::new(validator_interface_address, auth_interceptor)?;
        let (tpu, tpu_fwd) = client.fetch_tpu_config()?;

        Self::stream_from_proxy(
            client,
            heartbeat_sender,
            tpu,
            tpu_fwd,
            verified_packet_sender,
            backoff,
            bundle_sender,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.proxy_thread.join()?;
        self.heartbeat_thread.join()?;
        Ok(())
    }
}
