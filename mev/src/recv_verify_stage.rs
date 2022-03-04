//! The `recv_verify_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.
//! It notifies the tpu_proxy_advertiser on connect/disconnect.

use {
    crate::{
        backoff::BackoffStrategy,
        packet::PacketBatch as PbPacketBatch,
        validator_interface::{
            validator_interface_client::ValidatorInterfaceClient, GetTpuConfigsRequest,
            SubscribePacketsRequest,
        },
    },
    crossbeam_channel::Sender,
    futures_util::StreamExt,
    log::*,
    solana_perf::{
        cuda_runtime::PinnedVec,
        packet::{Packet, PacketBatch},
    },
    solana_sdk::{
        packet::{PacketFlags, PACKET_DATA_SIZE},
        signature::{Keypair, Signature},
        signer::Signer,
    },
    std::{
        cmp::min,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::{Arc, RwLockReadGuard},
        thread::{self, JoinHandle},
        time::Duration,
    },
    tokio::{
        runtime::Runtime,
        sync::{
            mpsc,
            mpsc::{UnboundedReceiver, UnboundedSender},
        },
        time::sleep,
    },
    tonic::{
        metadata::{MetadataMap, MetadataValue},
        transport::Channel,
        Request,
    },
};

pub struct RecvVerifyStage {
    validator_interface_thread: JoinHandle<()>,
    packet_converter_thread: JoinHandle<()>,
    proxy_bridge_thread: JoinHandle<()>,
}

const UNKNOWN_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));

impl RecvVerifyStage {
    pub fn new(
        keypair: RwLockReadGuard<Arc<Keypair>>,
        validator_interface_address: SocketAddr,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
        tpu_notify_sender: UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) -> Self {
        let (packet_converter_sender, packet_converter_receiver) =
            mpsc::unbounded_channel::<Vec<PbPacketBatch>>();
        let (proxy_bridge_sender, proxy_bridge_receiver) =
            mpsc::unbounded_channel::<Vec<PacketBatch>>();
        let msg: &[u8] = b"Let's get this money!";
        let sig: Signature = keypair.sign_message(msg);
        let pk = keypair.pubkey().to_bytes();
        let rt = Runtime::new().unwrap();
        let validator_interface_thread = thread::spawn(move || {
            rt.block_on({
                Self::validator_interface_connection_loop(
                    msg,
                    sig,
                    pk,
                    validator_interface_address,
                    packet_converter_sender,
                    tpu_notify_sender,
                )
            })
        });
        let packet_converter_thread = thread::spawn(move || {
            Self::convert_packets(packet_converter_receiver, proxy_bridge_sender)
        });
        let proxy_bridge_thread = thread::spawn(move || {
            Self::proxy_bridge(proxy_bridge_receiver, verified_packet_sender)
        });
        info!("[MEV] Started recv verify stage");
        Self {
            validator_interface_thread,
            packet_converter_thread,
            proxy_bridge_thread,
        }
    }

    // Proxy bridge continuously reads from the receiver
    // and writes to sender (banking stage).
    fn proxy_bridge(
        mut proxy_bridge_receiver: UnboundedReceiver<Vec<PacketBatch>>,
        verified_packet_sender: Sender<Vec<PacketBatch>>,
    ) {
        // In the loop below the None case is failure to read from
        // the packet converter thread, whereas send error is failure to
        // forward to banking. Both should only occur on shutdown.
        loop {
            let maybe_packet_batch = proxy_bridge_receiver.blocking_recv();
            match maybe_packet_batch {
                None => {
                    warn!("[MEV] Exiting proxy bridge, receiver dropped");
                    break;
                }
                Some(packet_batch) => {
                    if let Err(e) = verified_packet_sender.send(packet_batch) {
                        warn!("[MEV] Exiting proxy bridge, receiver dropped: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // convert_packets makes a blocking read from packet_converter_receiver
    // before converting all packets across packet batches in a batch list
    // into a single packet batch with all converted packets, which it sends
    // to the proxy_bridge_sender asynchronously.
    fn convert_packets(
        mut packet_converter_receiver: UnboundedReceiver<Vec<PbPacketBatch>>,
        proxy_bridge_sender: UnboundedSender<Vec<PacketBatch>>,
    ) {
        // In the loop below the None case is failure to read from
        // the validator connection thread, whereas send error is failure to
        // send to proxy bridge. Both should only occur on shutdown.
        loop {
            let maybe_batch_list = packet_converter_receiver.blocking_recv(); // TODO: inner try_recv
            match maybe_batch_list {
                None => {
                    warn!("[MEV] Exiting convert packets, receiver dropped");
                    break;
                }
                Some(batch_list) => {
                    let mut converted_batch_list = Vec::with_capacity(batch_list.len());
                    for batch in batch_list {
                        let mut packets = PinnedVec::with_capacity(batch.packets.len());
                        for p in batch.packets {
                            let mut data = [0; PACKET_DATA_SIZE];
                            let copy_len = min(data.len(), p.data.len());
                            data[..copy_len].copy_from_slice(&p.data[..copy_len]);
                            let mut packet = Packet::new(data, Default::default());
                            if let Some(meta) = p.meta {
                                packet.meta.size = meta.size as usize;
                                packet.meta.addr = meta.addr.parse().unwrap_or(UNKNOWN_IP);
                                packet.meta.port = meta.port as u16;
                                if let Some(flags) = meta.flags {
                                    if flags.simple_vote_tx {
                                        packet.meta.flags.insert(PacketFlags::SIMPLE_VOTE_TX);
                                    }
                                    if flags.forwarded {
                                        packet.meta.flags.insert(PacketFlags::FORWARDED);
                                    }
                                    if flags.tracer_tx {
                                        packet.meta.flags.insert(PacketFlags::TRACER_TX);
                                    }
                                    if flags.repair {
                                        packet.meta.flags.insert(PacketFlags::REPAIR);
                                    }
                                }
                            }
                            packets.push(packet);
                        }
                        converted_batch_list.push(PacketBatch { packets });
                    }
                    // Async send, from unbounded docs:
                    // A send on this channel will always succeed as long as
                    // the receive half has not been closed. If the receiver
                    // falls behind, messages will be arbitrarily buffered.
                    if let Err(e) = proxy_bridge_sender.send(converted_batch_list) {
                        warn!("[MEV] Exiting convert packets, sender dropped: {}", e);
                        break;
                    }
                }
            }
        }
    }

    // This function maintains a connection to the TPU
    // proxy backend. It is long lived and should
    // be called in a spawned thread. On connection
    // it spawns a thread to read from the open connection
    // and async forward to a passed unbounded channel.
    async fn validator_interface_connection_loop(
        msg: &[u8],
        sig: Signature,
        pk: [u8; 32],
        validator_interface_address: SocketAddr,
        packet_converter_sender: UnboundedSender<Vec<PbPacketBatch>>,
        tpu_notify_sender: UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) {
        // Use custom exponential backoff for ValidatorInterface connections
        let mut backoff = BackoffStrategy::new();
        loop {
            match ValidatorInterfaceClient::connect(format!(
                "http://{}",
                validator_interface_address
            ))
            .await
            {
                Ok(mut client) => {
                    // Fetch TPU proxy addresses
                    let tpu_address;
                    let tpu_forward_address;
                    let mut request = tonic::Request::new(GetTpuConfigsRequest {});
                    Self::add_auth_meta(request.metadata_mut(), msg, sig, pk);
                    let config_result = Self::fetch_tpu_proxy_config(&mut client, request).await;
                    match config_result {
                        Ok((address, forward_address)) => {
                            tpu_address = Some(address);
                            tpu_forward_address = Some(forward_address);
                        }
                        Err(error) => {
                            error!("[MEV] Error fetching TPU proxy config: {}", error);
                            sleep(Duration::from_millis(backoff.next_wait())).await;
                            continue;
                        }
                    }

                    // Reset backoff counter, connected and received TPU config
                    backoff.reset();

                    // Stream packets
                    let mut request = tonic::Request::new(SubscribePacketsRequest {});
                    Self::add_auth_meta(request.metadata_mut(), msg, sig, pk);
                    if let Err(e) = Self::stream_packets(
                        &mut client,
                        request,
                        &packet_converter_sender,
                        tpu_address,
                        tpu_forward_address,
                        &tpu_notify_sender,
                    )
                    .await
                    {
                        error!("[MEV] Error streaming packets: {}", e);
                        Self::advertise_tpu_addresses(None, None, &tpu_notify_sender);
                        continue;
                    } else {
                        // Stream packets returns Ok to indicate shutdown.
                        warn!("[MEV] Shutting down validator interface connection");
                        return;
                    }
                }
                Err(error) => {
                    error!(
                        "[MEV] {} Error connecting to validator interface: {}",
                        validator_interface_address.to_string(),
                        error
                    );
                    sleep(Duration::from_millis(backoff.next_wait())).await;
                }
            }
        }
    }

    fn advertise_tpu_addresses(
        tpu_socket: Option<SocketAddr>,
        tpu_forward_socket: Option<SocketAddr>,
        tpu_notify_sender: &UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) {
        if let Err(error) = tpu_notify_sender.send((tpu_socket, tpu_forward_socket)) {
            // It should not be possible for advertising to fail.
            // If for whatever reason it does, we encounter a situation where
            // we could be waiting for packets while advertising a disconnected
            // local fetch stage, or vice versa. These are unacceptable.
            panic!("[MEV] Error advertising TPU adresses: {}", error);
        }
    }

    async fn stream_packets(
        client: &mut ValidatorInterfaceClient<Channel>,
        request: Request<SubscribePacketsRequest>,
        packet_converter_sender: &UnboundedSender<Vec<PbPacketBatch>>,
        tpu_address: Option<SocketAddr>,
        tpu_forward_address: Option<SocketAddr>,
        tpu_notify_sender: &UnboundedSender<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stream = client.subscribe_packets(request).await?.into_inner();
        info!("[MEV] Connected to validator interface. Streaming packets...");
        Self::advertise_tpu_addresses(tpu_address, tpu_forward_address, tpu_notify_sender);
        loop {
            if let Err(e) = packet_converter_sender.send(
                stream
                    .next()
                    .await
                    .ok_or("received empty batch list")??
                    .batch_list,
            ) {
                // Packet converter should only be closed on shutdown.
                // Exit stream so the connection thread can close.
                warn!("[MEV] Exiting stream packets, sender dropped: {}", e);
                return Ok(());
            }
        }
    }

    async fn fetch_tpu_proxy_config(
        client: &mut ValidatorInterfaceClient<Channel>,
        request: Request<GetTpuConfigsRequest>,
    ) -> Result<(SocketAddr, SocketAddr), Box<dyn std::error::Error>> {
        let resp = client.get_tpu_configs(request).await?;
        let tpu_configs = resp.into_inner();
        let tpu_addr = tpu_configs.tpu.ok_or("missing TPU proxy address")?;
        let tpu_forward_addr = tpu_configs
            .tpu_forward
            .ok_or("missing TPU proxy forward address")?;
        let tpu_ip = tpu_addr.ip.parse::<Ipv4Addr>()?;
        let tpu_forward_ip = tpu_forward_addr.ip.parse::<Ipv4Addr>()?;
        let tpu_socket = SocketAddr::new(IpAddr::from(tpu_ip), tpu_addr.port as u16);
        let tpu_forward_socket =
            SocketAddr::new(IpAddr::from(tpu_forward_ip), tpu_forward_addr.port as u16);
        Ok((tpu_socket, tpu_forward_socket))
    }

    pub fn add_auth_meta(meta: &mut MetadataMap, msg: &[u8], sig: Signature, pk: [u8; 32]) {
        meta.append_bin("public-key-bin", MetadataValue::from_bytes(&pk));
        meta.append_bin("message-bin", MetadataValue::from_bytes(msg));
        meta.append_bin("signature-bin", MetadataValue::from_bytes(sig.as_ref()));
    }

    pub fn join(self) -> thread::Result<()> {
        let results = vec![
            self.proxy_bridge_thread.join(),
            self.packet_converter_thread.join(),
            self.validator_interface_thread.join(),
        ];

        if results.iter().all(|t| t.is_ok()) {
            Ok(())
        } else {
            Err(Box::new("failed to join a thread"))
        }
    }
}
