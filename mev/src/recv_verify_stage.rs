//! The `recv_verify_stage` maintains a connection with the validator
//! interface and streams packets from TPU proxy to the banking stage.
//! It notifies the tpu_proxy_advertiser on connect/disconnect.

use {
    crate::{
        backoff::BackoffStrategy,
        packet::PacketBatch as PbPacketBatch,
        validator_interface::{
            validator_interface_client::ValidatorInterfaceClient, SubscribePacketsRequest,
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
    tonic::metadata::{MetadataMap, MetadataValue},
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
        tpu_notify_sender: UnboundedSender<bool>,
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
        info!("[Jito] Started recv verify stage");
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
        loop {
            let maybe_packet_batch = proxy_bridge_receiver.blocking_recv();
            match maybe_packet_batch {
                None => {
                    warn!("[Jito] Received none in proxy bridge");
                }
                Some(packet_batch) => {
                    if let Err(error) = verified_packet_sender.send(packet_batch) {
                        error!("[Jito] Error forwarding to banking stage: {}", error);
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
        loop {
            let maybe_batch_list = packet_converter_receiver.blocking_recv();
            match maybe_batch_list {
                None => {
                    warn!("[Jito] Exiting convert packets, sender dropped");
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
                    if let Err(error) = proxy_bridge_sender.send(converted_batch_list) {
                        error!("[Jito] Error forwarding to proxy bridge: {}", error);
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
        tpu_notify_sender: UnboundedSender<bool>,
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
                    // Reset backoff counter
                    backoff.reset();

                    // Authenticate
                    let mut request = tonic::Request::new(SubscribePacketsRequest {});
                    Self::add_auth_meta(request.metadata_mut(), msg, sig, pk);
                    match client.subscribe_packets(request).await {
                        Ok(resp) => {
                            // Advertise
                            // Async send, from unbounded docs:
                            // A send on this channel will always succeed as long as
                            // the receive half has not been closed. If the receiver
                            // falls behind, messages will be arbitrarily buffered.
                            if let Err(error) = tpu_notify_sender.send(true) {
                                error!("[Jito] Error notifying TPU advertiser: {}", error);
                            }

                            info!("[Jito] Connected to validator interface. Streaming packets...");

                            // Blocking read from grpc stream, send to unbounded channel
                            let mut stream = resp.into_inner();
                            loop {
                                match stream.next().await {
                                    None => {
                                        warn!("[Jito] Received empty batch list");
                                    }
                                    Some(result) => match result {
                                        Ok(resp) => {
                                            if let Err(error) =
                                                packet_converter_sender.send(resp.batch_list)
                                            {
                                                error!("[Jito] Error forwarding to packet converter: {}", error);
                                            }
                                        }
                                        Err(error) => {
                                            error!("[Jito] Error reading from grpc: {}", error);
                                            break;
                                        }
                                    },
                                }
                            }
                        }
                        Err(error) => {
                            error!(
                                "[Jito] Error packet subscribing to validator interface: {}",
                                error
                            );
                        }
                    }
                }
                Err(error) => {
                    error!(
                        "[Jito] {} Error connecting to validator interface: {}",
                        validator_interface_address.to_string(),
                        error
                    );
                    sleep(Duration::from_millis(backoff.next_wait())).await;
                }
            }
            // Stop advertising, no active connection
            // Async send, from unbounded docs:
            // A send on this channel will always succeed as long as
            // the receive half has not been closed. If the receiver
            // falls behind, messages will be arbitrarily buffered.
            if let Err(error) = tpu_notify_sender.send(false) {
                error!("[Jito] Error notifying TPU advertiser: {}", error);
            }
        }
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
