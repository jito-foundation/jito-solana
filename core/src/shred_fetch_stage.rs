//! The `shred_fetch_stage` pulls shreds from UDP sockets and sends it to a channel.

use {
    crate::repair::{repair_service::OutstandingShredRepairs, serve_repair::ServeRepair},
    agave_feature_set::FeatureSet,
    bytes::Bytes,
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    itertools::Itertools,
    solana_clock::{Slot, DEFAULT_MS_PER_SLOT},
    solana_epoch_schedule::EpochSchedule,
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
    solana_ledger::shred::{self, should_discard_shred, ShredFetchStats},
    solana_packet::{Meta, PACKET_DATA_SIZE},
    solana_perf::packet::{
        BytesPacket, BytesPacketBatch, PacketBatch, PacketBatchRecycler, PacketFlags, PacketRef,
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::{BankForks, SharableBanks},
    solana_streamer::{
        evicting_sender::EvictingSender,
        streamer::{self, ChannelSend, PacketBatchReceiver, StreamerReceiveStats},
    },
    std::{
        net::{SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

// When running with very short epochs (e.g. for testing), we want to avoid
// filtering out shreds that we actually need. This value was chosen empirically
// because it's large enough to protect against observed short epoch problems
// while being small enough to keep the overhead small on deduper, blockstore,
// etc.
const MAX_SHRED_DISTANCE_MINIMUM: u64 = 500;

pub(crate) struct ShredFetchStage {
    thread_hdls: Vec<JoinHandle<()>>,
}

/// Ingress limit for the shred fetch channel (in terms of packet _batches_).
///
/// The general case sees shred and repair ingress in the hundreds of packet batches per second.
/// However, in the case of catch-up, we may see upwards of 8k packet batches per second, which would
/// suggest a roughly 16k packet batch limit for ample headroom. We're setting it to 4x that amount
/// to future proof for increases of CU limits (e.g., a future 100k CU limit).
pub(crate) const SHRED_FETCH_CHANNEL_SIZE: usize = 1024 * 64;

#[derive(Clone)]
struct RepairContext {
    repair_socket: Arc<UdpSocket>,
    cluster_info: Arc<ClusterInfo>,
    outstanding_repair_requests: Arc<RwLock<OutstandingShredRepairs>>,
}

impl ShredFetchStage {
    // updates packets received on a channel and sends them on another channel
    fn modify_packets(
        recvr: PacketBatchReceiver,
        recvr_stats: Option<Arc<StreamerReceiveStats>>,
        sendr: EvictingSender<PacketBatch>,
        sharable_banks: &SharableBanks,
        shred_version: u16,
        name: &'static str,
        flags: PacketFlags,
        repair_context: Option<&RepairContext>,
        turbine_disabled: Arc<AtomicBool>,
    ) {
        // Only repair shreds need repair context.
        debug_assert_eq!(
            flags.contains(PacketFlags::REPAIR),
            repair_context.is_some()
        );
        const STATS_SUBMIT_CADENCE: Duration = Duration::from_secs(1);
        let mut last_updated = Instant::now();
        let mut keypair = repair_context.as_ref().copied().map(RepairContext::keypair);
        let (
            mut last_root,
            mut slots_per_epoch,
            mut feature_set,
            mut epoch_schedule,
            mut last_slot,
        ) = {
            let root_bank = sharable_banks.root();
            (
                root_bank.slot(),
                root_bank.get_slots_in_epoch(root_bank.epoch()),
                root_bank.feature_set.clone(),
                root_bank.epoch_schedule().clone(),
                sharable_banks.working().slot(),
            )
        };
        let mut stats = ShredFetchStats::default();

        for mut packet_batch in recvr {
            if last_updated.elapsed().as_millis() as u64 > DEFAULT_MS_PER_SLOT {
                last_updated = Instant::now();
                last_slot = sharable_banks.working().slot();
                let root_bank = sharable_banks.root();
                feature_set = root_bank.feature_set.clone();
                epoch_schedule = root_bank.epoch_schedule().clone();
                last_root = root_bank.slot();
                slots_per_epoch = root_bank.get_slots_in_epoch(root_bank.epoch());
                keypair = repair_context.as_ref().copied().map(RepairContext::keypair);
            }
            stats.shred_count += packet_batch.len();

            if let Some(repair_context) = repair_context {
                debug_assert_eq!(flags, PacketFlags::REPAIR);
                debug_assert!(keypair.is_some());
                if let Some(ref keypair) = keypair {
                    ServeRepair::handle_repair_response_pings(
                        &repair_context.repair_socket,
                        keypair,
                        &mut packet_batch,
                        &mut stats,
                    );
                }
                // Discard packets if repair nonce does not verify.
                let now = solana_time_utils::timestamp();
                let mut outstanding_repair_requests =
                    repair_context.outstanding_repair_requests.write().unwrap();
                packet_batch
                    .iter_mut()
                    .filter(|packet| !packet.meta().discard())
                    .for_each(|mut packet| {
                        // Have to set repair flag here so that the nonce is
                        // taken off the shred's payload.
                        packet.meta_mut().flags |= PacketFlags::REPAIR;
                        if !verify_repair_nonce(
                            packet.as_ref(),
                            now,
                            &mut outstanding_repair_requests,
                        ) {
                            packet.meta_mut().set_discard(true);
                        }
                    });
            }

            // Filter out shreds that are way too far in the future to avoid the
            // overhead of having to hold onto them.
            let max_slot = last_slot + MAX_SHRED_DISTANCE_MINIMUM.max(2 * slots_per_epoch);
            let enforce_fixed_fec_set = |shred_slot| {
                check_feature_activation(
                    &agave_feature_set::enforce_fixed_fec_set::id(),
                    shred_slot,
                    &feature_set,
                    &epoch_schedule,
                )
            };
            let discard_unexpected_data_complete_shreds = |shred_slot| {
                check_feature_activation(
                    &agave_feature_set::discard_unexpected_data_complete_shreds::id(),
                    shred_slot,
                    &feature_set,
                    &epoch_schedule,
                )
            };
            let turbine_disabled = turbine_disabled.load(Ordering::Relaxed);
            let solanacdn = crate::solanacdn::global();
            let repair_disabled = flags.contains(PacketFlags::REPAIR)
                && solanacdn
                    .as_ref()
                    .is_some_and(|h| !h.repair_shreds_enabled());
            let solanacdn_publish = if flags.contains(PacketFlags::REPAIR) {
                None
            } else {
                solanacdn.as_ref().filter(|h| h.publish_shreds_enabled())
            };
            for mut packet in packet_batch.iter_mut() {
                let discard_by_solanacdn_only = !flags.contains(PacketFlags::REPAIR)
                    && solanacdn
                        .as_ref()
                        .is_some_and(|h| !h.should_ingest_tvu_shred(packet.meta().addr));
                let mut discarded = packet.meta().discard();
                if !discarded && repair_disabled {
                    discarded = true;
                } else if !discarded
                    && (turbine_disabled
                        || should_discard_shred(
                            packet.as_ref(),
                            last_root,
                            max_slot,
                            shred_version,
                            enforce_fixed_fec_set,
                            discard_unexpected_data_complete_shreds,
                            &mut stats,
                        ))
                {
                    discarded = true;
                }

                let discarded_for_publish = discarded;
                discarded |= discard_by_solanacdn_only;
                packet.meta_mut().set_discard(discarded);

                if !discarded {
                    packet.meta_mut().flags.insert(flags);
                }

                if let Some(handle) = solanacdn.as_ref().filter(|h| h.race_enabled()) {
                    // Race is most useful when it can observe both paths even if the slower copy
                    // is later discarded (e.g., because the slot is already rooted). Avoid copying
                    // packet bytes unless needed by publishing.
                    if let Some(shred_bytes) = packet.as_ref().data(..) {
                        if let Some(shred_id) =
                            solana_ledger::shred::layout::get_shred_id(shred_bytes)
                        {
                            handle.note_race_observation(shred_id, packet.meta().addr);
                        }
                    }
                }

                if let Some(bytes) = packet_payload_bytes(packet.as_ref()) {
                    let slot = solana_ledger::shred::layout::get_slot(bytes.as_ref());

                    // In `--solanacdn-only` mode, POPs may inject raw shreds directly to the TVU
                    // socket (bypassing PushShredBatch). Those shreds are sourced from POP IPs
                    // (added via `note_pop_endpoints` / `note_pop_egress_ip`), so count them here
                    // to make the SolanaCDN "Pushed" metric reflect actual delivery.
                    //
                    // Avoid double-counting the non-direct mode, which injects via localhost.
                    if solanacdn.as_ref().is_some_and(|h| {
                        h.is_connected()
                            && !packet.meta().addr.is_loopback()
                            && h.should_ignore_src_ip(packet.meta().addr)
                    }) {
                        solanacdn
                            .as_ref()
                            .expect("is_some_and implies Some")
                            .note_pop_delivered_shred_with_slot(bytes.len(), slot);
                    }

                    if solanacdn.as_ref().is_some_and(|h| {
                        h.is_connected() && !discarded && h.should_ignore_src_ip(packet.meta().addr)
                    }) {
                        solanacdn
                            .as_ref()
                            .expect("is_some_and implies Some")
                            .note_solanacdn_accepted_shred_with_slot(slot);
                    }

                    if let Some(handle) = solanacdn_publish {
                        handle.try_publish_tvu_shred(
                            packet.meta().addr,
                            bytes,
                            discarded_for_publish,
                        );
                    }
                }
            }
            if stats.maybe_submit(name, STATS_SUBMIT_CADENCE) {
                if let Some(stats) = recvr_stats.as_ref() {
                    stats.report();
                }
            }
            if let Err(send_err) = sendr.try_send(packet_batch) {
                match send_err {
                    crossbeam_channel::TrySendError::Full(v) => {
                        stats.overflow_shreds += v.len();
                    }
                    _ => unreachable!("EvictingSender holds on to both ends of the channel"),
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn packet_modifier(
        receiver_thread_name: &'static str,
        modifier_thread_name: &'static str,
        sockets: Vec<Arc<UdpSocket>>,
        exit: Arc<AtomicBool>,
        sender: EvictingSender<PacketBatch>,
        recycler: PacketBatchRecycler,
        bank_forks: Arc<RwLock<BankForks>>,
        shred_version: u16,
        name: &'static str,
        receiver_name: &'static str,
        flags: PacketFlags,
        repair_context: Option<RepairContext>,
        turbine_disabled: Arc<AtomicBool>,
    ) -> (Vec<JoinHandle<()>>, JoinHandle<()>) {
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
        let (packet_sender, packet_receiver) =
            EvictingSender::new_bounded(SHRED_FETCH_CHANNEL_SIZE);
        let receiver_stats = Arc::new(StreamerReceiveStats::new(receiver_name));
        let streamers = sockets
            .into_iter()
            .enumerate()
            .map(|(i, socket)| {
                streamer::receiver(
                    format!("{receiver_thread_name}{i:02}"),
                    socket,
                    exit.clone(),
                    packet_sender.clone(),
                    recycler.clone(),
                    receiver_stats.clone(),
                    Some(Duration::from_millis(5)), // coalesce
                    true,                           // use_pinned_memory
                    None,                           // in_vote_only_mode
                    false,                          // is_staked_service
                )
            })
            .collect();
        let modifier_hdl = Builder::new()
            .name(modifier_thread_name.to_string())
            .spawn(move || {
                Self::modify_packets(
                    packet_receiver,
                    Some(receiver_stats),
                    sender,
                    &sharable_banks,
                    shred_version,
                    name,
                    flags,
                    repair_context.as_ref(),
                    turbine_disabled,
                )
            })
            .unwrap();
        (streamers, modifier_hdl)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        sockets: Vec<Arc<UdpSocket>>,
        turbine_quic_endpoint_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
        repair_response_quic_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
        repair_socket: Arc<UdpSocket>,
        sender: EvictingSender<PacketBatch>,
        shred_version: u16,
        bank_forks: Arc<RwLock<BankForks>>,
        cluster_info: Arc<ClusterInfo>,
        outstanding_repair_requests: Arc<RwLock<OutstandingShredRepairs>>,
        turbine_disabled: Arc<AtomicBool>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let recycler = PacketBatchRecycler::warmed(100, 1024);
        let repair_context = RepairContext {
            repair_socket: repair_socket.clone(),
            cluster_info,
            outstanding_repair_requests,
        };

        let (mut tvu_threads, tvu_filter) = Self::packet_modifier(
            "solRcvrShred",
            "solTvuPktMod",
            sockets,
            exit.clone(),
            sender.clone(),
            recycler.clone(),
            bank_forks.clone(),
            shred_version,
            "shred_fetch",
            "shred_fetch_receiver",
            PacketFlags::empty(),
            None, // repair_context
            turbine_disabled.clone(),
        );

        let (repair_receiver, repair_handler) = Self::packet_modifier(
            "solRcvrShredRep",
            "solTvuRepPktMod",
            vec![repair_socket],
            exit.clone(),
            sender.clone(),
            recycler.clone(),
            bank_forks.clone(),
            shred_version,
            "shred_fetch_repair",
            "shred_fetch_repair_receiver",
            PacketFlags::REPAIR,
            Some(repair_context.clone()),
            turbine_disabled.clone(),
        );

        tvu_threads.extend(repair_receiver);
        tvu_threads.push(tvu_filter);
        tvu_threads.push(repair_handler);
        // Repair shreds fetched over QUIC protocol.
        {
            let (packet_sender, packet_receiver) = unbounded();
            let bank_forks = bank_forks.clone();
            let exit = exit.clone();
            let sender = sender.clone();
            let turbine_disabled = turbine_disabled.clone();
            tvu_threads.extend([
                Builder::new()
                    .name("solTvuRecvRpr".to_string())
                    .spawn(|| {
                        receive_quic_datagrams(
                            repair_response_quic_receiver,
                            PacketFlags::REPAIR,
                            packet_sender,
                            exit,
                        )
                    })
                    .unwrap(),
                Builder::new()
                    .name("solTvuFetchRpr".to_string())
                    .spawn(move || {
                        let sharable_banks = bank_forks.read().unwrap().sharable_banks();
                        Self::modify_packets(
                            packet_receiver,
                            None,
                            sender,
                            &sharable_banks,
                            shred_version,
                            "shred_fetch_repair_quic",
                            PacketFlags::REPAIR,
                            // No ping packets but need to verify repair nonce.
                            Some(&repair_context),
                            turbine_disabled,
                        )
                    })
                    .unwrap(),
            ]);
        }
        // Turbine shreds fetched over QUIC protocol.
        let (packet_sender, packet_receiver) = unbounded();
        tvu_threads.extend([
            Builder::new()
                .name("solTvuRecvQuic".to_string())
                .spawn(|| {
                    receive_quic_datagrams(
                        turbine_quic_endpoint_receiver,
                        PacketFlags::empty(),
                        packet_sender,
                        exit,
                    )
                })
                .unwrap(),
            Builder::new()
                .name("solTvuFetchQuic".to_string())
                .spawn(move || {
                    let sharable_banks = bank_forks.read().unwrap().sharable_banks();
                    Self::modify_packets(
                        packet_receiver,
                        None,
                        sender,
                        &sharable_banks,
                        shred_version,
                        "shred_fetch_quic",
                        PacketFlags::empty(),
                        None, // repair_context
                        turbine_disabled,
                    )
                })
                .unwrap(),
        ]);
        Self {
            thread_hdls: tvu_threads,
        }
    }

    pub(crate) fn join(self) -> thread::Result<()> {
        for thread_hdl in self.thread_hdls {
            thread_hdl.join()?;
        }
        Ok(())
    }
}

impl RepairContext {
    fn keypair(&self) -> Arc<Keypair> {
        self.cluster_info.keypair()
    }
}

fn packet_payload_bytes(packet: PacketRef<'_>) -> Option<Bytes> {
    match packet {
        PacketRef::Bytes(pkt) => Some(pkt.buffer().clone()),
        PacketRef::Packet(pkt) => pkt.data(..).map(Bytes::copy_from_slice),
    }
}

// Returns false if repair nonce is invalid and packet should be discarded.
#[must_use]
fn verify_repair_nonce(
    packet: PacketRef,
    now: u64, // solana_time_utils::timestamp()
    outstanding_repair_requests: &mut OutstandingShredRepairs,
) -> bool {
    debug_assert!(packet.meta().flags.contains(PacketFlags::REPAIR));
    let Some((shred, Some(nonce))) = shred::layout::get_shred_and_repair_nonce(packet) else {
        return false;
    };
    outstanding_repair_requests
        .register_response(nonce, shred, now, |_| ())
        .is_some()
}

pub(crate) fn receive_quic_datagrams(
    quic_datagrams_receiver: Receiver<(Pubkey, SocketAddr, Bytes)>,
    flags: PacketFlags,
    sender: Sender<PacketBatch>,
    exit: Arc<AtomicBool>,
) {
    const RECV_TIMEOUT: Duration = Duration::from_secs(1);
    const PACKET_COALESCE_DURATION: Duration = Duration::from_millis(1);
    while !exit.load(Ordering::Relaxed) {
        let entry = match quic_datagrams_receiver.recv_timeout(RECV_TIMEOUT) {
            Ok(entry) => entry,
            Err(RecvTimeoutError::Timeout) => continue,
            Err(RecvTimeoutError::Disconnected) => return,
        };
        let deadline = Instant::now() + PACKET_COALESCE_DURATION;
        let entries = std::iter::once(entry).chain(
            std::iter::repeat_with(|| quic_datagrams_receiver.recv_deadline(deadline).ok())
                .while_some(),
        );
        let packet_batch: BytesPacketBatch = entries
            .filter(|(_, _, bytes)| bytes.len() <= PACKET_DATA_SIZE)
            .map(|(_pubkey, addr, bytes)| {
                let meta = Meta {
                    size: bytes.len(),
                    addr: addr.ip(),
                    port: addr.port(),
                    flags,
                };
                BytesPacket::new(bytes, meta)
            })
            .collect();
        if !packet_batch.is_empty() && sender.send(packet_batch.into()).is_err() {
            return; // The receiver end of the channel is disconnected.
        }
    }
}

// Returns true if the feature is effective for the shred slot.
#[must_use]
fn check_feature_activation(
    feature: &Pubkey,
    shred_slot: Slot,
    feature_set: &FeatureSet,
    epoch_schedule: &EpochSchedule,
) -> bool {
    match feature_set.activated_slot(feature) {
        None => false,
        Some(feature_slot) => {
            let feature_epoch = epoch_schedule.get_epoch(feature_slot);
            let shred_epoch = epoch_schedule.get_epoch(shred_slot);
            feature_epoch < shred_epoch
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            repair::serve_repair::ShredRepairType,
            solanacdn::{new_handle_for_tests, set_global_for_tests, SolanaCdnConfig},
        },
        solana_ledger::{
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            shred::Shredder,
        },
        solana_gossip::contact_info::ContactInfo,
        solana_perf::packet::PinnedPacketBatch,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        solana_streamer::socket::SocketAddrSpace,
        solana_time_utils::timestamp,
        std::net::{IpAddr, Ipv4Addr, SocketAddr},
    };

    struct GlobalGuard;
    impl Drop for GlobalGuard {
        fn drop(&mut self) {
            set_global_for_tests(None);
        }
    }

    fn run_repair_flag_case(repair_shreds: bool) -> Option<bool> {
        let mut cfg = SolanaCdnConfig::default();
        cfg.repair_shreds = repair_shreds;
        let handle = new_handle_for_tests(cfg);
        set_global_for_tests(Some(handle));
        let _guard = GlobalGuard;

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);
        let sharable_banks = bank_forks.read().unwrap().sharable_banks();

        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info =
            Arc::new(ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified));
        let repair_socket = match UdpSocket::bind("127.0.0.1:0") {
            Ok(sock) => Arc::new(sock),
            Err(err) => {
                eprintln!("skipping test (udp bind failed): {err}");
                return None;
            }
        };
        let outstanding = Arc::new(RwLock::new(OutstandingShredRepairs::default()));
        let repair_context = RepairContext {
            repair_socket,
            cluster_info,
            outstanding_repair_requests: outstanding.clone(),
        };

        let slot = 5;
        let shred_keypair = Keypair::new();
        let shred = Shredder::single_shred_for_tests(slot, &shred_keypair);
        let repair_type = ShredRepairType::Shred(slot, shred.index() as u64);
        let nonce = outstanding
            .write()
            .unwrap()
            .add_request(repair_type, timestamp());

        let mut packet = shred.payload().to_packet(Some(nonce));
        packet.meta_mut().flags |= PacketFlags::REPAIR;
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234);
        packet.meta_mut().set_socket_addr(&addr);
        let batch = PacketBatch::from(PinnedPacketBatch::new(vec![packet]));

        let (input_tx, input_rx) = unbounded();
        let (sendr, output_rx) = EvictingSender::new_bounded(1);
        let turbine_disabled = Arc::new(AtomicBool::new(false));

        let handle_thread = std::thread::spawn(move || {
            ShredFetchStage::modify_packets(
                input_rx,
                None,
                sendr,
                &sharable_banks,
                42,
                "test_repair",
                PacketFlags::REPAIR,
                Some(&repair_context),
                turbine_disabled,
            );
        });

        input_tx.send(batch).unwrap();
        drop(input_tx);

        let out_batch = output_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let discard = out_batch
            .iter()
            .next()
            .expect("packet")
            .meta()
            .discard();

        handle_thread.join().unwrap();
        Some(discard)
    }

    #[test]
    fn solanacdn_no_repair_drops_repair_packets() {
        let allow = run_repair_flag_case(true);
        let deny = run_repair_flag_case(false);
        if allow.is_none() || deny.is_none() {
            return;
        }
        assert!(!allow.unwrap());
        assert!(deny.unwrap());
    }
}
