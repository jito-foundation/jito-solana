//! The `retransmit_stage` retransmits shreds between validators

use {
    crate::{
        MAX_SHRED_RECEIVER_ADDRESSES, ShredReceiverAddresses, XdpSender,
        cluster_nodes::{
            ClusterNodes, ClusterNodesCache, DATA_PLANE_FANOUT, Error, MAX_NUM_TURBINE_HOPS,
        },
    },
    agave_votor::event::VotorEvent,
    agave_votor_messages::migration::MigrationStatus,
    agave_worker_pool::{WorkerPool, WorkerPoolConfig},
    arc_swap::ArcSwap,
    crossbeam_channel::{Receiver, Sender, TrySendError},
    lru::LruCache,
    rand::Rng,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
    solana_ledger::{
        leader_schedule_cache::LeaderScheduleCache,
        shred::{
            self, ShredFlags, ShredId, ShredType,
            filter::{RetransmitSender, ShredBatch},
        },
    },
    solana_measure::measure::Measure,
    solana_net_utils::{SocketAddrSpace, bind_to_unspecified},
    solana_perf::deduper::Deduper,
    solana_pubkey::Pubkey,
    solana_rpc::{
        max_slots::MaxSlots, rpc_subscriptions::RpcSubscriptions,
        slot_status_notifier::SlotStatusNotifier,
    },
    solana_rpc_client_api::response::SlotUpdate,
    solana_runtime::{
        bank::{Bank, MAX_LEADER_SCHEDULE_STAKES},
        bank_forks::{BankForks, BankPair, SharableBanks},
    },
    solana_streamer::sendmmsg::{SendPktsError, multi_target_send},
    solana_time_utils::timestamp,
    std::{
        collections::{HashMap, HashSet},
        net::{SocketAddr, UdpSocket},
        num::NonZeroUsize,
        ops::AddAssign,
        sync::{Arc, RwLock, atomic::Ordering},
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const MAX_DUPLICATE_COUNT: usize = 2;
const DEDUPER_FALSE_POSITIVE_RATE: f64 = 0.001;
const DEDUPER_NUM_BITS: u64 = 637_534_199; // 76MB
const DEDUPER_RESET_CYCLE: Duration = Duration::from_secs(5 * 60);

/// Sets the upper bound on the number of batches stored in the retransmit stage ingress channel.
///
/// With `RETRANSMIT_SHRED_BATCH_SIZE` currently 16, the queue holds up to 262,144 shreds, or 307
/// MiB of payload data at 1,228 bytes per shred, excluding allocation overhead.
const CHANNEL_SIZE_RETRANSMIT_INGRESS: NonZeroUsize = NonZeroUsize::new(16 * 1024).unwrap();

/// Number of completed-batch statistics buffered per retransmit worker. At the measured average of
/// ~1.6 ms per batch, this absorbs ~32 ms of completions from fully occupied workers, starting from
/// an empty queue.
const STATS_BATCHES_PER_WORKER: usize = 20;

const _: () = const {
    // From https://github.com/anza-xyz/agave/pull/1735#discussion_r1644899183:
    // 1. There must be at least two epochs because near an epoch boundary you might receive
    //    shreds from the other side of the epoch boundary.
    // 2. It does not make sense to have capacity more than the number of epoch-stakes in Bank.
    assert!(CLUSTER_NODES_CACHE_NUM_EPOCH_CAP >= 2);
    assert!(CLUSTER_NODES_CACHE_NUM_EPOCH_CAP <= MAX_LEADER_SCHEDULE_STAKES as usize);
};
const CLUSTER_NODES_CACHE_NUM_EPOCH_CAP: usize = MAX_LEADER_SCHEDULE_STAKES as usize;
const CLUSTER_NODES_CACHE_TTL: Duration = Duration::from_secs(5);

// Output of fn retransmit_shred(...).
struct RetransmitShredOutput {
    shred: ShredId,
    // If the shred has ShredFlags::LAST_SHRED_IN_SLOT.
    last_shred_in_slot: bool,
    // This node's distance from the turbine root.
    root_distance: u8,
    // Number of nodes the shred was retransmitted to.
    num_nodes: usize,
}

#[derive(Default)]
pub(crate) struct RetransmitSlotStats {
    asof: u64,   // Latest timestamp struct was updated.
    outset: u64, // 1st shred retransmit timestamp.
    // Maximum code and data indices observed.
    pub(crate) max_index_code: u32,
    pub(crate) max_index_data: u32,
    // If any of the shreds had ShredFlags::LAST_SHRED_IN_SLOT.
    pub(crate) last_shred_in_slot: bool,
    // Number of shreds sent and received at different
    // distances from the turbine broadcast root.
    pub(crate) num_shreds_received: [usize; MAX_NUM_TURBINE_HOPS],
    num_shreds_sent: [usize; MAX_NUM_TURBINE_HOPS],
}

struct RetransmitStats {
    since: Instant,
    num_nodes: usize,
    num_addrs_failed: usize,
    num_shreds_dropped_xdp_full: usize,
    num_loopback_errs: usize,
    num_shreds: usize,
    num_shreds_skipped: usize,
    num_small_batches: usize,
    total_batches: usize,
    /// Sum of workers elapsed batch-processing time (us).
    total_worker_time_us: u64,
    epoch_fetch: u64,
    epoch_cache_update: u64,
    retransmit_total: u64,
    compute_turbine_peers_total: u64,
    slot_stats: LruCache<Slot, RetransmitSlotStats>,
    unknown_shred_slot_leader: usize,
}

struct RetransmitNotifiers {
    rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
    slot_status_notifier: Option<SlotStatusNotifier>,
    migration_status: Arc<MigrationStatus>,
    votor_event_sender: Sender<VotorEvent>,
}

struct WorkerContext {
    sharable_banks: SharableBanks,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    cluster_info: Arc<ClusterInfo>,
    retransmit_sockets: Arc<Vec<UdpSocket>>,
    external_sender_socket: UdpSocket,
    xdp_sender: Option<XdpSender>,
    cluster_nodes_cache: Arc<ClusterNodesCache<RetransmitStage>>,
    shred_deduper: Arc<ShredDeduper>,
    max_slots: Arc<MaxSlots>,
    shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
    bam_shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
    shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
    multicast_root_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
    stats_sender: Sender<BatchStats>,
}

impl RetransmitStats {
    fn maybe_submit(
        &mut self,
        sharable_banks: &SharableBanks,
        cluster_info: &ClusterInfo,
        cluster_nodes_cache: &ClusterNodesCache<RetransmitStage>,
        is_xdp: bool,
    ) {
        const SUBMIT_CADENCE: Duration = Duration::from_secs(2);
        if self.since.elapsed() < SUBMIT_CADENCE {
            return;
        }
        let BankPair {
            root_bank,
            working_bank,
        } = sharable_banks.load();
        cluster_nodes_cache
            .get(root_bank.slot(), &root_bank, &working_bank, cluster_info)
            .submit_metrics("cluster_nodes_retransmit", timestamp());
        datapoint_info!(
            "retransmit-stage",
            "is_xdp" => is_xdp.to_string(),
            ("total_worker_time_us", self.total_worker_time_us, i64),
            ("epoch_fetch", self.epoch_fetch, i64),
            ("epoch_cache_update", self.epoch_cache_update, i64),
            ("total_batches", self.total_batches, i64),
            ("num_small_batches", self.num_small_batches, i64),
            ("num_nodes", self.num_nodes, i64),
            ("num_addrs_failed", self.num_addrs_failed, i64),
            (
                "num_shreds_dropped_xdp_full",
                self.num_shreds_dropped_xdp_full,
                i64
            ),
            ("num_loopback_errs", self.num_loopback_errs, i64),
            ("num_shreds", self.num_shreds, i64),
            ("num_shreds_skipped", self.num_shreds_skipped, i64),
            ("retransmit_total", self.retransmit_total, i64),
            (
                "compute_turbine",
                self.compute_turbine_peers_total,
                i64
            ),
            (
                "unknown_shred_slot_leader",
                self.unknown_shred_slot_leader,
                i64
            ),
        );
        // slot_stats are submitted at a different cadence.
        let old = std::mem::replace(self, Self::new(Instant::now()));
        self.slot_stats = old.slot_stats;
    }
}

struct ShredDeduper<const K: usize = 2> {
    deduper: Deduper<K, /*shred:*/ [u8]>,
    shred_id_filter: Deduper<K, (ShredId, /*0..MAX_DUPLICATE_COUNT:*/ usize)>,
}

impl<const K: usize> ShredDeduper<K> {
    fn new<R: Rng>(rng: &mut R, num_bits: u64) -> Self {
        Self {
            deduper: Deduper::new(rng, num_bits),
            shred_id_filter: Deduper::new(rng, num_bits),
        }
    }

    fn maybe_reset<R: Rng>(&self, rng: &mut R, false_positive_rate: f64, reset_cycle: Duration) {
        self.deduper
            .maybe_reset(rng, false_positive_rate, reset_cycle);
        self.shred_id_filter
            .maybe_reset(rng, false_positive_rate, reset_cycle);
    }

    // Returns true if the shred is duplicate and should be discarded.
    #[must_use]
    fn dedup(&self, key: ShredId, shred: &[u8], max_duplicate_count: usize) -> bool {
        // Shreds in the retransmit stage:
        //   * don't have repair nonce (repaired shreds are not retransmitted).
        //   * are already resigned by this node as the retransmitter.
        //   * have their leader's signature verified.
        // Therefore in order to dedup shreds, it suffices to compare:
        //    (signature, slot, shred-index, shred-type)
        // Because ShredCommonHeader already includes all of the above tuple,
        // the rest of the payload can be skipped.
        // In order to detect duplicate blocks across cluster, we retransmit
        // max_duplicate_count different shreds for each ShredId.
        shred::layout::get_common_header_bytes(shred)
            .map(|header| self.deduper.dedup(header))
            .unwrap_or(true)
            || (0..max_duplicate_count).all(|i| self.shred_id_filter.dedup(&(key, i)))
    }
}

type SlotLeaderCache = HashMap<
    Slot,
    (
        /*leader:*/ Pubkey,
        Arc<ClusterNodes<RetransmitStage>>,
        /*include_bam_shred_receivers:*/ bool,
    ),
>;

#[derive(Default)]
struct BatchStats {
    root: Slot,
    slot_stats: Vec<(Slot, RetransmitSlotStats)>,
    num_shreds: usize,
    num_small_batches: usize,
    total_batches: usize,
    total_time: u64,
    epoch_fetch: u64,
    unknown_shred_slot_leader: usize,
    num_nodes: usize,
    num_addrs_failed: usize,
    num_shreds_dropped_xdp_full: usize,
    num_loopback_errs: usize,
    num_shreds_skipped: usize,
    retransmit_total: u64,
    compute_turbine_peers_total: u64,
}

impl BatchStats {
    fn record_slot_stats(&mut self, shred_stats: RetransmitShredOutput) {
        let now = timestamp();
        let slot = shred_stats.shred.slot();

        if let Some((_, stats)) = self.slot_stats.iter_mut().find(|(s, _)| *s == slot) {
            stats.record(now, shred_stats);
        } else {
            let mut stats = RetransmitSlotStats::default();
            stats.record(now, shred_stats);
            self.slot_stats.push((slot, stats));
        }
    }
}

enum RetransmitSocket<'a> {
    Socket(&'a UdpSocket),
    Xdp(&'a XdpSender),
    Multihomed {
        sockets: &'a [UdpSocket],
        interface_offset: usize,
        sockets_per_interface: usize,
        thread_index: usize,
    },
}

impl<'a> RetransmitSocket<'a> {
    pub fn new(
        thread_index: usize,
        retransmit_sockets: &'a [UdpSocket],
        xdp_sender: Option<&'a XdpSender>,
        cluster_info: &'a ClusterInfo,
    ) -> Self {
        if let Some(sender) = xdp_sender {
            RetransmitSocket::Xdp(sender)
        } else if cluster_info.bind_ip_addrs().multihoming_enabled() {
            let sockets_per_interface =
                retransmit_sockets.len() / cluster_info.bind_ip_addrs().len();
            let active_index = cluster_info.bind_ip_addrs().active_index();
            let interface_offset = sockets_per_interface.saturating_mul(active_index);

            RetransmitSocket::Multihomed {
                sockets: retransmit_sockets,
                interface_offset,
                sockets_per_interface,
                thread_index,
            }
        } else {
            let socket: &UdpSocket = &retransmit_sockets[thread_index % retransmit_sockets.len()];
            RetransmitSocket::Socket(socket)
        }
    }

    pub fn get_socket(&self) -> &'a UdpSocket {
        match self {
            RetransmitSocket::Socket(socket) => socket,
            RetransmitSocket::Multihomed {
                sockets,
                interface_offset,
                sockets_per_interface,
                thread_index,
            } => {
                let socket_index = interface_offset + (thread_index % sockets_per_interface);
                &sockets[socket_index]
            }
            RetransmitSocket::Xdp(_) => {
                unreachable!("get_socket() should not be called for XDP variants")
            }
        }
    }
}

fn retransmit_batch(mut shreds: ShredBatch, worker_index: usize, context: &WorkerContext) {
    let mut total_timer = Measure::start("retransmit");

    let mut stats = BatchStats {
        num_shreds: shreds.len(),
        num_small_batches: usize::from(shreds.len() < 2),
        total_batches: 1,
        ..BatchStats::default()
    };

    let mut epoch_fetch = Measure::start("retransmit_epoch_fetch");
    let BankPair {
        root_bank,
        working_bank,
    } = context.sharable_banks.load();
    epoch_fetch.stop();
    stats.epoch_fetch = epoch_fetch.as_us();
    stats.root = root_bank.slot();

    let shred_slots: HashSet<Slot> = shreds
        .iter()
        .filter_map(|shred| shred::layout::get_slot(shred))
        .collect();
    let shred_receiver_addresses = context.shred_receiver_addresses.load_full();
    let bam_shred_receiver_addresses = context.bam_shred_receiver_addresses.load_full();
    let shredstream_receiver_address = **context.shredstream_receiver_address.load();
    let multicast_root_receiver_address = **context.multicast_root_receiver_address.load();
    let leader_schedule_cache = context.leader_schedule_cache.as_ref();
    let my_pubkey = context.cluster_info.id();
    let mut leader_schedule_epoch = None;
    let mut leader_schedule = None;
    let mut leader_at_slot = |slot| {
        let (epoch, slot_index) = working_bank.get_epoch_and_slot_index(slot);
        let mut computed_slot_leader = None;
        if leader_schedule_epoch != Some(epoch) {
            leader_schedule = leader_schedule_cache.get_epoch_leader_schedule(epoch);
            if leader_schedule.is_none() {
                computed_slot_leader =
                    leader_schedule_cache.slot_leader_at(slot, Some(&working_bank));
                leader_schedule = leader_schedule_cache.get_epoch_leader_schedule(epoch);
            }
            leader_schedule_epoch = Some(epoch);
        }
        leader_schedule
            .as_ref()
            .map(|leader_schedule| leader_schedule[slot_index])
            .or(computed_slot_leader)
            .or_else(|| leader_schedule_cache.slot_leader_at(slot, Some(&working_bank)))
    };

    // Resolve the leader and retransmit tree once for each distinct slot in this capped batch.
    let cache: SlotLeaderCache = shred_slots
        .into_iter()
        .filter_map(|slot| {
            context
                .max_slots
                .retransmit
                .fetch_max(slot, Ordering::Relaxed);
            // Shreds have already passed signature verification, so the leader should be known.
            let Some(slot_leader) = leader_at_slot(slot) else {
                stats.unknown_shred_slot_leader += shreds
                    .iter()
                    .filter(|shred| shred::layout::get_slot(shred) == Some(slot))
                    .count();
                return None;
            };
            let include_bam_shred_receivers = !bam_shred_receiver_addresses.is_empty()
                && slot_leader.id != my_pubkey
                && (1..=4).any(|offset| {
                    slot.checked_add(offset).is_some_and(|lookahead_slot| {
                        lookahead_slot % NUM_CONSECUTIVE_LEADER_SLOTS.get() as u64 == 0
                            && lookahead_slot > working_bank.slot()
                            && leader_at_slot(lookahead_slot)
                                .is_some_and(|leader| leader.id == my_pubkey)
                    })
                });
            let cluster_nodes = context.cluster_nodes_cache.get(
                slot,
                &root_bank,
                &working_bank,
                &context.cluster_info,
            );
            Some((
                slot,
                (slot_leader.id, cluster_nodes, include_bam_shred_receivers),
            ))
        })
        .collect();

    let socket_addr_space = context.cluster_info.socket_addr_space();
    let socket = RetransmitSocket::new(
        worker_index,
        &context.retransmit_sockets,
        context.xdp_sender.as_ref(),
        &context.cluster_info,
    );
    for shred in shreds.drain(..) {
        let shred_stats = retransmit_shred(
            shred,
            &root_bank,
            &context.shred_deduper,
            &cache,
            socket_addr_space,
            &socket,
            &context.external_sender_socket,
            &mut stats,
            &shred_receiver_addresses,
            &bam_shred_receiver_addresses,
            &shredstream_receiver_address,
            &multicast_root_receiver_address,
        );
        if let Some(shred_stats) = shred_stats {
            stats.record_slot_stats(shred_stats);
        }
    }
    total_timer.stop();
    stats.total_time = total_timer.as_us();
    // If receiver has been dropped, retransmission has already completed and only these per-job
    // stats are lost. The worker has no recovery to perform.
    let _ = context.stats_sender.try_send(stats);
}

// Retransmit a single shred to all downstream nodes
#[allow(clippy::too_many_arguments)]
fn retransmit_shred(
    shred: shred::Payload,
    root_bank: &Bank,
    shred_deduper: &ShredDeduper,
    slot_leader_cache: &SlotLeaderCache,
    socket_addr_space: &SocketAddrSpace,
    socket: &RetransmitSocket<'_>,
    external_sender_socket: &UdpSocket,
    stats: &mut BatchStats,
    shred_receiver_addresses: &ShredReceiverAddresses,
    bam_shred_receiver_addresses: &ShredReceiverAddresses,
    shredstream_receiver_address: &Option<SocketAddr>,
    multicast_root_receiver_address: &Option<SocketAddr>,
) -> Option<RetransmitShredOutput> {
    let key = shred::layout::get_shred_id(shred.as_ref())?;
    if key.slot() < root_bank.slot()
        || shred_deduper.dedup(key, shred.as_ref(), MAX_DUPLICATE_COUNT)
    {
        stats.num_shreds_skipped += 1;
        return None;
    }
    let mut compute_turbine_peers = Measure::start("turbine_start");
    let (root_distance, addrs, include_bam_shred_receivers) =
        get_retransmit_addrs(&key, slot_leader_cache, socket_addr_space, stats)?;
    compute_turbine_peers.stop();
    stats.compute_turbine_peers_total += compute_turbine_peers.as_us();
    let last_shred_in_slot = shred::wire::get_flags(shred.as_ref())
        .map(|flags| flags.contains(ShredFlags::LAST_SHRED_IN_SLOT))
        .unwrap_or_default();
    let mut retransmit_time = Measure::start("retransmit_to");
    let mut turbine_addrs = Vec::with_capacity(addrs.len());
    for addr in addrs.iter() {
        if !turbine_addrs.contains(addr) {
            turbine_addrs.push(*addr);
        }
    }
    let num_shred_receiver_addresses = shred_receiver_addresses
        .len()
        .min(MAX_SHRED_RECEIVER_ADDRESSES);
    let external_addr_capacity = num_shred_receiver_addresses
        .saturating_add(if include_bam_shred_receivers {
            bam_shred_receiver_addresses.len()
        } else {
            0
        })
        .saturating_add(usize::from(root_distance == 0) * 2);
    let mut external_addrs = ShredReceiverAddresses::with_capacity(external_addr_capacity);
    let root_external_addrs = if root_distance == 0 {
        [
            multicast_root_receiver_address.as_ref(),
            shredstream_receiver_address.as_ref(),
        ]
    } else {
        [None, None]
    };
    for &addr in include_bam_shred_receivers
        .then_some(bam_shred_receiver_addresses)
        .into_iter()
        .flatten()
        .chain(root_external_addrs.into_iter().flatten())
        .chain(
            shred_receiver_addresses
                .iter()
                .take(num_shred_receiver_addresses),
        )
    {
        if !turbine_addrs.contains(&addr) && !external_addrs.contains(&addr) {
            external_addrs.push(addr);
        }
    }
    let num_addrs = turbine_addrs.len();
    let num_nodes = match socket {
        RetransmitSocket::Xdp(sender) => {
            let mut sent = num_addrs;
            let mut send_addrs =
                Vec::with_capacity(turbine_addrs.len().saturating_add(external_addrs.len()));
            send_addrs.extend(turbine_addrs.iter().copied());
            send_addrs.extend(external_addrs.iter().copied());
            if !send_addrs.is_empty()
                && let Err(e) =
                    sender.try_send(key.index() as usize, send_addrs, shred.bytes.clone())
            {
                // External shred receivers are intentionally not included in retransmit stats.
                log::warn!("xdp channel full: {e:?}");
                stats.num_shreds_dropped_xdp_full += num_addrs;
                sent = 0;
            }
            sent
        }
        RetransmitSocket::Socket(_) | RetransmitSocket::Multihomed { .. } => {
            let socket = socket.get_socket();
            let sent = if turbine_addrs.is_empty() {
                0usize
            } else {
                match multi_target_send(socket, &shred, &turbine_addrs) {
                    Ok(num_sent) => num_sent,
                    Err(SendPktsError::IoError(ioerr)) => {
                        error!(
                            "retransmit_to multi_target_send error: {ioerr:?}, {num_addrs} \
                             destinations"
                        );
                        0
                    }
                }
            };
            if !external_addrs.is_empty()
                && let Err(SendPktsError::IoError(ioerr)) =
                    multi_target_send(external_sender_socket, &shred, &external_addrs)
            {
                warn!(
                    "retransmit_to external multi_target_send error: {ioerr:?}, {} destinations",
                    external_addrs.len()
                );
            }
            sent
        }
    };
    retransmit_time.stop();
    stats.num_addrs_failed += num_addrs - num_nodes;
    stats.num_nodes += num_nodes;
    stats.retransmit_total += retransmit_time.as_us();
    Some(RetransmitShredOutput {
        shred: key,
        last_shred_in_slot,
        root_distance,
        num_nodes,
    })
}

#[allow(clippy::type_complexity)]
fn get_retransmit_addrs(
    shred: &ShredId,
    cache: &SlotLeaderCache,
    socket_addr_space: &SocketAddrSpace,
    stats: &mut BatchStats,
) -> Option<(
    /*root_distance:*/ u8,
    Vec<SocketAddr>,
    /*include_bam_shred_receivers:*/ bool,
)> {
    let (slot_leader, cluster_nodes, include_bam_shred_receivers) = cache.get(&shred.slot())?;
    let (root_distance, addrs) = cluster_nodes
        .get_retransmit_addrs(slot_leader, shred, DATA_PLANE_FANOUT, socket_addr_space)
        .inspect_err(|err| match err {
            Error::Loopback { .. } => {
                stats.num_loopback_errs += 1;
            }
        })
        .ok()?;
    Some((root_distance, addrs, *include_bam_shred_receivers))
}

/// Service to retransmit messages received from other peers in turbine.
pub struct RetransmitStage {
    stats_handle: JoinHandle<()>,
}

impl RetransmitStage {
    /// Construct the RetransmitStage.
    ///
    /// Key arguments:
    /// * `retransmit_sockets` - Sockets to use for transmission of shreds
    /// * `max_slots` - Structure to keep track of the Turbine progress
    /// * `bank_forks` - Reference to the BankForks structure
    /// * `leader_schedule_cache` - The leader schedule to verify shreds
    /// * `cluster_info` - This structure needs to be updated and populated by the bank and via gossip.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        bank_forks: Arc<RwLock<BankForks>>,
        leader_schedule_cache: Arc<LeaderScheduleCache>,
        cluster_info: Arc<ClusterInfo>,
        retransmit_sockets: Arc<Vec<UdpSocket>>,
        max_slots: Arc<MaxSlots>,
        rpc_subscriptions: Option<Arc<RpcSubscriptions>>,
        slot_status_notifier: Option<SlotStatusNotifier>,
        xdp_sender: Option<XdpSender>,
        votor_event_sender: Sender<VotorEvent>,
        shredstream_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
        shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
        bam_shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
        multicast_root_receiver_address: Arc<ArcSwap<Option<SocketAddr>>>,
    ) -> (RetransmitSender, Self) {
        let (sharable_banks, migration_status) = {
            let bank_forks = bank_forks.read().unwrap();
            (bank_forks.sharable_banks(), bank_forks.migration_status())
        };
        let cluster_nodes_cache = Arc::new(ClusterNodesCache::<RetransmitStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        ));
        let mut rng = rand::rng();
        let shred_deduper = Arc::new(ShredDeduper::new(&mut rng, DEDUPER_NUM_BITS));

        let num_workers = NonZeroUsize::new(retransmit_sockets.len())
            .expect("retransmit requires at least one socket");
        let (stats_sender, stats_receiver) =
            crossbeam_channel::bounded(STATS_BATCHES_PER_WORKER * num_workers.get());
        let is_xdp = xdp_sender.is_some();
        let external_sender_socket = {
            let socket =
                bind_to_unspecified().expect("bind retransmit external_sender_socket 0.0.0.0:0");
            if let Err(err) = socket.set_multicast_ttl_v4(64) {
                warn!("retransmit external_sender_socket: failed to set multicast TTL: {err}");
            }
            socket
        };
        let context = Arc::new(WorkerContext {
            sharable_banks: sharable_banks.clone(),
            leader_schedule_cache,
            cluster_info: cluster_info.clone(),
            retransmit_sockets,
            external_sender_socket,
            xdp_sender,
            cluster_nodes_cache: cluster_nodes_cache.clone(),
            shred_deduper: shred_deduper.clone(),
            max_slots,
            shred_receiver_addresses,
            bam_shred_receiver_addresses,
            shredstream_receiver_address,
            multicast_root_receiver_address,
            stats_sender,
        });

        // Match workers to retransmit sockets to preserve socket affinity.
        let config = WorkerPoolConfig {
            job_queue_capacity: CHANNEL_SIZE_RETRANSMIT_INGRESS,
            stack_size: Some(2 * 1024 * 1024),
            ..WorkerPoolConfig::new("solRetransmit", num_workers)
        };
        let retransmit_sender = Arc::new(
            WorkerPool::spawn(config, |worker_index| {
                let context = Arc::clone(&context);
                move |shreds| retransmit_batch(shreds, worker_index, context.as_ref())
            })
            .expect("retransmit workers should spawn"),
        );

        let stats_thread_context = StatsThreadContext {
            sharable_banks,
            cluster_info,
            cluster_nodes_cache,
            shred_deduper,
            is_xdp,
            notifiers: RetransmitNotifiers {
                rpc_subscriptions,
                slot_status_notifier,
                migration_status,
                votor_event_sender,
            },
        };

        let stats_handle = Builder::new()
            .name("solRetransSvc".to_string())
            .spawn(move || process_stats_update(stats_thread_context, stats_receiver))
            .unwrap();

        (retransmit_sender, Self { stats_handle })
    }

    pub fn join(self) -> thread::Result<()> {
        self.stats_handle.join()
    }
}

struct StatsThreadContext {
    sharable_banks: SharableBanks,
    cluster_info: Arc<ClusterInfo>,
    cluster_nodes_cache: Arc<ClusterNodesCache<RetransmitStage>>,
    shred_deduper: Arc<ShredDeduper>,
    is_xdp: bool,
    notifiers: RetransmitNotifiers,
}

impl AddAssign for RetransmitSlotStats {
    fn add_assign(&mut self, other: Self) {
        let Self {
            asof,
            outset,
            max_index_code,
            max_index_data,
            last_shred_in_slot,
            num_shreds_received,
            num_shreds_sent,
        } = other;
        self.asof = self.asof.max(asof);
        self.max_index_code = self.max_index_code.max(max_index_code);
        self.max_index_data = self.max_index_data.max(max_index_data);
        self.last_shred_in_slot |= last_shred_in_slot;
        self.outset = if self.outset == 0 {
            outset
        } else {
            self.outset.min(outset)
        };
        for k in 0..MAX_NUM_TURBINE_HOPS {
            self.num_shreds_received[k] += num_shreds_received[k];
            self.num_shreds_sent[k] += num_shreds_sent[k];
        }
    }
}

/// * Accumulates and reports retransmit stats.
/// * Sends first-shred notifications to Votor.
/// * Sends first-shred notifications to Geyser and RPC.
/// * Resets shared dedupers when needed.
///
/// Geyser plugin callbacks execute synchronously on this thread. A slow or blocked plugin delays
/// deduper reset checks, stats processing, and Votor first-shred notifications.
fn process_stats_update(
    StatsThreadContext {
        sharable_banks,
        cluster_info,
        cluster_nodes_cache,
        shred_deduper,
        is_xdp,
        notifiers,
    }: StatsThreadContext,
    receiver: Receiver<BatchStats>,
) {
    let mut stats = RetransmitStats::new(Instant::now());
    let mut pending_first_shred_event = None;
    for batch_stats in receiver.iter() {
        // Attempt to resend a pending first shred event to votor
        if let Some(event) = pending_first_shred_event.take()
            && let Err(TrySendError::Full(event)) = notifiers.votor_event_sender.try_send(event)
        {
            // Failed again, requeue
            pending_first_shred_event = Some(event);
        }
        let mut epoch_cache_update = Measure::start("retransmit_epoch_cache_update");
        shred_deduper.maybe_reset(
            &mut rand::rng(),
            DEDUPER_FALSE_POSITIVE_RATE,
            DEDUPER_RESET_CYCLE,
        );
        epoch_cache_update.stop();
        stats.epoch_cache_update += epoch_cache_update.as_us();

        stats.accumulate_job_counters(&batch_stats);
        stats.insert_slot_stats(
            batch_stats.slot_stats,
            batch_stats.root,
            &notifiers,
            &mut pending_first_shred_event,
        );
        stats.submit_slot_stats();

        stats.maybe_submit(
            &sharable_banks,
            cluster_info.as_ref(),
            &cluster_nodes_cache,
            is_xdp,
        );
    }
}

impl RetransmitStats {
    const SLOT_STATS_CACHE_CAPACITY: usize = 750;

    fn new(now: Instant) -> Self {
        Self {
            since: now,
            num_nodes: 0usize,
            num_addrs_failed: 0usize,
            num_shreds_dropped_xdp_full: 0usize,
            num_loopback_errs: 0usize,
            num_shreds: 0usize,
            num_shreds_skipped: 0usize,
            total_batches: 0usize,
            num_small_batches: 0usize,
            total_worker_time_us: 0u64,
            epoch_fetch: 0u64,
            epoch_cache_update: 0u64,
            retransmit_total: 0u64,
            compute_turbine_peers_total: 0u64,
            // Cache capacity is manually enforced by `SLOT_STATS_CACHE_CAPACITY`
            slot_stats: LruCache::<Slot, RetransmitSlotStats>::unbounded(),
            unknown_shred_slot_leader: 0usize,
        }
    }

    fn insert_slot_stats(
        &mut self,
        feed: impl IntoIterator<Item = (Slot, RetransmitSlotStats)>,
        root: Slot,
        notifiers: &RetransmitNotifiers,
        pending_first_shred_event: &mut Option<VotorEvent>,
    ) {
        for (slot, slot_stats) in feed {
            match self.slot_stats.get_mut(&slot) {
                None => {
                    if slot > root {
                        notify_subscribers(
                            slot,
                            slot_stats.outset,
                            notifiers,
                            pending_first_shred_event,
                        );
                    }
                    self.slot_stats.put(slot, slot_stats);
                }
                Some(entry) => {
                    *entry += slot_stats;
                }
            }
        }
    }

    fn submit_slot_stats(&mut self) {
        while self.slot_stats.len() > Self::SLOT_STATS_CACHE_CAPACITY {
            // Pop and submit metrics for the slot which was updated least
            // recently. At this point the node most likely will not receive
            // and retransmit any more shreds for this slot.
            match self.slot_stats.pop_lru() {
                Some((slot, stats)) => stats.submit(slot),
                None => break,
            }
        }
    }

    fn accumulate_job_counters(&mut self, job_stats: &BatchStats) {
        self.num_shreds += job_stats.num_shreds;
        self.num_small_batches += job_stats.num_small_batches;
        self.total_batches += job_stats.total_batches;
        self.num_nodes += job_stats.num_nodes;
        self.num_addrs_failed += job_stats.num_addrs_failed;
        self.num_shreds_dropped_xdp_full += job_stats.num_shreds_dropped_xdp_full;
        self.num_loopback_errs += job_stats.num_loopback_errs;
        self.num_shreds_skipped += job_stats.num_shreds_skipped;
        self.total_worker_time_us += job_stats.total_time;
        self.epoch_fetch += job_stats.epoch_fetch;
        self.retransmit_total += job_stats.retransmit_total;
        self.compute_turbine_peers_total += job_stats.compute_turbine_peers_total;
        self.unknown_shred_slot_leader += job_stats.unknown_shred_slot_leader;
    }
}

impl RetransmitSlotStats {
    fn record(&mut self, now: u64, out: RetransmitShredOutput) {
        self.outset = if self.outset == 0 {
            now
        } else {
            self.outset.min(now)
        };
        self.asof = self.asof.max(now);
        let max_index = match out.shred.shred_type() {
            ShredType::Code => &mut self.max_index_code,
            ShredType::Data => &mut self.max_index_data,
        };
        *max_index = (*max_index).max(out.shred.index());
        self.last_shred_in_slot |= out.last_shred_in_slot;
        self.num_shreds_received[usize::from(out.root_distance)] += 1;
        self.num_shreds_sent[usize::from(out.root_distance)] += out.num_nodes;
    }

    fn submit(&self, slot: Slot) {
        let num_shreds: usize = self.num_shreds_received.iter().sum();
        let num_nodes: usize = self.num_shreds_sent.iter().sum();
        let elapsed_millis = self.asof.saturating_sub(self.outset);
        datapoint_info!(
            "retransmit-stage-slot-stats",
            ("slot", slot, i64),
            ("outset_timestamp", self.outset, i64),
            ("elapsed_millis", elapsed_millis, i64),
            ("num_shreds", num_shreds, i64),
            ("num_nodes", num_nodes, i64),
            ("num_shreds_received_root", self.num_shreds_received[0], i64),
            (
                "num_shreds_received_1st_layer",
                self.num_shreds_received[1],
                i64
            ),
            (
                "num_shreds_received_2nd_layer",
                self.num_shreds_received[2],
                i64
            ),
            (
                "num_shreds_received_3rd_layer",
                self.num_shreds_received[3],
                i64
            ),
            ("num_shreds_sent_root", self.num_shreds_sent[0], i64),
            ("num_shreds_sent_1st_layer", self.num_shreds_sent[1], i64),
            ("num_shreds_sent_2nd_layer", self.num_shreds_sent[2], i64),
            ("num_shreds_sent_3rd_layer", self.num_shreds_sent[3], i64),
        );
    }
}

// Notifies subscribers of shreds received from a new slot.
fn notify_subscribers(
    slot: Slot,
    timestamp: u64, // When the first shred in the slot was received.
    notifiers: &RetransmitNotifiers,
    pending_first_shred_event: &mut Option<VotorEvent>,
) {
    if let Some(rpc_subscriptions) = notifiers.rpc_subscriptions.as_ref() {
        let slot_update = SlotUpdate::FirstShredReceived { slot, timestamp };
        rpc_subscriptions.notify_slot_update(slot_update);
        datapoint_info!("retransmit-first-shred", ("slot", slot, i64));
    }
    if let Some(slot_status_notifier) = notifiers.slot_status_notifier.as_ref() {
        slot_status_notifier
            .read()
            .unwrap()
            .notify_first_shred_received(slot);
    }

    if notifiers.migration_status.should_send_first_shred(slot) {
        match notifiers
            .votor_event_sender
            .try_send(VotorEvent::FirstShred(slot))
        {
            Ok(()) => (),
            Err(TrySendError::Full(event)) => {
                error!(
                    "Votor event channel is backed up len {}, something is wrong",
                    notifiers.votor_event_sender.len(),
                );
                // Only the latest first shred notification matters, requeue
                pending_first_shred_event.replace(event);
            }
            Err(TrySendError::Disconnected(_)) => {
                info!("Votor event channel disconnected, we are shutting down")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::TryRecvError,
        rand::SeedableRng,
        rand_chacha::ChaChaRng,
        solana_cluster_type::ClusterType,
        solana_entry::entry::create_ticks,
        solana_gossip::{cluster_info::ClusterInfo, node::Node},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_leader_schedule::NUM_CONSECUTIVE_LEADER_SLOTS,
        solana_ledger::{
            genesis_utils::create_genesis_config,
            shred::{ProcessShredsStats, Shredder},
        },
        solana_net_utils::sockets::bind_to_localhost_unique,
        solana_runtime::bank::Bank,
        solana_signer::Signer,
        std::{net::UdpSocket, time::Duration},
    };

    fn get_keypair() -> Keypair {
        const KEYPAIR: &str = "Fcc2HUvRC7Dv4GgehTziAremzRvwDw5miYu8Ahuu1rsGjA\
            5eCn55pXiSkEPcuqviV41rJxrFpZDmHmQkZWfoYYS";
        bs58::decode(KEYPAIR)
            .into_vec()
            .as_deref()
            .map(Keypair::try_from)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn test_retransmit_external_receivers_are_deduped_and_bam_gated() {
        let bind_receiver = || {
            let socket = bind_to_localhost_unique().unwrap();
            socket
                .set_read_timeout(Some(Duration::from_millis(200)))
                .unwrap();
            socket
        };
        let recv_one = |socket: &UdpSocket| {
            let mut buf = [0u8; 2048];
            socket.recv_from(&mut buf).is_ok()
        };
        let assert_no_packet = |socket: &UdpSocket| {
            let mut buf = [0u8; 2048];
            assert!(socket.recv_from(&mut buf).is_err());
        };

        let turbine_receiver = bind_receiver();
        let duplicate_configured_receiver = bind_receiver();
        let configured_receiver = bind_receiver();
        let bam_receiver = bind_receiver();
        let shredstream_receiver = bind_receiver();
        let multicast_receiver = bind_receiver();
        let turbine_addr = turbine_receiver.local_addr().unwrap();
        let duplicate_configured_addr = duplicate_configured_receiver.local_addr().unwrap();
        let configured_addr = configured_receiver.local_addr().unwrap();
        let bam_addr = bam_receiver.local_addr().unwrap();
        let shredstream_addr = shredstream_receiver.local_addr().unwrap();
        let multicast_addr = multicast_receiver.local_addr().unwrap();
        let retransmit_socket = bind_to_localhost_unique().unwrap();
        let external_sender_socket = bind_to_localhost_unique().unwrap();

        let keypair = get_keypair();
        let entries = create_ticks(1, 1, Hash::new_unique());
        let shredder = Shredder::new(5, 4, 1, 0).unwrap();
        let (data_shreds, _) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair,
            &entries,
            true,
            Hash::new_from_array(rand::rng().random()),
            0,
            0,
            &mut ProcessShredsStats::default(),
        );
        let shred = data_shreds[0].payload().clone();
        let key = shred::layout::get_shred_id(shred.as_ref()).unwrap();

        let genesis_config = create_genesis_config(10_000).genesis_config;
        let root_bank = Bank::new_for_tests(&genesis_config);
        let cluster_info = ClusterInfo::new(
            Node::new_localhost_with_pubkey(&keypair.pubkey()).info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        );
        for addr in [turbine_addr, duplicate_configured_addr] {
            let mut peer = Node::new_localhost_with_pubkey(&Pubkey::new_unique()).info;
            peer.set_tvu(solana_gossip::contact_info::Protocol::UDP, addr)
                .unwrap();
            cluster_info.insert_info(peer);
        }
        let shred_receiver_addresses: ShredReceiverAddresses =
            [duplicate_configured_addr, configured_addr]
                .into_iter()
                .collect();
        let bam_shred_receiver_addresses: ShredReceiverAddresses =
            [configured_addr, bam_addr, turbine_addr]
                .into_iter()
                .collect();

        let retransmit_once =
            |include_bam_shred_receivers,
             root_distance,
             shredstream_receiver_address: Option<SocketAddr>,
             multicast_root_receiver_address: Option<SocketAddr>| {
                // A single staked node is always the turbine root. Stake this node
                // for root forwarding, or another node to exercise a leaf.
                let staked_node = if root_distance == 0 {
                    cluster_info.id()
                } else {
                    Pubkey::new_unique()
                };
                let cluster_nodes =
                    Arc::new(crate::cluster_nodes::new_cluster_nodes::<RetransmitStage>(
                        &cluster_info,
                        ClusterType::Development,
                        &HashMap::from([(staked_node, 1)]),
                    ));
                let cache = HashMap::from([(
                    key.slot(),
                    (
                        Pubkey::new_unique(),
                        cluster_nodes,
                        include_bam_shred_receivers,
                    ),
                )]);
                let mut rng = ChaChaRng::from_seed([0xa5; 32]);
                let shred_deduper = ShredDeduper::<2>::new(&mut rng, /*num_bits:*/ 640_007);
                retransmit_shred(
                    shred.clone(),
                    &root_bank,
                    &shred_deduper,
                    &cache,
                    &SocketAddrSpace::Unspecified,
                    &RetransmitSocket::Socket(&retransmit_socket),
                    &external_sender_socket,
                    &mut BatchStats::default(),
                    &shred_receiver_addresses,
                    &bam_shred_receiver_addresses,
                    &shredstream_receiver_address,
                    &multicast_root_receiver_address,
                )
                .unwrap()
            };

        let out = retransmit_once(false, 0, None, None);
        assert_eq!(out.num_nodes, 2);
        assert!(recv_one(&turbine_receiver));
        assert!(recv_one(&duplicate_configured_receiver));
        assert!(recv_one(&configured_receiver));
        assert_no_packet(&turbine_receiver);
        assert_no_packet(&duplicate_configured_receiver);
        assert_no_packet(&configured_receiver);
        assert_no_packet(&bam_receiver);

        let out = retransmit_once(true, 0, None, None);
        assert_eq!(out.num_nodes, 2);
        assert!(recv_one(&turbine_receiver));
        assert!(recv_one(&duplicate_configured_receiver));
        assert!(recv_one(&configured_receiver));
        assert!(recv_one(&bam_receiver));
        assert_no_packet(&turbine_receiver);
        assert_no_packet(&duplicate_configured_receiver);
        assert_no_packet(&configured_receiver);
        assert_no_packet(&bam_receiver);

        let out = retransmit_once(false, 0, Some(shredstream_addr), Some(multicast_addr));
        assert_eq!(out.num_nodes, 2);
        assert!(recv_one(&turbine_receiver));
        assert!(recv_one(&duplicate_configured_receiver));
        assert!(recv_one(&configured_receiver));
        assert!(recv_one(&shredstream_receiver));
        assert!(recv_one(&multicast_receiver));
        assert_no_packet(&turbine_receiver);
        assert_no_packet(&duplicate_configured_receiver);
        assert_no_packet(&configured_receiver);
        assert_no_packet(&bam_receiver);
        assert_no_packet(&shredstream_receiver);
        assert_no_packet(&multicast_receiver);

        let out = retransmit_once(false, 1, Some(shredstream_addr), Some(multicast_addr));
        assert_eq!(out.root_distance, 1);
        assert_eq!(out.num_nodes, 0);
        assert!(recv_one(&duplicate_configured_receiver));
        assert!(recv_one(&configured_receiver));
        assert_no_packet(&turbine_receiver);
        assert_no_packet(&duplicate_configured_receiver);
        assert_no_packet(&configured_receiver);
        assert_no_packet(&bam_receiver);
        assert_no_packet(&shredstream_receiver);
        assert_no_packet(&multicast_receiver);

        let out = retransmit_once(true, 0, Some(turbine_addr), Some(configured_addr));
        assert_eq!(out.num_nodes, 2);
        assert!(recv_one(&turbine_receiver));
        assert!(recv_one(&duplicate_configured_receiver));
        assert!(recv_one(&configured_receiver));
        assert!(recv_one(&bam_receiver));
        assert_no_packet(&turbine_receiver);
        assert_no_packet(&duplicate_configured_receiver);
        assert_no_packet(&configured_receiver);
        assert_no_packet(&bam_receiver);
    }

    #[test]
    fn test_shred_deduper() {
        let keypair = get_keypair();
        let entries = create_ticks(10, 1, Hash::new_unique());
        let make_shreds_for_slot = |slot, parent| {
            let shredder = Shredder::new(slot, parent, 1, 0).unwrap();
            let next_shred_index = 0;
            let next_code_index = 0;
            shredder.entries_to_merkle_shreds_for_tests(
                &keypair,
                &entries,
                true,
                // chained_merkle_root
                Hash::new_from_array(rand::rng().random()),
                next_shred_index,
                next_code_index,
                &mut ProcessShredsStats::default(),
            )
        };

        let mut rng = ChaChaRng::from_seed([0xa5; 32]);
        let shred_deduper = ShredDeduper::<2>::new(&mut rng, /*num_bits:*/ 640_007);

        // make a set of shreds for slot 5 with parent slot 4
        let (shreds_data_5_4, shreds_code_5_4) = make_shreds_for_slot(5, 4);
        // make a set of shreds for slot 5 with parent slot 3
        let (shreds_data_5_3, shreds_code_5_3) = make_shreds_for_slot(5, 3);
        // make a set of shreds for slot 5 with parent slot 2
        let (shreds_data_5_2, shreds_code_5_2) = make_shreds_for_slot(5, 2);
        // pick a shred for tests
        let shred = shreds_data_5_4.last().unwrap().clone();
        // unique shred should pass
        assert!(
            !shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT),
            "First time shred X => Not dup because it is the only shred"
        );
        // duplicate shred blocked
        assert!(
            shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT),
            "
            Second time shred X => Dup because common header is duplicate
            "
        );
        // Pick a shred with same index as `shred` but different parent offset
        let shred_dup = shreds_data_5_3.last().unwrap().clone();
        // first shred passed through
        assert!(
            !shred_deduper.dedup(shred_dup.id(), shred_dup.payload(), MAX_DUPLICATE_COUNT),
            "First time seeing shred X with different parent slot (3 instead of 4) => Not dup \
             because common header is unique & shred ID only seen once"
        );
        // then blocked
        assert!(
            shred_deduper.dedup(shred_dup.id(), shred_dup.payload(), MAX_DUPLICATE_COUNT),
            "Second time seeing shred X with parent slot 3 => Dup because common header is not \
             unique & shred ID seen twice"
        );

        let shred_dup2 = shreds_data_5_2.last().unwrap().clone();

        assert!(
            shred_deduper.dedup(shred_dup2.id(), shred_dup2.payload(), MAX_DUPLICATE_COUNT),
            "First time seeing shred X with parent slot 2 => Dup because common header is unique \
             but shred ID seen twice already"
        );

        /* Coding shreds */

        // Pick a coding shred at index 4 based off FEC set index 0
        let shred = shreds_code_5_4[4].clone();
        // Coding passes
        assert!(
            !shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT),
            "
           First time seeing coding shred Y => Not dup because common header & shred ID are unique"
        );
        // then blocked
        assert!(
            shred_deduper.dedup(shred.id(), shred.payload(), MAX_DUPLICATE_COUNT),
            "
            Second time seeing coding shred Y => Dup because common header is dup
            "
        );

        // Pick a coding shred at index 4 from an erasure batch with parent slot 3
        let shred_inv_code_1 = shreds_code_5_3[4].clone();
        assert_eq!(
            shred.index(),
            shred_inv_code_1.index(),
            "we want a shred with same index but different signature"
        );
        // 2nd unique coding passes
        assert!(
            !shred_deduper.dedup(
                shred_inv_code_1.id(),
                shred_inv_code_1.payload(),
                MAX_DUPLICATE_COUNT
            ),
            "First time seeing shred Y from a different erasure batch => Not dup because common \
             header is unique & shred ID only seen once"
        );
        // same again is blocked
        assert!(
            shred_deduper.dedup(
                shred_inv_code_1.id(),
                shred_inv_code_1.payload(),
                MAX_DUPLICATE_COUNT
            ),
            "
           Second time seeing shred Y from a different erasure batch => Dup because common header \
             is not unique & shred ID seen twice "
        );
        // Pick a coding shred at index 4 from an erasure batch with parent slot 2
        let shred_inv_code_2 = shreds_code_5_2[4].clone();
        assert_eq!(
            shred.index(),
            shred_inv_code_2.index(),
            "we want a shred with same index but different signature"
        );
        assert!(
            shred_deduper.dedup(
                shred_inv_code_2.id(),
                shred_inv_code_2.payload(),
                MAX_DUPLICATE_COUNT
            ),
            "
           First time seeing shred Y from a third erasure batch => Dup because common header is \
             unique but shred ID seen twice already"
        );
    }

    #[test]
    fn test_notify_subscribers_sends_first_shred_after_genesis() {
        let (votor_event_sender, votor_event_receiver) = crossbeam_channel::bounded(1);
        let migration_status = Arc::new(MigrationStatus::post_migration_status());
        let genesis_slot = migration_status.genesis_block().unwrap().slot;
        let notifiers = RetransmitNotifiers {
            rpc_subscriptions: None,
            slot_status_notifier: None,
            migration_status,
            votor_event_sender,
        };
        let mut pending_first_shred_event = None;

        let slot = genesis_slot.checked_add(1).unwrap();
        assert!(!slot.is_multiple_of(NUM_CONSECUTIVE_LEADER_SLOTS.get() as Slot));
        notify_subscribers(
            slot,
            /*timestamp:*/ 0,
            &notifiers,
            &mut pending_first_shred_event,
        );

        assert!(matches!(
            votor_event_receiver.try_recv(),
            Ok(VotorEvent::FirstShred(received_slot)) if received_slot == slot
        ));
        assert!(pending_first_shred_event.is_none());

        // Later unaligned slots do not need a FirstShred event.
        let slot = slot.checked_add(1).unwrap();
        assert!(!slot.is_multiple_of(NUM_CONSECUTIVE_LEADER_SLOTS.get() as Slot));
        notify_subscribers(
            slot,
            /*timestamp:*/ 0,
            &notifiers,
            &mut pending_first_shred_event,
        );
        assert!(matches!(
            votor_event_receiver.try_recv(),
            Err(TryRecvError::Empty)
        ));
    }
}
