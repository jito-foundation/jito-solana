//! The `cluster_info` module defines a data structure that is shared by all the nodes in the network over
//! a gossip control plane.  The goal is to share small bits of off-chain information and detect and
//! repair partitions.
//!
//! This CRDT only supports a very limited set of types.  A map of Pubkey -> Versioned Struct.
//! The last version is always picked during an update.
//!
//! The network is arranged in layers:
//!
//! * layer 0 - Leader.
//! * layer 1 - As many nodes as we can fit
//! * layer 2 - Everyone else, if layer 1 is `2^10`, layer 2 should be able to fit `2^20` number of nodes.
//!
//! Bank needs to provide an interface for us to query the stake weight

use {
    crate::{
        cluster_info_metrics::{
            submit_gossip_stats, Counter, GossipStats, ScopedTimer, TimedGuard,
        },
        contact_info::{self, ContactInfo, ContactInfoQuery, Error as ContactInfoError},
        crds::{Crds, Cursor, GossipRoute},
        crds_data::{self, CrdsData, EpochSlotsIndex, LowestSlot, SnapshotHashes, Vote},
        crds_gossip::CrdsGossip,
        crds_gossip_error::CrdsGossipError,
        crds_gossip_pull::{
            get_max_bloom_filter_bytes, CrdsFilter, CrdsTimeouts, ProcessPullStats, PullRequest,
            CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS,
        },
        crds_value::{CrdsValue, CrdsValueLabel},
        duplicate_shred::DuplicateShred,
        epoch_slots::EpochSlots,
        epoch_specs::EpochSpecs,
        gossip_error::GossipError,
        ping_pong::Pong,
        protocol::{
            split_gossip_messages, Ping, PingCache, Protocol, PruneData,
            DUPLICATE_SHRED_MAX_PAYLOAD_SIZE, MAX_INCREMENTAL_SNAPSHOT_HASHES,
            MAX_PRUNE_DATA_NODES, PULL_RESPONSE_MAX_PAYLOAD_SIZE,
            PULL_RESPONSE_MIN_SERIALIZED_SIZE, PUSH_MESSAGE_MAX_PAYLOAD_SIZE,
        },
        restart_crds_values::{
            RestartHeaviestFork, RestartLastVotedForkSlots, RestartLastVotedForkSlotsError,
        },
        weighted_shuffle::WeightedShuffle,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    itertools::Itertools,
    rand::{seq::SliceRandom, CryptoRng, Rng},
    rayon::{prelude::*, ThreadPool, ThreadPoolBuilder},
    solana_ledger::shred::Shred,
    solana_net_utils::{
        bind_common_in_range_with_config, bind_common_with_config, bind_in_range,
        bind_in_range_with_config, bind_more_with_config, bind_to_localhost, bind_to_unspecified,
        bind_two_in_range_with_offset_and_config, find_available_port_in_range,
        multi_bind_in_range_with_config, PortRange, SocketConfig, VALIDATOR_PORT_RANGE,
    },
    solana_perf::{
        data_budget::DataBudget,
        packet::{Packet, PacketBatch, PacketBatchRecycler, PACKET_DATA_SIZE},
    },
    solana_rayon_threadlimit::get_thread_count,
    solana_runtime::bank_forks::BankForks,
    solana_sanitize::Sanitize,
    solana_sdk::{
        clock::{Slot, DEFAULT_MS_PER_SLOT, DEFAULT_SLOTS_PER_EPOCH},
        hash::Hash,
        pubkey::Pubkey,
        quic::QUIC_PORT_OFFSET,
        signature::{Keypair, Signable, Signature, Signer},
        timing::timestamp,
        transaction::Transaction,
    },
    solana_streamer::{
        packet,
        quic::DEFAULT_QUIC_ENDPOINTS,
        socket::SocketAddrSpace,
        streamer::{PacketBatchReceiver, PacketBatchSender},
    },
    solana_vote::vote_parser,
    solana_vote_program::vote_state::MAX_LOCKOUT_HISTORY,
    std::{
        collections::{HashMap, HashSet, VecDeque},
        fmt::Debug,
        fs::{self, File},
        io::{BufReader, BufWriter, Write},
        iter::repeat,
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, UdpSocket},
        num::NonZeroUsize,
        ops::{Deref, Div},
        path::{Path, PathBuf},
        result::Result,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock, RwLockReadGuard,
        },
        thread::{sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
};

const DEFAULT_EPOCH_DURATION: Duration =
    Duration::from_millis(DEFAULT_SLOTS_PER_EPOCH * DEFAULT_MS_PER_SLOT);
/// milliseconds we sleep for between gossip requests
pub const GOSSIP_SLEEP_MILLIS: u64 = 100;
/// A hard limit on incoming gossip messages
/// Chosen to be able to handle 1Gbps of pure gossip traffic
/// 128MB/PACKET_DATA_SIZE
const MAX_GOSSIP_TRAFFIC: usize = 128_000_000 / PACKET_DATA_SIZE;
const GOSSIP_PING_CACHE_CAPACITY: usize = 126976;
const GOSSIP_PING_CACHE_TTL: Duration = Duration::from_secs(1280);
const GOSSIP_PING_CACHE_RATE_LIMIT_DELAY: Duration = Duration::from_secs(1280 / 64);
pub const DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS: u64 = 10_000;
pub const DEFAULT_CONTACT_SAVE_INTERVAL_MILLIS: u64 = 60_000;
// Limit number of unique pubkeys in the crds table.
pub(crate) const CRDS_UNIQUE_PUBKEY_CAPACITY: usize = 8192;
/// Minimum stake that a node should have so that its CRDS values are
/// propagated through gossip (few types are exempted).
const MIN_STAKE_FOR_GOSSIP: u64 = solana_sdk::native_token::LAMPORTS_PER_SOL;
/// Minimum number of staked nodes for enforcing stakes in gossip.
const MIN_NUM_STAKED_NODES: usize = 500;

// Must have at least one socket to monitor the TVU port
// The unsafes are safe because we're using fixed, known non-zero values
pub const MINIMUM_NUM_TVU_SOCKETS: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1) };
pub const DEFAULT_NUM_TVU_SOCKETS: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(8) };

#[derive(Debug, PartialEq, Eq, Error)]
pub enum ClusterInfoError {
    #[error("NoPeers")]
    NoPeers,
    #[error("NoLeader")]
    NoLeader,
    #[error("BadContactInfo")]
    BadContactInfo,
    #[error("BadGossipAddress")]
    BadGossipAddress,
    #[error("TooManyIncrementalSnapshotHashes")]
    TooManyIncrementalSnapshotHashes,
}

pub struct ClusterInfo {
    /// The network
    pub gossip: CrdsGossip,
    /// set the keypair that will be used to sign crds values generated. It is unset only in tests.
    keypair: RwLock<Arc<Keypair>>,
    /// Network entrypoints
    entrypoints: RwLock<Vec<ContactInfo>>,
    outbound_budget: DataBudget,
    my_contact_info: RwLock<ContactInfo>,
    ping_cache: Mutex<PingCache>,
    stats: GossipStats,
    local_message_pending_push_queue: Mutex<Vec<CrdsValue>>,
    contact_debug_interval: u64, // milliseconds, 0 = disabled
    contact_save_interval: u64,  // milliseconds, 0 = disabled
    contact_info_path: PathBuf,
    socket_addr_space: SocketAddrSpace,
}

// Returns false if the CRDS value should be discarded.
#[inline]
#[must_use]
fn should_retain_crds_value(
    value: &CrdsValue,
    stakes: &HashMap<Pubkey, u64>,
    drop_unstaked_node_instance: bool,
) -> bool {
    match value.data() {
        CrdsData::ContactInfo(_) => true,
        CrdsData::LegacyContactInfo(_) => true,
        // May Impact new validators starting up without any stake yet.
        CrdsData::Vote(_, _) => true,
        // Unstaked nodes can still help repair.
        CrdsData::EpochSlots(_, _) => true,
        // Unstaked nodes can still serve snapshots.
        CrdsData::LegacySnapshotHashes(_) | CrdsData::SnapshotHashes(_) => true,
        // Otherwise unstaked voting nodes will show up with no version in
        // the various dashboards.
        CrdsData::Version(_) => true,
        CrdsData::AccountsHashes(_) => true,
        CrdsData::NodeInstance(_) if !drop_unstaked_node_instance => true,
        CrdsData::LowestSlot(_, _)
        | CrdsData::LegacyVersion(_)
        | CrdsData::DuplicateShred(_, _)
        | CrdsData::RestartHeaviestFork(_)
        | CrdsData::RestartLastVotedForkSlots(_)
        | CrdsData::NodeInstance(_) => {
            stakes.len() < MIN_NUM_STAKED_NODES || {
                let stake = stakes.get(&value.pubkey()).copied();
                stake.unwrap_or_default() >= MIN_STAKE_FOR_GOSSIP
            }
        }
    }
}

impl ClusterInfo {
    pub fn new(
        contact_info: ContactInfo,
        keypair: Arc<Keypair>,
        socket_addr_space: SocketAddrSpace,
    ) -> Self {
        assert_eq!(contact_info.pubkey(), &keypair.pubkey());
        let me = Self {
            gossip: CrdsGossip::default(),
            keypair: RwLock::new(keypair),
            entrypoints: RwLock::default(),
            outbound_budget: DataBudget::default(),
            my_contact_info: RwLock::new(contact_info),
            ping_cache: Mutex::new(PingCache::new(
                &mut rand::thread_rng(),
                Instant::now(),
                GOSSIP_PING_CACHE_TTL,
                GOSSIP_PING_CACHE_RATE_LIMIT_DELAY,
                GOSSIP_PING_CACHE_CAPACITY,
            )),
            stats: GossipStats::default(),
            local_message_pending_push_queue: Mutex::default(),
            contact_debug_interval: DEFAULT_CONTACT_DEBUG_INTERVAL_MILLIS,
            contact_info_path: PathBuf::default(),
            contact_save_interval: 0, // disabled
            socket_addr_space,
        };
        me.refresh_my_gossip_contact_info();
        me
    }

    pub fn set_contact_debug_interval(&mut self, new: u64) {
        self.contact_debug_interval = new;
    }

    pub fn socket_addr_space(&self) -> &SocketAddrSpace {
        &self.socket_addr_space
    }

    fn refresh_push_active_set(
        &self,
        recycler: &PacketBatchRecycler,
        stakes: &HashMap<Pubkey, u64>,
        gossip_validators: Option<&HashSet<Pubkey>>,
        sender: &PacketBatchSender,
    ) {
        let shred_version = self.my_contact_info.read().unwrap().shred_version();
        let self_keypair: Arc<Keypair> = self.keypair().clone();
        let mut pings = Vec::new();
        self.gossip.refresh_push_active_set(
            &self_keypair,
            shred_version,
            stakes,
            gossip_validators,
            &self.ping_cache,
            &mut pings,
            &self.socket_addr_space,
        );
        self.stats
            .new_pull_requests_pings_count
            .add_relaxed(pings.len() as u64);
        let pings: Vec<_> = pings
            .into_iter()
            .map(|(addr, ping)| (addr, Protocol::PingMessage(ping)))
            .collect();
        if !pings.is_empty() {
            self.stats
                .packets_sent_gossip_requests_count
                .add_relaxed(pings.len() as u64);
            let packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
                recycler,
                "refresh_push_active_set",
                &pings,
            );
            let _ = sender.send(packet_batch);
        }
    }

    // TODO kill insert_info, only used by tests
    pub fn insert_info(&self, node: ContactInfo) {
        let entry = CrdsValue::new(CrdsData::ContactInfo(node), &self.keypair());
        if let Err(err) = {
            let mut gossip_crds = self.gossip.crds.write().unwrap();
            gossip_crds.insert(entry, timestamp(), GossipRoute::LocalMessage)
        } {
            error!("ClusterInfo.insert_info: {err:?}");
        }
    }

    pub fn set_entrypoint(&self, entrypoint: ContactInfo) {
        self.set_entrypoints(vec![entrypoint]);
    }

    pub fn set_entrypoints(&self, entrypoints: Vec<ContactInfo>) {
        *self.entrypoints.write().unwrap() = entrypoints;
    }

    pub fn set_my_contact_info(&self, my_contact_info: ContactInfo) {
        *self.my_contact_info.write().unwrap() = my_contact_info;
    }

    pub fn save_contact_info(&self) {
        let _st = ScopedTimer::from(&self.stats.save_contact_info_time);
        let nodes = {
            let entrypoint_gossip_addrs = self
                .entrypoints
                .read()
                .unwrap()
                .iter()
                .filter_map(ContactInfo::gossip)
                .collect::<HashSet<_>>();
            let self_pubkey = self.id();
            let gossip_crds = self.gossip.crds.read().unwrap();
            gossip_crds
                .get_nodes()
                .filter_map(|v| {
                    // Don't save:
                    // 1. Our ContactInfo. No point
                    // 2. Entrypoint ContactInfo. This will avoid adopting the incorrect shred
                    //    version on restart if the entrypoint shred version changes.  Also
                    //    there's not much point in saving entrypoint ContactInfo since by
                    //    definition that information is already available
                    let contact_info = v.value.contact_info().unwrap();
                    if contact_info.pubkey() != &self_pubkey
                        && contact_info
                            .gossip()
                            .map(|addr| !entrypoint_gossip_addrs.contains(&addr))
                            .unwrap_or_default()
                    {
                        return Some(v.value.clone());
                    }
                    None
                })
                .collect::<Vec<_>>()
        };

        if nodes.is_empty() {
            return;
        }

        let filename = self.contact_info_path.join("contact-info.bin");
        let tmp_filename = &filename.with_extension("tmp");

        match File::create(tmp_filename) {
            Ok(file) => {
                let mut writer = BufWriter::new(file);
                if let Err(err) = bincode::serialize_into(&mut writer, &nodes) {
                    warn!(
                        "Failed to serialize contact info info {}: {}",
                        tmp_filename.display(),
                        err
                    );
                    return;
                }
                if let Err(err) = writer.flush() {
                    warn!("Failed to save contact info: {err}");
                }
            }
            Err(err) => {
                warn!("Failed to create {}: {}", tmp_filename.display(), err);
                return;
            }
        }

        match fs::rename(tmp_filename, &filename) {
            Ok(()) => {
                info!(
                    "Saved contact info for {} nodes into {}",
                    nodes.len(),
                    filename.display()
                );
            }
            Err(err) => {
                warn!(
                    "Failed to rename {} to {}: {}",
                    tmp_filename.display(),
                    filename.display(),
                    err
                );
            }
        }
    }

    pub fn restore_contact_info(&mut self, contact_info_path: &Path, contact_save_interval: u64) {
        self.contact_info_path = contact_info_path.into();
        self.contact_save_interval = contact_save_interval;

        let filename = contact_info_path.join("contact-info.bin");
        if !filename.exists() {
            return;
        }

        let nodes: Vec<CrdsValue> = match File::open(&filename) {
            Ok(file) => {
                bincode::deserialize_from(&mut BufReader::new(file)).unwrap_or_else(|err| {
                    warn!("Failed to deserialize {}: {}", filename.display(), err);
                    vec![]
                })
            }
            Err(err) => {
                warn!("Failed to open {}: {}", filename.display(), err);
                vec![]
            }
        };

        info!(
            "Loaded contact info for {} nodes from {}",
            nodes.len(),
            filename.display()
        );
        let now = timestamp();
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        for node in nodes {
            if let Err(err) = gossip_crds.insert(node, now, GossipRoute::LocalMessage) {
                warn!("crds insert failed {:?}", err);
            }
        }
    }

    pub fn id(&self) -> Pubkey {
        self.keypair.read().unwrap().pubkey()
    }

    pub fn keypair(&self) -> RwLockReadGuard<Arc<Keypair>> {
        self.keypair.read().unwrap()
    }

    pub fn set_keypair(&self, new_keypair: Arc<Keypair>) {
        let id = new_keypair.pubkey();
        *self.keypair.write().unwrap() = new_keypair;
        self.my_contact_info.write().unwrap().hot_swap_pubkey(id);

        self.refresh_my_gossip_contact_info();
    }

    pub fn set_tpu(&self, tpu_addr: SocketAddr) -> Result<(), ContactInfoError> {
        self.my_contact_info.write().unwrap().set_tpu(tpu_addr)?;
        self.refresh_my_gossip_contact_info();
        Ok(())
    }

    pub fn set_tpu_forwards(&self, tpu_forwards_addr: SocketAddr) -> Result<(), ContactInfoError> {
        self.my_contact_info
            .write()
            .unwrap()
            .set_tpu_forwards(tpu_forwards_addr)?;
        self.refresh_my_gossip_contact_info();
        Ok(())
    }

    pub fn lookup_contact_info<R>(
        &self,
        id: &Pubkey,
        query: impl ContactInfoQuery<R>,
    ) -> Option<R> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds.get(*id).map(query)
    }

    pub fn lookup_contact_info_by_gossip_addr(
        &self,
        gossip_addr: &SocketAddr,
    ) -> Option<ContactInfo> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        let mut nodes = gossip_crds.get_nodes_contact_info();
        nodes
            .find(|node| node.gossip() == Some(*gossip_addr))
            .cloned()
    }

    pub fn my_contact_info(&self) -> ContactInfo {
        self.my_contact_info.read().unwrap().clone()
    }

    pub fn my_shred_version(&self) -> u16 {
        self.my_contact_info.read().unwrap().shred_version()
    }

    fn lookup_epoch_slots(&self, ix: EpochSlotsIndex) -> EpochSlots {
        let self_pubkey = self.id();
        let label = CrdsValueLabel::EpochSlots(ix, self_pubkey);
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get::<&CrdsValue>(&label)
            .and_then(|v| v.epoch_slots())
            .cloned()
            .unwrap_or_else(|| EpochSlots::new(self_pubkey, timestamp()))
    }

    fn addr_to_string(&self, default_ip: &Option<IpAddr>, addr: &Option<SocketAddr>) -> String {
        addr.filter(|addr| self.socket_addr_space.check(addr))
            .map(|addr| {
                if &Some(addr.ip()) == default_ip {
                    addr.port().to_string()
                } else {
                    addr.to_string()
                }
            })
            .unwrap_or_else(|| String::from("none"))
    }

    pub fn rpc_info_trace(&self) -> String {
        let now = timestamp();
        let my_pubkey = self.id();
        let my_shred_version = self.my_shred_version();
        let nodes: Vec<_> = self
            .all_peers()
            .into_iter()
            .filter_map(|(node, last_updated)| {
                let node_rpc = node
                    .rpc()
                    .filter(|addr| self.socket_addr_space.check(addr))?;
                let node_version = self.get_node_version(node.pubkey());
                if my_shred_version != 0
                    && (node.shred_version() != 0 && node.shred_version() != my_shred_version)
                {
                    return None;
                }
                let rpc_addr = node_rpc.ip();
                Some(format!(
                    "{:15} {:2}| {:5} | {:44} |{:^9}| {:5}| {:5}| {}\n",
                    rpc_addr.to_string(),
                    if node.pubkey() == &my_pubkey {
                        "me"
                    } else {
                        ""
                    },
                    now.saturating_sub(last_updated),
                    node.pubkey().to_string(),
                    if let Some(node_version) = node_version {
                        node_version.to_string()
                    } else {
                        "-".to_string()
                    },
                    self.addr_to_string(&Some(rpc_addr), &node.rpc()),
                    self.addr_to_string(&Some(rpc_addr), &node.rpc_pubsub()),
                    node.shred_version(),
                ))
            })
            .collect();

        format!(
            "RPC Address       |Age(ms)| Node identifier                              \
             | Version | RPC  |PubSub|ShredVer\n\
             ------------------+-------+----------------------------------------------\
             +---------+------+------+--------\n\
             {}\
             RPC Enabled Nodes: {}",
            nodes.join(""),
            nodes.len(),
        )
    }

    pub fn contact_info_trace(&self) -> String {
        let now = timestamp();
        let mut shred_spy_nodes = 0usize;
        let mut total_spy_nodes = 0usize;
        let mut different_shred_nodes = 0usize;
        let my_pubkey = self.id();
        let my_shred_version = self.my_shred_version();
        let nodes: Vec<_> = self
            .all_peers()
            .into_iter()
            .filter_map(|(node, last_updated)| {
                let is_spy_node = Self::is_spy_node(&node, &self.socket_addr_space);
                if is_spy_node {
                    total_spy_nodes = total_spy_nodes.saturating_add(1);
                }

                let node_version = self.get_node_version(node.pubkey());
                if my_shred_version != 0 && (node.shred_version() != 0 && node.shred_version() != my_shred_version) {
                    different_shred_nodes = different_shred_nodes.saturating_add(1);
                    None
                } else {
                    if is_spy_node {
                        shred_spy_nodes = shred_spy_nodes.saturating_add(1);
                    }
                    let ip_addr = node.gossip().as_ref().map(SocketAddr::ip);
                    Some(format!(
                        "{:15} {:2}| {:5} | {:44} |{:^9}| {:5}|  {:5}| {:5}| {:5}| {:5}| {:5}| {:5}| {}\n",
                        node.gossip()
                            .filter(|addr| self.socket_addr_space.check(addr))
                            .as_ref()
                            .map(SocketAddr::ip)
                            .as_ref()
                            .map(IpAddr::to_string)
                            .unwrap_or_else(|| String::from("none")),
                        if node.pubkey() == &my_pubkey { "me" } else { "" },
                        now.saturating_sub(last_updated),
                        node.pubkey().to_string(),
                        if let Some(node_version) = node_version {
                            node_version.to_string()
                        } else {
                            "-".to_string()
                        },
                        self.addr_to_string(&ip_addr, &node.gossip()),
                        self.addr_to_string(&ip_addr, &node.tpu_vote(contact_info::Protocol::UDP)),
                        self.addr_to_string(&ip_addr, &node.tpu(contact_info::Protocol::UDP)),
                        self.addr_to_string(&ip_addr, &node.tpu_forwards(contact_info::Protocol::UDP)),
                        self.addr_to_string(&ip_addr, &node.tvu(contact_info::Protocol::UDP)),
                        self.addr_to_string(&ip_addr, &node.tvu(contact_info::Protocol::QUIC)),
                        self.addr_to_string(&ip_addr, &node.serve_repair(contact_info::Protocol::UDP)),
                        node.shred_version(),
                    ))
                }
            })
            .collect();

        format!(
            "IP Address        |Age(ms)| Node identifier                              \
             | Version |Gossip|TPUvote| TPU  |TPUfwd| TVU  |TVU Q |ServeR|ShredVer\n\
             ------------------+-------+----------------------------------------------\
             +---------+------+-------+------+------+------+------+------+--------\n\
             {}\
             Nodes: {}{}{}",
            nodes.join(""),
            nodes.len().saturating_sub(shred_spy_nodes),
            if total_spy_nodes > 0 {
                format!("\nSpies: {total_spy_nodes}")
            } else {
                "".to_string()
            },
            if different_shred_nodes > 0 {
                format!("\nNodes with different shred version: {different_shred_nodes}")
            } else {
                "".to_string()
            }
        )
    }

    // TODO: This has a race condition if called from more than one thread.
    pub fn push_lowest_slot(&self, min: Slot) {
        let self_pubkey = self.id();
        let last = {
            let gossip_crds = self.gossip.crds.read().unwrap();
            gossip_crds
                .get::<&LowestSlot>(self_pubkey)
                .map(|x| x.lowest)
                .unwrap_or_default()
        };
        if min > last {
            let now = timestamp();
            let entry = CrdsValue::new(
                CrdsData::LowestSlot(0, LowestSlot::new(self_pubkey, min, now)),
                &self.keypair(),
            );
            self.push_message(entry);
        }
    }

    // TODO: If two threads call into this function then epoch_slot_index has a
    // race condition and the threads will overwrite each other in crds table.
    pub fn push_epoch_slots(&self, mut update: &[Slot]) {
        let self_pubkey = self.id();
        let current_slots: Vec<_> = {
            let gossip_crds =
                self.time_gossip_read_lock("lookup_epoch_slots", &self.stats.epoch_slots_lookup);
            (0..crds_data::MAX_EPOCH_SLOTS)
                .filter_map(|ix| {
                    let label = CrdsValueLabel::EpochSlots(ix, self_pubkey);
                    let crds_value = gossip_crds.get::<&CrdsValue>(&label)?;
                    let epoch_slots = crds_value.epoch_slots()?;
                    let first_slot = epoch_slots.first_slot()?;
                    Some((epoch_slots.wallclock, first_slot, ix))
                })
                .collect()
        };
        let min_slot: Slot = current_slots
            .iter()
            .map(|(_wallclock, slot, _index)| *slot)
            .min()
            .unwrap_or_default();
        let max_slot: Slot = update.iter().max().cloned().unwrap_or(0);
        let total_slots = max_slot as isize - min_slot as isize;
        // WARN if CRDS is not storing at least a full epoch worth of slots
        if DEFAULT_SLOTS_PER_EPOCH as isize > total_slots
            && crds_data::MAX_EPOCH_SLOTS as usize <= current_slots.len()
        {
            self.stats.epoch_slots_filled.add_relaxed(1);
            warn!(
                "EPOCH_SLOTS are filling up FAST {}/{}",
                total_slots,
                current_slots.len()
            );
        }
        let mut reset = false;
        let mut epoch_slot_index = match current_slots.iter().max() {
            Some((_wallclock, _slot, index)) => *index,
            None => 0,
        };
        let mut entries = Vec::default();
        let keypair = self.keypair();
        while !update.is_empty() {
            let ix = epoch_slot_index % crds_data::MAX_EPOCH_SLOTS;
            let now = timestamp();
            let mut slots = if !reset {
                self.lookup_epoch_slots(ix)
            } else {
                EpochSlots::new(self_pubkey, now)
            };
            let n = slots.fill(update, now);
            update = &update[n..];
            if n > 0 {
                let epoch_slots = CrdsData::EpochSlots(ix, slots);
                let entry = CrdsValue::new(epoch_slots, &keypair);
                entries.push(entry);
            }
            epoch_slot_index += 1;
            reset = true;
        }
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        let now = timestamp();
        for entry in entries {
            if let Err(err) = gossip_crds.insert(entry, now, GossipRoute::LocalMessage) {
                error!("push_epoch_slots failed: {:?}", err);
            }
        }
    }

    pub fn push_restart_last_voted_fork_slots(
        &self,
        fork: &[Slot],
        last_vote_bankhash: Hash,
    ) -> Result<(), RestartLastVotedForkSlotsError> {
        let now = timestamp();
        let last_voted_fork_slots = RestartLastVotedForkSlots::new(
            self.id(),
            now,
            fork,
            last_vote_bankhash,
            self.my_shred_version(),
        )?;
        self.push_message(CrdsValue::new(
            CrdsData::RestartLastVotedForkSlots(last_voted_fork_slots),
            &self.keypair(),
        ));
        Ok(())
    }

    pub fn push_restart_heaviest_fork(
        &self,
        last_slot: Slot,
        last_slot_hash: Hash,
        observed_stake: u64,
    ) {
        let restart_heaviest_fork = RestartHeaviestFork {
            from: self.id(),
            wallclock: timestamp(),
            last_slot,
            last_slot_hash,
            observed_stake,
            shred_version: self.my_shred_version(),
        };
        self.push_message(CrdsValue::new(
            CrdsData::RestartHeaviestFork(restart_heaviest_fork),
            &self.keypair(),
        ));
    }

    fn time_gossip_read_lock<'a>(
        &'a self,
        label: &'static str,
        counter: &'a Counter,
    ) -> TimedGuard<'a, RwLockReadGuard<'a, Crds>> {
        TimedGuard::new(self.gossip.crds.read().unwrap(), label, counter)
    }

    fn push_message(&self, message: CrdsValue) {
        self.local_message_pending_push_queue
            .lock()
            .unwrap()
            .push(message);
    }

    pub fn push_snapshot_hashes(
        &self,
        full: (Slot, Hash),
        incremental: Vec<(Slot, Hash)>,
    ) -> Result<(), ClusterInfoError> {
        if incremental.len() > MAX_INCREMENTAL_SNAPSHOT_HASHES {
            return Err(ClusterInfoError::TooManyIncrementalSnapshotHashes);
        }

        let message = CrdsData::SnapshotHashes(SnapshotHashes {
            from: self.id(),
            full,
            incremental,
            wallclock: timestamp(),
        });
        self.push_message(CrdsValue::new(message, &self.keypair()));

        Ok(())
    }

    pub fn push_vote_at_index(&self, vote: Transaction, vote_index: u8) {
        assert!((vote_index as usize) < MAX_LOCKOUT_HISTORY);
        let self_pubkey = self.id();
        let now = timestamp();
        let vote = Vote::new(self_pubkey, vote, now).unwrap();
        let vote = CrdsData::Vote(vote_index, vote);
        let vote = CrdsValue::new(vote, &self.keypair());
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        if let Err(err) = gossip_crds.insert(vote, now, GossipRoute::LocalMessage) {
            error!("push_vote failed: {:?}", err);
        }
    }

    /// If there are less than `MAX_LOCKOUT_HISTORY` votes present, returns the next index
    /// without a vote. If there are `MAX_LOCKOUT_HISTORY` votes:
    /// - Finds the oldest wallclock vote and returns its index
    /// - Otherwise returns the total amount of observed votes
    ///
    /// If there exists a newer vote in gossip than `new_vote_slot` return `None` as this indicates
    /// that we might be submitting slashable votes after an improper restart
    fn find_vote_index_to_evict(&self, new_vote_slot: Slot) -> Option<u8> {
        let self_pubkey = self.id();
        let mut num_crds_votes = 0;
        let mut exists_newer_vote = false;
        let vote_index = {
            let gossip_crds =
                self.time_gossip_read_lock("gossip_read_push_vote", &self.stats.push_vote_read);
            (0..MAX_LOCKOUT_HISTORY as u8)
                .filter_map(|ix| {
                    let vote = CrdsValueLabel::Vote(ix, self_pubkey);
                    let vote: &CrdsData = gossip_crds.get(&vote)?;
                    num_crds_votes += 1;
                    match &vote {
                        CrdsData::Vote(_, vote) if vote.slot() < Some(new_vote_slot) => {
                            Some((vote.wallclock, ix))
                        }
                        CrdsData::Vote(_, _) => {
                            exists_newer_vote = true;
                            None
                        }
                        _ => panic!("this should not happen!"),
                    }
                })
                .min() // Boot the oldest evicted vote by wallclock.
                .map(|(_ /*wallclock*/, ix)| ix)
        };
        if exists_newer_vote {
            return None;
        }
        if num_crds_votes < MAX_LOCKOUT_HISTORY as u8 {
            // Do not evict if there is space in crds
            Some(num_crds_votes)
        } else {
            vote_index
        }
    }

    pub fn push_vote(&self, tower: &[Slot], vote: Transaction) {
        debug_assert!(tower.iter().tuple_windows().all(|(a, b)| a < b));
        // Find the oldest crds vote by wallclock that has a lower slot than `tower`
        // and recycle its vote-index. If the crds buffer is not full we instead add a new vote-index.
        let Some(vote_index) =
            self.find_vote_index_to_evict(tower.last().copied().expect("Cannot push empty vote"))
        else {
            // In this case we have restarted with a mangled/missing tower and are attempting
            // to push an old vote. This could be a slashable offense so better to panic here.
            let (_, vote, hash, _) = vote_parser::parse_vote_transaction(&vote).unwrap();
            panic!(
                "Submitting old vote, switch: {}, vote slots: {:?}, tower: {:?}",
                hash.is_some(),
                vote.slots(),
                tower
            );
        };
        debug_assert!(vote_index < MAX_LOCKOUT_HISTORY as u8);
        self.push_vote_at_index(vote, vote_index);
    }

    pub fn refresh_vote(&self, refresh_vote: Transaction, refresh_vote_slot: Slot) {
        let vote_index = {
            let self_pubkey = self.id();
            let gossip_crds =
                self.time_gossip_read_lock("gossip_read_push_vote", &self.stats.push_vote_read);
            (0..MAX_LOCKOUT_HISTORY as u8).find(|ix| {
                let vote = CrdsValueLabel::Vote(*ix, self_pubkey);
                let Some(vote) = gossip_crds.get::<&CrdsData>(&vote) else {
                    return false;
                };
                let CrdsData::Vote(_, prev_vote) = &vote else {
                    panic!("this should not happen!");
                };
                match prev_vote.slot() {
                    Some(prev_vote_slot) => prev_vote_slot == refresh_vote_slot,
                    None => {
                        error!("crds vote with no slots!");
                        false
                    }
                }
            })
        };

        // We don't write to an arbitrary index, because it may replace one of this validator's
        // existing votes on the network.
        if let Some(vote_index) = vote_index {
            self.push_vote_at_index(refresh_vote, vote_index);
        } else {
            // If you don't see a vote with the same slot yet, this means you probably
            // restarted, and need to repush and evict the oldest vote
            let Some(vote_index) = self.find_vote_index_to_evict(refresh_vote_slot) else {
                warn!(
                    "trying to refresh slot {} but all votes in gossip table are for newer slots",
                    refresh_vote_slot,
                );
                return;
            };
            debug_assert!(vote_index < MAX_LOCKOUT_HISTORY as u8);
            self.push_vote_at_index(refresh_vote, vote_index);
        }
    }

    /// Returns votes inserted since the given cursor.
    pub fn get_votes(&self, cursor: &mut Cursor) -> Vec<Transaction> {
        let txs: Vec<Transaction> = self
            .time_gossip_read_lock("get_votes", &self.stats.get_votes)
            .get_votes(cursor)
            .map(|vote| {
                let CrdsData::Vote(_, vote) = vote.value.data() else {
                    panic!("this should not happen!");
                };
                vote.transaction().clone()
            })
            .collect();
        self.stats.get_votes_count.add_relaxed(txs.len() as u64);
        txs
    }

    /// Returns votes and the associated labels inserted since the given cursor.
    pub fn get_votes_with_labels(
        &self,
        cursor: &mut Cursor,
    ) -> (Vec<CrdsValueLabel>, Vec<Transaction>) {
        let (labels, txs): (_, Vec<_>) = self
            .time_gossip_read_lock("get_votes", &self.stats.get_votes)
            .get_votes(cursor)
            .map(|vote| {
                let label = vote.value.label();
                let CrdsData::Vote(_, vote) = vote.value.data() else {
                    panic!("this should not happen!");
                };
                (label, vote.transaction().clone())
            })
            .unzip();
        self.stats.get_votes_count.add_relaxed(txs.len() as u64);
        (labels, txs)
    }

    pub fn push_duplicate_shred(
        &self,
        shred: &Shred,
        other_payload: &[u8],
    ) -> Result<(), GossipError> {
        self.gossip.push_duplicate_shred(
            &self.keypair(),
            shred,
            other_payload,
            None::<fn(Slot) -> Option<Pubkey>>, // Leader schedule
            DUPLICATE_SHRED_MAX_PAYLOAD_SIZE,
            self.my_shred_version(),
        )?;
        Ok(())
    }

    pub fn get_snapshot_hashes_for_node(&self, pubkey: &Pubkey) -> Option<SnapshotHashes> {
        self.gossip
            .crds
            .read()
            .unwrap()
            .get::<&SnapshotHashes>(*pubkey)
            .cloned()
    }

    /// Returns epoch-slots inserted since the given cursor.
    /// Excludes entries from nodes with unknown or different shred version.
    pub fn get_epoch_slots(&self, cursor: &mut Cursor) -> Vec<EpochSlots> {
        let self_shred_version = Some(self.my_shred_version());
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_epoch_slots(cursor)
            .filter(|entry| {
                let origin = entry.value.pubkey();
                gossip_crds.get_shred_version(&origin) == self_shred_version
            })
            .map(|entry| match entry.value.data() {
                CrdsData::EpochSlots(_, slots) => slots.clone(),
                _ => panic!("this should not happen!"),
            })
            .collect()
    }

    pub fn get_restart_last_voted_fork_slots(
        &self,
        cursor: &mut Cursor,
    ) -> Vec<RestartLastVotedForkSlots> {
        let self_shred_version = self.my_shred_version();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_entries(cursor)
            .filter_map(|entry| {
                let CrdsData::RestartLastVotedForkSlots(slots) = entry.value.data() else {
                    return None;
                };
                (slots.shred_version == self_shred_version).then_some(slots)
            })
            .cloned()
            .collect()
    }

    pub fn get_restart_heaviest_fork(&self, cursor: &mut Cursor) -> Vec<RestartHeaviestFork> {
        let self_shred_version = self.my_shred_version();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_entries(cursor)
            .filter_map(|entry| {
                let CrdsData::RestartHeaviestFork(fork) = entry.value.data() else {
                    return None;
                };
                (fork.shred_version == self_shred_version).then_some(fork)
            })
            .cloned()
            .collect()
    }

    /// Returns duplicate-shreds inserted since the given cursor.
    pub(crate) fn get_duplicate_shreds(&self, cursor: &mut Cursor) -> Vec<DuplicateShred> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_duplicate_shreds(cursor)
            .map(|entry| match entry.value.data() {
                CrdsData::DuplicateShred(_, dup) => dup.clone(),
                _ => panic!("this should not happen!"),
            })
            .collect()
    }

    pub fn get_node_version(&self, pubkey: &Pubkey) -> Option<solana_version::Version> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get::<&ContactInfo>(*pubkey)
            .map(ContactInfo::version)
            .cloned()
    }

    fn check_socket_addr_space(&self, addr: &Option<SocketAddr>) -> bool {
        addr.as_ref()
            .map(|addr| self.socket_addr_space.check(addr))
            .unwrap_or_default()
    }

    /// all validators that have a valid rpc port regardless of `shred_version`.
    pub fn all_rpc_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|node| {
                node.pubkey() != &self_pubkey && self.check_socket_addr_space(&node.rpc())
            })
            .cloned()
            .collect()
    }

    // All nodes in gossip (including spy nodes) and the last time we heard about them
    pub fn all_peers(&self) -> Vec<(ContactInfo, u64)> {
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes()
            .map(|x| (x.value.contact_info().unwrap().clone(), x.local_timestamp))
            .collect()
    }

    pub fn gossip_peers(&self) -> Vec<ContactInfo> {
        let me = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            // shred_version not considered for gossip peers (ie, spy nodes do not set shred_version)
            .filter(|node| node.pubkey() != &me && self.check_socket_addr_space(&node.gossip()))
            .cloned()
            .collect()
    }

    /// all validators that have a valid tvu port regardless of `shred_version`.
    pub fn all_tvu_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        self.time_gossip_read_lock("all_tvu_peers", &self.stats.all_tvu_peers)
            .get_nodes_contact_info()
            .filter(|node| {
                node.pubkey() != &self_pubkey
                    && self.check_socket_addr_space(&node.tvu(contact_info::Protocol::UDP))
            })
            .cloned()
            .collect()
    }

    /// all validators that have a valid tvu port and are on the same `shred_version`.
    pub fn tvu_peers<R>(&self, query: impl ContactInfoQuery<R>) -> Vec<R> {
        let self_pubkey = self.id();
        let self_shred_version = self.my_shred_version();
        self.time_gossip_read_lock("tvu_peers", &self.stats.tvu_peers)
            .get_nodes_contact_info()
            .filter(|node| {
                node.pubkey() != &self_pubkey
                    && node.shred_version() == self_shred_version
                    && self.check_socket_addr_space(&node.tvu(contact_info::Protocol::UDP))
            })
            .map(query)
            .collect()
    }

    /// all tvu peers with valid gossip addrs that likely have the slot being requested
    pub fn repair_peers(&self, slot: Slot) -> Vec<ContactInfo> {
        let _st = ScopedTimer::from(&self.stats.repair_peers);
        let self_pubkey = self.id();
        let self_shred_version = self.my_shred_version();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|node| {
                node.pubkey() != &self_pubkey
                    && node.shred_version() == self_shred_version
                    && self.check_socket_addr_space(&node.tvu(contact_info::Protocol::UDP))
                    && self.check_socket_addr_space(&node.serve_repair(contact_info::Protocol::UDP))
                    && match gossip_crds.get::<&LowestSlot>(*node.pubkey()) {
                        None => true, // fallback to legacy behavior
                        Some(lowest_slot) => lowest_slot.lowest <= slot,
                    }
            })
            .cloned()
            .collect()
    }

    fn is_spy_node(node: &ContactInfo, socket_addr_space: &SocketAddrSpace) -> bool {
        ![
            node.tpu(contact_info::Protocol::UDP),
            node.gossip(),
            node.tvu(contact_info::Protocol::UDP),
        ]
        .into_iter()
        .all(|addr| {
            addr.map(|addr| socket_addr_space.check(&addr))
                .unwrap_or_default()
        })
    }

    /// compute broadcast table
    pub fn tpu_peers(&self) -> Vec<ContactInfo> {
        let self_pubkey = self.id();
        let gossip_crds = self.gossip.crds.read().unwrap();
        gossip_crds
            .get_nodes_contact_info()
            .filter(|node| {
                node.pubkey() != &self_pubkey
                    && self.check_socket_addr_space(&node.tpu(contact_info::Protocol::UDP))
            })
            .cloned()
            .collect()
    }

    fn refresh_my_gossip_contact_info(&self) {
        let keypair: Arc<Keypair> = self.keypair().clone();
        let node = {
            let mut node = self.my_contact_info.write().unwrap();
            node.set_wallclock(timestamp());
            node.clone()
        };
        let node = CrdsValue::new(CrdsData::ContactInfo(node), &keypair);
        if let Err(err) = {
            let mut gossip_crds = self.gossip.crds.write().unwrap();
            gossip_crds.insert(node, timestamp(), GossipRoute::LocalMessage)
        } {
            error!("refresh_my_gossip_contact_info failed: {err:?}");
        }
    }

    // If the network entrypoint hasn't been discovered yet, add it to the crds table
    fn append_entrypoint_to_pulls(
        &self,
        thread_pool: &ThreadPool,
        max_bloom_filter_bytes: usize,
        pulls: &mut Vec<(ContactInfo, Vec<CrdsFilter>)>,
    ) {
        const THROTTLE_DELAY: u64 = CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS / 2;
        let entrypoint = {
            let mut entrypoints = self.entrypoints.write().unwrap();
            let Some(entrypoint) = entrypoints.choose_mut(&mut rand::thread_rng()) else {
                return;
            };
            if !pulls.is_empty() {
                let now = timestamp();
                if now <= entrypoint.wallclock().saturating_add(THROTTLE_DELAY) {
                    return;
                }
                entrypoint.set_wallclock(now);
                if let Some(entrypoint_gossip) = entrypoint.gossip() {
                    if self
                        .time_gossip_read_lock("entrypoint", &self.stats.entrypoint)
                        .get_nodes_contact_info()
                        .any(|node| node.gossip() == Some(entrypoint_gossip))
                    {
                        return; // Found the entrypoint, no need to pull from it
                    }
                }
            }
            entrypoint.clone()
        };
        let filters = if pulls.is_empty() {
            let _st = ScopedTimer::from(&self.stats.entrypoint2);
            self.gossip.pull.build_crds_filters(
                thread_pool,
                &self.gossip.crds,
                max_bloom_filter_bytes,
            )
        } else {
            pulls
                .iter()
                .flat_map(|(_, filters)| filters)
                .cloned()
                .collect()
        };
        self.stats.pull_from_entrypoint_count.add_relaxed(1);
        pulls.push((entrypoint, filters));
    }

    #[allow(clippy::type_complexity)]
    fn new_pull_requests(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
    ) -> (
        Vec<(SocketAddr, Ping)>,     // Ping packets.
        Vec<(SocketAddr, Protocol)>, // Pull requests
    ) {
        let now = timestamp();
        let self_info = CrdsValue::new(CrdsData::from(self.my_contact_info()), &self.keypair());
        let max_bloom_filter_bytes = get_max_bloom_filter_bytes(&self_info);
        let mut pings = Vec::new();
        let mut pulls = {
            let _st = ScopedTimer::from(&self.stats.new_pull_requests);
            self.gossip
                .new_pull_request(
                    thread_pool,
                    self.keypair().deref(),
                    self.my_shred_version(),
                    now,
                    gossip_validators,
                    stakes,
                    max_bloom_filter_bytes,
                    &self.ping_cache,
                    &mut pings,
                    &self.socket_addr_space,
                )
                .unwrap_or_default()
        };
        self.append_entrypoint_to_pulls(thread_pool, max_bloom_filter_bytes, &mut pulls);
        let num_requests = pulls
            .iter()
            .map(|(_, filters)| filters.len())
            .sum::<usize>() as u64;
        self.stats.new_pull_requests_count.add_relaxed(num_requests);
        let pulls = pulls
            .into_iter()
            .filter_map(|(peer, filters)| Some((peer.gossip()?, filters)))
            .flat_map(|(addr, filters)| repeat(addr).zip(filters))
            .map(|(gossip_addr, filter)| {
                let request = Protocol::PullRequest(filter, self_info.clone());
                (gossip_addr, request)
            });
        self.stats
            .new_pull_requests_pings_count
            .add_relaxed(pings.len() as u64);
        (pings, pulls.collect())
    }

    pub fn flush_push_queue(&self) {
        let entries: Vec<CrdsValue> =
            std::mem::take(&mut *self.local_message_pending_push_queue.lock().unwrap());
        if !entries.is_empty() {
            let mut gossip_crds = self.gossip.crds.write().unwrap();
            let now = timestamp();
            for entry in entries {
                let _ = gossip_crds.insert(entry, now, GossipRoute::LocalMessage);
            }
        }
    }
    fn new_push_requests(&self, stakes: &HashMap<Pubkey, u64>) -> Vec<(SocketAddr, Protocol)> {
        let self_id = self.id();
        let (entries, push_messages, num_pushes) = {
            let _st = ScopedTimer::from(&self.stats.new_push_requests);
            self.flush_push_queue();
            self.gossip
                .new_push_messages(&self_id, timestamp(), stakes, |value| {
                    should_retain_crds_value(
                        value, stakes, /*drop_unstaked_node_instance:*/ false,
                    )
                })
        };
        self.stats
            .push_fanout_num_entries
            .add_relaxed(entries.len() as u64);
        self.stats
            .push_fanout_num_nodes
            .add_relaxed(num_pushes as u64);
        let push_messages: Vec<_> = {
            let gossip_crds =
                self.time_gossip_read_lock("push_req_lookup", &self.stats.new_push_requests2);
            push_messages
                .into_iter()
                .filter_map(|(pubkey, messages)| {
                    let peer: &ContactInfo = gossip_crds.get(pubkey)?;
                    Some((peer.gossip()?, messages))
                })
                .collect()
        };
        let messages: Vec<_> = push_messages
            .into_iter()
            .flat_map(|(peer, msgs)| {
                let msgs = msgs.into_iter().map(|k| entries[k].clone());
                split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, msgs)
                    .map(move |payload| (peer, Protocol::PushMessage(self_id, payload)))
            })
            .collect();
        self.stats
            .new_push_requests_num
            .add_relaxed(messages.len() as u64);
        messages
    }

    // Generate new push and pull requests
    fn generate_new_gossip_requests(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        stakes: &HashMap<Pubkey, u64>,
        generate_pull_requests: bool,
    ) -> Vec<(SocketAddr, Protocol)> {
        self.trim_crds_table(CRDS_UNIQUE_PUBKEY_CAPACITY, stakes);
        // This will flush local pending push messages before generating
        // pull-request bloom filters, preventing pull responses to return the
        // same values back to the node itself. Note that packets will arrive
        // and are processed out of order.
        let mut out: Vec<_> = self.new_push_requests(stakes);
        self.stats
            .packets_sent_push_messages_count
            .add_relaxed(out.len() as u64);
        if generate_pull_requests {
            let (pings, pull_requests) =
                self.new_pull_requests(thread_pool, gossip_validators, stakes);
            self.stats
                .packets_sent_pull_requests_count
                .add_relaxed(pull_requests.len() as u64);
            let pings = pings
                .into_iter()
                .map(|(addr, ping)| (addr, Protocol::PingMessage(ping)));
            out.extend(pull_requests);
            out.extend(pings);
        }
        out
    }

    /// At random pick a node and try to get updated changes from them
    fn run_gossip(
        &self,
        thread_pool: &ThreadPool,
        gossip_validators: Option<&HashSet<Pubkey>>,
        recycler: &PacketBatchRecycler,
        stakes: &HashMap<Pubkey, u64>,
        sender: &PacketBatchSender,
        generate_pull_requests: bool,
    ) -> Result<(), GossipError> {
        let _st = ScopedTimer::from(&self.stats.gossip_transmit_loop_time);
        let reqs = self.generate_new_gossip_requests(
            thread_pool,
            gossip_validators,
            stakes,
            generate_pull_requests,
        );
        if !reqs.is_empty() {
            let packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
                recycler,
                "run_gossip",
                &reqs,
            );
            self.stats
                .packets_sent_gossip_requests_count
                .add_relaxed(packet_batch.len() as u64);
            sender.send(packet_batch)?;
        }
        self.stats
            .gossip_transmit_loop_iterations_since_last_report
            .add_relaxed(1);
        Ok(())
    }

    fn process_entrypoints(&self) -> bool {
        let mut entrypoints = self.entrypoints.write().unwrap();
        if entrypoints.is_empty() {
            // No entrypoint specified.  Nothing more to process
            return true;
        }
        for entrypoint in entrypoints.iter_mut() {
            if entrypoint.pubkey() == &Pubkey::default() {
                // If a pull from the entrypoint was successful it should exist in the CRDS table
                if let Some(entrypoint_from_gossip) = entrypoint
                    .gossip()
                    .and_then(|addr| self.lookup_contact_info_by_gossip_addr(&addr))
                {
                    // Update the entrypoint's id so future entrypoint pulls correctly reference it
                    *entrypoint = entrypoint_from_gossip;
                }
            }
        }
        // Adopt an entrypoint's `shred_version` if ours is unset
        if self.my_shred_version() == 0 {
            if let Some(entrypoint) = entrypoints
                .iter()
                .find(|entrypoint| entrypoint.shred_version() != 0)
            {
                info!(
                    "Setting shred version to {:?} from entrypoint {:?}",
                    entrypoint.shred_version(),
                    entrypoint.pubkey()
                );
                self.my_contact_info
                    .write()
                    .unwrap()
                    .set_shred_version(entrypoint.shred_version());
            }
        }
        self.my_shred_version() != 0
            && entrypoints
                .iter()
                .all(|entrypoint| entrypoint.pubkey() != &Pubkey::default())
    }

    fn handle_purge(
        &self,
        thread_pool: &ThreadPool,
        epoch_duration: Duration,
        stakes: &HashMap<Pubkey, u64>,
    ) {
        let self_pubkey = self.id();
        let timeouts = self
            .gossip
            .make_timeouts(self_pubkey, stakes, epoch_duration);
        let num_purged = {
            let _st = ScopedTimer::from(&self.stats.purge);
            self.gossip
                .purge(&self_pubkey, thread_pool, timestamp(), &timeouts)
        };
        self.stats.purge_count.add_relaxed(num_purged as u64);
    }

    // Trims the CRDS table by dropping all values associated with the pubkeys
    // with the lowest stake, so that the number of unique pubkeys are bounded.
    fn trim_crds_table(&self, cap: usize, stakes: &HashMap<Pubkey, u64>) {
        if !self.gossip.crds.read().unwrap().should_trim(cap) {
            return;
        }
        let keep: Vec<_> = self
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .map(ContactInfo::pubkey)
            .copied()
            .chain(std::iter::once(self.id()))
            .collect();
        self.stats.trim_crds_table.add_relaxed(1);
        let mut gossip_crds = self.gossip.crds.write().unwrap();
        match gossip_crds.trim(cap, &keep, stakes, timestamp()) {
            Err(err) => {
                self.stats.trim_crds_table_failed.add_relaxed(1);
                // TODO: Stakes are coming from the root-bank. Debug why/when
                // they are empty/zero.
                debug!("crds table trim failed: {:?}", err);
            }
            Ok(num_purged) => {
                self.stats
                    .trim_crds_table_purged_values_count
                    .add_relaxed(num_purged as u64);
            }
        }
    }

    /// randomly pick a node and ask them for updates asynchronously
    pub fn gossip(
        self: Arc<Self>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
        sender: PacketBatchSender,
        gossip_validators: Option<HashSet<Pubkey>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(std::cmp::min(get_thread_count(), 8))
            .thread_name(|i| format!("solRunGossip{i:02}"))
            .build()
            .unwrap();
        let mut epoch_specs = bank_forks.map(EpochSpecs::from);
        Builder::new()
            .name("solGossip".to_string())
            .spawn(move || {
                let mut last_push = 0;
                let mut last_contact_info_trace = timestamp();
                let mut last_contact_info_save = timestamp();
                let mut entrypoints_processed = false;
                let recycler = PacketBatchRecycler::default();
                let mut generate_pull_requests = true;
                while !exit.load(Ordering::Relaxed) {
                    let start = timestamp();
                    if self.contact_debug_interval != 0
                        && start - last_contact_info_trace > self.contact_debug_interval
                    {
                        // Log contact info
                        info!(
                            "\n{}\n\n{}",
                            self.contact_info_trace(),
                            self.rpc_info_trace()
                        );
                        last_contact_info_trace = start;
                    }

                    if self.contact_save_interval != 0
                        && start - last_contact_info_save > self.contact_save_interval
                    {
                        self.save_contact_info();
                        last_contact_info_save = start;
                    }
                    let stakes = epoch_specs
                        .as_mut()
                        .map(EpochSpecs::current_epoch_staked_nodes)
                        .cloned()
                        .unwrap_or_default();
                    let _ = self.run_gossip(
                        &thread_pool,
                        gossip_validators.as_ref(),
                        &recycler,
                        &stakes,
                        &sender,
                        generate_pull_requests,
                    );
                    let epoch_duration = epoch_specs
                        .as_mut()
                        .map(EpochSpecs::epoch_duration)
                        .unwrap_or(DEFAULT_EPOCH_DURATION);
                    self.handle_purge(&thread_pool, epoch_duration, &stakes);
                    entrypoints_processed = entrypoints_processed || self.process_entrypoints();
                    //TODO: possibly tune this parameter
                    //we saw a deadlock passing an self.read().unwrap().timeout into sleep
                    if start - last_push > CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS / 2 {
                        self.refresh_my_gossip_contact_info();
                        self.refresh_push_active_set(
                            &recycler,
                            &stakes,
                            gossip_validators.as_ref(),
                            &sender,
                        );
                        last_push = timestamp();
                    }
                    let elapsed = timestamp() - start;
                    if GOSSIP_SLEEP_MILLIS > elapsed {
                        let time_left = GOSSIP_SLEEP_MILLIS - elapsed;
                        sleep(Duration::from_millis(time_left));
                    }
                    generate_pull_requests = !generate_pull_requests;
                }
            })
            .unwrap()
    }

    fn handle_batch_prune_messages(&self, messages: Vec<PruneData>, stakes: &HashMap<Pubkey, u64>) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_prune_messages_time);
        if messages.is_empty() {
            return;
        }
        self.stats
            .prune_message_count
            .add_relaxed(messages.len() as u64);
        self.stats
            .prune_message_len
            .add_relaxed(messages.iter().map(|data| data.prunes.len() as u64).sum());
        let mut prune_message_timeout = 0;
        let mut bad_prune_destination = 0;
        let self_pubkey = self.id();
        {
            let _st = ScopedTimer::from(&self.stats.process_prune);
            let now = timestamp();
            for data in messages {
                match self.gossip.process_prune_msg(
                    &self_pubkey,
                    &data.pubkey,
                    &data.destination,
                    &data.prunes,
                    data.wallclock,
                    now,
                    stakes,
                ) {
                    Err(CrdsGossipError::PruneMessageTimeout) => {
                        prune_message_timeout += 1;
                    }
                    Err(CrdsGossipError::BadPruneDestination) => {
                        bad_prune_destination += 1;
                    }
                    _ => (),
                }
            }
        }
        if prune_message_timeout != 0 {
            self.stats
                .prune_message_timeout
                .add_relaxed(prune_message_timeout);
        }
        if bad_prune_destination != 0 {
            self.stats
                .bad_prune_destination
                .add_relaxed(bad_prune_destination);
        }
    }

    fn handle_batch_pull_requests(
        &self,
        requests: Vec<PullRequest>,
        thread_pool: &ThreadPool,
        recycler: &PacketBatchRecycler,
        stakes: &HashMap<Pubkey, u64>,
        response_sender: &PacketBatchSender,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pull_requests_time);
        if !requests.is_empty() {
            self.stats
                .pull_requests_count
                .add_relaxed(requests.len() as u64);
            let response = self.handle_pull_requests(thread_pool, recycler, requests, stakes);
            if !response.is_empty() {
                self.stats
                    .packets_sent_pull_responses_count
                    .add_relaxed(response.len() as u64);
                let _ = response_sender.send(response);
            }
        }
    }

    fn update_data_budget(&self, num_staked: usize) -> usize {
        const INTERVAL_MS: u64 = 100;
        // epoch slots + votes ~= 1.5kB/slot ~= 4kB/s
        // Allow 10kB/s per staked validator.
        const BYTES_PER_INTERVAL: usize = 1024;
        const MAX_BUDGET_MULTIPLE: usize = 5; // allow budget build-up to 5x the interval default
        let num_staked = num_staked.max(2);
        self.outbound_budget.update(INTERVAL_MS, |bytes| {
            std::cmp::min(
                bytes + num_staked * BYTES_PER_INTERVAL,
                MAX_BUDGET_MULTIPLE * num_staked * BYTES_PER_INTERVAL,
            )
        })
    }

    // Returns a predicate checking if the pull request is from a valid
    // address, and if the address have responded to a ping request. Also
    // appends ping packets for the addresses which need to be (re)verified.
    fn check_pull_request<'a, R>(
        &'a self,
        now: Instant,
        rng: &'a mut R,
        packet_batch: &'a mut PacketBatch,
    ) -> impl FnMut(&PullRequest) -> bool + 'a
    where
        R: Rng + CryptoRng,
    {
        let mut cache = HashMap::<(Pubkey, SocketAddr), bool>::new();
        let mut ping_cache = self.ping_cache.lock().unwrap();
        let mut hard_check = move |node| {
            let (check, ping) = ping_cache.check(rng, &self.keypair(), now, node);
            if let Some(ping) = ping {
                let ping = Protocol::PingMessage(ping);
                match Packet::from_data(Some(&node.1), ping) {
                    Ok(packet) => packet_batch.push(packet),
                    Err(err) => error!("failed to write ping packet: {:?}", err),
                };
            }
            if !check {
                self.stats
                    .pull_request_ping_pong_check_failed_count
                    .add_relaxed(1)
            }
            check
        };
        // Because pull-responses are sent back to packet.meta().socket_addr() of
        // incoming pull-requests, pings are also sent to request.from_addr (as
        // opposed to caller.gossip address).
        move |request| {
            ContactInfo::is_valid_address(&request.addr, &self.socket_addr_space) && {
                let node = (request.pubkey, request.addr);
                *cache.entry(node).or_insert_with(|| hard_check(node))
            }
        }
    }

    // Pull requests take an incoming bloom filter of contained entries from a node
    // and tries to send back to them the values it detects are missing.
    fn handle_pull_requests(
        &self,
        thread_pool: &ThreadPool,
        recycler: &PacketBatchRecycler,
        mut requests: Vec<PullRequest>,
        stakes: &HashMap<Pubkey, u64>,
    ) -> PacketBatch {
        const DEFAULT_EPOCH_DURATION_MS: u64 = DEFAULT_SLOTS_PER_EPOCH * DEFAULT_MS_PER_SLOT;
        let output_size_limit =
            self.update_data_budget(stakes.len()) / PULL_RESPONSE_MIN_SERIALIZED_SIZE;
        let mut packet_batch =
            PacketBatch::new_unpinned_with_recycler(recycler, 64, "handle_pull_requests");
        let mut rng = rand::thread_rng();
        requests.retain({
            let now = Instant::now();
            self.check_pull_request(now, &mut rng, &mut packet_batch)
        });
        let now = timestamp();
        let self_id = self.id();
        let pull_responses = {
            let _st = ScopedTimer::from(&self.stats.generate_pull_responses);
            self.gossip.generate_pull_responses(
                thread_pool,
                &requests,
                output_size_limit,
                now,
                |value| {
                    should_retain_crds_value(
                        value, stakes, /*drop_unstaked_node_instance:*/ true,
                    )
                },
                &self.stats,
            )
        };
        // Prioritize more recent values, staked values and ContactInfos.
        let get_score = |value: &CrdsValue| -> u64 {
            let age = now.saturating_sub(value.wallclock());
            // score CrdsValue: 2x score if staked; 2x score if ContactInfo
            let score = DEFAULT_EPOCH_DURATION_MS
                .saturating_sub(age)
                .div(CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS)
                .max(1);
            let score = if stakes.contains_key(&value.pubkey()) {
                2 * score
            } else {
                score
            };
            let score = match value.data() {
                CrdsData::ContactInfo(_) => 2 * score,
                _ => score,
            };
            score
        };
        let mut num_crds_values = 0;
        let (scores, mut pull_responses): (Vec<_>, Vec<_>) = requests
            .iter()
            .zip(pull_responses)
            .flat_map(|(PullRequest { addr, .. }, values)| {
                num_crds_values += values.len();
                split_gossip_messages(PULL_RESPONSE_MAX_PAYLOAD_SIZE, values).map(move |values| {
                    let score = values.iter().map(get_score).max().unwrap_or_default();
                    (score, (addr, values))
                })
            })
            .collect();
        let (total_bytes, sent_crds_values, sent_pull_responses) =
            WeightedShuffle::new("handle-pull-requests", scores)
                .shuffle(&mut rng)
                .filter_map(|k| {
                    let (addr, values) = &mut pull_responses[k];
                    let num_values = values.len();
                    let response = Protocol::PullResponse(self_id, std::mem::take(values));
                    let packet = Packet::from_data(Some(addr), response)
                        .inspect_err(|err| error!("failed to write pull-response packet: {err:?}"))
                        .ok()?;
                    Some((packet, num_values))
                })
                .take_while(|(packet, _)| {
                    if self.outbound_budget.take(packet.meta().size) {
                        true
                    } else {
                        self.stats.gossip_pull_request_no_budget.add_relaxed(1);
                        false
                    }
                })
                .map(|(packet, num_values)| {
                    let num_bytes = packet.meta().size;
                    packet_batch.push(packet);
                    (num_bytes, num_values)
                })
                .fold((0, 0, 0), |a, b| (a.0 + b.0, a.1 + b.1, a.2 + 1));
        let dropped_responses = num_crds_values.saturating_sub(sent_crds_values);
        self.stats
            .gossip_pull_request_sent_requests
            .add_relaxed(sent_pull_responses as u64);
        self.stats
            .gossip_pull_request_dropped_requests
            .add_relaxed(dropped_responses as u64);
        self.stats
            .gossip_pull_request_sent_bytes
            .add_relaxed(total_bytes as u64);
        packet_batch
    }

    fn handle_batch_pull_responses(
        &self,
        responses: Vec<CrdsValue>,
        stakes: &HashMap<Pubkey, u64>,
        epoch_duration: Duration,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pull_responses_time);
        if !responses.is_empty() {
            let self_pubkey = self.id();
            let timeouts = self
                .gossip
                .make_timeouts(self_pubkey, stakes, epoch_duration);
            self.handle_pull_response(responses, &timeouts);
        }
    }

    // Returns (failed, timeout, success)
    fn handle_pull_response(
        &self,
        crds_values: Vec<CrdsValue>,
        timeouts: &CrdsTimeouts,
    ) -> (usize, usize, usize) {
        let len = crds_values.len();
        let mut pull_stats = ProcessPullStats::default();
        let (filtered_pulls, filtered_pulls_expired_timeout, failed_inserts) = {
            let _st = ScopedTimer::from(&self.stats.filter_pull_response);
            self.gossip
                .filter_pull_responses(timeouts, crds_values, timestamp(), &mut pull_stats)
        };
        if !filtered_pulls.is_empty()
            || !filtered_pulls_expired_timeout.is_empty()
            || !failed_inserts.is_empty()
        {
            let _st = ScopedTimer::from(&self.stats.process_pull_response);
            self.gossip.process_pull_responses(
                filtered_pulls,
                filtered_pulls_expired_timeout,
                failed_inserts,
                timestamp(),
                &mut pull_stats,
            );
        }
        self.stats.process_pull_response_count.add_relaxed(1);
        self.stats.process_pull_response_len.add_relaxed(len as u64);
        self.stats
            .process_pull_response_fail_insert
            .add_relaxed(pull_stats.failed_insert as u64);
        self.stats
            .process_pull_response_fail_timeout
            .add_relaxed(pull_stats.failed_timeout as u64);
        self.stats
            .process_pull_response_success
            .add_relaxed(pull_stats.success as u64);

        (
            pull_stats.failed_insert + pull_stats.failed_timeout,
            pull_stats.failed_timeout,
            pull_stats.success,
        )
    }

    fn handle_batch_ping_messages<I>(
        &self,
        pings: I,
        recycler: &PacketBatchRecycler,
        response_sender: &PacketBatchSender,
    ) where
        I: IntoIterator<Item = (SocketAddr, Ping)>,
    {
        let _st = ScopedTimer::from(&self.stats.handle_batch_ping_messages_time);
        if let Some(response) = self.handle_ping_messages(pings, recycler) {
            let _ = response_sender.send(response);
        }
    }

    fn handle_ping_messages<I>(
        &self,
        pings: I,
        recycler: &PacketBatchRecycler,
    ) -> Option<PacketBatch>
    where
        I: IntoIterator<Item = (SocketAddr, Ping)>,
    {
        let keypair = self.keypair();
        let pongs_and_dests: Vec<_> = pings
            .into_iter()
            .map(|(addr, ping)| {
                let pong = Pong::new(&ping, &keypair);
                (addr, Protocol::PongMessage(pong))
            })
            .collect();
        if pongs_and_dests.is_empty() {
            None
        } else {
            let packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
                recycler,
                "handle_ping_messages",
                &pongs_and_dests,
            );
            Some(packet_batch)
        }
    }

    fn handle_batch_pong_messages<I>(&self, pongs: I, now: Instant)
    where
        I: IntoIterator<Item = (SocketAddr, Pong)>,
    {
        let _st = ScopedTimer::from(&self.stats.handle_batch_pong_messages_time);
        let mut pongs = pongs.into_iter().peekable();
        if pongs.peek().is_some() {
            let mut ping_cache = self.ping_cache.lock().unwrap();
            for (addr, pong) in pongs {
                ping_cache.add(&pong, addr, now);
            }
        }
    }

    fn handle_batch_push_messages(
        &self,
        messages: Vec<(Pubkey, Vec<CrdsValue>)>,
        thread_pool: &ThreadPool,
        recycler: &PacketBatchRecycler,
        stakes: &HashMap<Pubkey, u64>,
        response_sender: &PacketBatchSender,
    ) {
        let _st = ScopedTimer::from(&self.stats.handle_batch_push_messages_time);
        if messages.is_empty() {
            return;
        }
        self.stats
            .push_message_count
            .add_relaxed(messages.len() as u64);
        let num_crds_values: u64 = messages.iter().map(|(_, data)| data.len() as u64).sum();
        self.stats
            .push_message_value_count
            .add_relaxed(num_crds_values);
        // Origins' pubkeys of upserted crds values.
        let origins: HashSet<_> = {
            let _st = ScopedTimer::from(&self.stats.process_push_message);
            let now = timestamp();
            self.gossip.process_push_message(messages, now)
        };
        // Generate prune messages.
        let prune_messages = self.generate_prune_messages(thread_pool, origins, stakes);
        let mut packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
            recycler,
            "handle_batch_push_messages",
            &prune_messages,
        );
        let num_prune_packets = packet_batch.len();
        self.stats
            .push_response_count
            .add_relaxed(packet_batch.len() as u64);
        let new_push_requests = self.new_push_requests(stakes);
        self.stats
            .push_message_pushes
            .add_relaxed(new_push_requests.len() as u64);
        for (address, request) in new_push_requests {
            if ContactInfo::is_valid_address(&address, &self.socket_addr_space) {
                match Packet::from_data(Some(&address), &request) {
                    Ok(packet) => packet_batch.push(packet),
                    Err(err) => error!("failed to write push-request packet: {:?}", err),
                }
            } else {
                trace!("Dropping Gossip push response, as destination is unknown");
            }
        }
        self.stats
            .packets_sent_prune_messages_count
            .add_relaxed(num_prune_packets as u64);
        self.stats
            .packets_sent_push_messages_count
            .add_relaxed((packet_batch.len() - num_prune_packets) as u64);
        if !packet_batch.is_empty() {
            let _ = response_sender.send(packet_batch);
        }
    }

    fn generate_prune_messages(
        &self,
        thread_pool: &ThreadPool,
        // Unique origin pubkeys of upserted CRDS values from push messages.
        origins: impl IntoIterator<Item = Pubkey>,
        stakes: &HashMap<Pubkey, u64>,
    ) -> Vec<(SocketAddr, Protocol /*::PruneMessage*/)> {
        let _st = ScopedTimer::from(&self.stats.generate_prune_messages);
        let self_pubkey = self.id();
        // Obtain redundant gossip links which can be pruned.
        let prunes: HashMap</*gossip peer:*/ Pubkey, /*origins:*/ Vec<Pubkey>> = {
            let _st = ScopedTimer::from(&self.stats.prune_received_cache);
            self.gossip
                .prune_received_cache(&self_pubkey, origins, stakes)
        };
        // Look up gossip addresses of destination nodes.
        let prunes: Vec<(
            Pubkey,      // gossip peer to be pruned
            SocketAddr,  // gossip socket-addr of peer
            Vec<Pubkey>, // CRDS value origins
        )> = {
            let gossip_crds = self.gossip.crds.read().unwrap();
            thread_pool.install(|| {
                prunes
                    .into_par_iter()
                    .filter_map(|(pubkey, prunes)| {
                        let addr = gossip_crds.get::<&ContactInfo>(pubkey)?.gossip()?;
                        Some((pubkey, addr, prunes))
                    })
                    .collect()
            })
        };
        // Create and sign Protocol::PruneMessages.
        thread_pool.install(|| {
            let wallclock = timestamp();
            let keypair: Arc<Keypair> = self.keypair().clone();
            prunes
                .into_par_iter()
                .flat_map(|(destination, addr, prunes)| {
                    // Chunk up origins so that each chunk fits into a packet.
                    let prunes = prunes.into_par_iter().chunks(MAX_PRUNE_DATA_NODES);
                    rayon::iter::repeat((destination, addr)).zip(prunes)
                })
                .map(|((destination, addr), prunes)| {
                    let mut prune_data = PruneData {
                        pubkey: self_pubkey,
                        prunes,
                        signature: Signature::default(),
                        destination,
                        wallclock,
                    };
                    prune_data.sign(&keypair);
                    let prune_message = Protocol::PruneMessage(self_pubkey, prune_data);
                    (addr, prune_message)
                })
                .collect()
        })
    }

    fn process_packets(
        &self,
        packets: VecDeque<(/*from:*/ SocketAddr, Protocol)>,
        thread_pool: &ThreadPool,
        recycler: &PacketBatchRecycler,
        response_sender: &PacketBatchSender,
        stakes: &HashMap<Pubkey, u64>,
        epoch_duration: Duration,
        should_check_duplicate_instance: bool,
    ) -> Result<(), GossipError> {
        let _st = ScopedTimer::from(&self.stats.process_gossip_packets_time);
        let self_pubkey = self.id();
        // Filter out values if the shred-versions are different.
        let self_shred_version = self.my_shred_version();
        let packets = if self_shred_version == 0 {
            packets
        } else {
            let gossip_crds = self.gossip.crds.read().unwrap();
            thread_pool.install(|| {
                packets
                    .into_par_iter()
                    .with_min_len(1024)
                    .filter_map(|(from, msg)| {
                        let msg = filter_on_shred_version(
                            msg,
                            self_shred_version,
                            &gossip_crds,
                            &self.stats,
                        )?;
                        Some((from, msg))
                    })
                    .collect()
            })
        };

        // Check if there is a duplicate instance of
        // this node with more recent timestamp.
        let check_duplicate_instance = {
            let my_contact_info = self.my_contact_info();
            move |values: &[CrdsValue]| {
                let mut nodes = values.iter().filter_map(CrdsValue::contact_info);
                if nodes.any(|other| my_contact_info.check_duplicate(other)) {
                    Err(GossipError::DuplicateNodeInstance)
                } else {
                    Ok(())
                }
            }
        };
        let mut pings = Vec::new();
        let mut rng = rand::thread_rng();
        let keypair: Arc<Keypair> = self.keypair().clone();
        let mut verify_gossip_addr = |value: &CrdsValue| {
            if verify_gossip_addr(
                &mut rng,
                &keypair,
                value,
                stakes,
                &self.socket_addr_space,
                &self.ping_cache,
                &mut pings,
            ) {
                true
            } else {
                self.stats.num_unverifed_gossip_addrs.add_relaxed(1);
                false
            }
        };
        // Split packets based on their types.
        let mut pull_requests = vec![];
        let mut pull_responses = vec![];
        let mut push_messages = vec![];
        let mut prune_messages = vec![];
        let mut ping_messages = vec![];
        let mut pong_messages = vec![];
        for (from_addr, packet) in packets {
            match packet {
                Protocol::PullRequest(filter, caller) => {
                    let request = PullRequest {
                        pubkey: caller.pubkey(),
                        addr: from_addr,
                        wallclock: caller.wallclock(),
                        filter,
                    };
                    if request.pubkey == self_pubkey {
                        self.stats.window_request_loopback.add_relaxed(1);
                    } else {
                        pull_requests.push(request);
                    }
                }
                Protocol::PullResponse(_, mut data) => {
                    if should_check_duplicate_instance {
                        check_duplicate_instance(&data)?;
                    }
                    data.retain(&mut verify_gossip_addr);
                    if !data.is_empty() {
                        pull_responses.append(&mut data);
                    }
                }
                Protocol::PushMessage(from, mut data) => {
                    if should_check_duplicate_instance {
                        check_duplicate_instance(&data)?;
                    }
                    data.retain(&mut verify_gossip_addr);
                    if !data.is_empty() {
                        push_messages.push((from, data));
                    }
                }
                Protocol::PruneMessage(_from, data) => prune_messages.push(data),
                Protocol::PingMessage(ping) => ping_messages.push((from_addr, ping)),
                Protocol::PongMessage(pong) => pong_messages.push((from_addr, pong)),
            }
        }
        if !pings.is_empty() {
            self.stats
                .packets_sent_gossip_requests_count
                .add_relaxed(pings.len() as u64);
            let packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
                recycler,
                "ping_contact_infos",
                &pings,
            );
            let _ = response_sender.send(packet_batch);
        }
        self.handle_batch_ping_messages(ping_messages, recycler, response_sender);
        self.handle_batch_prune_messages(prune_messages, stakes);
        self.handle_batch_push_messages(
            push_messages,
            thread_pool,
            recycler,
            stakes,
            response_sender,
        );
        self.handle_batch_pull_responses(pull_responses, stakes, epoch_duration);
        self.trim_crds_table(CRDS_UNIQUE_PUBKEY_CAPACITY, stakes);
        self.handle_batch_pong_messages(pong_messages, Instant::now());
        self.handle_batch_pull_requests(
            pull_requests,
            thread_pool,
            recycler,
            stakes,
            response_sender,
        );
        Ok(())
    }

    // Consumes packets received from the socket, deserializing, sanitizing and
    // verifying them and then sending them down the channel for the actual
    // handling of requests/messages.
    fn run_socket_consume(
        &self,
        thread_pool: &ThreadPool,
        epoch_specs: Option<&mut EpochSpecs>,
        receiver: &PacketBatchReceiver,
        sender: &Sender<Vec<(/*from:*/ SocketAddr, Protocol)>>,
    ) -> Result<(), GossipError> {
        const RECV_TIMEOUT: Duration = Duration::from_secs(1);
        fn count_dropped_packets(packets: &PacketBatch, dropped_packets_counts: &mut [u64; 7]) {
            for packet in packets {
                let k = packet
                    .data(..4)
                    .and_then(|data| <[u8; 4]>::try_from(data).ok())
                    .map(u32::from_le_bytes)
                    .filter(|&k| k < 6)
                    .unwrap_or(/*invalid:*/ 6) as usize;
                dropped_packets_counts[k] += 1;
            }
        }
        let mut dropped_packets_counts = [0u64; 7];
        let mut num_packets = 0;
        let mut packets = VecDeque::with_capacity(2);
        for packet_batch in receiver
            .recv_timeout(RECV_TIMEOUT)
            .map(std::iter::once)?
            .chain(receiver.try_iter())
        {
            num_packets += packet_batch.len();
            packets.push_back(packet_batch);
            while num_packets > MAX_GOSSIP_TRAFFIC {
                // Discard older packets in favor of more recent ones.
                let Some(packet_batch) = packets.pop_front() else {
                    break;
                };
                num_packets -= packet_batch.len();
                count_dropped_packets(&packet_batch, &mut dropped_packets_counts);
            }
        }
        let num_packets_dropped = self.stats.record_dropped_packets(&dropped_packets_counts);
        self.stats
            .packets_received_count
            .add_relaxed(num_packets as u64 + num_packets_dropped);
        fn verify_packet(
            packet: &Packet,
            stakes: &HashMap<Pubkey, u64>,
            stats: &GossipStats,
        ) -> Option<(SocketAddr, Protocol)> {
            let mut protocol: Protocol =
                stats.record_received_packet(packet.deserialize_slice::<Protocol, _>(..))?;
            protocol.sanitize().ok()?;
            if let Protocol::PullResponse(_, values) | Protocol::PushMessage(_, values) =
                &mut protocol
            {
                values.retain(|value| {
                    should_retain_crds_value(
                        value, stakes, /*drop_unstaked_node_instance:*/ false,
                    )
                });
                if values.is_empty() {
                    return None;
                }
            }
            protocol.par_verify().then(|| {
                stats.packets_received_verified_count.add_relaxed(1);
                (packet.meta().socket_addr(), protocol)
            })
        }
        let stakes = epoch_specs
            .map(EpochSpecs::current_epoch_staked_nodes)
            .cloned()
            .unwrap_or_default();
        let packets: Vec<_> = {
            let _st = ScopedTimer::from(&self.stats.verify_gossip_packets_time);
            thread_pool.install(|| {
                if packets.len() == 1 {
                    packets[0]
                        .par_iter()
                        .filter_map(|packet| verify_packet(packet, &stakes, &self.stats))
                        .collect()
                } else {
                    packets
                        .par_iter()
                        .flatten()
                        .filter_map(|packet| verify_packet(packet, &stakes, &self.stats))
                        .collect()
                }
            })
        };
        Ok(sender.send(packets)?)
    }

    /// Process messages from the network
    fn run_listen(
        &self,
        recycler: &PacketBatchRecycler,
        mut epoch_specs: Option<&mut EpochSpecs>,
        receiver: &Receiver<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        response_sender: &PacketBatchSender,
        thread_pool: &ThreadPool,
        last_print: &mut Instant,
        should_check_duplicate_instance: bool,
    ) -> Result<(), GossipError> {
        let _st = ScopedTimer::from(&self.stats.gossip_listen_loop_time);
        const RECV_TIMEOUT: Duration = Duration::from_secs(1);
        const SUBMIT_GOSSIP_STATS_INTERVAL: Duration = Duration::from_secs(2);
        let mut packets = VecDeque::from(receiver.recv_timeout(RECV_TIMEOUT)?);
        for payload in receiver.try_iter() {
            packets.extend(payload);
            let excess_count = packets.len().saturating_sub(MAX_GOSSIP_TRAFFIC);
            if excess_count > 0 {
                packets.drain(0..excess_count);
                self.stats
                    .gossip_packets_dropped_count
                    .add_relaxed(excess_count as u64);
            }
        }
        let stakes = epoch_specs
            .as_mut()
            .map(|epoch_specs| epoch_specs.current_epoch_staked_nodes())
            .cloned()
            .unwrap_or_default();
        let epoch_duration = epoch_specs
            .map(EpochSpecs::epoch_duration)
            .unwrap_or(DEFAULT_EPOCH_DURATION);
        self.process_packets(
            packets,
            thread_pool,
            recycler,
            response_sender,
            &stakes,
            epoch_duration,
            should_check_duplicate_instance,
        )?;
        if last_print.elapsed() > SUBMIT_GOSSIP_STATS_INTERVAL {
            submit_gossip_stats(&self.stats, &self.gossip, &stakes);
            *last_print = Instant::now();
        }
        self.stats
            .gossip_listen_loop_iterations_since_last_report
            .add_relaxed(1);
        Ok(())
    }

    pub(crate) fn start_socket_consume_thread(
        self: Arc<Self>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
        receiver: PacketBatchReceiver,
        sender: Sender<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(get_thread_count().min(8))
            .thread_name(|i| format!("solGossipCons{i:02}"))
            .build()
            .unwrap();
        let mut epoch_specs = bank_forks.map(EpochSpecs::from);
        let run_consume = move || {
            while !exit.load(Ordering::Relaxed) {
                match self.run_socket_consume(
                    &thread_pool,
                    epoch_specs.as_mut(),
                    &receiver,
                    &sender,
                ) {
                    Err(GossipError::RecvTimeoutError(RecvTimeoutError::Disconnected)) => break,
                    Err(GossipError::RecvTimeoutError(RecvTimeoutError::Timeout)) => (),
                    // A send operation can only fail if the receiving end of a
                    // channel is disconnected.
                    Err(GossipError::SendError) => break,
                    Err(err) => error!("gossip consume: {}", err),
                    Ok(()) => (),
                }
            }
        };
        let thread_name = String::from("solGossipConsum");
        Builder::new().name(thread_name).spawn(run_consume).unwrap()
    }

    pub(crate) fn listen(
        self: Arc<Self>,
        bank_forks: Option<Arc<RwLock<BankForks>>>,
        requests_receiver: Receiver<Vec<(/*from:*/ SocketAddr, Protocol)>>,
        response_sender: PacketBatchSender,
        should_check_duplicate_instance: bool,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let mut last_print = Instant::now();
        let recycler = PacketBatchRecycler::default();
        let thread_pool = ThreadPoolBuilder::new()
            .num_threads(get_thread_count().min(8))
            .thread_name(|i| format!("solGossipWork{i:02}"))
            .build()
            .unwrap();
        let mut epoch_specs = bank_forks.map(EpochSpecs::from);
        Builder::new()
            .name("solGossipListen".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    if let Err(err) = self.run_listen(
                        &recycler,
                        epoch_specs.as_mut(),
                        &requests_receiver,
                        &response_sender,
                        &thread_pool,
                        &mut last_print,
                        should_check_duplicate_instance,
                    ) {
                        match err {
                            GossipError::RecvTimeoutError(RecvTimeoutError::Disconnected) => break,
                            GossipError::RecvTimeoutError(RecvTimeoutError::Timeout) => {
                                let table_size = self.gossip.crds.read().unwrap().len();
                                debug!(
                                    "{}: run_listen timeout, table size: {}",
                                    self.id(),
                                    table_size,
                                );
                            }
                            GossipError::DuplicateNodeInstance => {
                                error!(
                                    "duplicate running instances of the same validator node: {}",
                                    self.id()
                                );
                                exit.store(true, Ordering::Relaxed);
                                // TODO: Pass through Exit here so
                                // that this will exit cleanly.
                                std::process::exit(1);
                            }
                            _ => error!("gossip run_listen failed: {}", err),
                        }
                    }
                }
            })
            .unwrap()
    }

    pub fn gossip_contact_info(id: Pubkey, gossip: SocketAddr, shred_version: u16) -> ContactInfo {
        let mut node = ContactInfo::new(id, /*wallclock:*/ timestamp(), shred_version);
        let _ = node.set_gossip(gossip);
        node
    }

    /// An alternative to Spy Node that has a valid gossip address and fully participate in Gossip.
    pub fn gossip_node(
        id: Pubkey,
        gossip_addr: &SocketAddr,
        shred_version: u16,
    ) -> (ContactInfo, UdpSocket, Option<TcpListener>) {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let (port, (gossip_socket, ip_echo)) =
            Node::get_gossip_port(gossip_addr, VALIDATOR_PORT_RANGE, bind_ip_addr);
        let contact_info =
            Self::gossip_contact_info(id, SocketAddr::new(gossip_addr.ip(), port), shred_version);

        (contact_info, gossip_socket, Some(ip_echo))
    }

    /// A Node with dummy ports to spy on gossip via pull requests
    pub fn spy_node(
        id: Pubkey,
        shred_version: u16,
    ) -> (ContactInfo, UdpSocket, Option<TcpListener>) {
        let bind_ip_addr = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let (port, gossip_socket) = bind_in_range(bind_ip_addr, VALIDATOR_PORT_RANGE).unwrap();
        let contact_info = Self::gossip_contact_info(
            id,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port),
            shred_version,
        );
        (contact_info, gossip_socket, None)
    }
}

#[derive(Debug)]
pub struct Sockets {
    pub gossip: UdpSocket,
    pub ip_echo: Option<TcpListener>,
    pub tvu: Vec<UdpSocket>,
    pub tvu_quic: UdpSocket,
    pub tpu: Vec<UdpSocket>,
    pub tpu_forwards: Vec<UdpSocket>,
    pub tpu_vote: Vec<UdpSocket>,
    pub broadcast: Vec<UdpSocket>,
    // Socket sending out local repair requests,
    // and receiving repair responses from the cluster.
    pub repair: UdpSocket,
    pub repair_quic: UdpSocket,
    pub retransmit_sockets: Vec<UdpSocket>,
    // Socket receiving remote repair requests from the cluster,
    // and sending back repair responses.
    pub serve_repair: UdpSocket,
    pub serve_repair_quic: UdpSocket,
    // Socket sending out local RepairProtocol::AncestorHashes,
    // and receiving AncestorHashesResponse from the cluster.
    pub ancestor_hashes_requests: UdpSocket,
    pub ancestor_hashes_requests_quic: UdpSocket,
    pub tpu_quic: Vec<UdpSocket>,
    pub tpu_forwards_quic: Vec<UdpSocket>,
    pub tpu_vote_quic: Vec<UdpSocket>,
}

pub struct NodeConfig {
    pub gossip_addr: SocketAddr,
    pub port_range: PortRange,
    pub bind_ip_addr: IpAddr,
    pub public_tpu_addr: Option<SocketAddr>,
    pub public_tpu_forwards_addr: Option<SocketAddr>,
    /// The number of TVU sockets to create
    pub num_tvu_sockets: NonZeroUsize,
    /// The number of QUIC tpu endpoints
    pub num_quic_endpoints: NonZeroUsize,
}

#[derive(Debug)]
pub struct Node {
    pub info: ContactInfo,
    pub sockets: Sockets,
}

impl Node {
    pub fn new_localhost() -> Self {
        let pubkey = solana_pubkey::new_rand();
        Self::new_localhost_with_pubkey(&pubkey)
    }

    pub fn new_localhost_with_pubkey(pubkey: &Pubkey) -> Self {
        Self::new_localhost_with_pubkey_and_quic_endpoints(pubkey, DEFAULT_QUIC_ENDPOINTS)
    }

    pub fn new_localhost_with_pubkey_and_quic_endpoints(
        pubkey: &Pubkey,
        num_quic_endpoints: usize,
    ) -> Self {
        let localhost_ip_addr = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let port_range = (1024, 65535);

        let udp_config = SocketConfig::default();
        let quic_config = SocketConfig::default().reuseport(true);
        let ((_tpu_port, tpu), (_tpu_quic_port, tpu_quic)) =
            bind_two_in_range_with_offset_and_config(
                localhost_ip_addr,
                port_range,
                QUIC_PORT_OFFSET,
                udp_config,
                quic_config,
            )
            .unwrap();
        let tpu_quic = bind_more_with_config(tpu_quic, num_quic_endpoints, quic_config).unwrap();
        let (gossip_port, (gossip, ip_echo)) =
            bind_common_in_range_with_config(localhost_ip_addr, port_range, udp_config).unwrap();
        let gossip_addr = SocketAddr::new(localhost_ip_addr, gossip_port);
        let tvu = bind_to_localhost().unwrap();
        let tvu_quic = bind_to_localhost().unwrap();
        let ((_tpu_forwards_port, tpu_forwards), (_tpu_forwards_quic_port, tpu_forwards_quic)) =
            bind_two_in_range_with_offset_and_config(
                localhost_ip_addr,
                port_range,
                QUIC_PORT_OFFSET,
                udp_config,
                quic_config,
            )
            .unwrap();
        let tpu_forwards_quic =
            bind_more_with_config(tpu_forwards_quic, num_quic_endpoints, quic_config).unwrap();
        let tpu_vote = bind_to_localhost().unwrap();
        let tpu_vote_quic = bind_to_localhost().unwrap();
        let tpu_vote_quic =
            bind_more_with_config(tpu_vote_quic, num_quic_endpoints, quic_config).unwrap();

        let repair = bind_to_localhost().unwrap();
        let repair_quic = bind_to_localhost().unwrap();
        let rpc_port = find_available_port_in_range(localhost_ip_addr, port_range).unwrap();
        let rpc_addr = SocketAddr::new(localhost_ip_addr, rpc_port);
        let rpc_pubsub_port = find_available_port_in_range(localhost_ip_addr, port_range).unwrap();
        let rpc_pubsub_addr = SocketAddr::new(localhost_ip_addr, rpc_pubsub_port);
        let broadcast = vec![bind_to_unspecified().unwrap()];
        let retransmit_socket = bind_to_unspecified().unwrap();
        let serve_repair = bind_to_localhost().unwrap();
        let serve_repair_quic = bind_to_localhost().unwrap();
        let ancestor_hashes_requests = bind_to_unspecified().unwrap();
        let ancestor_hashes_requests_quic = bind_to_unspecified().unwrap();

        let mut info = ContactInfo::new(
            *pubkey,
            timestamp(), // wallclock
            0u16,        // shred_version
        );
        macro_rules! set_socket {
            ($method:ident, $addr:expr, $name:literal) => {
                info.$method($addr).expect(&format!(
                    "Operator must spin up node with valid {} address",
                    $name
                ))
            };
            ($method:ident, $protocol:ident, $addr:expr, $name:literal) => {{
                info.$method(contact_info::Protocol::$protocol, $addr)
                    .expect(&format!(
                        "Operator must spin up node with valid {} address",
                        $name
                    ))
            }};
        }
        set_socket!(set_gossip, gossip_addr, "gossip");
        set_socket!(set_tvu, UDP, tvu.local_addr().unwrap(), "TVU");
        set_socket!(set_tvu, QUIC, tvu_quic.local_addr().unwrap(), "TVU QUIC");
        set_socket!(set_tpu, tpu.local_addr().unwrap(), "TPU");
        set_socket!(
            set_tpu_forwards,
            tpu_forwards.local_addr().unwrap(),
            "TPU-forwards"
        );
        set_socket!(
            set_tpu_vote,
            UDP,
            tpu_vote.local_addr().unwrap(),
            "TPU-vote"
        );
        set_socket!(
            set_tpu_vote,
            QUIC,
            tpu_vote_quic[0].local_addr().unwrap(),
            "TPU-vote QUIC"
        );
        set_socket!(set_rpc, rpc_addr, "RPC");
        set_socket!(set_rpc_pubsub, rpc_pubsub_addr, "RPC-pubsub");
        set_socket!(
            set_serve_repair,
            UDP,
            serve_repair.local_addr().unwrap(),
            "serve-repair"
        );
        set_socket!(
            set_serve_repair,
            QUIC,
            serve_repair_quic.local_addr().unwrap(),
            "serve-repair QUIC"
        );
        Node {
            info,
            sockets: Sockets {
                gossip,
                ip_echo: Some(ip_echo),
                tvu: vec![tvu],
                tvu_quic,
                tpu: vec![tpu],
                tpu_forwards: vec![tpu_forwards],
                tpu_vote: vec![tpu_vote],
                broadcast,
                repair,
                repair_quic,
                retransmit_sockets: vec![retransmit_socket],
                serve_repair,
                serve_repair_quic,
                ancestor_hashes_requests,
                ancestor_hashes_requests_quic,
                tpu_quic,
                tpu_forwards_quic,
                tpu_vote_quic,
            },
        }
    }

    fn get_gossip_port(
        gossip_addr: &SocketAddr,
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> (u16, (UdpSocket, TcpListener)) {
        let config = SocketConfig::default();
        if gossip_addr.port() != 0 {
            (
                gossip_addr.port(),
                bind_common_with_config(bind_ip_addr, gossip_addr.port(), config).unwrap_or_else(
                    |e| panic!("gossip_addr bind_to port {}: {}", gossip_addr.port(), e),
                ),
            )
        } else {
            bind_common_in_range_with_config(bind_ip_addr, port_range, config)
                .expect("Failed to bind")
        }
    }

    fn bind_with_config(
        bind_ip_addr: IpAddr,
        port_range: PortRange,
        config: SocketConfig,
    ) -> (u16, UdpSocket) {
        bind_in_range_with_config(bind_ip_addr, port_range, config).expect("Failed to bind")
    }

    pub fn new_single_bind(
        pubkey: &Pubkey,
        gossip_addr: &SocketAddr,
        port_range: PortRange,
        bind_ip_addr: IpAddr,
    ) -> Self {
        let (gossip_port, (gossip, ip_echo)) =
            Self::get_gossip_port(gossip_addr, port_range, bind_ip_addr);

        let socket_config = SocketConfig::default();
        let socket_config_reuseport = SocketConfig::default().reuseport(true);
        let (tvu_port, tvu) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (tvu_quic_port, tvu_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let ((tpu_port, tpu), (_tpu_quic_port, tpu_quic)) =
            bind_two_in_range_with_offset_and_config(
                bind_ip_addr,
                port_range,
                QUIC_PORT_OFFSET,
                socket_config,
                socket_config_reuseport,
            )
            .unwrap();
        let tpu_quic: Vec<UdpSocket> =
            bind_more_with_config(tpu_quic, DEFAULT_QUIC_ENDPOINTS, socket_config_reuseport)
                .unwrap();

        let ((tpu_forwards_port, tpu_forwards), (_tpu_forwards_quic_port, tpu_forwards_quic)) =
            bind_two_in_range_with_offset_and_config(
                bind_ip_addr,
                port_range,
                QUIC_PORT_OFFSET,
                socket_config,
                socket_config_reuseport,
            )
            .unwrap();
        let tpu_forwards_quic = bind_more_with_config(
            tpu_forwards_quic,
            DEFAULT_QUIC_ENDPOINTS,
            socket_config_reuseport,
        )
        .unwrap();

        let (tpu_vote_port, tpu_vote) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (tpu_vote_quic_port, tpu_vote_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let tpu_vote_quic: Vec<UdpSocket> = bind_more_with_config(
            tpu_vote_quic,
            DEFAULT_QUIC_ENDPOINTS,
            socket_config_reuseport,
        )
        .unwrap();

        let (_, retransmit_socket) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, repair) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, repair_quic) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (serve_repair_port, serve_repair) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (serve_repair_quic_port, serve_repair_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, broadcast) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, ancestor_hashes_requests) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, ancestor_hashes_requests_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let rpc_port = find_available_port_in_range(bind_ip_addr, port_range).unwrap();
        let rpc_pubsub_port = find_available_port_in_range(bind_ip_addr, port_range).unwrap();

        let addr = gossip_addr.ip();
        let mut info = ContactInfo::new(
            *pubkey,
            timestamp(), // wallclock
            0u16,        // shred_version
        );
        macro_rules! set_socket {
            ($method:ident, $port:ident, $name:literal) => {
                info.$method((addr, $port)).expect(&format!(
                    "Operator must spin up node with valid {} address",
                    $name
                ))
            };
            ($method:ident, $protocol:ident, $port:ident, $name:literal) => {{
                info.$method(contact_info::Protocol::$protocol, (addr, $port))
                    .expect(&format!(
                        "Operator must spin up node with valid {} address",
                        $name
                    ))
            }};
        }
        set_socket!(set_gossip, gossip_port, "gossip");
        set_socket!(set_tvu, UDP, tvu_port, "TVU");
        set_socket!(set_tvu, QUIC, tvu_quic_port, "TVU QUIC");
        set_socket!(set_tpu, tpu_port, "TPU");
        set_socket!(set_tpu_forwards, tpu_forwards_port, "TPU-forwards");
        set_socket!(set_tpu_vote, UDP, tpu_vote_port, "TPU-vote");
        set_socket!(set_tpu_vote, QUIC, tpu_vote_quic_port, "TPU-vote QUIC");
        set_socket!(set_rpc, rpc_port, "RPC");
        set_socket!(set_rpc_pubsub, rpc_pubsub_port, "RPC-pubsub");
        set_socket!(set_serve_repair, UDP, serve_repair_port, "serve-repair");
        set_socket!(
            set_serve_repair,
            QUIC,
            serve_repair_quic_port,
            "serve-repair QUIC"
        );

        trace!("new ContactInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                ip_echo: Some(ip_echo),
                tvu: vec![tvu],
                tvu_quic,
                tpu: vec![tpu],
                tpu_forwards: vec![tpu_forwards],
                tpu_vote: vec![tpu_vote],
                broadcast: vec![broadcast],
                repair,
                repair_quic,
                retransmit_sockets: vec![retransmit_socket],
                serve_repair,
                serve_repair_quic,
                ancestor_hashes_requests,
                ancestor_hashes_requests_quic,
                tpu_quic,
                tpu_forwards_quic,
                tpu_vote_quic,
            },
        }
    }

    pub fn new_with_external_ip(pubkey: &Pubkey, config: NodeConfig) -> Node {
        let NodeConfig {
            gossip_addr,
            port_range,
            bind_ip_addr,
            public_tpu_addr,
            public_tpu_forwards_addr,
            num_tvu_sockets,
            num_quic_endpoints,
        } = config;

        let (gossip_port, (gossip, ip_echo)) =
            Self::get_gossip_port(&gossip_addr, port_range, bind_ip_addr);

        let socket_config = SocketConfig::default();
        let socket_config_reuseport = SocketConfig::default().reuseport(true);

        let (tvu_port, tvu_sockets) = multi_bind_in_range_with_config(
            bind_ip_addr,
            port_range,
            socket_config_reuseport,
            num_tvu_sockets.get(),
        )
        .expect("tvu multi_bind");

        let (tvu_quic_port, tvu_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let (tpu_port, tpu_sockets) =
            multi_bind_in_range_with_config(bind_ip_addr, port_range, socket_config_reuseport, 32)
                .expect("tpu multi_bind");

        let (_tpu_port_quic, tpu_quic) = Self::bind_with_config(
            bind_ip_addr,
            (tpu_port + QUIC_PORT_OFFSET, tpu_port + QUIC_PORT_OFFSET + 1),
            socket_config_reuseport,
        );
        let tpu_quic =
            bind_more_with_config(tpu_quic, num_quic_endpoints.get(), socket_config_reuseport)
                .unwrap();

        let (tpu_forwards_port, tpu_forwards_sockets) =
            multi_bind_in_range_with_config(bind_ip_addr, port_range, socket_config_reuseport, 8)
                .expect("tpu_forwards multi_bind");

        let (_tpu_forwards_port_quic, tpu_forwards_quic) = Self::bind_with_config(
            bind_ip_addr,
            (
                tpu_forwards_port + QUIC_PORT_OFFSET,
                tpu_forwards_port + QUIC_PORT_OFFSET + 1,
            ),
            socket_config_reuseport,
        );
        let tpu_forwards_quic = bind_more_with_config(
            tpu_forwards_quic,
            num_quic_endpoints.get(),
            socket_config_reuseport,
        )
        .unwrap();

        let (tpu_vote_port, tpu_vote_sockets) =
            multi_bind_in_range_with_config(bind_ip_addr, port_range, socket_config_reuseport, 1)
                .expect("tpu_vote multi_bind");

        let (tpu_vote_quic_port, tpu_vote_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let tpu_vote_quic = bind_more_with_config(
            tpu_vote_quic,
            num_quic_endpoints.get(),
            socket_config_reuseport,
        )
        .unwrap();

        let (_, retransmit_sockets) =
            multi_bind_in_range_with_config(bind_ip_addr, port_range, socket_config_reuseport, 8)
                .expect("retransmit multi_bind");

        let (_, repair) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, repair_quic) = Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let (serve_repair_port, serve_repair) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (serve_repair_quic_port, serve_repair_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let (_, broadcast) =
            multi_bind_in_range_with_config(bind_ip_addr, port_range, socket_config_reuseport, 4)
                .expect("broadcast multi_bind");

        let (_, ancestor_hashes_requests) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);
        let (_, ancestor_hashes_requests_quic) =
            Self::bind_with_config(bind_ip_addr, port_range, socket_config);

        let mut info = ContactInfo::new(
            *pubkey,
            timestamp(), // wallclock
            0u16,        // shred_version
        );
        let addr = gossip_addr.ip();
        use contact_info::Protocol::{QUIC, UDP};
        info.set_gossip((addr, gossip_port)).unwrap();
        info.set_tvu(UDP, (addr, tvu_port)).unwrap();
        info.set_tvu(QUIC, (addr, tvu_quic_port)).unwrap();
        info.set_tpu(public_tpu_addr.unwrap_or_else(|| SocketAddr::new(addr, tpu_port)))
            .unwrap();
        info.set_tpu_forwards(
            public_tpu_forwards_addr.unwrap_or_else(|| SocketAddr::new(addr, tpu_forwards_port)),
        )
        .unwrap();
        info.set_tpu_vote(UDP, (addr, tpu_vote_port)).unwrap();
        info.set_tpu_vote(QUIC, (addr, tpu_vote_quic_port)).unwrap();
        info.set_serve_repair(UDP, (addr, serve_repair_port))
            .unwrap();
        info.set_serve_repair(QUIC, (addr, serve_repair_quic_port))
            .unwrap();

        trace!("new ContactInfo: {:?}", info);

        Node {
            info,
            sockets: Sockets {
                gossip,
                tvu: tvu_sockets,
                tvu_quic,
                tpu: tpu_sockets,
                tpu_forwards: tpu_forwards_sockets,
                tpu_vote: tpu_vote_sockets,
                broadcast,
                repair,
                repair_quic,
                retransmit_sockets,
                serve_repair,
                serve_repair_quic,
                ip_echo: Some(ip_echo),
                ancestor_hashes_requests,
                ancestor_hashes_requests_quic,
                tpu_quic,
                tpu_forwards_quic,
                tpu_vote_quic,
            },
        }
    }
}

pub fn push_messages_to_peer_for_tests(
    messages: Vec<CrdsValue>,
    self_id: Pubkey,
    peer_gossip: SocketAddr,
    socket_addr_space: &SocketAddrSpace,
) -> Result<(), GossipError> {
    let reqs: Vec<_> = split_gossip_messages(PUSH_MESSAGE_MAX_PAYLOAD_SIZE, messages)
        .map(move |payload| (peer_gossip, Protocol::PushMessage(self_id, payload)))
        .collect();
    let packet_batch = PacketBatch::new_unpinned_with_recycler_data_and_dests(
        &PacketBatchRecycler::default(),
        "push_messages_to_peer",
        &reqs,
    );
    let sock = bind_to_unspecified().unwrap();
    packet::send_to(&packet_batch, &sock, socket_addr_space)?;
    Ok(())
}

// Filters out values from nodes with different shred-version.
fn filter_on_shred_version(
    mut msg: Protocol,
    self_shred_version: u16,
    crds: &Crds,
    stats: &GossipStats,
) -> Option<Protocol> {
    let filter_values = |from: &Pubkey, values: &mut Vec<CrdsValue>, skipped_counter: &Counter| {
        let num_values = values.len();
        // Node-instances are always exempted from shred-version check so that:
        // * their propagation across cluster is expedited.
        // * prevent two running instances of the same identity key cross
        //   contaminate gossip between clusters.
        if crds.get_shred_version(from) == Some(self_shred_version) {
            values.retain(|value| match value.data() {
                // Allow contact-infos so that shred-versions are updated.
                CrdsData::ContactInfo(_) => true,
                CrdsData::LegacyContactInfo(_) => true,
                CrdsData::NodeInstance(_) => true,
                // Only retain values with the same shred version.
                _ => crds.get_shred_version(&value.pubkey()) == Some(self_shred_version),
            })
        } else {
            values.retain(|value| match value.data() {
                // Allow node to update its own contact info in case their
                // shred-version changes
                CrdsData::ContactInfo(node) => node.pubkey() == from,
                CrdsData::LegacyContactInfo(node) => node.pubkey() == from,
                CrdsData::NodeInstance(_) => true,
                _ => false,
            })
        }
        let num_skipped = num_values - values.len();
        if num_skipped != 0 {
            skipped_counter.add_relaxed(num_skipped as u64);
        }
    };
    match &mut msg {
        Protocol::PullRequest(_, caller) => match caller.data() {
            // Allow spy nodes with shred-verion == 0 to pull from other nodes.
            CrdsData::LegacyContactInfo(node)
                if node.shred_version() == 0 || node.shred_version() == self_shred_version =>
            {
                Some(msg)
            }
            CrdsData::ContactInfo(node)
                if node.shred_version() == 0 || node.shred_version() == self_shred_version =>
            {
                Some(msg)
            }
            _ => {
                stats.skip_pull_shred_version.add_relaxed(1);
                None
            }
        },
        Protocol::PullResponse(from, values) => {
            filter_values(from, values, &stats.skip_pull_response_shred_version);
            if values.is_empty() {
                None
            } else {
                Some(msg)
            }
        }
        Protocol::PushMessage(from, values) => {
            filter_values(from, values, &stats.skip_push_message_shred_version);
            if values.is_empty() {
                None
            } else {
                Some(msg)
            }
        }
        Protocol::PruneMessage(_, _) | Protocol::PingMessage(_) | Protocol::PongMessage(_) => {
            Some(msg)
        }
    }
}

// If the CRDS value is an unstaked contact-info, verifies if
// it has responded to ping on its gossip socket address.
// Returns false if the CRDS value should be discarded.
#[must_use]
fn verify_gossip_addr<R: Rng + CryptoRng>(
    rng: &mut R,
    keypair: &Keypair,
    value: &CrdsValue,
    stakes: &HashMap<Pubkey, u64>,
    socket_addr_space: &SocketAddrSpace,
    ping_cache: &Mutex<PingCache>,
    pings: &mut Vec<(SocketAddr, Protocol /* ::PingMessage */)>,
) -> bool {
    let (pubkey, addr) = match value.data() {
        CrdsData::ContactInfo(node) => (node.pubkey(), node.gossip()),
        CrdsData::LegacyContactInfo(node) => (node.pubkey(), node.gossip()),
        _ => return true, // If not a contact-info, nothing to verify.
    };
    // For (sufficiently) staked nodes, don't bother with ping/pong.
    if stakes.get(pubkey) >= Some(&MIN_STAKE_FOR_GOSSIP) {
        return true;
    }
    // Invalid addresses are not verifiable.
    let Some(addr) = addr.filter(|addr| socket_addr_space.check(addr)) else {
        return false;
    };
    let (out, ping) = {
        let node = (*pubkey, addr);
        let mut ping_cache = ping_cache.lock().unwrap();
        ping_cache.check(rng, keypair, Instant::now(), node)
    };
    if let Some(ping) = ping {
        pings.push((addr, Protocol::PingMessage(ping)));
    }
    out
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            crds_gossip_pull::tests::MIN_NUM_BLOOM_FILTERS,
            crds_value::{CrdsValue, CrdsValueLabel},
            duplicate_shred::tests::new_rand_shred,
            protocol::tests::new_rand_remote_node,
            socketaddr,
        },
        bincode::serialize,
        itertools::izip,
        solana_ledger::shred::Shredder,
        solana_net_utils::bind_to,
        solana_sdk::signature::{Keypair, Signer},
        solana_vote_program::{vote_instruction, vote_state::Vote},
        std::{
            iter::repeat_with,
            net::{IpAddr, Ipv4Addr},
            panic,
            sync::Arc,
        },
    };
    const DEFAULT_NUM_QUIC_ENDPOINTS: NonZeroUsize =
        unsafe { NonZeroUsize::new_unchecked(DEFAULT_QUIC_ENDPOINTS) };

    #[test]
    fn test_gossip_node() {
        //check that a gossip nodes always show up as spies
        let (node, _, _) = ClusterInfo::spy_node(solana_pubkey::new_rand(), 0);
        assert!(ClusterInfo::is_spy_node(
            &node,
            &SocketAddrSpace::Unspecified
        ));
        let (node, _, _) =
            ClusterInfo::gossip_node(solana_pubkey::new_rand(), &"1.1.1.1:0".parse().unwrap(), 0);
        assert!(ClusterInfo::is_spy_node(
            &node,
            &SocketAddrSpace::Unspecified
        ));
    }

    #[test]
    fn test_handle_pull() {
        solana_logger::setup();
        let cluster_info = Arc::new({
            let keypair = Arc::new(Keypair::new());
            let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
            ClusterInfo::new(node.info, keypair, SocketAddrSpace::Unspecified)
        });
        let entrypoint_pubkey = solana_pubkey::new_rand();
        let data = test_crds_values(entrypoint_pubkey);
        let stakes = HashMap::from([(Pubkey::new_unique(), 1u64)]);
        let timeouts = CrdsTimeouts::new(
            cluster_info.id(),
            CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS, // default_timeout
            Duration::from_secs(48 * 3600),   // epoch_duration
            &stakes,
        );
        assert_eq!(
            (0, 0, 1),
            cluster_info.handle_pull_response(data.clone(), &timeouts)
        );
        assert_eq!(
            (1, 0, 0),
            cluster_info.handle_pull_response(data, &timeouts)
        );
    }

    #[test]
    fn test_handle_pong_messages() {
        let now = Instant::now();
        let mut rng = rand::thread_rng();
        let this_node = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&this_node.pubkey(), timestamp()),
            this_node.clone(),
            SocketAddrSpace::Unspecified,
        );
        let remote_nodes: Vec<(Keypair, SocketAddr)> =
            repeat_with(|| new_rand_remote_node(&mut rng))
                .take(128)
                .collect();
        let pings: Vec<_> = {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            remote_nodes
                .iter()
                .map(|(keypair, socket)| {
                    let node = (keypair.pubkey(), *socket);
                    let (check, ping) = ping_cache.check(&mut rng, &this_node, now, node);
                    // Assert that initially remote nodes will not pass the
                    // ping/pong check.
                    assert!(!check);
                    ping.unwrap()
                })
                .collect()
        };
        let pongs: Vec<(SocketAddr, Pong)> = pings
            .iter()
            .zip(&remote_nodes)
            .map(|(ping, (keypair, socket))| (*socket, Pong::new(ping, keypair)))
            .collect();
        let now = now + Duration::from_millis(1);
        cluster_info.handle_batch_pong_messages(pongs, now);
        // Assert that remote nodes now pass the ping/pong check.
        {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            for (keypair, socket) in &remote_nodes {
                let node = (keypair.pubkey(), *socket);
                let (check, _) = ping_cache.check(&mut rng, &this_node, now, node);
                assert!(check);
            }
        }
        // Assert that a new random remote node still will not pass the check.
        {
            let mut ping_cache = cluster_info.ping_cache.lock().unwrap();
            let (keypair, socket) = new_rand_remote_node(&mut rng);
            let node = (keypair.pubkey(), socket);
            let (check, _) = ping_cache.check(&mut rng, &this_node, now, node);
            assert!(!check);
        }
    }

    #[test]
    fn test_handle_ping_messages() {
        let mut rng = rand::thread_rng();
        let this_node = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&this_node.pubkey(), timestamp()),
            this_node.clone(),
            SocketAddrSpace::Unspecified,
        );
        let remote_nodes: Vec<(Keypair, SocketAddr)> =
            repeat_with(|| new_rand_remote_node(&mut rng))
                .take(128)
                .collect();
        let pings: Vec<_> = remote_nodes
            .iter()
            .map(|(keypair, _)| Ping::new(rng.gen(), keypair))
            .collect();
        let pongs: Vec<_> = pings
            .iter()
            .map(|ping| Pong::new(ping, &this_node))
            .collect();
        let recycler = PacketBatchRecycler::default();
        let packets = cluster_info
            .handle_ping_messages(
                remote_nodes.iter().map(|(_, socket)| *socket).zip(pings),
                &recycler,
            )
            .unwrap();
        assert_eq!(remote_nodes.len(), packets.len());
        for (packet, (_, socket), pong) in izip!(
            packets.into_iter(),
            remote_nodes.into_iter(),
            pongs.into_iter()
        ) {
            assert_eq!(packet.meta().socket_addr(), socket);
            let bytes = serialize(&pong).unwrap();
            match packet.deserialize_slice(..).unwrap() {
                Protocol::PongMessage(pong) => assert_eq!(serialize(&pong).unwrap(), bytes),
                _ => panic!("invalid packet!"),
            }
        }
    }

    fn test_crds_values(pubkey: Pubkey) -> Vec<CrdsValue> {
        let entrypoint = ContactInfo::new_localhost(&pubkey, timestamp());
        let entrypoint_crdsvalue = CrdsValue::new_unsigned(CrdsData::from(entrypoint));
        vec![entrypoint_crdsvalue]
    }

    #[test]
    fn test_cluster_spy_gossip() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        //check that gossip doesn't try to push to invalid addresses
        let (spy, _, _) = ClusterInfo::spy_node(solana_pubkey::new_rand(), 0);
        let cluster_info = Arc::new({
            let keypair = Arc::new(Keypair::new());
            let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
            ClusterInfo::new(node.info, keypair, SocketAddrSpace::Unspecified)
        });
        cluster_info.insert_info(spy);
        cluster_info.gossip.refresh_push_active_set(
            &cluster_info.keypair(),
            cluster_info.my_shred_version(),
            &HashMap::new(), // stakes
            None,            // gossip validators
            &cluster_info.ping_cache,
            &mut Vec::new(), // pings
            &SocketAddrSpace::Unspecified,
        );
        let reqs = cluster_info.generate_new_gossip_requests(
            &thread_pool,
            None,            // gossip_validators
            &HashMap::new(), // stakes
            true,            // generate_pull_requests
        );
        //assert none of the addrs are invalid.
        reqs.iter().all(|(addr, _)| {
            let res = ContactInfo::is_valid_address(addr, &SocketAddrSpace::Unspecified);
            assert!(res);
            res
        });
    }

    #[test]
    fn test_cluster_info_new() {
        let keypair = Arc::new(Keypair::new());
        let d = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(d.clone(), keypair, SocketAddrSpace::Unspecified);
        assert_eq!(d.pubkey(), &cluster_info.id());
    }

    #[test]
    fn insert_info_test() {
        let keypair = Arc::new(Keypair::new());
        let d = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(d, keypair, SocketAddrSpace::Unspecified);
        let d = ContactInfo::new_localhost(&solana_pubkey::new_rand(), timestamp());
        let label = CrdsValueLabel::ContactInfo(*d.pubkey());
        cluster_info.insert_info(d);
        let gossip_crds = cluster_info.gossip.crds.read().unwrap();
        assert!(gossip_crds.get::<&CrdsValue>(&label).is_some());
    }

    fn assert_in_range(x: u16, range: (u16, u16)) {
        assert!(x >= range.0);
        assert!(x < range.1);
    }

    fn check_sockets(sockets: &[UdpSocket], ip: IpAddr, range: (u16, u16)) {
        assert!(!sockets.is_empty());
        let port = sockets[0].local_addr().unwrap().port();
        for socket in sockets.iter() {
            check_socket(socket, ip, range);
            assert_eq!(socket.local_addr().unwrap().port(), port);
        }
    }

    fn check_socket(socket: &UdpSocket, ip: IpAddr, range: (u16, u16)) {
        let local_addr = socket.local_addr().unwrap();
        assert_eq!(local_addr.ip(), ip);
        assert_in_range(local_addr.port(), range);
    }

    fn check_node_sockets(node: &Node, ip: IpAddr, range: (u16, u16)) {
        check_socket(&node.sockets.gossip, ip, range);
        check_socket(&node.sockets.repair, ip, range);
        check_socket(&node.sockets.tvu_quic, ip, range);

        check_sockets(&node.sockets.tvu, ip, range);
        check_sockets(&node.sockets.tpu, ip, range);
    }

    #[test]
    fn new_with_external_ip_test_random() {
        let ip = Ipv4Addr::LOCALHOST;
        let config = NodeConfig {
            gossip_addr: socketaddr!(ip, 0),
            port_range: VALIDATOR_PORT_RANGE,
            bind_ip_addr: IpAddr::V4(ip),
            public_tpu_addr: None,
            public_tpu_forwards_addr: None,
            num_tvu_sockets: MINIMUM_NUM_TVU_SOCKETS,
            num_quic_endpoints: DEFAULT_NUM_QUIC_ENDPOINTS,
        };

        let node = Node::new_with_external_ip(&solana_pubkey::new_rand(), config);

        check_node_sockets(&node, IpAddr::V4(ip), VALIDATOR_PORT_RANGE);
    }

    #[test]
    fn new_with_external_ip_test_gossip() {
        // Can't use VALIDATOR_PORT_RANGE because if this test runs in parallel with others, the
        // port returned by `bind_in_range()` might be snatched up before `Node::new_with_external_ip()` runs
        let (start, end) = VALIDATOR_PORT_RANGE;
        let port_range = (end, end + (end - start));
        let ip = IpAddr::V4(Ipv4Addr::LOCALHOST);
        let port = bind_in_range(ip, port_range).expect("Failed to bind").0;
        let config = NodeConfig {
            gossip_addr: socketaddr!(Ipv4Addr::LOCALHOST, port),
            port_range,
            bind_ip_addr: ip,
            public_tpu_addr: None,
            public_tpu_forwards_addr: None,
            num_tvu_sockets: MINIMUM_NUM_TVU_SOCKETS,
            num_quic_endpoints: DEFAULT_NUM_QUIC_ENDPOINTS,
        };

        let node = Node::new_with_external_ip(&solana_pubkey::new_rand(), config);

        check_node_sockets(&node, ip, port_range);

        assert_eq!(node.sockets.gossip.local_addr().unwrap().port(), port);
    }

    //test that all cluster_info objects only generate signed messages
    //when constructed with keypairs
    #[test]
    fn test_gossip_signature_verification() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        //create new cluster info, leader, and peer
        let keypair = Keypair::new();
        let peer_keypair = Keypair::new();
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let peer = ContactInfo::new_localhost(&peer_keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(
            contact_info,
            Arc::new(keypair),
            SocketAddrSpace::Unspecified,
        );
        let stakes = HashMap::<Pubkey, u64>::default();
        cluster_info.ping_cache.lock().unwrap().mock_pong(
            *peer.pubkey(),
            peer.gossip().unwrap(),
            Instant::now(),
        );
        cluster_info.insert_info(peer);
        cluster_info.gossip.refresh_push_active_set(
            &cluster_info.keypair(),
            cluster_info.my_shred_version(),
            &stakes,
            None, // gossip validators
            &cluster_info.ping_cache,
            &mut Vec::new(), // pings
            &SocketAddrSpace::Unspecified,
        );
        //check that all types of gossip messages are signed correctly
        cluster_info.flush_push_queue();
        let (entries, push_messages, _) = cluster_info.gossip.new_push_messages(
            &cluster_info.id(),
            timestamp(),
            &stakes,
            |_| true, // should_retain_crds_value
        );
        let push_messages = push_messages
            .into_iter()
            .map(|(pubkey, indices)| {
                let values = indices.into_iter().map(|k| entries[k].clone()).collect();
                (pubkey, values)
            })
            .collect::<HashMap<_, Vec<_>>>();
        // there should be some pushes ready
        assert!(!push_messages.is_empty());
        push_messages
            .values()
            .for_each(|v| v.par_iter().for_each(|v| assert!(v.verify())));

        let mut pings = Vec::new();
        cluster_info
            .gossip
            .new_pull_request(
                &thread_pool,
                cluster_info.keypair().deref(),
                cluster_info.my_shred_version(),
                timestamp(),
                None,
                &HashMap::new(),
                992, // max_bloom_filter_bytes
                &cluster_info.ping_cache,
                &mut pings,
                &cluster_info.socket_addr_space,
            )
            .ok()
            .unwrap();
    }

    #[test]
    fn test_refresh_vote_eviction() {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);

        // Push MAX_LOCKOUT_HISTORY votes into gossip, one for each slot between
        // [lowest_vote_slot, lowest_vote_slot + MAX_LOCKOUT_HISTORY)
        let lowest_vote_slot = 1;
        let max_vote_slot = lowest_vote_slot + MAX_LOCKOUT_HISTORY as Slot;
        let mut first_vote = None;
        let mut prev_votes = vec![];
        for slot in 1..max_vote_slot {
            prev_votes.push(slot);
            let unrefresh_vote = Vote::new(vec![slot], Hash::new_unique());
            let vote_ix = vote_instruction::vote(
                &Pubkey::new_unique(), // vote_pubkey
                &Pubkey::new_unique(), // authorized_voter_pubkey
                unrefresh_vote,
            );
            let vote_tx = Transaction::new_with_payer(
                &[vote_ix], // instructions
                None,       // payer
            );
            if first_vote.is_none() {
                first_vote = Some(vote_tx.clone());
            }
            cluster_info.push_vote(&prev_votes, vote_tx);
        }

        let initial_votes = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(initial_votes.len(), MAX_LOCKOUT_HISTORY);

        // Trying to refresh a vote less than all votes in gossip should do nothing
        let refresh_slot = lowest_vote_slot - 1;
        let refresh_vote = Vote::new(vec![refresh_slot], Hash::new_unique());
        let refresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            refresh_vote.clone(),
        );
        let refresh_tx = Transaction::new_with_payer(
            &[refresh_ix], // instructions
            None,          // payer
        );
        cluster_info.refresh_vote(refresh_tx.clone(), refresh_slot);
        let current_votes = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(initial_votes, current_votes);
        assert!(!current_votes.contains(&refresh_tx));

        // Trying to refresh a vote should evict the first slot less than the refreshed vote slot
        let refresh_slot = max_vote_slot + 1;
        let refresh_vote = Vote::new(vec![refresh_slot], Hash::new_unique());
        let refresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            refresh_vote.clone(),
        );
        let refresh_tx = Transaction::new_with_payer(
            &[refresh_ix], // instructions
            None,          // payer
        );
        cluster_info.refresh_vote(refresh_tx.clone(), refresh_slot);

        // This should evict the latest vote since it's for a slot less than refresh_slot
        let votes = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(votes.len(), MAX_LOCKOUT_HISTORY);
        assert!(votes.contains(&refresh_tx));
        assert!(!votes.contains(&first_vote.unwrap()));
    }

    #[test]
    fn test_refresh_vote() {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);

        // Construct and push a vote for some other slot
        let unrefresh_slot = 5;
        let unrefresh_tower = vec![1, 3, unrefresh_slot];
        let unrefresh_vote = Vote::new(unrefresh_tower.clone(), Hash::new_unique());
        let unrefresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            unrefresh_vote,
        );
        let unrefresh_tx = Transaction::new_with_payer(
            &[unrefresh_ix], // instructions
            None,            // payer
        );
        cluster_info.push_vote(&unrefresh_tower, unrefresh_tx.clone());
        let mut cursor = Cursor::default();
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![unrefresh_tx.clone()]);

        // Now construct vote for the slot to be refreshed later.
        let refresh_slot = unrefresh_slot + 1;
        let refresh_tower = vec![1, 3, unrefresh_slot, refresh_slot];
        let refresh_vote = Vote::new(refresh_tower.clone(), Hash::new_unique());
        let refresh_ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            refresh_vote.clone(),
        );
        let refresh_tx = Transaction::new_with_payer(
            &[refresh_ix], // instructions
            None,          // payer
        );

        // Trying to refresh vote when it doesn't yet exist in gossip
        // should add the vote without eviction if there is room in the gossip table.
        cluster_info.refresh_vote(refresh_tx.clone(), refresh_slot);

        // Should be two votes in gossip
        cursor = Cursor::default();
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 2);
        assert!(votes.contains(&unrefresh_tx));
        assert!(votes.contains(&refresh_tx));

        // Refresh a few times, we should only have the latest update
        let mut latest_refresh_tx = refresh_tx;
        for _ in 0..10 {
            let latest_refreshed_recent_blockhash = Hash::new_unique();
            let new_signer = Keypair::new();
            let refresh_ix = vote_instruction::vote(
                &new_signer.pubkey(), // vote_pubkey
                &new_signer.pubkey(), // authorized_voter_pubkey
                refresh_vote.clone(),
            );
            latest_refresh_tx = Transaction::new_signed_with_payer(
                &[refresh_ix],
                None,
                &[&new_signer],
                latest_refreshed_recent_blockhash,
            );
            cluster_info.refresh_vote(latest_refresh_tx.clone(), refresh_slot);
            // Sleep to avoid votes with same timestamp causing later vote to not override prior vote
            std::thread::sleep(Duration::from_millis(1));
        }
        // The diff since `max_ts` should only be the latest refreshed vote
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes.len(), 1);
        assert_eq!(votes[0], latest_refresh_tx);

        // Should still be two votes in gossip
        let votes = cluster_info.get_votes(&mut Cursor::default());
        assert_eq!(votes.len(), 2);
        assert!(votes.contains(&unrefresh_tx));
        assert!(votes.contains(&latest_refresh_tx));
    }

    #[test]
    fn test_push_vote() {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info =
            ClusterInfo::new(contact_info, keypair.clone(), SocketAddrSpace::Unspecified);

        // make sure empty crds is handled correctly
        let mut cursor = Cursor::default();
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![]);

        // add a vote
        let vote = Vote::new(
            vec![1, 3, 7], // slots
            Hash::new_unique(),
        );
        let ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            vote,
        );
        let tx = Transaction::new_with_payer(
            &[ix], // instructions
            None,  // payer
        );
        let tower = vec![7]; // Last slot in the vote.
        cluster_info.push_vote(&tower, tx.clone());

        let (labels, votes) = cluster_info.get_votes_with_labels(&mut cursor);
        assert_eq!(votes, vec![tx]);
        assert_eq!(labels.len(), 1);
        match labels[0] {
            CrdsValueLabel::Vote(_, pubkey) => {
                assert_eq!(pubkey, keypair.pubkey());
            }

            _ => panic!("Bad match"),
        }
        // make sure timestamp filter works
        let votes = cluster_info.get_votes(&mut cursor);
        assert_eq!(votes, vec![]);
    }

    fn new_vote_transaction(slots: Vec<Slot>) -> Transaction {
        let vote = Vote::new(slots, Hash::new_unique());
        let ix = vote_instruction::vote(
            &Pubkey::new_unique(), // vote_pubkey
            &Pubkey::new_unique(), // authorized_voter_pubkey
            vote,
        );
        Transaction::new_with_payer(
            &[ix], // instructions
            None,  // payer
        )
    }

    #[test]
    fn test_push_votes_with_tower() {
        let get_vote_slots = |cluster_info: &ClusterInfo| -> Vec<Slot> {
            let (labels, _) = cluster_info.get_votes_with_labels(&mut Cursor::default());
            let gossip_crds = cluster_info.gossip.crds.read().unwrap();
            let mut vote_slots = HashSet::new();
            for label in labels {
                let CrdsData::Vote(_, vote) = &gossip_crds.get::<&CrdsData>(&label).unwrap() else {
                    panic!("this should not happen!");
                };
                assert!(vote_slots.insert(vote.slot().unwrap()));
            }
            vote_slots.into_iter().sorted().collect()
        };
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);
        let mut tower = Vec::new();

        // Evict the oldest vote
        for k in 0..2 * MAX_LOCKOUT_HISTORY {
            let slot = k as Slot;
            tower.push(slot);
            let vote = new_vote_transaction(vec![slot]);
            cluster_info.push_vote(&tower, vote);

            let vote_slots = get_vote_slots(&cluster_info);
            let min_vote = k.saturating_sub(MAX_LOCKOUT_HISTORY - 1) as u64;
            assert!(vote_slots.into_iter().eq(min_vote..=(k as u64)));
            sleep(Duration::from_millis(1));
        }

        // Attempting to push an older vote will panic
        let slot = 30;
        tower.clear();
        tower.extend(0..=slot);
        let vote = new_vote_transaction(vec![slot]);
        assert!(panic::catch_unwind(|| cluster_info.push_vote(&tower, vote))
            .err()
            .and_then(|a| a
                .downcast_ref::<String>()
                .map(|s| { s.starts_with("Submitting old vote") }))
            .unwrap_or_default());
    }

    #[test]
    fn test_push_epoch_slots() {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert!(slots.is_empty());
        cluster_info.push_epoch_slots(&[0]);

        let mut cursor = Cursor::default();
        let slots = cluster_info.get_epoch_slots(&mut cursor);
        assert_eq!(slots.len(), 1);

        let slots = cluster_info.get_epoch_slots(&mut cursor);
        assert!(slots.is_empty());

        // Test with different shred versions.
        let mut rng = rand::thread_rng();
        let node_pubkey = Pubkey::new_unique();
        let mut node = ContactInfo::new_rand(&mut rng, Some(node_pubkey));
        node.set_shred_version(42);
        let epoch_slots = EpochSlots::new_rand(&mut rng, Some(node_pubkey));
        let entries = vec![
            CrdsValue::new_unsigned(CrdsData::from(node)),
            CrdsValue::new_unsigned(CrdsData::EpochSlots(0, epoch_slots)),
        ];
        {
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            for entry in entries {
                assert!(gossip_crds
                    .insert(entry, /*now=*/ 0, GossipRoute::LocalMessage)
                    .is_ok());
            }
        }
        // Should exclude other node's epoch-slot because of different
        // shred-version.
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].from, cluster_info.id());
        // Match shred versions.
        {
            let mut node = cluster_info.my_contact_info.write().unwrap();
            node.set_shred_version(42);
        }
        cluster_info.refresh_my_gossip_contact_info();
        cluster_info.flush_push_queue();
        // Should now include both epoch slots.
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].from, cluster_info.id());
        assert_eq!(slots[1].from, node_pubkey);
    }

    #[test]
    fn test_append_entrypoint_to_pulls() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        let entrypoint_pubkey = solana_pubkey::new_rand();
        let entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        cluster_info.set_entrypoint(entrypoint.clone());
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &HashMap::new());
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        for (addr, msg) in pulls {
            assert_eq!(addr, entrypoint.gossip().unwrap());
            match msg {
                Protocol::PullRequest(_, value) => {
                    assert!(value.verify());
                    assert_eq!(value.pubkey(), cluster_info.id())
                }
                _ => panic!("wrong protocol"),
            }
        }
        // now add this message back to the table and make sure after the next pull, the entrypoint is unset
        let entrypoint_crdsvalue = CrdsValue::new_unsigned(CrdsData::from(&entrypoint));
        let cluster_info = Arc::new(cluster_info);
        let stakes = HashMap::from([(Pubkey::new_unique(), 1u64)]);
        let timeouts = cluster_info.gossip.make_timeouts(
            cluster_info.id(),
            &stakes,
            Duration::from_millis(cluster_info.gossip.pull.crds_timeout),
        );
        cluster_info.handle_pull_response(vec![entrypoint_crdsvalue], &timeouts);
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &HashMap::new());
        assert_eq!(pings.len(), 1);
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert_eq!(*cluster_info.entrypoints.read().unwrap(), vec![entrypoint]);
    }

    #[test]
    fn test_tvu_peers_and_stakes() {
        let keypair = Arc::new(Keypair::new());
        let d = ContactInfo::new_localhost(&keypair.pubkey(), timestamp());
        let cluster_info = ClusterInfo::new(d.clone(), keypair, SocketAddrSpace::Unspecified);
        let mut stakes = HashMap::new();

        // no stake
        let id = Pubkey::from([1u8; 32]);
        let contact_info = ContactInfo::new_localhost(&id, timestamp());
        cluster_info.insert_info(contact_info);

        // normal
        let id2 = Pubkey::from([2u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id2, timestamp());
        cluster_info.insert_info(contact_info.clone());
        stakes.insert(id2, 10);

        // duplicate
        contact_info.set_wallclock(timestamp() + 1);
        cluster_info.insert_info(contact_info);

        // no tvu
        let id3 = Pubkey::from([3u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id3, timestamp());
        contact_info.remove_tvu();
        cluster_info.insert_info(contact_info);
        stakes.insert(id3, 10);

        // normal but with different shred version
        let id4 = Pubkey::from([4u8; 32]);
        let mut contact_info = ContactInfo::new_localhost(&id4, timestamp());
        contact_info.set_shred_version(1);
        assert_ne!(contact_info.shred_version(), d.shred_version());
        cluster_info.insert_info(contact_info);
        stakes.insert(id4, 10);
    }

    #[test]
    fn test_pull_from_entrypoint_if_not_present() {
        let thread_pool = ThreadPoolBuilder::new().build().unwrap();
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        let entrypoint_pubkey = solana_pubkey::new_rand();
        let mut entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        entrypoint
            .set_gossip(socketaddr!("127.0.0.2:1234"))
            .unwrap();
        cluster_info.set_entrypoint(entrypoint.clone());

        let mut stakes = HashMap::new();

        let other_node_pubkey = solana_pubkey::new_rand();
        let other_node = ContactInfo::new_localhost(&other_node_pubkey, timestamp());
        assert_ne!(other_node.gossip().unwrap(), entrypoint.gossip().unwrap());
        cluster_info.ping_cache.lock().unwrap().mock_pong(
            *other_node.pubkey(),
            other_node.gossip().unwrap(),
            Instant::now(),
        );
        cluster_info.insert_info(other_node.clone());
        stakes.insert(other_node_pubkey, 10);

        // Pull request 1:  `other_node` is present but `entrypoint` was just added (so it has a
        // fresh timestamp).  There should only be one pull request to `other_node`
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert!(pulls
            .into_iter()
            .all(|(addr, _)| addr == other_node.gossip().unwrap()));

        // Pull request 2: pretend it's been a while since we've pulled from `entrypoint`.  There should
        // now be two pull requests
        cluster_info.entrypoints.write().unwrap()[0].set_wallclock(0);
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), 2 * MIN_NUM_BLOOM_FILTERS);
        for node in [&other_node, &entrypoint] {
            assert_eq!(
                pulls
                    .iter()
                    .filter(|(addr, _)| *addr == node.gossip().unwrap())
                    .count(),
                MIN_NUM_BLOOM_FILTERS
            );
        }
        // Pull request 3:  `other_node` is present and `entrypoint` was just pulled from.  There should
        // only be one pull request to `other_node`
        let (pings, pulls) = cluster_info.new_pull_requests(&thread_pool, None, &stakes);
        assert!(pings.is_empty());
        assert_eq!(pulls.len(), MIN_NUM_BLOOM_FILTERS);
        assert!(pulls
            .into_iter()
            .all(|(addr, _)| addr == other_node.gossip().unwrap()));
    }

    #[test]
    fn test_repair_peers() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        for i in 0..10 {
            // make these invalid for the upcoming repair request
            let peer_lowest = if i >= 5 { 10 } else { 0 };
            let other_node_pubkey = solana_pubkey::new_rand();
            let other_node = ContactInfo::new_localhost(&other_node_pubkey, timestamp());
            cluster_info.insert_info(other_node.clone());
            let value = CrdsValue::new_unsigned(CrdsData::LowestSlot(
                0,
                LowestSlot::new(other_node_pubkey, peer_lowest, timestamp()),
            ));
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            let _ = gossip_crds.insert(value, timestamp(), GossipRoute::LocalMessage);
        }
        // only half the visible peers should be eligible to serve this repair
        assert_eq!(cluster_info.repair_peers(5).len(), 5);
    }

    #[test]
    fn test_push_epoch_slots_large() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        );
        //random should be hard to compress
        let mut rng = rand::thread_rng();
        let range: Vec<Slot> = repeat_with(|| rng.gen_range(1..32))
            .scan(0, |slot, step| {
                *slot += step;
                Some(*slot)
            })
            .take(32000)
            .collect();
        cluster_info.push_epoch_slots(&range[..16000]);
        cluster_info.push_epoch_slots(&range[16000..]);
        let slots = cluster_info.get_epoch_slots(&mut Cursor::default());
        let slots: Vec<_> = slots.iter().flat_map(|x| x.to_slots(0)).collect();
        assert_eq!(slots, range);
    }

    #[test]
    fn test_process_entrypoint_adopt_shred_version() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        ));
        assert_eq!(cluster_info.my_shred_version(), 0);

        // Simulating starting up with two entrypoints, no known id, only a gossip
        // address
        let entrypoint1_gossip_addr = socketaddr!("127.0.0.2:1234");
        let mut entrypoint1 = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint1.set_gossip(entrypoint1_gossip_addr).unwrap();
        assert_eq!(entrypoint1.shred_version(), 0);

        let entrypoint2_gossip_addr = socketaddr!("127.0.0.2:5678");
        let mut entrypoint2 = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint2.set_gossip(entrypoint2_gossip_addr).unwrap();
        assert_eq!(entrypoint2.shred_version(), 0);
        cluster_info.set_entrypoints(vec![entrypoint1, entrypoint2]);

        // Simulate getting entrypoint ContactInfo from gossip with an entrypoint1 shred version of
        // 0
        let mut gossiped_entrypoint1_info =
            ContactInfo::new_localhost(&solana_pubkey::new_rand(), timestamp());
        gossiped_entrypoint1_info
            .set_gossip(entrypoint1_gossip_addr)
            .unwrap();
        gossiped_entrypoint1_info.set_shred_version(0);
        cluster_info.insert_info(gossiped_entrypoint1_info.clone());
        assert!(!cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint1_info));

        // Adopt the entrypoint's gossiped contact info and verify
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 2);
        assert!(cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint1_info));

        assert!(!entrypoints_processed); // <--- entrypoint processing incomplete because shred adoption still pending
        assert_eq!(cluster_info.my_shred_version(), 0); // <-- shred version still 0

        // Simulate getting entrypoint ContactInfo from gossip with an entrypoint2 shred version of
        // !0
        let mut gossiped_entrypoint2_info =
            ContactInfo::new_localhost(&solana_pubkey::new_rand(), timestamp());
        gossiped_entrypoint2_info
            .set_gossip(entrypoint2_gossip_addr)
            .unwrap();
        gossiped_entrypoint2_info.set_shred_version(1);
        cluster_info.insert_info(gossiped_entrypoint2_info.clone());
        assert!(!cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint2_info));

        // Adopt the entrypoint's gossiped contact info and verify
        error!("Adopt the entrypoint's gossiped contact info and verify");
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 2);
        assert!(cluster_info
            .entrypoints
            .read()
            .unwrap()
            .iter()
            .any(|entrypoint| *entrypoint == gossiped_entrypoint2_info));

        assert!(entrypoints_processed);
        assert_eq!(cluster_info.my_shred_version(), 1); // <-- shred version now adopted from entrypoint2
    }

    #[test]
    fn test_process_entrypoint_without_adopt_shred_version() {
        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = Arc::new(ClusterInfo::new(
            {
                let mut contact_info =
                    ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp());
                contact_info.set_shred_version(2);
                contact_info
            },
            node_keypair,
            SocketAddrSpace::Unspecified,
        ));
        assert_eq!(cluster_info.my_shred_version(), 2);

        // Simulating starting up with default entrypoint, no known id, only a gossip
        // address
        let entrypoint_gossip_addr = socketaddr!("127.0.0.2:1234");
        let mut entrypoint = ContactInfo::new_localhost(&Pubkey::default(), timestamp());
        entrypoint.set_gossip(entrypoint_gossip_addr).unwrap();
        assert_eq!(entrypoint.shred_version(), 0);
        cluster_info.set_entrypoint(entrypoint);

        // Simulate getting entrypoint ContactInfo from gossip
        let mut gossiped_entrypoint_info =
            ContactInfo::new_localhost(&solana_pubkey::new_rand(), timestamp());
        gossiped_entrypoint_info
            .set_gossip(entrypoint_gossip_addr)
            .unwrap();
        gossiped_entrypoint_info.set_shred_version(1);
        cluster_info.insert_info(gossiped_entrypoint_info.clone());

        // Adopt the entrypoint's gossiped contact info and verify
        let entrypoints_processed = ClusterInfo::process_entrypoints(&cluster_info);
        assert_eq!(cluster_info.entrypoints.read().unwrap().len(), 1);
        assert_eq!(
            cluster_info.entrypoints.read().unwrap()[0],
            gossiped_entrypoint_info,
        );
        assert!(entrypoints_processed);
        assert_eq!(cluster_info.my_shred_version(), 2); // <--- No change to shred version
    }

    #[test]
    #[ignore] // TODO: debug why this is flaky on buildkite!
    fn test_pull_request_time_pruning() {
        let node = Node::new_localhost();
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info,
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));
        let entrypoint_pubkey = solana_pubkey::new_rand();
        let entrypoint = ContactInfo::new_localhost(&entrypoint_pubkey, timestamp());
        cluster_info.set_entrypoint(entrypoint);

        let mut rng = rand::thread_rng();
        let shred_version = cluster_info.my_shred_version();
        let mut peers: Vec<Pubkey> = vec![];

        const NO_ENTRIES: usize = CRDS_UNIQUE_PUBKEY_CAPACITY + 128;
        let data: Vec<_> = repeat_with(|| {
            let keypair = Keypair::new();
            peers.push(keypair.pubkey());
            let mut rand_ci = ContactInfo::new_rand(&mut rng, Some(keypair.pubkey()));
            rand_ci.set_shred_version(shred_version);
            rand_ci.set_wallclock(timestamp());
            CrdsValue::new(CrdsData::from(rand_ci), &keypair)
        })
        .take(NO_ENTRIES)
        .collect();
        let stakes = HashMap::from([(Pubkey::new_unique(), 1u64)]);
        let timeouts = CrdsTimeouts::new(
            cluster_info.id(),
            CRDS_GOSSIP_PULL_CRDS_TIMEOUT_MS * 4, // default_timeout
            Duration::from_secs(48 * 3600),       // epoch_duration
            &stakes,
        );
        assert_eq!(
            (0, 0, NO_ENTRIES),
            cluster_info.handle_pull_response(data, &timeouts)
        );
    }

    #[test]
    fn test_get_duplicate_shreds() {
        let host1_key = Arc::new(Keypair::new());
        let node = Node::new_localhost_with_pubkey(&host1_key.pubkey());
        let cluster_info = Arc::new(ClusterInfo::new(
            node.info,
            host1_key.clone(),
            SocketAddrSpace::Unspecified,
        ));
        let mut cursor = Cursor::default();
        assert!(cluster_info.get_duplicate_shreds(&mut cursor).is_empty());

        let mut rng = rand::thread_rng();
        let (slot, parent_slot, reference_tick, version) = (53084024, 53084023, 0, 0);
        let shredder = Shredder::new(slot, parent_slot, reference_tick, version).unwrap();
        let next_shred_index = 353;
        let leader = Arc::new(Keypair::new());
        let shred1 = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader);
        let shred2 = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader);
        assert!(cluster_info
            .push_duplicate_shred(&shred1, shred2.payload())
            .is_ok());
        cluster_info.flush_push_queue();
        let entries = cluster_info.get_duplicate_shreds(&mut cursor);
        // One duplicate shred proof is split into 3 chunks.
        assert_eq!(3, entries.len());
        for (i, shred_data) in entries.iter().enumerate() {
            assert_eq!(shred_data.from, host1_key.pubkey());
            assert_eq!(shred_data.slot, 53084024);
            assert_eq!(shred_data.chunk_index() as usize, i);
        }

        let slot = 53084025;
        let shredder = Shredder::new(slot, parent_slot, reference_tick, version).unwrap();
        let next_shred_index = 354;
        let shred3 = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader);
        let shred4 = new_rand_shred(&mut rng, next_shred_index, &shredder, &leader);
        assert!(cluster_info
            .push_duplicate_shred(&shred3, shred4.payload())
            .is_ok());
        cluster_info.flush_push_queue();
        let entries1 = cluster_info.get_duplicate_shreds(&mut cursor);
        // One duplicate shred proof is split into 3 chunks.
        assert_eq!(3, entries1.len());
        for (i, shred_data) in entries1.iter().enumerate() {
            assert_eq!(shred_data.from, host1_key.pubkey());
            assert_eq!(shred_data.slot, 53084025);
            assert_eq!(shred_data.chunk_index() as usize, i);
        }
    }

    #[test]
    fn test_push_restart_last_voted_fork_slots() {
        let keypair = Arc::new(Keypair::new());
        let contact_info = ContactInfo::new_localhost(&keypair.pubkey(), 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);
        let slots = cluster_info.get_restart_last_voted_fork_slots(&mut Cursor::default());
        assert!(slots.is_empty());
        let mut update: Vec<Slot> = vec![0];
        for i in 0..81 {
            for j in 0..1000 {
                update.push(i * 1050 + j);
            }
        }
        assert!(cluster_info
            .push_restart_last_voted_fork_slots(&update, Hash::default())
            .is_ok());
        cluster_info.flush_push_queue();

        let mut cursor = Cursor::default();
        let slots = cluster_info.get_restart_last_voted_fork_slots(&mut cursor);
        assert_eq!(slots.len(), 1);
        let retrieved_slots = slots[0].to_slots(0);
        assert!(retrieved_slots[0] < 69000);
        assert_eq!(retrieved_slots.last(), Some(84999).as_ref());

        let slots = cluster_info.get_restart_last_voted_fork_slots(&mut cursor);
        assert!(slots.is_empty());

        // Test with different shred versions.
        let mut rng = rand::thread_rng();
        let node_pubkey = Pubkey::new_unique();
        let mut node = ContactInfo::new_rand(&mut rng, Some(node_pubkey));
        node.set_shred_version(42);
        let mut slots = RestartLastVotedForkSlots::new_rand(&mut rng, Some(node_pubkey));
        slots.shred_version = 42;
        let entries = vec![
            CrdsValue::new_unsigned(CrdsData::from(node)),
            CrdsValue::new_unsigned(CrdsData::RestartLastVotedForkSlots(slots)),
        ];
        {
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            for entry in entries {
                assert!(gossip_crds
                    .insert(entry, /*now=*/ 0, GossipRoute::LocalMessage)
                    .is_ok());
            }
        }
        // Should exclude other node's last-voted-fork-slot because of different
        // shred-version.
        let slots = cluster_info.get_restart_last_voted_fork_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 1);
        assert_eq!(slots[0].from, cluster_info.id());

        // Match shred versions.
        {
            let mut node = cluster_info.my_contact_info.write().unwrap();
            node.set_shred_version(42);
        }
        assert!(cluster_info
            .push_restart_last_voted_fork_slots(&update, Hash::default())
            .is_ok());
        cluster_info.flush_push_queue();
        // Should now include both slots.
        let slots = cluster_info.get_restart_last_voted_fork_slots(&mut Cursor::default());
        assert_eq!(slots.len(), 2);
        assert_eq!(slots[0].from, node_pubkey);
        assert_eq!(slots[1].from, cluster_info.id());
    }

    #[test]
    fn test_push_restart_heaviest_fork() {
        solana_logger::setup();
        let keypair = Arc::new(Keypair::new());
        let pubkey = keypair.pubkey();
        let contact_info = ContactInfo::new_localhost(&pubkey, 0);
        let cluster_info = ClusterInfo::new(contact_info, keypair, SocketAddrSpace::Unspecified);

        // make sure empty crds is handled correctly
        let mut cursor = Cursor::default();
        let heaviest_forks = cluster_info.get_restart_heaviest_fork(&mut cursor);
        assert_eq!(heaviest_forks, vec![]);

        // add new message
        let slot1 = 53;
        let hash1 = Hash::new_unique();
        let stake1 = 15_000_000;
        cluster_info.push_restart_heaviest_fork(slot1, hash1, stake1);
        cluster_info.flush_push_queue();

        let heaviest_forks = cluster_info.get_restart_heaviest_fork(&mut cursor);
        assert_eq!(heaviest_forks.len(), 1);
        let fork = &heaviest_forks[0];
        assert_eq!(fork.last_slot, slot1);
        assert_eq!(fork.last_slot_hash, hash1);
        assert_eq!(fork.observed_stake, stake1);
        assert_eq!(fork.from, pubkey);

        // Test with different shred versions.
        let mut rng = rand::thread_rng();
        let pubkey2 = Pubkey::new_unique();
        let mut new_node = ContactInfo::new_rand(&mut rng, Some(pubkey2));
        new_node.set_shred_version(42);
        let slot2 = 54;
        let hash2 = Hash::new_unique();
        let stake2 = 23_000_000;
        let entries = vec![
            CrdsValue::new_unsigned(CrdsData::from(new_node)),
            CrdsValue::new_unsigned(CrdsData::RestartHeaviestFork(RestartHeaviestFork {
                from: pubkey2,
                wallclock: timestamp(),
                last_slot: slot2,
                last_slot_hash: hash2,
                observed_stake: stake2,
                shred_version: 42,
            })),
        ];
        {
            let mut gossip_crds = cluster_info.gossip.crds.write().unwrap();
            for entry in entries {
                assert!(gossip_crds
                    .insert(entry, /*now=*/ 0, GossipRoute::LocalMessage)
                    .is_ok());
            }
        }
        // Should exclude other node's heaviest_fork because of different
        // shred-version.
        let heaviest_forks = cluster_info.get_restart_heaviest_fork(&mut Cursor::default());
        assert_eq!(heaviest_forks.len(), 1);
        assert_eq!(heaviest_forks[0].from, pubkey);
        // Match shred versions.
        {
            let mut node = cluster_info.my_contact_info.write().unwrap();
            node.set_shred_version(42);
        }
        cluster_info.refresh_my_gossip_contact_info();
        cluster_info.flush_push_queue();

        // Should now include the previous heaviest_fork from the other node.
        let heaviest_forks = cluster_info.get_restart_heaviest_fork(&mut Cursor::default());
        assert_eq!(heaviest_forks.len(), 1);
        assert_eq!(heaviest_forks[0].from, pubkey2);
    }

    #[test]
    fn test_contact_trace() {
        solana_logger::setup();
        let keypair43 = Arc::new(
            Keypair::from_bytes(&[
                198, 203, 8, 178, 196, 71, 119, 152, 31, 96, 221, 142, 115, 224, 45, 34, 173, 138,
                254, 39, 181, 238, 168, 70, 183, 47, 210, 91, 221, 179, 237, 153, 14, 58, 154, 59,
                67, 220, 235, 106, 241, 99, 4, 72, 60, 245, 53, 30, 225, 122, 145, 225, 8, 40, 30,
                174, 26, 228, 125, 127, 125, 21, 96, 28,
            ])
            .unwrap(),
        );
        let keypair44 = Arc::new(
            Keypair::from_bytes(&[
                66, 88, 3, 70, 228, 215, 125, 64, 130, 183, 180, 98, 22, 166, 201, 234, 89, 80,
                135, 24, 228, 35, 20, 52, 105, 130, 50, 51, 46, 229, 244, 108, 70, 57, 45, 247, 57,
                177, 39, 126, 190, 238, 50, 96, 186, 208, 28, 168, 148, 56, 9, 106, 92, 213, 63,
                205, 252, 225, 244, 101, 77, 182, 4, 2,
            ])
            .unwrap(),
        );

        let cluster_info44 = Arc::new({
            let mut node = Node::new_localhost_with_pubkey(&keypair44.pubkey());
            node.sockets.gossip = bind_to(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                /*port*/ 65534,
                /*reuseport:*/ false,
            )
            .unwrap();
            info!("{:?}", node);
            ClusterInfo::new(node.info, keypair44.clone(), SocketAddrSpace::Unspecified)
        });
        let cluster_info43 = Arc::new({
            let node = Node::new_localhost_with_pubkey(&keypair43.pubkey());
            ClusterInfo::new(node.info, keypair43.clone(), SocketAddrSpace::Unspecified)
        });

        assert_eq!(keypair43.pubkey().to_string().len(), 43);
        assert_eq!(keypair44.pubkey().to_string().len(), 44);

        let trace = cluster_info44.contact_info_trace();
        info!("cluster:\n{}", trace);
        assert_eq!(trace.len(), 431);

        let trace = cluster_info44.rpc_info_trace();
        info!("rpc:\n{}", trace);
        assert_eq!(trace.len(), 335);

        let trace = cluster_info43.contact_info_trace();
        info!("cluster:\n{}", trace);
        assert_eq!(trace.len(), 431);

        let trace = cluster_info43.rpc_info_trace();
        info!("rpc:\n{}", trace);
        assert_eq!(trace.len(), 335);
    }
}
