use {
    agave_snapshots::{
        paths as snapshot_paths, snapshot_archive_info::SnapshotArchiveInfoGetter as _,
        ArchiveFormat,
    },
    itertools::Itertools,
    log::*,
    rand::{seq::SliceRandom, thread_rng},
    rayon::prelude::*,
    reqwest::Url,
    reqwest::blocking::Client as HttpClient,
    serde::Deserialize,
    serde_json::Value as JsonValue,
    solana_account::ReadableAccount,
    solana_clock::Slot,
    solana_commitment_config::CommitmentConfig,
    solana_core::validator::{ValidatorConfig, ValidatorStartProgress},
    solana_genesis_utils::download_then_check_genesis_hash,
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfo, Protocol},
        gossip_service::GossipService,
        node::Node,
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_metrics::datapoint_info,
    solana_pubkey::Pubkey,
    solana_rpc_client::rpc_client::RpcClient,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_vote_program::vote_state::VoteStateV4,
    std::{
        collections::{hash_map::{DefaultHasher, RandomState}, HashSet},
        fs,
        hash::{Hash as StdHash, Hasher},
        io::{Read, Seek, SeekFrom, Write},
        net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
        path::Path,
        process::exit,
        sync::{
            atomic::{AtomicBool, Ordering},
            mpsc,
            Arc, RwLock,
        },
        thread,
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[cfg(test)]
use std::collections::HashMap;

/// If we don't have any alternative peers after this long, better off trying
/// blacklisted peers again.
const BLACKLIST_CLEAR_THRESHOLD: Duration = Duration::from_secs(60);
/// If we haven't found any RPC peers after this time, just give up.
const GET_RPC_PEERS_TIMEOUT: Duration = Duration::from_secs(300);

pub const MAX_RPC_CONNECTIONS_EVALUATED_PER_ITERATION: usize = 32;

pub const PING_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, PartialEq, Clone)]
pub struct RpcBootstrapConfig {
    pub no_genesis_fetch: bool,
    pub no_snapshot_fetch: bool,
    pub only_known_rpc: bool,
    pub max_genesis_archive_unpacked_size: u64,
    pub check_vote_account: Option<String>,
    /// Optional explicit RPC nodes to use for bootstrap instead of gossip discovery.
    pub bootstrap_rpc_addrs: Vec<SocketAddr>,
    /// Optional URL that returns a JSON list of RPC socket addresses for bootstrap.
    pub bootstrap_rpc_addrs_url: Option<String>,
    pub incremental_snapshot_fetch: bool,
    pub snapshot_manifest_url: String,
    pub snapshot_download_concurrency: usize,
    pub snapshot_download_chunk_size_bytes: u64,
    pub snapshot_download_timeout_ms: u64,
    pub snapshot_download_max_retries: u32,
}

const BOOTSTRAP_RPC_ADDRS_URL_MAX_BYTES: usize = 1024 * 1024;

fn deterministic_pubkey_for_socket_addr(addr: &SocketAddr) -> Pubkey {
    let mut bytes = [0u8; 32];
    let addr_str = addr.to_string();
    for i in 0u8..4 {
        let mut hasher = DefaultHasher::new();
        i.hash(&mut hasher);
        addr_str.hash(&mut hasher);
        let h = hasher.finish();
        bytes[(i as usize) * 8..(i as usize + 1) * 8].copy_from_slice(&h.to_le_bytes());
    }
    Pubkey::new_from_array(bytes)
}

fn parse_socket_addrs_from_json(value: &JsonValue) -> Vec<SocketAddr> {
    fn walk(value: &JsonValue, out: &mut Vec<SocketAddr>) {
        match value {
            JsonValue::String(s) => {
                if let Ok(addr) = s.parse::<SocketAddr>() {
                    out.push(addr);
                }
            }
            JsonValue::Array(items) => {
                for item in items {
                    walk(item, out);
                }
            }
            JsonValue::Object(map) => {
                for (_, v) in map {
                    walk(v, out);
                }
            }
            _ => {}
        }
    }

    let mut out = Vec::new();
    match value {
        JsonValue::Array(_) => walk(value, &mut out),
        JsonValue::Object(map) => {
            for key in [
                "rpc_addrs",
                "bootstrap_rpc_addrs",
                "bootstrapRpcAddrs",
                "rpc_endpoints",
                "rpcEndpoints",
                "rpcs",
                "addrs",
                "data",
            ] {
                if let Some(v) = map.get(key) {
                    walk(v, &mut out);
                    break;
                }
            }
            if out.is_empty() {
                walk(value, &mut out);
            }
        }
        _ => {}
    }
    out.sort();
    out.dedup();
    out
}

fn fetch_bootstrap_rpc_addrs_from_url(
    client: &HttpClient,
    url: &Url,
) -> Result<Vec<SocketAddr>, String> {
    let resp = client
        .get(url.clone())
        .send()
        .and_then(|resp| resp.error_for_status())
        .map_err(|e| format!("failed to fetch bootstrap rpc addrs {url}: {e}"))?;

    let mut body = Vec::new();
    resp.take((BOOTSTRAP_RPC_ADDRS_URL_MAX_BYTES as u64).saturating_add(1))
        .read_to_end(&mut body)
        .map_err(|e| format!("failed to read bootstrap rpc addrs response: {e}"))?;
    if body.len() > BOOTSTRAP_RPC_ADDRS_URL_MAX_BYTES {
        return Err(format!(
            "bootstrap rpc addrs response exceeded {} bytes",
            BOOTSTRAP_RPC_ADDRS_URL_MAX_BYTES
        ));
    }

    let json: JsonValue = serde_json::from_slice(&body)
        .map_err(|e| format!("failed to decode bootstrap rpc addrs json: {e}"))?;
    Ok(parse_socket_addrs_from_json(&json))
}

fn bootstrap_rpc_peers_from_config(
    cluster_info: &ClusterInfo,
    validator_config: &ValidatorConfig,
    blacklisted_rpc_nodes: &HashSet<Pubkey>,
    bootstrap_config: &RpcBootstrapConfig,
) -> Option<Vec<ContactInfo>> {
    let mut addrs = bootstrap_config.bootstrap_rpc_addrs.clone();
    let url = bootstrap_config.bootstrap_rpc_addrs_url.as_deref().map(str::trim);
    if let Some(url) = url.filter(|s| !s.is_empty()) {
        match Url::parse(url) {
            Ok(url) => {
                let timeout =
                    Duration::from_millis(bootstrap_config.snapshot_download_timeout_ms.max(1000));
                let client = HttpClient::builder().timeout(timeout).build();
                match client
                    .as_ref()
                    .map_err(|e| format!("failed to build bootstrap rpc http client: {e}"))
                    .and_then(|client| fetch_bootstrap_rpc_addrs_from_url(client, &url))
                {
                    Ok(mut fetched) => addrs.append(&mut fetched),
                    Err(err) => {
                        warn!("bootstrap rpc addrs url fetch failed; falling back to gossip discovery: {err}");
                    }
                }
            }
            Err(err) => warn!(
                "invalid --bootstrap-rpc-addrs-url; falling back to gossip discovery: {err}"
            ),
        }
    }

    addrs.sort();
    addrs.dedup();
    addrs.truncate(1024);

    if addrs.is_empty() {
        return None;
    }

    let shred_version = validator_config
        .expected_shred_version
        .unwrap_or_else(|| cluster_info.my_shred_version());

    let wallclock = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    let mut peers = Vec::with_capacity(addrs.len());
    for addr in addrs {
        let pubkey = deterministic_pubkey_for_socket_addr(&addr);
        if blacklisted_rpc_nodes.contains(&pubkey) {
            continue;
        }
        let mut ci = ContactInfo::new(pubkey, wallclock, shred_version);
        if ci.set_rpc(addr).is_ok() {
            peers.push(ci);
        }
    }
    Some(peers)
}

fn verify_reachable_ports(
    node: &Node,
    cluster_entrypoint: &ContactInfo,
    validator_config: &ValidatorConfig,
    socket_addr_space: &SocketAddrSpace,
) -> bool {
    let verify_address = |addr: &Option<SocketAddr>| -> bool {
        addr.as_ref()
            .map(|addr| socket_addr_space.check(addr))
            .unwrap_or_default()
    };

    let mut udp_sockets = vec![&node.sockets.repair];
    udp_sockets.extend(node.sockets.gossip.iter());

    if verify_address(&node.info.serve_repair(Protocol::UDP)) {
        udp_sockets.push(&node.sockets.serve_repair);
    }
    if verify_address(&node.info.tpu(Protocol::UDP)) {
        udp_sockets.extend(node.sockets.tpu.iter());
        udp_sockets.extend(&node.sockets.tpu_quic);
    }
    if verify_address(&node.info.tpu_forwards(Protocol::UDP)) {
        udp_sockets.extend(node.sockets.tpu_forwards.iter());
        udp_sockets.extend(&node.sockets.tpu_forwards_quic);
    }
    if verify_address(&node.info.tpu_vote(Protocol::UDP)) {
        udp_sockets.extend(node.sockets.tpu_vote.iter());
    }
    if verify_address(&node.info.tvu(Protocol::UDP)) {
        udp_sockets.extend(node.sockets.tvu.iter());
        udp_sockets.extend(node.sockets.broadcast.iter());
        udp_sockets.extend(node.sockets.retransmit_sockets.iter());
    }
    if !solana_net_utils::verify_all_reachable_udp(
        &cluster_entrypoint.gossip().unwrap(),
        &udp_sockets,
    ) {
        return false;
    }

    let mut tcp_listeners = vec![];
    if let Some((rpc_addr, rpc_pubsub_addr)) = validator_config.rpc_addrs {
        for (purpose, bind_addr, public_addr) in &[
            ("RPC", rpc_addr, node.info.rpc()),
            ("RPC pubsub", rpc_pubsub_addr, node.info.rpc_pubsub()),
        ] {
            if verify_address(public_addr) {
                tcp_listeners.push(TcpListener::bind(bind_addr).unwrap_or_else(|err| {
                    error!("Unable to bind to tcp {bind_addr:?} for {purpose}: {err}");
                    exit(1);
                }));
            }
        }
    }

    if let Some(ip_echo) = &node.sockets.ip_echo {
        let ip_echo = ip_echo.try_clone().expect("unable to clone tcp_listener");
        tcp_listeners.push(ip_echo);
    }

    solana_net_utils::verify_all_reachable_tcp(&cluster_entrypoint.gossip().unwrap(), tcp_listeners)
}

fn is_known_validator(id: &Pubkey, known_validators: &Option<HashSet<Pubkey>>) -> bool {
    if let Some(known_validators) = known_validators {
        known_validators.contains(id)
    } else {
        false
    }
}

fn start_gossip_node(
    identity_keypair: Arc<Keypair>,
    cluster_entrypoints: &[ContactInfo],
    ledger_path: &Path,
    gossip_addr: &SocketAddr,
    gossip_sockets: Arc<[UdpSocket]>,
    expected_shred_version: u16,
    gossip_validators: Option<HashSet<Pubkey>>,
    should_check_duplicate_instance: bool,
    socket_addr_space: SocketAddrSpace,
) -> (Arc<ClusterInfo>, Arc<AtomicBool>, GossipService) {
    let contact_info = ClusterInfo::gossip_contact_info(
        identity_keypair.pubkey(),
        *gossip_addr,
        expected_shred_version,
    );
    let mut cluster_info = ClusterInfo::new(contact_info, identity_keypair, socket_addr_space);
    cluster_info.set_entrypoints(cluster_entrypoints.to_vec());
    cluster_info.restore_contact_info(ledger_path, 0);
    let cluster_info = Arc::new(cluster_info);

    let gossip_exit_flag = Arc::new(AtomicBool::new(false));
    let gossip_service = GossipService::new(
        &cluster_info,
        None,
        gossip_sockets,
        gossip_validators,
        should_check_duplicate_instance,
        None,
        gossip_exit_flag.clone(),
    );
    (cluster_info, gossip_exit_flag, gossip_service)
}

fn get_rpc_peers(
    cluster_info: &ClusterInfo,
    validator_config: &ValidatorConfig,
    blacklisted_rpc_nodes: &mut HashSet<Pubkey>,
    blacklist_timeout: &Instant,
    retry_reason: &mut Option<String>,
    bootstrap_config: &RpcBootstrapConfig,
) -> Vec<ContactInfo> {
    let shred_version = validator_config
        .expected_shred_version
        .unwrap_or_else(|| cluster_info.my_shred_version());

    info!(
        "Searching for an RPC service with shred version {shred_version}{}...",
        retry_reason
            .as_ref()
            .map(|s| format!(" (Retrying: {s})"))
            .unwrap_or_default()
    );

    let mut rpc_peers = cluster_info.rpc_peers();
    if bootstrap_config.only_known_rpc {
        rpc_peers.retain(|rpc_peer| {
            is_known_validator(rpc_peer.pubkey(), &validator_config.known_validators)
        });
    }

    let rpc_peers_total = rpc_peers.len();

    // Filter out blacklisted nodes
    let rpc_peers: Vec<_> = rpc_peers
        .into_iter()
        .filter(|rpc_peer| !blacklisted_rpc_nodes.contains(rpc_peer.pubkey()))
        .collect();
    let rpc_peers_blacklisted = rpc_peers_total - rpc_peers.len();
    let rpc_known_peers = rpc_peers
        .iter()
        .filter(|rpc_peer| {
            is_known_validator(rpc_peer.pubkey(), &validator_config.known_validators)
        })
        .count();

    info!(
        "Total {rpc_peers_total} RPC nodes found. {rpc_known_peers} known, \
         {rpc_peers_blacklisted} blacklisted"
    );

    if rpc_peers_blacklisted == rpc_peers_total {
        *retry_reason = if !blacklisted_rpc_nodes.is_empty()
            && blacklist_timeout.elapsed() > BLACKLIST_CLEAR_THRESHOLD
        {
            // All nodes are blacklisted and no additional nodes recently discovered.
            // Remove all nodes from the blacklist and try them again.
            blacklisted_rpc_nodes.clear();
            Some("Blacklist timeout expired".to_owned())
        } else {
            Some("Wait for known rpc peers".to_owned())
        };
        return vec![];
    }
    rpc_peers
}

fn check_vote_account(
    rpc_client: &RpcClient,
    identity_pubkey: &Pubkey,
    vote_account_address: &Pubkey,
    authorized_voter_pubkeys: &[Pubkey],
) -> Result<(), String> {
    let vote_account = rpc_client
        .get_account_with_commitment(vote_account_address, CommitmentConfig::confirmed())
        .map_err(|err| format!("failed to fetch vote account: {err}"))?
        .value
        .ok_or_else(|| format!("vote account does not exist: {vote_account_address}"))?;

    if vote_account.owner != solana_vote_program::id() {
        return Err(format!(
            "not a vote account (owned by {}): {}",
            vote_account.owner, vote_account_address
        ));
    }

    let identity_account = rpc_client
        .get_account_with_commitment(identity_pubkey, CommitmentConfig::confirmed())
        .map_err(|err| format!("failed to fetch identity account: {err}"))?
        .value
        .ok_or_else(|| format!("identity account does not exist: {identity_pubkey}"))?;

    let vote_state = VoteStateV4::deserialize(vote_account.data(), vote_account_address).ok();
    if let Some(vote_state) = vote_state {
        if vote_state.authorized_voters.is_empty() {
            return Err("Vote account not yet initialized".to_string());
        }

        if vote_state.node_pubkey != *identity_pubkey {
            return Err(format!(
                "vote account's identity ({}) does not match the validator's identity {}).",
                vote_state.node_pubkey, identity_pubkey
            ));
        }

        for (_, vote_account_authorized_voter_pubkey) in vote_state.authorized_voters.iter() {
            if !authorized_voter_pubkeys.contains(vote_account_authorized_voter_pubkey) {
                return Err(format!(
                    "authorized voter {vote_account_authorized_voter_pubkey} not available"
                ));
            }
        }
    } else {
        return Err(format!(
            "invalid vote account data for {vote_account_address}"
        ));
    }

    // Maybe we can calculate minimum voting fee; rather than 1 lamport
    if identity_account.lamports <= 1 {
        return Err(format!(
            "underfunded identity account ({}): only {} lamports available",
            identity_pubkey, identity_account.lamports
        ));
    }

    Ok(())
}

#[derive(Error, Debug)]
pub enum GetRpcNodeError {
    #[error("Unable to find any RPC peers")]
    NoRpcPeersFound,

    #[error("Giving up, did not get newer snapshots from the cluster")]
    NoNewerSnapshots,

    #[error("Snapshot manifest error: {0}")]
    SnapshotManifestError(String),
}

/// Struct to wrap the return value from get_rpc_nodes().  The `rpc_contact_info` is the peer to
/// download from, and `snapshot_hash` is the (optional) full and (optional) incremental
/// snapshots to download.
#[derive(Debug)]
struct GetRpcNodeResult {
    rpc_contact_info: ContactInfo,
    snapshot_hash: Option<SnapshotHash>,
}

/// Struct to wrap the peers & snapshot hashes together.
#[derive(Debug, PartialEq, Eq, Clone)]
#[cfg(test)]
struct PeerSnapshotHash {
    rpc_contact_info: ContactInfo,
    snapshot_hash: SnapshotHash,
}

/// A snapshot hash.  In this context (bootstrap *with* incremental snapshots), a snapshot hash
/// is _both_ a full snapshot hash and an (optional) incremental snapshot hash.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Copy)]
pub struct SnapshotHash {
    full: (Slot, Hash),
    incr: Option<(Slot, Hash)>,
}

pub fn fail_rpc_node(
    err: String,
    known_validators: &Option<HashSet<Pubkey, RandomState>>,
    rpc_id: &Pubkey,
    blacklisted_rpc_nodes: &mut HashSet<Pubkey, RandomState>,
) {
    let is_known = known_validators
        .as_ref()
        .is_some_and(|known_validators| known_validators.contains(rpc_id));
    if is_known {
        warn!("{err}");
        return;
    }

    debug!("{err}");
    debug!("Excluding {rpc_id} as a future RPC candidate");
    blacklisted_rpc_nodes.insert(*rpc_id);
}

fn shutdown_gossip_service(gossip: (Arc<ClusterInfo>, Arc<AtomicBool>, GossipService)) {
    let (cluster_info, gossip_exit_flag, gossip_service) = gossip;
    cluster_info.save_contact_info();
    gossip_exit_flag.store(true, Ordering::Relaxed);
    gossip_service.join().unwrap();
}

#[allow(clippy::too_many_arguments)]
pub fn attempt_download_genesis_and_snapshot(
    rpc_contact_info: &ContactInfo,
    ledger_path: &Path,
    validator_config: &mut ValidatorConfig,
    bootstrap_config: &RpcBootstrapConfig,
    use_progress_bar: bool,
    gossip: &mut Option<(Arc<ClusterInfo>, Arc<AtomicBool>, GossipService)>,
    rpc_client: &RpcClient,
    maximum_local_snapshot_age: Slot,
    start_progress: &Arc<RwLock<ValidatorStartProgress>>,
    minimal_snapshot_download_speed: f32,
    maximum_snapshot_download_abort: u64,
    download_abort_count: &mut u64,
    snapshot_hash: Option<SnapshotHash>,
    identity_keypair: &Arc<Keypair>,
    vote_account: &Pubkey,
    authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
) -> Result<(), String> {
    download_then_check_genesis_hash(
        &rpc_contact_info
            .rpc()
            .ok_or_else(|| String::from("Invalid RPC address"))?,
        ledger_path,
        &mut validator_config.expected_genesis_hash,
        bootstrap_config.max_genesis_archive_unpacked_size,
        bootstrap_config.no_genesis_fetch,
        use_progress_bar,
        rpc_client,
    )?;

    if let Some(gossip) = gossip.take() {
        shutdown_gossip_service(gossip);
    }

    let rpc_client_slot = rpc_client
        .get_slot_with_commitment(CommitmentConfig::finalized())
        .map_err(|err| format!("Failed to get RPC node slot: {err}"))?;
    info!("RPC node root slot: {rpc_client_slot}");

    download_snapshots(
        validator_config,
        bootstrap_config,
        use_progress_bar,
        maximum_local_snapshot_age,
        start_progress,
        minimal_snapshot_download_speed,
        maximum_snapshot_download_abort,
        download_abort_count,
        snapshot_hash,
        rpc_contact_info,
    )?;

    if let Some(url) = bootstrap_config.check_vote_account.as_ref() {
        let rpc_client = RpcClient::new(url);
        check_vote_account(
            &rpc_client,
            &identity_keypair.pubkey(),
            vote_account,
            &authorized_voter_keypairs
                .read()
                .unwrap()
                .iter()
                .map(|k| k.pubkey())
                .collect::<Vec<_>>(),
        )
        .unwrap_or_else(|err| {
            // Consider failures here to be more likely due to user error (eg,
            // incorrect `agave-validator` command-line arguments) rather than the
            // RPC node failing.
            //
            // Power users can always use the `--no-check-vote-account` option to
            // bypass this check entirely
            error!("{err}");
            exit(1);
        });
    }
    Ok(())
}

/// simple ping helper function which returns the time to connect
fn ping(addr: &SocketAddr) -> Option<Duration> {
    let start = Instant::now();
    match TcpStream::connect_timeout(addr, PING_TIMEOUT) {
        Ok(_) => Some(start.elapsed()),
        Err(_) => None,
    }
}

// Populates `vetted_rpc_nodes` with a list of RPC nodes that are ready to be
// used for downloading latest snapshots and/or the genesis block. Guaranteed to
// find at least one viable node or terminate the process.
fn get_vetted_rpc_nodes(
    vetted_rpc_nodes: &mut Vec<(ContactInfo, Option<SnapshotHash>, RpcClient)>,
    cluster_info: &Arc<ClusterInfo>,
    validator_config: &ValidatorConfig,
    blacklisted_rpc_nodes: &mut HashSet<Pubkey>,
    bootstrap_config: &RpcBootstrapConfig,
) {
    while vetted_rpc_nodes.is_empty() {
        let rpc_node_details = match get_rpc_nodes(
            cluster_info,
            validator_config,
            blacklisted_rpc_nodes,
            bootstrap_config,
        ) {
            Ok(rpc_node_details) => rpc_node_details,
            Err(err) => {
                error!(
                    "Failed to get RPC nodes: {err}. Consider checking system clock, removing \
                     `--no-port-check`, or adjusting `--known-validator ...` arguments as \
                     applicable"
                );
                exit(1);
            }
        };

        let newly_blacklisted_rpc_nodes = RwLock::new(HashSet::new());
        vetted_rpc_nodes.extend(
            rpc_node_details
                .into_par_iter()
                .filter_map(|rpc_node_details| {
                    let GetRpcNodeResult {
                        rpc_contact_info,
                        snapshot_hash,
                    } = rpc_node_details;

                    info!(
                        "Using RPC service from node {}: {:?}",
                        rpc_contact_info.pubkey(),
                        rpc_contact_info.rpc()
                    );

                    let rpc_addr = rpc_contact_info.rpc()?;
                    let ping_time = ping(&rpc_addr);

                    let rpc_client =
                        RpcClient::new_socket_with_timeout(rpc_addr, Duration::from_secs(5));

                    Some((rpc_contact_info, snapshot_hash, rpc_client, ping_time))
                })
                .filter(
                    |(rpc_contact_info, _snapshot_hash, rpc_client, ping_time)| match rpc_client
                        .get_version()
                    {
                        Ok(rpc_version) => {
                            if let Some(ping_time) = ping_time {
                                info!(
                                    "RPC node version: {} Ping: {}ms",
                                    rpc_version.solana_core,
                                    ping_time.as_millis()
                                );
                                true
                            } else {
                                fail_rpc_node(
                                    "Failed to ping RPC".to_string(),
                                    &validator_config.known_validators,
                                    rpc_contact_info.pubkey(),
                                    &mut newly_blacklisted_rpc_nodes.write().unwrap(),
                                );
                                false
                            }
                        }
                        Err(err) => {
                            fail_rpc_node(
                                format!("Failed to get RPC node version: {err}"),
                                &validator_config.known_validators,
                                rpc_contact_info.pubkey(),
                                &mut newly_blacklisted_rpc_nodes.write().unwrap(),
                            );
                            false
                        }
                    },
                )
                .collect::<Vec<(
                    ContactInfo,
                    Option<SnapshotHash>,
                    RpcClient,
                    Option<Duration>,
                )>>()
                .into_iter()
                .sorted_by_key(|(_, _, _, ping_time)| ping_time.unwrap())
                .map(|(rpc_contact_info, snapshot_hash, rpc_client, _)| {
                    (rpc_contact_info, snapshot_hash, rpc_client)
                })
                .collect::<Vec<(ContactInfo, Option<SnapshotHash>, RpcClient)>>(),
        );
        blacklisted_rpc_nodes.extend(newly_blacklisted_rpc_nodes.into_inner().unwrap());
    }
}

#[allow(clippy::too_many_arguments)]
pub fn rpc_bootstrap(
    node: &Node,
    identity_keypair: &Arc<Keypair>,
    ledger_path: &Path,
    vote_account: &Pubkey,
    authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    cluster_entrypoints: &[ContactInfo],
    validator_config: &mut ValidatorConfig,
    bootstrap_config: RpcBootstrapConfig,
    do_port_check: bool,
    use_progress_bar: bool,
    maximum_local_snapshot_age: Slot,
    should_check_duplicate_instance: bool,
    start_progress: &Arc<RwLock<ValidatorStartProgress>>,
    minimal_snapshot_download_speed: f32,
    maximum_snapshot_download_abort: u64,
    socket_addr_space: SocketAddrSpace,
) {
    if do_port_check {
        let mut order: Vec<_> = (0..cluster_entrypoints.len()).collect();
        order.shuffle(&mut thread_rng());
        if order.into_iter().all(|i| {
            !verify_reachable_ports(
                node,
                &cluster_entrypoints[i],
                validator_config,
                &socket_addr_space,
            )
        }) {
            exit(1);
        }
    }

    if bootstrap_config.no_genesis_fetch && bootstrap_config.no_snapshot_fetch {
        return;
    }

    let total_snapshot_download_time = Instant::now();
    let mut get_rpc_nodes_time = Duration::new(0, 0);
    let mut snapshot_download_time = Duration::new(0, 0);
    let mut blacklisted_rpc_nodes = HashSet::new();
    let mut gossip = None;
    let mut vetted_rpc_nodes = vec![];
    let mut download_abort_count = 0;
    loop {
        if gossip.is_none() {
            *start_progress.write().unwrap() = ValidatorStartProgress::SearchingForRpcService;

            gossip = Some(start_gossip_node(
                identity_keypair.clone(),
                cluster_entrypoints,
                ledger_path,
                &node
                    .info
                    .gossip()
                    .expect("Operator must spin up node with valid gossip address"),
                node.sockets.gossip.clone(),
                validator_config
                    .expected_shred_version
                    .expect("expected_shred_version should not be None"),
                validator_config.gossip_validators.clone(),
                should_check_duplicate_instance,
                socket_addr_space,
            ));
        }

        let get_rpc_nodes_start = Instant::now();
        get_vetted_rpc_nodes(
            &mut vetted_rpc_nodes,
            &gossip.as_ref().unwrap().0,
            validator_config,
            &mut blacklisted_rpc_nodes,
            &bootstrap_config,
        );
        let (rpc_contact_info, snapshot_hash, rpc_client) = vetted_rpc_nodes.pop().unwrap();
        get_rpc_nodes_time += get_rpc_nodes_start.elapsed();

        let snapshot_download_start = Instant::now();
        let download_result = attempt_download_genesis_and_snapshot(
            &rpc_contact_info,
            ledger_path,
            validator_config,
            &bootstrap_config,
            use_progress_bar,
            &mut gossip,
            &rpc_client,
            maximum_local_snapshot_age,
            start_progress,
            minimal_snapshot_download_speed,
            maximum_snapshot_download_abort,
            &mut download_abort_count,
            snapshot_hash,
            identity_keypair,
            vote_account,
            authorized_voter_keypairs.clone(),
        );
        snapshot_download_time += snapshot_download_start.elapsed();
        match download_result {
            Ok(()) => break,
            Err(err) => {
                fail_rpc_node(
                    err,
                    &validator_config.known_validators,
                    rpc_contact_info.pubkey(),
                    &mut blacklisted_rpc_nodes,
                );
            }
        }
    }

    if let Some(gossip) = gossip.take() {
        shutdown_gossip_service(gossip);
    }

    datapoint_info!(
        "bootstrap-snapshot-download",
        (
            "total_time_secs",
            total_snapshot_download_time.elapsed().as_secs(),
            i64
        ),
        ("get_rpc_nodes_time_secs", get_rpc_nodes_time.as_secs(), i64),
        (
            "snapshot_download_time_secs",
            snapshot_download_time.as_secs(),
            i64
        ),
        ("download_abort_count", download_abort_count, i64),
        ("blacklisted_nodes_count", blacklisted_rpc_nodes.len(), i64),
    );
}

/// Get RPC peer node candidates to download from.
///
/// This function selects snapshots from the snapshot service manifest and returns RPC peers.
fn get_rpc_nodes(
    cluster_info: &ClusterInfo,
    validator_config: &ValidatorConfig,
    blacklisted_rpc_nodes: &mut HashSet<Pubkey>,
    bootstrap_config: &RpcBootstrapConfig,
) -> Result<Vec<GetRpcNodeResult>, GetRpcNodeError> {
    let blacklist_timeout = Instant::now();
    let get_rpc_peers_timout = Instant::now();
    let mut retry_reason = None;
    loop {
        let maybe_bootstrap_rpc_peers = bootstrap_rpc_peers_from_config(
            cluster_info,
            validator_config,
            blacklisted_rpc_nodes,
            bootstrap_config,
        );

        let rpc_peers = if let Some(rpc_peers) = maybe_bootstrap_rpc_peers {
            let rpc_peers_total = rpc_peers.len() + blacklisted_rpc_nodes.len();
            if rpc_peers.is_empty() && rpc_peers_total > 0 {
                retry_reason = if !blacklisted_rpc_nodes.is_empty()
                    && blacklist_timeout.elapsed() > BLACKLIST_CLEAR_THRESHOLD
                {
                    blacklisted_rpc_nodes.clear();
                    Some("Blacklist timeout expired".to_owned())
                } else {
                    Some("Wait for bootstrap RPC peers".to_owned())
                };
                if get_rpc_peers_timout.elapsed() > GET_RPC_PEERS_TIMEOUT {
                    return Err(GetRpcNodeError::NoRpcPeersFound);
                }
                continue;
            }

            info!(
                "Using bootstrap RPC peers ({}/{})",
                rpc_peers.len(),
                rpc_peers_total
            );
            rpc_peers
        } else {
            // Give gossip some time to populate and not spin on grabbing the crds lock
            std::thread::sleep(Duration::from_secs(1));
            debug!("\n{}", cluster_info.rpc_info_trace());

            get_rpc_peers(
                cluster_info,
                validator_config,
                blacklisted_rpc_nodes,
                &blacklist_timeout,
                &mut retry_reason,
                bootstrap_config,
            )
        };
        if rpc_peers.is_empty() {
            if get_rpc_peers_timout.elapsed() > GET_RPC_PEERS_TIMEOUT {
                return Err(GetRpcNodeError::NoRpcPeersFound);
            }
            continue;
        }

        let snapshot_hash = if bootstrap_config.no_snapshot_fetch {
            None
        } else {
            let manifest_url = Url::parse(&bootstrap_config.snapshot_manifest_url).map_err(|e| {
                GetRpcNodeError::SnapshotManifestError(format!(
                    "invalid --snapshot-manifest-url: {e}"
                ))
            })?;
            let timeout = Duration::from_millis(bootstrap_config.snapshot_download_timeout_ms);
            let client = HttpClient::builder()
                .timeout(timeout)
                .build()
                .map_err(|e| {
                    GetRpcNodeError::SnapshotManifestError(format!(
                        "failed to build snapshot http client: {e}"
                    ))
                })?;
            let manifest = fetch_snapshot_manifest(&client, &manifest_url)
                .map_err(GetRpcNodeError::SnapshotManifestError)?;
            let (full, incremental) = select_snapshot_files_from_manifest(
                &manifest_url,
                &manifest,
                bootstrap_config.incremental_snapshot_fetch,
            )
            .map_err(GetRpcNodeError::SnapshotManifestError)?;

            let snapshot_hash = SnapshotHash {
                full: (full.slot, full.hash),
                incr: incremental.as_ref().map(|s| (s.slot, s.hash)),
            };

            info!(
                "Snapshot service manifest updated_at={:?}, full_slot={}, incremental_slot={:?}",
                manifest.updated_at,
                snapshot_hash.full.0,
                snapshot_hash.incr.map(|(slot, _)| slot),
            );
            Some(snapshot_hash)
        };

        let mut out = Vec::with_capacity(MAX_RPC_CONNECTIONS_EVALUATED_PER_ITERATION);
        for rpc_contact_info in rpc_peers
            .into_iter()
            .take(MAX_RPC_CONNECTIONS_EVALUATED_PER_ITERATION)
        {
            out.push(GetRpcNodeResult {
                rpc_contact_info,
                snapshot_hash,
            });
        }
        return Ok(out);
    }
}

/// Get the Slot and Hash of the local snapshot with the highest slot.  Can be either a full
/// snapshot or an incremental snapshot.
fn get_highest_local_snapshot_hash(
    full_snapshot_archives_dir: impl AsRef<Path>,
    incremental_snapshot_archives_dir: impl AsRef<Path>,
    incremental_snapshot_fetch: bool,
) -> Option<(Slot, Hash)> {
    snapshot_paths::get_highest_full_snapshot_archive_info(full_snapshot_archives_dir)
        .and_then(|full_snapshot_info| {
            if incremental_snapshot_fetch {
                snapshot_paths::get_highest_incremental_snapshot_archive_info(
                    incremental_snapshot_archives_dir,
                    full_snapshot_info.slot(),
                )
                .map(|incremental_snapshot_info| {
                    (
                        incremental_snapshot_info.slot(),
                        *incremental_snapshot_info.hash(),
                    )
                })
            } else {
                None
            }
            .or_else(|| Some((full_snapshot_info.slot(), *full_snapshot_info.hash())))
        })
        .map(|(slot, snapshot_hash)| (slot, snapshot_hash.0))
}

/// Map full snapshot hashes to a set of incremental snapshot hashes.  Each full snapshot hash
/// is treated as the base for its set of incremental snapshot hashes.
#[cfg(test)]
type KnownSnapshotHashes = HashMap<(Slot, Hash), HashSet<(Slot, Hash)>>;

/// Build the known snapshot hashes from a set of nodes.
///
/// The `get_snapshot_hashes_for_node` parameter is a function that map a pubkey to its snapshot
/// hashes.  This parameter exist to provide a way to test the inner algorithm without needing
/// runtime information such as the ClusterInfo or ValidatorConfig.
#[cfg(test)]
fn build_known_snapshot_hashes<'a>(
    nodes: impl IntoIterator<Item = &'a Pubkey>,
    get_snapshot_hashes_for_node: impl Fn(&'a Pubkey) -> Option<SnapshotHash>,
) -> KnownSnapshotHashes {
    let mut known_snapshot_hashes = KnownSnapshotHashes::new();

    /// Check to see if there exists another snapshot hash in the haystack with the *same* slot
    /// but *different* hash as the needle.
    fn is_any_same_slot_and_different_hash<'a>(
        needle: &(Slot, Hash),
        haystack: impl IntoIterator<Item = &'a (Slot, Hash)>,
    ) -> bool {
        haystack
            .into_iter()
            .any(|hay| needle.0 == hay.0 && needle.1 != hay.1)
    }

    'to_next_node: for node in nodes {
        let Some(SnapshotHash {
            full: full_snapshot_hash,
            incr: incremental_snapshot_hash,
        }) = get_snapshot_hashes_for_node(node)
        else {
            continue 'to_next_node;
        };

        // Do not add this snapshot hash if there's already a full snapshot hash with the
        // same slot but with a _different_ hash.
        // NOTE: Nodes should not produce snapshots at the same slot with _different_
        // hashes.  So if it happens, keep the first and ignore the rest.
        if is_any_same_slot_and_different_hash(&full_snapshot_hash, known_snapshot_hashes.keys()) {
            warn!(
                "Ignoring all snapshot hashes from node {node} since we've seen a different full \
                 snapshot hash with this slot. full snapshot hash: {full_snapshot_hash:?}"
            );
            debug!(
                "known full snapshot hashes: {:#?}",
                known_snapshot_hashes.keys(),
            );
            continue 'to_next_node;
        }

        // Insert a new full snapshot hash into the known snapshot hashes IFF an entry
        // doesn't already exist.  This is to ensure we don't overwrite existing
        // incremental snapshot hashes that may be present for this full snapshot hash.
        let known_incremental_snapshot_hashes =
            known_snapshot_hashes.entry(full_snapshot_hash).or_default();

        if let Some(incremental_snapshot_hash) = incremental_snapshot_hash {
            // Do not add this snapshot hash if there's already an incremental snapshot
            // hash with the same slot, but with a _different_ hash.
            // NOTE: Nodes should not produce snapshots at the same slot with _different_
            // hashes.  So if it happens, keep the first and ignore the rest.
            if is_any_same_slot_and_different_hash(
                &incremental_snapshot_hash,
                known_incremental_snapshot_hashes.iter(),
            ) {
                warn!(
                    "Ignoring incremental snapshot hash from node {node} since we've seen a \
                     different incremental snapshot hash with this slot. full snapshot hash: \
                     {full_snapshot_hash:?}, incremental snapshot hash: \
                     {incremental_snapshot_hash:?}"
                );
                debug!(
                    "known incremental snapshot hashes based on this slot: {:#?}",
                    known_incremental_snapshot_hashes.iter(),
                );
                continue 'to_next_node;
            }

            known_incremental_snapshot_hashes.insert(incremental_snapshot_hash);
        };
    }

    trace!("known snapshot hashes: {known_snapshot_hashes:?}");
    known_snapshot_hashes
}

/// Retain the peer snapshot hashes that match a snapshot hash from the known snapshot hashes
#[cfg(test)]
fn retain_peer_snapshot_hashes_that_match_known_snapshot_hashes(
    known_snapshot_hashes: &KnownSnapshotHashes,
    peer_snapshot_hashes: &mut Vec<PeerSnapshotHash>,
) {
    peer_snapshot_hashes.retain(|peer_snapshot_hash| {
        known_snapshot_hashes
            .get(&peer_snapshot_hash.snapshot_hash.full)
            .map(|known_incremental_hashes| {
                if peer_snapshot_hash.snapshot_hash.incr.is_none() {
                    // If the peer's full snapshot hashes match, but doesn't have any
                    // incremental snapshots, that's fine; keep 'em!
                    true
                } else {
                    known_incremental_hashes
                        .contains(peer_snapshot_hash.snapshot_hash.incr.as_ref().unwrap())
                }
            })
            .unwrap_or(false)
    });

    trace!(
        "retain peer snapshot hashes that match known snapshot hashes: {peer_snapshot_hashes:?}"
    );
}

/// Retain the peer snapshot hashes with the highest full snapshot slot
#[cfg(test)]
fn retain_peer_snapshot_hashes_with_highest_full_snapshot_slot(
    peer_snapshot_hashes: &mut Vec<PeerSnapshotHash>,
) {
    let highest_full_snapshot_hash = peer_snapshot_hashes
        .iter()
        .map(|peer_snapshot_hash| peer_snapshot_hash.snapshot_hash.full)
        .max_by_key(|(slot, _hash)| *slot);
    let Some(highest_full_snapshot_hash) = highest_full_snapshot_hash else {
        // `max_by_key` will only be `None` IFF the input `peer_snapshot_hashes` is empty.
        // In that case there's nothing to do (additionally, without a valid 'max' value, there
        // will be nothing to compare against within the `retain()` predicate).
        return;
    };

    peer_snapshot_hashes.retain(|peer_snapshot_hash| {
        peer_snapshot_hash.snapshot_hash.full == highest_full_snapshot_hash
    });

    trace!("retain peer snapshot hashes with highest full snapshot slot: {peer_snapshot_hashes:?}");
}

/// Retain the peer snapshot hashes with the highest incremental snapshot slot
#[cfg(test)]
fn retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot(
    peer_snapshot_hashes: &mut Vec<PeerSnapshotHash>,
) {
    let highest_incremental_snapshot_hash = peer_snapshot_hashes
        .iter()
        .flat_map(|peer_snapshot_hash| peer_snapshot_hash.snapshot_hash.incr)
        .max_by_key(|(slot, _hash)| *slot);

    peer_snapshot_hashes.retain(|peer_snapshot_hash| {
        peer_snapshot_hash.snapshot_hash.incr == highest_incremental_snapshot_hash
    });

    trace!(
        "retain peer snapshot hashes with highest incremental snapshot slot: \
         {peer_snapshot_hashes:?}"
    );
}

#[derive(Debug, Deserialize)]
struct SnapshotManifest {
    #[serde(default)]
    updated_at: Option<String>,
    full_snapshot: SnapshotManifestSnapshot,
    #[serde(default)]
    incremental_snapshots: Vec<SnapshotManifestIncrementalSnapshot>,
}

#[derive(Debug, Deserialize)]
struct SnapshotManifestSnapshot {
    filename: String,
    slot: Slot,
    size_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct SnapshotManifestIncrementalSnapshot {
    filename: String,
    base_slot: Slot,
    slot: Slot,
    size_bytes: u64,
}

#[derive(Debug, Clone)]
struct SnapshotServiceFile {
    url: Url,
    slot: Slot,
    hash: Hash,
    size_bytes: u64,
    archive_format: ArchiveFormat,
}

fn fetch_snapshot_manifest(
    client: &HttpClient,
    manifest_url: &Url,
) -> Result<SnapshotManifest, String> {
    client
        .get(manifest_url.clone())
        .send()
        .and_then(|resp| resp.error_for_status())
        .map_err(|e| format!("failed to fetch snapshot manifest {manifest_url}: {e}"))?
        .json::<SnapshotManifest>()
        .map_err(|e| format!("failed to decode snapshot manifest {manifest_url}: {e}"))
}

fn parse_full_snapshot_from_manifest(
    manifest_url: &Url,
    entry: &SnapshotManifestSnapshot,
) -> Result<SnapshotServiceFile, String> {
    let filename = std::path::Path::new(&entry.filename)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("invalid snapshot filename in manifest: {}", entry.filename))?;
    let (slot, hash, archive_format) =
        snapshot_paths::parse_full_snapshot_archive_filename(filename).map_err(|e| e.to_string())?;
    if slot != entry.slot {
        return Err(format!(
            "snapshot manifest mismatch: filename slot {slot} != field slot {} ({})",
            entry.slot, entry.filename
        ));
    }
    let url = manifest_url
        .join(&entry.filename)
        .map_err(|e| format!("invalid snapshot url join: {e}"))?;
    Ok(SnapshotServiceFile {
        url,
        slot,
        hash: hash.0,
        size_bytes: entry.size_bytes,
        archive_format,
    })
}

fn parse_incremental_snapshot_from_manifest(
    manifest_url: &Url,
    entry: &SnapshotManifestIncrementalSnapshot,
) -> Result<SnapshotServiceFile, String> {
    let filename = std::path::Path::new(&entry.filename)
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("invalid snapshot filename in manifest: {}", entry.filename))?;
    let (base_slot, slot, hash, archive_format) =
        snapshot_paths::parse_incremental_snapshot_archive_filename(filename)
            .map_err(|e| e.to_string())?;
    if base_slot != entry.base_slot || slot != entry.slot {
        return Err(format!(
            "snapshot manifest mismatch: filename base_slot {base_slot} slot {slot} != fields base_slot {} slot {} ({})",
            entry.base_slot, entry.slot, entry.filename
        ));
    }
    let url = manifest_url
        .join(&entry.filename)
        .map_err(|e| format!("invalid snapshot url join: {e}"))?;
    Ok(SnapshotServiceFile {
        url,
        slot,
        hash: hash.0,
        size_bytes: entry.size_bytes,
        archive_format,
    })
}

fn select_snapshot_files_from_manifest(
    manifest_url: &Url,
    manifest: &SnapshotManifest,
    incremental_snapshot_fetch: bool,
) -> Result<(SnapshotServiceFile, Option<SnapshotServiceFile>), String> {
    let full = parse_full_snapshot_from_manifest(manifest_url, &manifest.full_snapshot)?;

    let incremental = incremental_snapshot_fetch
        .then(|| {
            manifest
                .incremental_snapshots
                .iter()
                .filter(|snap| snap.base_slot == full.slot)
                .max_by_key(|snap| snap.slot)
        })
        .flatten()
        .map(|snap| parse_incremental_snapshot_from_manifest(manifest_url, snap))
        .transpose()?;

    Ok((full, incremental))
}

fn download_http_range_to_file(
    client: &HttpClient,
    url: &Url,
    file: &mut fs::File,
    start: u64,
    end: u64,
    buf: &mut [u8],
) -> Result<(), String> {
    let range = format!("bytes={start}-{end}");
    let mut resp = client
        .get(url.clone())
        .header(reqwest::header::RANGE, range)
        .send()
        .map_err(|e| format!("snapshot download request failed: {e}"))?;
    let status = resp.status();
    if status != reqwest::StatusCode::PARTIAL_CONTENT {
        return Err(format!(
            "snapshot download requires HTTP range support, got status {status}"
        ));
    }

    file.seek(SeekFrom::Start(start))
        .map_err(|e| format!("snapshot file seek failed: {e}"))?;

    let mut remaining: u64 = end
        .checked_sub(start)
        .and_then(|v| v.checked_add(1))
        .ok_or_else(|| "invalid range".to_string())?;
    while remaining > 0 {
        let want = (remaining as usize).min(buf.len());
        let n = resp
            .read(&mut buf[..want])
            .map_err(|e| format!("snapshot download read failed: {e}"))?;
        if n == 0 {
            return Err("snapshot download truncated (unexpected EOF)".to_string());
        }
        file.write_all(&buf[..n])
            .map_err(|e| format!("snapshot file write failed: {e}"))?;
        remaining = remaining.saturating_sub(n as u64);
    }
    Ok(())
}

fn download_http_file_concurrent_ranges(
    client: &HttpClient,
    url: &Url,
    destination_path: &Path,
    expected_size_bytes: u64,
    concurrency: usize,
    chunk_size_bytes: u64,
    max_retries: u32,
) -> Result<(), String> {
    if destination_path.is_file() {
        if let Ok(meta) = destination_path.metadata() {
            if meta.len() == expected_size_bytes {
                return Ok(());
            }
        }
        return Err(format!(
            "snapshot archive already exists but size does not match: {}",
            destination_path.display()
        ));
    }

    if expected_size_bytes == 0 {
        return Err("snapshot archive size is zero".to_string());
    }

    let Some(parent) = destination_path.parent() else {
        return Err(format!(
            "snapshot archive destination has no parent dir: {}",
            destination_path.display()
        ));
    };
    fs::create_dir_all(parent)
        .map_err(|e| format!("failed to create snapshot dir {}: {e}", parent.display()))?;

    let file_name = destination_path
        .file_name()
        .and_then(|s| s.to_str())
        .ok_or_else(|| format!("invalid snapshot destination: {}", destination_path.display()))?;
    let tmp_path = destination_path.with_file_name(format!("{file_name}.part"));
    let _ = fs::remove_file(&tmp_path);

    {
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .read(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|e| format!("failed to create snapshot temp file: {e}"))?;
        file.set_len(expected_size_bytes)
            .map_err(|e| format!("failed to preallocate snapshot file: {e}"))?;
    }

    let chunk_size_bytes = chunk_size_bytes.max(1024 * 1024);
    let num_chunks_u64 = expected_size_bytes.div_ceil(chunk_size_bytes);
    let num_chunks: usize = num_chunks_u64
        .try_into()
        .map_err(|_| format!("snapshot is too large to chunk: {expected_size_bytes}"))?;
    let worker_count = concurrency.max(1).min(num_chunks.max(1));

    let next_chunk = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let downloaded_bytes = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let (err_tx, err_rx) = mpsc::channel::<String>();

    let progress_done = Arc::new(AtomicBool::new(false));
    let progress_name = destination_path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("snapshot");
    let progress_start = Instant::now();
    let progress_thread = {
        let downloaded_bytes = Arc::clone(&downloaded_bytes);
        let stop = Arc::clone(&stop);
        let progress_done = Arc::clone(&progress_done);
        let progress_name = progress_name.to_string();
        std::thread::spawn(move || {
            let mut last_bytes: u64 = 0;
            let mut last_at = Instant::now();
            let mut next_log = last_at + Duration::from_secs(5);
            loop {
                if progress_done.load(Ordering::Relaxed) || stop.load(Ordering::Relaxed) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(200));
                if progress_done.load(Ordering::Relaxed) || stop.load(Ordering::Relaxed) {
                    return;
                }

                let now = Instant::now();
                if now < next_log {
                    continue;
                }
                let bytes = downloaded_bytes.load(Ordering::Relaxed);
                let delta_bytes = bytes.saturating_sub(last_bytes);
                let dt = now.duration_since(last_at).as_secs_f64().max(0.001);
                let mib_per_sec = (delta_bytes as f64) / (1024.0 * 1024.0) / dt;

                let pct = ((bytes as f64) / (expected_size_bytes as f64) * 100.0)
                    .clamp(0.0, 100.0);

                info!(
                    "Snapshot download progress {}: {:.1}% ({}/{}) at {:.1} MiB/s",
                    progress_name, pct, bytes, expected_size_bytes, mib_per_sec
                );

                last_bytes = bytes;
                last_at = now;
                next_log = now + Duration::from_secs(5);
            }
        })
    };

    thread::scope(|scope| {
        for _ in 0..worker_count {
            let client = client.clone();
            let url = url.clone();
            let tmp_path = tmp_path.clone();
            let next_chunk = Arc::clone(&next_chunk);
            let stop = Arc::clone(&stop);
            let downloaded_bytes = Arc::clone(&downloaded_bytes);
            let err_tx = err_tx.clone();
            scope.spawn(move || {
                let mut file = match fs::OpenOptions::new().write(true).open(&tmp_path) {
                    Ok(f) => f,
                    Err(e) => {
                        let _ = err_tx.send(format!("failed to open snapshot temp file: {e}"));
                        stop.store(true, Ordering::Relaxed);
                        return;
                    }
                };

                let mut buf = vec![0u8; 1024 * 1024];
                loop {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let chunk_index = next_chunk.fetch_add(1, Ordering::Relaxed);
                    if chunk_index >= num_chunks {
                        return;
                    }
                    let start = (chunk_index as u64).saturating_mul(chunk_size_bytes);
                    let end = start
                        .saturating_add(chunk_size_bytes.saturating_sub(1))
                        .min(expected_size_bytes.saturating_sub(1));

                    let mut attempt: u32 = 0;
                    loop {
                        attempt = attempt.saturating_add(1);
                        match download_http_range_to_file(&client, &url, &mut file, start, end, &mut buf)
                        {
                            Ok(()) => {
                                let n = end.saturating_sub(start).saturating_add(1);
                                downloaded_bytes.fetch_add(n, Ordering::Relaxed);
                                break;
                            }
                            Err(e) => {
                                if attempt > max_retries.saturating_add(1) {
                                    let _ = err_tx.send(e);
                                    stop.store(true, Ordering::Relaxed);
                                    return;
                                }
                                thread::sleep(Duration::from_millis(200_u64.saturating_mul(attempt as u64)));
                            }
                        }
                    }
                }
            });
        }
    });

    drop(err_tx);
    progress_done.store(true, Ordering::Relaxed);
    let _ = progress_thread.join();
    if let Ok(err) = err_rx.try_recv() {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }

    fs::rename(&tmp_path, destination_path)
        .map_err(|e| format!("failed to move snapshot into place: {e}"))?;

    let elapsed = progress_start.elapsed().as_secs_f64().max(0.001);
    let avg_mib_per_sec = (expected_size_bytes as f64) / (1024.0 * 1024.0) / elapsed;
    info!(
        "Snapshot download complete {}: {} bytes in {:.1}s ({:.1} MiB/s avg)",
        progress_name, expected_size_bytes, elapsed, avg_mib_per_sec
    );
    Ok(())
}

/// Check to see if we can use our local snapshots, otherwise download newer ones.
#[allow(clippy::too_many_arguments)]
fn download_snapshots(
    validator_config: &ValidatorConfig,
    bootstrap_config: &RpcBootstrapConfig,
    use_progress_bar: bool,
    maximum_local_snapshot_age: Slot,
    start_progress: &Arc<RwLock<ValidatorStartProgress>>,
    minimal_snapshot_download_speed: f32,
    maximum_snapshot_download_abort: u64,
    download_abort_count: &mut u64,
    snapshot_hash: Option<SnapshotHash>,
    rpc_contact_info: &ContactInfo,
) -> Result<(), String> {
    let _ = use_progress_bar;
    let _ = minimal_snapshot_download_speed;
    let _ = maximum_snapshot_download_abort;
    let _ = download_abort_count;

    if bootstrap_config.no_snapshot_fetch {
        return Ok(());
    }

    let manifest_url = Url::parse(&bootstrap_config.snapshot_manifest_url)
        .map_err(|e| format!("invalid --snapshot-manifest-url: {e}"))?;
    let timeout = Duration::from_millis(bootstrap_config.snapshot_download_timeout_ms);
    let client = HttpClient::builder()
        .timeout(timeout)
        .pool_max_idle_per_host(bootstrap_config.snapshot_download_concurrency.max(1))
        .build()
        .map_err(|e| format!("failed to build snapshot http client: {e}"))?;
    let manifest = fetch_snapshot_manifest(&client, &manifest_url)?;
    let (full, incremental) = select_snapshot_files_from_manifest(
        &manifest_url,
        &manifest,
        bootstrap_config.incremental_snapshot_fetch,
    )?;

    let full_snapshot_hash = (full.slot, full.hash);
    let incremental_snapshot_hash = incremental.as_ref().map(|s| (s.slot, s.hash));
    let full_snapshot_archives_dir = &validator_config.snapshot_config.full_snapshot_archives_dir;
    let incremental_snapshot_archives_dir = &validator_config
        .snapshot_config
        .incremental_snapshot_archives_dir;

    // Backwards-compat: if caller provided a snapshot hash, log if it differs from the manifest.
    if let Some(snapshot_hash) = snapshot_hash {
        let manifest_hash = SnapshotHash {
            full: full_snapshot_hash,
            incr: incremental_snapshot_hash,
        };
        if snapshot_hash != manifest_hash {
            warn!(
                "Snapshot service selection differs from bootstrap snapshot hash. Using manifest. bootstrap={snapshot_hash:?} manifest={manifest_hash:?}"
            );
        }
    }

    // If the local snapshots are new enough, then use 'em; no need to download new snapshots
    if should_use_local_snapshot(
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        maximum_local_snapshot_age,
        full_snapshot_hash,
        incremental_snapshot_hash,
        bootstrap_config.incremental_snapshot_fetch,
    ) {
        return Ok(());
    }

    // Purge old snapshot archives first (same behavior as the legacy download path).
    solana_runtime::snapshot_utils::purge_old_snapshot_archives(
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        validator_config
            .snapshot_config
            .maximum_full_snapshot_archives_to_retain,
        validator_config
            .snapshot_config
            .maximum_incremental_snapshot_archives_to_retain,
    );

    // Check and see if we've already got the full snapshot; if not, download it.
    if snapshot_paths::get_full_snapshot_archives(full_snapshot_archives_dir)
        .into_iter()
        .any(|snapshot_archive| {
            snapshot_archive.slot() == full_snapshot_hash.0
                && snapshot_archive.hash().0 == full_snapshot_hash.1
        })
    {
        info!(
            "Full snapshot archive already exists locally. Skipping download. slot: {}, hash: {}",
            full_snapshot_hash.0, full_snapshot_hash.1
        );
    } else {
        let remote_dir = snapshot_paths::build_snapshot_archives_remote_dir(full_snapshot_archives_dir);
        fs::create_dir_all(&remote_dir).map_err(|e| {
            format!(
                "failed to create full snapshot download dir {}: {e}",
                remote_dir.display()
            )
        })?;
        let dest = snapshot_paths::build_full_snapshot_archive_path(
            &remote_dir,
            full.slot,
            &agave_snapshots::snapshot_hash::SnapshotHash(full.hash),
            full.archive_format,
        );
        info!(
            "Downloading full snapshot from {} ({} bytes) -> {}",
            full.url,
            full.size_bytes,
            dest.display()
        );
        *start_progress.write().unwrap() = ValidatorStartProgress::DownloadingSnapshot {
            slot: full.slot,
            rpc_addr: rpc_contact_info
                .rpc()
                .ok_or_else(|| String::from("Invalid RPC address"))?,
        };
        download_http_file_concurrent_ranges(
            &client,
            &full.url,
            &dest,
            full.size_bytes,
            bootstrap_config.snapshot_download_concurrency,
            bootstrap_config.snapshot_download_chunk_size_bytes,
            bootstrap_config.snapshot_download_max_retries,
        )?;
    }

    if bootstrap_config.incremental_snapshot_fetch {
        // Check and see if we've already got the incremental snapshot; if not, download it
        if let Some(incremental_snapshot_hash) = incremental_snapshot_hash {
            if snapshot_paths::get_incremental_snapshot_archives(incremental_snapshot_archives_dir)
                .into_iter()
                .any(|snapshot_archive| {
                    snapshot_archive.slot() == incremental_snapshot_hash.0
                        && snapshot_archive.hash().0 == incremental_snapshot_hash.1
                        && snapshot_archive.base_slot() == full_snapshot_hash.0
                })
            {
                info!(
                    "Incremental snapshot archive already exists locally. Skipping download. \
                     slot: {}, hash: {}",
                    incremental_snapshot_hash.0, incremental_snapshot_hash.1
                );
            } else {
                let incremental = incremental
                    .as_ref()
                    .expect("incremental snapshot hash implies incremental snapshot file");
                let remote_dir = snapshot_paths::build_snapshot_archives_remote_dir(
                    incremental_snapshot_archives_dir,
                );
                fs::create_dir_all(&remote_dir).map_err(|e| {
                    format!(
                        "failed to create incremental snapshot download dir {}: {e}",
                        remote_dir.display()
                    )
                })?;
                let dest = snapshot_paths::build_incremental_snapshot_archive_path(
                    &remote_dir,
                    full.slot,
                    incremental.slot,
                    &agave_snapshots::snapshot_hash::SnapshotHash(incremental.hash),
                    incremental.archive_format,
                );
                info!(
                    "Downloading incremental snapshot from {} ({} bytes) -> {}",
                    incremental.url,
                    incremental.size_bytes,
                    dest.display()
                );
                *start_progress.write().unwrap() = ValidatorStartProgress::DownloadingSnapshot {
                    slot: incremental.slot,
                    rpc_addr: rpc_contact_info
                        .rpc()
                        .ok_or_else(|| String::from("Invalid RPC address"))?,
                };
                download_http_file_concurrent_ranges(
                    &client,
                    &incremental.url,
                    &dest,
                    incremental.size_bytes,
                    bootstrap_config.snapshot_download_concurrency,
                    bootstrap_config.snapshot_download_chunk_size_bytes,
                    bootstrap_config.snapshot_download_max_retries,
                )?;
            }
        }
    }

    Ok(())
}

/// Check to see if bootstrap should load from its local snapshots or not.  If not, then snapshots
/// will be downloaded.
fn should_use_local_snapshot(
    full_snapshot_archives_dir: &Path,
    incremental_snapshot_archives_dir: &Path,
    maximum_local_snapshot_age: Slot,
    full_snapshot_hash: (Slot, Hash),
    incremental_snapshot_hash: Option<(Slot, Hash)>,
    incremental_snapshot_fetch: bool,
) -> bool {
    let cluster_snapshot_slot = incremental_snapshot_hash
        .map(|(slot, _)| slot)
        .unwrap_or(full_snapshot_hash.0);

    match get_highest_local_snapshot_hash(
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        incremental_snapshot_fetch,
    ) {
        None => {
            info!(
                "Downloading a snapshot for slot {cluster_snapshot_slot} since there is not a \
                 local snapshot."
            );
            false
        }
        Some((local_snapshot_slot, _)) => {
            if local_snapshot_slot
                >= cluster_snapshot_slot.saturating_sub(maximum_local_snapshot_age)
            {
                info!(
                    "Reusing local snapshot at slot {local_snapshot_slot} instead of downloading \
                     a snapshot for slot {cluster_snapshot_slot}."
                );
                true
            } else {
                info!(
                    "Local snapshot from slot {local_snapshot_slot} is too old. Downloading a \
                     newer snapshot for slot {cluster_snapshot_slot}."
                );
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::net::TcpListener;

    impl PeerSnapshotHash {
        fn new(
            rpc_contact_info: ContactInfo,
            full_snapshot_hash: (Slot, Hash),
            incremental_snapshot_hash: Option<(Slot, Hash)>,
        ) -> Self {
            Self {
                rpc_contact_info,
                snapshot_hash: SnapshotHash {
                    full: full_snapshot_hash,
                    incr: incremental_snapshot_hash,
                },
            }
        }
    }

    fn default_contact_info_for_tests() -> ContactInfo {
        ContactInfo::new_localhost(&Pubkey::default(), /*now:*/ 1_681_834_947_321)
    }

    #[test]
    fn fail_rpc_node_blacklists_unless_known() {
        let rpc_id = Pubkey::new_unique();

        let mut blacklist = HashSet::new();
        fail_rpc_node(
            "test error".to_string(),
            &None,
            &rpc_id,
            &mut blacklist,
        );
        assert!(blacklist.contains(&rpc_id));

        let known_validators: HashSet<Pubkey> = [rpc_id].into_iter().collect();
        let mut blacklist = HashSet::new();
        fail_rpc_node(
            "test error".to_string(),
            &Some(known_validators),
            &rpc_id,
            &mut blacklist,
        );
        assert!(!blacklist.contains(&rpc_id));
    }

    #[test]
    fn deterministic_pubkey_for_socket_addr_is_stable() {
        let a: SocketAddr = "127.0.0.1:8899".parse().unwrap();
        let b: SocketAddr = "127.0.0.1:8900".parse().unwrap();

        let a1 = deterministic_pubkey_for_socket_addr(&a);
        let a2 = deterministic_pubkey_for_socket_addr(&a);
        assert_eq!(a1, a2);
        assert_ne!(a1, deterministic_pubkey_for_socket_addr(&b));
    }

    #[test]
    fn parse_socket_addrs_from_json_accepts_arrays_and_objects() {
        let json: JsonValue = serde_json::from_str(
            r#"{"rpc_addrs":["2.2.2.2:8899","1.1.1.1:8899","1.1.1.1:8899"],"ignored":["9.9.9.9:1234"]}"#,
        )
        .unwrap();
        let addrs = parse_socket_addrs_from_json(&json);
        assert_eq!(
            addrs,
            vec![
                "1.1.1.1:8899".parse().unwrap(),
                "2.2.2.2:8899".parse().unwrap(),
            ]
        );

        let json: JsonValue = serde_json::from_str(r#"["3.3.3.3:8899","4.4.4.4:8899"]"#).unwrap();
        let addrs = parse_socket_addrs_from_json(&json);
        assert_eq!(
            addrs,
            vec![
                "3.3.3.3:8899".parse().unwrap(),
                "4.4.4.4:8899".parse().unwrap(),
            ]
        );
    }

    #[test]
    fn snapshot_manifest_selection_prefers_highest_incremental_for_base() {
        let full_hash = Hash::new_unique();
        let inc_hash1 = Hash::new_unique();
        let inc_hash2 = Hash::new_unique();

        let manifest = SnapshotManifest {
            updated_at: Some("2026-01-01T00:00:00Z".to_string()),
            full_snapshot: SnapshotManifestSnapshot {
                filename: format!("snapshots/snapshot-100-{}.tar.zst", full_hash),
                slot: 100,
                size_bytes: 123,
            },
            incremental_snapshots: vec![
                SnapshotManifestIncrementalSnapshot {
                    filename: format!(
                        "snapshots/incremental-snapshot-100-101-{}.tar.zst",
                        inc_hash1
                    ),
                    base_slot: 100,
                    slot: 101,
                    size_bytes: 10,
                },
                SnapshotManifestIncrementalSnapshot {
                    filename: format!(
                        "snapshots/incremental-snapshot-100-102-{}.tar.zst",
                        inc_hash2
                    ),
                    base_slot: 100,
                    slot: 102,
                    size_bytes: 11,
                },
                // Different base slot should be ignored.
                SnapshotManifestIncrementalSnapshot {
                    filename: format!(
                        "snapshots/incremental-snapshot-99-200-{}.tar.zst",
                        Hash::new_unique()
                    ),
                    base_slot: 99,
                    slot: 200,
                    size_bytes: 999,
                },
            ],
        };

        let manifest_url = Url::parse("https://data.example.com/snapshot-manifest.json").unwrap();
        let (full, incremental) =
            select_snapshot_files_from_manifest(&manifest_url, &manifest, true).unwrap();

        assert_eq!(full.slot, 100);
        assert_eq!(full.hash, full_hash);
        assert_eq!(full.url.as_str(), &format!("https://data.example.com/{}", manifest.full_snapshot.filename));

        let incremental = incremental.unwrap();
        assert_eq!(incremental.slot, 102);
        assert_eq!(incremental.hash, inc_hash2);
    }

    #[test]
    fn snapshot_range_downloader_roundtrips() {
        let chunk_size: u64 = 64 * 1024;
        let concurrency: usize = 4;

        let content: Vec<u8> = (0..(1024 * 1024))
            .map(|i| (i % 251) as u8)
            .collect();
        let content_server = content.clone();
        let total_size = content.len() as u64;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let done_server = Arc::clone(&done);
        let server = thread::spawn(move || {
            while !done_server.load(Ordering::Relaxed) {
                let (mut stream, _peer) = match listener.accept() {
                    Ok(v) => v,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => panic!("accept failed: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                let mut req = Vec::new();
                let mut buf = [0u8; 1024];
                loop {
                    let n = stream.read(&mut buf).unwrap();
                    if n == 0 {
                        break;
                    }
                    req.extend_from_slice(&buf[..n]);
                    if req.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                    if req.len() > 64 * 1024 {
                        break;
                    }
                }
                let req_str = String::from_utf8_lossy(&req);
                let range_line = req_str
                    .lines()
                    .find(|l| l.to_ascii_lowercase().starts_with("range:"))
                    .expect("range header present");
                let range = range_line
                    .splitn(2, ':')
                    .nth(1)
                    .unwrap()
                    .trim()
                    .strip_prefix("bytes=")
                    .unwrap();
                let (start_s, end_s) = range.split_once('-').unwrap();
                let start: usize = start_s.parse().unwrap();
                let end: usize = end_s.parse().unwrap();
                let slice = &content_server[start..=end];
                let header = format!(
                    "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    slice.len()
                );
                stream.write_all(header.as_bytes()).unwrap();
                stream.write_all(slice).unwrap();
            }
        });

        let client = HttpClient::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        let url = Url::parse(&format!("http://{addr}/snapshot.tar.zst")).unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let dest = tmp.path().join("snapshot.tar.zst");

        download_http_file_concurrent_ranges(
            &client,
            &url,
            &dest,
            total_size,
            concurrency,
            chunk_size,
            0,
        )
        .unwrap();

        let downloaded = fs::read(&dest).unwrap();
        assert_eq!(downloaded, content);

        done.store(true, Ordering::Relaxed);
        server.join().unwrap();
    }

    #[test]
    fn download_snapshots_from_manifest_uses_ranges_and_skips_redownload() {
        struct SnapshotHttpServerState {
            manifest_body: Vec<u8>,
            files: HashMap<String, Vec<u8>>,
            manifest_requests: AtomicUsize,
            range_requests: AtomicUsize,
        }

        fn write_http_response(mut stream: std::net::TcpStream, status: &str, body: &[u8]) {
            let header = format!(
                "HTTP/1.1 {status}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        }

        fn handle_http_connection(mut stream: std::net::TcpStream, state: Arc<SnapshotHttpServerState>) {
            stream.set_nonblocking(false).unwrap();

            let mut req = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let n = stream.read(&mut buf).unwrap();
                if n == 0 {
                    break;
                }
                req.extend_from_slice(&buf[..n]);
                if req.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                if req.len() > 64 * 1024 {
                    break;
                }
            }

            let req_str = String::from_utf8_lossy(&req);
            let Some(request_line) = req_str.lines().next() else {
                write_http_response(stream, "400 Bad Request", b"");
                return;
            };
            let mut parts = request_line.split_whitespace();
            let method = parts.next().unwrap_or("");
            let path = parts.next().unwrap_or("");

            if method != "GET" {
                write_http_response(stream, "405 Method Not Allowed", b"");
                return;
            }

            if path == "/snapshot-manifest.json" {
                state.manifest_requests.fetch_add(1, Ordering::Relaxed);
                write_http_response(stream, "200 OK", &state.manifest_body);
                return;
            }

            let Some(content) = state.files.get(path) else {
                write_http_response(stream, "404 Not Found", b"");
                return;
            };

            let range_line = req_str
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("range:"));
            let Some(range_line) = range_line else {
                write_http_response(stream, "416 Range Not Satisfiable", b"");
                return;
            };
            let range = range_line
                .splitn(2, ':')
                .nth(1)
                .unwrap_or("")
                .trim()
                .strip_prefix("bytes=")
                .unwrap_or("");
            let Some((start_s, end_s)) = range.split_once('-') else {
                write_http_response(stream, "416 Range Not Satisfiable", b"");
                return;
            };
            let start: usize = start_s.parse().unwrap();
            let end: usize = end_s.parse().unwrap();
            assert!(start <= end);
            assert!(end < content.len());

            state.range_requests.fetch_add(1, Ordering::Relaxed);

            let slice = &content[start..=end];
            let header = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Length: {}\r\nContent-Range: bytes {}-{}/{}\r\nConnection: close\r\n\r\n",
                slice.len(),
                start,
                end,
                content.len()
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(slice).unwrap();
        }

        let full_slot: Slot = 100;
        let incremental_slot: Slot = 101;
        let full_hash = Hash::new_unique();
        let incremental_hash = Hash::new_unique();

        let full_file = format!("snapshot-{full_slot}-{full_hash}.tar.zst");
        let incremental_file = format!(
            "incremental-snapshot-{full_slot}-{incremental_slot}-{incremental_hash}.tar.zst"
        );

        let full_content: Vec<u8> = (0..(3 * 1024 * 1024))
            .map(|i| (i % 251) as u8)
            .collect();
        let incremental_content: Vec<u8> = (0..(2 * 1024 * 1024))
            .map(|i| (i % 241) as u8)
            .collect();

        let manifest_body = serde_json::json!({
            "updated_at": "2026-01-01T00:00:00Z",
            "full_snapshot": {
                "filename": format!("snapshots/{full_file}"),
                "slot": full_slot,
                "size_bytes": full_content.len() as u64,
            },
            "incremental_snapshots": [{
                "filename": format!("snapshots/{incremental_file}"),
                "base_slot": full_slot,
                "slot": incremental_slot,
                "size_bytes": incremental_content.len() as u64,
            }],
        })
        .to_string()
        .into_bytes();

        let mut files = HashMap::new();
        files.insert(format!("/snapshots/{full_file}"), full_content.clone());
        files.insert(
            format!("/snapshots/{incremental_file}"),
            incremental_content.clone(),
        );

        let state = Arc::new(SnapshotHttpServerState {
            manifest_body,
            files,
            manifest_requests: AtomicUsize::new(0),
            range_requests: AtomicUsize::new(0),
        });

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let addr = listener.local_addr().unwrap();

        let done = Arc::new(AtomicBool::new(false));
        let done_server = Arc::clone(&done);
        let state_server = Arc::clone(&state);
        let server = thread::spawn(move || {
            let mut handlers = Vec::new();
            while !done_server.load(Ordering::Relaxed) {
                let (stream, _peer) = match listener.accept() {
                    Ok(v) => v,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(5));
                        continue;
                    }
                    Err(e) => panic!("accept failed: {e}"),
                };
                let state = Arc::clone(&state_server);
                handlers.push(thread::spawn(move || handle_http_connection(stream, state)));
            }
            for h in handlers {
                let _ = h.join();
            }
        });

        let tmp = tempfile::tempdir().unwrap();
        let full_dir = tmp.path().join("full");
        let incremental_dir = tmp.path().join("incremental");
        fs::create_dir_all(&full_dir).unwrap();
        fs::create_dir_all(&incremental_dir).unwrap();

        let mut validator_config = ValidatorConfig::default_for_test();
        validator_config.snapshot_config.full_snapshot_archives_dir = full_dir.clone();
        validator_config.snapshot_config.incremental_snapshot_archives_dir = incremental_dir.clone();

        let bootstrap_config = RpcBootstrapConfig {
            no_genesis_fetch: false,
            no_snapshot_fetch: false,
            only_known_rpc: false,
            max_genesis_archive_unpacked_size: 0,
            check_vote_account: None,
            bootstrap_rpc_addrs: Vec::new(),
            bootstrap_rpc_addrs_url: None,
            incremental_snapshot_fetch: true,
            snapshot_manifest_url: format!("http://{addr}/snapshot-manifest.json"),
            snapshot_download_concurrency: 4,
            snapshot_download_chunk_size_bytes: 1024 * 1024,
            snapshot_download_timeout_ms: 5_000,
            snapshot_download_max_retries: 0,
        };

        let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::Initializing));
        let mut download_abort_count = 0u64;
        let rpc_contact_info = ContactInfo::new_localhost(&Pubkey::new_unique(), 1_681_834_947_321);

        download_snapshots(
            &validator_config,
            &bootstrap_config,
            false,
            0,
            &start_progress,
            0.0,
            0,
            &mut download_abort_count,
            None,
            &rpc_contact_info,
        )
        .unwrap();

        assert!(state.manifest_requests.load(Ordering::Relaxed) >= 1);
        assert_eq!(state.range_requests.load(Ordering::Relaxed), 5);

        let full_remote = snapshot_paths::build_snapshot_archives_remote_dir(&full_dir);
        let (slot, hash, archive_format) =
            snapshot_paths::parse_full_snapshot_archive_filename(&full_file).unwrap();
        let full_path =
            snapshot_paths::build_full_snapshot_archive_path(&full_remote, slot, &hash, archive_format);
        assert_eq!(fs::read(&full_path).unwrap(), full_content);

        let incremental_remote = snapshot_paths::build_snapshot_archives_remote_dir(&incremental_dir);
        let (base_slot, slot, hash, archive_format) =
            snapshot_paths::parse_incremental_snapshot_archive_filename(&incremental_file).unwrap();
        let incremental_path = snapshot_paths::build_incremental_snapshot_archive_path(
            &incremental_remote,
            base_slot,
            slot,
            &hash,
            archive_format,
        );
        assert_eq!(fs::read(&incremental_path).unwrap(), incremental_content);

        let before = state.range_requests.load(Ordering::Relaxed);
        download_snapshots(
            &validator_config,
            &bootstrap_config,
            false,
            0,
            &start_progress,
            0.0,
            0,
            &mut download_abort_count,
            None,
            &rpc_contact_info,
        )
        .unwrap();
        assert_eq!(state.range_requests.load(Ordering::Relaxed), before);

        done.store(true, Ordering::Relaxed);
        server.join().unwrap();
    }

    #[test]
    fn test_build_known_snapshot_hashes() {
        agave_logger::setup();
        let full_snapshot_hash1 = (400_000, Hash::new_unique());
        let full_snapshot_hash2 = (400_000, Hash::new_unique());

        let incremental_snapshot_hash1 = (400_800, Hash::new_unique());
        let incremental_snapshot_hash2 = (400_800, Hash::new_unique());

        // simulate a set of known validators with various snapshot hashes
        let oracle = {
            let mut oracle = HashMap::new();

            for (full, incr) in [
                // only a full snapshot
                (full_snapshot_hash1, None),
                // full and incremental snapshots
                (full_snapshot_hash1, Some(incremental_snapshot_hash1)),
                // full and incremental snapshots, with different incremental hash
                (full_snapshot_hash1, Some(incremental_snapshot_hash2)),
                // ...and now with different full hashes
                (full_snapshot_hash2, None),
                (full_snapshot_hash2, Some(incremental_snapshot_hash1)),
                (full_snapshot_hash2, Some(incremental_snapshot_hash2)),
            ] {
                // also simulate multiple known validators having the same snapshot hashes
                oracle.insert(Pubkey::new_unique(), Some(SnapshotHash { full, incr }));
                oracle.insert(Pubkey::new_unique(), Some(SnapshotHash { full, incr }));
                oracle.insert(Pubkey::new_unique(), Some(SnapshotHash { full, incr }));
            }

            // no snapshots at all
            oracle.insert(Pubkey::new_unique(), None);
            oracle.insert(Pubkey::new_unique(), None);
            oracle.insert(Pubkey::new_unique(), None);

            oracle
        };

        let node_to_snapshot_hashes = |node| *oracle.get(node).unwrap();

        let known_snapshot_hashes =
            build_known_snapshot_hashes(oracle.keys(), node_to_snapshot_hashes);

        // ensure there's only one full snapshot hash, since they all used the same slot and there
        // can be only one snapshot hash per slot
        let known_full_snapshot_hashes = known_snapshot_hashes.keys();
        assert_eq!(known_full_snapshot_hashes.len(), 1);
        let known_full_snapshot_hash = known_full_snapshot_hashes.into_iter().next().unwrap();

        // and for the same reasons, ensure there is only one incremental snapshot hash
        let known_incremental_snapshot_hashes =
            known_snapshot_hashes.get(known_full_snapshot_hash).unwrap();
        assert_eq!(known_incremental_snapshot_hashes.len(), 1);
        let known_incremental_snapshot_hash =
            known_incremental_snapshot_hashes.iter().next().unwrap();

        // The resulting `known_snapshot_hashes` can be different from run-to-run due to how
        // `oracle.keys()` returns nodes during iteration.  Because of that, we cannot just assert
        // the full and incremental snapshot hashes are `full_snapshot_hash1` and
        // `incremental_snapshot_hash1`.  Instead, we assert that the full and incremental
        // snapshot hashes are exactly one or the other, since it depends on which nodes are seen
        // "first" when building the known snapshot hashes.
        assert!(
            known_full_snapshot_hash == &full_snapshot_hash1
                || known_full_snapshot_hash == &full_snapshot_hash2
        );
        assert!(
            known_incremental_snapshot_hash == &incremental_snapshot_hash1
                || known_incremental_snapshot_hash == &incremental_snapshot_hash2
        );
    }

    #[test]
    fn test_retain_peer_snapshot_hashes_that_match_known_snapshot_hashes() {
        let known_snapshot_hashes: KnownSnapshotHashes = [
            (
                (200_000, Hash::new_unique()),
                [
                    (200_200, Hash::new_unique()),
                    (200_400, Hash::new_unique()),
                    (200_600, Hash::new_unique()),
                    (200_800, Hash::new_unique()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
            (
                (300_000, Hash::new_unique()),
                [
                    (300_200, Hash::new_unique()),
                    (300_400, Hash::new_unique()),
                    (300_600, Hash::new_unique()),
                ]
                .iter()
                .cloned()
                .collect(),
            ),
        ]
        .iter()
        .cloned()
        .collect();

        let known_snapshot_hash = known_snapshot_hashes.iter().next().unwrap();
        let known_full_snapshot_hash = known_snapshot_hash.0;
        let known_incremental_snapshot_hash = known_snapshot_hash.1.iter().next().unwrap();

        let contact_info = default_contact_info_for_tests();
        let peer_snapshot_hashes = vec![
            // bad full snapshot hash, no incremental snapshot hash
            PeerSnapshotHash::new(contact_info.clone(), (111_000, Hash::default()), None),
            // bad everything
            PeerSnapshotHash::new(
                contact_info.clone(),
                (111_000, Hash::default()),
                Some((111_111, Hash::default())),
            ),
            // good full snapshot hash, no incremental snapshot hash
            PeerSnapshotHash::new(contact_info.clone(), *known_full_snapshot_hash, None),
            // bad full snapshot hash, good (not possible) incremental snapshot hash
            PeerSnapshotHash::new(
                contact_info.clone(),
                (111_000, Hash::default()),
                Some(*known_incremental_snapshot_hash),
            ),
            // good full snapshot hash, bad incremental snapshot hash
            PeerSnapshotHash::new(
                contact_info.clone(),
                *known_full_snapshot_hash,
                Some((111_111, Hash::default())),
            ),
            // good everything
            PeerSnapshotHash::new(
                contact_info.clone(),
                *known_full_snapshot_hash,
                Some(*known_incremental_snapshot_hash),
            ),
        ];

        let expected = vec![
            PeerSnapshotHash::new(contact_info.clone(), *known_full_snapshot_hash, None),
            PeerSnapshotHash::new(
                contact_info,
                *known_full_snapshot_hash,
                Some(*known_incremental_snapshot_hash),
            ),
        ];
        let mut actual = peer_snapshot_hashes;
        retain_peer_snapshot_hashes_that_match_known_snapshot_hashes(
            &known_snapshot_hashes,
            &mut actual,
        );
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_retain_peer_snapshot_hashes_with_highest_full_snapshot_slot() {
        let contact_info = default_contact_info_for_tests();
        let peer_snapshot_hashes = vec![
            // old
            PeerSnapshotHash::new(contact_info.clone(), (100_000, Hash::default()), None),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (100_000, Hash::default()),
                Some((100_100, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (100_000, Hash::default()),
                Some((100_200, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (100_000, Hash::default()),
                Some((100_300, Hash::default())),
            ),
            // new
            PeerSnapshotHash::new(contact_info.clone(), (200_000, Hash::default()), None),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_100, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_200, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_300, Hash::default())),
            ),
        ];

        let expected = vec![
            PeerSnapshotHash::new(contact_info.clone(), (200_000, Hash::default()), None),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_100, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_200, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info,
                (200_000, Hash::default()),
                Some((200_300, Hash::default())),
            ),
        ];
        let mut actual = peer_snapshot_hashes;
        retain_peer_snapshot_hashes_with_highest_full_snapshot_slot(&mut actual);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot_some() {
        let contact_info = default_contact_info_for_tests();
        let peer_snapshot_hashes = vec![
            PeerSnapshotHash::new(contact_info.clone(), (200_000, Hash::default()), None),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_100, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_200, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_300, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_010, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_020, Hash::default())),
            ),
            PeerSnapshotHash::new(
                contact_info.clone(),
                (200_000, Hash::default()),
                Some((200_030, Hash::default())),
            ),
        ];

        let expected = vec![PeerSnapshotHash::new(
            contact_info,
            (200_000, Hash::default()),
            Some((200_300, Hash::default())),
        )];
        let mut actual = peer_snapshot_hashes;
        retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot(&mut actual);
        assert_eq!(expected, actual);
    }

    /// Ensure that retaining the highest incremental snapshot hashes works as expected even if
    /// there are *zero* peers with incremental snapshots.
    #[test]
    fn test_retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot_none() {
        let contact_info = default_contact_info_for_tests();
        let peer_snapshot_hashes = vec![
            PeerSnapshotHash::new(contact_info.clone(), (200_000, Hash::new_unique()), None),
            PeerSnapshotHash::new(contact_info.clone(), (200_000, Hash::new_unique()), None),
            PeerSnapshotHash::new(contact_info, (200_000, Hash::new_unique()), None),
        ];

        let expected = peer_snapshot_hashes.clone();
        let mut actual = peer_snapshot_hashes;
        retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot(&mut actual);
        assert_eq!(expected, actual);
    }

    /// Ensure that retaining the highest snapshot hashes works (i.e. doesn't crash) even if the
    /// peer snapshot hashes input is empty.
    #[test]
    fn test_retain_peer_snapshot_hashes_with_highest_slot_empty() {
        {
            let mut actual = vec![];
            let expected = actual.clone();
            retain_peer_snapshot_hashes_with_highest_full_snapshot_slot(&mut actual);
            assert_eq!(expected, actual);
        }
        {
            let mut actual = vec![];
            let expected = actual.clone();
            retain_peer_snapshot_hashes_with_highest_incremental_snapshot_slot(&mut actual);
            assert_eq!(expected, actual);
        }
    }
}
