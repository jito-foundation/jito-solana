#![allow(clippy::integer_arithmetic, dead_code)]
use {
    log::*,
    solana_client::rpc_client::RpcClient,
    solana_core::{
        broadcast_stage::BroadcastStageType,
        consensus::{Tower, SWITCH_FORK_THRESHOLD},
        tower_storage::{FileTowerStorage, SavedTower, SavedTowerVersions, TowerStorage},
        validator::ValidatorConfig,
    },
    solana_gossip::gossip_service::discover_cluster,
    solana_ledger::{
        ancestor_iterator::AncestorIterator,
        blockstore::{Blockstore, PurgeType},
        blockstore_db::{AccessType, BlockstoreOptions},
        leader_schedule::{FixedSchedule, LeaderSchedule},
    },
    solana_local_cluster::{
        cluster::{Cluster, ClusterValidatorInfo},
        cluster_tests,
        local_cluster::{ClusterConfig, LocalCluster},
        validator_configs::*,
    },
    solana_runtime::snapshot_config::SnapshotConfig,
    solana_sdk::{
        account::AccountSharedData,
        clock::{self, Slot, DEFAULT_MS_PER_SLOT, DEFAULT_TICKS_PER_SLOT},
        hash::Hash,
        native_token::LAMPORTS_PER_SOL,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        collections::HashSet,
        fs, iter,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::sleep,
        time::Duration,
    },
    tempfile::TempDir,
};

pub const RUST_LOG_FILTER: &str =
    "error,solana_core::replay_stage=warn,solana_local_cluster=info,local_cluster=info";

pub const DEFAULT_CLUSTER_LAMPORTS: u64 = 10_000_000 * LAMPORTS_PER_SOL;
pub const DEFAULT_NODE_STAKE: u64 = 10 * LAMPORTS_PER_SOL;

pub fn last_vote_in_tower(tower_path: &Path, node_pubkey: &Pubkey) -> Option<(Slot, Hash)> {
    restore_tower(tower_path, node_pubkey).map(|tower| tower.last_voted_slot_hash().unwrap())
}

pub fn restore_tower(tower_path: &Path, node_pubkey: &Pubkey) -> Option<Tower> {
    let file_tower_storage = FileTowerStorage::new(tower_path.to_path_buf());

    let tower = Tower::restore(&file_tower_storage, node_pubkey);
    if let Err(tower_err) = tower {
        if tower_err.is_file_missing() {
            return None;
        } else {
            panic!("tower restore failed...: {:?}", tower_err);
        }
    }
    // actually saved tower must have at least one vote.
    Tower::restore(&file_tower_storage, node_pubkey).ok()
}

pub fn remove_tower(tower_path: &Path, node_pubkey: &Pubkey) {
    let file_tower_storage = FileTowerStorage::new(tower_path.to_path_buf());
    fs::remove_file(file_tower_storage.filename(node_pubkey)).unwrap();
}

pub fn open_blockstore(ledger_path: &Path) -> Blockstore {
    Blockstore::open_with_options(
        ledger_path,
        BlockstoreOptions {
            access_type: AccessType::Primary,
            recovery_mode: None,
            enforce_ulimit_nofile: true,
            ..BlockstoreOptions::default()
        },
    )
    // Fall back on Secondary if Primary fails; Primary will fail if
    // a handle to Blockstore is being held somewhere else
    .unwrap_or_else(|_| {
        Blockstore::open_with_options(
            ledger_path,
            BlockstoreOptions {
                access_type: AccessType::Secondary,
                recovery_mode: None,
                enforce_ulimit_nofile: true,
                ..BlockstoreOptions::default()
            },
        )
        .unwrap_or_else(|e| {
            panic!("Failed to open ledger at {:?}, err: {}", ledger_path, e);
        })
    })
}

pub fn purge_slots(blockstore: &Blockstore, start_slot: Slot, slot_count: Slot) {
    blockstore.purge_from_next_slots(start_slot, start_slot + slot_count);
    blockstore.purge_slots(start_slot, start_slot + slot_count, PurgeType::Exact);
}

// Fetches the last vote in the tower, blocking until it has also appeared in blockstore.
// Fails if tower is empty
pub fn wait_for_last_vote_in_tower_to_land_in_ledger(
    ledger_path: &Path,
    node_pubkey: &Pubkey,
) -> Slot {
    let (last_vote, _) = last_vote_in_tower(ledger_path, node_pubkey).unwrap();
    loop {
        // We reopen in a loop to make sure we get updates
        let blockstore = open_blockstore(ledger_path);
        if blockstore.is_full(last_vote) {
            break;
        }
        sleep(Duration::from_millis(100));
    }
    last_vote
}

pub fn copy_blocks(end_slot: Slot, source: &Blockstore, dest: &Blockstore) {
    for slot in std::iter::once(end_slot).chain(AncestorIterator::new(end_slot, source)) {
        let source_meta = source.meta(slot).unwrap().unwrap();
        assert!(source_meta.is_full());

        let shreds = source.get_data_shreds_for_slot(slot, 0).unwrap();
        dest.insert_shreds(shreds, None, false).unwrap();

        let dest_meta = dest.meta(slot).unwrap().unwrap();
        assert!(dest_meta.is_full());
        assert_eq!(dest_meta.last_index, source_meta.last_index);
    }
}

/// Computes the numbr of milliseconds `num_blocks` blocks will take given
/// each slot contains `ticks_per_slot`
pub fn ms_for_n_slots(num_blocks: u64, ticks_per_slot: u64) -> u64 {
    ((ticks_per_slot * DEFAULT_MS_PER_SLOT * num_blocks) + DEFAULT_TICKS_PER_SLOT - 1)
        / DEFAULT_TICKS_PER_SLOT
}

#[allow(clippy::assertions_on_constants)]
pub fn run_kill_partition_switch_threshold<C>(
    stakes_to_kill: &[(usize, usize)],
    alive_stakes: &[(usize, usize)],
    ticks_per_slot: Option<u64>,
    partition_context: C,
    on_partition_start: impl Fn(&mut LocalCluster, &[Pubkey], Vec<ClusterValidatorInfo>, &mut C),
    on_before_partition_resolved: impl Fn(&mut LocalCluster, &mut C),
    on_partition_resolved: impl Fn(&mut LocalCluster, &mut C),
) {
    // Needs to be at least 1/3 or there will be no overlap
    // with the confirmation supermajority 2/3
    assert!(SWITCH_FORK_THRESHOLD >= 1f64 / 3f64);
    info!(
        "stakes_to_kill: {:?}, alive_stakes: {:?}",
        stakes_to_kill, alive_stakes
    );

    // This test:
    // 1) Spins up three partitions
    // 2) Kills the first partition with the stake `failures_stake`
    // 5) runs `on_partition_resolved`
    let partitions: Vec<(usize, usize)> = stakes_to_kill
        .iter()
        .cloned()
        .chain(alive_stakes.iter().cloned())
        .collect();

    let stake_partitions: Vec<usize> = partitions.iter().map(|(stake, _)| *stake).collect();
    let num_slots_per_validator: Vec<usize> =
        partitions.iter().map(|(_, num_slots)| *num_slots).collect();

    let (leader_schedule, validator_keys) =
        create_custom_leader_schedule_with_random_keys(&num_slots_per_validator);

    info!(
        "Validator ids: {:?}",
        validator_keys
            .iter()
            .map(|k| k.pubkey())
            .collect::<Vec<_>>()
    );
    let validator_pubkeys: Vec<Pubkey> = validator_keys.iter().map(|k| k.pubkey()).collect();
    let on_partition_start = |cluster: &mut LocalCluster, partition_context: &mut C| {
        let dead_validator_infos: Vec<ClusterValidatorInfo> = validator_pubkeys
            [0..stakes_to_kill.len()]
            .iter()
            .map(|validator_to_kill| {
                info!("Killing validator with id: {}", validator_to_kill);
                cluster.exit_node(validator_to_kill)
            })
            .collect();
        on_partition_start(
            cluster,
            &validator_pubkeys,
            dead_validator_infos,
            partition_context,
        );
    };
    run_cluster_partition(
        &stake_partitions,
        Some((leader_schedule, validator_keys)),
        partition_context,
        on_partition_start,
        on_before_partition_resolved,
        on_partition_resolved,
        ticks_per_slot,
        vec![],
    )
}

pub fn create_custom_leader_schedule(
    validator_key_to_slots: impl Iterator<Item = (Pubkey, usize)>,
) -> LeaderSchedule {
    let mut leader_schedule = vec![];
    for (k, num_slots) in validator_key_to_slots {
        for _ in 0..num_slots {
            leader_schedule.push(k)
        }
    }

    info!("leader_schedule: {}", leader_schedule.len());
    LeaderSchedule::new_from_schedule(leader_schedule)
}

pub fn create_custom_leader_schedule_with_random_keys(
    validator_num_slots: &[usize],
) -> (LeaderSchedule, Vec<Arc<Keypair>>) {
    let validator_keys: Vec<_> = iter::repeat_with(|| Arc::new(Keypair::new()))
        .take(validator_num_slots.len())
        .collect();
    let leader_schedule = create_custom_leader_schedule(
        validator_keys
            .iter()
            .map(|k| k.pubkey())
            .zip(validator_num_slots.iter().cloned()),
    );
    (leader_schedule, validator_keys)
}

/// This function runs a network, initiates a partition based on a
/// configuration, resolve the partition, then checks that the network
/// continues to achieve consensus
/// # Arguments
/// * `partitions` - A slice of partition configurations, where each partition
/// configuration is a usize representing a node's stake
/// * `leader_schedule` - An option that specifies whether the cluster should
/// run with a fixed, predetermined leader schedule
#[allow(clippy::cognitive_complexity)]
pub fn run_cluster_partition<C>(
    partitions: &[usize],
    leader_schedule: Option<(LeaderSchedule, Vec<Arc<Keypair>>)>,
    mut context: C,
    on_partition_start: impl FnOnce(&mut LocalCluster, &mut C),
    on_before_partition_resolved: impl FnOnce(&mut LocalCluster, &mut C),
    on_partition_resolved: impl FnOnce(&mut LocalCluster, &mut C),
    ticks_per_slot: Option<u64>,
    additional_accounts: Vec<(Pubkey, AccountSharedData)>,
) {
    solana_logger::setup_with_default(RUST_LOG_FILTER);
    info!("PARTITION_TEST!");
    let num_nodes = partitions.len();
    let node_stakes: Vec<_> = partitions
        .iter()
        .map(|stake_weight| 100 * *stake_weight as u64)
        .collect();
    assert_eq!(node_stakes.len(), num_nodes);
    let cluster_lamports = node_stakes.iter().sum::<u64>() * 2;
    let turbine_disabled = Arc::new(AtomicBool::new(false));
    let mut validator_config = ValidatorConfig {
        turbine_disabled: Some(turbine_disabled.clone()),
        ..ValidatorConfig::default_for_test()
    };

    let (validator_keys, partition_duration): (Vec<_>, Duration) = {
        if let Some((leader_schedule, validator_keys)) = leader_schedule {
            assert_eq!(validator_keys.len(), num_nodes);
            let num_slots_per_rotation = leader_schedule.num_slots() as u64;
            let fixed_schedule = FixedSchedule {
                leader_schedule: Arc::new(leader_schedule),
            };
            validator_config.fixed_leader_schedule = Some(fixed_schedule);
            (
                validator_keys,
                // partition for the duration of one full iteration of the  leader schedule
                Duration::from_millis(num_slots_per_rotation * clock::DEFAULT_MS_PER_SLOT),
            )
        } else {
            (
                iter::repeat_with(|| Arc::new(Keypair::new()))
                    .take(partitions.len())
                    .collect(),
                Duration::from_secs(10),
            )
        }
    };

    let slots_per_epoch = 2048;
    let mut config = ClusterConfig {
        cluster_lamports,
        node_stakes,
        validator_configs: make_identical_validator_configs(&validator_config, num_nodes),
        validator_keys: Some(
            validator_keys
                .into_iter()
                .zip(iter::repeat_with(|| true))
                .collect(),
        ),
        slots_per_epoch,
        stakers_slot_offset: slots_per_epoch,
        skip_warmup_slots: true,
        additional_accounts,
        ticks_per_slot: ticks_per_slot.unwrap_or(DEFAULT_TICKS_PER_SLOT),
        ..ClusterConfig::default()
    };

    info!(
        "PARTITION_TEST starting cluster with {:?} partitions slots_per_epoch: {}",
        partitions, config.slots_per_epoch,
    );
    let mut cluster = LocalCluster::new(&mut config, SocketAddrSpace::Unspecified);

    info!("PARTITION_TEST spend_and_verify_all_nodes(), ensure all nodes are caught up");
    cluster_tests::spend_and_verify_all_nodes(
        &cluster.entry_point_info,
        &cluster.funding_keypair,
        num_nodes,
        HashSet::new(),
        SocketAddrSpace::Unspecified,
    );

    let cluster_nodes = discover_cluster(
        &cluster.entry_point_info.gossip,
        num_nodes,
        SocketAddrSpace::Unspecified,
    )
    .unwrap();

    // Check epochs have correct number of slots
    info!("PARTITION_TEST sleeping until partition starting condition",);
    for node in &cluster_nodes {
        let node_client = RpcClient::new_socket(node.rpc);
        let epoch_info = node_client.get_epoch_info().unwrap();
        assert_eq!(epoch_info.slots_in_epoch, slots_per_epoch);
    }

    info!("PARTITION_TEST start partition");
    on_partition_start(&mut cluster, &mut context);
    turbine_disabled.store(true, Ordering::Relaxed);

    sleep(partition_duration);

    on_before_partition_resolved(&mut cluster, &mut context);
    info!("PARTITION_TEST remove partition");
    turbine_disabled.store(false, Ordering::Relaxed);

    // Give partitions time to propagate their blocks from during the partition
    // after the partition resolves
    let timeout_duration = Duration::from_secs(10);
    let propagation_duration = partition_duration;
    info!(
        "PARTITION_TEST resolving partition. sleeping {} ms",
        timeout_duration.as_millis()
    );
    sleep(timeout_duration);
    info!(
        "PARTITION_TEST waiting for blocks to propagate after partition {}ms",
        propagation_duration.as_millis()
    );
    sleep(propagation_duration);
    info!("PARTITION_TEST resuming normal operation");
    on_partition_resolved(&mut cluster, &mut context);
}

pub fn test_faulty_node(
    faulty_node_type: BroadcastStageType,
    node_stakes: Vec<u64>,
) -> (LocalCluster, Vec<Arc<Keypair>>) {
    solana_logger::setup_with_default("solana_local_cluster=info");
    let num_nodes = node_stakes.len();

    let error_validator_config = ValidatorConfig {
        broadcast_stage_type: faulty_node_type,
        ..ValidatorConfig::default_for_test()
    };
    let mut validator_configs = Vec::with_capacity(num_nodes);

    // First validator is the bootstrap leader with the malicious broadcast logic.
    validator_configs.push(error_validator_config);
    validator_configs.resize_with(num_nodes, ValidatorConfig::default_for_test);

    let mut validator_keys = Vec::with_capacity(num_nodes);
    validator_keys.resize_with(num_nodes, || (Arc::new(Keypair::new()), true));

    assert_eq!(node_stakes.len(), num_nodes);
    assert_eq!(validator_keys.len(), num_nodes);

    let mut cluster_config = ClusterConfig {
        cluster_lamports: 10_000,
        node_stakes,
        validator_configs,
        validator_keys: Some(validator_keys.clone()),
        skip_warmup_slots: true,
        ..ClusterConfig::default()
    };

    let cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::Unspecified);
    let validator_keys: Vec<Arc<Keypair>> = validator_keys
        .into_iter()
        .map(|(keypair, _)| keypair)
        .collect();

    (cluster, validator_keys)
}

pub fn farf_dir() -> PathBuf {
    std::env::var("FARF_DIR")
        .unwrap_or_else(|_| "farf".to_string())
        .into()
}

pub fn generate_account_paths(num_account_paths: usize) -> (Vec<TempDir>, Vec<PathBuf>) {
    let account_storage_dirs: Vec<TempDir> = (0..num_account_paths)
        .map(|_| tempfile::tempdir_in(farf_dir()).unwrap())
        .collect();
    let account_storage_paths: Vec<_> = account_storage_dirs
        .iter()
        .map(|a| a.path().to_path_buf())
        .collect();
    (account_storage_dirs, account_storage_paths)
}

pub struct SnapshotValidatorConfig {
    pub bank_snapshots_dir: TempDir,
    pub snapshot_archives_dir: TempDir,
    pub account_storage_dirs: Vec<TempDir>,
    pub validator_config: ValidatorConfig,
}

impl SnapshotValidatorConfig {
    pub fn new(
        full_snapshot_archive_interval_slots: Slot,
        incremental_snapshot_archive_interval_slots: Slot,
        accounts_hash_interval_slots: Slot,
        num_account_paths: usize,
    ) -> SnapshotValidatorConfig {
        assert!(accounts_hash_interval_slots > 0);
        assert!(full_snapshot_archive_interval_slots > 0);
        assert!(full_snapshot_archive_interval_slots % accounts_hash_interval_slots == 0);
        if incremental_snapshot_archive_interval_slots != Slot::MAX {
            assert!(incremental_snapshot_archive_interval_slots > 0);
            assert!(
                incremental_snapshot_archive_interval_slots % accounts_hash_interval_slots == 0
            );
            assert!(
                full_snapshot_archive_interval_slots % incremental_snapshot_archive_interval_slots
                    == 0
            );
        }

        // Create the snapshot config
        let _ = fs::create_dir_all(farf_dir());
        let bank_snapshots_dir = tempfile::tempdir_in(farf_dir()).unwrap();
        let snapshot_archives_dir = tempfile::tempdir_in(farf_dir()).unwrap();
        let snapshot_config = SnapshotConfig {
            full_snapshot_archive_interval_slots,
            incremental_snapshot_archive_interval_slots,
            snapshot_archives_dir: snapshot_archives_dir.path().to_path_buf(),
            bank_snapshots_dir: bank_snapshots_dir.path().to_path_buf(),
            maximum_full_snapshot_archives_to_retain: usize::MAX,
            maximum_incremental_snapshot_archives_to_retain: usize::MAX,
            ..SnapshotConfig::default()
        };

        // Create the account paths
        let (account_storage_dirs, account_storage_paths) =
            generate_account_paths(num_account_paths);

        // Create the validator config
        let validator_config = ValidatorConfig {
            snapshot_config: Some(snapshot_config),
            account_paths: account_storage_paths,
            accounts_hash_interval_slots,
            ..ValidatorConfig::default_for_test()
        };

        SnapshotValidatorConfig {
            bank_snapshots_dir,
            snapshot_archives_dir,
            account_storage_dirs,
            validator_config,
        }
    }
}

pub fn setup_snapshot_validator_config(
    snapshot_interval_slots: Slot,
    num_account_paths: usize,
) -> SnapshotValidatorConfig {
    SnapshotValidatorConfig::new(
        snapshot_interval_slots,
        Slot::MAX,
        snapshot_interval_slots,
        num_account_paths,
    )
}

pub fn save_tower(tower_path: &Path, tower: &Tower, node_keypair: &Keypair) {
    let file_tower_storage = FileTowerStorage::new(tower_path.to_path_buf());
    let saved_tower = SavedTower::new(tower, node_keypair).unwrap();
    file_tower_storage
        .store(&SavedTowerVersions::from(saved_tower))
        .unwrap();
}
