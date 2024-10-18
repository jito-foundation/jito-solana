#![allow(clippy::arithmetic_side_effects)]

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;
use {
    agave_validator::{
        admin_rpc_service,
        admin_rpc_service::{load_staked_nodes_overrides, StakedNodesOverrides},
        bootstrap,
        cli::{self, app, warn_for_deprecated_arguments, DefaultArgs},
        dashboard::Dashboard,
        ledger_lockfile, lock_ledger, new_spinner_progress_bar, println_name_value,
        redirect_stderr_to_file,
    },
    clap::{crate_name, value_t, value_t_or_exit, values_t, values_t_or_exit, ArgMatches},
    console::style,
    crossbeam_channel::unbounded,
    log::*,
    rand::{seq::SliceRandom, thread_rng},
    solana_accounts_db::{
        accounts_db::{AccountShrinkThreshold, AccountsDb, AccountsDbConfig, CreateAncientStorage},
        accounts_file::StorageAccess,
        accounts_index::{
            AccountIndex, AccountSecondaryIndexes, AccountSecondaryIndexesIncludeExclude,
            AccountsIndexConfig, IndexLimitMb,
        },
        partitioned_rewards::TestPartitionedEpochRewards,
        utils::{create_all_accounts_run_and_snapshot_dirs, create_and_canonicalize_directories},
    },
    solana_clap_utils::input_parsers::{keypair_of, keypairs_of, pubkey_of, value_of, values_of},
    solana_core::{
        banking_trace::DISABLED_BAKING_TRACE_DIR,
        consensus::tower_storage,
        proxy::{block_engine_stage::BlockEngineConfig, relayer_stage::RelayerConfig},
        system_monitor_service::SystemMonitorService,
        tip_manager::{TipDistributionAccountConfig, TipManagerConfig},
        tpu::DEFAULT_TPU_COALESCE,
        validator::{
            is_snapshot_config_valid, BlockProductionMethod, BlockVerificationMethod, Validator,
            ValidatorConfig, ValidatorStartProgress,
        },
    },
    solana_gossip::{
        cluster_info::{Node, NodeConfig},
        contact_info::ContactInfo,
    },
    solana_ledger::{
        blockstore_cleanup_service::{DEFAULT_MAX_LEDGER_SHREDS, DEFAULT_MIN_MAX_LEDGER_SHREDS},
        blockstore_options::{
            BlockstoreCompressionType, BlockstoreRecoveryMode, LedgerColumnOptions,
            ShredStorageType,
        },
        use_snapshot_archives_at_startup::{self, UseSnapshotArchivesAtStartup},
    },
    solana_perf::recycler::enable_recycler_warming,
    solana_poh::poh_service,
    solana_rpc::{
        rpc::{JsonRpcConfig, RpcBigtableConfig},
        rpc_pubsub_service::PubSubConfig,
    },
    solana_rpc_client::rpc_client::RpcClient,
    solana_rpc_client_api::config::RpcLeaderScheduleConfig,
    solana_runtime::{
        runtime_config::RuntimeConfig,
        snapshot_bank_utils::DISABLED_SNAPSHOT_ARCHIVE_INTERVAL,
        snapshot_config::{SnapshotConfig, SnapshotUsage},
        snapshot_utils::{self, ArchiveFormat, SnapshotVersion},
    },
    solana_runtime_plugin::{
        runtime_plugin_admin_rpc_service,
        runtime_plugin_admin_rpc_service::RuntimePluginAdminRpcRequestMetadata,
    },
    solana_sdk::{
        clock::{Slot, DEFAULT_S_PER_SLOT},
        commitment_config::CommitmentConfig,
        hash::Hash,
        pubkey::Pubkey,
        signature::{read_keypair, Keypair, Signer},
    },
    solana_send_transaction_service::send_transaction_service,
    solana_streamer::socket::SocketAddrSpace,
    solana_tpu_client::tpu_client::DEFAULT_TPU_ENABLE_UDP,
    std::{
        collections::{HashSet, VecDeque},
        env,
        fs::{self, File},
        net::{IpAddr, Ipv4Addr, SocketAddr},
        num::NonZeroUsize,
        path::{Path, PathBuf},
        process::exit,
        str::FromStr,
        sync::{atomic::AtomicBool, Arc, Mutex, RwLock},
        time::{Duration, SystemTime},
    },
    tokio::runtime::Runtime,
};

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[derive(Debug, PartialEq, Eq)]
enum Operation {
    Initialize,
    Run,
}

const MILLIS_PER_SECOND: u64 = 1000;

fn monitor_validator(ledger_path: &Path) {
    let dashboard = Dashboard::new(ledger_path, None, None).unwrap_or_else(|err| {
        println!(
            "Error: Unable to connect to validator at {}: {:?}",
            ledger_path.display(),
            err,
        );
        exit(1);
    });
    dashboard.run(Duration::from_secs(2));
}

fn wait_for_restart_window(
    ledger_path: &Path,
    identity: Option<Pubkey>,
    min_idle_time_in_minutes: usize,
    max_delinquency_percentage: u8,
    skip_new_snapshot_check: bool,
    skip_health_check: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let sleep_interval = Duration::from_secs(5);

    let min_idle_slots = (min_idle_time_in_minutes as f64 * 60. / DEFAULT_S_PER_SLOT) as Slot;

    let admin_client = admin_rpc_service::connect(ledger_path);
    let rpc_addr = admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.rpc_addr().await })
        .map_err(|err| format!("Unable to get validator RPC address: {err}"))?;

    let Some(rpc_client) = rpc_addr.map(RpcClient::new_socket) else {
        return Err("RPC not available".into());
    };

    let my_identity = rpc_client.get_identity()?;
    let identity = identity.unwrap_or(my_identity);
    let monitoring_another_validator = identity != my_identity;
    println_name_value("Identity:", &identity.to_string());
    println_name_value(
        "Minimum Idle Time:",
        &format!("{min_idle_slots} slots (~{min_idle_time_in_minutes} minutes)"),
    );

    println!("Maximum permitted delinquency: {max_delinquency_percentage}%");

    let mut current_epoch = None;
    let mut leader_schedule = VecDeque::new();
    let mut restart_snapshot = None;
    let mut upcoming_idle_windows = vec![]; // Vec<(starting slot, idle window length in slots)>

    let progress_bar = new_spinner_progress_bar();
    let monitor_start_time = SystemTime::now();

    let mut seen_incremential_snapshot = false;
    loop {
        let snapshot_slot_info = rpc_client.get_highest_snapshot_slot().ok();
        let snapshot_slot_info_has_incremential = snapshot_slot_info
            .as_ref()
            .map(|snapshot_slot_info| snapshot_slot_info.incremental.is_some())
            .unwrap_or_default();
        seen_incremential_snapshot |= snapshot_slot_info_has_incremential;

        let epoch_info = rpc_client.get_epoch_info_with_commitment(CommitmentConfig::processed())?;
        let healthy = skip_health_check || rpc_client.get_health().ok().is_some();
        let delinquent_stake_percentage = {
            let vote_accounts = rpc_client.get_vote_accounts()?;
            let current_stake: u64 = vote_accounts
                .current
                .iter()
                .map(|va| va.activated_stake)
                .sum();
            let delinquent_stake: u64 = vote_accounts
                .delinquent
                .iter()
                .map(|va| va.activated_stake)
                .sum();
            let total_stake = current_stake + delinquent_stake;
            delinquent_stake as f64 / total_stake as f64
        };

        if match current_epoch {
            None => true,
            Some(current_epoch) => current_epoch != epoch_info.epoch,
        } {
            progress_bar.set_message(format!(
                "Fetching leader schedule for epoch {}...",
                epoch_info.epoch
            ));
            let first_slot_in_epoch = epoch_info.absolute_slot - epoch_info.slot_index;
            leader_schedule = rpc_client
                .get_leader_schedule_with_config(
                    Some(first_slot_in_epoch),
                    RpcLeaderScheduleConfig {
                        identity: Some(identity.to_string()),
                        ..RpcLeaderScheduleConfig::default()
                    },
                )?
                .ok_or_else(|| {
                    format!("Unable to get leader schedule from slot {first_slot_in_epoch}")
                })?
                .get(&identity.to_string())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(|slot_index| first_slot_in_epoch.saturating_add(slot_index as u64))
                .filter(|slot| *slot > epoch_info.absolute_slot)
                .collect::<VecDeque<_>>();

            upcoming_idle_windows.clear();
            {
                let mut leader_schedule = leader_schedule.clone();
                let mut max_idle_window = 0;

                let mut idle_window_start_slot = epoch_info.absolute_slot;
                while let Some(next_leader_slot) = leader_schedule.pop_front() {
                    let idle_window = next_leader_slot - idle_window_start_slot;
                    max_idle_window = max_idle_window.max(idle_window);
                    if idle_window > min_idle_slots {
                        upcoming_idle_windows.push((idle_window_start_slot, idle_window));
                    }
                    idle_window_start_slot = next_leader_slot;
                }
                if !leader_schedule.is_empty() && upcoming_idle_windows.is_empty() {
                    return Err(format!(
                        "Validator has no idle window of at least {} slots. Largest idle window \
                         for epoch {} is {} slots",
                        min_idle_slots, epoch_info.epoch, max_idle_window
                    )
                    .into());
                }
            }

            current_epoch = Some(epoch_info.epoch);
        }

        let status = {
            if !healthy {
                style("Node is unhealthy").red().to_string()
            } else {
                // Wait until a hole in the leader schedule before restarting the node
                let in_leader_schedule_hole = if epoch_info.slot_index + min_idle_slots
                    > epoch_info.slots_in_epoch
                {
                    Err("Current epoch is almost complete".to_string())
                } else {
                    while leader_schedule
                        .front()
                        .map(|slot| *slot < epoch_info.absolute_slot)
                        .unwrap_or(false)
                    {
                        leader_schedule.pop_front();
                    }
                    while upcoming_idle_windows
                        .first()
                        .map(|(slot, _)| *slot < epoch_info.absolute_slot)
                        .unwrap_or(false)
                    {
                        upcoming_idle_windows.pop();
                    }

                    match leader_schedule.front() {
                        None => {
                            Ok(()) // Validator has no leader slots
                        }
                        Some(next_leader_slot) => {
                            let idle_slots =
                                next_leader_slot.saturating_sub(epoch_info.absolute_slot);
                            if idle_slots >= min_idle_slots {
                                Ok(())
                            } else {
                                Err(match upcoming_idle_windows.first() {
                                    Some((starting_slot, length_in_slots)) => {
                                        format!(
                                            "Next idle window in {} slots, for {} slots",
                                            starting_slot.saturating_sub(epoch_info.absolute_slot),
                                            length_in_slots
                                        )
                                    }
                                    None => format!(
                                        "Validator will be leader soon. Next leader slot is \
                                         {next_leader_slot}"
                                    ),
                                })
                            }
                        }
                    }
                };

                match in_leader_schedule_hole {
                    Ok(_) => {
                        if skip_new_snapshot_check {
                            break; // Restart!
                        }
                        let snapshot_slot = snapshot_slot_info.map(|snapshot_slot_info| {
                            snapshot_slot_info
                                .incremental
                                .unwrap_or(snapshot_slot_info.full)
                        });
                        if restart_snapshot.is_none() {
                            restart_snapshot = snapshot_slot;
                        }
                        if restart_snapshot == snapshot_slot && !monitoring_another_validator {
                            "Waiting for a new snapshot".to_string()
                        } else if delinquent_stake_percentage
                            >= (max_delinquency_percentage as f64 / 100.)
                        {
                            style("Delinquency too high").red().to_string()
                        } else if seen_incremential_snapshot && !snapshot_slot_info_has_incremential
                        {
                            // Restarts using just a full snapshot will put the node significantly
                            // further behind than if an incremental snapshot is also used, as full
                            // snapshots are larger and take much longer to create.
                            //
                            // Therefore if the node just created a new full snapshot, wait a
                            // little longer until it creates the first incremental snapshot for
                            // the full snapshot.
                            "Waiting for incremental snapshot".to_string()
                        } else {
                            break; // Restart!
                        }
                    }
                    Err(why) => style(why).yellow().to_string(),
                }
            }
        };

        progress_bar.set_message(format!(
            "{} | Processed Slot: {} {} | {:.2}% delinquent stake | {}",
            {
                let elapsed =
                    chrono::Duration::from_std(monitor_start_time.elapsed().unwrap()).unwrap();

                format!(
                    "{:02}:{:02}:{:02}",
                    elapsed.num_hours(),
                    elapsed.num_minutes() % 60,
                    elapsed.num_seconds() % 60
                )
            },
            epoch_info.absolute_slot,
            if monitoring_another_validator {
                "".to_string()
            } else {
                format!(
                    "| Full Snapshot Slot: {} | Incremental Snapshot Slot: {}",
                    snapshot_slot_info
                        .as_ref()
                        .map(|snapshot_slot_info| snapshot_slot_info.full.to_string())
                        .unwrap_or_else(|| '-'.to_string()),
                    snapshot_slot_info
                        .as_ref()
                        .and_then(|snapshot_slot_info| snapshot_slot_info
                            .incremental
                            .map(|incremental| incremental.to_string()))
                        .unwrap_or_else(|| '-'.to_string()),
                )
            },
            delinquent_stake_percentage * 100.,
            status
        ));
        std::thread::sleep(sleep_interval);
    }
    drop(progress_bar);
    println!("{}", style("Ready to restart").green());
    Ok(())
}

fn set_repair_whitelist(
    ledger_path: &Path,
    whitelist: Vec<Pubkey>,
) -> Result<(), Box<dyn std::error::Error>> {
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_repair_whitelist(whitelist).await })
        .map_err(|err| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("setRepairWhitelist request failed: {err}"),
            )
        })?;
    Ok(())
}

/// Returns the default fifo shred storage size (include both data and coding
/// shreds) based on the validator config.
fn default_fifo_shred_storage_size(vc: &ValidatorConfig) -> Option<u64> {
    // The max shred size is around 1228 bytes.
    // Here we reserve a little bit more than that to give extra storage for FIFO
    // to prevent it from purging data that have not yet being marked as obsoleted
    // by LedgerCleanupService.
    const RESERVED_BYTES_PER_SHRED: u64 = 1500;
    vc.max_ledger_shreds.map(|max_ledger_shreds| {
        // x2 as we have data shred and coding shred.
        max_ledger_shreds * RESERVED_BYTES_PER_SHRED * 2
    })
}

// This function is duplicated in ledger-tool/src/main.rs...
fn hardforks_of(matches: &ArgMatches<'_>, name: &str) -> Option<Vec<Slot>> {
    if matches.is_present(name) {
        Some(values_t_or_exit!(matches, name, Slot))
    } else {
        None
    }
}

fn validators_set(
    identity_pubkey: &Pubkey,
    matches: &ArgMatches<'_>,
    matches_name: &str,
    arg_name: &str,
) -> Option<HashSet<Pubkey>> {
    if matches.is_present(matches_name) {
        let validators_set: HashSet<_> = values_t_or_exit!(matches, matches_name, Pubkey)
            .into_iter()
            .collect();
        if validators_set.contains(identity_pubkey) {
            eprintln!("The validator's identity pubkey cannot be a {arg_name}: {identity_pubkey}");
            exit(1);
        }
        Some(validators_set)
    } else {
        None
    }
}

fn get_cluster_shred_version(entrypoints: &[SocketAddr]) -> Option<u16> {
    let entrypoints = {
        let mut index: Vec<_> = (0..entrypoints.len()).collect();
        index.shuffle(&mut rand::thread_rng());
        index.into_iter().map(|i| &entrypoints[i])
    };
    for entrypoint in entrypoints {
        match solana_net_utils::get_cluster_shred_version(entrypoint) {
            Err(err) => eprintln!("get_cluster_shred_version failed: {entrypoint}, {err}"),
            Ok(0) => eprintln!("zero shred-version from entrypoint: {entrypoint}"),
            Ok(shred_version) => {
                info!(
                    "obtained shred-version {} from {}",
                    shred_version, entrypoint
                );
                return Some(shred_version);
            }
        }
    }
    None
}

fn configure_banking_trace_dir_byte_limit(
    validator_config: &mut ValidatorConfig,
    matches: &ArgMatches,
) {
    validator_config.banking_trace_dir_byte_limit = if matches.is_present("disable_banking_trace") {
        // disable with an explicit flag; This effectively becomes `opt-out` by resetting to
        // DISABLED_BAKING_TRACE_DIR, while allowing us to specify a default sensible limit in clap
        // configuration for cli help.
        DISABLED_BAKING_TRACE_DIR
    } else {
        // a default value in clap configuration (BANKING_TRACE_DIR_DEFAULT_BYTE_LIMIT) or
        // explicit user-supplied override value
        value_t_or_exit!(matches, "banking_trace_dir_byte_limit", u64)
    };
}

pub fn main() {
    let default_args = DefaultArgs::new();
    let solana_version = solana_version::version!();
    let cli_app = app(solana_version, &default_args);
    let matches = cli_app.get_matches();
    warn_for_deprecated_arguments(&matches);

    let socket_addr_space = SocketAddrSpace::new(matches.is_present("allow_private_addr"));
    let ledger_path = PathBuf::from(matches.value_of("ledger_path").unwrap());

    let operation = match matches.subcommand() {
        ("", _) | ("run", _) => Operation::Run,
        ("set-block-engine-config", Some(subcommand_matches)) => {
            let block_engine_url = value_t_or_exit!(subcommand_matches, "block_engine_url", String);
            let trust_packets = subcommand_matches.is_present("trust_block_engine_packets");
            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move {
                    admin_client
                        .await?
                        .set_block_engine_config(block_engine_url, trust_packets)
                        .await
                })
                .unwrap_or_else(|err| {
                    println!("set block engine config failed: {}", err);
                    exit(1);
                });
            return;
        }
        ("set-relayer-config", Some(subcommand_matches)) => {
            let relayer_url = value_t_or_exit!(subcommand_matches, "relayer_url", String);
            let trust_packets = subcommand_matches.is_present("trust_relayer_packets");
            let expected_heartbeat_interval_ms: u64 =
                value_of(subcommand_matches, "relayer_expected_heartbeat_interval_ms").unwrap();
            let max_failed_heartbeats: u64 =
                value_of(subcommand_matches, "relayer_max_failed_heartbeats").unwrap();
            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move {
                    admin_client
                        .await?
                        .set_relayer_config(
                            relayer_url,
                            trust_packets,
                            expected_heartbeat_interval_ms,
                            max_failed_heartbeats,
                        )
                        .await
                })
                .unwrap_or_else(|err| {
                    println!("set relayer config failed: {}", err);
                    exit(1);
                });
            return;
        }
        ("set-shred-receiver-address", Some(subcommand_matches)) => {
            let addr = value_t_or_exit!(subcommand_matches, "shred_receiver_address", String);
            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move { admin_client.await?.set_shred_receiver_address(addr).await })
                .unwrap_or_else(|err| {
                    println!("set shred receiver address failed: {}", err);
                    exit(1);
                });
            return;
        }
        ("authorized-voter", Some(authorized_voter_subcommand_matches)) => {
            match authorized_voter_subcommand_matches.subcommand() {
                ("add", Some(subcommand_matches)) => {
                    if let Ok(authorized_voter_keypair) =
                        value_t!(subcommand_matches, "authorized_voter_keypair", String)
                    {
                        let authorized_voter_keypair = fs::canonicalize(&authorized_voter_keypair)
                            .unwrap_or_else(|err| {
                                println!(
                                    "Unable to access path: {authorized_voter_keypair}: {err:?}"
                                );
                                exit(1);
                            });
                        println!(
                            "Adding authorized voter path: {}",
                            authorized_voter_keypair.display()
                        );

                        let admin_client = admin_rpc_service::connect(&ledger_path);
                        admin_rpc_service::runtime()
                            .block_on(async move {
                                admin_client
                                    .await?
                                    .add_authorized_voter(
                                        authorized_voter_keypair.display().to_string(),
                                    )
                                    .await
                            })
                            .unwrap_or_else(|err| {
                                println!("addAuthorizedVoter request failed: {err}");
                                exit(1);
                            });
                    } else {
                        let mut stdin = std::io::stdin();
                        let authorized_voter_keypair =
                            read_keypair(&mut stdin).unwrap_or_else(|err| {
                                println!("Unable to read JSON keypair from stdin: {err:?}");
                                exit(1);
                            });
                        println!(
                            "Adding authorized voter: {}",
                            authorized_voter_keypair.pubkey()
                        );

                        let admin_client = admin_rpc_service::connect(&ledger_path);
                        admin_rpc_service::runtime()
                            .block_on(async move {
                                admin_client
                                    .await?
                                    .add_authorized_voter_from_bytes(Vec::from(
                                        authorized_voter_keypair.to_bytes(),
                                    ))
                                    .await
                            })
                            .unwrap_or_else(|err| {
                                println!("addAuthorizedVoterFromBytes request failed: {err}");
                                exit(1);
                            });
                    }

                    return;
                }
                ("remove-all", _) => {
                    let admin_client = admin_rpc_service::connect(&ledger_path);
                    admin_rpc_service::runtime()
                        .block_on(async move {
                            admin_client.await?.remove_all_authorized_voters().await
                        })
                        .unwrap_or_else(|err| {
                            println!("removeAllAuthorizedVoters request failed: {err}");
                            exit(1);
                        });
                    println!("All authorized voters removed");
                    return;
                }
                _ => unreachable!(),
            }
        }
        ("plugin", Some(plugin_subcommand_matches)) => {
            match plugin_subcommand_matches.subcommand() {
                ("list", _) => {
                    let admin_client = admin_rpc_service::connect(&ledger_path);
                    let plugins = admin_rpc_service::runtime()
                        .block_on(async move { admin_client.await?.list_plugins().await })
                        .unwrap_or_else(|err| {
                            println!("Failed to list plugins: {err}");
                            exit(1);
                        });
                    if !plugins.is_empty() {
                        println!("Currently the following plugins are loaded:");
                        for (plugin, i) in plugins.into_iter().zip(1..) {
                            println!("  {i}) {plugin}");
                        }
                    } else {
                        println!("There are currently no plugins loaded");
                    }
                    return;
                }
                ("unload", Some(subcommand_matches)) => {
                    if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                        let admin_client = admin_rpc_service::connect(&ledger_path);
                        admin_rpc_service::runtime()
                            .block_on(async {
                                admin_client.await?.unload_plugin(name.clone()).await
                            })
                            .unwrap_or_else(|err| {
                                println!("Failed to unload plugin {name}: {err:?}");
                                exit(1);
                            });
                        println!("Successfully unloaded plugin: {name}");
                    }
                    return;
                }
                ("load", Some(subcommand_matches)) => {
                    if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                        let admin_client = admin_rpc_service::connect(&ledger_path);
                        let name = admin_rpc_service::runtime()
                            .block_on(async {
                                admin_client.await?.load_plugin(config.clone()).await
                            })
                            .unwrap_or_else(|err| {
                                println!("Failed to load plugin {config}: {err:?}");
                                exit(1);
                            });
                        println!("Successfully loaded plugin: {name}");
                    }
                    return;
                }
                ("reload", Some(subcommand_matches)) => {
                    if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                        if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                            let admin_client = admin_rpc_service::connect(&ledger_path);
                            admin_rpc_service::runtime()
                                .block_on(async {
                                    admin_client
                                        .await?
                                        .reload_plugin(name.clone(), config.clone())
                                        .await
                                })
                                .unwrap_or_else(|err| {
                                    println!("Failed to reload plugin {name}: {err:?}");
                                    exit(1);
                                });
                            println!("Successfully reloaded plugin: {name}");
                        }
                    }
                    return;
                }
                _ => unreachable!(),
            }
        }
        ("runtime-plugin", Some(plugin_subcommand_matches)) => {
            let runtime_plugin_rpc_client = runtime_plugin_admin_rpc_service::connect(&ledger_path);
            let runtime = Runtime::new().unwrap();
            match plugin_subcommand_matches.subcommand() {
                ("list", _) => {
                    let plugins = runtime
                        .block_on(
                            async move { runtime_plugin_rpc_client.await?.list_plugins().await },
                        )
                        .unwrap_or_else(|err| {
                            println!("Failed to list plugins: {err}");
                            exit(1);
                        });
                    if !plugins.is_empty() {
                        println!("Currently the following plugins are loaded:");
                        for (plugin, i) in plugins.into_iter().zip(1..) {
                            println!("  {i}) {plugin}");
                        }
                    } else {
                        println!("There are currently no plugins loaded");
                    }
                    return;
                }
                ("unload", Some(subcommand_matches)) => {
                    if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                        runtime
                            .block_on(async {
                                runtime_plugin_rpc_client
                                    .await?
                                    .unload_plugin(name.clone())
                                    .await
                            })
                            .unwrap_or_else(|err| {
                                println!("Failed to unload plugin {name}: {err:?}");
                                exit(1);
                            });
                        println!("Successfully unloaded plugin: {name}");
                    }
                    return;
                }
                ("load", Some(subcommand_matches)) => {
                    if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                        let name = runtime
                            .block_on(async {
                                runtime_plugin_rpc_client
                                    .await?
                                    .load_plugin(config.clone())
                                    .await
                            })
                            .unwrap_or_else(|err| {
                                println!("Failed to load plugin {config}: {err:?}");
                                exit(1);
                            });
                        println!("Successfully loaded plugin: {name}");
                    }
                    return;
                }
                ("reload", Some(subcommand_matches)) => {
                    if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                        if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                            println!(
                                "This command does not work as intended on some systems.\
                                To correctly reload an existing plugin make sure to:\
                                    1. Rename the new plugin binary file.\
                                    2. Unload the previous version.\
                                    3. Load the new, renamed binary using the 'Load' command."
                            );
                            runtime
                                .block_on(async {
                                    runtime_plugin_rpc_client
                                        .await?
                                        .reload_plugin(name.clone(), config.clone())
                                        .await
                                })
                                .unwrap_or_else(|err| {
                                    println!("Failed to reload plugin {name}: {err:?}");
                                    exit(1);
                                });
                            println!("Successfully reloaded plugin: {name}");
                        }
                    }
                    return;
                }
                _ => unreachable!(),
            }
        }
        ("contact-info", Some(subcommand_matches)) => {
            let output_mode = subcommand_matches.value_of("output");
            let admin_client = admin_rpc_service::connect(&ledger_path);
            let contact_info = admin_rpc_service::runtime()
                .block_on(async move { admin_client.await?.contact_info().await })
                .unwrap_or_else(|err| {
                    eprintln!("Contact info query failed: {err}");
                    exit(1);
                });
            if let Some(mode) = output_mode {
                match mode {
                    "json" => println!("{}", serde_json::to_string_pretty(&contact_info).unwrap()),
                    "json-compact" => print!("{}", serde_json::to_string(&contact_info).unwrap()),
                    _ => unreachable!(),
                }
            } else {
                print!("{contact_info}");
            }
            return;
        }
        ("init", _) => Operation::Initialize,
        ("exit", Some(subcommand_matches)) => {
            let min_idle_time = value_t_or_exit!(subcommand_matches, "min_idle_time", usize);
            let force = subcommand_matches.is_present("force");
            let monitor = subcommand_matches.is_present("monitor");
            let skip_new_snapshot_check = subcommand_matches.is_present("skip_new_snapshot_check");
            let skip_health_check = subcommand_matches.is_present("skip_health_check");
            let max_delinquent_stake =
                value_t_or_exit!(subcommand_matches, "max_delinquent_stake", u8);

            if !force {
                wait_for_restart_window(
                    &ledger_path,
                    None,
                    min_idle_time,
                    max_delinquent_stake,
                    skip_new_snapshot_check,
                    skip_health_check,
                )
                .unwrap_or_else(|err| {
                    println!("{err}");
                    exit(1);
                });
            }

            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move { admin_client.await?.exit().await })
                .unwrap_or_else(|err| {
                    println!("exit request failed: {err}");
                    exit(1);
                });
            println!("Exit request sent");

            if monitor {
                monitor_validator(&ledger_path);
            }
            return;
        }
        ("monitor", _) => {
            monitor_validator(&ledger_path);
            return;
        }
        ("staked-nodes-overrides", Some(subcommand_matches)) => {
            if !subcommand_matches.is_present("path") {
                println!(
                    "staked-nodes-overrides requires argument of location of the configuration"
                );
                exit(1);
            }

            let path = subcommand_matches.value_of("path").unwrap();

            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move {
                    admin_client
                        .await?
                        .set_staked_nodes_overrides(path.to_string())
                        .await
                })
                .unwrap_or_else(|err| {
                    println!("setStakedNodesOverrides request failed: {err}");
                    exit(1);
                });
            return;
        }
        ("set-identity", Some(subcommand_matches)) => {
            let require_tower = subcommand_matches.is_present("require_tower");

            if let Ok(identity_keypair) = value_t!(subcommand_matches, "identity", String) {
                let identity_keypair = fs::canonicalize(&identity_keypair).unwrap_or_else(|err| {
                    println!("Unable to access path: {identity_keypair}: {err:?}");
                    exit(1);
                });
                println!(
                    "New validator identity path: {}",
                    identity_keypair.display()
                );

                let admin_client = admin_rpc_service::connect(&ledger_path);
                admin_rpc_service::runtime()
                    .block_on(async move {
                        admin_client
                            .await?
                            .set_identity(identity_keypair.display().to_string(), require_tower)
                            .await
                    })
                    .unwrap_or_else(|err| {
                        println!("setIdentity request failed: {err}");
                        exit(1);
                    });
            } else {
                let mut stdin = std::io::stdin();
                let identity_keypair = read_keypair(&mut stdin).unwrap_or_else(|err| {
                    println!("Unable to read JSON keypair from stdin: {err:?}");
                    exit(1);
                });
                println!("New validator identity: {}", identity_keypair.pubkey());

                let admin_client = admin_rpc_service::connect(&ledger_path);
                admin_rpc_service::runtime()
                    .block_on(async move {
                        admin_client
                            .await?
                            .set_identity_from_bytes(
                                Vec::from(identity_keypair.to_bytes()),
                                require_tower,
                            )
                            .await
                    })
                    .unwrap_or_else(|err| {
                        println!("setIdentityFromBytes request failed: {err}");
                        exit(1);
                    });
            };

            return;
        }
        ("set-log-filter", Some(subcommand_matches)) => {
            let filter = value_t_or_exit!(subcommand_matches, "filter", String);
            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move { admin_client.await?.set_log_filter(filter).await })
                .unwrap_or_else(|err| {
                    println!("set log filter failed: {err}");
                    exit(1);
                });
            return;
        }
        ("wait-for-restart-window", Some(subcommand_matches)) => {
            let min_idle_time = value_t_or_exit!(subcommand_matches, "min_idle_time", usize);
            let identity = pubkey_of(subcommand_matches, "identity");
            let max_delinquent_stake =
                value_t_or_exit!(subcommand_matches, "max_delinquent_stake", u8);
            let skip_new_snapshot_check = subcommand_matches.is_present("skip_new_snapshot_check");
            let skip_health_check = subcommand_matches.is_present("skip_health_check");

            wait_for_restart_window(
                &ledger_path,
                identity,
                min_idle_time,
                max_delinquent_stake,
                skip_new_snapshot_check,
                skip_health_check,
            )
            .unwrap_or_else(|err| {
                println!("{err}");
                exit(1);
            });
            return;
        }
        ("repair-shred-from-peer", Some(subcommand_matches)) => {
            let pubkey = value_t!(subcommand_matches, "pubkey", Pubkey).ok();
            let slot = value_t_or_exit!(subcommand_matches, "slot", u64);
            let shred_index = value_t_or_exit!(subcommand_matches, "shred", u64);
            let admin_client = admin_rpc_service::connect(&ledger_path);
            admin_rpc_service::runtime()
                .block_on(async move {
                    admin_client
                        .await?
                        .repair_shred_from_peer(pubkey, slot, shred_index)
                        .await
                })
                .unwrap_or_else(|err| {
                    println!("repair shred from peer failed: {err}");
                    exit(1);
                });
            return;
        }
        ("repair-whitelist", Some(repair_whitelist_subcommand_matches)) => {
            match repair_whitelist_subcommand_matches.subcommand() {
                ("get", Some(subcommand_matches)) => {
                    let output_mode = subcommand_matches.value_of("output");
                    let admin_client = admin_rpc_service::connect(&ledger_path);
                    let repair_whitelist = admin_rpc_service::runtime()
                        .block_on(async move { admin_client.await?.repair_whitelist().await })
                        .unwrap_or_else(|err| {
                            eprintln!("Repair whitelist query failed: {err}");
                            exit(1);
                        });
                    if let Some(mode) = output_mode {
                        match mode {
                            "json" => println!(
                                "{}",
                                serde_json::to_string_pretty(&repair_whitelist).unwrap()
                            ),
                            "json-compact" => {
                                print!("{}", serde_json::to_string(&repair_whitelist).unwrap())
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        print!("{repair_whitelist}");
                    }
                    return;
                }
                ("set", Some(subcommand_matches)) => {
                    let whitelist = if subcommand_matches.is_present("whitelist") {
                        let validators_set: HashSet<_> =
                            values_t_or_exit!(subcommand_matches, "whitelist", Pubkey)
                                .into_iter()
                                .collect();
                        validators_set.into_iter().collect::<Vec<_>>()
                    } else {
                        return;
                    };
                    set_repair_whitelist(&ledger_path, whitelist).unwrap_or_else(|err| {
                        eprintln!("{err}");
                        exit(1);
                    });
                    return;
                }
                ("remove-all", _) => {
                    set_repair_whitelist(&ledger_path, Vec::default()).unwrap_or_else(|err| {
                        eprintln!("{err}");
                        exit(1);
                    });
                    return;
                }
                _ => unreachable!(),
            }
        }
        ("set-public-address", Some(subcommand_matches)) => {
            let parse_arg_addr = |arg_name: &str, arg_long: &str| -> Option<SocketAddr> {
                subcommand_matches.value_of(arg_name).map(|host_port| {
                    solana_net_utils::parse_host_port(host_port).unwrap_or_else(|err| {
                        eprintln!(
                            "Failed to parse --{arg_long} address. It must be in the HOST:PORT \
                             format. {err}"
                        );
                        exit(1);
                    })
                })
            };
            let tpu_addr = parse_arg_addr("tpu_addr", "tpu");
            let tpu_forwards_addr = parse_arg_addr("tpu_forwards_addr", "tpu-forwards");

            macro_rules! set_public_address {
                ($public_addr:expr, $set_public_address:ident, $request:literal) => {
                    if let Some(public_addr) = $public_addr {
                        let admin_client = admin_rpc_service::connect(&ledger_path);
                        admin_rpc_service::runtime()
                            .block_on(async move {
                                admin_client.await?.$set_public_address(public_addr).await
                            })
                            .unwrap_or_else(|err| {
                                eprintln!("{} request failed: {err}", $request);
                                exit(1);
                            });
                    }
                };
            }
            set_public_address!(tpu_addr, set_public_tpu_address, "setPublicTpuAddress");
            set_public_address!(
                tpu_forwards_addr,
                set_public_tpu_forwards_address,
                "setPublicTpuForwardsAddress"
            );
            return;
        }
        _ => unreachable!(),
    };

    let identity_keypair = keypair_of(&matches, "identity").unwrap_or_else(|| {
        clap::Error::with_description(
            "The --identity <KEYPAIR> argument is required",
            clap::ErrorKind::ArgumentNotFound,
        )
        .exit();
    });

    let logfile = {
        let logfile = matches
            .value_of("logfile")
            .map(|s| s.into())
            .unwrap_or_else(|| format!("agave-validator-{}.log", identity_keypair.pubkey()));

        if logfile == "-" {
            None
        } else {
            println!("log file: {logfile}");
            Some(logfile)
        }
    };
    let use_progress_bar = logfile.is_none();
    let _logger_thread = redirect_stderr_to_file(logfile);

    info!("{} {}", crate_name!(), solana_version);
    info!("Starting validator with: {:#?}", std::env::args_os());

    let cuda = matches.is_present("cuda");
    if cuda {
        solana_perf::perf_libs::init_cuda();
        enable_recycler_warming();
    }

    solana_core::validator::report_target_features();

    let authorized_voter_keypairs = keypairs_of(&matches, "authorized_voter_keypairs")
        .map(|keypairs| keypairs.into_iter().map(Arc::new).collect())
        .unwrap_or_else(|| {
            vec![Arc::new(
                keypair_of(&matches, "identity").expect("identity"),
            )]
        });
    let authorized_voter_keypairs = Arc::new(RwLock::new(authorized_voter_keypairs));

    let staked_nodes_overrides_path = matches
        .value_of("staked_nodes_overrides")
        .map(str::to_string);
    let staked_nodes_overrides = Arc::new(RwLock::new(
        match &staked_nodes_overrides_path {
            None => StakedNodesOverrides::default(),
            Some(p) => load_staked_nodes_overrides(p).unwrap_or_else(|err| {
                error!("Failed to load stake-nodes-overrides from {}: {}", p, err);
                clap::Error::with_description(
                    "Failed to load configuration of stake-nodes-overrides argument",
                    clap::ErrorKind::InvalidValue,
                )
                .exit()
            }),
        }
        .staked_map_id,
    ));

    let init_complete_file = matches.value_of("init_complete_file");

    let rpc_bootstrap_config = bootstrap::RpcBootstrapConfig {
        no_genesis_fetch: matches.is_present("no_genesis_fetch"),
        no_snapshot_fetch: matches.is_present("no_snapshot_fetch"),
        check_vote_account: matches
            .value_of("check_vote_account")
            .map(|url| url.to_string()),
        only_known_rpc: matches.is_present("only_known_rpc"),
        max_genesis_archive_unpacked_size: value_t_or_exit!(
            matches,
            "max_genesis_archive_unpacked_size",
            u64
        ),
        incremental_snapshot_fetch: !matches.is_present("no_incremental_snapshots"),
    };

    let private_rpc = matches.is_present("private_rpc");
    let do_port_check = !matches.is_present("no_port_check");
    let tpu_coalesce = value_t!(matches, "tpu_coalesce_ms", u64)
        .map(Duration::from_millis)
        .unwrap_or(DEFAULT_TPU_COALESCE);
    let wal_recovery_mode = matches
        .value_of("wal_recovery_mode")
        .map(BlockstoreRecoveryMode::from);

    // Canonicalize ledger path to avoid issues with symlink creation
    let ledger_path = create_and_canonicalize_directories([&ledger_path])
        .unwrap_or_else(|err| {
            eprintln!(
                "Unable to access ledger path '{}': {err}",
                ledger_path.display(),
            );
            exit(1);
        })
        .pop()
        .unwrap();

    let accounts_hash_cache_path = matches
        .value_of("accounts_hash_cache_path")
        .map(Into::into)
        .unwrap_or_else(|| ledger_path.join(AccountsDb::DEFAULT_ACCOUNTS_HASH_CACHE_DIR));
    let accounts_hash_cache_path = create_and_canonicalize_directories([&accounts_hash_cache_path])
        .unwrap_or_else(|err| {
            eprintln!(
                "Unable to access accounts hash cache path '{}': {err}",
                accounts_hash_cache_path.display(),
            );
            exit(1);
        })
        .pop()
        .unwrap();

    let debug_keys: Option<Arc<HashSet<_>>> = if matches.is_present("debug_key") {
        Some(Arc::new(
            values_t_or_exit!(matches, "debug_key", Pubkey)
                .into_iter()
                .collect(),
        ))
    } else {
        None
    };

    let known_validators = validators_set(
        &identity_keypair.pubkey(),
        &matches,
        "known_validators",
        "--known-validator",
    );
    let repair_validators = validators_set(
        &identity_keypair.pubkey(),
        &matches,
        "repair_validators",
        "--repair-validator",
    );
    let repair_whitelist = validators_set(
        &identity_keypair.pubkey(),
        &matches,
        "repair_whitelist",
        "--repair-whitelist",
    );
    let repair_whitelist = Arc::new(RwLock::new(repair_whitelist.unwrap_or_default()));
    let gossip_validators = validators_set(
        &identity_keypair.pubkey(),
        &matches,
        "gossip_validators",
        "--gossip-validator",
    );

    let bind_address = solana_net_utils::parse_host(matches.value_of("bind_address").unwrap())
        .expect("invalid bind_address");
    let rpc_bind_address = if matches.is_present("rpc_bind_address") {
        solana_net_utils::parse_host(matches.value_of("rpc_bind_address").unwrap())
            .expect("invalid rpc_bind_address")
    } else if private_rpc {
        solana_net_utils::parse_host("127.0.0.1").unwrap()
    } else {
        bind_address
    };

    let contact_debug_interval = value_t_or_exit!(matches, "contact_debug_interval", u64);

    let account_indexes = process_account_indexes(&matches);

    let restricted_repair_only_mode = matches.is_present("restricted_repair_only_mode");
    let accounts_shrink_optimize_total_space =
        value_t_or_exit!(matches, "accounts_shrink_optimize_total_space", bool);
    let tpu_use_quic = !matches.is_present("tpu_disable_quic");
    let tpu_enable_udp = if matches.is_present("tpu_enable_udp") {
        true
    } else {
        DEFAULT_TPU_ENABLE_UDP
    };

    let tpu_connection_pool_size = value_t_or_exit!(matches, "tpu_connection_pool_size", usize);
    let tpu_max_connections_per_ipaddr_per_minute =
        value_t_or_exit!(matches, "tpu_max_connections_per_ipaddr_per_minute", u64);

    let shrink_ratio = value_t_or_exit!(matches, "accounts_shrink_ratio", f64);
    if !(0.0..=1.0).contains(&shrink_ratio) {
        eprintln!(
            "The specified account-shrink-ratio is invalid, it must be between 0. and 1.0 \
             inclusive: {shrink_ratio}"
        );
        exit(1);
    }

    let accounts_shrink_ratio = if accounts_shrink_optimize_total_space {
        AccountShrinkThreshold::TotalSpace { shrink_ratio }
    } else {
        AccountShrinkThreshold::IndividualStore { shrink_ratio }
    };
    let entrypoint_addrs = values_t!(matches, "entrypoint", String)
        .unwrap_or_default()
        .into_iter()
        .map(|entrypoint| {
            solana_net_utils::parse_host_port(&entrypoint).unwrap_or_else(|e| {
                eprintln!("failed to parse entrypoint address: {e}");
                exit(1);
            })
        })
        .collect::<HashSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for addr in &entrypoint_addrs {
        if !socket_addr_space.check(addr) {
            eprintln!("invalid entrypoint address: {addr}");
            exit(1);
        }
    }
    // TODO: Once entrypoints are updated to return shred-version, this should
    // abort if it fails to obtain a shred-version, so that nodes always join
    // gossip with a valid shred-version. The code to adopt entrypoint shred
    // version can then be deleted from gossip and get_rpc_node above.
    let expected_shred_version = value_t!(matches, "expected_shred_version", u16)
        .ok()
        .or_else(|| get_cluster_shred_version(&entrypoint_addrs));

    let tower_storage: Arc<dyn tower_storage::TowerStorage> =
        match value_t_or_exit!(matches, "tower_storage", String).as_str() {
            "file" => {
                let tower_path = value_t!(matches, "tower", PathBuf)
                    .ok()
                    .unwrap_or_else(|| ledger_path.clone());

                Arc::new(tower_storage::FileTowerStorage::new(tower_path))
            }
            "etcd" => {
                let endpoints = values_t_or_exit!(matches, "etcd_endpoint", String);
                let domain_name = value_t_or_exit!(matches, "etcd_domain_name", String);
                let ca_certificate_file = value_t_or_exit!(matches, "etcd_cacert_file", String);
                let identity_certificate_file = value_t_or_exit!(matches, "etcd_cert_file", String);
                let identity_private_key_file = value_t_or_exit!(matches, "etcd_key_file", String);

                let read = |file| {
                    fs::read(&file).unwrap_or_else(|err| {
                        eprintln!("Unable to read {file}: {err}");
                        exit(1)
                    })
                };

                let tls_config = tower_storage::EtcdTlsConfig {
                    domain_name,
                    ca_certificate: read(ca_certificate_file),
                    identity_certificate: read(identity_certificate_file),
                    identity_private_key: read(identity_private_key_file),
                };

                Arc::new(
                    tower_storage::EtcdTowerStorage::new(endpoints, Some(tls_config))
                        .unwrap_or_else(|err| {
                            eprintln!("Failed to connect to etcd: {err}");
                            exit(1);
                        }),
                )
            }
            _ => unreachable!(),
        };

    let mut accounts_index_config = AccountsIndexConfig {
        started_from_validator: true, // this is the only place this is set
        ..AccountsIndexConfig::default()
    };
    if let Ok(bins) = value_t!(matches, "accounts_index_bins", usize) {
        accounts_index_config.bins = Some(bins);
    }

    let test_partitioned_epoch_rewards =
        if matches.is_present("partitioned_epoch_rewards_compare_calculation") {
            TestPartitionedEpochRewards::CompareResults
        } else if matches.is_present("partitioned_epoch_rewards_force_enable_single_slot") {
            TestPartitionedEpochRewards::ForcePartitionedEpochRewardsInOneBlock
        } else {
            TestPartitionedEpochRewards::None
        };

    accounts_index_config.index_limit_mb =
        if let Ok(limit) = value_t!(matches, "accounts_index_memory_limit_mb", usize) {
            IndexLimitMb::Limit(limit)
        } else if matches.is_present("disable_accounts_disk_index") {
            IndexLimitMb::InMemOnly
        } else {
            IndexLimitMb::Unspecified
        };

    {
        let mut accounts_index_paths: Vec<PathBuf> = if matches.is_present("accounts_index_path") {
            values_t_or_exit!(matches, "accounts_index_path", String)
                .into_iter()
                .map(PathBuf::from)
                .collect()
        } else {
            vec![]
        };
        if accounts_index_paths.is_empty() {
            accounts_index_paths = vec![ledger_path.join("accounts_index")];
        }
        accounts_index_config.drives = Some(accounts_index_paths);
    }

    const MB: usize = 1_024 * 1_024;
    accounts_index_config.scan_results_limit_bytes =
        value_t!(matches, "accounts_index_scan_results_limit_mb", usize)
            .ok()
            .map(|mb| mb * MB);

    let account_shrink_paths: Option<Vec<PathBuf>> =
        values_t!(matches, "account_shrink_path", String)
            .map(|shrink_paths| shrink_paths.into_iter().map(PathBuf::from).collect())
            .ok();
    let account_shrink_paths = account_shrink_paths.as_ref().map(|paths| {
        create_and_canonicalize_directories(paths).unwrap_or_else(|err| {
            eprintln!("Unable to access account shrink path: {err}");
            exit(1);
        })
    });
    let (account_shrink_run_paths, account_shrink_snapshot_paths) = account_shrink_paths
        .map(|paths| {
            create_all_accounts_run_and_snapshot_dirs(&paths).unwrap_or_else(|err| {
                eprintln!("Error: {err}");
                exit(1);
            })
        })
        .unzip();

    let read_cache_limit_bytes = values_of::<usize>(&matches, "accounts_db_read_cache_limit_mb")
        .map(|limits| {
            match limits.len() {
                // we were given explicit low and high watermark values, so use them
                2 => (limits[0] * MB, limits[1] * MB),
                // we were given a single value, so use it for both low and high watermarks
                1 => (limits[0] * MB, limits[0] * MB),
                _ => {
                    // clap will enforce either one or two values is given
                    unreachable!(
                        "invalid number of values given to accounts-db-read-cache-limit-mb"
                    )
                }
            }
        });
    let create_ancient_storage = matches
        .value_of("accounts_db_squash_storages_method")
        .map(|method| match method {
            "pack" => CreateAncientStorage::Pack,
            "append" => CreateAncientStorage::Append,
            _ => {
                // clap will enforce one of the above values is given
                unreachable!("invalid value given to accounts-db-squash-storages-method")
            }
        })
        .unwrap_or_default();
    let storage_access = matches
        .value_of("accounts_db_access_storages_method")
        .map(|method| match method {
            "mmap" => StorageAccess::Mmap,
            "file" => StorageAccess::File,
            _ => {
                // clap will enforce one of the above values is given
                unreachable!("invalid value given to accounts-db-access-storages-method")
            }
        })
        .unwrap_or_default();

    let accounts_db_config = AccountsDbConfig {
        index: Some(accounts_index_config),
        base_working_path: Some(ledger_path.clone()),
        accounts_hash_cache_path: Some(accounts_hash_cache_path),
        shrink_paths: account_shrink_run_paths,
        read_cache_limit_bytes,
        write_cache_limit_bytes: value_t!(matches, "accounts_db_cache_limit_mb", u64)
            .ok()
            .map(|mb| mb * MB as u64),
        ancient_append_vec_offset: value_t!(matches, "accounts_db_ancient_append_vecs", i64).ok(),
        exhaustively_verify_refcounts: matches.is_present("accounts_db_verify_refcounts"),
        create_ancient_storage,
        test_partitioned_epoch_rewards,
        test_skip_rewrites_but_include_in_bank_hash: matches
            .is_present("accounts_db_test_skip_rewrites"),
        storage_access,
        ..AccountsDbConfig::default()
    };

    let accounts_db_config = Some(accounts_db_config);

    let on_start_geyser_plugin_config_files = if matches.is_present("geyser_plugin_config") {
        Some(
            values_t_or_exit!(matches, "geyser_plugin_config", String)
                .into_iter()
                .map(PathBuf::from)
                .collect(),
        )
    } else {
        None
    };
    let starting_with_geyser_plugins: bool = on_start_geyser_plugin_config_files.is_some();

    let rpc_bigtable_config = if matches.is_present("enable_rpc_bigtable_ledger_storage")
        || matches.is_present("enable_bigtable_ledger_upload")
    {
        Some(RpcBigtableConfig {
            enable_bigtable_ledger_upload: matches.is_present("enable_bigtable_ledger_upload"),
            bigtable_instance_name: value_t_or_exit!(matches, "rpc_bigtable_instance_name", String),
            bigtable_app_profile_id: value_t_or_exit!(
                matches,
                "rpc_bigtable_app_profile_id",
                String
            ),
            timeout: value_t!(matches, "rpc_bigtable_timeout", u64)
                .ok()
                .map(Duration::from_secs),
            max_message_size: value_t_or_exit!(matches, "rpc_bigtable_max_message_size", usize),
        })
    } else {
        None
    };

    let rpc_send_retry_rate_ms = value_t_or_exit!(matches, "rpc_send_transaction_retry_ms", u64);
    let rpc_send_batch_size = value_t_or_exit!(matches, "rpc_send_transaction_batch_size", usize);
    let rpc_send_batch_send_rate_ms =
        value_t_or_exit!(matches, "rpc_send_transaction_batch_ms", u64);

    if rpc_send_batch_send_rate_ms > rpc_send_retry_rate_ms {
        eprintln!(
            "The specified rpc-send-batch-ms ({rpc_send_batch_send_rate_ms}) is invalid, it must \
             be <= rpc-send-retry-ms ({rpc_send_retry_rate_ms})"
        );
        exit(1);
    }

    let tps = rpc_send_batch_size as u64 * MILLIS_PER_SECOND / rpc_send_batch_send_rate_ms;
    if tps > send_transaction_service::MAX_TRANSACTION_SENDS_PER_SECOND {
        eprintln!(
            "Either the specified rpc-send-batch-size ({}) or rpc-send-batch-ms ({}) is invalid, \
             'rpc-send-batch-size * 1000 / rpc-send-batch-ms' must be smaller than ({}) .",
            rpc_send_batch_size,
            rpc_send_batch_send_rate_ms,
            send_transaction_service::MAX_TRANSACTION_SENDS_PER_SECOND
        );
        exit(1);
    }
    let rpc_send_transaction_tpu_peers = matches
        .values_of("rpc_send_transaction_tpu_peer")
        .map(|values| {
            values
                .map(solana_net_utils::parse_host_port)
                .collect::<Result<Vec<SocketAddr>, String>>()
        })
        .transpose()
        .unwrap_or_else(|e| {
            eprintln!("failed to parse rpc send-transaction-service tpu peer address: {e}");
            exit(1);
        });
    let rpc_send_transaction_also_leader = matches.is_present("rpc_send_transaction_also_leader");
    let leader_forward_count =
        if rpc_send_transaction_tpu_peers.is_some() && !rpc_send_transaction_also_leader {
            // rpc-sts is configured to send only to specific tpu peers. disable leader forwards
            0
        } else {
            value_t_or_exit!(matches, "rpc_send_transaction_leader_forward_count", u64)
        };

    let full_api = matches.is_present("full_rpc_api");

    let cli::thread_args::NumThreadConfig {
        ip_echo_server_threads,
        replay_forks_threads,
        replay_transactions_threads,
        tvu_receive_threads,
    } = cli::thread_args::parse_num_threads_args(&matches);

    let voting_disabled = matches.is_present("no_voting") || restricted_repair_only_mode;
    let tip_manager_config = tip_manager_config_from_matches(&matches, voting_disabled);

    let block_engine_config = BlockEngineConfig {
        block_engine_url: if matches.is_present("block_engine_url") {
            value_of(&matches, "block_engine_url").expect("couldn't parse block_engine_url")
        } else {
            "".to_string()
        },
        trust_packets: matches.is_present("trust_block_engine_packets"),
    };

    // Defaults are set in cli definition, safe to use unwrap() here
    let expected_heartbeat_interval_ms: u64 =
        value_of(&matches, "relayer_expected_heartbeat_interval_ms").unwrap();
    assert!(
        expected_heartbeat_interval_ms > 0,
        "relayer-max-failed-heartbeats must be greater than zero"
    );
    let max_failed_heartbeats: u64 = value_of(&matches, "relayer_max_failed_heartbeats").unwrap();
    assert!(
        max_failed_heartbeats > 0,
        "relayer-max-failed-heartbeats must be greater than zero"
    );

    let relayer_config = RelayerConfig {
        relayer_url: if matches.is_present("relayer_url") {
            value_of(&matches, "relayer_url").expect("couldn't parse relayer_url")
        } else {
            "".to_string()
        },
        expected_heartbeat_interval: Duration::from_millis(expected_heartbeat_interval_ms),
        oldest_allowed_heartbeat: Duration::from_millis(
            max_failed_heartbeats * expected_heartbeat_interval_ms,
        ),
        trust_packets: matches.is_present("trust_relayer_packets"),
    };

    let mut validator_config = ValidatorConfig {
        require_tower: matches.is_present("require_tower"),
        tower_storage,
        halt_at_slot: value_t!(matches, "dev_halt_at_slot", Slot).ok(),
        expected_genesis_hash: matches
            .value_of("expected_genesis_hash")
            .map(|s| Hash::from_str(s).unwrap()),
        expected_bank_hash: matches
            .value_of("expected_bank_hash")
            .map(|s| Hash::from_str(s).unwrap()),
        expected_shred_version,
        new_hard_forks: hardforks_of(&matches, "hard_forks"),
        rpc_config: JsonRpcConfig {
            enable_rpc_transaction_history: matches.is_present("enable_rpc_transaction_history"),
            enable_extended_tx_metadata_storage: matches.is_present("enable_cpi_and_log_storage")
                || matches.is_present("enable_extended_tx_metadata_storage"),
            rpc_bigtable_config,
            faucet_addr: matches.value_of("rpc_faucet_addr").map(|address| {
                solana_net_utils::parse_host_port(address).expect("failed to parse faucet address")
            }),
            full_api,
            max_multiple_accounts: Some(value_t_or_exit!(
                matches,
                "rpc_max_multiple_accounts",
                usize
            )),
            health_check_slot_distance: value_t_or_exit!(
                matches,
                "health_check_slot_distance",
                u64
            ),
            disable_health_check: false,
            rpc_threads: value_t_or_exit!(matches, "rpc_threads", usize),
            rpc_niceness_adj: value_t_or_exit!(matches, "rpc_niceness_adj", i8),
            account_indexes: account_indexes.clone(),
            rpc_scan_and_fix_roots: matches.is_present("rpc_scan_and_fix_roots"),
            max_request_body_size: Some(value_t_or_exit!(
                matches,
                "rpc_max_request_body_size",
                usize
            )),
        },
        on_start_geyser_plugin_config_files,
        rpc_addrs: value_t!(matches, "rpc_port", u16).ok().map(|rpc_port| {
            (
                SocketAddr::new(rpc_bind_address, rpc_port),
                SocketAddr::new(rpc_bind_address, rpc_port + 1),
                // If additional ports are added, +2 needs to be skipped to avoid a conflict with
                // the websocket port (which is +2) in web3.js This odd port shifting is tracked at
                // https://github.com/solana-labs/solana/issues/12250
            )
        }),
        pubsub_config: PubSubConfig {
            enable_block_subscription: matches.is_present("rpc_pubsub_enable_block_subscription"),
            enable_vote_subscription: matches.is_present("rpc_pubsub_enable_vote_subscription"),
            max_active_subscriptions: value_t_or_exit!(
                matches,
                "rpc_pubsub_max_active_subscriptions",
                usize
            ),
            queue_capacity_items: value_t_or_exit!(
                matches,
                "rpc_pubsub_queue_capacity_items",
                usize
            ),
            queue_capacity_bytes: value_t_or_exit!(
                matches,
                "rpc_pubsub_queue_capacity_bytes",
                usize
            ),
            worker_threads: value_t_or_exit!(matches, "rpc_pubsub_worker_threads", usize),
            notification_threads: value_t!(matches, "rpc_pubsub_notification_threads", usize)
                .ok()
                .and_then(NonZeroUsize::new),
        },
        voting_disabled: matches.is_present("no_voting") || restricted_repair_only_mode,
        wait_for_supermajority: value_t!(matches, "wait_for_supermajority", Slot).ok(),
        known_validators,
        repair_validators,
        repair_whitelist,
        gossip_validators,
        wal_recovery_mode,
        run_verification: !(matches.is_present("skip_poh_verify")
            || matches.is_present("skip_startup_ledger_verification")),
        debug_keys,
        contact_debug_interval,
        send_transaction_service_config: send_transaction_service::Config {
            retry_rate_ms: rpc_send_retry_rate_ms,
            leader_forward_count,
            default_max_retries: value_t!(
                matches,
                "rpc_send_transaction_default_max_retries",
                usize
            )
            .ok(),
            service_max_retries: value_t_or_exit!(
                matches,
                "rpc_send_transaction_service_max_retries",
                usize
            ),
            batch_send_rate_ms: rpc_send_batch_send_rate_ms,
            batch_size: rpc_send_batch_size,
            retry_pool_max_size: value_t_or_exit!(
                matches,
                "rpc_send_transaction_retry_pool_max_size",
                usize
            ),
            tpu_peers: rpc_send_transaction_tpu_peers,
        },
        no_poh_speed_test: matches.is_present("no_poh_speed_test"),
        no_os_memory_stats_reporting: matches.is_present("no_os_memory_stats_reporting"),
        no_os_network_stats_reporting: matches.is_present("no_os_network_stats_reporting"),
        no_os_cpu_stats_reporting: matches.is_present("no_os_cpu_stats_reporting"),
        no_os_disk_stats_reporting: matches.is_present("no_os_disk_stats_reporting"),
        poh_pinned_cpu_core: value_of(&matches, "poh_pinned_cpu_core")
            .unwrap_or(poh_service::DEFAULT_PINNED_CPU_CORE),
        poh_hashes_per_batch: value_of(&matches, "poh_hashes_per_batch")
            .unwrap_or(poh_service::DEFAULT_HASHES_PER_BATCH),
        process_ledger_before_services: matches.is_present("process_ledger_before_services"),
        account_indexes,
        accounts_db_test_hash_calculation: matches.is_present("accounts_db_test_hash_calculation"),
        accounts_db_config,
        accounts_db_skip_shrink: true,
        accounts_db_force_initial_clean: matches.is_present("no_skip_initial_accounts_db_clean"),
        tpu_coalesce,
        no_wait_for_vote_to_start_leader: matches.is_present("no_wait_for_vote_to_start_leader"),
        accounts_shrink_ratio,
        runtime_config: RuntimeConfig {
            log_messages_bytes_limit: value_of(&matches, "log_messages_bytes_limit"),
            ..RuntimeConfig::default()
        },
        relayer_config: Arc::new(Mutex::new(relayer_config)),
        block_engine_config: Arc::new(Mutex::new(block_engine_config)),
        tip_manager_config,
        shred_receiver_address: Arc::new(RwLock::new(
            matches
                .value_of("shred_receiver_address")
                .map(|addr| SocketAddr::from_str(addr).expect("shred_receiver_address invalid")),
        )),
        staked_nodes_overrides: staked_nodes_overrides.clone(),
        use_snapshot_archives_at_startup: value_t_or_exit!(
            matches,
            use_snapshot_archives_at_startup::cli::NAME,
            UseSnapshotArchivesAtStartup
        ),
        ip_echo_server_threads,
        replay_forks_threads,
        replay_transactions_threads,
        delay_leader_block_for_pending_fork: matches
            .is_present("delay_leader_block_for_pending_fork"),
        preallocated_bundle_cost: value_of(&matches, "preallocated_bundle_cost")
            .expect("preallocated_bundle_cost set as default"),
        ..ValidatorConfig::default()
    };

    let vote_account = pubkey_of(&matches, "vote_account").unwrap_or_else(|| {
        if !validator_config.voting_disabled {
            warn!("--vote-account not specified, validator will not vote");
            validator_config.voting_disabled = true;
        }
        Keypair::new().pubkey()
    });

    let dynamic_port_range =
        solana_net_utils::parse_port_range(matches.value_of("dynamic_port_range").unwrap())
            .expect("invalid dynamic_port_range");

    let account_paths: Vec<PathBuf> =
        if let Ok(account_paths) = values_t!(matches, "account_paths", String) {
            account_paths
                .join(",")
                .split(',')
                .map(PathBuf::from)
                .collect()
        } else {
            vec![ledger_path.join("accounts")]
        };
    let account_paths = create_and_canonicalize_directories(account_paths).unwrap_or_else(|err| {
        eprintln!("Unable to access account path: {err}");
        exit(1);
    });

    let (account_run_paths, account_snapshot_paths) =
        create_all_accounts_run_and_snapshot_dirs(&account_paths).unwrap_or_else(|err| {
            eprintln!("Error: {err}");
            exit(1);
        });

    // From now on, use run/ paths in the same way as the previous account_paths.
    validator_config.account_paths = account_run_paths;

    // These snapshot paths are only used for initial clean up, add in shrink paths if they exist.
    validator_config.account_snapshot_paths =
        if let Some(account_shrink_snapshot_paths) = account_shrink_snapshot_paths {
            account_snapshot_paths
                .into_iter()
                .chain(account_shrink_snapshot_paths)
                .collect()
        } else {
            account_snapshot_paths
        };

    let maximum_local_snapshot_age = value_t_or_exit!(matches, "maximum_local_snapshot_age", u64);
    let maximum_full_snapshot_archives_to_retain =
        value_t_or_exit!(matches, "maximum_full_snapshots_to_retain", NonZeroUsize);
    let maximum_incremental_snapshot_archives_to_retain = value_t_or_exit!(
        matches,
        "maximum_incremental_snapshots_to_retain",
        NonZeroUsize
    );
    let snapshot_packager_niceness_adj =
        value_t_or_exit!(matches, "snapshot_packager_niceness_adj", i8);
    let minimal_snapshot_download_speed =
        value_t_or_exit!(matches, "minimal_snapshot_download_speed", f32);
    let maximum_snapshot_download_abort =
        value_t_or_exit!(matches, "maximum_snapshot_download_abort", u64);

    let snapshots_dir = if let Some(snapshots) = matches.value_of("snapshots") {
        Path::new(snapshots)
    } else {
        &ledger_path
    };

    let bank_snapshots_dir = snapshots_dir.join("snapshots");
    fs::create_dir_all(&bank_snapshots_dir).unwrap_or_else(|err| {
        eprintln!(
            "Failed to create bank snapshots directory '{}': {err}",
            bank_snapshots_dir.display(),
        );
        exit(1);
    });

    let full_snapshot_archives_dir = PathBuf::from(
        if let Some(full_snapshot_archive_path) = matches.value_of("full_snapshot_archive_path") {
            Path::new(full_snapshot_archive_path)
        } else {
            snapshots_dir
        },
    );
    fs::create_dir_all(&full_snapshot_archives_dir).unwrap_or_else(|err| {
        eprintln!(
            "Failed to create full snapshot archives directory '{}': {err}",
            full_snapshot_archives_dir.display(),
        );
        exit(1);
    });

    let incremental_snapshot_archives_dir = PathBuf::from(
        if let Some(incremental_snapshot_archive_path) =
            matches.value_of("incremental_snapshot_archive_path")
        {
            Path::new(incremental_snapshot_archive_path)
        } else {
            snapshots_dir
        },
    );
    fs::create_dir_all(&incremental_snapshot_archives_dir).unwrap_or_else(|err| {
        eprintln!(
            "Failed to create incremental snapshot archives directory '{}': {err}",
            incremental_snapshot_archives_dir.display(),
        );
        exit(1);
    });

    let archive_format = {
        let archive_format_str = value_t_or_exit!(matches, "snapshot_archive_format", String);
        ArchiveFormat::from_cli_arg(&archive_format_str)
            .unwrap_or_else(|| panic!("Archive format not recognized: {archive_format_str}"))
    };

    let snapshot_version =
        matches
            .value_of("snapshot_version")
            .map_or(SnapshotVersion::default(), |s| {
                s.parse::<SnapshotVersion>().unwrap_or_else(|err| {
                    eprintln!("Error: {err}");
                    exit(1)
                })
            });

    let (full_snapshot_archive_interval_slots, incremental_snapshot_archive_interval_slots) = match (
        !matches.is_present("no_incremental_snapshots"),
        value_t_or_exit!(matches, "snapshot_interval_slots", u64),
    ) {
        (_, 0) => {
            // snapshots are disabled
            (
                DISABLED_SNAPSHOT_ARCHIVE_INTERVAL,
                DISABLED_SNAPSHOT_ARCHIVE_INTERVAL,
            )
        }
        (true, incremental_snapshot_interval_slots) => {
            // incremental snapshots are enabled
            // use --snapshot-interval-slots for the incremental snapshot interval
            (
                value_t_or_exit!(matches, "full_snapshot_interval_slots", u64),
                incremental_snapshot_interval_slots,
            )
        }
        (false, full_snapshot_interval_slots) => {
            // incremental snapshots are *disabled*
            // use --snapshot-interval-slots for the *full* snapshot interval
            // also warn if --full-snapshot-interval-slots was specified
            if matches.occurrences_of("full_snapshot_interval_slots") > 0 {
                warn!(
                    "Incremental snapshots are disabled, yet --full-snapshot-interval-slots was specified! \
                     Note that --full-snapshot-interval-slots is *ignored* when incremental snapshots are disabled. \
                     Use --snapshot-interval-slots instead.",
                );
            }
            (
                full_snapshot_interval_slots,
                DISABLED_SNAPSHOT_ARCHIVE_INTERVAL,
            )
        }
    };

    validator_config.snapshot_config = SnapshotConfig {
        usage: if full_snapshot_archive_interval_slots == DISABLED_SNAPSHOT_ARCHIVE_INTERVAL {
            SnapshotUsage::LoadOnly
        } else {
            SnapshotUsage::LoadAndGenerate
        },
        full_snapshot_archive_interval_slots,
        incremental_snapshot_archive_interval_slots,
        bank_snapshots_dir,
        full_snapshot_archives_dir: full_snapshot_archives_dir.clone(),
        incremental_snapshot_archives_dir: incremental_snapshot_archives_dir.clone(),
        archive_format,
        snapshot_version,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
        accounts_hash_debug_verify: validator_config.accounts_db_test_hash_calculation,
        packager_thread_niceness_adj: snapshot_packager_niceness_adj,
    };

    // The accounts hash interval shall match the snapshot interval
    validator_config.accounts_hash_interval_slots = std::cmp::min(
        full_snapshot_archive_interval_slots,
        incremental_snapshot_archive_interval_slots,
    );

    info!(
        "Snapshot configuration: full snapshot interval: {} slots, incremental snapshot interval: {} slots",
        if full_snapshot_archive_interval_slots == DISABLED_SNAPSHOT_ARCHIVE_INTERVAL {
            "disabled".to_string()
        } else {
            full_snapshot_archive_interval_slots.to_string()
        },
        if incremental_snapshot_archive_interval_slots == DISABLED_SNAPSHOT_ARCHIVE_INTERVAL {
            "disabled".to_string()
        } else {
            incremental_snapshot_archive_interval_slots.to_string()
        },
    );

    if !is_snapshot_config_valid(
        &validator_config.snapshot_config,
        validator_config.accounts_hash_interval_slots,
    ) {
        eprintln!(
            "Invalid snapshot configuration provided: snapshot intervals are incompatible. \
             \n\t- full snapshot interval MUST be a multiple of incremental snapshot interval \
             (if enabled) \
             \n\t- full snapshot interval MUST be larger than incremental snapshot interval \
             (if enabled)",
        );
        exit(1);
    }

    if matches.is_present("limit_ledger_size") {
        let limit_ledger_size = match matches.value_of("limit_ledger_size") {
            Some(_) => value_t_or_exit!(matches, "limit_ledger_size", u64),
            None => DEFAULT_MAX_LEDGER_SHREDS,
        };
        if limit_ledger_size < DEFAULT_MIN_MAX_LEDGER_SHREDS {
            eprintln!(
                "The provided --limit-ledger-size value was too small, the minimum value is \
                 {DEFAULT_MIN_MAX_LEDGER_SHREDS}"
            );
            exit(1);
        }
        validator_config.max_ledger_shreds = Some(limit_ledger_size);
    }

    configure_banking_trace_dir_byte_limit(&mut validator_config, &matches);
    validator_config.block_verification_method = value_t!(
        matches,
        "block_verification_method",
        BlockVerificationMethod
    )
    .unwrap_or_default();
    validator_config.block_production_method = value_t!(
        matches, // comment to align formatting...
        "block_production_method",
        BlockProductionMethod
    )
    .unwrap_or_default();
    validator_config.enable_block_production_forwarding = staked_nodes_overrides_path.is_some();
    validator_config.unified_scheduler_handler_threads =
        value_t!(matches, "unified_scheduler_handler_threads", usize).ok();

    validator_config.ledger_column_options = LedgerColumnOptions {
        compression_type: match matches.value_of("rocksdb_ledger_compression") {
            None => BlockstoreCompressionType::default(),
            Some(ledger_compression_string) => match ledger_compression_string {
                "none" => BlockstoreCompressionType::None,
                "snappy" => BlockstoreCompressionType::Snappy,
                "lz4" => BlockstoreCompressionType::Lz4,
                "zlib" => BlockstoreCompressionType::Zlib,
                _ => panic!("Unsupported ledger_compression: {ledger_compression_string}"),
            },
        },
        shred_storage_type: match matches.value_of("rocksdb_shred_compaction") {
            None => ShredStorageType::default(),
            Some(shred_compaction_string) => match shred_compaction_string {
                "level" => ShredStorageType::RocksLevel,
                "fifo" => {
                    warn!(
                        "The value \"fifo\" for --rocksdb-shred-compaction has been deprecated. \
                         Use of \"fifo\" will still work for now, but is planned for full removal \
                         in v2.1. To update, use \"level\" for --rocksdb-shred-compaction, or \
                         remove the --rocksdb-shred-compaction argument altogether. Note that the \
                         entire \"rocksdb_fifo\" subdirectory within the ledger directory will \
                         need to be manually removed once the validator is running with \"level\"."
                    );
                    match matches.value_of("rocksdb_fifo_shred_storage_size") {
                        None => ShredStorageType::rocks_fifo(default_fifo_shred_storage_size(
                            &validator_config,
                        )),
                        Some(_) => ShredStorageType::rocks_fifo(Some(value_t_or_exit!(
                            matches,
                            "rocksdb_fifo_shred_storage_size",
                            u64
                        ))),
                    }
                }
                _ => panic!("Unrecognized rocksdb-shred-compaction: {shred_compaction_string}"),
            },
        },
        rocks_perf_sample_interval: value_t_or_exit!(
            matches,
            "rocksdb_perf_sample_interval",
            usize
        ),
    };

    let public_rpc_addr = matches.value_of("public_rpc_addr").map(|addr| {
        solana_net_utils::parse_host_port(addr).unwrap_or_else(|e| {
            eprintln!("failed to parse public rpc address: {e}");
            exit(1);
        })
    });

    if !matches.is_present("no_os_network_limits_test") {
        if SystemMonitorService::check_os_network_limits() {
            info!("OS network limits test passed.");
        } else {
            eprintln!("OS network limit test failed. See: https://docs.solanalabs.com/operations/guides/validator-start#system-tuning");
            exit(1);
        }
    }

    let mut ledger_lock = ledger_lockfile(&ledger_path);
    let _ledger_write_guard = lock_ledger(&ledger_path, &mut ledger_lock);

    let start_progress = Arc::new(RwLock::new(ValidatorStartProgress::default()));
    let admin_service_post_init = Arc::new(RwLock::new(None));
    let (rpc_to_plugin_manager_sender, rpc_to_plugin_manager_receiver) =
        if starting_with_geyser_plugins {
            let (sender, receiver) = unbounded();
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };
    admin_rpc_service::run(
        &ledger_path,
        admin_rpc_service::AdminRpcRequestMetadata {
            rpc_addr: validator_config.rpc_addrs.map(|(rpc_addr, _)| rpc_addr),
            start_time: std::time::SystemTime::now(),
            validator_exit: validator_config.validator_exit.clone(),
            start_progress: start_progress.clone(),
            authorized_voter_keypairs: authorized_voter_keypairs.clone(),
            post_init: admin_service_post_init.clone(),
            tower_storage: validator_config.tower_storage.clone(),
            staked_nodes_overrides,
            rpc_to_plugin_manager_sender,
        },
    );

    let runtime_plugin_config_and_rpc_rx = {
        let plugin_exit = Arc::new(AtomicBool::new(false));
        let (rpc_request_sender, rpc_request_receiver) = unbounded();
        solana_runtime_plugin::runtime_plugin_admin_rpc_service::run(
            &ledger_path,
            RuntimePluginAdminRpcRequestMetadata {
                rpc_request_sender,
                validator_exit: validator_config.validator_exit.clone(),
            },
            plugin_exit,
        );

        if matches.is_present("runtime_plugin_config") {
            (
                values_t_or_exit!(matches, "runtime_plugin_config", String)
                    .into_iter()
                    .map(PathBuf::from)
                    .collect(),
                rpc_request_receiver,
            )
        } else {
            (vec![], rpc_request_receiver)
        }
    };

    let gossip_host: IpAddr = matches
        .value_of("gossip_host")
        .map(|gossip_host| {
            solana_net_utils::parse_host(gossip_host).unwrap_or_else(|err| {
                eprintln!("Failed to parse --gossip-host: {err}");
                exit(1);
            })
        })
        .unwrap_or_else(|| {
            if !entrypoint_addrs.is_empty() {
                let mut order: Vec<_> = (0..entrypoint_addrs.len()).collect();
                order.shuffle(&mut thread_rng());

                let gossip_host = order.into_iter().find_map(|i| {
                    let entrypoint_addr = &entrypoint_addrs[i];
                    info!(
                        "Contacting {} to determine the validator's public IP address",
                        entrypoint_addr
                    );
                    solana_net_utils::get_public_ip_addr(entrypoint_addr).map_or_else(
                        |err| {
                            eprintln!(
                                "Failed to contact cluster entrypoint {entrypoint_addr}: {err}"
                            );
                            None
                        },
                        Some,
                    )
                });

                gossip_host.unwrap_or_else(|| {
                    eprintln!("Unable to determine the validator's public IP address");
                    exit(1);
                })
            } else {
                IpAddr::V4(Ipv4Addr::LOCALHOST)
            }
        });

    let gossip_addr = SocketAddr::new(
        gossip_host,
        value_t!(matches, "gossip_port", u16).unwrap_or_else(|_| {
            solana_net_utils::find_available_port_in_range(bind_address, (0, 1)).unwrap_or_else(
                |err| {
                    eprintln!("Unable to find an available gossip port: {err}");
                    exit(1);
                },
            )
        }),
    );

    let public_tpu_addr = matches.value_of("public_tpu_addr").map(|public_tpu_addr| {
        solana_net_utils::parse_host_port(public_tpu_addr).unwrap_or_else(|err| {
            eprintln!("Failed to parse --public-tpu-address: {err}");
            exit(1);
        })
    });

    let public_tpu_forwards_addr =
        matches
            .value_of("public_tpu_forwards_addr")
            .map(|public_tpu_forwards_addr| {
                solana_net_utils::parse_host_port(public_tpu_forwards_addr).unwrap_or_else(|err| {
                    eprintln!("Failed to parse --public-tpu-forwards-address: {err}");
                    exit(1);
                })
            });

    let node_config = NodeConfig {
        gossip_addr,
        port_range: dynamic_port_range,
        bind_ip_addr: bind_address,
        public_tpu_addr,
        public_tpu_forwards_addr,
        num_tvu_sockets: tvu_receive_threads,
    };

    let cluster_entrypoints = entrypoint_addrs
        .iter()
        .map(ContactInfo::new_gossip_entry_point)
        .collect::<Vec<_>>();

    let mut node = Node::new_with_external_ip(&identity_keypair.pubkey(), node_config);

    if restricted_repair_only_mode {
        // When in --restricted_repair_only_mode is enabled only the gossip and repair ports
        // need to be reachable by the entrypoint to respond to gossip pull requests and repair
        // requests initiated by the node.  All other ports are unused.
        node.info.remove_tpu();
        node.info.remove_tpu_forwards();
        node.info.remove_tvu();
        node.info.remove_serve_repair();

        // A node in this configuration shouldn't be an entrypoint to other nodes
        node.sockets.ip_echo = None;
    }

    if !private_rpc {
        macro_rules! set_socket {
            ($method:ident, $addr:expr, $name:literal) => {
                node.info.$method($addr).expect(&format!(
                    "Operator must spin up node with valid {} address",
                    $name
                ))
            };
        }
        if let Some(public_rpc_addr) = public_rpc_addr {
            set_socket!(set_rpc, public_rpc_addr, "RPC");
            set_socket!(set_rpc_pubsub, public_rpc_addr, "RPC-pubsub");
        } else if let Some((rpc_addr, rpc_pubsub_addr)) = validator_config.rpc_addrs {
            let addr = node
                .info
                .gossip()
                .expect("Operator must spin up node with valid gossip address")
                .ip();
            set_socket!(set_rpc, (addr, rpc_addr.port()), "RPC");
            set_socket!(set_rpc_pubsub, (addr, rpc_pubsub_addr.port()), "RPC-pubsub");
        }
    }

    solana_metrics::set_host_id(identity_keypair.pubkey().to_string());
    solana_metrics::set_panic_hook("validator", Some(String::from(solana_version)));
    solana_entry::entry::init_poh();
    snapshot_utils::remove_tmp_snapshot_archives(&full_snapshot_archives_dir);
    snapshot_utils::remove_tmp_snapshot_archives(&incremental_snapshot_archives_dir);

    let identity_keypair = Arc::new(identity_keypair);

    let should_check_duplicate_instance = true;
    if !cluster_entrypoints.is_empty() {
        bootstrap::rpc_bootstrap(
            &node,
            &identity_keypair,
            &ledger_path,
            &full_snapshot_archives_dir,
            &incremental_snapshot_archives_dir,
            &vote_account,
            authorized_voter_keypairs.clone(),
            &cluster_entrypoints,
            &mut validator_config,
            rpc_bootstrap_config,
            do_port_check,
            use_progress_bar,
            maximum_local_snapshot_age,
            should_check_duplicate_instance,
            &start_progress,
            minimal_snapshot_download_speed,
            maximum_snapshot_download_abort,
            socket_addr_space,
        );
        *start_progress.write().unwrap() = ValidatorStartProgress::Initializing;
    }

    if operation == Operation::Initialize {
        info!("Validator ledger initialization complete");
        return;
    }

    // Bootstrap code above pushes a contact-info with more recent timestamp to
    // gossip. If the node is staked the contact-info lingers in gossip causing
    // false duplicate nodes error.
    // Below line refreshes the timestamp on contact-info so that it overrides
    // the one pushed by bootstrap.
    node.info.hot_swap_pubkey(identity_keypair.pubkey());

    let validator = Validator::new(
        node,
        identity_keypair,
        &ledger_path,
        &vote_account,
        authorized_voter_keypairs,
        cluster_entrypoints,
        &validator_config,
        should_check_duplicate_instance,
        rpc_to_plugin_manager_receiver,
        start_progress,
        socket_addr_space,
        tpu_use_quic,
        tpu_connection_pool_size,
        tpu_enable_udp,
        tpu_max_connections_per_ipaddr_per_minute,
        admin_service_post_init,
        Some(runtime_plugin_config_and_rpc_rx),
    )
    .unwrap_or_else(|e| {
        error!("Failed to start validator: {:?}", e);
        exit(1);
    });

    if let Some(filename) = init_complete_file {
        File::create(filename).unwrap_or_else(|_| {
            error!("Unable to create: {}", filename);
            exit(1);
        });
    }
    info!("Validator initialized");
    validator.join();
    info!("Validator exiting..");
}

fn process_account_indexes(matches: &ArgMatches) -> AccountSecondaryIndexes {
    let account_indexes: HashSet<AccountIndex> = matches
        .values_of("account_indexes")
        .unwrap_or_default()
        .map(|value| match value {
            "program-id" => AccountIndex::ProgramId,
            "spl-token-mint" => AccountIndex::SplTokenMint,
            "spl-token-owner" => AccountIndex::SplTokenOwner,
            _ => unreachable!(),
        })
        .collect();

    let account_indexes_include_keys: HashSet<Pubkey> =
        values_t!(matches, "account_index_include_key", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let account_indexes_exclude_keys: HashSet<Pubkey> =
        values_t!(matches, "account_index_exclude_key", Pubkey)
            .unwrap_or_default()
            .iter()
            .cloned()
            .collect();

    let exclude_keys = !account_indexes_exclude_keys.is_empty();
    let include_keys = !account_indexes_include_keys.is_empty();

    let keys = if !account_indexes.is_empty() && (exclude_keys || include_keys) {
        let account_indexes_keys = AccountSecondaryIndexesIncludeExclude {
            exclude: exclude_keys,
            keys: if exclude_keys {
                account_indexes_exclude_keys
            } else {
                account_indexes_include_keys
            },
        };
        Some(account_indexes_keys)
    } else {
        None
    };

    AccountSecondaryIndexes {
        keys,
        indexes: account_indexes,
    }
}

fn tip_manager_config_from_matches(
    matches: &ArgMatches,
    voting_disabled: bool,
) -> TipManagerConfig {
    TipManagerConfig {
        tip_payment_program_id: pubkey_of(matches, "tip_payment_program_pubkey").unwrap_or_else(
            || {
                if !voting_disabled {
                    panic!("--tip-payment-program-pubkey argument required when validator is voting");
                }
                Pubkey::new_unique()
            },
        ),
        tip_distribution_program_id: pubkey_of(matches, "tip_distribution_program_pubkey")
            .unwrap_or_else(|| {
                if !voting_disabled {
                    panic!("--tip-distribution-program-pubkey argument required when validator is voting");
                }
                Pubkey::new_unique()
            }),
        tip_distribution_account_config: TipDistributionAccountConfig {
            merkle_root_upload_authority: pubkey_of(matches, "merkle_root_upload_authority")
                .unwrap_or_else(|| {
                    if !voting_disabled {
                        panic!("--merkle-root-upload-authority argument required when validator is voting");
                    }
                    Pubkey::new_unique()
                }),
            vote_account: pubkey_of(matches, "vote_account").unwrap_or_else(|| {
                if !voting_disabled {
                    panic!("--vote-account argument required when validator is voting");
                }
                Pubkey::new_unique()
            }),
            commission_bps: value_t!(matches, "commission_bps", u16).unwrap_or_else(|_| {
                if !voting_disabled {
                    panic!("--commission-bps argument required when validator is voting");
                }
                0
            }),
        },
    }
}
