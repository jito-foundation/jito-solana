use {
    clap::{crate_name, value_t, value_t_or_exit, values_t_or_exit, App, Arg},
    crossbeam_channel::unbounded,
    log::*,
    solana_clap_utils::{
        input_parsers::{pubkey_of, pubkeys_of, value_of},
        input_validators::{
            is_parsable, is_pubkey, is_pubkey_or_keypair, is_slot, is_url_or_moniker,
            normalize_to_url_if_moniker,
        },
    },
    solana_client::rpc_client::RpcClient,
    solana_core::tower_storage::FileTowerStorage,
    solana_faucet::faucet::{run_local_faucet_with_port, FAUCET_PORT},
    solana_rpc::{
        rpc::{JsonRpcConfig, RpcBigtableConfig},
        rpc_pubsub_service::PubSubConfig,
    },
    solana_sdk::{
        account::AccountSharedData,
        clock::Slot,
        epoch_schedule::{EpochSchedule, MINIMUM_SLOTS_PER_EPOCH},
        native_token::sol_to_lamports,
        pubkey::Pubkey,
        rent::Rent,
        rpc_port,
        signature::{read_keypair_file, write_keypair_file, Keypair, Signer},
        system_program,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_test_validator::*,
    solana_validator::{
        admin_rpc_service, dashboard::Dashboard, ledger_lockfile, lock_ledger, println_name_value,
        redirect_stderr_to_file,
    },
    std::{
        collections::HashSet,
        fs, io,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        process::exit,
        sync::{Arc, RwLock},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

/* 10,000 was derived empirically by watching the size
 * of the rocksdb/ directory self-limit itself to the
 * 40MB-150MB range when running `solana-test-validator`
 */
const DEFAULT_MAX_LEDGER_SHREDS: u64 = 10_000;

const DEFAULT_FAUCET_SOL: f64 = 1_000_000.;

#[derive(PartialEq)]
enum Output {
    None,
    Log,
    Dashboard,
}

fn main() {
    let default_rpc_port = rpc_port::DEFAULT_RPC_PORT.to_string();
    let default_faucet_port = FAUCET_PORT.to_string();
    let default_limit_ledger_size = DEFAULT_MAX_LEDGER_SHREDS.to_string();
    let default_faucet_sol = DEFAULT_FAUCET_SOL.to_string();

    let matches = App::new("solana-test-validator")
        .about("Test Validator")
        .version(solana_version::version!())
        .arg({
            let arg = Arg::with_name("config_file")
                .short("C")
                .long("config")
                .value_name("PATH")
                .takes_value(true)
                .help("Configuration file to use");
            if let Some(ref config_file) = *solana_cli_config::CONFIG_FILE {
                arg.default_value(config_file)
            } else {
                arg
            }
        })
        .arg(
            Arg::with_name("json_rpc_url")
                .short("u")
                .long("url")
                .value_name("URL_OR_MONIKER")
                .takes_value(true)
                .validator(is_url_or_moniker)
                .help(
                    "URL for Solana's JSON RPC or moniker (or their first letter): \
                   [mainnet-beta, testnet, devnet, localhost]",
                ),
        )
        .arg(
            Arg::with_name("mint_address")
                .long("mint")
                .value_name("PUBKEY")
                .validator(is_pubkey)
                .takes_value(true)
                .help(
                    "Address of the mint account that will receive tokens \
                       created at genesis.  If the ledger already exists then \
                       this parameter is silently ignored [default: client keypair]",
                ),
        )
        .arg(
            Arg::with_name("ledger_path")
                .short("l")
                .long("ledger")
                .value_name("DIR")
                .takes_value(true)
                .required(true)
                .default_value("test-ledger")
                .help("Use DIR as ledger location"),
        )
        .arg(
            Arg::with_name("reset")
                .short("r")
                .long("reset")
                .takes_value(false)
                .help(
                    "Reset the ledger to genesis if it exists. \
                       By default the validator will resume an existing ledger (if present)",
                ),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .takes_value(false)
                .conflicts_with("log")
                .help("Quiet mode: suppress normal output"),
        )
        .arg(
            Arg::with_name("log")
                .long("log")
                .takes_value(false)
                .conflicts_with("quiet")
                .help("Log mode: stream the validator log"),
        )
        .arg(
            Arg::with_name("faucet_port")
                .long("faucet-port")
                .value_name("PORT")
                .takes_value(true)
                .default_value(&default_faucet_port)
                .validator(solana_validator::port_validator)
                .help("Enable the faucet on this port"),
        )
        .arg(
            Arg::with_name("rpc_port")
                .long("rpc-port")
                .value_name("PORT")
                .takes_value(true)
                .default_value(&default_rpc_port)
                .validator(solana_validator::port_validator)
                .help("Enable JSON RPC on this port, and the next port for the RPC websocket"),
        )
        .arg(
            Arg::with_name("enable_rpc_bigtable_ledger_storage")
                .long("enable-rpc-bigtable-ledger-storage")
                .takes_value(false)
                .hidden(true)
                .help("Fetch historical transaction info from a BigTable instance \
                       as a fallback to local ledger data"),
        )
        .arg(
            Arg::with_name("rpc_bigtable_instance")
                .long("rpc-bigtable-instance")
                .value_name("INSTANCE_NAME")
                .takes_value(true)
                .hidden(true)
                .default_value("solana-ledger")
                .help("Name of BigTable instance to target"),
        )
        .arg(
            Arg::with_name("rpc_pubsub_enable_vote_subscription")
                .long("rpc-pubsub-enable-vote-subscription")
                .takes_value(false)
                .help("Enable the unstable RPC PubSub `voteSubscribe` subscription"),
        )
        .arg(
            Arg::with_name("bpf_program")
                .long("bpf-program")
                .value_name("ADDRESS_OR_PATH BPF_PROGRAM.SO")
                .takes_value(true)
                .number_of_values(2)
                .multiple(true)
                .help(
                    "Add a BPF program to the genesis configuration. \
                       If the ledger already exists then this parameter is silently ignored. \
                       First argument can be a public key or path to file that can be parsed as a keypair",
                ),
        )
        .arg(
            Arg::with_name("account")
                .long("account")
                .value_name("ADDRESS FILENAME.JSON")
                .takes_value(true)
                .number_of_values(2)
                .multiple(true)
                .help(
                    "Load an account from the provided JSON file (see `solana account --help` on how to dump \
                        an account to file). Files are searched for relatively to CWD and tests/fixtures. \
                        If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("no_bpf_jit")
                .long("no-bpf-jit")
                .takes_value(false)
                .help("Disable the just-in-time compiler and instead use the interpreter for BPF. Windows always disables JIT."),
        )
        .arg(
            Arg::with_name("ticks_per_slot")
                .long("ticks-per-slot")
                .value_name("TICKS")
                .validator(is_parsable::<u64>)
                .takes_value(true)
                .help("The number of ticks in a slot"),
        )
        .arg(
            Arg::with_name("slots_per_epoch")
                .long("slots-per-epoch")
                .value_name("SLOTS")
                .validator(|value| {
                    value
                        .parse::<Slot>()
                        .map_err(|err| format!("error parsing '{}': {}", value, err))
                        .and_then(|slot| {
                            if slot < MINIMUM_SLOTS_PER_EPOCH {
                                Err(format!("value must be >= {}", MINIMUM_SLOTS_PER_EPOCH))
                            } else {
                                Ok(())
                            }
                        })
                })
                .takes_value(true)
                .help(
                    "Override the number of slots in an epoch. \
                       If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("gossip_port")
                .long("gossip-port")
                .value_name("PORT")
                .takes_value(true)
                .help("Gossip port number for the validator"),
        )
        .arg(
            Arg::with_name("gossip_host")
                .long("gossip-host")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .help(
                    "Gossip DNS name or IP address for the validator to advertise in gossip \
                       [default: 127.0.0.1]",
                ),
        )
        .arg(
            Arg::with_name("dynamic_port_range")
                .long("dynamic-port-range")
                .value_name("MIN_PORT-MAX_PORT")
                .takes_value(true)
                .validator(solana_validator::port_range_validator)
                .help(
                    "Range to use for dynamically assigned ports \
                    [default: 1024-65535]",
                ),
        )
        .arg(
            Arg::with_name("bind_address")
                .long("bind-address")
                .value_name("HOST")
                .takes_value(true)
                .validator(solana_net_utils::is_host)
                .default_value("0.0.0.0")
                .help("IP address to bind the validator ports [default: 0.0.0.0]"),
        )
        .arg(
            Arg::with_name("clone_account")
                .long("clone")
                .short("c")
                .value_name("ADDRESS")
                .takes_value(true)
                .validator(is_pubkey_or_keypair)
                .multiple(true)
                .requires("json_rpc_url")
                .help(
                    "Copy an account from the cluster referenced by the --url argument the \
                     genesis configuration. \
                     If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("maybe_clone_account")
                .long("maybe-clone")
                .value_name("ADDRESS")
                .takes_value(true)
                .validator(is_pubkey_or_keypair)
                .multiple(true)
                .requires("json_rpc_url")
                .help(
                    "Copy an account from the cluster referenced by the --url argument, \
                     skipping it if it doesn't exist. \
                     If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("warp_slot")
                .required(false)
                .long("warp-slot")
                .short("w")
                .takes_value(true)
                .value_name("WARP_SLOT")
                .validator(is_slot)
                .min_values(0)
                .max_values(1)
                .help(
                    "Warp the ledger to WARP_SLOT after starting the validator. \
                        If no slot is provided then the current slot of the cluster \
                        referenced by the --url argument will be used",
                ),
        )
        .arg(
            Arg::with_name("limit_ledger_size")
                .long("limit-ledger-size")
                .value_name("SHRED_COUNT")
                .takes_value(true)
                .default_value(default_limit_ledger_size.as_str())
                .help("Keep this amount of shreds in root slots."),
        )
        .arg(
            Arg::with_name("faucet_sol")
                .long("faucet-sol")
                .takes_value(true)
                .value_name("SOL")
                .default_value(default_faucet_sol.as_str())
                .help(
                    "Give the faucet address this much SOL in genesis. \
                     If the ledger already exists then this parameter is silently ignored",
                ),
        )
        .arg(
            Arg::with_name("geyser_plugin_config")
                .long("geyser-plugin-config")
                .alias("accountsdb-plugin-config")
                .value_name("FILE")
                .takes_value(true)
                .multiple(true)
                .help("Specify the configuration file for the Geyser plugin."),
        )
        .arg(
            Arg::with_name("no_accounts_db_caching")
                .long("no-accounts-db-caching")
                .help("Disables accounts caching"),
        )
        .arg(
            Arg::with_name("deactivate_feature")
                .long("deactivate-feature")
                .takes_value(true)
                .value_name("FEATURE_PUBKEY")
                .validator(is_pubkey)
                .multiple(true)
                .help("deactivate this feature in genesis.")
        )
        .arg(
            Arg::with_name("max_compute_units")
                .long("max-compute-units")
                .value_name("COMPUTE_UNITS")
                .validator(is_parsable::<u64>)
                .takes_value(true)
                .help("Override the runtime's maximum compute units")
        )
        .get_matches();

    let output = if matches.is_present("quiet") {
        Output::None
    } else if matches.is_present("log") {
        Output::Log
    } else {
        Output::Dashboard
    };

    let ledger_path = value_t_or_exit!(matches, "ledger_path", PathBuf);
    let reset_ledger = matches.is_present("reset");

    if !ledger_path.exists() {
        fs::create_dir(&ledger_path).unwrap_or_else(|err| {
            println!(
                "Error: Unable to create directory {}: {}",
                ledger_path.display(),
                err
            );
            exit(1);
        });
    }

    let mut ledger_lock = ledger_lockfile(&ledger_path);
    let _ledger_write_guard = lock_ledger(&ledger_path, &mut ledger_lock);
    if reset_ledger {
        remove_directory_contents(&ledger_path).unwrap_or_else(|err| {
            println!("Error: Unable to remove {}: {}", ledger_path.display(), err);
            exit(1);
        })
    }
    solana_runtime::snapshot_utils::remove_tmp_snapshot_archives(&ledger_path);

    let validator_log_symlink = ledger_path.join("validator.log");

    let logfile = if output != Output::Log {
        let validator_log_with_timestamp = format!(
            "validator-{}.log",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );

        let _ = fs::remove_file(&validator_log_symlink);
        symlink::symlink_file(&validator_log_with_timestamp, &validator_log_symlink).unwrap();

        Some(
            ledger_path
                .join(validator_log_with_timestamp)
                .into_os_string()
                .into_string()
                .unwrap(),
        )
    } else {
        None
    };
    let _logger_thread = redirect_stderr_to_file(logfile);

    info!("{} {}", crate_name!(), solana_version::version!());
    info!("Starting validator with: {:#?}", std::env::args_os());
    solana_core::validator::report_target_features();

    // TODO: Ideally test-validator should *only* allow private addresses.
    let socket_addr_space = SocketAddrSpace::new(/*allow_private_addr=*/ true);
    let cli_config = if let Some(config_file) = matches.value_of("config_file") {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };

    let cluster_rpc_client = value_t!(matches, "json_rpc_url", String)
        .map(normalize_to_url_if_moniker)
        .map(RpcClient::new);

    let (mint_address, random_mint) = pubkey_of(&matches, "mint_address")
        .map(|pk| (pk, false))
        .unwrap_or_else(|| {
            read_keypair_file(&cli_config.keypair_path)
                .map(|kp| (kp.pubkey(), false))
                .unwrap_or_else(|_| (Keypair::new().pubkey(), true))
        });

    let rpc_port = value_t_or_exit!(matches, "rpc_port", u16);
    let enable_vote_subscription = matches.is_present("rpc_pubsub_enable_vote_subscription");
    let faucet_port = value_t_or_exit!(matches, "faucet_port", u16);
    let ticks_per_slot = value_t!(matches, "ticks_per_slot", u64).ok();
    let slots_per_epoch = value_t!(matches, "slots_per_epoch", Slot).ok();
    let gossip_host = matches.value_of("gossip_host").map(|gossip_host| {
        solana_net_utils::parse_host(gossip_host).unwrap_or_else(|err| {
            eprintln!("Failed to parse --gossip-host: {}", err);
            exit(1);
        })
    });
    let gossip_port = value_t!(matches, "gossip_port", u16).ok();
    let dynamic_port_range = matches.value_of("dynamic_port_range").map(|port_range| {
        solana_net_utils::parse_port_range(port_range).unwrap_or_else(|| {
            eprintln!("Failed to parse --dynamic-port-range");
            exit(1);
        })
    });
    let bind_address = matches.value_of("bind_address").map(|bind_address| {
        solana_net_utils::parse_host(bind_address).unwrap_or_else(|err| {
            eprintln!("Failed to parse --bind-address: {}", err);
            exit(1);
        })
    });
    let max_compute_units = value_t!(matches, "max_compute_units", u64).ok();

    let faucet_addr = Some(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        faucet_port,
    ));

    let mut programs_to_load = vec![];
    if let Some(values) = matches.values_of("bpf_program") {
        let values: Vec<&str> = values.collect::<Vec<_>>();
        for address_program in values.chunks(2) {
            match address_program {
                [address, program] => {
                    let address = address
                        .parse::<Pubkey>()
                        .or_else(|_| read_keypair_file(address).map(|keypair| keypair.pubkey()))
                        .unwrap_or_else(|err| {
                            println!("Error: invalid address {}: {}", address, err);
                            exit(1);
                        });

                    let program_path = PathBuf::from(program);
                    if !program_path.exists() {
                        println!(
                            "Error: program file does not exist: {}",
                            program_path.display()
                        );
                        exit(1);
                    }

                    programs_to_load.push(ProgramInfo {
                        program_id: address,
                        loader: solana_sdk::bpf_loader::id(),
                        program_path,
                    });
                }
                _ => unreachable!(),
            }
        }
    }

    let mut accounts_to_load = vec![];
    if let Some(values) = matches.values_of("account") {
        let values: Vec<&str> = values.collect::<Vec<_>>();
        for address_filename in values.chunks(2) {
            match address_filename {
                [address, filename] => {
                    let address = address.parse::<Pubkey>().unwrap_or_else(|err| {
                        println!("Error: invalid address {}: {}", address, err);
                        exit(1);
                    });

                    accounts_to_load.push(AccountInfo { address, filename });
                }
                _ => unreachable!(),
            }
        }
    }

    let accounts_to_clone: HashSet<_> = pubkeys_of(&matches, "clone_account")
        .map(|v| v.into_iter().collect())
        .unwrap_or_default();

    let accounts_to_maybe_clone: HashSet<_> = pubkeys_of(&matches, "maybe_clone_account")
        .map(|v| v.into_iter().collect())
        .unwrap_or_default();

    let warp_slot = if matches.is_present("warp_slot") {
        Some(match matches.value_of("warp_slot") {
            Some(_) => value_t_or_exit!(matches, "warp_slot", Slot),
            None => {
                cluster_rpc_client.as_ref().unwrap_or_else(|_| {
                        println!("The --url argument must be provided if --warp-slot/-w is used without an explicit slot");
                        exit(1);

                }).get_slot()
                    .unwrap_or_else(|err| {
                        println!("Unable to get current cluster slot: {}", err);
                        exit(1);
                    })
            }
        })
    } else {
        None
    };

    let faucet_lamports = sol_to_lamports(value_of(&matches, "faucet_sol").unwrap());
    let faucet_keypair_file = ledger_path.join("faucet-keypair.json");
    if !faucet_keypair_file.exists() {
        write_keypair_file(&Keypair::new(), faucet_keypair_file.to_str().unwrap()).unwrap_or_else(
            |err| {
                println!(
                    "Error: Failed to write {}: {}",
                    faucet_keypair_file.display(),
                    err
                );
                exit(1);
            },
        );
    }

    let faucet_keypair =
        read_keypair_file(faucet_keypair_file.to_str().unwrap()).unwrap_or_else(|err| {
            println!(
                "Error: Failed to read {}: {}",
                faucet_keypair_file.display(),
                err
            );
            exit(1);
        });
    let faucet_pubkey = faucet_keypair.pubkey();

    if let Some(faucet_addr) = &faucet_addr {
        let (sender, receiver) = unbounded();
        run_local_faucet_with_port(faucet_keypair, sender, None, faucet_addr.port());
        let _ = receiver.recv().expect("run faucet").unwrap_or_else(|err| {
            println!("Error: failed to start faucet: {}", err);
            exit(1);
        });
    }

    let features_to_deactivate = pubkeys_of(&matches, "deactivate_feature").unwrap_or_default();

    if TestValidatorGenesis::ledger_exists(&ledger_path) {
        for (name, long) in &[
            ("bpf_program", "--bpf-program"),
            ("clone_account", "--clone"),
            ("account", "--account"),
            ("mint_address", "--mint"),
            ("ticks_per_slot", "--ticks-per-slot"),
            ("slots_per_epoch", "--slots-per-epoch"),
            ("faucet_sol", "--faucet-sol"),
            ("deactivate_feature", "--deactivate-feature"),
        ] {
            if matches.is_present(name) {
                println!("{} argument ignored, ledger already exists", long);
            }
        }
    } else if random_mint {
        println_name_value(
            "\nNotice!",
            "No wallet available. `solana airdrop` localnet SOL after creating one\n",
        );
    }

    let mut genesis = TestValidatorGenesis::default();
    genesis.max_ledger_shreds = value_of(&matches, "limit_ledger_size");
    genesis.max_genesis_archive_unpacked_size = Some(u64::MAX);
    genesis.accounts_db_caching_enabled = !matches.is_present("no_accounts_db_caching");

    let tower_storage = Arc::new(FileTowerStorage::new(ledger_path.clone()));

    let admin_service_post_init = Arc::new(RwLock::new(None));
    admin_rpc_service::run(
        &ledger_path,
        admin_rpc_service::AdminRpcRequestMetadata {
            rpc_addr: Some(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
                rpc_port,
            )),
            start_progress: genesis.start_progress.clone(),
            start_time: std::time::SystemTime::now(),
            validator_exit: genesis.validator_exit.clone(),
            authorized_voter_keypairs: genesis.authorized_voter_keypairs.clone(),
            post_init: admin_service_post_init.clone(),
            tower_storage: tower_storage.clone(),
        },
    );
    let dashboard = if output == Output::Dashboard {
        Some(
            Dashboard::new(
                &ledger_path,
                Some(&validator_log_symlink),
                Some(&mut genesis.validator_exit.write().unwrap()),
            )
            .unwrap(),
        )
    } else {
        None
    };

    let rpc_bigtable_config = if matches.is_present("enable_rpc_bigtable_ledger_storage") {
        Some(RpcBigtableConfig {
            enable_bigtable_ledger_upload: false,
            bigtable_instance_name: value_t_or_exit!(matches, "rpc_bigtable_instance", String),
            timeout: None,
        })
    } else {
        None
    };

    genesis
        .ledger_path(&ledger_path)
        .tower_storage(tower_storage)
        .add_account(
            faucet_pubkey,
            AccountSharedData::new(faucet_lamports, 0, &system_program::id()),
        )
        .rpc_config(JsonRpcConfig {
            enable_rpc_transaction_history: true,
            enable_extended_tx_metadata_storage: true,
            rpc_bigtable_config,
            faucet_addr,
            ..JsonRpcConfig::default_for_test()
        })
        .pubsub_config(PubSubConfig {
            enable_vote_subscription,
            ..PubSubConfig::default()
        })
        .bpf_jit(!matches.is_present("no_bpf_jit"))
        .rpc_port(rpc_port)
        .add_programs_with_path(&programs_to_load)
        .add_accounts_from_json_files(&accounts_to_load)
        .deactivate_features(&features_to_deactivate);

    if !accounts_to_clone.is_empty() {
        genesis.clone_accounts(
            accounts_to_clone,
            cluster_rpc_client
                .as_ref()
                .expect("bug: --url argument missing?"),
            false,
        );
    }

    if !accounts_to_maybe_clone.is_empty() {
        genesis.clone_accounts(
            accounts_to_maybe_clone,
            cluster_rpc_client
                .as_ref()
                .expect("bug: --url argument missing?"),
            true,
        );
    }

    if let Some(warp_slot) = warp_slot {
        genesis.warp_slot(warp_slot);
    }

    if let Some(ticks_per_slot) = ticks_per_slot {
        genesis.ticks_per_slot(ticks_per_slot);
    }

    if let Some(slots_per_epoch) = slots_per_epoch {
        genesis.epoch_schedule(EpochSchedule::custom(
            slots_per_epoch,
            slots_per_epoch,
            /* enable_warmup_epochs = */ false,
        ));

        genesis.rent = Rent::with_slots_per_epoch(slots_per_epoch);
    }

    if let Some(gossip_host) = gossip_host {
        genesis.gossip_host(gossip_host);
    }

    if let Some(gossip_port) = gossip_port {
        genesis.gossip_port(gossip_port);
    }

    if let Some(dynamic_port_range) = dynamic_port_range {
        genesis.port_range(dynamic_port_range);
    }

    if let Some(bind_address) = bind_address {
        genesis.bind_ip_addr(bind_address);
    }

    if matches.is_present("geyser_plugin_config") {
        genesis.geyser_plugin_config_files = Some(
            values_t_or_exit!(matches, "geyser_plugin_config", String)
                .into_iter()
                .map(PathBuf::from)
                .collect(),
        );
    }

    if let Some(max_compute_units) = max_compute_units {
        genesis.max_compute_units(max_compute_units);
    }

    match genesis.start_with_mint_address(mint_address, socket_addr_space) {
        Ok(test_validator) => {
            *admin_service_post_init.write().unwrap() =
                Some(admin_rpc_service::AdminRpcRequestMetadataPostInit {
                    bank_forks: test_validator.bank_forks(),
                    cluster_info: test_validator.cluster_info(),
                    vote_account: test_validator.vote_account_address(),
                });
            if let Some(dashboard) = dashboard {
                dashboard.run(Duration::from_millis(250));
            }
            test_validator.join();
        }
        Err(err) => {
            drop(dashboard);
            println!("Error: failed to start validator: {}", err);
            exit(1);
        }
    }
}

fn remove_directory_contents(ledger_path: &Path) -> Result<(), io::Error> {
    for entry in fs::read_dir(&ledger_path)? {
        let entry = entry?;
        if entry.metadata()?.is_dir() {
            fs::remove_dir_all(&entry.path())?
        } else {
            fs::remove_file(&entry.path())?
        }
    }
    Ok(())
}
