#![allow(clippy::integer_arithmetic)]
use {
    log::*,
    solana_cli_output::CliAccount,
    solana_client::{nonblocking, rpc_client::RpcClient},
    solana_core::{
        tower_storage::TowerStorage,
        validator::{Validator, ValidatorConfig, ValidatorStartProgress},
    },
    solana_gossip::{
        cluster_info::{ClusterInfo, Node},
        gossip_service::discover_cluster,
        socketaddr,
    },
    solana_ledger::{
        blockstore::create_new_ledger, blockstore_db::LedgerColumnOptions, create_new_tmp_ledger,
    },
    solana_net_utils::PortRange,
    solana_program_runtime::compute_budget::ComputeBudget,
    solana_rpc::{rpc::JsonRpcConfig, rpc_pubsub_service::PubSubConfig},
    solana_runtime::{
        accounts_db::AccountsDbConfig, accounts_index::AccountsIndexConfig, bank_forks::BankForks,
        genesis_utils::create_genesis_config_with_leader_ex,
        hardened_unpack::MAX_GENESIS_ARCHIVE_UNPACKED_SIZE, runtime_config::RuntimeConfig,
        snapshot_config::SnapshotConfig,
    },
    solana_sdk::{
        account::{Account, AccountSharedData},
        clock::{Slot, DEFAULT_MS_PER_SLOT},
        commitment_config::CommitmentConfig,
        epoch_schedule::EpochSchedule,
        exit::Exit,
        feature_set::FEATURE_NAMES,
        fee_calculator::{FeeCalculator, FeeRateGovernor},
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        message::Message,
        native_token::sol_to_lamports,
        pubkey::Pubkey,
        rent::Rent,
        signature::{read_keypair_file, write_keypair_file, Keypair, Signer},
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{
        collections::{HashMap, HashSet},
        fs::{remove_dir_all, File},
        io::Read,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        str::FromStr,
        sync::{Arc, RwLock},
        time::Duration,
    },
    tokio::time::sleep,
};

#[derive(Clone)]
pub struct AccountInfo<'a> {
    pub address: Pubkey,
    pub filename: &'a str,
}

#[derive(Clone)]
pub struct ProgramInfo {
    pub program_id: Pubkey,
    pub loader: Pubkey,
    pub program_path: PathBuf,
}

#[derive(Debug)]
pub struct TestValidatorNodeConfig {
    gossip_addr: SocketAddr,
    port_range: PortRange,
    bind_ip_addr: IpAddr,
}

impl Default for TestValidatorNodeConfig {
    fn default() -> Self {
        const MIN_PORT_RANGE: u16 = 1024;
        const MAX_PORT_RANGE: u16 = 65535;

        let bind_ip_addr = IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0));
        let port_range = (MIN_PORT_RANGE, MAX_PORT_RANGE);

        Self {
            gossip_addr: socketaddr!("127.0.0.1:0"),
            port_range,
            bind_ip_addr,
        }
    }
}

pub struct TestValidatorGenesis {
    fee_rate_governor: FeeRateGovernor,
    ledger_path: Option<PathBuf>,
    tower_storage: Option<Arc<dyn TowerStorage>>,
    pub rent: Rent,
    rpc_config: JsonRpcConfig,
    pubsub_config: PubSubConfig,
    rpc_ports: Option<(u16, u16)>, // (JsonRpc, JsonRpcPubSub), None == random ports
    warp_slot: Option<Slot>,
    no_bpf_jit: bool,
    accounts: HashMap<Pubkey, AccountSharedData>,
    programs: Vec<ProgramInfo>,
    ticks_per_slot: Option<u64>,
    epoch_schedule: Option<EpochSchedule>,
    node_config: TestValidatorNodeConfig,
    pub validator_exit: Arc<RwLock<Exit>>,
    pub start_progress: Arc<RwLock<ValidatorStartProgress>>,
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub max_ledger_shreds: Option<u64>,
    pub max_genesis_archive_unpacked_size: Option<u64>,
    pub geyser_plugin_config_files: Option<Vec<PathBuf>>,
    pub accounts_db_caching_enabled: bool,
    deactivate_feature_set: HashSet<Pubkey>,
    max_compute_units: Option<u64>,
}

impl Default for TestValidatorGenesis {
    fn default() -> Self {
        Self {
            fee_rate_governor: FeeRateGovernor::default(),
            ledger_path: Option::<PathBuf>::default(),
            tower_storage: Option::<Arc<dyn TowerStorage>>::default(),
            rent: Rent::default(),
            rpc_config: JsonRpcConfig::default_for_test(),
            pubsub_config: PubSubConfig::default(),
            rpc_ports: Option::<(u16, u16)>::default(),
            warp_slot: Option::<Slot>::default(),
            no_bpf_jit: bool::default(),
            accounts: HashMap::<Pubkey, AccountSharedData>::default(),
            programs: Vec::<ProgramInfo>::default(),
            ticks_per_slot: Option::<u64>::default(),
            epoch_schedule: Option::<EpochSchedule>::default(),
            node_config: TestValidatorNodeConfig::default(),
            validator_exit: Arc::<RwLock<Exit>>::default(),
            start_progress: Arc::<RwLock<ValidatorStartProgress>>::default(),
            authorized_voter_keypairs: Arc::<RwLock<Vec<Arc<Keypair>>>>::default(),
            max_ledger_shreds: Option::<u64>::default(),
            max_genesis_archive_unpacked_size: Option::<u64>::default(),
            geyser_plugin_config_files: Option::<Vec<PathBuf>>::default(),
            accounts_db_caching_enabled: bool::default(),
            deactivate_feature_set: HashSet::<Pubkey>::default(),
            max_compute_units: Option::<u64>::default(),
        }
    }
}

impl TestValidatorGenesis {
    /// Adds features to deactivate to a set, eliminating redundancies
    /// during `initialize_ledger`, if member of the set is not a Feature
    /// it will be silently ignored
    pub fn deactivate_features(&mut self, deactivate_list: &[Pubkey]) -> &mut Self {
        self.deactivate_feature_set.extend(deactivate_list);
        self
    }
    pub fn ledger_path<P: Into<PathBuf>>(&mut self, ledger_path: P) -> &mut Self {
        self.ledger_path = Some(ledger_path.into());
        self
    }

    pub fn tower_storage(&mut self, tower_storage: Arc<dyn TowerStorage>) -> &mut Self {
        self.tower_storage = Some(tower_storage);
        self
    }

    /// Check if a given TestValidator ledger has already been initialized
    pub fn ledger_exists(ledger_path: &Path) -> bool {
        ledger_path.join("vote-account-keypair.json").exists()
    }

    pub fn fee_rate_governor(&mut self, fee_rate_governor: FeeRateGovernor) -> &mut Self {
        self.fee_rate_governor = fee_rate_governor;
        self
    }

    pub fn ticks_per_slot(&mut self, ticks_per_slot: u64) -> &mut Self {
        self.ticks_per_slot = Some(ticks_per_slot);
        self
    }

    pub fn epoch_schedule(&mut self, epoch_schedule: EpochSchedule) -> &mut Self {
        self.epoch_schedule = Some(epoch_schedule);
        self
    }

    pub fn rent(&mut self, rent: Rent) -> &mut Self {
        self.rent = rent;
        self
    }

    pub fn rpc_config(&mut self, rpc_config: JsonRpcConfig) -> &mut Self {
        self.rpc_config = rpc_config;
        self
    }

    pub fn pubsub_config(&mut self, pubsub_config: PubSubConfig) -> &mut Self {
        self.pubsub_config = pubsub_config;
        self
    }

    pub fn rpc_port(&mut self, rpc_port: u16) -> &mut Self {
        self.rpc_ports = Some((rpc_port, rpc_port + 1));
        self
    }

    pub fn faucet_addr(&mut self, faucet_addr: Option<SocketAddr>) -> &mut Self {
        self.rpc_config.faucet_addr = faucet_addr;
        self
    }

    pub fn warp_slot(&mut self, warp_slot: Slot) -> &mut Self {
        self.warp_slot = Some(warp_slot);
        self
    }

    pub fn bpf_jit(&mut self, bpf_jit: bool) -> &mut Self {
        self.no_bpf_jit = !bpf_jit;
        self
    }

    pub fn gossip_host(&mut self, gossip_host: IpAddr) -> &mut Self {
        self.node_config.gossip_addr.set_ip(gossip_host);
        self
    }

    pub fn gossip_port(&mut self, gossip_port: u16) -> &mut Self {
        self.node_config.gossip_addr.set_port(gossip_port);
        self
    }

    pub fn port_range(&mut self, port_range: PortRange) -> &mut Self {
        self.node_config.port_range = port_range;
        self
    }

    pub fn bind_ip_addr(&mut self, bind_ip_addr: IpAddr) -> &mut Self {
        self.node_config.bind_ip_addr = bind_ip_addr;
        self
    }

    pub fn max_compute_units(&mut self, max_compute_units: u64) -> &mut Self {
        self.max_compute_units = Some(max_compute_units);
        self
    }

    /// Add an account to the test environment
    pub fn add_account(&mut self, address: Pubkey, account: AccountSharedData) -> &mut Self {
        self.accounts.insert(address, account);
        self
    }

    pub fn add_accounts<T>(&mut self, accounts: T) -> &mut Self
    where
        T: IntoIterator<Item = (Pubkey, AccountSharedData)>,
    {
        for (address, account) in accounts {
            self.add_account(address, account);
        }
        self
    }

    pub fn clone_accounts<T>(
        &mut self,
        addresses: T,
        rpc_client: &RpcClient,
        skip_missing: bool,
    ) -> &mut Self
    where
        T: IntoIterator<Item = Pubkey>,
    {
        for address in addresses {
            info!("Fetching {} over RPC...", address);
            let res = rpc_client.get_account(&address);
            if let Ok(account) = res {
                self.add_account(address, AccountSharedData::from(account));
            } else if skip_missing {
                warn!("Could not find {}, skipping.", address);
            } else {
                error!("Failed to fetch {}: {}", address, res.unwrap_err());
                solana_core::validator::abort();
            }
        }
        self
    }

    pub fn add_accounts_from_json_files(&mut self, accounts: &[AccountInfo]) -> &mut Self {
        for account in accounts {
            let account_path =
                solana_program_test::find_file(account.filename).unwrap_or_else(|| {
                    error!("Unable to locate {}", account.filename);
                    solana_core::validator::abort();
                });
            let mut file = File::open(&account_path).unwrap();
            let mut account_info_raw = String::new();
            file.read_to_string(&mut account_info_raw).unwrap();

            let result: serde_json::Result<CliAccount> = serde_json::from_str(&account_info_raw);
            let account_info = match result {
                Err(err) => {
                    error!(
                        "Unable to deserialize {}: {}",
                        account_path.to_str().unwrap(),
                        err
                    );
                    solana_core::validator::abort();
                }
                Ok(deserialized) => deserialized,
            };
            let address = Pubkey::from_str(account_info.keyed_account.pubkey.as_str()).unwrap();
            let account = account_info
                .keyed_account
                .account
                .decode::<AccountSharedData>()
                .unwrap();

            self.add_account(address, account);
        }
        self
    }

    /// Add an account to the test environment with the account data in the provided `filename`
    pub fn add_account_with_file_data(
        &mut self,
        address: Pubkey,
        lamports: u64,
        owner: Pubkey,
        filename: &str,
    ) -> &mut Self {
        self.add_account(
            address,
            AccountSharedData::from(Account {
                lamports,
                data: solana_program_test::read_file(
                    solana_program_test::find_file(filename).unwrap_or_else(|| {
                        panic!("Unable to locate {}", filename);
                    }),
                ),
                owner,
                executable: false,
                rent_epoch: 0,
            }),
        )
    }

    /// Add an account to the test environment with the account data in the provided as a base 64
    /// string
    pub fn add_account_with_base64_data(
        &mut self,
        address: Pubkey,
        lamports: u64,
        owner: Pubkey,
        data_base64: &str,
    ) -> &mut Self {
        self.add_account(
            address,
            AccountSharedData::from(Account {
                lamports,
                data: base64::decode(data_base64)
                    .unwrap_or_else(|err| panic!("Failed to base64 decode: {}", err)),
                owner,
                executable: false,
                rent_epoch: 0,
            }),
        )
    }

    /// Add a BPF program to the test environment.
    ///
    /// `program_name` will also used to locate the BPF shared object in the current or fixtures
    /// directory.
    pub fn add_program(&mut self, program_name: &str, program_id: Pubkey) -> &mut Self {
        let program_path = solana_program_test::find_file(&format!("{}.so", program_name))
            .unwrap_or_else(|| panic!("Unable to locate program {}", program_name));

        self.programs.push(ProgramInfo {
            program_id,
            loader: solana_sdk::bpf_loader::id(),
            program_path,
        });
        self
    }

    /// Add a list of programs to the test environment.
    ///pub fn add_programs_with_path<'a>(&'a mut self, programs: &[ProgramInfo]) -> &'a mut Self {
    pub fn add_programs_with_path(&mut self, programs: &[ProgramInfo]) -> &mut Self {
        for program in programs {
            self.programs.push(program.clone());
        }
        self
    }

    /// Start a test validator with the address of the mint account that will receive tokens
    /// created at genesis.
    ///
    pub fn start_with_mint_address(
        &self,
        mint_address: Pubkey,
        socket_addr_space: SocketAddrSpace,
    ) -> Result<TestValidator, Box<dyn std::error::Error>> {
        TestValidator::start(mint_address, self, socket_addr_space).map(|test_validator| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_io()
                .enable_time()
                .build()
                .unwrap();
            runtime.block_on(test_validator.wait_for_nonzero_fees());
            test_validator
        })
    }

    /// Start a test validator
    ///
    /// Returns a new `TestValidator` as well as the keypair for the mint account that will receive tokens
    /// created at genesis.
    ///
    /// This function panics on initialization failure.
    pub fn start(&self) -> (TestValidator, Keypair) {
        self.start_with_socket_addr_space(SocketAddrSpace::new(/*allow_private_addr=*/ true))
    }

    /// Start a test validator with the given `SocketAddrSpace`
    ///
    /// Returns a new `TestValidator` as well as the keypair for the mint account that will receive tokens
    /// created at genesis.
    ///
    /// This function panics on initialization failure.
    pub fn start_with_socket_addr_space(
        &self,
        socket_addr_space: SocketAddrSpace,
    ) -> (TestValidator, Keypair) {
        let mint_keypair = Keypair::new();
        self.start_with_mint_address(mint_keypair.pubkey(), socket_addr_space)
            .map(|test_validator| (test_validator, mint_keypair))
            .unwrap_or_else(|err| panic!("Test validator failed to start: {}", err))
    }

    pub async fn start_async(&self) -> (TestValidator, Keypair) {
        self.start_async_with_socket_addr_space(SocketAddrSpace::new(
            /*allow_private_addr=*/ true,
        ))
        .await
    }

    pub async fn start_async_with_socket_addr_space(
        &self,
        socket_addr_space: SocketAddrSpace,
    ) -> (TestValidator, Keypair) {
        let mint_keypair = Keypair::new();
        match TestValidator::start(mint_keypair.pubkey(), self, socket_addr_space) {
            Ok(test_validator) => {
                test_validator.wait_for_nonzero_fees().await;
                (test_validator, mint_keypair)
            }
            Err(err) => panic!("Test validator failed to start: {}", err),
        }
    }
}

pub struct TestValidator {
    ledger_path: PathBuf,
    preserve_ledger: bool,
    rpc_pubsub_url: String,
    rpc_url: String,
    tpu: SocketAddr,
    gossip: SocketAddr,
    validator: Option<Validator>,
    vote_account_address: Pubkey,
}

impl TestValidator {
    /// Create and start a `TestValidator` with no transaction fees and minimal rent.
    /// Faucet optional.
    ///
    /// This function panics on initialization failure.
    pub fn with_no_fees(
        mint_address: Pubkey,
        faucet_addr: Option<SocketAddr>,
        socket_addr_space: SocketAddrSpace,
    ) -> Self {
        TestValidatorGenesis::default()
            .fee_rate_governor(FeeRateGovernor::new(0, 0))
            .rent(Rent {
                lamports_per_byte_year: 1,
                exemption_threshold: 1.0,
                ..Rent::default()
            })
            .faucet_addr(faucet_addr)
            .start_with_mint_address(mint_address, socket_addr_space)
            .expect("validator start failed")
    }

    /// Create and start a `TestValidator` with custom transaction fees and minimal rent.
    /// Faucet optional.
    ///
    /// This function panics on initialization failure.
    pub fn with_custom_fees(
        mint_address: Pubkey,
        target_lamports_per_signature: u64,
        faucet_addr: Option<SocketAddr>,
        socket_addr_space: SocketAddrSpace,
    ) -> Self {
        TestValidatorGenesis::default()
            .fee_rate_governor(FeeRateGovernor::new(target_lamports_per_signature, 0))
            .rent(Rent {
                lamports_per_byte_year: 1,
                exemption_threshold: 1.0,
                ..Rent::default()
            })
            .faucet_addr(faucet_addr)
            .start_with_mint_address(mint_address, socket_addr_space)
            .expect("validator start failed")
    }

    /// Initialize the ledger directory
    ///
    /// If `ledger_path` is `None`, a temporary ledger will be created.  Otherwise the ledger will
    /// be initialized in the provided directory if it doesn't already exist.
    ///
    /// Returns the path to the ledger directory.
    fn initialize_ledger(
        mint_address: Pubkey,
        config: &TestValidatorGenesis,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let validator_identity = Keypair::new();
        let validator_vote_account = Keypair::new();
        let validator_stake_account = Keypair::new();
        let validator_identity_lamports = sol_to_lamports(500.);
        let validator_stake_lamports = sol_to_lamports(1_000_000.);
        let mint_lamports = sol_to_lamports(500_000_000.);

        let mut accounts = config.accounts.clone();
        for (address, account) in solana_program_test::programs::spl_programs(&config.rent) {
            accounts.entry(address).or_insert(account);
        }
        for program in &config.programs {
            let data = solana_program_test::read_file(&program.program_path);
            accounts.insert(
                program.program_id,
                AccountSharedData::from(Account {
                    lamports: Rent::default().minimum_balance(data.len()).min(1),
                    data,
                    owner: program.loader,
                    executable: true,
                    rent_epoch: 0,
                }),
            );
        }

        let mut genesis_config = create_genesis_config_with_leader_ex(
            mint_lamports,
            &mint_address,
            &validator_identity.pubkey(),
            &validator_vote_account.pubkey(),
            &validator_stake_account.pubkey(),
            validator_stake_lamports,
            validator_identity_lamports,
            config.fee_rate_governor.clone(),
            config.rent,
            solana_sdk::genesis_config::ClusterType::Development,
            accounts.into_iter().collect(),
        );
        genesis_config.epoch_schedule = config
            .epoch_schedule
            .unwrap_or_else(EpochSchedule::without_warmup);

        if let Some(ticks_per_slot) = config.ticks_per_slot {
            genesis_config.ticks_per_slot = ticks_per_slot;
        }

        // Remove features tagged to deactivate
        for deactivate_feature_pk in &config.deactivate_feature_set {
            if FEATURE_NAMES.contains_key(deactivate_feature_pk) {
                match genesis_config.accounts.remove(deactivate_feature_pk) {
                    Some(_) => info!("Feature for {:?} deactivated", deactivate_feature_pk),
                    None => warn!(
                        "Feature {:?} set for deactivation not found in genesis_config account list, ignored.",
                        deactivate_feature_pk
                    ),
                }
            } else {
                warn!(
                    "Feature {:?} set for deactivation is not a known Feature public key",
                    deactivate_feature_pk
                );
            }
        }

        let ledger_path = match &config.ledger_path {
            None => create_new_tmp_ledger!(&genesis_config).0,
            Some(ledger_path) => {
                if TestValidatorGenesis::ledger_exists(ledger_path) {
                    return Ok(ledger_path.to_path_buf());
                }

                let _ = create_new_ledger(
                    ledger_path,
                    &genesis_config,
                    config
                        .max_genesis_archive_unpacked_size
                        .unwrap_or(MAX_GENESIS_ARCHIVE_UNPACKED_SIZE),
                    LedgerColumnOptions::default(),
                )
                .map_err(|err| {
                    format!(
                        "Failed to create ledger at {}: {}",
                        ledger_path.display(),
                        err
                    )
                })?;
                ledger_path.to_path_buf()
            }
        };

        write_keypair_file(
            &validator_identity,
            ledger_path.join("validator-keypair.json").to_str().unwrap(),
        )?;

        // `ledger_exists` should fail until the vote account keypair is written
        assert!(!TestValidatorGenesis::ledger_exists(&ledger_path));

        write_keypair_file(
            &validator_vote_account,
            ledger_path
                .join("vote-account-keypair.json")
                .to_str()
                .unwrap(),
        )?;

        Ok(ledger_path)
    }

    /// Starts a TestValidator at the provided ledger directory
    fn start(
        mint_address: Pubkey,
        config: &TestValidatorGenesis,
        socket_addr_space: SocketAddrSpace,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let preserve_ledger = config.ledger_path.is_some();
        let ledger_path = TestValidator::initialize_ledger(mint_address, config)?;

        let validator_identity =
            read_keypair_file(ledger_path.join("validator-keypair.json").to_str().unwrap())?;
        let validator_vote_account = read_keypair_file(
            ledger_path
                .join("vote-account-keypair.json")
                .to_str()
                .unwrap(),
        )?;

        let mut node = Node::new_single_bind(
            &validator_identity.pubkey(),
            &config.node_config.gossip_addr,
            config.node_config.port_range,
            config.node_config.bind_ip_addr,
        );
        if let Some((rpc, rpc_pubsub)) = config.rpc_ports {
            node.info.rpc = SocketAddr::new(node.info.gossip.ip(), rpc);
            node.info.rpc_pubsub = SocketAddr::new(node.info.gossip.ip(), rpc_pubsub);
        }

        let vote_account_address = validator_vote_account.pubkey();
        let rpc_url = format!("http://{}", node.info.rpc);
        let rpc_pubsub_url = format!("ws://{}/", node.info.rpc_pubsub);
        let tpu = node.info.tpu;
        let gossip = node.info.gossip;

        {
            let mut authorized_voter_keypairs = config.authorized_voter_keypairs.write().unwrap();
            if !authorized_voter_keypairs
                .iter()
                .any(|x| x.pubkey() == vote_account_address)
            {
                authorized_voter_keypairs.push(Arc::new(validator_vote_account))
            }
        }

        let accounts_db_config = Some(AccountsDbConfig {
            index: Some(AccountsIndexConfig {
                started_from_validator: true,
                ..AccountsIndexConfig::default()
            }),
            ..AccountsDbConfig::default()
        });

        let runtime_config = RuntimeConfig {
            bpf_jit: !config.no_bpf_jit,
            compute_budget: config.max_compute_units.map(|max_units| ComputeBudget {
                max_units,
                ..ComputeBudget::default()
            }),
        };

        let mut validator_config = ValidatorConfig {
            geyser_plugin_config_files: config.geyser_plugin_config_files.clone(),
            accounts_db_caching_enabled: config.accounts_db_caching_enabled,
            rpc_addrs: Some((
                SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), node.info.rpc.port()),
                SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
                    node.info.rpc_pubsub.port(),
                ),
            )),
            rpc_config: config.rpc_config.clone(),
            pubsub_config: config.pubsub_config.clone(),
            accounts_hash_interval_slots: 100,
            account_paths: vec![ledger_path.join("accounts")],
            poh_verify: false, // Skip PoH verification of ledger on startup for speed
            snapshot_config: Some(SnapshotConfig {
                full_snapshot_archive_interval_slots: 100,
                incremental_snapshot_archive_interval_slots: Slot::MAX,
                bank_snapshots_dir: ledger_path.join("snapshot"),
                snapshot_archives_dir: ledger_path.to_path_buf(),
                ..SnapshotConfig::default()
            }),
            enforce_ulimit_nofile: false,
            warp_slot: config.warp_slot,
            validator_exit: config.validator_exit.clone(),
            rocksdb_compaction_interval: Some(100), // Compact every 100 slots
            max_ledger_shreds: config.max_ledger_shreds,
            no_wait_for_vote_to_start_leader: true,
            accounts_db_config,
            runtime_config,
            ..ValidatorConfig::default_for_test()
        };
        if let Some(ref tower_storage) = config.tower_storage {
            validator_config.tower_storage = tower_storage.clone();
        }

        let validator = Some(Validator::new(
            node,
            Arc::new(validator_identity),
            &ledger_path,
            &vote_account_address,
            config.authorized_voter_keypairs.clone(),
            vec![],
            &validator_config,
            true, // should_check_duplicate_instance
            config.start_progress.clone(),
            socket_addr_space,
            false, // use_quic
        ));

        // Needed to avoid panics in `solana-responder-gossip` in tests that create a number of
        // test validators concurrently...
        discover_cluster(&gossip, 1, socket_addr_space)
            .map_err(|err| format!("TestValidator startup failed: {:?}", err))?;

        let test_validator = TestValidator {
            ledger_path,
            preserve_ledger,
            rpc_pubsub_url,
            rpc_url,
            tpu,
            gossip,
            validator,
            vote_account_address,
        };
        Ok(test_validator)
    }

    /// This is a hack to delay until the fees are non-zero for test consistency
    /// (fees from genesis are zero until the first block with a transaction in it is completed
    ///  due to a bug in the Bank)
    async fn wait_for_nonzero_fees(&self) {
        let rpc_client = nonblocking::rpc_client::RpcClient::new_with_commitment(
            self.rpc_url.clone(),
            CommitmentConfig::processed(),
        );
        let mut message = Message::new(
            &[Instruction::new_with_bytes(
                Pubkey::new_unique(),
                &[],
                vec![AccountMeta::new(Pubkey::new_unique(), true)],
            )],
            None,
        );
        const MAX_TRIES: u64 = 10;
        let mut num_tries = 0;
        loop {
            num_tries += 1;
            if num_tries > MAX_TRIES {
                break;
            }
            println!("Waiting for fees to stabilize {:?}...", num_tries);
            match rpc_client.get_latest_blockhash().await {
                Ok(blockhash) => {
                    message.recent_blockhash = blockhash;
                    match rpc_client.get_fee_for_message(&message).await {
                        Ok(fee) => {
                            if fee != 0 {
                                break;
                            }
                        }
                        Err(err) => {
                            warn!("get_fee_for_message() failed: {:?}", err);
                            break;
                        }
                    }
                }
                Err(err) => {
                    warn!("get_latest_blockhash() failed: {:?}", err);
                    break;
                }
            }
            sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT)).await;
        }
    }

    /// Return the validator's TPU address
    pub fn tpu(&self) -> &SocketAddr {
        &self.tpu
    }

    /// Return the validator's Gossip address
    pub fn gossip(&self) -> &SocketAddr {
        &self.gossip
    }

    /// Return the validator's JSON RPC URL
    pub fn rpc_url(&self) -> String {
        self.rpc_url.clone()
    }

    /// Return the validator's JSON RPC PubSub URL
    pub fn rpc_pubsub_url(&self) -> String {
        self.rpc_pubsub_url.clone()
    }

    /// Return the validator's vote account address
    pub fn vote_account_address(&self) -> Pubkey {
        self.vote_account_address
    }

    /// Return an RpcClient for the validator.  As a convenience, also return a recent blockhash and
    /// associated fee calculator
    #[deprecated(since = "1.9.0", note = "Please use `get_rpc_client` instead")]
    pub fn rpc_client(&self) -> (RpcClient, Hash, FeeCalculator) {
        let rpc_client =
            RpcClient::new_with_commitment(self.rpc_url.clone(), CommitmentConfig::processed());
        #[allow(deprecated)]
        let (recent_blockhash, fee_calculator) = rpc_client
            .get_recent_blockhash()
            .expect("get_recent_blockhash");

        (rpc_client, recent_blockhash, fee_calculator)
    }

    /// Return an RpcClient for the validator.
    pub fn get_rpc_client(&self) -> RpcClient {
        RpcClient::new_with_commitment(self.rpc_url.clone(), CommitmentConfig::processed())
    }

    /// Return a nonblocking RpcClient for the validator.
    pub fn get_async_rpc_client(&self) -> nonblocking::rpc_client::RpcClient {
        nonblocking::rpc_client::RpcClient::new_with_commitment(
            self.rpc_url.clone(),
            CommitmentConfig::processed(),
        )
    }

    pub fn join(mut self) {
        if let Some(validator) = self.validator.take() {
            validator.join();
        }
    }

    pub fn cluster_info(&self) -> Arc<ClusterInfo> {
        self.validator.as_ref().unwrap().cluster_info.clone()
    }

    pub fn bank_forks(&self) -> Arc<RwLock<BankForks>> {
        self.validator.as_ref().unwrap().bank_forks.clone()
    }
}

impl Drop for TestValidator {
    fn drop(&mut self) {
        if let Some(validator) = self.validator.take() {
            validator.close();
        }
        if !self.preserve_ledger {
            remove_dir_all(&self.ledger_path).unwrap_or_else(|err| {
                panic!(
                    "Failed to remove ledger directory {}: {}",
                    self.ledger_path.display(),
                    err
                )
            });
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn get_health() {
        let (test_validator, _payer) = TestValidatorGenesis::default().start();
        let rpc_client = test_validator.get_rpc_client();
        rpc_client.get_health().expect("health");
    }

    #[tokio::test]
    async fn nonblocking_get_health() {
        let (test_validator, _payer) = TestValidatorGenesis::default().start_async().await;
        let rpc_client = test_validator.get_async_rpc_client();
        rpc_client.get_health().await.expect("health");
    }

    #[tokio::test]
    #[should_panic]
    async fn document_tokio_panic() {
        // `start()` blows up when run within tokio
        let (_test_validator, _payer) = TestValidatorGenesis::default().start();
    }
}
