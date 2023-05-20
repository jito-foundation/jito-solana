use {
    jsonrpc_core::{MetaIoHandler, Metadata, Result},
    jsonrpc_core_client::{transports::ipc, RpcError},
    jsonrpc_derive::rpc,
    jsonrpc_ipc_server::{RequestContext, ServerBuilder},
    jsonrpc_server_utils::tokio,
    log::*,
    serde::{de::Deserializer, Deserialize, Serialize},
    solana_core::{
        consensus::Tower,
        proxy::{
            block_engine_stage::{BlockEngineConfig, BlockEngineStage},
            relayer_stage::{RelayerConfig, RelayerStage},
        },
        tower_storage::TowerStorage,
        validator::ValidatorStartProgress,
    },
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{
        exit::Exit,
        pubkey::Pubkey,
        signature::{read_keypair_file, Keypair, Signer},
    },
    std::{
        collections::HashMap,
        error,
        fmt::{self, Display},
        net::SocketAddr,
        path::{Path, PathBuf},
        str::FromStr,
        sync::{Arc, Mutex, RwLock},
        thread::{self, Builder},
        time::{Duration, SystemTime},
    },
};

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub relayer_config: Arc<Mutex<RelayerConfig>>,
    pub block_engine_config: Arc<Mutex<BlockEngineConfig>>,
    pub shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
}

#[derive(Clone)]
pub struct AdminRpcRequestMetadata {
    pub rpc_addr: Option<SocketAddr>,
    pub start_time: SystemTime,
    pub start_progress: Arc<RwLock<ValidatorStartProgress>>,
    pub validator_exit: Arc<RwLock<Exit>>,
    pub authorized_voter_keypairs: Arc<RwLock<Vec<Arc<Keypair>>>>,
    pub tower_storage: Arc<dyn TowerStorage>,
    pub staked_nodes_overrides: Arc<RwLock<HashMap<Pubkey, u64>>>,
    pub post_init: Arc<RwLock<Option<AdminRpcRequestMetadataPostInit>>>,
}
impl Metadata for AdminRpcRequestMetadata {}

impl AdminRpcRequestMetadata {
    fn with_post_init<F, R>(&self, func: F) -> Result<R>
    where
        F: FnOnce(&AdminRpcRequestMetadataPostInit) -> Result<R>,
    {
        if let Some(post_init) = self.post_init.read().unwrap().as_ref() {
            func(post_init)
        } else {
            Err(jsonrpc_core::error::Error::invalid_params(
                "Retry once validator start up is complete",
            ))
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AdminRpcContactInfo {
    pub id: String,
    pub gossip: SocketAddr,
    pub tvu: SocketAddr,
    pub tvu_forwards: SocketAddr,
    pub repair: SocketAddr,
    pub tpu: SocketAddr,
    pub tpu_forwards: SocketAddr,
    pub tpu_vote: SocketAddr,
    pub rpc: SocketAddr,
    pub rpc_pubsub: SocketAddr,
    pub serve_repair: SocketAddr,
    pub last_updated_timestamp: u64,
    pub shred_version: u16,
}

impl From<ContactInfo> for AdminRpcContactInfo {
    fn from(contact_info: ContactInfo) -> Self {
        let ContactInfo {
            id,
            gossip,
            tvu,
            tvu_forwards,
            repair,
            tpu,
            tpu_forwards,
            tpu_vote,
            rpc,
            rpc_pubsub,
            serve_repair,
            wallclock,
            shred_version,
        } = contact_info;
        Self {
            id: id.to_string(),
            last_updated_timestamp: wallclock,
            gossip,
            tvu,
            tvu_forwards,
            repair,
            tpu,
            tpu_forwards,
            tpu_vote,
            rpc,
            rpc_pubsub,
            serve_repair,
            shred_version,
        }
    }
}

impl Display for AdminRpcContactInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "Identity: {}", self.id)?;
        writeln!(f, "Gossip: {}", self.gossip)?;
        writeln!(f, "TVU: {}", self.tvu)?;
        writeln!(f, "TVU Forwards: {}", self.tvu_forwards)?;
        writeln!(f, "Repair: {}", self.repair)?;
        writeln!(f, "TPU: {}", self.tpu)?;
        writeln!(f, "TPU Forwards: {}", self.tpu_forwards)?;
        writeln!(f, "TPU Votes: {}", self.tpu_vote)?;
        writeln!(f, "RPC: {}", self.rpc)?;
        writeln!(f, "RPC Pubsub: {}", self.rpc_pubsub)?;
        writeln!(f, "Serve Repair: {}", self.serve_repair)?;
        writeln!(f, "Last Updated Timestamp: {}", self.last_updated_timestamp)?;
        writeln!(f, "Shred Version: {}", self.shred_version)
    }
}

#[rpc]
pub trait AdminRpc {
    type Metadata;

    #[rpc(meta, name = "exit")]
    fn exit(&self, meta: Self::Metadata) -> Result<()>;

    #[rpc(meta, name = "rpcAddress")]
    fn rpc_addr(&self, meta: Self::Metadata) -> Result<Option<SocketAddr>>;

    #[rpc(name = "setLogFilter")]
    fn set_log_filter(&self, filter: String) -> Result<()>;

    #[rpc(meta, name = "startTime")]
    fn start_time(&self, meta: Self::Metadata) -> Result<SystemTime>;

    #[rpc(meta, name = "startProgress")]
    fn start_progress(&self, meta: Self::Metadata) -> Result<ValidatorStartProgress>;

    #[rpc(meta, name = "addAuthorizedVoter")]
    fn add_authorized_voter(&self, meta: Self::Metadata, keypair_file: String) -> Result<()>;

    #[rpc(meta, name = "addAuthorizedVoterFromBytes")]
    fn add_authorized_voter_from_bytes(&self, meta: Self::Metadata, keypair: Vec<u8>)
        -> Result<()>;

    #[rpc(meta, name = "removeAllAuthorizedVoters")]
    fn remove_all_authorized_voters(&self, meta: Self::Metadata) -> Result<()>;

    #[rpc(meta, name = "setIdentity")]
    fn set_identity(
        &self,
        meta: Self::Metadata,
        keypair_file: String,
        require_tower: bool,
    ) -> Result<()>;

    #[rpc(meta, name = "setIdentityFromBytes")]
    fn set_identity_from_bytes(
        &self,
        meta: Self::Metadata,
        identity_keypair: Vec<u8>,
        require_tower: bool,
    ) -> Result<()>;

    #[rpc(meta, name = "setStakedNodesOverrides")]
    fn set_staked_nodes_overrides(&self, meta: Self::Metadata, path: String) -> Result<()>;

    #[rpc(meta, name = "contactInfo")]
    fn contact_info(&self, meta: Self::Metadata) -> Result<AdminRpcContactInfo>;

    #[rpc(meta, name = "setBlockEngineConfig")]
    fn set_block_engine_config(
        &self,
        meta: Self::Metadata,
        block_engine_url: String,
        trust_packets: bool,
    ) -> Result<()>;

    #[rpc(meta, name = "setRelayerConfig")]
    fn set_relayer_config(
        &self,
        meta: Self::Metadata,
        relayer_url: String,
        trust_packets: bool,
        expected_heartbeat_interval_ms: u64,
        max_failed_heartbeats: u64,
    ) -> Result<()>;

    #[rpc(meta, name = "setShredReceiverAddress")]
    fn set_shred_receiver_address(&self, meta: Self::Metadata, addr: String) -> Result<()>;
}

pub struct AdminRpcImpl;
impl AdminRpc for AdminRpcImpl {
    type Metadata = AdminRpcRequestMetadata;

    fn exit(&self, meta: Self::Metadata) -> Result<()> {
        debug!("exit admin rpc request received");

        thread::Builder::new()
            .name("solProcessExit".into())
            .spawn(move || {
                // Delay exit signal until this RPC request completes, otherwise the caller of `exit` might
                // receive a confusing error as the validator shuts down before a response is sent back.
                thread::sleep(Duration::from_millis(100));

                warn!("validator exit requested");
                meta.validator_exit.write().unwrap().exit();

                // TODO: Debug why Exit doesn't always cause the validator to fully exit
                // (rocksdb background processing or some other stuck thread perhaps?).
                //
                // If the process is still alive after five seconds, exit harder
                thread::sleep(Duration::from_secs(5));
                warn!("validator exit timeout");
                std::process::exit(0);
            })
            .unwrap();
        Ok(())
    }

    fn rpc_addr(&self, meta: Self::Metadata) -> Result<Option<SocketAddr>> {
        debug!("rpc_addr admin rpc request received");
        Ok(meta.rpc_addr)
    }

    fn set_log_filter(&self, filter: String) -> Result<()> {
        debug!("set_log_filter admin rpc request received");
        solana_logger::setup_with(&filter);
        Ok(())
    }

    fn start_time(&self, meta: Self::Metadata) -> Result<SystemTime> {
        debug!("start_time admin rpc request received");
        Ok(meta.start_time)
    }

    fn start_progress(&self, meta: Self::Metadata) -> Result<ValidatorStartProgress> {
        debug!("start_progress admin rpc request received");
        Ok(*meta.start_progress.read().unwrap())
    }

    fn add_authorized_voter(&self, meta: Self::Metadata, keypair_file: String) -> Result<()> {
        debug!("add_authorized_voter request received");

        let authorized_voter = read_keypair_file(keypair_file)
            .map_err(|err| jsonrpc_core::error::Error::invalid_params(format!("{}", err)))?;

        AdminRpcImpl::add_authorized_voter_keypair(meta, authorized_voter)
    }

    fn add_authorized_voter_from_bytes(
        &self,
        meta: Self::Metadata,
        keypair: Vec<u8>,
    ) -> Result<()> {
        debug!("add_authorized_voter_from_bytes request received");

        let authorized_voter = Keypair::from_bytes(&keypair).map_err(|err| {
            jsonrpc_core::error::Error::invalid_params(format!(
                "Failed to read authorized voter keypair from provided byte array: {}",
                err
            ))
        })?;

        AdminRpcImpl::add_authorized_voter_keypair(meta, authorized_voter)
    }

    fn remove_all_authorized_voters(&self, meta: Self::Metadata) -> Result<()> {
        debug!("remove_all_authorized_voters received");
        meta.authorized_voter_keypairs.write().unwrap().clear();
        Ok(())
    }

    fn set_block_engine_config(
        &self,
        meta: Self::Metadata,
        block_engine_url: String,
        trust_packets: bool,
    ) -> Result<()> {
        debug!("set_block_engine_config request received");
        let config = BlockEngineConfig {
            block_engine_url,
            trust_packets,
        };
        // Detailed log messages are printed inside validate function
        if BlockEngineStage::is_valid_block_engine_config(&config) {
            meta.with_post_init(|post_init| {
                *post_init.block_engine_config.lock().unwrap() = config;
                Ok(())
            })
        } else {
            Err(jsonrpc_core::error::Error::invalid_params(
                "failed to set block engine config. see logs for details.",
            ))
        }
    }

    fn set_identity(
        &self,
        meta: Self::Metadata,
        keypair_file: String,
        require_tower: bool,
    ) -> Result<()> {
        debug!("set_identity request received");

        let identity_keypair = read_keypair_file(&keypair_file).map_err(|err| {
            jsonrpc_core::error::Error::invalid_params(format!(
                "Failed to read identity keypair from {}: {}",
                keypair_file, err
            ))
        })?;

        AdminRpcImpl::set_identity_keypair(meta, identity_keypair, require_tower)
    }

    fn set_identity_from_bytes(
        &self,
        meta: Self::Metadata,
        identity_keypair: Vec<u8>,
        require_tower: bool,
    ) -> Result<()> {
        debug!("set_identity_from_bytes request received");

        let identity_keypair = Keypair::from_bytes(&identity_keypair).map_err(|err| {
            jsonrpc_core::error::Error::invalid_params(format!(
                "Failed to read identity keypair from provided byte array: {}",
                err
            ))
        })?;

        AdminRpcImpl::set_identity_keypair(meta, identity_keypair, require_tower)
    }

    fn set_relayer_config(
        &self,
        meta: Self::Metadata,
        relayer_url: String,
        trust_packets: bool,
        expected_heartbeat_interval_ms: u64,
        max_failed_heartbeats: u64,
    ) -> Result<()> {
        debug!("set_relayer_config request received");
        let expected_heartbeat_interval = Duration::from_millis(expected_heartbeat_interval_ms);
        let oldest_allowed_heartbeat =
            Duration::from_millis(max_failed_heartbeats * expected_heartbeat_interval_ms);
        let config = RelayerConfig {
            relayer_url,
            expected_heartbeat_interval,
            oldest_allowed_heartbeat,
            trust_packets,
        };
        // Detailed log messages are printed inside validate function
        if RelayerStage::is_valid_relayer_config(&config) {
            meta.with_post_init(|post_init| {
                *post_init.relayer_config.lock().unwrap() = config;
                Ok(())
            })
        } else {
            Err(jsonrpc_core::error::Error::invalid_params(
                "failed to set relayer config. see logs for details.",
            ))
        }
    }

    fn set_shred_receiver_address(&self, meta: Self::Metadata, addr: String) -> Result<()> {
        let shred_receiver_address = if addr.is_empty() {
            None
        } else {
            Some(SocketAddr::from_str(&addr).map_err(|_| {
                jsonrpc_core::error::Error::invalid_params(format!(
                    "invalid shred receiver address: {}",
                    addr
                ))
            })?)
        };

        meta.with_post_init(|post_init| {
            *post_init.shred_receiver_address.write().unwrap() = shred_receiver_address;
            Ok(())
        })
    }

    fn set_staked_nodes_overrides(&self, meta: Self::Metadata, path: String) -> Result<()> {
        let loaded_config = load_staked_nodes_overrides(&path)
            .map_err(|err| {
                error!(
                    "Failed to load staked nodes overrides from {}: {}",
                    &path, err
                );
                jsonrpc_core::error::Error::internal_error()
            })?
            .staked_map_id;
        let mut write_staked_nodes = meta.staked_nodes_overrides.write().unwrap();
        write_staked_nodes.clear();
        write_staked_nodes.extend(loaded_config.into_iter());
        info!("Staked nodes overrides loaded from {}", path);
        debug!("overrides map: {:?}", write_staked_nodes);
        Ok(())
    }

    fn contact_info(&self, meta: Self::Metadata) -> Result<AdminRpcContactInfo> {
        meta.with_post_init(|post_init| Ok(post_init.cluster_info.my_contact_info().into()))
    }
}

impl AdminRpcImpl {
    fn add_authorized_voter_keypair(
        meta: AdminRpcRequestMetadata,
        authorized_voter: Keypair,
    ) -> Result<()> {
        let mut authorized_voter_keypairs = meta.authorized_voter_keypairs.write().unwrap();

        if authorized_voter_keypairs
            .iter()
            .any(|x| x.pubkey() == authorized_voter.pubkey())
        {
            Err(jsonrpc_core::error::Error::invalid_params(
                "Authorized voter already present",
            ))
        } else {
            authorized_voter_keypairs.push(Arc::new(authorized_voter));
            Ok(())
        }
    }

    fn set_identity_keypair(
        meta: AdminRpcRequestMetadata,
        identity_keypair: Keypair,
        require_tower: bool,
    ) -> Result<()> {
        meta.with_post_init(|post_init| {
            if require_tower {
                let _ = Tower::restore(meta.tower_storage.as_ref(), &identity_keypair.pubkey())
                    .map_err(|err| {
                        jsonrpc_core::error::Error::invalid_params(format!(
                            "Unable to load tower file for identity {}: {}",
                            identity_keypair.pubkey(),
                            err
                        ))
                    })?;
            }

            solana_metrics::set_host_id(identity_keypair.pubkey().to_string());
            post_init
                .cluster_info
                .set_keypair(Arc::new(identity_keypair));
            warn!("Identity set to {}", post_init.cluster_info.id());
            Ok(())
        })
    }
}

// Start the Admin RPC interface
pub fn run(ledger_path: &Path, metadata: AdminRpcRequestMetadata) {
    let admin_rpc_path = admin_rpc_path(ledger_path);

    let event_loop = tokio::runtime::Builder::new_multi_thread()
        .thread_name("solAdminRpcEl")
        .worker_threads(3) // Three still seems like a lot, and better than the default of available core count
        .enable_all()
        .build()
        .unwrap();

    Builder::new()
        .name("solAdminRpc".to_string())
        .spawn(move || {
            let mut io = MetaIoHandler::default();
            io.extend_with(AdminRpcImpl.to_delegate());

            let validator_exit = metadata.validator_exit.clone();
            let server = ServerBuilder::with_meta_extractor(io, move |_req: &RequestContext| {
                metadata.clone()
            })
            .event_loop_executor(event_loop.handle().clone())
            .start(&format!("{}", admin_rpc_path.display()));

            match server {
                Err(err) => {
                    warn!("Unable to start admin rpc service: {:?}", err);
                }
                Ok(server) => {
                    let close_handle = server.close_handle();
                    validator_exit
                        .write()
                        .unwrap()
                        .register_exit(Box::new(move || {
                            close_handle.close();
                        }));

                    server.wait();
                }
            }
        })
        .unwrap();
}

fn admin_rpc_path(ledger_path: &Path) -> PathBuf {
    #[cfg(target_family = "windows")]
    {
        // More information about the wackiness of pipe names over at
        // https://docs.microsoft.com/en-us/windows/win32/ipc/pipe-names
        if let Some(ledger_filename) = ledger_path.file_name() {
            PathBuf::from(format!(
                "\\\\.\\pipe\\{}-admin.rpc",
                ledger_filename.to_string_lossy()
            ))
        } else {
            PathBuf::from("\\\\.\\pipe\\admin.rpc")
        }
    }
    #[cfg(not(target_family = "windows"))]
    {
        ledger_path.join("admin.rpc")
    }
}

// Connect to the Admin RPC interface
pub async fn connect(ledger_path: &Path) -> std::result::Result<gen_client::Client, RpcError> {
    let admin_rpc_path = admin_rpc_path(ledger_path);
    if !admin_rpc_path.exists() {
        Err(RpcError::Client(format!(
            "{} does not exist",
            admin_rpc_path.display()
        )))
    } else {
        ipc::connect::<_, gen_client::Client>(&format!("{}", admin_rpc_path.display())).await
    }
}

pub fn runtime() -> jsonrpc_server_utils::tokio::runtime::Runtime {
    jsonrpc_server_utils::tokio::runtime::Runtime::new().expect("new tokio runtime")
}

#[derive(Default, Deserialize, Clone)]
pub struct StakedNodesOverrides {
    #[serde(deserialize_with = "deserialize_pubkey_map")]
    pub staked_map_id: HashMap<Pubkey, u64>,
}

pub fn deserialize_pubkey_map<'de, D>(des: D) -> std::result::Result<HashMap<Pubkey, u64>, D::Error>
where
    D: Deserializer<'de>,
{
    let container: HashMap<String, u64> = serde::Deserialize::deserialize(des)?;
    let mut container_typed: HashMap<Pubkey, u64> = HashMap::new();
    for (key, value) in container.iter() {
        let typed_key = Pubkey::try_from(key.as_str())
            .map_err(|_| serde::de::Error::invalid_type(serde::de::Unexpected::Map, &"PubKey"))?;
        container_typed.insert(typed_key, *value);
    }
    Ok(container_typed)
}

pub fn load_staked_nodes_overrides(
    path: &String,
) -> std::result::Result<StakedNodesOverrides, Box<dyn error::Error>> {
    debug!("Loading staked nodes overrides configuration from {}", path);
    if Path::new(&path).exists() {
        let file = std::fs::File::open(path)?;
        Ok(serde_yaml::from_reader(file)?)
    } else {
        Err(format!(
            "Staked nodes overrides provided '{}' a non-existing file path.",
            path
        )
        .into())
    }
}
