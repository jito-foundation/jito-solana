use {
    crate::{
        cluster_slots_service::cluster_slots::ClusterSlots,
        proxy::{block_engine_stage::BlockEngineConfig, relayer_stage::RelayerConfig},
        repair::{outstanding_requests::OutstandingRequests, serve_repair::ShredRepairType},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_pubkey::Pubkey,
    solana_quic_definitions::NotifyKeyUpdate,
    solana_runtime::bank_forks::BankForks,
    std::{
        collections::{HashMap, HashSet},
        net::{SocketAddr, UdpSocket},
        sync::{Arc, Mutex, RwLock},
    },
};

/// Key updaters:
#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub enum KeyUpdaterType {
    /// TPU key updater
    Tpu,
    /// TPU forwards key updater
    TpuForwards,
    /// TPU vote key updater
    TpuVote,
    /// Forward key updater
    Forward,
    /// For the RPC service
    RpcService,
}

/// Responsible for managing the updaters for identity key change
#[derive(Default)]
pub struct KeyUpdaters {
    updaters: HashMap<KeyUpdaterType, Arc<dyn NotifyKeyUpdate + Sync + Send>>,
}

impl KeyUpdaters {
    /// Add a new key updater to the list
    pub fn add(
        &mut self,
        updater_type: KeyUpdaterType,
        updater: Arc<dyn NotifyKeyUpdate + Sync + Send>,
    ) {
        self.updaters.insert(updater_type, updater);
    }

    /// Remove a key updater by its key
    pub fn remove(&mut self, updater_type: &KeyUpdaterType) {
        self.updaters.remove(updater_type);
    }
}

/// Implement the Iterator trait for KeyUpdaters
impl<'a> IntoIterator for &'a KeyUpdaters {
    type Item = (
        &'a KeyUpdaterType,
        &'a Arc<dyn NotifyKeyUpdate + Sync + Send>,
    );
    type IntoIter = std::collections::hash_map::Iter<
        'a,
        KeyUpdaterType,
        Arc<dyn NotifyKeyUpdate + Sync + Send>,
    >;

    fn into_iter(self) -> Self::IntoIter {
        self.updaters.iter()
    }
}

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub notifies: Arc<RwLock<KeyUpdaters>>,
    pub repair_socket: Arc<UdpSocket>,
    pub outstanding_repair_requests: Arc<RwLock<OutstandingRequests<ShredRepairType>>>,
    pub cluster_slots: Arc<ClusterSlots>,
    pub block_engine_config: Arc<Mutex<BlockEngineConfig>>,
    pub relayer_config: Arc<Mutex<RelayerConfig>>,
    pub shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
    pub shred_retransmit_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
}
