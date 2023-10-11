use {
    crate::proxy::{block_engine_stage::BlockEngineConfig, relayer_stage::RelayerConfig},
    solana_gossip::cluster_info::ClusterInfo,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::pubkey::Pubkey,
    std::{
        collections::HashSet,
        net::SocketAddr,
        sync::{Arc, Mutex, RwLock},
    },
};

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub block_engine_config: Arc<Mutex<BlockEngineConfig>>,
    pub relayer_config: Arc<Mutex<RelayerConfig>>,
    pub shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
}
