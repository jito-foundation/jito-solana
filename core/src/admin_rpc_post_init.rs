use {
    crate::{
        cluster_slots_service::cluster_slots::ClusterSlots,
        proxy::{block_engine_stage::BlockEngineConfig, relayer_stage::RelayerConfig},
        repair::{outstanding_requests::OutstandingRequests, serve_repair::ShredRepairType},
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_runtime::bank_forks::BankForks,
    solana_sdk::{pubkey::Pubkey, quic::NotifyKeyUpdate},
    std::{
        collections::HashSet,
        net::{SocketAddr, UdpSocket},
        sync::{Arc, Mutex, RwLock},
    },
};

#[derive(Clone)]
pub struct AdminRpcRequestMetadataPostInit {
    pub cluster_info: Arc<ClusterInfo>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub vote_account: Pubkey,
    pub repair_whitelist: Arc<RwLock<HashSet<Pubkey>>>,
    pub notifies: Vec<Arc<dyn NotifyKeyUpdate + Sync + Send>>,
    pub repair_socket: Arc<UdpSocket>,
    pub outstanding_repair_requests: Arc<RwLock<OutstandingRequests<ShredRepairType>>>,
    pub cluster_slots: Arc<ClusterSlots>,
    pub block_engine_config: Arc<Mutex<BlockEngineConfig>>,
    pub relayer_config: Arc<Mutex<RelayerConfig>>,
    pub shred_receiver_address: Arc<RwLock<Option<SocketAddr>>>,
}
