/// Dependencies that are needed for the BAM (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{atomic::AtomicU8, Arc, Mutex};
use {
    crate::proxy::block_engine_stage::BlockBuilderFeeInfo,
    jito_protos::proto::{
        bam_api::{scheduler_message::VersionedMsg, SchedulerMessage, SchedulerMessageV0},
        bam_types,
    },
    solana_gossip::cluster_info::ClusterInfo,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    std::sync::RwLock,
};

pub enum BamOutboundMessage {
    AtomicTxnBatchResult(bam_types::AtomicTxnBatchResult),
    Heartbeat(bam_types::ValidatorHeartBeat),
    LeaderState(bam_types::LeaderState),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BamConnectionState {
    Disconnected = 0,
    Connecting = 1,
    Connected = 2,
}

impl BamConnectionState {
    pub fn from_u8(state: u8) -> Self {
        match state {
            1 => Self::Connecting,
            2 => Self::Connected,
            _ => Self::Disconnected,
        }
    }
}

#[derive(Clone)]
pub struct BamDependencies {
    /// 0 = disconnected, 1 = connecting, 2 = connected
    pub bam_enabled: Arc<AtomicU8>,

    pub batch_sender: crossbeam_channel::Sender<bam_types::AtomicTxnBatch>,
    pub batch_receiver: crossbeam_channel::Receiver<bam_types::AtomicTxnBatch>,

    pub outbound_sender: crossbeam_channel::Sender<BamOutboundMessage>,
    pub outbound_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,

    pub cluster_info: Arc<ClusterInfo>,
    pub block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub bam_node_pubkey: Arc<Mutex<Pubkey>>,
    pub bam_tpu_info: Arc<RwLock<Option<(std::net::SocketAddr, std::net::SocketAddr)>>>,
}

pub fn v0_to_versioned_proto(v0: SchedulerMessageV0) -> SchedulerMessage {
    SchedulerMessage {
        versioned_msg: Some(VersionedMsg::V0(v0)),
    }
}
