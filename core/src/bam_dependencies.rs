/// Dependencies that are needed for the BAM (Jito Scheduler Service) to function.
/// All-in-one for convenience.
use std::sync::{Arc, atomic::AtomicU8};
use {
    crate::{
        banking_stage::{committer::CommitTransactionDetails, consumer::Consumer},
        bundle_stage::bundle_account_locker::BundleAccountLocker,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::{TipManager, TipManagerError},
    },
    arc_swap::{ArcSwap, ArcSwapOption},
    jito_protos::proto::{
        bam_api::{SchedulerMessage, SchedulerMessageV0, scheduler_message::VersionedMsg},
        bam_types,
    },
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::{Bank, BankId},
        bank_forks::BankForks,
    },
    solana_transaction_error::TransactionError,
    solana_turbine::ShredReceiverAddresses,
    std::{
        sync::{Mutex, RwLock, TryLockError},
        time::{Duration, Instant},
    },
    thiserror::Error,
    tokio::sync::mpsc,
};

pub enum BamOutboundMessage {
    AtomicTxnBatchResult(bam_types::AtomicTxnBatchResult),
    LeaderState(bam_types::LeaderState),
}

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BamConnectionState {
    Disconnected = 0,
    Connecting = 1,
    DrainingBlockEngine = 2,
    BlockEngineDrained = 3,
    Connected = 4,
}

impl BamConnectionState {
    pub fn from_u8(state: u8) -> Self {
        match state {
            1 => Self::Connecting,
            2 => Self::DrainingBlockEngine,
            3 => Self::BlockEngineDrained,
            4 => Self::Connected,
            _ => Self::Disconnected,
        }
    }
}

#[derive(Clone)]
pub struct BamDependencies {
    /// Also carries the internal Block Engine drain state during BAM cutover.
    pub bam_enabled: Arc<AtomicU8>,

    pub batch_sender: crossbeam_channel::Sender<bam_types::MultipleAtomicTxnBatch>,
    pub batch_receiver: crossbeam_channel::Receiver<bam_types::MultipleAtomicTxnBatch>,

    pub outbound_sender: mpsc::Sender<BamOutboundMessage>,

    pub cluster_info: Arc<ClusterInfo>,
    pub block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    pub bank_forks: Arc<RwLock<BankForks>>,
    pub bam_node_pubkey: Arc<ArcSwap<Pubkey>>,
    pub bam_tpu_info: Arc<ArcSwap<Option<(std::net::SocketAddr, std::net::SocketAddr)>>>,
    pub bam_shred_receiver_addresses: Arc<ArcSwap<ShredReceiverAddresses>>,
}

/// Why tip programs could not be prepared on a Bank.
#[derive(Clone, Debug, Error, PartialEq)]
pub(crate) enum TipProgramsError {
    #[error("tip bundle construction failed: {0}")]
    Construction(#[from] TipManagerError),
    #[error("tip bundle commit failed: {0}")]
    Commit(#[from] TransactionError),
}

#[derive(Clone)]
pub struct TipProcessingDependencies {
    pub tip_manager: TipManager,
    pub block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
    pub cluster_info: Arc<ClusterInfo>,
    pub bundle_account_locker: BundleAccountLocker,
}

impl TipProcessingDependencies {
    /// Prepare this Bank on the scheduler thread using the metadata snapshot it caches.
    /// Reports each distinct failure once per slot, remembering the last in `last_error`.
    pub(crate) fn process_tip_programs(
        &self,
        consumer: &Consumer,
        bank: &Arc<Bank>,
        builder: &BlockBuilderFeeInfo,
        last_error: &mut Option<(Slot, TipProgramsError)>,
    ) -> bool {
        let bank_key = (bank.slot(), bank.bank_id());
        // Match BundleStage's local-cluster policy: free-rent PDAs are discarded by AccountsDb.
        if bank.rent_collector().rent.minimum_balance(0) == 0 {
            return true;
        }
        if builder.block_builder == Pubkey::default() {
            return false;
        }
        let keypair = self.cluster_info.keypair();
        let mut fail = |error: TipProgramsError| {
            if last_error
                .as_ref()
                .is_some_and(|(slot, last)| *slot == bank.slot() && *last == error)
            {
                return false;
            }
            let balance = bank.get_balance(&self.cluster_info.id());
            datapoint_error!(
                "bam_scheduler-tip_programs_error",
                ("slot", bank.slot(), i64),
                ("error", error.to_string(), String),
                ("identity_balance", balance, i64),
            );
            *last_error = Some((bank.slot(), error));
            false
        };
        let mut process = |bundle: crate::tip_manager::Result<SmallVec<[_; 2]>>| {
            let bundle = match bundle {
                Ok(bundle) => bundle,
                Err(error) => return fail(error.into()),
            };
            if bundle.is_empty() {
                return true;
            }
            let results = consumer
                .process_and_record_transactions_with_policy(
                    bank,
                    &bundle,
                    Some(&self.bundle_account_locker),
                    true,
                )
                .execute_and_commit_transactions_output
                .commit_transactions_result;
            debug!("tip bundle result for bank {bank_key:?}: {results:?}");
            // Recording opens after this Bank is published and closes before it ends; retry.
            let Ok(results) = results else {
                return false;
            };
            // A reverted bundle marks its other transactions CommitCancelled; report the cause.
            let Some(error) = results
                .into_iter()
                .filter_map(|result| match result {
                    CommitTransactionDetails::Committed { result, .. } => result.err(),
                    CommitTransactionDetails::NotCommitted(error) => Some(error),
                })
                .min_by_key(|error| *error == TransactionError::CommitCancelled)
            else {
                return true;
            };
            fail(error.into())
        };
        // Crank construction reads the accounts created by initialization; keep it lazy.
        process(
            self.tip_manager
                .get_initialize_tip_programs_bundle(bank, &keypair),
        ) && process(
            self.tip_manager
                .get_tip_programs_crank_bundle(bank, &keypair, builder),
        )
    }
}

/// Prepares the tip programs once per bank before external scheduler bundles execute on it.
pub(crate) struct TipPreparation {
    dependencies: TipProcessingDependencies,
    /// The bank and block builder the tip programs were last prepared for.
    prepared: ArcSwapOption<(BankId, Arc<BlockBuilderFeeInfo>)>,
    /// Held while cranking; a thread finding it taken reports not ready and is retried.
    crank: Mutex<TipPreparationState>,
}

/// Minimum delay between tip program crank attempts after a failure, as in BAM.
const TIP_RETRY_INTERVAL: Duration = Duration::from_millis(1);

#[derive(Default)]
struct TipPreparationState {
    retry_at: Option<Instant>,
    last_error: Option<(Slot, TipProgramsError)>,
}

impl TipPreparation {
    pub(crate) fn new(dependencies: TipProcessingDependencies) -> Self {
        Self {
            dependencies,
            prepared: ArcSwapOption::empty(),
            crank: Mutex::default(),
        }
    }

    /// Returns true once the tip programs are prepared on `bank` for the current block
    /// builder. Failures are retried after [`TIP_RETRY_INTERVAL`]. Never blocks: while another
    /// thread cranks, the caller gets false and the scheduler retries.
    pub(crate) fn prepare(&self, consumer: &Consumer, bank: &Arc<Bank>) -> bool {
        if self.is_prepared(bank, &self.dependencies.block_builder_fee_info.load()) {
            return true;
        }
        let mut state = match self.crank.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return false,
            Err(TryLockError::Poisoned(error)) => panic!("{error}"),
        };
        // Snapshot the builder under the lock so a crank never regresses to an older value.
        let builder = self.dependencies.block_builder_fee_info.load_full();
        if self.is_prepared(bank, &builder) {
            return true;
        }
        let now = Instant::now();
        if state.retry_at.is_some_and(|retry_at| now < retry_at) {
            return false;
        }
        let ready =
            self.dependencies
                .process_tip_programs(consumer, bank, &builder, &mut state.last_error);
        if ready {
            self.prepared
                .store(Some(Arc::new((bank.bank_id(), builder))));
        }
        state.retry_at = if ready {
            None
        } else {
            now.checked_add(TIP_RETRY_INTERVAL)
        };
        ready
    }

    fn is_prepared(&self, bank: &Bank, builder: &Arc<BlockBuilderFeeInfo>) -> bool {
        self.prepared.load().as_ref().is_some_and(|prepared| {
            prepared.0 == bank.bank_id() && Arc::ptr_eq(&prepared.1, builder)
        })
    }
}

#[cfg(test)]
impl TipPreparation {
    /// Tip preparation for `block_builder` with a fresh identity and tip manager.
    pub(crate) fn new_for_tests(block_builder: Pubkey) -> Arc<Self> {
        use {
            crate::tip_manager::{TipDistributionAccountConfig, TipManagerConfig},
            solana_gossip::node::Node,
            solana_keypair::Keypair,
            solana_net_utils::SocketAddrSpace,
            solana_program_binaries::{jito_tip_distribution, jito_tip_payment},
            solana_signer::Signer,
        };

        let leader_keypair = Keypair::new();
        let node = Node::new_localhost_with_pubkey(&leader_keypair.pubkey());
        Arc::new(Self::new(TipProcessingDependencies {
            tip_manager: TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::new_from_array(*jito_tip_payment::id().as_array()),
                tip_distribution_program_id: Pubkey::new_from_array(
                    *jito_tip_distribution::id().as_array(),
                ),
                tip_distribution_account_config: TipDistributionAccountConfig::default(),
            }),
            block_builder_fee_info: Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder,
                block_builder_commission: 0,
            })),
            cluster_info: Arc::new(ClusterInfo::new(
                node.info.clone(),
                Arc::new(leader_keypair),
                SocketAddrSpace::Unspecified,
            )),
            bundle_account_locker: BundleAccountLocker::default(),
        }))
    }
}

pub fn v0_to_versioned_proto(v0: SchedulerMessageV0) -> SchedulerMessage {
    SchedulerMessage {
        versioned_msg: Some(VersionedMsg::V0(v0)),
    }
}
