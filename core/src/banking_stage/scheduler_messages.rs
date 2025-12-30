use {
    crate::banking_stage::consumer::RetryableIndex,
    jito_protos::proto::bam_types::TransactionCommittedResult,
    solana_clock::{Epoch, Slot},
    solana_transaction_error::TransactionError,
    std::fmt::Display,
};

/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub struct TransactionBatchId(pub u64);

impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

impl Display for TransactionBatchId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub type TransactionId = usize;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct MaxAge {
    pub sanitized_epoch: Epoch,
    pub alt_invalidation_slot: Slot,
}

impl MaxAge {
    pub const MAX: Self = Self {
        sanitized_epoch: Epoch::MAX,
        alt_invalidation_slot: Slot::MAX,
    };
}

/// Message: [Scheduler -> Worker]
/// Transactions to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork<Tx> {
    pub batch_id: TransactionBatchId,
    pub ids: Vec<TransactionId>,
    pub transactions: Vec<Tx>,
    pub max_ages: Vec<MaxAge>,
    pub revert_on_error: bool,
    pub respond_with_extra_info: bool,
    pub max_schedule_slot: Option<Slot>,
}

/// Message: [Worker -> Scheduler]
/// Processed transactions.
pub struct FinishedConsumeWork<Tx> {
    pub work: ConsumeWork<Tx>,
    pub retryable_indexes: Vec<RetryableIndex>,
    pub extra_info: Option<FinishedConsumeWorkExtraInfo>,
}

#[derive(Debug)]
pub struct FinishedConsumeWorkExtraInfo {
    pub processed_results: Vec<TransactionResult>,
}

#[derive(Clone, Debug)]
pub enum TransactionResult {
    Committed(TransactionCommittedResult),
    NotCommitted(NotCommittedReason),
}

#[derive(Clone, Debug)]
pub enum NotCommittedReason {
    PohTimeout,
    Error(TransactionError),
}
