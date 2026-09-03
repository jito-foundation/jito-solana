use {
    crate::banking_stage::consumer::RetryableIndex,
    jito_protos::proto::bam_types::TransactionCommittedResult,
    solana_clock::{BankId, Epoch, Slot},
    solana_transaction_error::TransactionError,
    std::fmt::Display,
};

/// A unique identifier for a transaction batch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TransactionBatchId(pub u64);

impl TransactionBatchId {
    pub fn new(index: u64) -> Self {
        Self(index)
    }
}

impl std::hash::Hash for TransactionBatchId {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(self.0)
    }
}

impl solana_nohash_hasher::IsEnabled for TransactionBatchId {}

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

#[cfg(test)]
impl MaxAge {
    pub const MAX: Self = Self {
        sanitized_epoch: Epoch::MAX,
        alt_invalidation_slot: Slot::MAX,
    };
}

/// Cost-model admission decided by the scheduler before dispatch.
///
/// The scheduler reserved the estimated cost of every `Ok(())` transaction in the cost tracker
/// of the bank identified by `bank_id`, in scheduling order. A worker that executes the work on
/// that same bank must not reserve again; it executes, commits, and settles the reservation
/// through the usual `QosService::remove_or_update_costs` path. On any other bank the
/// reservation is moot and the worker admits locally as if no admission were attached.
///
/// Ownership travels with the value: a worker takes the admission out of the work once it has
/// settled the reservation, so work that comes back to the scheduler still carrying one was not
/// settled (the bank completed or was replaced before execution) and the scheduler releases it
/// on `bank_id`'s bank.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CostAdmission {
    /// Bank whose cost tracker holds the reservation.
    pub bank_id: BankId,
    /// One entry per transaction: `Ok(())` reserved, `Err` rejected by the cost model.
    pub results: Vec<Result<(), TransactionError>>,
    /// Sum of the reserved estimates; checked against the worker's recomputation in debug builds.
    pub reserved_cost: u64,
}

impl CostAdmission {
    pub fn num_admitted(&self) -> usize {
        self.results.iter().filter(|result| result.is_ok()).count()
    }
}

/// Message: [Scheduler -> Worker]
/// Transactions to be consumed (i.e. executed, recorded, and committed)
pub struct ConsumeWork<Tx> {
    pub target_slot: Slot,
    pub batch_id: TransactionBatchId,
    pub ids: Vec<TransactionId>,
    pub transactions: Vec<Tx>,
    pub max_ages: Vec<MaxAge>,
    pub revert_on_error: bool,
    pub respond_with_extra_info: bool,
    pub max_schedule_slot: Option<Slot>,
    /// Cost admission already performed by the scheduler; `None` means the worker admits locally.
    /// A worker takes it out once the reservation is settled; see [`CostAdmission`].
    pub admission: Option<CostAdmission>,
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
