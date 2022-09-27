use {
    crate::leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
    solana_runtime::transaction_error_metrics::TransactionErrorMetrics,
};

// Stats emitted only during leader slots
#[derive(Default)]
pub(crate) struct BundleStageLeaderStats {
    pub(crate) num_sanitized_ok: u64,
    pub(crate) sanitize_bundle_elapsed_us: u64,

    pub(crate) locked_bundle_elapsed_us: u64,

    pub(crate) transaction_errors: TransactionErrorMetrics,

    pub(crate) num_lock_errors: u64,

    pub(crate) num_tip_init_ok: u64,
    pub(crate) num_tip_init_error: u64,

    pub(crate) num_change_tip_receivers_ok: u64,
    pub(crate) num_change_tip_receivers_error: u64,

    pub(crate) execute_time_us: u64,

    pub(crate) execute_and_commit_timings: LeaderExecuteAndCommitTimings,
}

impl BundleStageLeaderStats {
    pub(crate) fn report(&self) {}
}
