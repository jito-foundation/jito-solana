use {
    crate::{
        banking_stage::leader_slot_metrics::{self, LeaderSlotMetricsTracker},
        immutable_deserialized_bundle::DeserializedBundleError,
    },
    solana_bundle::{
        bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
    },
    solana_clock::Slot,
    solana_runtime::bank::Bank,
    std::{num::Saturating, ops::AddAssign, sync::Arc},
};

pub struct BundleStageLeaderMetrics {
    bundle_stage_metrics_tracker: BundleStageStatsMetricsTracker,
    leader_slot_metrics_tracker: LeaderSlotMetricsTracker,
}

pub(crate) enum MetricsTrackerAction {
    Noop,
    ReportAndResetTracker,
    NewTracker(Option<BundleStageStats>),
    ReportAndNewTracker(Option<BundleStageStats>),
}

impl BundleStageLeaderMetrics {
    pub fn new(id: u32) -> Self {
        Self {
            bundle_stage_metrics_tracker: BundleStageStatsMetricsTracker::new(id),
            leader_slot_metrics_tracker: LeaderSlotMetricsTracker::default(),
        }
    }

    pub(crate) fn check_leader_slot_boundary(
        &mut self,
        bank: Option<&Arc<Bank>>,
    ) -> (
        leader_slot_metrics::MetricsTrackerAction,
        MetricsTrackerAction,
    ) {
        let banking_stage_metrics_action = self
            .leader_slot_metrics_tracker
            .check_leader_slot_boundary(bank);
        let bundle_stage_metrics_action = self
            .bundle_stage_metrics_tracker
            .check_leader_slot_boundary(bank);
        (banking_stage_metrics_action, bundle_stage_metrics_action)
    }

    pub(crate) fn apply_action(
        &mut self,
        banking_stage_metrics_action: leader_slot_metrics::MetricsTrackerAction,
        bundle_stage_metrics_action: MetricsTrackerAction,
    ) -> Option<Slot> {
        self.leader_slot_metrics_tracker
            .apply_action(banking_stage_metrics_action);
        self.bundle_stage_metrics_tracker
            .apply_action(bundle_stage_metrics_action)
    }

    pub fn leader_slot_metrics_tracker(&mut self) -> &mut LeaderSlotMetricsTracker {
        &mut self.leader_slot_metrics_tracker
    }

    pub fn bundle_stage_metrics_tracker(&mut self) -> &mut BundleStageStatsMetricsTracker {
        &mut self.bundle_stage_metrics_tracker
    }
}

pub struct BundleStageStatsMetricsTracker {
    bundle_stage_metrics: Option<BundleStageStats>,
    id: u32,
}

impl BundleStageStatsMetricsTracker {
    pub fn new(id: u32) -> Self {
        Self {
            bundle_stage_metrics: None,
            id,
        }
    }

    /// Similar to as LeaderSlotMetricsTracker::check_leader_slot_boundary
    pub(crate) fn check_leader_slot_boundary(
        &mut self,
        bank: Option<&Arc<Bank>>,
    ) -> MetricsTrackerAction {
        match (self.bundle_stage_metrics.as_mut(), bank) {
            (None, None) => MetricsTrackerAction::Noop,
            (Some(_), None) => MetricsTrackerAction::ReportAndResetTracker,
            // Our leader slot has begun, time to create a new slot tracker
            (None, Some(bank)) => {
                MetricsTrackerAction::NewTracker(Some(BundleStageStats::new(self.id, bank.slot())))
            }
            (Some(bundle_stage_metrics), Some(bank)) => {
                if bundle_stage_metrics.slot != bank.slot() {
                    // Last slot has ended, new slot has began
                    MetricsTrackerAction::ReportAndNewTracker(Some(BundleStageStats::new(
                        self.id,
                        bank.slot(),
                    )))
                } else {
                    MetricsTrackerAction::Noop
                }
            }
        }
    }

    /// Similar to LeaderSlotMetricsTracker::apply_action
    pub(crate) fn apply_action(&mut self, action: MetricsTrackerAction) -> Option<Slot> {
        match action {
            MetricsTrackerAction::Noop => None,
            MetricsTrackerAction::ReportAndResetTracker => {
                let mut reported_slot = None;
                if let Some(bundle_stage_metrics) = self.bundle_stage_metrics.as_mut() {
                    bundle_stage_metrics.report();
                    reported_slot = bundle_stage_metrics.reported_slot();
                }
                self.bundle_stage_metrics = None;
                reported_slot
            }
            MetricsTrackerAction::NewTracker(new_bundle_stage_metrics) => {
                self.bundle_stage_metrics = new_bundle_stage_metrics;
                self.bundle_stage_metrics.as_ref().unwrap().reported_slot()
            }
            MetricsTrackerAction::ReportAndNewTracker(new_bundle_stage_metrics) => {
                let mut reported_slot = None;
                if let Some(bundle_stage_metrics) = self.bundle_stage_metrics.as_mut() {
                    bundle_stage_metrics.report();
                    reported_slot = bundle_stage_metrics.reported_slot();
                }
                self.bundle_stage_metrics = new_bundle_stage_metrics;
                reported_slot
            }
        }
    }

    pub(crate) fn increment_sanitize_transaction_result(
        &mut self,
        result: &Result<SanitizedBundle, DeserializedBundleError>,
    ) {
        if let Some(bundle_stage_metrics) = self.bundle_stage_metrics.as_mut() {
            match result {
                Ok(_) => {
                    bundle_stage_metrics
                        .sanitize_transaction_ok
                        .add_assign(Saturating(1));
                }
                Err(e) => match e {
                    DeserializedBundleError::VoteOnlyMode => {
                        bundle_stage_metrics
                            .sanitize_transaction_vote_only_mode
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::BlacklistedAccount => {
                        bundle_stage_metrics
                            .sanitize_transaction_blacklisted_account
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::FailedToSerializeTransaction => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_to_serialize
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::DuplicateTransaction => {
                        bundle_stage_metrics
                            .sanitize_transaction_duplicate_transaction
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::FailedCheckTransactions => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_check
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::FailedToSerializePacket(_) => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_to_serialize
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::EmptyBatch => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_empty_batch
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::TooManyPackets => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_too_many_packets
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::MarkedDiscard => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_marked_discard
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::SignatureVerificationFailure => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_sig_verify_failed
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::FailedVerifyPrecompiles => {
                        bundle_stage_metrics
                            .failed_verify_precompiles
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::TooManyAccountLocks => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_too_many_account_locks
                            .add_assign(Saturating(1));
                    }
                    DeserializedBundleError::InvalidComputeBudgetLimits => {
                        bundle_stage_metrics
                            .sanitize_transaction_failed_invalid_compute_budget_limits
                            .add_assign(Saturating(1));
                    }
                },
            }
        }
    }

    pub fn increment_bundle_execution_result(&mut self, result: &Result<(), BundleExecutionError>) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            match result {
                Ok(_) => {
                    bundle_stage_metrics
                        .execution_results_ok
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::PohRecordError(_))
                | Err(BundleExecutionError::BankProcessingTimeLimitReached) => {
                    bundle_stage_metrics
                        .execution_results_poh_max_height
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::ProcessingTimeExceeded(_),
                )) => {
                    bundle_stage_metrics
                        .num_execution_timeouts
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::TransactionError { .. },
                )) => {
                    bundle_stage_metrics
                        .execution_results_transaction_failures
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::LockError { .. },
                ))
                | Err(BundleExecutionError::LockError) => {
                    bundle_stage_metrics
                        .num_lock_errors
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::ExceedsCostModel) => {
                    bundle_stage_metrics
                        .execution_results_exceeds_cost_model
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::TipError(_)) => {
                    bundle_stage_metrics
                        .execution_results_tip_errors
                        .add_assign(Saturating(1));
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::InvalidPreOrPostAccounts,
                )) => {
                    bundle_stage_metrics.bad_argument.add_assign(Saturating(1));
                }
            }
        }
    }

    pub(crate) fn increment_sanitize_bundle_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .sanitize_bundle_elapsed_us
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_locked_bundle_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .locked_bundle_elapsed_us
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_num_init_tip_account_errors(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .num_init_tip_account_errors
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_num_init_tip_account_ok(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .num_init_tip_account_ok
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_num_change_tip_receiver_errors(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .num_change_tip_receiver_errors
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_num_change_tip_receiver_ok(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .num_change_tip_receiver_ok
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_change_tip_receiver_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .change_tip_receiver_elapsed_us
                .add_assign(Saturating(count));
        }
    }

    pub(crate) fn increment_num_execution_retries(&mut self, count: Saturating<u64>) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics.num_execution_retries.add_assign(count);
        }
    }

    pub(crate) fn increment_execute_locked_bundles_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            bundle_stage_metrics
                .execute_locked_bundles_elapsed_us
                .add_assign(Saturating(count));
        }
    }
}

#[derive(Default)]
pub struct BundleStageStats {
    id: u32,
    slot: u64,
    is_reported: bool,

    sanitize_transaction_ok: Saturating<u64>,
    sanitize_transaction_vote_only_mode: Saturating<u64>,
    sanitize_transaction_blacklisted_account: Saturating<u64>,
    sanitize_transaction_failed_to_serialize: Saturating<u64>,
    sanitize_transaction_duplicate_transaction: Saturating<u64>,
    sanitize_transaction_failed_check: Saturating<u64>,
    sanitize_bundle_elapsed_us: Saturating<u64>,
    sanitize_transaction_failed_empty_batch: Saturating<u64>,
    sanitize_transaction_failed_too_many_packets: Saturating<u64>,
    sanitize_transaction_failed_marked_discard: Saturating<u64>,
    sanitize_transaction_failed_sig_verify_failed: Saturating<u64>,
    failed_verify_precompiles: Saturating<u64>,

    locked_bundle_elapsed_us: Saturating<u64>,

    num_lock_errors: Saturating<u64>,

    num_init_tip_account_errors: Saturating<u64>,
    num_init_tip_account_ok: Saturating<u64>,

    num_change_tip_receiver_errors: Saturating<u64>,
    num_change_tip_receiver_ok: Saturating<u64>,
    change_tip_receiver_elapsed_us: Saturating<u64>,

    num_execution_timeouts: Saturating<u64>,
    num_execution_retries: Saturating<u64>,

    execute_locked_bundles_elapsed_us: Saturating<u64>,

    execution_results_ok: Saturating<u64>,
    execution_results_poh_max_height: Saturating<u64>,
    execution_results_transaction_failures: Saturating<u64>,
    execution_results_exceeds_cost_model: Saturating<u64>,
    execution_results_tip_errors: Saturating<u64>,
    execution_results_max_retries: Saturating<u64>,

    bad_argument: Saturating<u64>,

    sanitize_transaction_failed_too_many_account_locks: Saturating<u64>,
    sanitize_transaction_failed_invalid_compute_budget_limits: Saturating<u64>,
}

impl BundleStageStats {
    pub fn new(id: u32, slot: Slot) -> BundleStageStats {
        BundleStageStats {
            id,
            slot,
            is_reported: false,
            ..BundleStageStats::default()
        }
    }

    /// Returns `Some(self.slot)` if the metrics have been reported, otherwise returns None
    fn reported_slot(&self) -> Option<Slot> {
        if self.is_reported {
            Some(self.slot)
        } else {
            None
        }
    }

    pub fn report(&mut self) {
        self.is_reported = true;

        datapoint_info!(
            "bundle_stage-stats",
            ("id", self.id, i64),
            ("slot", self.slot, i64),
            ("num_sanitized_ok", self.sanitize_transaction_ok.0, i64),
            (
                "sanitize_transaction_vote_only_mode",
                self.sanitize_transaction_vote_only_mode.0,
                i64
            ),
            (
                "sanitize_transaction_blacklisted_account",
                self.sanitize_transaction_blacklisted_account.0,
                i64
            ),
            (
                "sanitize_transaction_failed_to_serialize",
                self.sanitize_transaction_failed_to_serialize.0,
                i64
            ),
            (
                "sanitize_transaction_duplicate_transaction",
                self.sanitize_transaction_duplicate_transaction.0,
                i64
            ),
            (
                "sanitize_transaction_failed_check",
                self.sanitize_transaction_failed_check.0,
                i64
            ),
            (
                "sanitize_bundle_elapsed_us",
                self.sanitize_bundle_elapsed_us.0,
                i64
            ),
            (
                "sanitize_transaction_failed_empty_batch",
                self.sanitize_transaction_failed_empty_batch.0,
                i64
            ),
            (
                "sanitize_transaction_failed_too_many_packets",
                self.sanitize_transaction_failed_too_many_packets.0,
                i64
            ),
            (
                "sanitize_transaction_failed_marked_discard",
                self.sanitize_transaction_failed_marked_discard.0,
                i64
            ),
            (
                "sanitize_transaction_failed_sig_verify_failed",
                self.sanitize_transaction_failed_sig_verify_failed.0,
                i64
            ),
            (
                "failed_verify_precompiles",
                self.failed_verify_precompiles.0,
                i64
            ),
            (
                "locked_bundle_elapsed_us",
                self.locked_bundle_elapsed_us.0,
                i64
            ),
            ("num_lock_errors", self.num_lock_errors.0, i64),
            (
                "num_init_tip_account_errors",
                self.num_init_tip_account_errors.0,
                i64
            ),
            (
                "num_init_tip_account_ok",
                self.num_init_tip_account_ok.0,
                i64
            ),
            (
                "num_change_tip_receiver_errors",
                self.num_change_tip_receiver_errors.0,
                i64
            ),
            (
                "num_change_tip_receiver_ok",
                self.num_change_tip_receiver_ok.0,
                i64
            ),
            (
                "change_tip_receiver_elapsed_us",
                self.change_tip_receiver_elapsed_us.0,
                i64
            ),
            ("num_execution_timeouts", self.num_execution_timeouts.0, i64),
            ("num_execution_retries", self.num_execution_retries.0, i64),
            (
                "execute_locked_bundles_elapsed_us",
                self.execute_locked_bundles_elapsed_us.0,
                i64
            ),
            ("execution_results_ok", self.execution_results_ok.0, i64),
            (
                "execution_results_poh_max_height",
                self.execution_results_poh_max_height.0,
                i64
            ),
            (
                "execution_results_transaction_failures",
                self.execution_results_transaction_failures.0,
                i64
            ),
            (
                "execution_results_exceeds_cost_model",
                self.execution_results_exceeds_cost_model.0,
                i64
            ),
            (
                "execution_results_tip_errors",
                self.execution_results_tip_errors.0,
                i64
            ),
            (
                "execution_results_max_retries",
                self.execution_results_max_retries.0,
                i64
            ),
            (
                "sanitize_transaction_failed_too_many_account_locks",
                self.sanitize_transaction_failed_too_many_account_locks.0,
                i64
            ),
            (
                "sanitize_transaction_failed_invalid_compute_budget_limits",
                self.sanitize_transaction_failed_invalid_compute_budget_limits
                    .0,
                i64
            ),
            ("bad_argument", self.bad_argument.0, i64)
        );
    }
}
