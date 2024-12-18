use {
    crate::{
        banking_stage::{
            leader_slot_metrics::{self, LeaderSlotMetricsTracker},
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        immutable_deserialized_bundle::DeserializedBundleError,
    },
    solana_bundle::{
        bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
    },
    solana_poh::poh_recorder::BankStart,
    solana_sdk::{clock::Slot, saturating_add_assign},
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
            leader_slot_metrics_tracker: LeaderSlotMetricsTracker::new(id),
        }
    }

    pub(crate) fn check_leader_slot_boundary(
        &mut self,
        bank_start: Option<&BankStart>,
        unprocessed_transaction_storage: Option<&UnprocessedTransactionStorage>,
    ) -> (
        leader_slot_metrics::MetricsTrackerAction,
        MetricsTrackerAction,
    ) {
        let banking_stage_metrics_action = self
            .leader_slot_metrics_tracker
            .check_leader_slot_boundary(bank_start, unprocessed_transaction_storage);
        let bundle_stage_metrics_action = self
            .bundle_stage_metrics_tracker
            .check_leader_slot_boundary(bank_start);
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
        bank_start: Option<&BankStart>,
    ) -> MetricsTrackerAction {
        match (self.bundle_stage_metrics.as_mut(), bank_start) {
            (None, None) => MetricsTrackerAction::Noop,
            (Some(_), None) => MetricsTrackerAction::ReportAndResetTracker,
            // Our leader slot has begun, time to create a new slot tracker
            (None, Some(bank_start)) => MetricsTrackerAction::NewTracker(Some(
                BundleStageStats::new(self.id, bank_start.working_bank.slot()),
            )),
            (Some(bundle_stage_metrics), Some(bank_start)) => {
                if bundle_stage_metrics.slot != bank_start.working_bank.slot() {
                    // Last slot has ended, new slot has began
                    MetricsTrackerAction::ReportAndNewTracker(Some(BundleStageStats::new(
                        self.id,
                        bank_start.working_bank.slot(),
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
                    saturating_add_assign!(bundle_stage_metrics.sanitize_transaction_ok, 1);
                }
                Err(e) => match e {
                    DeserializedBundleError::VoteOnlyMode => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_vote_only_mode,
                            1
                        );
                    }
                    DeserializedBundleError::BlacklistedAccount => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_blacklisted_account,
                            1
                        );
                    }
                    DeserializedBundleError::FailedToSerializeTransaction => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_to_serialize,
                            1
                        );
                    }
                    DeserializedBundleError::DuplicateTransaction => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_duplicate_transaction,
                            1
                        );
                    }
                    DeserializedBundleError::FailedCheckTransactions => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_check,
                            1
                        );
                    }
                    DeserializedBundleError::FailedToSerializePacket(_) => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_to_serialize,
                            1
                        );
                    }
                    DeserializedBundleError::EmptyBatch => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_empty_batch,
                            1
                        );
                    }
                    DeserializedBundleError::TooManyPackets => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_too_many_packets,
                            1
                        );
                    }
                    DeserializedBundleError::MarkedDiscard => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_marked_discard,
                            1
                        );
                    }
                    DeserializedBundleError::SignatureVerificationFailure => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_sig_verify_failed,
                            1
                        );
                    }
                    DeserializedBundleError::PacketFilterFailure(_) => {
                        saturating_add_assign!(
                            bundle_stage_metrics.sanitize_transaction_failed_sig_verify_failed,
                            1
                        );
                    }
                },
            }
        }
    }

    pub fn increment_bundle_execution_result(&mut self, result: &Result<(), BundleExecutionError>) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            match result {
                Ok(_) => {
                    saturating_add_assign!(bundle_stage_metrics.execution_results_ok, 1);
                }
                Err(BundleExecutionError::PohRecordError(_))
                | Err(BundleExecutionError::BankProcessingTimeLimitReached) => {
                    saturating_add_assign!(
                        bundle_stage_metrics.execution_results_poh_max_height,
                        1
                    );
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::ProcessingTimeExceeded(_),
                )) => {
                    saturating_add_assign!(bundle_stage_metrics.num_execution_timeouts, 1);
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::TransactionError { .. },
                )) => {
                    saturating_add_assign!(
                        bundle_stage_metrics.execution_results_transaction_failures,
                        1
                    );
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::LockError { .. },
                ))
                | Err(BundleExecutionError::LockError) => {
                    saturating_add_assign!(bundle_stage_metrics.num_lock_errors, 1);
                }
                Err(BundleExecutionError::ExceedsCostModel) => {
                    saturating_add_assign!(
                        bundle_stage_metrics.execution_results_exceeds_cost_model,
                        1
                    );
                }
                Err(BundleExecutionError::TipError(_)) => {
                    saturating_add_assign!(bundle_stage_metrics.execution_results_tip_errors, 1);
                }
                Err(BundleExecutionError::TransactionFailure(
                    LoadAndExecuteBundleError::InvalidPreOrPostAccounts,
                )) => {
                    saturating_add_assign!(bundle_stage_metrics.bad_argument, 1);
                }
            }
        }
    }

    pub(crate) fn increment_sanitize_bundle_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.sanitize_bundle_elapsed_us, count);
        }
    }

    pub(crate) fn increment_locked_bundle_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.locked_bundle_elapsed_us, count);
        }
    }

    pub(crate) fn increment_num_init_tip_account_errors(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.num_init_tip_account_errors, count);
        }
    }

    pub(crate) fn increment_num_init_tip_account_ok(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.num_init_tip_account_ok, count);
        }
    }

    pub(crate) fn increment_num_change_tip_receiver_errors(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.num_change_tip_receiver_errors, count);
        }
    }

    pub(crate) fn increment_num_change_tip_receiver_ok(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.num_change_tip_receiver_ok, count);
        }
    }

    pub(crate) fn increment_change_tip_receiver_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.change_tip_receiver_elapsed_us, count);
        }
    }

    pub(crate) fn increment_num_execution_retries(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(bundle_stage_metrics.num_execution_retries, count);
        }
    }

    pub(crate) fn increment_execute_locked_bundles_elapsed_us(&mut self, count: u64) {
        if let Some(bundle_stage_metrics) = &mut self.bundle_stage_metrics {
            saturating_add_assign!(
                bundle_stage_metrics.execute_locked_bundles_elapsed_us,
                count
            );
        }
    }
}

#[derive(Default)]
pub struct BundleStageStats {
    id: u32,
    slot: u64,
    is_reported: bool,

    sanitize_transaction_ok: u64,
    sanitize_transaction_vote_only_mode: u64,
    sanitize_transaction_blacklisted_account: u64,
    sanitize_transaction_failed_to_serialize: u64,
    sanitize_transaction_duplicate_transaction: u64,
    sanitize_transaction_failed_check: u64,
    sanitize_bundle_elapsed_us: u64,
    sanitize_transaction_failed_empty_batch: u64,
    sanitize_transaction_failed_too_many_packets: u64,
    sanitize_transaction_failed_marked_discard: u64,
    sanitize_transaction_failed_sig_verify_failed: u64,
    packet_filter_failure: u64,

    locked_bundle_elapsed_us: u64,

    num_lock_errors: u64,

    num_init_tip_account_errors: u64,
    num_init_tip_account_ok: u64,

    num_change_tip_receiver_errors: u64,
    num_change_tip_receiver_ok: u64,
    change_tip_receiver_elapsed_us: u64,

    num_execution_timeouts: u64,
    num_execution_retries: u64,

    execute_locked_bundles_elapsed_us: u64,

    execution_results_ok: u64,
    execution_results_poh_max_height: u64,
    execution_results_transaction_failures: u64,
    execution_results_exceeds_cost_model: u64,
    execution_results_tip_errors: u64,
    execution_results_max_retries: u64,

    bad_argument: u64,
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
            ("num_sanitized_ok", self.sanitize_transaction_ok, i64),
            (
                "sanitize_transaction_vote_only_mode",
                self.sanitize_transaction_vote_only_mode,
                i64
            ),
            (
                "sanitize_transaction_blacklisted_account",
                self.sanitize_transaction_blacklisted_account,
                i64
            ),
            (
                "sanitize_transaction_failed_to_serialize",
                self.sanitize_transaction_failed_to_serialize,
                i64
            ),
            (
                "sanitize_transaction_duplicate_transaction",
                self.sanitize_transaction_duplicate_transaction,
                i64
            ),
            (
                "sanitize_transaction_failed_check",
                self.sanitize_transaction_failed_check,
                i64
            ),
            (
                "sanitize_bundle_elapsed_us",
                self.sanitize_bundle_elapsed_us,
                i64
            ),
            (
                "sanitize_transaction_failed_empty_batch",
                self.sanitize_transaction_failed_empty_batch,
                i64
            ),
            (
                "sanitize_transaction_failed_too_many_packets",
                self.sanitize_transaction_failed_too_many_packets,
                i64
            ),
            (
                "sanitize_transaction_failed_marked_discard",
                self.sanitize_transaction_failed_marked_discard,
                i64
            ),
            (
                "sanitize_transaction_failed_sig_verify_failed",
                self.sanitize_transaction_failed_sig_verify_failed,
                i64
            ),
            ("packet_filter_failure", self.packet_filter_failure, i64),
            (
                "locked_bundle_elapsed_us",
                self.locked_bundle_elapsed_us,
                i64
            ),
            ("num_lock_errors", self.num_lock_errors, i64),
            (
                "num_init_tip_account_errors",
                self.num_init_tip_account_errors,
                i64
            ),
            ("num_init_tip_account_ok", self.num_init_tip_account_ok, i64),
            (
                "num_change_tip_receiver_errors",
                self.num_change_tip_receiver_errors,
                i64
            ),
            (
                "num_change_tip_receiver_ok",
                self.num_change_tip_receiver_ok,
                i64
            ),
            (
                "change_tip_receiver_elapsed_us",
                self.change_tip_receiver_elapsed_us,
                i64
            ),
            ("num_execution_timeouts", self.num_execution_timeouts, i64),
            ("num_execution_retries", self.num_execution_retries, i64),
            (
                "execute_locked_bundles_elapsed_us",
                self.execute_locked_bundles_elapsed_us,
                i64
            ),
            ("execution_results_ok", self.execution_results_ok, i64),
            (
                "execution_results_poh_max_height",
                self.execution_results_poh_max_height,
                i64
            ),
            (
                "execution_results_transaction_failures",
                self.execution_results_transaction_failures,
                i64
            ),
            (
                "execution_results_exceeds_cost_model",
                self.execution_results_exceeds_cost_model,
                i64
            ),
            (
                "execution_results_tip_errors",
                self.execution_results_tip_errors,
                i64
            ),
            (
                "execution_results_max_retries",
                self.execution_results_max_retries,
                i64
            ),
            ("bad_argument", self.bad_argument, i64)
        );
    }
}
