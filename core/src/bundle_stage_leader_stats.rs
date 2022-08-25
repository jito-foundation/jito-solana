use {
    crate::leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
    solana_poh::poh_recorder::BankStart,
    solana_runtime::transaction_error_metrics::TransactionErrorMetrics,
    solana_sdk::{clock::Slot, saturating_add_assign},
};

// Stats emitted only during leader slots
#[derive(Default)]
pub struct BundleStageLeaderSlotTrackingMetrics {
    pub(crate) current_slot: Option<Slot>,
    bundle_stage_leader_stats: BundleStageLeaderStats,
}

impl BundleStageLeaderSlotTrackingMetrics {
    pub fn maybe_report(&mut self, id: u32, bank_start: &Option<&BankStart>) {
        match (self.current_slot, bank_start) {
            // not was leader, not is leader
            (None, None) => {}
            // was leader, not leader anymore
            (Some(current_slot), None) => {
                self.bundle_stage_leader_stats.report(id, current_slot);
                self.bundle_stage_leader_stats = BundleStageLeaderStats::default();
            }
            // was leader, is leader
            (Some(current_slot), Some(bank_start)) => {
                if current_slot != bank_start.working_bank.slot() {
                    self.bundle_stage_leader_stats.report(id, current_slot);
                    self.bundle_stage_leader_stats = BundleStageLeaderStats::default();
                }
            }
            // not was leader, is leader
            (None, Some(_)) => {
                self.bundle_stage_leader_stats = BundleStageLeaderStats::default();
            }
        }

        self.current_slot = bank_start
            .as_ref()
            .map(|bank_start| bank_start.working_bank.slot());
    }

    pub fn bundle_stage_leader_stats(&mut self) -> &mut BundleStageLeaderStats {
        &mut self.bundle_stage_leader_stats
    }
}

#[derive(Default)]
pub struct BundleStageLeaderStats {
    transaction_errors: TransactionErrorMetrics,
    execute_and_commit_timings: LeaderExecuteAndCommitTimings,
    bundle_stage_stats: BundleStageStats,
}

impl BundleStageLeaderStats {
    pub fn transaction_errors(&mut self) -> &mut TransactionErrorMetrics {
        &mut self.transaction_errors
    }

    pub fn execute_and_commit_timings(&mut self) -> &mut LeaderExecuteAndCommitTimings {
        &mut self.execute_and_commit_timings
    }

    pub fn bundle_stage_stats(&mut self) -> &mut BundleStageStats {
        &mut self.bundle_stage_stats
    }

    pub fn report(&self, id: u32, slot: Slot) {
        self.transaction_errors.report(id, slot);
        self.execute_and_commit_timings.report(id, slot);
        self.bundle_stage_stats.report(id, slot);
    }
}

#[derive(Default)]
pub struct BundleStageStats {
    sanitize_transaction_ok: u64,
    sanitize_transaction_vote_only_mode: u64,
    sanitize_transaction_failed_precheck: u64,
    sanitize_transaction_blacklisted_account: u64,
    sanitize_transaction_failed_to_serialize: u64,
    sanitize_transaction_duplicate_transaction: u64,
    sanitize_transaction_failed_check: u64,
    sanitize_bundle_elapsed_us: u64,

    locked_bundle_elapsed_us: u64,

    num_lock_errors: u64,

    num_init_tip_account_errors: u64,
    num_init_tip_account_ok: u64,

    num_change_tip_receiver_errors: u64,
    num_change_tip_receiver_ok: u64,
    change_tip_receiver_elapsed_us: u64,

    num_execution_failures: u64,
    num_execution_timeouts: u64,
    num_execution_retries: u64,

    execute_locked_bundles_elapsed_us: u64,

    execution_results_ok: u64,
    execution_results_poh_max_height: u64,
    execution_results_transaction_failures: u64,
    execution_results_exceeds_cost_model: u64,
    execution_results_tip_errors: u64,
    execution_results_max_retries: u64,
    execution_results_lock_errors: u64,
}

impl BundleStageStats {
    pub fn report(&self, id: u32, slot: Slot) {
        datapoint_info!(
            "bundle_stage-stats",
            ("id", id, i64),
            ("slot", slot, i64),
            ("num_sanitized_ok", self.sanitize_transaction_ok, i64),
            (
                "sanitize_transaction_vote_only_mode",
                self.sanitize_transaction_vote_only_mode,
                i64
            ),
            (
                "sanitize_transaction_failed_precheck",
                self.sanitize_transaction_failed_precheck,
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
            ("num_execution_failures", self.num_execution_failures, i64),
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
            (
                "execution_results_lock_errors",
                self.execution_results_lock_errors,
                i64
            ),
        );
    }

    pub fn increment_sanitize_transaction_ok(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_ok, num);
    }

    pub fn increment_sanitize_transaction_vote_only_mode(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_vote_only_mode, num);
    }

    pub fn increment_sanitize_transaction_failed_precheck(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_failed_precheck, num);
    }

    pub fn increment_sanitize_transaction_blacklisted_account(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_blacklisted_account, num);
    }

    pub fn increment_sanitize_transaction_failed_to_serialize(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_failed_to_serialize, num);
    }

    pub fn increment_sanitize_transaction_duplicate_transaction(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_duplicate_transaction, num);
    }

    pub fn increment_sanitize_transaction_failed_check(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_transaction_failed_check, num);
    }

    pub fn increment_sanitize_bundle_elapsed_us(&mut self, num: u64) {
        saturating_add_assign!(self.sanitize_bundle_elapsed_us, num);
    }

    pub fn increment_locked_bundle_elapsed_us(&mut self, num: u64) {
        saturating_add_assign!(self.locked_bundle_elapsed_us, num);
    }

    pub fn increment_num_lock_errors(&mut self, num: u64) {
        saturating_add_assign!(self.num_lock_errors, num);
    }

    pub fn increment_num_init_tip_account_errors(&mut self, num: u64) {
        saturating_add_assign!(self.num_init_tip_account_errors, num);
    }

    pub fn increment_num_init_tip_account_ok(&mut self, num: u64) {
        saturating_add_assign!(self.num_init_tip_account_ok, num);
    }

    pub fn increment_num_change_tip_receiver_errors(&mut self, num: u64) {
        saturating_add_assign!(self.num_change_tip_receiver_errors, num);
    }

    pub fn increment_num_change_tip_receiver_ok(&mut self, num: u64) {
        saturating_add_assign!(self.num_change_tip_receiver_ok, num);
    }

    pub fn increment_change_tip_receiver_elapsed_us(&mut self, num: u64) {
        saturating_add_assign!(self.change_tip_receiver_elapsed_us, num);
    }

    pub fn increment_num_execution_failures(&mut self, num: u64) {
        saturating_add_assign!(self.num_execution_failures, num);
    }

    pub fn increment_num_execution_timeouts(&mut self, num: u64) {
        saturating_add_assign!(self.num_execution_timeouts, num);
    }

    pub fn increment_num_execution_retries(&mut self, num: u64) {
        saturating_add_assign!(self.num_execution_retries, num);
    }

    pub fn increment_execute_locked_bundles_elapsed_us(&mut self, num: u64) {
        saturating_add_assign!(self.execute_locked_bundles_elapsed_us, num);
    }

    pub fn increment_execution_results_ok(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_ok, num);
    }

    pub fn increment_execution_results_poh_max_height(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_poh_max_height, num);
    }

    pub fn increment_execution_results_transaction_failures(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_transaction_failures, num);
    }

    pub fn increment_execution_results_exceeds_cost_model(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_exceeds_cost_model, num);
    }

    pub fn increment_execution_results_tip_errors(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_tip_errors, num);
    }

    pub fn increment_execution_results_max_retries(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_max_retries, num);
    }

    pub fn increment_execution_results_lock_errors(&mut self, num: u64) {
        saturating_add_assign!(self.execution_results_lock_errors, num);
    }
}
