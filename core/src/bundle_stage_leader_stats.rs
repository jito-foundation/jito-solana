use {
    crate::leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
    solana_poh::poh_recorder::BankStart,
    solana_runtime::transaction_error_metrics::TransactionErrorMetrics,
    solana_sdk::{clock::Slot, saturating_add_assign},
};

// Stats emitted only during leader slots
#[derive(Default)]
pub struct BundleStageLeaderSlotTrackingMetrics {
    last_slot_update: Option<Slot>,
    bundle_stage_leader_stats: BundleStageLeaderStats,
}

impl BundleStageLeaderSlotTrackingMetrics {
    pub fn maybe_report(&mut self, id: u32, bank_start: &Option<&BankStart>) {
        match (self.last_slot_update, bank_start) {
            // not was leader, not is leader
            (None, None) => {}
            // was leader, not leader anymore
            (Some(last_update), None) => {}
            // was leader, is leader
            (Some(last_update), Some(bank_start)) => {}
            // not was leader, is leader
            (None, Some(bank_start)) => {}
        }
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
}

#[derive(Default)]
pub struct BundleStageStats {
    num_sanitized_ok: u64,
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
}

impl BundleStageStats {
    pub fn increment_num_sanitized_ok(&mut self, num: u64) {
        saturating_add_assign!(self.num_sanitized_ok, num);
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
}
