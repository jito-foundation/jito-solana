use {
    crate::leader_slot_banking_stage_timing_metrics::*,
    solana_poh::poh_recorder::BankStart,
    solana_runtime::transaction_error_metrics::*,
    solana_sdk::{clock::Slot, saturating_add_assign},
    std::time::Instant,
};

/// A summary of what happened to transactions passed to the execution pipeline.
/// Transactions can
/// 1) Did not even make it to execution due to being filtered out by things like AccountInUse
/// lock conflicts or CostModel compute limits. These types of errors are retryable and
/// counted in `Self::retryable_transaction_indexes`.
/// 2) Did not execute due to some fatal error like too old, or duplicate signature. These
/// will be dropped from the transactions queue and not counted in `Self::retryable_transaction_indexes`
/// 3) Were executed and committed, captured by `committed_transactions_count` below.
/// 4) Were executed and failed commit, captured by `failed_commit_count` below.
pub(crate) struct ProcessTransactionsSummary {
    // Returns true if we hit the end of the block/max PoH height for the block before
    // processing all the transactions in the batch.
    pub reached_max_poh_height: bool,

    // Total number of transactions that were passed as candidates for execution. See description
    // of struct above for possible outcomes for these transactions
    pub transactions_attempted_execution_count: usize,

    // Total number of transactions that made it into the block
    pub committed_transactions_count: usize,

    // Total number of transactions that made it into the block where the transactions
    // output from execution was success/no error.
    pub committed_transactions_with_successful_result_count: usize,

    // All transactions that were executed but then failed record because the
    // slot ended
    pub failed_commit_count: usize,

    // Indexes of transactions in the transactions slice that were not committed but are retryable
    pub retryable_transaction_indexes: Vec<usize>,

    // The number of transactions filtered out by the cost model
    pub cost_model_throttled_transactions_count: usize,

    // Total amount of time spent running the cost model
    pub cost_model_us: u64,

    // Breakdown of time spent executing and comitting transactions
    pub execute_and_commit_timings: LeaderExecuteAndCommitTimings,

    // Breakdown of all the transaction errors from transactions passed for execution
    pub error_counters: TransactionErrorMetrics,
}

// Metrics describing packets ingested/processed in various parts of BankingStage during this
// validator's leader slot
#[derive(Debug, Default)]
struct LeaderSlotPacketCountMetrics {
    // total number of live packets TPU received from verified receiver for processing.
    total_new_valid_packets: u64,

    // total number of packets TPU received from sigverify that failed signature verification.
    newly_failed_sigverify_count: u64,

    // total number of dropped packet due to the thread's buffered packets capacity being reached.
    exceeded_buffer_limit_dropped_packets_count: u64,

    // total number of packets that got added to the pending buffer after arriving to BankingStage
    newly_buffered_packets_count: u64,

    // total number of transactions in the buffer that were filtered out due to things like age and
    // duplicate signature checks
    retryable_packets_filtered_count: u64,

    // total number of transactions that attempted execution in this slot. Should equal the sum
    // of `committed_transactions_count`, `retryable_errored_transaction_count`, and
    // `nonretryable_errored_transactions_count`.
    transactions_attempted_execution_count: u64,

    // total number of transactions that were executed and committed into the block
    // on this thread
    committed_transactions_count: u64,

    // total number of transactions that were executed, got a successful execution output/no error,
    // and were then committed into the block
    committed_transactions_with_successful_result_count: u64,

    // total number of transactions that were not executed or failed commit, BUT were added back to the buffered
    // queue becaus they were retryable errors
    retryable_errored_transaction_count: u64,

    // The size of the unprocessed buffer at the end of the slot
    end_of_slot_unprocessed_buffer_len: u64,

    // total number of transactions that were rebuffered into the queue after not being
    // executed on a previous pass
    retryable_packets_count: u64,

    // total number of transactions that attempted execution due to some fatal error (too old, duplicate signature, etc.)
    // AND were dropped from the buffered queue
    nonretryable_errored_transactions_count: u64,

    // total number of transactions that were executed, but failed to be committed into the Poh stream because
    // the block ended. Some of these may be already counted in `nonretryable_errored_transactions_count` if they
    // then hit the age limit after failing to be comitted.
    executed_transactions_failed_commit_count: u64,

    // total number of transactions that were excluded from the block because they were too expensive
    // according to the cost model. These transactions are added back to the buffered queue and are
    // already counted in `self.retrayble_errored_transaction_count`.
    cost_model_throttled_transactions_count: u64,

    // total number of forwardsable packets that failed forwarding
    failed_forwarded_packets_count: u64,

    // total number of forwardsable packets that were successfully forwarded
    successful_forwarded_packets_count: u64,

    // total number of attempted forwards that failed. Note this is not a count of the number of packets
    // that failed, just the total number of batches of packets that failed forwarding
    packet_batch_forward_failure_count: u64,

    // total number of valid unprocessed packets in the buffer that were removed after being forwarded
    cleared_from_buffer_after_forward_count: u64,

    // total number of packets removed at the end of the slot due to being too old, duplicate, etc.
    end_of_slot_filtered_invalid_count: u64,
}

impl LeaderSlotPacketCountMetrics {
    fn new() -> Self {
        Self { ..Self::default() }
    }

    fn report(&self, id: u32, slot: Slot) {
        datapoint_info!(
            "banking_stage-leader_slot_packet_counts",
            ("id", id as i64, i64),
            ("slot", slot as i64, i64),
            (
                "total_new_valid_packets",
                self.total_new_valid_packets as i64,
                i64
            ),
            (
                "newly_failed_sigverify_count",
                self.newly_failed_sigverify_count as i64,
                i64
            ),
            (
                "exceeded_buffer_limit_dropped_packets_count",
                self.exceeded_buffer_limit_dropped_packets_count as i64,
                i64
            ),
            (
                "newly_buffered_packets_count",
                self.newly_buffered_packets_count as i64,
                i64
            ),
            (
                "retryable_packets_filtered_count",
                self.retryable_packets_filtered_count as i64,
                i64
            ),
            (
                "transactions_attempted_execution_count",
                self.transactions_attempted_execution_count as i64,
                i64
            ),
            (
                "committed_transactions_count",
                self.committed_transactions_count as i64,
                i64
            ),
            (
                "committed_transactions_with_successful_result_count",
                self.committed_transactions_with_successful_result_count as i64,
                i64
            ),
            (
                "retryable_errored_transaction_count",
                self.retryable_errored_transaction_count as i64,
                i64
            ),
            (
                "retryable_packets_count",
                self.retryable_packets_count as i64,
                i64
            ),
            (
                "nonretryable_errored_transactions_count",
                self.nonretryable_errored_transactions_count as i64,
                i64
            ),
            (
                "executed_transactions_failed_commit_count",
                self.executed_transactions_failed_commit_count as i64,
                i64
            ),
            (
                "cost_model_throttled_transactions_count",
                self.cost_model_throttled_transactions_count as i64,
                i64
            ),
            (
                "failed_forwarded_packets_count",
                self.failed_forwarded_packets_count as i64,
                i64
            ),
            (
                "successful_forwarded_packets_count",
                self.successful_forwarded_packets_count as i64,
                i64
            ),
            (
                "packet_batch_forward_failure_count",
                self.packet_batch_forward_failure_count as i64,
                i64
            ),
            (
                "cleared_from_buffer_after_forward_count",
                self.cleared_from_buffer_after_forward_count as i64,
                i64
            ),
            (
                "end_of_slot_filtered_invalid_count",
                self.end_of_slot_filtered_invalid_count as i64,
                i64
            ),
            (
                "end_of_slot_unprocessed_buffer_len",
                self.end_of_slot_unprocessed_buffer_len as i64,
                i64
            ),
        );
    }
}

#[derive(Debug)]
pub(crate) struct LeaderSlotMetrics {
    // banking_stage creates one QosService instance per working threads, that is uniquely
    // identified by id. This field allows to categorize metrics for gossip votes, TPU votes
    // and other transactions.
    id: u32,

    // aggregate metrics per slot
    slot: Slot,

    packet_count_metrics: LeaderSlotPacketCountMetrics,

    transaction_error_metrics: TransactionErrorMetrics,

    timing_metrics: LeaderSlotTimingMetrics,

    // Used by tests to check if the `self.report()` method was called
    is_reported: bool,
}

impl LeaderSlotMetrics {
    pub(crate) fn new(id: u32, slot: Slot, bank_creation_time: &Instant) -> Self {
        Self {
            id,
            slot,
            packet_count_metrics: LeaderSlotPacketCountMetrics::new(),
            transaction_error_metrics: TransactionErrorMetrics::new(),
            timing_metrics: LeaderSlotTimingMetrics::new(bank_creation_time),
            is_reported: false,
        }
    }

    pub(crate) fn report(&mut self) {
        self.is_reported = true;

        self.timing_metrics.report(self.id, self.slot);
        self.transaction_error_metrics.report(self.id, self.slot);
        self.packet_count_metrics.report(self.id, self.slot);
    }

    /// Returns `Some(self.slot)` if the metrics have been reported, otherwise returns None
    fn reported_slot(&self) -> Option<Slot> {
        if self.is_reported {
            Some(self.slot)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct LeaderSlotMetricsTracker {
    // Only `Some` if BankingStage detects it's time to construct our leader slot,
    // otherwise `None`
    leader_slot_metrics: Option<LeaderSlotMetrics>,
    id: u32,
}

impl LeaderSlotMetricsTracker {
    pub fn new(id: u32) -> Self {
        Self {
            leader_slot_metrics: None,
            id,
        }
    }

    // Returns reported slot if metrics were reported
    pub(crate) fn update_on_leader_slot_boundary(
        &mut self,
        bank_start: &Option<BankStart>,
    ) -> Option<Slot> {
        match (self.leader_slot_metrics.as_mut(), bank_start) {
            (None, None) => None,

            (Some(leader_slot_metrics), None) => {
                leader_slot_metrics.report();
                // Ensure tests catch that `report()` method was called
                let reported_slot = leader_slot_metrics.reported_slot();
                // Slot has ended, time to report metrics
                self.leader_slot_metrics = None;
                reported_slot
            }

            (None, Some(bank_start)) => {
                // Our leader slot has begain, time to create a new slot tracker
                self.leader_slot_metrics = Some(LeaderSlotMetrics::new(
                    self.id,
                    bank_start.working_bank.slot(),
                    &bank_start.bank_creation_time,
                ));
                self.leader_slot_metrics.as_ref().unwrap().reported_slot()
            }

            (Some(leader_slot_metrics), Some(bank_start)) => {
                if leader_slot_metrics.slot != bank_start.working_bank.slot() {
                    // Last slot has ended, new slot has began
                    leader_slot_metrics.report();
                    // Ensure tests catch that `report()` method was called
                    let reported_slot = leader_slot_metrics.reported_slot();
                    self.leader_slot_metrics = Some(LeaderSlotMetrics::new(
                        self.id,
                        bank_start.working_bank.slot(),
                        &bank_start.bank_creation_time,
                    ));
                    reported_slot
                } else {
                    leader_slot_metrics.reported_slot()
                }
            }
        }
    }

    pub(crate) fn accumulate_process_transactions_summary(
        &mut self,
        process_transactions_summary: &ProcessTransactionsSummary,
    ) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            let ProcessTransactionsSummary {
                transactions_attempted_execution_count,
                committed_transactions_count,
                committed_transactions_with_successful_result_count,
                failed_commit_count,
                ref retryable_transaction_indexes,
                cost_model_throttled_transactions_count,
                cost_model_us,
                ref execute_and_commit_timings,
                ..
            } = process_transactions_summary;

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .transactions_attempted_execution_count,
                *transactions_attempted_execution_count as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .committed_transactions_count,
                *committed_transactions_count as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .committed_transactions_with_successful_result_count,
                *committed_transactions_with_successful_result_count as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .executed_transactions_failed_commit_count,
                *failed_commit_count as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .retryable_errored_transaction_count,
                retryable_transaction_indexes.len() as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .nonretryable_errored_transactions_count,
                transactions_attempted_execution_count
                    .saturating_sub(*committed_transactions_count)
                    .saturating_sub(retryable_transaction_indexes.len()) as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .cost_model_throttled_transactions_count,
                *cost_model_throttled_transactions_count as u64
            );

            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_packets_timings
                    .cost_model_us,
                *cost_model_us as u64
            );

            leader_slot_metrics
                .timing_metrics
                .execute_and_commit_timings
                .accumulate(execute_and_commit_timings);
        }
    }

    pub(crate) fn accumulate_transaction_errors(
        &mut self,
        error_metrics: &TransactionErrorMetrics,
    ) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            leader_slot_metrics
                .transaction_error_metrics
                .accumulate(error_metrics);
        }
    }

    // Packet inflow/outflow/processing metrics
    pub(crate) fn increment_total_new_valid_packets(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .total_new_valid_packets,
                count
            );
        }
    }

    pub(crate) fn increment_newly_failed_sigverify_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .newly_failed_sigverify_count,
                count
            );
        }
    }

    pub(crate) fn increment_exceeded_buffer_limit_dropped_packets_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .exceeded_buffer_limit_dropped_packets_count,
                count
            );
        }
    }

    pub(crate) fn increment_newly_buffered_packets_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .newly_buffered_packets_count,
                count
            );
        }
    }

    pub(crate) fn increment_retryable_packets_filtered_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .retryable_packets_filtered_count,
                count
            );
        }
    }

    pub(crate) fn increment_failed_forwarded_packets_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .failed_forwarded_packets_count,
                count
            );
        }
    }

    pub(crate) fn increment_successful_forwarded_packets_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .successful_forwarded_packets_count,
                count
            );
        }
    }

    pub(crate) fn increment_packet_batch_forward_failure_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .packet_batch_forward_failure_count,
                count
            );
        }
    }

    pub(crate) fn increment_cleared_from_buffer_after_forward_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .cleared_from_buffer_after_forward_count,
                count
            );
        }
    }

    pub(crate) fn increment_retryable_packets_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .retryable_packets_count,
                count
            );
        }
    }

    pub(crate) fn increment_end_of_slot_filtered_invalid_count(&mut self, count: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .packet_count_metrics
                    .end_of_slot_filtered_invalid_count,
                count
            );
        }
    }

    pub(crate) fn set_end_of_slot_unprocessed_buffer_len(&mut self, len: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            leader_slot_metrics
                .packet_count_metrics
                .end_of_slot_unprocessed_buffer_len = len;
        }
    }

    // Outermost banking thread's loop timing metrics
    pub(crate) fn increment_process_buffered_packets_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .outer_loop_timings
                    .process_buffered_packets_us,
                us
            );
        }
    }

    pub(crate) fn increment_slot_metrics_check_slot_boundary_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .outer_loop_timings
                    .slot_metrics_check_slot_boundary_us,
                us
            );
        }
    }

    pub(crate) fn increment_receive_and_buffer_packets_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .outer_loop_timings
                    .receive_and_buffer_packets_us,
                us
            );
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .outer_loop_timings
                    .receive_and_buffer_packets_invoked_count,
                1
            );
        }
    }

    // Processing buffer timing metrics
    pub(crate) fn increment_make_decision_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_buffered_packets_timings
                    .make_decision_us,
                us
            );
        }
    }

    pub(crate) fn increment_consume_buffered_packets_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_buffered_packets_timings
                    .consume_buffered_packets_us,
                us
            );
        }
    }

    pub(crate) fn increment_forward_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_buffered_packets_timings
                    .forward_us,
                us
            );
        }
    }

    pub(crate) fn increment_forward_and_hold_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_buffered_packets_timings
                    .forward_and_hold_us,
                us
            );
        }
    }

    // Consuming buffered packets timing metrics
    pub(crate) fn increment_end_of_slot_filtering_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .consume_buffered_packets_timings
                    .end_of_slot_filtering_us,
                us
            );
        }
    }

    pub(crate) fn increment_consume_buffered_packets_poh_recorder_lock_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .consume_buffered_packets_timings
                    .poh_recorder_lock_us,
                us
            );
        }
    }

    pub(crate) fn increment_process_packets_transactions_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .consume_buffered_packets_timings
                    .process_packets_transactions_us,
                us
            );
        }
    }

    // Processing packets timing metrics
    pub(crate) fn increment_transactions_from_packets_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_packets_timings
                    .transactions_from_packets_us,
                us
            );
        }
    }

    pub(crate) fn increment_process_transactions_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_packets_timings
                    .process_transactions_us,
                us
            );
        }
    }

    pub(crate) fn increment_filter_retryable_packets_us(&mut self, us: u64) {
        if let Some(leader_slot_metrics) = &mut self.leader_slot_metrics {
            saturating_add_assign!(
                leader_slot_metrics
                    .timing_metrics
                    .process_packets_timings
                    .filter_retryable_packets_us,
                us
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_runtime::{bank::Bank, genesis_utils::create_genesis_config},
        solana_sdk::pubkey::Pubkey,
        std::sync::Arc,
    };

    struct TestSlotBoundaryComponents {
        first_bank: Arc<Bank>,
        first_poh_recorder_bank: BankStart,
        next_bank: Arc<Bank>,
        next_poh_recorder_bank: BankStart,
        leader_slot_metrics_tracker: LeaderSlotMetricsTracker,
    }

    fn setup_test_slot_boundary_banks() -> TestSlotBoundaryComponents {
        let genesis = create_genesis_config(10);
        let first_bank = Arc::new(Bank::new_for_tests(&genesis.genesis_config));
        let first_poh_recorder_bank = BankStart {
            working_bank: first_bank.clone(),
            bank_creation_time: Arc::new(Instant::now()),
        };

        // Create a child descended from the first bank
        let next_bank = Arc::new(Bank::new_from_parent(
            &first_bank,
            &Pubkey::new_unique(),
            first_bank.slot() + 1,
        ));
        let next_poh_recorder_bank = BankStart {
            working_bank: next_bank.clone(),
            bank_creation_time: Arc::new(Instant::now()),
        };

        let banking_stage_thread_id = 0;
        let leader_slot_metrics_tracker = LeaderSlotMetricsTracker::new(banking_stage_thread_id);

        TestSlotBoundaryComponents {
            first_bank,
            first_poh_recorder_bank,
            next_bank,
            next_poh_recorder_bank,
            leader_slot_metrics_tracker,
        }
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_not_leader_to_not_leader() {
        let TestSlotBoundaryComponents {
            mut leader_slot_metrics_tracker,
            ..
        } = setup_test_slot_boundary_banks();
        // Test that with no bank being tracked, and no new bank being tracked, nothing is reported
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&None)
            .is_none());
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_not_leader_to_leader() {
        let TestSlotBoundaryComponents {
            first_poh_recorder_bank,
            mut leader_slot_metrics_tracker,
            ..
        } = setup_test_slot_boundary_banks();

        // Test case where the thread has not detected a leader bank, and now sees a leader bank.
        // Metrics should not be reported because leader slot has not ended
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank))
            .is_none());
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_some());
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_leader_to_not_leader() {
        let TestSlotBoundaryComponents {
            first_bank,
            first_poh_recorder_bank,
            mut leader_slot_metrics_tracker,
            ..
        } = setup_test_slot_boundary_banks();

        // Test case where the thread has a leader bank, and now detects there's no more leader bank,
        // implying the slot has ended. Metrics should be reported for `first_bank.slot()`,
        // because that leader slot has just ended.
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank))
            .is_none());
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&None)
                .unwrap(),
            first_bank.slot()
        );
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&None)
            .is_none());
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_leader_to_leader_same_slot() {
        let TestSlotBoundaryComponents {
            first_bank,
            first_poh_recorder_bank,
            mut leader_slot_metrics_tracker,
            ..
        } = setup_test_slot_boundary_banks();

        // Test case where the thread has a leader bank, and now detects the same leader bank,
        // implying the slot is still running. Metrics should not be reported
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank.clone()))
            .is_none());
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank))
            .is_none());
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&None)
                .unwrap(),
            first_bank.slot()
        );
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_leader_to_leader_bigger_slot() {
        let TestSlotBoundaryComponents {
            first_bank,
            first_poh_recorder_bank,
            next_bank,
            next_poh_recorder_bank,
            mut leader_slot_metrics_tracker,
        } = setup_test_slot_boundary_banks();

        // Test case where the thread has a leader bank, and now detects there's a new leader bank
        // for a bigger slot, implying the slot has ended. Metrics should be reported for the
        // smaller slot
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank))
            .is_none());
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&Some(next_poh_recorder_bank))
                .unwrap(),
            first_bank.slot()
        );
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&None)
                .unwrap(),
            next_bank.slot()
        );
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
    }

    #[test]
    pub fn test_update_on_leader_slot_boundary_leader_to_leader_smaller_slot() {
        let TestSlotBoundaryComponents {
            first_bank,
            first_poh_recorder_bank,
            next_bank,
            next_poh_recorder_bank,
            mut leader_slot_metrics_tracker,
        } = setup_test_slot_boundary_banks();
        // Test case where the thread has a leader bank, and now detects there's a new leader bank
        // for a samller slot, implying the slot has ended. Metrics should be reported for the
        // bigger slot
        assert!(leader_slot_metrics_tracker
            .update_on_leader_slot_boundary(&Some(next_poh_recorder_bank))
            .is_none());
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&Some(first_poh_recorder_bank))
                .unwrap(),
            next_bank.slot()
        );
        assert_eq!(
            leader_slot_metrics_tracker
                .update_on_leader_slot_boundary(&None)
                .unwrap(),
            first_bank.slot()
        );
        assert!(leader_slot_metrics_tracker.leader_slot_metrics.is_none());
    }
}
