//! Control flow for BankingStage's transaction scheduler.
//!

use {
    super::{
        scheduler::Scheduler,
        scheduler_error::SchedulerError,
        scheduler_metrics::{
            SchedulerCountMetrics, SchedulerLeaderDetectionMetrics, SchedulerTimingMetrics,
        },
        transaction_id_generator::TransactionIdGenerator,
        transaction_state::SanitizedTransactionTTL,
        transaction_state_container::TransactionStateContainer,
    },
    crate::banking_stage::{
        consume_worker::ConsumeWorkerMetrics,
        consumer::Consumer,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        forwarder::Forwarder,
        immutable_deserialized_packet::ImmutableDeserializedPacket,
        packet_deserializer::PacketDeserializer,
        scheduler_messages::MaxAge,
        ForwardOption, LikeClusterInfo, TOTAL_BUFFERED_PACKETS,
    },
    arrayvec::ArrayVec,
    crossbeam_channel::RecvTimeoutError,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_cost_model::cost_model::CostModel,
    solana_measure::measure_us,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::instructions_processor::process_compute_budget_instructions,
    solana_sdk::{
        self,
        address_lookup_table::state::estimate_last_valid_slot,
        clock::{Epoch, Slot, FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET, MAX_PROCESSING_AGE},
        fee::FeeBudgetLimits,
        pubkey::Pubkey,
        saturating_add_assign,
        transaction::SanitizedTransaction,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    std::{
        collections::HashSet,
        sync::{Arc, RwLock},
        time::{Duration, Instant},
    },
};

/// Controls packet and transaction flow into scheduler, and scheduling execution.
pub(crate) struct SchedulerController<C, S>
where
    C: LikeClusterInfo,
    S: Scheduler,
{
    /// Decision maker for determining what should be done with transactions.
    decision_maker: DecisionMaker,
    /// Packet/Transaction ingress.
    packet_receiver: PacketDeserializer,
    bank_forks: Arc<RwLock<BankForks>>,
    /// Generates unique IDs for incoming transactions.
    transaction_id_generator: TransactionIdGenerator,
    /// Container for transaction state.
    /// Shared resource between `packet_receiver` and `scheduler`.
    container: TransactionStateContainer,
    /// State for scheduling and communicating with worker threads.
    scheduler: S,
    /// Metrics tracking time for leader bank detection.
    leader_detection_metrics: SchedulerLeaderDetectionMetrics,
    /// Metrics tracking counts on transactions in different states
    /// over an interval and during a leader slot.
    count_metrics: SchedulerCountMetrics,
    /// Metrics tracking time spent in difference code sections
    /// over an interval and during a leader slot.
    timing_metrics: SchedulerTimingMetrics,
    /// Metric report handles for the worker threads.
    worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
    /// State for forwarding packets to the leader, if enabled.
    forwarder: Option<Forwarder<C>>,
    blacklisted_accounts: HashSet<Pubkey>,
}

impl<C, S> SchedulerController<C, S>
where
    C: LikeClusterInfo,
    S: Scheduler,
{
    pub fn new(
        decision_maker: DecisionMaker,
        packet_deserializer: PacketDeserializer,
        bank_forks: Arc<RwLock<BankForks>>,
        scheduler: S,
        worker_metrics: Vec<Arc<ConsumeWorkerMetrics>>,
        forwarder: Option<Forwarder<C>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self {
            decision_maker,
            packet_receiver: packet_deserializer,
            bank_forks,
            transaction_id_generator: TransactionIdGenerator::default(),
            container: TransactionStateContainer::with_capacity(TOTAL_BUFFERED_PACKETS),
            scheduler,
            leader_detection_metrics: SchedulerLeaderDetectionMetrics::default(),
            count_metrics: SchedulerCountMetrics::default(),
            timing_metrics: SchedulerTimingMetrics::default(),
            worker_metrics,
            forwarder,
            blacklisted_accounts,
        }
    }

    pub fn run(mut self) -> Result<(), SchedulerError> {
        loop {
            // BufferedPacketsDecision is shared with legacy BankingStage, which will forward
            // packets. Initially, not renaming these decision variants but the actions taken
            // are different, since new BankingStage will not forward packets.
            // For `Forward` and `ForwardAndHold`, we want to receive packets but will not
            // forward them to the next leader. In this case, `ForwardAndHold` is
            // indistinguishable from `Hold`.
            //
            // `Forward` will drop packets from the buffer instead of forwarding.
            // During receiving, since packets would be dropped from buffer anyway, we can
            // bypass sanitization and buffering and immediately drop the packets.
            let (decision, decision_time_us) =
                measure_us!(self.decision_maker.make_consume_or_forward_decision());
            self.timing_metrics.update(|timing_metrics| {
                saturating_add_assign!(timing_metrics.decision_time_us, decision_time_us);
            });
            let new_leader_slot = decision.bank_start().map(|b| b.working_bank.slot());
            self.leader_detection_metrics
                .update_and_maybe_report(decision.bank_start());
            self.count_metrics
                .maybe_report_and_reset_slot(new_leader_slot);
            self.timing_metrics
                .maybe_report_and_reset_slot(new_leader_slot);

            self.receive_completed()?;
            self.process_transactions(&decision)?;
            if !self.receive_and_buffer_packets(&decision) {
                break;
            }
            // Report metrics only if there is data.
            // Reset intervals when appropriate, regardless of report.
            let should_report = self.count_metrics.interval_has_data();
            let priority_min_max = self.container.get_min_max_priority();
            self.count_metrics.update(|count_metrics| {
                count_metrics.update_priority_stats(priority_min_max);
            });
            self.count_metrics
                .maybe_report_and_reset_interval(should_report);
            self.timing_metrics
                .maybe_report_and_reset_interval(should_report);
            self.worker_metrics
                .iter()
                .for_each(|metrics| metrics.maybe_report_and_reset());
        }

        Ok(())
    }

    /// Process packets based on decision.
    fn process_transactions(
        &mut self,
        decision: &BufferedPacketsDecision,
    ) -> Result<(), SchedulerError> {
        let forwarding_enabled = self.forwarder.is_some();
        match decision {
            BufferedPacketsDecision::Consume(bank_start) => {
                let (scheduling_summary, schedule_time_us) = measure_us!(self.scheduler.schedule(
                    &mut self.container,
                    |txs, results| {
                        Self::pre_graph_filter(
                            txs,
                            results,
                            &bank_start.working_bank,
                            MAX_PROCESSING_AGE,
                        )
                    },
                    |tx| { pre_lock_filter(tx, &self.blacklisted_accounts) }
                )?);

                self.count_metrics.update(|count_metrics| {
                    saturating_add_assign!(
                        count_metrics.num_scheduled,
                        scheduling_summary.num_scheduled
                    );
                    saturating_add_assign!(
                        count_metrics.num_unschedulable,
                        scheduling_summary.num_unschedulable
                    );
                    saturating_add_assign!(
                        count_metrics.num_schedule_filtered_out,
                        scheduling_summary.num_filtered_out
                    );
                });

                self.timing_metrics.update(|timing_metrics| {
                    saturating_add_assign!(
                        timing_metrics.schedule_filter_time_us,
                        scheduling_summary.filter_time_us
                    );
                    saturating_add_assign!(timing_metrics.schedule_time_us, schedule_time_us);
                });
            }
            BufferedPacketsDecision::Forward => {
                if forwarding_enabled {
                    let (_, forward_time_us) = measure_us!(self.forward_packets(false));
                    self.timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.forward_time_us, forward_time_us);
                    });
                } else {
                    let (_, clear_time_us) = measure_us!(self.clear_container());
                    self.timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.clear_time_us, clear_time_us);
                    });
                }
            }
            BufferedPacketsDecision::ForwardAndHold => {
                if forwarding_enabled {
                    let (_, forward_time_us) = measure_us!(self.forward_packets(true));
                    self.timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.forward_time_us, forward_time_us);
                    });
                } else {
                    let (_, clean_time_us) = measure_us!(self.clean_queue());
                    self.timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.clean_time_us, clean_time_us);
                    });
                }
            }
            BufferedPacketsDecision::Hold => {}
        }

        Ok(())
    }

    fn pre_graph_filter(
        transactions: &[&SanitizedTransaction],
        results: &mut [bool],
        bank: &Bank,
        max_age: usize,
    ) {
        let lock_results = vec![Ok(()); transactions.len()];
        let mut error_counters = TransactionErrorMetrics::default();
        let check_results =
            bank.check_transactions(transactions, &lock_results, max_age, &mut error_counters);

        let fee_check_results: Vec<_> = check_results
            .into_iter()
            .zip(transactions)
            .map(|(result, tx)| {
                result?; // if there's already error do nothing
                Consumer::check_fee_payer_unlocked(bank, tx.message(), &mut error_counters)
            })
            .collect();

        for (fee_check_result, result) in fee_check_results.into_iter().zip(results.iter_mut()) {
            *result = fee_check_result.is_ok();
        }
    }

    /// Forward packets to the next leader.
    fn forward_packets(&mut self, hold: bool) {
        const MAX_FORWARDING_DURATION: Duration = Duration::from_millis(100);
        let start = Instant::now();
        let bank = self.bank_forks.read().unwrap().working_bank();
        let feature_set = &bank.feature_set;
        let forwarder = self.forwarder.as_mut().expect("forwarder must exist");

        // Pop from the container in chunks, filter using bank checks, then attempt to forward.
        // This doubles as a way to clean the queue as well as forwarding transactions.
        const CHUNK_SIZE: usize = 64;
        let mut num_forwarded: usize = 0;
        let mut ids_to_add_back = Vec::new();
        let mut max_time_reached = false;
        while !self.container.is_empty() {
            let mut filter_array = [true; CHUNK_SIZE];
            let mut ids = Vec::with_capacity(CHUNK_SIZE);
            let mut txs = Vec::with_capacity(CHUNK_SIZE);

            for _ in 0..CHUNK_SIZE {
                if let Some(id) = self.container.pop() {
                    ids.push(id);
                } else {
                    break;
                }
            }
            let chunk_size = ids.len();
            ids.iter().for_each(|id| {
                let transaction = self.container.get_transaction_ttl(&id.id).unwrap();
                txs.push(&transaction.transaction);
            });

            // use same filter we use for processing transactions:
            // age, already processed, fee-check.
            Self::pre_graph_filter(
                &txs,
                &mut filter_array,
                &bank,
                MAX_PROCESSING_AGE
                    .saturating_sub(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET as usize),
            );

            for (id, filter_result) in ids.iter().zip(&filter_array[..chunk_size]) {
                if !*filter_result {
                    self.container.remove_by_id(&id.id);
                    continue;
                }

                ids_to_add_back.push(*id); // add back to the queue at end
                let state = self.container.get_mut_transaction_state(&id.id).unwrap();
                let sanitized_transaction = &state.transaction_ttl().transaction;
                let immutable_packet = state.packet().clone();

                // If not already forwarded and can be forwarded, add to forwardable packets.
                if state.should_forward()
                    && forwarder.try_add_packet(
                        sanitized_transaction,
                        immutable_packet,
                        feature_set,
                    )
                {
                    saturating_add_assign!(num_forwarded, 1);
                    state.mark_forwarded();
                }
            }

            if start.elapsed() >= MAX_FORWARDING_DURATION {
                max_time_reached = true;
                break;
            }
        }

        // Forward each batch of transactions
        forwarder.forward_batched_packets(&ForwardOption::ForwardTransaction);
        forwarder.clear_batches();

        // If we hit the time limit. Drop everything that was not checked/processed.
        // If we cannot run these simple checks in time, then we cannot run them during
        // leader slot.
        if max_time_reached {
            while let Some(id) = self.container.pop() {
                self.container.remove_by_id(&id.id);
            }
        }

        if hold {
            for priority_id in ids_to_add_back {
                self.container.push_id_into_queue(priority_id);
            }
        } else {
            for priority_id in ids_to_add_back {
                self.container.remove_by_id(&priority_id.id);
            }
        }

        self.count_metrics.update(|count_metrics| {
            saturating_add_assign!(count_metrics.num_forwarded, num_forwarded);
        });
    }

    /// Clears the transaction state container.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clear_container(&mut self) {
        let mut num_dropped_on_clear: usize = 0;
        while let Some(id) = self.container.pop() {
            self.container.remove_by_id(&id.id);
            saturating_add_assign!(num_dropped_on_clear, 1);
        }

        self.count_metrics.update(|count_metrics| {
            saturating_add_assign!(count_metrics.num_dropped_on_clear, num_dropped_on_clear);
        });
    }

    /// Clean unprocessable transactions from the queue. These will be transactions that are
    /// expired, already processed, or are no longer sanitizable.
    /// This only clears pending transactions, and does **not** clear in-flight transactions.
    fn clean_queue(&mut self) {
        // Clean up any transactions that have already been processed, are too old, or do not have
        // valid nonce accounts.
        const MAX_TRANSACTION_CHECKS: usize = 10_000;
        let mut transaction_ids = Vec::with_capacity(MAX_TRANSACTION_CHECKS);

        while let Some(id) = self.container.pop() {
            transaction_ids.push(id);
        }

        let bank = self.bank_forks.read().unwrap().working_bank();

        const CHUNK_SIZE: usize = 128;
        let mut error_counters = TransactionErrorMetrics::default();
        let mut num_dropped_on_age_and_status: usize = 0;
        for chunk in transaction_ids.chunks(CHUNK_SIZE) {
            let lock_results = vec![Ok(()); chunk.len()];
            let sanitized_txs: Vec<_> = chunk
                .iter()
                .map(|id| {
                    &self
                        .container
                        .get_transaction_ttl(&id.id)
                        .expect("transaction must exist")
                        .transaction
                })
                .collect();

            let check_results = bank.check_transactions(
                &sanitized_txs,
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut error_counters,
            );

            for (result, id) in check_results.into_iter().zip(chunk.iter()) {
                if result.is_err() {
                    saturating_add_assign!(num_dropped_on_age_and_status, 1);
                    self.container.remove_by_id(&id.id);
                } else {
                    self.container.push_id_into_queue(*id);
                }
            }
        }

        self.count_metrics.update(|count_metrics| {
            saturating_add_assign!(
                count_metrics.num_dropped_on_age_and_status,
                num_dropped_on_age_and_status
            );
        });
    }

    /// Receives completed transactions from the workers and updates metrics.
    fn receive_completed(&mut self) -> Result<(), SchedulerError> {
        let ((num_transactions, num_retryable), receive_completed_time_us) =
            measure_us!(self.scheduler.receive_completed(&mut self.container)?);

        self.count_metrics.update(|count_metrics| {
            saturating_add_assign!(count_metrics.num_finished, num_transactions);
            saturating_add_assign!(count_metrics.num_retryable, num_retryable);
        });
        self.timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(
                timing_metrics.receive_completed_time_us,
                receive_completed_time_us
            );
        });

        Ok(())
    }

    /// Returns whether the packet receiver is still connected.
    fn receive_and_buffer_packets(&mut self, decision: &BufferedPacketsDecision) -> bool {
        const MAX_RECEIVE_PACKETS: usize = 5_000;

        const MAX_PACKET_RECEIVE_TIME: Duration = Duration::from_millis(10);
        let (recv_timeout, should_buffer) = match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => (
                if self.container.is_empty() {
                    MAX_PACKET_RECEIVE_TIME
                } else {
                    Duration::ZERO
                },
                true,
            ),
            BufferedPacketsDecision::Forward => (MAX_PACKET_RECEIVE_TIME, self.forwarder.is_some()),
            BufferedPacketsDecision::ForwardAndHold => (MAX_PACKET_RECEIVE_TIME, true),
        };

        let (received_packet_results, receive_time_us) = measure_us!(self
            .packet_receiver
            .receive_packets(recv_timeout, MAX_RECEIVE_PACKETS, |packet| {
                packet.check_excessive_precompiles()?;
                Ok(packet)
            }));

        self.timing_metrics.update(|timing_metrics| {
            saturating_add_assign!(timing_metrics.receive_time_us, receive_time_us);
        });

        match received_packet_results {
            Ok(receive_packet_results) => {
                let num_received_packets = receive_packet_results.deserialized_packets.len();

                self.count_metrics.update(|count_metrics| {
                    saturating_add_assign!(count_metrics.num_received, num_received_packets);
                });

                if should_buffer {
                    let (_, buffer_time_us) = measure_us!(
                        self.buffer_packets(receive_packet_results.deserialized_packets)
                    );
                    self.timing_metrics.update(|timing_metrics| {
                        saturating_add_assign!(timing_metrics.buffer_time_us, buffer_time_us);
                    });
                } else {
                    self.count_metrics.update(|count_metrics| {
                        saturating_add_assign!(
                            count_metrics.num_dropped_on_receive,
                            num_received_packets
                        );
                    });
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => return false,
        }

        true
    }

    fn buffer_packets(&mut self, packets: Vec<ImmutableDeserializedPacket>) {
        // Convert to Arcs
        let packets: Vec<_> = packets.into_iter().map(Arc::new).collect();
        // Sanitize packets, generate IDs, and insert into the container.
        let (root_bank, working_bank) = {
            let bank_forks = self.bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let vote_only = working_bank.vote_only_bank();

        const CHUNK_SIZE: usize = 128;
        let lock_results: [_; CHUNK_SIZE] = core::array::from_fn(|_| Ok(()));

        let mut arc_packets = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut transactions = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut max_ages = ArrayVec::<_, CHUNK_SIZE>::new();
        let mut fee_budget_limits_vec = ArrayVec::<_, CHUNK_SIZE>::new();

        let mut error_counts = TransactionErrorMetrics::default();
        for chunk in packets.chunks(CHUNK_SIZE) {
            let mut post_sanitization_count: usize = 0;
            chunk
                .iter()
                .filter_map(|packet| {
                    packet
                        .build_sanitized_transaction(
                            vote_only,
                            root_bank.as_ref(),
                            root_bank.get_reserved_account_keys(),
                        )
                        .map(|(tx, deactivation_slot)| (packet.clone(), tx, deactivation_slot))
                })
                .inspect(|_| saturating_add_assign!(post_sanitization_count, 1))
                .filter(|(_packet, tx, _deactivation_slot)| {
                    validate_account_locks(
                        tx.message().account_keys(),
                        transaction_account_lock_limit,
                    )
                    .is_ok()
                })
                .filter_map(|(packet, tx, deactivation_slot)| {
                    process_compute_budget_instructions(
                        SVMMessage::program_instructions_iter(&tx),
                        &working_bank.feature_set,
                    )
                    .map(|compute_budget| (packet, tx, deactivation_slot, compute_budget.into()))
                    .ok()
                })
                .for_each(|(packet, tx, deactivation_slot, fee_budget_limits)| {
                    arc_packets.push(packet);
                    transactions.push(tx);
                    max_ages.push(calculate_max_age(
                        sanitized_epoch,
                        deactivation_slot,
                        alt_resolved_slot,
                    ));
                    fee_budget_limits_vec.push(fee_budget_limits);
                });

            let check_results: Vec<
                Result<
                    solana_svm::account_loader::CheckedTransactionDetails,
                    solana_sdk::transaction::TransactionError,
                >,
            > = working_bank.check_transactions(
                &transactions,
                &lock_results[..transactions.len()],
                MAX_PROCESSING_AGE,
                &mut error_counts,
            );
            let post_lock_validation_count = transactions.len();

            let mut post_transaction_check_count: usize = 0;
            let mut num_dropped_on_capacity: usize = 0;
            let mut num_buffered: usize = 0;
            for ((((packet, transaction), max_age), fee_budget_limits), _check_result) in
                arc_packets
                    .drain(..)
                    .zip(transactions.drain(..))
                    .zip(max_ages.drain(..))
                    .zip(fee_budget_limits_vec.drain(..))
                    .zip(check_results)
                    .filter(|(_, check_result)| check_result.is_ok())
                    .filter(|((((_, tx), _), _), _)| {
                        Consumer::check_fee_payer_unlocked(
                            &working_bank,
                            tx.message(),
                            &mut error_counts,
                        )
                        .is_ok()
                    })
            {
                saturating_add_assign!(post_transaction_check_count, 1);
                let transaction_id = self.transaction_id_generator.next();

                let (priority, cost) = Self::calculate_priority_and_cost(
                    &transaction,
                    &fee_budget_limits,
                    &working_bank,
                );
                let transaction_ttl = SanitizedTransactionTTL {
                    transaction,
                    max_age,
                };

                if self.container.insert_new_transaction(
                    transaction_id,
                    transaction_ttl,
                    packet,
                    priority,
                    cost,
                ) {
                    saturating_add_assign!(num_dropped_on_capacity, 1);
                }
                saturating_add_assign!(num_buffered, 1);
            }

            // Update metrics for transactions that were dropped.
            let num_dropped_on_sanitization = chunk.len().saturating_sub(post_sanitization_count);
            let num_dropped_on_lock_validation =
                post_sanitization_count.saturating_sub(post_lock_validation_count);
            let num_dropped_on_transaction_checks =
                post_lock_validation_count.saturating_sub(post_transaction_check_count);

            self.count_metrics.update(|count_metrics| {
                saturating_add_assign!(
                    count_metrics.num_dropped_on_capacity,
                    num_dropped_on_capacity
                );
                saturating_add_assign!(count_metrics.num_buffered, num_buffered);
                saturating_add_assign!(
                    count_metrics.num_dropped_on_sanitization,
                    num_dropped_on_sanitization
                );
                saturating_add_assign!(
                    count_metrics.num_dropped_on_validate_locks,
                    num_dropped_on_lock_validation
                );
                saturating_add_assign!(
                    count_metrics.num_dropped_on_receive_transaction_checks,
                    num_dropped_on_transaction_checks
                );
            });
        }
    }

    /// Calculate priority and cost for a transaction:
    ///
    /// Cost is calculated through the `CostModel`,
    /// and priority is calculated through a formula here that attempts to sell
    /// blockspace to the highest bidder.
    ///
    /// The priority is calculated as:
    /// P = R / (1 + C)
    /// where P is the priority, R is the reward,
    /// and C is the cost towards block-limits.
    ///
    /// Current minimum costs are on the order of several hundred,
    /// so the denominator is effectively C, and the +1 is simply
    /// to avoid any division by zero due to a bug - these costs
    /// are calculated by the cost-model and are not direct
    /// from user input. They should never be zero.
    /// Any difference in the prioritization is negligible for
    /// the current transaction costs.
    fn calculate_priority_and_cost(
        transaction: &SanitizedTransaction,
        fee_budget_limits: &FeeBudgetLimits,
        bank: &Bank,
    ) -> (u64, u64) {
        let cost = CostModel::calculate_cost(transaction, &bank.feature_set).sum();
        let reward = bank.calculate_reward_for_transaction(transaction, fee_budget_limits);

        // We need a multiplier here to avoid rounding down too aggressively.
        // For many transactions, the cost will be greater than the fees in terms of raw lamports.
        // For the purposes of calculating prioritization, we multiply the fees by a large number so that
        // the cost is a small fraction.
        // An offset of 1 is used in the denominator to explicitly avoid division by zero.
        const MULTIPLIER: u64 = 1_000_000;
        (
            reward
                .saturating_mul(MULTIPLIER)
                .saturating_div(cost.saturating_add(1)),
            cost,
        )
    }
}

fn pre_lock_filter(tx: &SanitizedTransaction, blacklisted_accounts: &HashSet<Pubkey>) -> bool {
    !tx.message()
        .account_keys()
        .iter()
        .any(|a| blacklisted_accounts.contains(a))
}

/// Given the epoch, the minimum deactivation slot, and the current slot,
/// return the `MaxAge` that should be used for the transaction. This is used
/// to determine the maximum slot that a transaction will be considered valid
/// for, without re-resolving addresses or resanitizing.
///
/// This function considers the deactivation period of Address Table
/// accounts. If the deactivation period runs past the end of the epoch,
/// then the transaction is considered valid until the end of the epoch.
/// Otherwise, the transaction is considered valid until the deactivation
/// period.
///
/// Since the deactivation period technically uses blocks rather than
/// slots, the value used here is the lower-bound on the deactivation
/// period, i.e. the transaction's address lookups are valid until
/// AT LEAST this slot.
fn calculate_max_age(
    sanitized_epoch: Epoch,
    deactivation_slot: Slot,
    current_slot: Slot,
) -> MaxAge {
    let alt_min_expire_slot = estimate_last_valid_slot(deactivation_slot.min(current_slot));
    MaxAge {
        sanitized_epoch,
        alt_invalidation_slot: alt_min_expire_slot,
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            banking_stage::{
                consumer::TARGET_NUM_TRANSACTIONS_PER_BATCH,
                packet_deserializer::PacketDeserializer,
                scheduler_messages::{ConsumeWork, FinishedConsumeWork, TransactionBatchId},
                tests::create_slow_genesis_config,
                transaction_scheduler::prio_graph_scheduler::PrioGraphScheduler,
            },
            banking_trace::BankingPacketBatch,
            sigverify::SigverifyTracerPacketStats,
        },
        crossbeam_channel::{unbounded, Receiver, Sender},
        itertools::Itertools,
        solana_gossip::cluster_info::ClusterInfo,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::GenesisConfigInfo,
            get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::{to_packet_batches, PacketBatch, NUM_PACKETS},
        solana_poh::poh_recorder::{PohRecorder, Record, WorkingBankEntry},
        solana_runtime::bank::Bank,
        solana_sdk::{
            compute_budget::ComputeBudgetInstruction, fee_calculator::FeeRateGovernor, hash::Hash,
            message::Message, poh_config::PohConfig, pubkey::Pubkey, signature::Keypair,
            signer::Signer, system_instruction, system_transaction, transaction::Transaction,
        },
        std::sync::{atomic::AtomicBool, Arc, RwLock},
        tempfile::TempDir,
    };

    fn create_channels<T>(num: usize) -> (Vec<Sender<T>>, Vec<Receiver<T>>) {
        (0..num).map(|_| unbounded()).unzip()
    }

    // Helper struct to create tests that hold channels, files, etc.
    // such that our tests can be more easily set up and run.
    struct TestFrame {
        bank: Arc<Bank>,
        mint_keypair: Keypair,
        _ledger_path: TempDir,
        _entry_receiver: Receiver<WorkingBankEntry>,
        _record_receiver: Receiver<Record>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        banking_packet_sender: Sender<Arc<(Vec<PacketBatch>, Option<SigverifyTracerPacketStats>)>>,

        consume_work_receivers: Vec<Receiver<ConsumeWork>>,
        finished_consume_work_sender: Sender<FinishedConsumeWork>,
    }

    fn create_test_frame(
        num_threads: usize,
    ) -> (
        TestFrame,
        SchedulerController<Arc<ClusterInfo>, PrioGraphScheduler>,
    ) {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);
        genesis_config.fee_rate_governor = FeeRateGovernor::new(5000, 0);
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let (poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let decision_maker = DecisionMaker::new(Pubkey::new_unique(), poh_recorder.clone());

        let (banking_packet_sender, banking_packet_receiver) = unbounded();
        let packet_deserializer = PacketDeserializer::new(banking_packet_receiver);

        let (consume_work_senders, consume_work_receivers) = create_channels(num_threads);
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();

        let test_frame = TestFrame {
            bank,
            mint_keypair,
            _ledger_path: ledger_path,
            _entry_receiver: entry_receiver,
            _record_receiver: record_receiver,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            finished_consume_work_sender,
        };

        let scheduler_controller = SchedulerController::new(
            decision_maker,
            packet_deserializer,
            bank_forks,
            PrioGraphScheduler::new(consume_work_senders, finished_consume_work_receiver),
            vec![], // no actual workers with metrics to report, this can be empty
            None,
            HashSet::default(),
        );

        (test_frame, scheduler_controller)
    }

    fn create_and_fund_prioritized_transfer(
        bank: &Bank,
        mint_keypair: &Keypair,
        from_keypair: &Keypair,
        to_pubkey: &Pubkey,
        lamports: u64,
        compute_unit_price: u64,
        recent_blockhash: Hash,
    ) -> Transaction {
        // Fund the sending key, so that the transaction does not get filtered by the fee-payer check.
        {
            let transfer = system_transaction::transfer(
                mint_keypair,
                &from_keypair.pubkey(),
                500_000, // just some amount that will always be enough
                bank.last_blockhash(),
            );
            bank.process_transaction(&transfer).unwrap();
        }

        let transfer = system_instruction::transfer(&from_keypair.pubkey(), to_pubkey, lamports);
        let prioritization = ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price);
        let message = Message::new(&[transfer, prioritization], Some(&from_keypair.pubkey()));
        Transaction::new(&vec![from_keypair], message, recent_blockhash)
    }

    fn to_banking_packet_batch(txs: &[Transaction]) -> BankingPacketBatch {
        let packet_batch = to_packet_batches(txs, NUM_PACKETS);
        Arc::new((packet_batch, None))
    }

    // Helper function to let test receive and then schedule packets.
    // The order of operations here is convenient for testing, but does not
    // match the order of operations in the actual scheduler.
    // The actual scheduler will process immediately after the decision,
    // in order to keep the decision as recent as possible for processing.
    // In the tests, the decision will not become stale, so it is more convenient
    // to receive first and then schedule.
    fn test_receive_then_schedule(
        scheduler_controller: &mut SchedulerController<Arc<ClusterInfo>, impl Scheduler>,
    ) {
        let decision = scheduler_controller
            .decision_maker
            .make_consume_or_forward_decision();
        assert!(matches!(decision, BufferedPacketsDecision::Consume(_)));
        assert!(scheduler_controller.receive_completed().is_ok());
        assert!(scheduler_controller.receive_and_buffer_packets(&decision));
        assert!(scheduler_controller.process_transactions(&decision).is_ok());
    }

    #[test]
    #[should_panic(expected = "batch id 0 is not being tracked")]
    fn test_unexpected_batch_id() {
        let (test_frame, scheduler_controller) = create_test_frame(1);
        let TestFrame {
            finished_consume_work_sender,
            ..
        } = &test_frame;

        finished_consume_work_sender
            .send(FinishedConsumeWork {
                work: ConsumeWork {
                    batch_id: TransactionBatchId::new(0),
                    ids: vec![],
                    transactions: vec![],
                    max_ages: vec![],
                },
                retryable_indexes: vec![],
            })
            .unwrap();

        scheduler_controller.run().unwrap();
    }

    #[test]
    fn test_schedule_consume_single_threaded_no_conflicts() {
        let (test_frame, mut scheduler_controller) = create_test_frame(1);
        let TestFrame {
            bank,
            mint_keypair,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            ..
        } = &test_frame;

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        // Send packet batch to the scheduler - should do nothing until we become the leader.
        let tx1 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            1000,
            bank.last_blockhash(),
        );
        let tx2 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            2000,
            bank.last_blockhash(),
        );
        let tx1_hash = tx1.message().hash();
        let tx2_hash = tx2.message().hash();

        let txs = vec![tx1, tx2];
        banking_packet_sender
            .send(to_banking_packet_batch(&txs))
            .unwrap();

        test_receive_then_schedule(&mut scheduler_controller);
        let consume_work = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(consume_work.ids.len(), 2);
        assert_eq!(consume_work.transactions.len(), 2);
        let message_hashes = consume_work
            .transactions
            .iter()
            .map(|tx| tx.message_hash())
            .collect_vec();
        assert_eq!(message_hashes, vec![&tx2_hash, &tx1_hash]);
    }

    #[test]
    fn test_schedule_consume_single_threaded_conflict() {
        let (test_frame, mut scheduler_controller) = create_test_frame(1);
        let TestFrame {
            bank,
            mint_keypair,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            ..
        } = &test_frame;

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        let pk = Pubkey::new_unique();
        let tx1 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &pk,
            1,
            1000,
            bank.last_blockhash(),
        );
        let tx2 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &pk,
            1,
            2000,
            bank.last_blockhash(),
        );
        let tx1_hash = tx1.message().hash();
        let tx2_hash = tx2.message().hash();

        let txs = vec![tx1, tx2];
        banking_packet_sender
            .send(to_banking_packet_batch(&txs))
            .unwrap();

        // We expect 2 batches to be scheduled
        test_receive_then_schedule(&mut scheduler_controller);
        let consume_works = (0..2)
            .map(|_| consume_work_receivers[0].try_recv().unwrap())
            .collect_vec();

        let num_txs_per_batch = consume_works.iter().map(|cw| cw.ids.len()).collect_vec();
        let message_hashes = consume_works
            .iter()
            .flat_map(|cw| cw.transactions.iter().map(|tx| tx.message_hash()))
            .collect_vec();
        assert_eq!(num_txs_per_batch, vec![1; 2]);
        assert_eq!(message_hashes, vec![&tx2_hash, &tx1_hash]);
    }

    #[test]
    fn test_schedule_consume_single_threaded_multi_batch() {
        let (test_frame, mut scheduler_controller) = create_test_frame(1);
        let TestFrame {
            bank,
            mint_keypair,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            ..
        } = &test_frame;

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        // Send multiple batches - all get scheduled
        let txs1 = (0..2 * TARGET_NUM_TRANSACTIONS_PER_BATCH)
            .map(|i| {
                create_and_fund_prioritized_transfer(
                    bank,
                    mint_keypair,
                    &Keypair::new(),
                    &Pubkey::new_unique(),
                    i as u64,
                    1,
                    bank.last_blockhash(),
                )
            })
            .collect_vec();
        let txs2 = (0..2 * TARGET_NUM_TRANSACTIONS_PER_BATCH)
            .map(|i| {
                create_and_fund_prioritized_transfer(
                    bank,
                    mint_keypair,
                    &Keypair::new(),
                    &Pubkey::new_unique(),
                    i as u64,
                    2,
                    bank.last_blockhash(),
                )
            })
            .collect_vec();

        banking_packet_sender
            .send(to_banking_packet_batch(&txs1))
            .unwrap();
        banking_packet_sender
            .send(to_banking_packet_batch(&txs2))
            .unwrap();

        // We expect 4 batches to be scheduled
        test_receive_then_schedule(&mut scheduler_controller);
        let consume_works = (0..4)
            .map(|_| consume_work_receivers[0].try_recv().unwrap())
            .collect_vec();

        assert_eq!(
            consume_works.iter().map(|cw| cw.ids.len()).collect_vec(),
            vec![TARGET_NUM_TRANSACTIONS_PER_BATCH; 4]
        );
    }

    #[test]
    fn test_schedule_consume_simple_thread_selection() {
        let (test_frame, mut scheduler_controller) = create_test_frame(2);
        let TestFrame {
            bank,
            mint_keypair,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            ..
        } = &test_frame;

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        // Send 4 transactions w/o conflicts. 2 should be scheduled on each thread
        let txs = (0..4)
            .map(|i| {
                create_and_fund_prioritized_transfer(
                    bank,
                    mint_keypair,
                    &Keypair::new(),
                    &Pubkey::new_unique(),
                    1,
                    i * 10_000,
                    bank.last_blockhash(),
                )
            })
            .collect_vec();
        banking_packet_sender
            .send(to_banking_packet_batch(&txs))
            .unwrap();

        // Priority Expectation:
        // Thread 0: [3, 1]
        // Thread 1: [2, 0]
        let t0_expected = [3, 1]
            .into_iter()
            .map(|i| txs[i].message().hash())
            .collect_vec();
        let t1_expected = [2, 0]
            .into_iter()
            .map(|i| txs[i].message().hash())
            .collect_vec();

        test_receive_then_schedule(&mut scheduler_controller);
        let t0_actual = consume_work_receivers[0]
            .try_recv()
            .unwrap()
            .transactions
            .iter()
            .map(|tx| *tx.message_hash())
            .collect_vec();
        let t1_actual = consume_work_receivers[1]
            .try_recv()
            .unwrap()
            .transactions
            .iter()
            .map(|tx| *tx.message_hash())
            .collect_vec();

        assert_eq!(t0_actual, t0_expected);
        assert_eq!(t1_actual, t1_expected);
    }

    #[test]
    fn test_schedule_consume_retryable() {
        let (test_frame, mut scheduler_controller) = create_test_frame(1);
        let TestFrame {
            bank,
            mint_keypair,
            poh_recorder,
            banking_packet_sender,
            consume_work_receivers,
            finished_consume_work_sender,
            ..
        } = &test_frame;

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());

        // Send packet batch to the scheduler - should do nothing until we become the leader.
        let tx1 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            1000,
            bank.last_blockhash(),
        );
        let tx2 = create_and_fund_prioritized_transfer(
            bank,
            mint_keypair,
            &Keypair::new(),
            &Pubkey::new_unique(),
            1,
            2000,
            bank.last_blockhash(),
        );
        let tx1_hash = tx1.message().hash();
        let tx2_hash = tx2.message().hash();

        let txs = vec![tx1, tx2];
        banking_packet_sender
            .send(to_banking_packet_batch(&txs))
            .unwrap();

        test_receive_then_schedule(&mut scheduler_controller);
        let consume_work = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(consume_work.ids.len(), 2);
        assert_eq!(consume_work.transactions.len(), 2);
        let message_hashes = consume_work
            .transactions
            .iter()
            .map(|tx| tx.message_hash())
            .collect_vec();
        assert_eq!(message_hashes, vec![&tx2_hash, &tx1_hash]);

        // Complete the batch - marking the second transaction as retryable
        finished_consume_work_sender
            .send(FinishedConsumeWork {
                work: consume_work,
                retryable_indexes: vec![1],
            })
            .unwrap();

        // Transaction should be rescheduled
        test_receive_then_schedule(&mut scheduler_controller);
        let consume_work = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(consume_work.ids.len(), 1);
        assert_eq!(consume_work.transactions.len(), 1);
        let message_hashes = consume_work
            .transactions
            .iter()
            .map(|tx| tx.message_hash())
            .collect_vec();
        assert_eq!(message_hashes, vec![&tx1_hash]);
    }

    #[test]
    fn test_calculate_max_age() {
        let current_slot = 100;
        let sanitized_epoch = 10;

        // ALT deactivation slot is delayed
        assert_eq!(
            calculate_max_age(sanitized_epoch, current_slot - 1, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot - 1
                    + solana_sdk::slot_hashes::get_entries() as u64,
            }
        );

        // no deactivation slot
        assert_eq!(
            calculate_max_age(sanitized_epoch, u64::MAX, current_slot),
            MaxAge {
                sanitized_epoch,
                alt_invalidation_slot: current_slot + solana_sdk::slot_hashes::get_entries() as u64,
            }
        );
    }
}
