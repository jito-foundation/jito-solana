use {
    crate::banking_stage::{
        committer::{CommitTransactionDetails, Committer},
        consumer::{
            ExecuteAndCommitTransactionsOutput, ExecutionFlags, LeaderProcessedTransactionCounts,
            ProcessTransactionBatchOutput, RetryableIndex,
        },
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::QosService,
        scheduler_messages::MaxAge,
    },
    itertools::Itertools,
    solana_clock::MAX_PROCESSING_AGE,
    solana_measure::measure_us,
    solana_poh::transaction_recorder::{
        RecordTransactionsSummary, RecordTransactionsTimings, TransactionRecorder,
    },
    solana_runtime::{
        bank::{Bank, LoadAndExecuteTransactionsOutput},
        transaction_batch::TransactionBatch,
    },
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::{
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_processing_result::TransactionProcessingResultExtensions,
        transaction_processor::{ExecutionRecordingConfig, TransactionProcessingConfig},
    },
    solana_transaction::TransactionError,
    std::{
        iter::repeat,
        num::Saturating,
        thread::sleep,
        time::{Duration, Instant},
        vec,
    },
};

pub struct BundleConsumer {
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,
}

impl BundleConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        committer: Committer,
        transaction_recorder: TransactionRecorder,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
    ) -> Self {
        Self {
            committer,
            transaction_recorder,
            qos_service,
            log_messages_bytes_limit,
        }
    }

    // A bundle is a series of transactions to be executed sequentially, atomically, and all-or-nothing.
    // Sequentially:
    //  - Transactions are executed in order
    // Atomically:
    //  - All transactions in a bundle get recoded to PoH and committed to the bank in the same slot. Account locks
    //  for all accounts in all transactions in a bundle are held during the entire execution to remove POH record race conditions
    //  with transactions in BankingStage.
    // All-or-nothing:
    //  - All transactions are committed or none. Modified state for the entire bundle isn't recorded to PoH and committed to the
    //  bank until all transactions in the bundle have executed.
    //
    // Some corner cases to be aware of when working with BundleStage:
    // A bundle is not allowed to call the Tip Payment program in a bundle (or BankingStage).
    // This is to avoid stealing of tips by malicious parties with bundles that crank the tip
    // payment program and set the tip receiver to themself.
    pub fn process_and_record_aged_transactions(
        &mut self,
        bank: &Bank,
        txs: &[impl TransactionWithMeta],
        max_ages: &[MaxAge],
        max_bundle_duration: Duration,
    ) -> ProcessTransactionBatchOutput {
        // Need to filter out transactions since they were sanitized earlier.
        // This means that the transaction may cross and epoch boundary (not allowed),
        //  or account lookup tables may have been closed.
        let pre_results = txs
            .iter()
            .zip(max_ages)
            .map(|(tx, max_age)| {
                // If the transaction was sanitized before this bank's epoch,
                // additional checks are necessary.
                if bank.epoch() != max_age.sanitized_epoch {
                    // Reserved key set may have changed, so we must verify that
                    // no writable keys are reserved.
                    bank.check_reserved_keys(tx)?;
                }

                if bank.slot() > max_age.alt_invalidation_slot {
                    // The address table lookup **may** have expired, but the
                    // expiration is not guaranteed since there may have been
                    // skipped slot.
                    // If the addresses still resolve here, then the transaction is still
                    // valid, and we can continue with processing.
                    // If they do not, then the ATL has expired and the transaction
                    // can be dropped.
                    let (_addresses, _deactivation_slot) =
                        bank.load_addresses_from_ref(tx.message_address_table_lookups())?;
                }

                Ok(())
            })
            .collect_vec();

        let mut error_counters = TransactionErrorMetrics::default();
        let check_results =
            bank.check_transactions(txs, &pre_results, MAX_PROCESSING_AGE, &mut error_counters);
        if let Some(err) = check_results.iter().find(|result| result.is_err()) {
            let err = err.clone().unwrap_err();
            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us: 0,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    execute_and_commit_timings: LeaderExecuteAndCommitTimings::default(),
                    error_counters,
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    retryable_transaction_indexes: vec![], // nothing is retryable
                    commit_transactions_result: Ok(vec![CommitTransactionDetails::NotCommitted(
                        err.clone(),
                    )]),
                },
            };
        }

        self.process_and_record_transactions_with_pre_results(bank, txs, max_bundle_duration)
    }

    fn process_and_record_transactions_with_pre_results(
        &mut self,
        bank: &Bank,
        txs: &[impl TransactionWithMeta],
        max_bundle_duration: Duration,
    ) -> ProcessTransactionBatchOutput {
        // Select and accumulate transaction costs. If any transaction inside the bundle can't fit in the block,
        // undo the cost model reservations and return an error.
        let (
            (transaction_qos_cost_results, cost_model_throttled_transactions_count),
            cost_model_us,
        ) = measure_us!(self.qos_service.select_and_accumulate_transaction_costs(
            bank,
            txs,
            repeat(Ok(())),
            &|_| 0,
        ));
        if let Some(err) = transaction_qos_cost_results.iter().find(|r| r.is_err()) {
            let err = err.as_ref().unwrap_err().clone();

            QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);

            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count,
                cost_model_us,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    // everything is retryable, but not immediately because the QoS isn't reset until the next slot
                    retryable_transaction_indexes: (0..txs.len())
                        .map(|index| RetryableIndex {
                            index,
                            immediately_retryable: false,
                        })
                        .collect(),
                    commit_transactions_result: Ok(vec![
                        CommitTransactionDetails::NotCommitted(err);
                        txs.len()
                    ]),
                    execute_and_commit_timings: LeaderExecuteAndCommitTimings::default(),
                    error_counters: TransactionErrorMetrics::default(),
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                },
            };
        }

        // Try to lock the batch for the maximum amount of time allowed
        // The bundle account locker should handle pre-locking in BankingStage.
        let (batch, lock_us) = measure_us!(Self::try_lock_batch(bank, txs, max_bundle_duration));
        if let Err(err) = batch {
            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    transaction_counts: LeaderProcessedTransactionCounts {
                        attempted_processing_count: 0,
                        processed_count: 0,
                        processed_with_successful_result_count: 0,
                    },
                    retryable_transaction_indexes: if err == TransactionError::AccountInUse {
                        (0..txs.len())
                            .map(|index| RetryableIndex {
                                index,
                                immediately_retryable: true,
                            })
                            .collect()
                    } else {
                        vec![]
                    },
                    commit_transactions_result: Ok(vec![
                        CommitTransactionDetails::NotCommitted(err);
                        txs.len()
                    ]),
                    execute_and_commit_timings: LeaderExecuteAndCommitTimings::default(),
                    error_counters: TransactionErrorMetrics::default(),
                    min_prioritization_fees: 0,
                    max_prioritization_fees: 0,
                },
            };
        }
        let batch = batch.unwrap();

        let execute_and_commit_transactions_output = self.execute_and_commit_transactions_locked(
            bank,
            &batch,
            ExecutionFlags {
                drop_on_failure: true,
                all_or_nothing: true,
            },
        );

        // // Once the accounts are new transactions can enter the pipeline to process them
        let (_, unlock_us) = measure_us!(drop(batch));

        let ExecuteAndCommitTransactionsOutput {
            ref commit_transactions_result,
            ..
        } = execute_and_commit_transactions_output;

        // Costs of all transactions are added to the cost_tracker before processing.
        // To ensure accurate tracking of compute units, transactions that ultimately
        // were not included in the block should have their cost removed, the rest
        // should update with their actually consumed units.
        QosService::remove_or_update_costs(
            transaction_qos_cost_results.iter(),
            commit_transactions_result.as_ref().ok(),
            bank,
        );

        // reports qos service stats for this batch
        self.qos_service.report_metrics(bank.slot());

        debug!(
            "bank: {} lock: {}us unlock: {}us txs_len: {}",
            bank.slot(),
            lock_us,
            unlock_us,
            txs.len(),
        );

        ProcessTransactionBatchOutput {
            cost_model_throttled_transactions_count,
            cost_model_us,
            execute_and_commit_transactions_output,
        }
    }

    fn try_lock_batch<'a, 'b>(
        bank: &'a Bank,
        txs: &'b [impl TransactionWithMeta],
        max_bundle_duration: Duration,
    ) -> Result<TransactionBatch<'a, 'b, impl TransactionWithMeta>, TransactionError> {
        let start = Instant::now();
        while start.elapsed() < max_bundle_duration {
            let batch = bank.prepare_sanitized_batch_relax_intrabatch_account_locks(txs);
            if let Some(err) = batch.lock_results().iter().find(|x| x.is_err()) {
                if err.as_ref().unwrap_err() == &TransactionError::AccountInUse {
                    sleep(Duration::from_millis(1));
                } else {
                    return Err(err.as_ref().unwrap_err().clone());
                }
            } else {
                return Ok(batch);
            }
        }

        Err(TransactionError::AccountInUse)
    }

    fn execute_and_commit_transactions_locked(
        &self,
        bank: &Bank,
        batch: &TransactionBatch<impl TransactionWithMeta>,
        flags: ExecutionFlags,
    ) -> ExecuteAndCommitTransactionsOutput {
        let transaction_status_sender_enabled = self.committer.transaction_status_sender_enabled();
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        let min_max = batch
            .sanitized_transactions()
            .iter()
            .filter_map(|transaction| {
                transaction
                    .compute_budget_instruction_details()
                    .sanitize_and_convert_to_compute_budget_limits(&bank.feature_set)
                    .ok()
                    .map(|limits| limits.compute_unit_price)
            })
            .minmax();
        let (min_prioritization_fees, max_prioritization_fees) =
            min_max.into_option().unwrap_or_default();

        let mut error_counters = TransactionErrorMetrics::default();

        let (load_and_execute_transactions_output, load_execute_us) = measure_us!(bank
            .load_and_execute_transactions(
                batch,
                MAX_PROCESSING_AGE,
                &mut execute_and_commit_timings.execute_timings,
                &mut error_counters,
                TransactionProcessingConfig {
                    account_overrides: None,
                    check_program_modification_slot: bank.check_program_modification_slot(),
                    log_messages_bytes_limit: self.log_messages_bytes_limit,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig::new_single_setting(
                        transaction_status_sender_enabled
                    ),
                    drop_on_failure: flags.drop_on_failure,
                    all_or_nothing: flags.all_or_nothing,
                }
            ));
        execute_and_commit_timings.load_execute_us = load_execute_us;

        let LoadAndExecuteTransactionsOutput {
            processing_results,
            processed_counts,
            balance_collector,
        } = load_and_execute_transactions_output;

        // BundleStage: all transactions must execute successfully to be committed
        if let Some(err) = processing_results.iter().find(|result| result.is_err()) {
            let err = err.clone().unwrap_err();

            return ExecuteAndCommitTransactionsOutput {
                transaction_counts: LeaderProcessedTransactionCounts {
                    attempted_processing_count: batch.sanitized_transactions().len() as u64,
                    processed_count: 0,
                    processed_with_successful_result_count: 0,
                },
                // nothing is retryable because the transactions didn't execute successfully
                retryable_transaction_indexes: vec![],
                commit_transactions_result: Ok(vec![
                    CommitTransactionDetails::NotCommitted(err);
                    batch.sanitized_transactions().len()
                ]),
                execute_and_commit_timings,
                error_counters,
                min_prioritization_fees,
                max_prioritization_fees,
            };
        }

        let actual_execute_time = execute_and_commit_timings
            .execute_timings
            .execute_accessories
            .process_instructions
            .total_us
            .0;
        let actual_executed_cu = processing_results
            .iter()
            .map(|processing_result| {
                processing_result
                    .as_ref()
                    .map_or(0, |pr| pr.executed_units())
            })
            .sum();
        self.qos_service
            .accumulate_actual_execute_cu(actual_executed_cu);
        self.qos_service
            .accumulate_actual_execute_time(actual_execute_time);

        let transaction_counts = LeaderProcessedTransactionCounts {
            processed_count: processed_counts.processed_transactions_count,
            processed_with_successful_result_count: processed_counts
                .processed_with_successful_result_count,
            attempted_processing_count: processing_results.len() as u64,
        };

        let (processed_transactions, processing_results_to_transactions_us) =
            measure_us!(processing_results
                .iter()
                .zip(batch.sanitized_transactions())
                .filter_map(|(processing_result, tx)| {
                    if processing_result.was_processed() {
                        Some(tx.to_versioned_transaction())
                    } else {
                        None
                    }
                })
                .collect_vec());

        let (freeze_lock, freeze_lock_us) = measure_us!(bank.freeze_lock());
        execute_and_commit_timings.freeze_lock_us = freeze_lock_us;

        // BundleStage: executes multiple transactions which may contain overlapping accounts
        // This needs to happen until the relax_intrabatch_account_locks feature is enabled
        let (record_transactions_summary, record_us) = measure_us!(self
            .transaction_recorder
            .record_bundle(bank.bank_id(), processed_transactions));
        execute_and_commit_timings.record_us = record_us;

        let RecordTransactionsSummary {
            result: record_transactions_result,
            record_transactions_timings,
            starting_transaction_index,
        } = record_transactions_summary;
        execute_and_commit_timings.record_transactions_timings = RecordTransactionsTimings {
            processing_results_to_transactions_us: Saturating(
                processing_results_to_transactions_us,
            ),
            ..record_transactions_timings
        };

        // If recording error, all transactions are retryable
        // Any transaction failures trigger a bailout of the entire bundle above
        if let Err(recorder_err) = record_transactions_result {
            return ExecuteAndCommitTransactionsOutput {
                transaction_counts,
                retryable_transaction_indexes: (0..batch.sanitized_transactions().len())
                    .map(|index| RetryableIndex {
                        index,
                        immediately_retryable: true,
                    })
                    .collect(),
                commit_transactions_result: Err(recorder_err),
                execute_and_commit_timings,
                error_counters,
                min_prioritization_fees,
                max_prioritization_fees,
            };
        }

        let (commit_time_us, commit_transaction_statuses) =
            if processed_counts.processed_transactions_count != 0 {
                self.committer.commit_transactions(
                    batch,
                    processing_results,
                    starting_transaction_index,
                    bank,
                    balance_collector,
                    &mut execute_and_commit_timings,
                    &processed_counts,
                )
            } else {
                (
                    0,
                    processing_results
                        .into_iter()
                        .map(|processing_result| match processing_result {
                            Ok(_) => unreachable!("processed transaction count is 0"),
                            Err(err) => CommitTransactionDetails::NotCommitted(err),
                        })
                        .collect(),
                )
            };

        drop(freeze_lock);

        debug!(
            "bank: {} process_and_record_locked: {}us record: {}us commit: {}us txs_len: {}",
            bank.slot(),
            load_execute_us,
            record_us,
            commit_time_us,
            batch.sanitized_transactions().len(),
        );

        debug!(
            "execute_and_commit_transactions_locked: {:?}",
            execute_and_commit_timings.execute_timings,
        );

        debug_assert_eq!(
            transaction_counts.attempted_processing_count,
            commit_transaction_statuses.len() as u64,
        );

        ExecuteAndCommitTransactionsOutput {
            transaction_counts,
            retryable_transaction_indexes: vec![], // nothing is retryable because the entire bundle was committed successfully
            commit_transactions_result: Ok(commit_transaction_statuses),
            execute_and_commit_timings,
            error_counters,
            min_prioritization_fees,
            max_prioritization_fees,
        }
    }
}

#[cfg(test)]
mod tests {

    // #[test]
    // fn test_single_tx_ok_bundle_committed() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_single_tx_bad_not_committed() {
    //     // ensure QoS is rolled back
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_multi_bundle_seed_fee_payer_ok() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_last_tx_bad_not_committed() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_tx_compute_reservation_exceeds_drops_bundle() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_transaction_already_processed_fails() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_bundle_fails_qos_rolls_back_qos() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_bundle_fails_commit_rolls_back_qos() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_bundle_fails_recorder_rolls_back_qos() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_bundle_account_in_use_rolls_back_qos() {
    //     panic!("Not implemented");
    // }

    // #[test]
    // fn test_overlapping_bundle_accounts_in_different_entries() {
    //     panic!("Not implemented");
    // }
}
