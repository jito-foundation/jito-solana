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

        return Err(TransactionError::AccountInUse);
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

    // #[allow(clippy::too_many_arguments)]
    // fn process_bundle(
    //     bundle_account_locker: &BundleAccountLocker,
    //     tip_manager: &TipManager,
    //     last_tip_updated_slot: &mut Slot,
    //     cluster_info: &Arc<ClusterInfo>,
    //     block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    //     committer: &Committer,
    //     recorder: &TransactionRecorder,
    //     qos_service: &QosService,
    //     log_messages_bytes_limit: &Option<usize>,
    //     max_bundle_retry_duration: Duration,
    //     locked_bundle: &LockedBundle,
    //     bank: &Arc<Bank>,
    //     bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    // ) -> Result<(), BundleExecutionError> {
    //     if bank.is_complete() {
    //         return Err(BundleExecutionError::BankProcessingTimeLimitReached);
    //     }

    //     if bank.slot() != *last_tip_updated_slot
    //         && Self::bundle_touches_tip_pdas(
    //             locked_bundle.sanitized_bundle(),
    //             &tip_manager.get_tip_accounts(),
    //         )
    //     {
    //         let start = Instant::now();
    //         let result = Self::handle_tip_programs(
    //             bundle_account_locker,
    //             tip_manager,
    //             cluster_info,
    //             block_builder_fee_info,
    //             committer,
    //             recorder,
    //             qos_service,
    //             log_messages_bytes_limit,
    //             max_bundle_retry_duration,
    //             bank,
    //             bundle_stage_leader_metrics,
    //         );

    //         bundle_stage_leader_metrics
    //             .bundle_stage_metrics_tracker()
    //             .increment_change_tip_receiver_elapsed_us(start.elapsed().as_micros() as u64);

    //         result?;

    //         *last_tip_updated_slot = bank.slot();
    //     }

    //     Self::update_qos_and_execute_record_commit_bundle(
    //         committer,
    //         recorder,
    //         qos_service,
    //         log_messages_bytes_limit,
    //         max_bundle_retry_duration,
    //         locked_bundle.sanitized_bundle(),
    //         bank,
    //         bundle_stage_leader_metrics,
    //     )?;

    //     Ok(())
    // }

    // /// The validator needs to manage state on two programs related to tips
    // #[allow(clippy::too_many_arguments)]
    // fn handle_tip_programs(
    //     bundle_account_locker: &BundleAccountLocker,
    //     tip_manager: &TipManager,
    //     cluster_info: &Arc<ClusterInfo>,
    //     block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    //     committer: &Committer,
    //     recorder: &TransactionRecorder,
    //     qos_service: &QosService,
    //     log_messages_bytes_limit: &Option<usize>,
    //     max_bundle_retry_duration: Duration,
    //     bank: &Arc<Bank>,
    //     bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    // ) -> Result<(), BundleExecutionError> {
    //     debug!("handle_tip_programs");

    //     // This will setup the tip payment and tip distribution program if they haven't been
    //     // initialized yet, which is typically helpful for local validators. On mainnet and testnet,
    //     // this code should never run.
    //     let keypair = cluster_info.keypair().clone();
    //     let initialize_tip_programs_bundle =
    //         tip_manager.get_initialize_tip_programs_bundle(bank, &keypair);
    //     if let Some(bundle) = initialize_tip_programs_bundle {
    //         debug!(
    //             "initializing tip programs with {} transactions, bundle id: {}",
    //             bundle.transactions.len(),
    //             bundle.bundle_id
    //         );

    //         let locked_init_tip_programs_bundle = bundle_account_locker
    //             .prepare_locked_bundle(&bundle, bank)
    //             .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

    //         Self::update_qos_and_execute_record_commit_bundle(
    //             committer,
    //             recorder,
    //             qos_service,
    //             log_messages_bytes_limit,
    //             max_bundle_retry_duration,
    //             locked_init_tip_programs_bundle.sanitized_bundle(),
    //             bank,
    //             bundle_stage_leader_metrics,
    //         )
    //         .map_err(|e| {
    //             bundle_stage_leader_metrics
    //                 .bundle_stage_metrics_tracker()
    //                 .increment_num_init_tip_account_errors(1);
    //             error!(
    //                 "bundle: {} error initializing tip programs: {:?}",
    //                 locked_init_tip_programs_bundle.sanitized_bundle().bundle_id,
    //                 e
    //             );
    //             BundleExecutionError::TipError(TipError::InitializeProgramsError)
    //         })?;

    //         bundle_stage_leader_metrics
    //             .bundle_stage_metrics_tracker()
    //             .increment_num_init_tip_account_ok(1);
    //     }

    //     // There are two frequently run internal cranks inside the jito-solana validator that have to do with managing MEV tips.
    //     // One is initialize the TipDistributionAccount, which is a validator's "tip piggy bank" for an epoch
    //     // The other is ensuring the tip_receiver is configured correctly to ensure tips are routed to the correct
    //     // address. The validator must drain the tip accounts to the previous tip receiver before setting the tip receiver to
    //     // themselves.

    //     let kp = cluster_info.keypair().clone();
    //     let tip_crank_bundle = tip_manager.get_tip_programs_crank_bundle(
    //         bank,
    //         &kp,
    //         &block_builder_fee_info.lock().unwrap(),
    //     )?;
    //     debug!("tip_crank_bundle is_some: {}", tip_crank_bundle.is_some());

    //     if let Some(bundle) = tip_crank_bundle {
    //         info!(
    //             "bundle id: {} cranking tip programs with {} transactions",
    //             bundle.bundle_id,
    //             bundle.transactions.len()
    //         );

    //         let locked_tip_crank_bundle = bundle_account_locker
    //             .prepare_locked_bundle(&bundle, bank)
    //             .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

    //         Self::update_qos_and_execute_record_commit_bundle(
    //             committer,
    //             recorder,
    //             qos_service,
    //             log_messages_bytes_limit,
    //             max_bundle_retry_duration,
    //             locked_tip_crank_bundle.sanitized_bundle(),
    //             bank,
    //             bundle_stage_leader_metrics,
    //         )
    //         .map_err(|e| {
    //             bundle_stage_leader_metrics
    //                 .bundle_stage_metrics_tracker()
    //                 .increment_num_change_tip_receiver_errors(1);
    //             error!(
    //                 "bundle: {} error cranking tip programs: {:?}",
    //                 locked_tip_crank_bundle.sanitized_bundle().bundle_id,
    //                 e
    //             );
    //             BundleExecutionError::TipError(TipError::CrankTipError)
    //         })?;

    //         bundle_stage_leader_metrics
    //             .bundle_stage_metrics_tracker()
    //             .increment_num_change_tip_receiver_ok(1);
    //     }

    //     Ok(())
    // }

    // /// Reserves space for the entire bundle up-front to ensure the entire bundle can execute.
    // /// Rolls back the reserved space if there's not enough blockspace for all transactions in the bundle.
    // fn reserve_bundle_blockspace<'a>(
    //     qos_service: &QosService,
    //     sanitized_bundle: &'a SanitizedBundle,
    //     bank: &Arc<Bank>,
    // ) -> ReserveBundleBlockspaceResult<'a> {
    //     let (transaction_qos_cost_results, cost_model_throttled_transactions_count) = qos_service
    //         .select_and_accumulate_transaction_costs(
    //             bank,
    //             &sanitized_bundle.transactions,
    //             std::iter::repeat(Ok(())),
    //             // bundle stage does not respect the cost model reservation
    //             &|_| 0,
    //         );

    //     // rollback all transaction costs if it can't fit and
    //     if transaction_qos_cost_results.iter().any(|c| c.is_err()) {
    //         QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
    //         return Err(BundleExecutionError::ExceedsCostModel);
    //     }

    //     Ok((
    //         transaction_qos_cost_results,
    //         cost_model_throttled_transactions_count,
    //     ))
    // }

    // fn update_qos_and_execute_record_commit_bundle(
    //     committer: &Committer,
    //     recorder: &TransactionRecorder,
    //     qos_service: &QosService,
    //     log_messages_bytes_limit: &Option<usize>,
    //     max_bundle_retry_duration: Duration,
    //     sanitized_bundle: &SanitizedBundle,
    //     bank: &Arc<Bank>,
    //     bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    // ) -> BundleExecutionResult<()> {
    //     debug!(
    //         "bundle: {} reserving blockspace for {} transactions",
    //         sanitized_bundle.bundle_id,
    //         sanitized_bundle.transactions.len()
    //     );

    //     let (
    //         (transaction_qos_cost_results, _cost_model_throttled_transactions_count),
    //         cost_model_elapsed_us,
    //     ) = measure_us!(Self::reserve_bundle_blockspace(
    //         qos_service,
    //         sanitized_bundle,
    //         bank
    //     )?);

    //     debug!(
    //         "bundle: {} executing, recording, and committing",
    //         sanitized_bundle.bundle_id
    //     );

    //     let (result, process_transactions_us) = measure_us!(Self::execute_record_commit_bundle(
    //         committer,
    //         recorder,
    //         log_messages_bytes_limit,
    //         max_bundle_retry_duration,
    //         sanitized_bundle,
    //         bank,
    //     ));

    //     bundle_stage_leader_metrics
    //         .bundle_stage_metrics_tracker()
    //         .increment_num_execution_retries(result.execution_metrics.num_retries);
    //     bundle_stage_leader_metrics
    //         .leader_slot_metrics_tracker()
    //         .accumulate_transaction_errors(&result.transaction_error_counter);
    //     bundle_stage_leader_metrics
    //         .leader_slot_metrics_tracker()
    //         .increment_process_transactions_us(process_transactions_us);

    //     let (cu, us) = result
    //         .execute_and_commit_timings
    //         .execute_timings
    //         .accumulate_execute_units_and_time();
    //     qos_service.accumulate_actual_execute_cu(cu);
    //     qos_service.accumulate_actual_execute_time(us);

    //     let num_committed = result
    //         .commit_transaction_details
    //         .iter()
    //         .filter(|c| matches!(c, CommitTransactionDetails::Committed { .. }))
    //         .count();
    //     bundle_stage_leader_metrics
    //         .leader_slot_metrics_tracker()
    //         .accumulate_process_transactions_summary(&ProcessTransactionsSummary {
    //             reached_max_poh_height: matches!(
    //                 result.result,
    //                 Err(BundleExecutionError::BankProcessingTimeLimitReached)
    //                     | Err(BundleExecutionError::PohRecordError(_))
    //             ),
    //             transaction_counts: CommittedTransactionsCounts {
    //                 attempted_processing_count: Saturating(
    //                     sanitized_bundle.transactions.len() as u64
    //                 ),
    //                 committed_transactions_count: Saturating(num_committed as u64),
    //                 // NOTE: this assumes that bundles are committed all-or-nothing
    //                 committed_transactions_with_successful_result_count: Saturating(
    //                     num_committed as u64,
    //                 ),
    //                 processed_but_failed_commit: Saturating(0),
    //             },
    //             retryable_transaction_indexes: vec![],
    //             cost_model_throttled_transactions_count: 0,
    //             cost_model_us: cost_model_elapsed_us,
    //             execute_and_commit_timings: result.execute_and_commit_timings,
    //             error_counters: result.transaction_error_counter,
    //         });

    //     match result.result {
    //         Ok(_) => {
    //             QosService::remove_or_update_costs(
    //                 transaction_qos_cost_results.iter(),
    //                 Some(&result.commit_transaction_details),
    //                 bank,
    //             );

    //             qos_service.report_metrics(bank.slot());
    //             Ok(())
    //         }
    //         Err(e) => {
    //             // on bundle failure, none of the transactions are committed, so need to revert
    //             // all compute reserved
    //             QosService::remove_or_update_costs(transaction_qos_cost_results.iter(), None, bank);
    //             qos_service.report_metrics(bank.slot());

    //             Err(e)
    //         }
    //     }
    // }

    // fn execute_record_commit_bundle(
    //     committer: &Committer,
    //     recorder: &TransactionRecorder,
    //     log_messages_bytes_limit: &Option<usize>,
    //     max_bundle_retry_duration: Duration,
    //     sanitized_bundle: &SanitizedBundle,
    //     bank: &Arc<Bank>,
    // ) -> ExecuteRecordCommitResult {
    //     let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();

    //     let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

    //     debug!("bundle: {} executing", sanitized_bundle.bundle_id);
    //     let default_accounts = vec![None; sanitized_bundle.transactions.len()];
    //     let bundle_execution_results = load_and_execute_bundle(
    //         bank,
    //         sanitized_bundle,
    //         MAX_PROCESSING_AGE,
    //         &max_bundle_retry_duration,
    //         transaction_status_sender_enabled,
    //         log_messages_bytes_limit,
    //         false,
    //         None,
    //         &default_accounts,
    //         &default_accounts,
    //     );

    //     let execution_metrics = bundle_execution_results.metrics.clone();

    //     execute_and_commit_timings.load_execute_us = execution_metrics.load_execute_us.0;
    //     execute_and_commit_timings
    //         .execute_timings
    //         .accumulate(&execution_metrics.execute_timings);
    //     let transaction_error_counter = execution_metrics.errors.clone();

    //     debug!(
    //         "bundle: {} executed, is_ok: {}",
    //         sanitized_bundle.bundle_id,
    //         bundle_execution_results.result.is_ok()
    //     );

    //     // don't commit bundle if failure executing any part of the bundle
    //     if let Err(e) = bundle_execution_results.result {
    //         return ExecuteRecordCommitResult {
    //             commit_transaction_details: vec![],
    //             result: Err(e.clone().into()),
    //             execution_metrics,
    //             execute_and_commit_timings,
    //             transaction_error_counter,
    //         };
    //     }

    //     let (executed_batches, execution_results_to_transactions_us) =
    //         measure_us!(bundle_execution_results
    //             .bundle_transaction_results
    //             .iter()
    //             .map(|br| br.executed_versioned_transactions())
    //             .collect::<Vec<Vec<VersionedTransaction>>>());

    //     debug!(
    //         "bundle: {} recording {} batches of {:?} transactions",
    //         sanitized_bundle.bundle_id,
    //         executed_batches.len(),
    //         executed_batches
    //             .iter()
    //             .map(|b| b.len())
    //             .collect::<Vec<usize>>()
    //     );

    //     let (freeze_lock, freeze_lock_us) = measure_us!(bank.freeze_lock());
    //     execute_and_commit_timings.freeze_lock_us = freeze_lock_us;

    //     let (hashes, hash_us) = measure_us!(executed_batches
    //         .iter()
    //         .map(|txs| hash_transactions(txs))
    //         .collect());
    //     let (starting_index, poh_record_us) =
    //         measure_us!(recorder.record(bank.slot(), hashes, executed_batches));

    //     let RecordTransactionsSummary {
    //         record_transactions_timings,
    //         result: record_transactions_result,
    //         starting_transaction_index,
    //     } = match starting_index {
    //         Ok(starting_transaction_index) => RecordTransactionsSummary {
    //             record_transactions_timings: RecordTransactionsTimings {
    //                 processing_results_to_transactions_us: Saturating(0), // TODO (LB)
    //                 hash_us: Saturating(hash_us),
    //                 poh_record_us: Saturating(poh_record_us),
    //             },
    //             result: Ok(()),
    //             starting_transaction_index,
    //         },
    //         Err(e) => RecordTransactionsSummary {
    //             record_transactions_timings: RecordTransactionsTimings {
    //                 processing_results_to_transactions_us: Saturating(0), // TODO (LB)
    //                 hash_us: Saturating(hash_us),
    //                 poh_record_us: Saturating(poh_record_us),
    //             },
    //             result: Err(e),
    //             starting_transaction_index: None,
    //         },
    //     };

    //     execute_and_commit_timings.record_us = record_transactions_timings.poh_record_us.0;
    //     execute_and_commit_timings.record_transactions_timings = record_transactions_timings;
    //     execute_and_commit_timings
    //         .record_transactions_timings
    //         .processing_results_to_transactions_us =
    //         Saturating(execution_results_to_transactions_us);

    //     debug!(
    //         "bundle: {} record result: {}",
    //         sanitized_bundle.bundle_id,
    //         record_transactions_result.is_ok()
    //     );

    //     // don't commit bundle if failed to record
    //     if let Err(e) = record_transactions_result {
    //         return ExecuteRecordCommitResult {
    //             commit_transaction_details: vec![],
    //             result: Err(e.into()),
    //             execution_metrics,
    //             execute_and_commit_timings,
    //             transaction_error_counter,
    //         };
    //     }

    //     // note: execute_and_commit_timings.commit_us handled inside this function
    //     let (commit_us, commit_bundle_details) = committer.commit_bundle(
    //         bundle_execution_results,
    //         starting_transaction_index,
    //         bank,
    //         &mut execute_and_commit_timings,
    //     );
    //     execute_and_commit_timings.commit_us = commit_us;

    //     drop(freeze_lock);

    //     // commit_bundle_details contains transactions that were and were not committed
    //     // given the current implementation only executes, records, and commits bundles
    //     // where all transactions executed, we can filter out the non-committed
    //     // TODO (LB): does this make more sense in commit_bundle for future when failing bundles are accepted?
    //     let commit_transaction_details = commit_bundle_details
    //         .commit_transaction_details
    //         .into_iter()
    //         .flat_map(|commit_details| {
    //             commit_details
    //                 .into_iter()
    //                 .filter(|d| matches!(d, CommitTransactionDetails::Committed { .. }))
    //         })
    //         .collect();
    //     debug!(
    //         "bundle: {} commit details: {:?}",
    //         sanitized_bundle.bundle_id, commit_transaction_details
    //     );

    //     ExecuteRecordCommitResult {
    //         commit_transaction_details,
    //         result: Ok(()),
    //         execution_metrics,
    //         execute_and_commit_timings,
    //         transaction_error_counter,
    //     }
    // }

    // /// Returns true if any of the transactions in a bundle mention one of the tip PDAs
    // fn bundle_touches_tip_pdas(bundle: &SanitizedBundle, tip_pdas: &HashSet<Pubkey>) -> bool {
    //     bundle.transactions.iter().any(|tx| {
    //         tx.message()
    //             .account_keys()
    //             .iter()
    //             .any(|a| tip_pdas.contains(a))
    //     })
    // }
}

// #[cfg(test)]
// mod tests {
//     use {
//         crate::{
//             bundle_stage::{
//                 bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
//                 bundle_packet_deserializer::BundlePacketDeserializer,
//                 bundle_stage_leader_metrics::BundleStageLeaderMetrics, committer::Committer,
//                 BundleStorage, QosService,
//             },
//             packet_bundle::PacketBundle,
//             proxy::block_engine_stage::BlockBuilderFeeInfo,
//             tip_manager::{
//                 derive_tip_distribution_account_address, TipDistributionAccountConfig, TipManager,
//                 TipManagerConfig,
//             },
//         },
//         crossbeam_channel::{unbounded, Receiver},
//         rand::{thread_rng, RngCore},
//         solana_bundle::SanitizedBundle,
//         solana_bundle_sdk::derive_bundle_id,
//         solana_clock::MAX_PROCESSING_AGE,
//         solana_cluster_type::ClusterType,
//         solana_cost_model::cost_model::CostModel,
//         solana_fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
//         solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
//         solana_hash::Hash,
//         solana_keypair::Keypair,
//         solana_ledger::{
//             blockstore::Blockstore, genesis_utils::create_genesis_config,
//             get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache,
//         },
//         solana_native_token::LAMPORTS_PER_SOL,
//         solana_perf::packet::{BytesPacket, PacketBatch},
//         solana_poh::{
//             poh_recorder::{PohRecorder, Record, WorkingBankEntry},
//             poh_service::PohService,
//             transaction_recorder::TransactionRecorder,
//         },
//         solana_poh_config::PohConfig,
//         solana_program_binaries::spl_programs,
//         solana_pubkey::Pubkey,
//         solana_rent::Rent,
//         solana_runtime::{
//             bank::Bank,
//             bank_forks::BankForks,
//             genesis_utils::{create_genesis_config_with_leader_ex, GenesisConfigInfo},
//             installed_scheduler_pool::BankWithScheduler,
//             prioritization_fee_cache::PrioritizationFeeCache,
//         },
//         solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
//         solana_signer::Signer,
//         solana_streamer::socket::SocketAddrSpace,
//         solana_svm::{
//             account_loader::TransactionCheckResult,
//             transaction_error_metrics::TransactionErrorMetrics,
//         },
//         solana_system_transaction::transfer,
//         solana_transaction::versioned::VersionedTransaction,
//         solana_transaction_error::TransactionError,
//         solana_vote_interface::state::VoteStateV3,
//         std::{
//             collections::HashSet,
//             str::FromStr,
//             sync::{
//                 atomic::{AtomicBool, Ordering},
//                 Arc, Mutex, RwLock,
//             },
//             thread::{Builder, JoinHandle},
//             time::Duration,
//         },
//     };

//     struct TestFixture {
//         genesis_config_info: GenesisConfigInfo,
//         leader_keypair: Keypair,
//         bank: Arc<Bank>,
//         exit: Arc<AtomicBool>,
//         poh_recorder: Arc<RwLock<PohRecorder>>,
//         transaction_recorder: TransactionRecorder,
//         poh_simulator: JoinHandle<()>,
//         entry_receiver: Receiver<WorkingBankEntry>,
//         bank_forks: Arc<RwLock<BankForks>>,
//     }

//     pub fn simulate_poh(
//         record_receiver: Receiver<Record>,
//         poh_recorder: &Arc<RwLock<PohRecorder>>,
//     ) -> JoinHandle<()> {
//         let poh_recorder = poh_recorder.clone();
//         let is_exited = poh_recorder.read().unwrap().is_exited.clone();
//         let tick_producer = Builder::new()
//             .name("solana-simulate_poh".to_string())
//             .spawn(move || loop {
//                 PohService::read_record_receiver_and_process(
//                     &poh_recorder,
//                     &record_receiver,
//                     Duration::from_millis(10),
//                 );
//                 if is_exited.load(Ordering::Relaxed) {
//                     break;
//                 }
//             });
//         tick_producer.unwrap()
//     }

//     struct TestRecorder {
//         exit: Arc<AtomicBool>,
//         poh_recorder: Arc<RwLock<PohRecorder>>,
//         transaction_recorder: TransactionRecorder,
//         poh_simulator: JoinHandle<()>,
//         entry_receiver: Receiver<WorkingBankEntry>,
//     }

//     fn create_test_recorder(
//         bank: &Arc<Bank>,
//         blockstore: Arc<Blockstore>,
//         poh_config: Option<PohConfig>,
//         leader_schedule_cache: Option<Arc<LeaderScheduleCache>>,
//     ) -> TestRecorder {
//         let leader_schedule_cache = match leader_schedule_cache {
//             Some(provided_cache) => provided_cache,
//             None => Arc::new(LeaderScheduleCache::new_from_bank(bank)),
//         };
//         let exit = Arc::new(AtomicBool::new(false));
//         let poh_config = poh_config.unwrap_or_default();

//         let (mut poh_recorder, entry_receiver) = PohRecorder::new(
//             bank.tick_height(),
//             bank.last_blockhash(),
//             bank.clone(),
//             Some((4, 4)),
//             bank.ticks_per_slot(),
//             blockstore,
//             &leader_schedule_cache,
//             &poh_config,
//             exit.clone(),
//         );
//         poh_recorder.set_bank(BankWithScheduler::new_without_scheduler(bank.clone()));

//         let (record_sender, record_receiver) = unbounded();
//         let transaction_recorder = TransactionRecorder::new(record_sender, exit.clone());

//         let poh_recorder = Arc::new(RwLock::new(poh_recorder));
//         let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

//         TestRecorder {
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//         }
//     }

//     fn create_test_fixture(mint_sol: u64) -> TestFixture {
//         let mint_keypair = Keypair::new();
//         let leader_keypair = Keypair::new();
//         let voting_keypair = Keypair::new();

//         let rent = Rent::default();

//         let mut genesis_config = create_genesis_config_with_leader_ex(
//             mint_sol * LAMPORTS_PER_SOL,
//             &mint_keypair.pubkey(),
//             &leader_keypair.pubkey(),
//             &voting_keypair.pubkey(),
//             &Pubkey::new_unique(),
//             rent.minimum_balance(VoteStateV3::size_of()) + (LAMPORTS_PER_SOL * 1_000_000),
//             LAMPORTS_PER_SOL * 1_000_000,
//             FeeRateGovernor {
//                 // Initialize with a non-zero fee
//                 lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
//                 ..FeeRateGovernor::default()
//             },
//             rent.clone(), // most tests don't expect rent
//             ClusterType::Development,
//             spl_programs(&rent),
//         );
//         genesis_config.ticks_per_slot *= 8;

//         // workaround for https://github.com/solana-labs/solana/issues/30085
//         // the test can deploy and use spl_programs in the genensis slot without waiting for the next one
//         let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

//         let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));

//         let ledger_path = get_tmp_ledger_path_auto_delete!();
//         let blockstore = Arc::new(
//             Blockstore::open(ledger_path.path())
//                 .expect("Expected to be able to open database ledger"),
//         );

//         let TestRecorder {
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//         } = create_test_recorder(&bank, blockstore, Some(PohConfig::default()), None);

//         let validator_pubkey = voting_keypair.pubkey();
//         TestFixture {
//             genesis_config_info: GenesisConfigInfo {
//                 genesis_config,
//                 mint_keypair,
//                 voting_keypair,
//                 validator_pubkey,
//             },
//             leader_keypair,
//             bank,
//             bank_forks,
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//         }
//     }

//     fn make_random_overlapping_bundles(
//         mint_keypair: &Keypair,
//         num_bundles: usize,
//         num_packets_per_bundle: usize,
//         hash: Hash,
//         max_transfer_amount: u64,
//     ) -> Vec<PacketBundle> {
//         let mut rng = thread_rng();

//         (0..num_bundles)
//             .map(|_| {
//                 let transfers: Vec<_> = (0..num_packets_per_bundle)
//                     .map(|_| {
//                         VersionedTransaction::from(transfer(
//                             mint_keypair,
//                             &mint_keypair.pubkey(),
//                             rng.next_u64() % max_transfer_amount,
//                             hash,
//                         ))
//                     })
//                     .collect();
//                 let bundle_id = derive_bundle_id(&transfers).unwrap();

//                 PacketBundle {
//                     batch: PacketBatch::from(
//                         transfers
//                             .iter()
//                             .map(|tx| BytesPacket::from_data(None, tx).unwrap())
//                             .collect::<Vec<_>>(),
//                     ),
//                     bundle_id,
//                 }
//             })
//             .collect::<Vec<_>>()
//     }

//     fn get_tip_manager(vote_account: &Pubkey) -> TipManager {
//         TipManager::new(TipManagerConfig {
//             tip_payment_program_id: Pubkey::from_str("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
//                 .unwrap(),
//             tip_distribution_program_id: Pubkey::from_str(
//                 "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
//             )
//             .unwrap(),
//             tip_distribution_account_config: TipDistributionAccountConfig {
//                 merkle_root_upload_authority: Pubkey::new_unique(),
//                 vote_account: *vote_account,
//                 commission_bps: 10,
//             },
//         })
//     }

//     /// Happy-path bundle execution w/ no tip management
//     #[test]
//     fn test_bundle_no_tip_success() {
//         solana_logger::setup();
//         let TestFixture {
//             genesis_config_info,
//             leader_keypair,
//             bank,
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//             bank_forks: _bank_forks,
//         } = create_test_fixture(1_000_000);

//         let status = poh_recorder
//             .read()
//             .unwrap()
//             .reached_leader_slot(&leader_keypair.pubkey());
//         info!("status: {:?}", status);

//         let (replay_vote_sender, _replay_vote_receiver) = unbounded();
//         let committer = Committer::new(
//             None,
//             replay_vote_sender,
//             Arc::new(PrioritizationFeeCache::new(0u64)),
//         );

//         let block_builder_pubkey = Pubkey::new_unique();
//         let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
//         let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
//             block_builder: block_builder_pubkey,
//             block_builder_commission: 10,
//         }));

//         let cluster_info = Arc::new(ClusterInfo::new(
//             ContactInfo::new(leader_keypair.pubkey(), 0, 0),
//             Arc::new(leader_keypair),
//             SocketAddrSpace::new(true),
//         ));

//         let mut consumer = BundleConsumer::new(
//             committer,
//             transaction_recorder,
//             QosService::new(1),
//             None,
//             tip_manager,
//             BundleAccountLocker::default(),
//             block_builder_info,
//             Duration::from_secs(10),
//             cluster_info,
//         );

//         let working_bank = poh_recorder.read().unwrap().bank().unwrap();

//         let mut bundle_storage = BundleStorage::default();
//         let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);

//         let mut packet_bundles = make_random_overlapping_bundles(
//             &genesis_config_info.mint_keypair,
//             1,
//             3,
//             genesis_config_info.genesis_config.hash(),
//             10_000,
//         );
//         let deserialized_bundle =
//             BundlePacketDeserializer::deserialize_bundle(packet_bundles.get_mut(0).unwrap(), None)
//                 .unwrap();
//         let mut error_metrics = TransactionErrorMetrics::default();
//         let sanitized_bundle = deserialized_bundle
//             .build_sanitized_bundle(&working_bank, &HashSet::default(), &mut error_metrics)
//             .unwrap();

//         let summary = bundle_storage.insert_unprocessed_bundles(vec![deserialized_bundle]);
//         assert_eq!(
//             summary.num_packets_inserted,
//             sanitized_bundle.transactions.len()
//         );
//         assert_eq!(summary.num_bundles_dropped, 0);
//         assert_eq!(summary.num_bundles_inserted, 1);

//         consumer.consume_buffered_bundles(
//             &working_bank,
//             &mut bundle_storage,
//             &mut bundle_stage_leader_metrics,
//         );

//         let mut transactions = Vec::new();
//         while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
//             assert_eq!(bank.slot(), wbe_bank.slot());
//             transactions.extend(entry.transactions);
//             if transactions.len() == sanitized_bundle.transactions.len() {
//                 break;
//             }
//         }

//         let bundle_versioned_transactions: Vec<_> = sanitized_bundle
//             .transactions
//             .iter()
//             .map(|tx| tx.to_versioned_transaction())
//             .collect();
//         assert_eq!(transactions, bundle_versioned_transactions);

//         let check_results = bank.check_transactions(
//             &sanitized_bundle.transactions,
//             &vec![Ok(()); sanitized_bundle.transactions.len()],
//             MAX_PROCESSING_AGE,
//             &mut error_metrics,
//         );

//         let expected_result: Vec<TransactionCheckResult> =
//             vec![Err(TransactionError::AlreadyProcessed); sanitized_bundle.transactions.len()];

//         assert_eq!(check_results, expected_result);

//         poh_recorder
//             .write()
//             .unwrap()
//             .is_exited
//             .store(true, Ordering::Relaxed);
//         exit.store(true, Ordering::Relaxed);
//         poh_simulator.join().unwrap();
//     }

//     /// Happy-path bundle execution to ensure tip management works.
//     /// Tip management involves cranking setup bundles before executing the test bundle
//     #[test]
//     fn test_bundle_tip_program_setup_success() {
//         solana_logger::setup();
//         let TestFixture {
//             genesis_config_info,
//             leader_keypair,
//             bank,
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//             bank_forks: _bank_forks,
//         } = create_test_fixture(1_000_000);

//         let (replay_vote_sender, _replay_vote_receiver) = unbounded();
//         let committer = Committer::new(
//             None,
//             replay_vote_sender,
//             Arc::new(PrioritizationFeeCache::new(0u64)),
//         );

//         let block_builder_pubkey = Pubkey::new_unique();
//         let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
//         let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
//             block_builder: block_builder_pubkey,
//             block_builder_commission: 10,
//         }));

//         let cluster_info = Arc::new(ClusterInfo::new(
//             ContactInfo::new(leader_keypair.pubkey(), 0, 0),
//             Arc::new(leader_keypair),
//             SocketAddrSpace::new(true),
//         ));

//         let mut consumer = BundleConsumer::new(
//             committer,
//             transaction_recorder,
//             QosService::new(1),
//             None,
//             tip_manager.clone(),
//             BundleAccountLocker::default(),
//             block_builder_info,
//             Duration::from_secs(10),
//             cluster_info.clone(),
//         );

//         let working_bank = poh_recorder.read().unwrap().bank().unwrap();

//         let mut bundle_storage = BundleStorage::default();
//         let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
//         // MAIN LOGIC

//         // a bundle that tips the tip program
//         let tip_accounts = tip_manager.get_tip_accounts();
//         let tip_account = tip_accounts.iter().collect::<Vec<_>>()[0];
//         let mut packet_bundle = PacketBundle {
//             batch: PacketBatch::from(vec![BytesPacket::from_data(
//                 None,
//                 transfer(
//                     &genesis_config_info.mint_keypair,
//                     tip_account,
//                     1,
//                     genesis_config_info.genesis_config.hash(),
//                 ),
//             )
//             .unwrap()]),
//             bundle_id: "test_transfer".to_string(),
//         };

//         let deserialized_bundle =
//             BundlePacketDeserializer::deserialize_bundle(&mut packet_bundle, None).unwrap();
//         let mut error_metrics = TransactionErrorMetrics::default();
//         let sanitized_bundle = deserialized_bundle
//             .build_sanitized_bundle(&working_bank, &HashSet::default(), &mut error_metrics)
//             .unwrap();

//         let summary = bundle_storage.insert_unprocessed_bundles(vec![deserialized_bundle]);
//         assert_eq!(summary.num_bundles_inserted, 1);
//         assert_eq!(summary.num_packets_inserted, 1);
//         assert_eq!(summary.num_bundles_dropped, 0);

//         consumer.consume_buffered_bundles(
//             &working_bank,
//             &mut bundle_storage,
//             &mut bundle_stage_leader_metrics,
//         );

//         // its expected there are 3 transactions. One to initialize the tip program configuration, one to change the tip receiver,
//         // and another with the tip

//         let mut transactions = Vec::new();
//         while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
//             assert_eq!(bank.slot(), wbe_bank.slot());
//             transactions.extend(entry.transactions);
//             if transactions.len() == 5 {
//                 break;
//             }
//         }

//         // tip management on the first bundle involves:
//         // calling initialize on the tip payment and tip distribution programs
//         // creating the tip distribution account for this validator's epoch (the MEV piggy bank)
//         // changing the tip receiver and block builder tx
//         // the original transfer that was sent
//         let keypair = cluster_info.keypair().clone();

//         assert_eq!(
//             transactions[0],
//             tip_manager
//                 .initialize_tip_payment_program_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         assert_eq!(
//             transactions[1],
//             tip_manager
//                 .initialize_tip_distribution_config_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         assert_eq!(
//             transactions[2],
//             tip_manager
//                 .initialize_tip_distribution_account_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
//         // TipPayment program during initialization
//         assert_eq!(
//             transactions[3],
//             tip_manager
//                 .build_change_tip_receiver_and_block_builder_tx(
//                     &keypair.pubkey(),
//                     &derive_tip_distribution_account_address(
//                         &tip_manager.tip_distribution_program_id(),
//                         &genesis_config_info.validator_pubkey,
//                         working_bank.epoch()
//                     )
//                     .0,
//                     &working_bank,
//                     &keypair,
//                     &keypair.pubkey(),
//                     &block_builder_pubkey,
//                     10
//                 )
//                 .to_versioned_transaction()
//         );
//         assert_eq!(
//             transactions[4],
//             sanitized_bundle.transactions[0].to_versioned_transaction()
//         );

//         poh_recorder
//             .write()
//             .unwrap()
//             .is_exited
//             .store(true, Ordering::Relaxed);
//         exit.store(true, Ordering::Relaxed);
//         poh_simulator.join().unwrap();
//     }

//     #[test]
//     fn test_handle_tip_programs() {
//         solana_logger::setup();
//         let TestFixture {
//             genesis_config_info,
//             leader_keypair,
//             bank,
//             exit,
//             poh_recorder,
//             transaction_recorder,
//             poh_simulator,
//             entry_receiver,
//             bank_forks: _bank_forks,
//         } = create_test_fixture(1_000_000);

//         let (replay_vote_sender, _replay_vote_receiver) = unbounded();
//         let committer = Committer::new(
//             None,
//             replay_vote_sender,
//             Arc::new(PrioritizationFeeCache::new(0u64)),
//         );

//         let block_builder_pubkey = Pubkey::new_unique();
//         let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
//         let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
//             block_builder: block_builder_pubkey,
//             block_builder_commission: 10,
//         }));

//         let cluster_info = Arc::new(ClusterInfo::new(
//             ContactInfo::new(leader_keypair.pubkey(), 0, 0),
//             Arc::new(leader_keypair),
//             SocketAddrSpace::new(true),
//         ));

//         let working_bank = poh_recorder.read().unwrap().bank().unwrap();

//         let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
//         assert_matches!(
//             BundleConsumer::handle_tip_programs(
//                 &BundleAccountLocker::default(),
//                 &tip_manager,
//                 &cluster_info,
//                 &block_builder_info,
//                 &committer,
//                 &transaction_recorder,
//                 &QosService::new(1),
//                 &None,
//                 Duration::from_secs(10),
//                 &working_bank,
//                 &mut bundle_stage_leader_metrics
//             ),
//             Ok(())
//         );

//         let mut transactions = Vec::new();
//         while let Ok((wbe_bank, (entry, _ticks))) = entry_receiver.recv() {
//             assert_eq!(bank.slot(), wbe_bank.slot());
//             transactions.extend(entry.transactions);
//             if transactions.len() == 4 {
//                 break;
//             }
//         }

//         let keypair = cluster_info.keypair().clone();
//         // expect to see initialize tip payment program, tip distribution program, initialize tip distribution account, change tip receiver + change block builder
//         assert_eq!(
//             transactions[0],
//             tip_manager
//                 .initialize_tip_payment_program_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         assert_eq!(
//             transactions[1],
//             tip_manager
//                 .initialize_tip_distribution_config_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         assert_eq!(
//             transactions[2],
//             tip_manager
//                 .initialize_tip_distribution_account_tx(&bank, &keypair)
//                 .to_versioned_transaction()
//         );
//         // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
//         // TipPayment program during initialization
//         assert_eq!(
//             transactions[3],
//             tip_manager
//                 .build_change_tip_receiver_and_block_builder_tx(
//                     &keypair.pubkey(),
//                     &derive_tip_distribution_account_address(
//                         &tip_manager.tip_distribution_program_id(),
//                         &genesis_config_info.validator_pubkey,
//                         working_bank.epoch()
//                     )
//                     .0,
//                     &working_bank,
//                     &keypair,
//                     &keypair.pubkey(),
//                     &block_builder_pubkey,
//                     10
//                 )
//                 .to_versioned_transaction()
//         );

//         poh_recorder
//             .write()
//             .unwrap()
//             .is_exited
//             .store(true, Ordering::Relaxed);
//         exit.store(true, Ordering::Relaxed);
//         poh_simulator.join().unwrap();
//     }

//     #[test]
//     fn test_reserve_bundle_blockspace_success() {
//         let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
//         let bank = Arc::new(Bank::new_for_tests(&genesis_config));

//         let keypair1 = Keypair::new();
//         let keypair2 = Keypair::new();
//         let transfer_tx = RuntimeTransaction::from_transaction_for_tests(transfer(
//             &keypair1,
//             &keypair2.pubkey(),
//             1,
//             bank.parent_hash(),
//         ));
//         let sanitized_bundle = SanitizedBundle {
//             transactions: vec![transfer_tx],
//             bundle_id: String::default(),
//         };

//         let transfer_cost =
//             CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);

//         let qos_service = QosService::new(1);
//         assert!(
//             BundleConsumer::reserve_bundle_blockspace(&qos_service, &sanitized_bundle, &bank)
//                 .is_ok()
//         );
//         assert_eq!(
//             bank.read_cost_tracker().unwrap().block_cost(),
//             transfer_cost.sum()
//         );
//     }

//     #[test]
//     fn test_reserve_bundle_blockspace_failure() {
//         let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
//         let bank = Arc::new(Bank::new_for_tests(&genesis_config));

//         let keypair1 = Keypair::new();
//         let keypair2 = Keypair::new();
//         let transfer_tx1 = RuntimeTransaction::from_transaction_for_tests(transfer(
//             &keypair1,
//             &keypair2.pubkey(),
//             1,
//             bank.parent_hash(),
//         ));
//         let transfer_tx2 = RuntimeTransaction::from_transaction_for_tests(transfer(
//             &keypair1,
//             &keypair2.pubkey(),
//             2,
//             bank.parent_hash(),
//         ));
//         let sanitized_bundle = SanitizedBundle {
//             transactions: vec![transfer_tx1, transfer_tx2],
//             bundle_id: String::default(),
//         };

//         // set block cost limit to 1 transfer transaction, try to process 2, should return an error
//         // and rollback block cost added
//         let transfer_cost =
//             CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);
//         bank.write_cost_tracker()
//             .unwrap()
//             .set_limits(u64::MAX, transfer_cost.sum(), u64::MAX);

//         let qos_service = QosService::new(1);

//         assert!(
//             BundleConsumer::reserve_bundle_blockspace(&qos_service, &sanitized_bundle, &bank)
//                 .is_err()
//         );
//         // the block cost shall not be modified
//         assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);
//     }
// }
