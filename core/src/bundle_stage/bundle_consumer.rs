use {
    crate::banking_stage::{
        committer::{CommitTransactionDetails, Committer},
        consumer::{
            Consumer, ExecuteAndCommitTransactionsOutput, ExecutionFlags,
            LeaderProcessedTransactionCounts, ProcessTransactionBatchOutput,
        },
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
        scheduler_messages::MaxAge,
    },
    smallvec::SmallVec,
    solana_clock::MAX_PROCESSING_AGE,
    solana_poh::transaction_recorder::TransactionRecorder,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_transaction::TransactionError,
    std::{
        iter::repeat,
        thread::sleep,
        time::{Duration, Instant},
    },
};

pub struct BundleConsumer {
    consumer: Consumer,
}

impl BundleConsumer {
    pub fn new(
        committer: Committer,
        transaction_recorder: TransactionRecorder,
        log_messages_bytes_limit: Option<usize>,
    ) -> Self {
        Self {
            consumer: Consumer::new(committer, transaction_recorder, log_messages_bytes_limit),
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
        &self,
        bank: &Bank,
        txs: &[impl TransactionWithMeta],
        max_ages: &[MaxAge],
        max_bundle_duration: Duration,
    ) -> ProcessTransactionBatchOutput {
        // Need to filter out transactions since they were sanitized earlier.
        // This means that the transaction may cross and epoch boundary (not allowed),
        //  or account lookup tables may have been closed.
        let pre_results: SmallVec<[_; 5]> = txs
            .iter()
            .zip(max_ages)
            .map(|(tx, max_age)| {
                bank.resanitize_transaction_minimally(
                    tx,
                    max_age.sanitized_epoch,
                    max_age.alt_invalidation_slot,
                )
            })
            .collect();

        let mut error_counters = TransactionErrorMetrics::default();
        let (check_results, _) = bank.check_transactions_external(
            txs,
            &pre_results,
            MAX_PROCESSING_AGE,
            false,
            &mut error_counters,
        );
        if let Some((index, err)) = check_results
            .into_iter()
            .enumerate()
            .find_map(|(index, result)| result.err().map(|err| (index, err)))
        {
            let mut commit_transactions_result =
                vec![
                    CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled);
                    txs.len()
                ];
            commit_transactions_result[index] = CommitTransactionDetails::NotCommitted(err);

            return ProcessTransactionBatchOutput {
                cost_model_throttled_transactions_count: 0,
                cost_model_us: 0,
                execute_and_commit_transactions_output: ExecuteAndCommitTransactionsOutput {
                    execute_and_commit_timings: LeaderExecuteAndCommitTimings::default(),
                    error_counters,
                    transaction_counts: LeaderProcessedTransactionCounts::default(),
                    retryable_transaction_indexes: vec![],
                    commit_transactions_result: Ok(commit_transactions_result),
                },
            };
        }

        // The caller holds this bundle in the bundle account locker, which keeps new BankingStage
        // batches off its accounts, so wait for the batches already holding them.
        let start = Instant::now();
        loop {
            let output = self
                .consumer
                .process_and_record_transactions_with_pre_results(
                    bank,
                    txs,
                    repeat(Ok(())),
                    None,
                    &ExecutionFlags {
                        simulate: false,
                        drop_on_failure: true,
                        all_or_nothing: true,
                    },
                    None,
                    true,
                );
            if !Self::blocked_on_account_locks(&output) || start.elapsed() >= max_bundle_duration {
                return output;
            }
            sleep(Duration::from_millis(1));
        }
    }

    /// True if account locks are the only thing that stopped this bundle.
    fn blocked_on_account_locks(output: &ProcessTransactionBatchOutput) -> bool {
        output
            .execute_and_commit_transactions_output
            .commit_transactions_result
            .as_ref()
            .is_ok_and(|results| {
                use {CommitTransactionDetails::NotCommitted, TransactionError::*};
                results
                    .iter()
                    .any(|result| matches!(result, NotCommitted(AccountInUse)))
                    && results.iter().all(|result| {
                        matches!(result, NotCommitted(AccountInUse | CommitCancelled))
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            banking_stage::{
                committer::{CommitTransactionDetails, Committer},
                consumer::{
                    ExecuteAndCommitTransactionsOutput, LeaderProcessedTransactionCounts,
                    ProcessTransactionBatchOutput,
                },
                scheduler_messages::MaxAge,
            },
            bundle_stage::{BundleExecutionError, BundleStage, bundle_consumer::BundleConsumer},
        },
        crossbeam_channel::unbounded,
        solana_cost_model::cost_model::CostModel,
        solana_entry::entry::Entry,
        solana_genesis_config::GenesisConfig,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::{
            GenesisConfigInfo, bootstrap_validator_stake_lamports,
            create_genesis_config_with_leader,
        },
        solana_poh::{
            poh_recorder::PohRecorderError,
            record_channels::{RecordReceiver, record_channels},
            transaction_recorder::TransactionRecorder,
        },
        solana_pubkey::{Pubkey, new_rand},
        solana_runtime::{
            bank::{Bank, entry_bytes_budget::EntryBytesReserveError},
            bank_forks::BankForks,
            prioritization_fee_cache::PrioritizationFeeCache,
            vote_sender_types::ReplayVoteReceiver,
        },
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_sdk_ids::sysvar::clock,
        solana_signer::Signer,
        solana_system_transaction::transfer,
        solana_transaction::{
            InstructionError, Transaction, TransactionError, sanitized::SanitizedTransaction,
        },
        std::{
            assert_matches,
            sync::{Arc, RwLock},
            time::{Duration, Instant},
        },
        test_case::test_case,
    };

    const TEST_BUNDLE_LEN: usize = 5;

    fn sanitize_transactions(
        txs: Vec<Transaction>,
    ) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
        txs.into_iter()
            .map(RuntimeTransaction::from_transaction_for_tests)
            .collect()
    }

    fn new_test_consumer(bank: &Bank) -> (BundleConsumer, RecordReceiver, ReplayVoteReceiver) {
        let (record_sender, mut record_receiver) = record_channels(false);
        record_receiver.restart(bank.bank_id());
        let (replay_vote_sender, replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Some(Arc::new(PrioritizationFeeCache::new(0u64))),
        );
        let consumer =
            BundleConsumer::new(committer, TransactionRecorder::new(record_sender), None);
        (consumer, record_receiver, replay_vote_receiver)
    }

    fn test_bank() -> (GenesisConfig, Keypair, Arc<Bank>, Arc<RwLock<BankForks>>) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config_with_leader(
            10_000,
            &Pubkey::new_unique(),
            bootstrap_validator_stake_lamports(),
        );
        let (bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        (genesis_config, mint_keypair, bank, bank_forks)
    }

    fn process_bundle(
        consumer: &BundleConsumer,
        bank: &Bank,
        transactions: &[RuntimeTransaction<SanitizedTransaction>],
    ) -> (Result<Vec<CommitTransactionDetails>, PohRecorderError>, u64) {
        let ProcessTransactionBatchOutput {
            execute_and_commit_transactions_output:
                ExecuteAndCommitTransactionsOutput {
                    commit_transactions_result,
                    ..
                },
            cost_model_throttled_transactions_count,
            ..
        } = consumer.process_and_record_aged_transactions(
            bank,
            transactions,
            &vec![MaxAge::MAX; transactions.len()],
            Duration::from_millis(20),
        );
        (
            commit_transactions_result,
            cost_model_throttled_transactions_count,
        )
    }

    #[test]
    fn test_bundle_waits_for_account_locks() {
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();
        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);
        let transfer_from_mint = || {
            sanitize_transactions(vec![transfer(
                &mint_keypair,
                &new_rand(),
                1,
                genesis_config.hash(),
            )])
        };
        let held = transfer_from_mint();
        let bundle = transfer_from_mint();
        let batch = bank.prepare_sanitized_batch(&held);

        let start = Instant::now();
        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &bundle);
        assert!(start.elapsed() >= Duration::from_millis(20));
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 1);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::NotCommitted(TransactionError::AccountInUse)
        );
        // The bundle's QoS reservation is rolled back.
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);

        std::thread::scope(|scope| {
            scope.spawn(move || {
                std::thread::sleep(Duration::from_millis(5));
                drop(batch);
            });
            let output = consumer.process_and_record_aged_transactions(
                &bank,
                &bundle,
                &[MaxAge::MAX],
                Duration::from_secs(60),
            );
            assert_matches!(BundleStage::to_bundle_result(&output), Ok(()));
        });
    }

    #[test]
    fn test_quiesced_bank_bundle_is_retryable() {
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();
        let recipients = [Pubkey::new_unique(), Pubkey::new_unique()];
        let transactions = sanitize_transactions(
            recipients
                .iter()
                .map(|recipient| transfer(&mint_keypair, recipient, 1, genesis_config.hash()))
                .collect(),
        );
        let mint_balance = bank.get_balance(&mint_keypair.pubkey());
        let block_cost = bank.read_cost_tracker().unwrap().block_cost();
        let (consumer, record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        bank.quiesce_transaction_execution();
        let output = consumer.process_and_record_aged_transactions(
            &bank,
            &transactions,
            &[MaxAge::MAX; 2],
            Duration::from_millis(20),
        );

        let execute_output = &output.execute_and_commit_transactions_output;
        assert_matches!(
            &execute_output.commit_transactions_result,
            Err(PohRecorderError::MaxHeightReached)
        );
        assert_eq!(
            execute_output.transaction_counts,
            LeaderProcessedTransactionCounts {
                attempted_processing_count: transactions.len() as u64,
                ..LeaderProcessedTransactionCounts::default()
            }
        );
        assert!(
            execute_output
                .retryable_transaction_indexes
                .iter()
                .enumerate()
                .all(|(index, retryable)| retryable.index == index
                    && retryable.immediately_retryable)
        );
        assert_eq!(
            execute_output.retryable_transaction_indexes.len(),
            transactions.len()
        );
        assert_matches!(
            BundleStage::to_bundle_result(&output),
            Err(BundleExecutionError::ErrorRetryable)
        );
        assert!(record_receiver.try_recv().is_err());
        assert_eq!(bank.get_balance(&mint_keypair.pubkey()), mint_balance);
        assert!(
            recipients
                .iter()
                .all(|recipient| bank.get_balance(recipient) == 0)
        );
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), block_cost);
    }

    // `MaxAge::MAX` isn't the bank's epoch, so the pre-check re-checks reserved keys.
    #[test_case(false, TransactionError::InvalidAccountForFee; "unfunded_fee_payer")]
    #[test_case(true, TransactionError::ResanitizationNeeded; "writes_reserved_account")]
    fn test_multi_tx_bundle_last_tx_bad_not_committed(
        writes_reserved_account: bool,
        expected_err: TransactionError,
    ) {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();

        let bad_tx = if writes_reserved_account {
            transfer(&mint_keypair, &clock::id(), 1, genesis_config.hash())
        } else {
            transfer(&Keypair::new(), &new_rand(), 1, genesis_config.hash())
        };
        let transactions = sanitize_transactions(vec![
            transfer(&mint_keypair, &new_rand(), 1, genesis_config.hash()),
            transfer(&mint_keypair, &new_rand(), 1, genesis_config.hash()),
            transfer(&mint_keypair, &new_rand(), 1, genesis_config.hash()),
            bad_tx,
        ]);

        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 4);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_matches!(
            commit_transactions_result[1],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_matches!(
            commit_transactions_result[2],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_eq!(
            commit_transactions_result[3],
            CommitTransactionDetails::NotCommitted(expected_err)
        );
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);
    }

    #[test]
    fn test_multi_bundle_seed_fee_payer_ok() {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let transactions = sanitize_transactions(vec![
            transfer(&mint_keypair, &kp1.pubkey(), 1, genesis_config.hash()),
            transfer(&kp1, &kp2.pubkey(), 1, genesis_config.hash()),
            transfer(&kp2, &new_rand(), 1, genesis_config.hash()),
        ]);

        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 3);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::Committed { result: Ok(_), .. }
        );
        assert_matches!(
            commit_transactions_result[1],
            CommitTransactionDetails::Committed { result: Ok(_), .. }
        );
        assert_matches!(
            commit_transactions_result[2],
            CommitTransactionDetails::Committed { result: Ok(_), .. }
        );
    }

    #[test_case(false; "one_byte_short")]
    #[test_case(true; "exact_boundary")]
    fn test_bundle_entry_bytes_budget(exact_boundary: bool) {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();
        let recipients: [Pubkey; TEST_BUNDLE_LEN] = std::array::from_fn(|_| Pubkey::new_unique());
        let transactions: [_; TEST_BUNDLE_LEN] = std::array::from_fn(|index| {
            RuntimeTransaction::from_transaction_for_tests(transfer(
                &mint_keypair,
                &recipients[index],
                1,
                genesis_config.hash(),
            ))
        });
        let entries = vec![Entry {
            num_hashes: 0,
            hash: genesis_config.hash(),
            transactions: transactions
                .iter()
                .map(|tx| tx.to_versioned_transaction())
                .collect(),
        }];
        let entry_bytes = wincode::serialized_size(&entries).unwrap();
        let entry_bytes_limit = bank.entry_bytes_budget().slot_limit();
        let remaining_entry_bytes = entry_bytes_limit
            .checked_sub(entry_bytes)
            .expect("test bundle must fit in an empty slot");
        bank.entry_bytes_budget()
            .reserve(remaining_entry_bytes + u64::from(!exact_boundary))
            .unwrap();

        let mint_balance = bank.get_balance(&mint_keypair.pubkey());
        let block_cost = bank.read_cost_tracker().unwrap().block_cost();

        let (consumer, record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let max_ages = [MaxAge::MAX; TEST_BUNDLE_LEN];
        let ProcessTransactionBatchOutput {
            execute_and_commit_transactions_output:
                ExecuteAndCommitTransactionsOutput {
                    transaction_counts,
                    retryable_transaction_indexes,
                    commit_transactions_result,
                    ..
                },
            cost_model_throttled_transactions_count,
            ..
        } = consumer.process_and_record_aged_transactions(
            &bank,
            &transactions,
            &max_ages,
            Duration::from_millis(20),
        );

        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert_eq!(
            transaction_counts.processed_count,
            transactions.len() as u64
        );
        if exact_boundary {
            assert!(retryable_transaction_indexes.is_empty());
            let commit_transactions_result = commit_transactions_result.unwrap();
            assert_eq!(commit_transactions_result.len(), transactions.len());
            assert!(commit_transactions_result.iter().all(|result| matches!(
                result,
                CommitTransactionDetails::Committed { result: Ok(_), .. }
            )));
            let record = record_receiver.try_recv().unwrap();
            assert_eq!(record.bank_id, bank.bank_id());
            assert!(!record.reschedule_on_sad_handover);
            assert_eq!(record.transactions, entries[0].transactions.clone());
            assert!(record_receiver.try_recv().is_err());
            assert!(
                recipients
                    .iter()
                    .all(|recipient| bank.get_balance(recipient) == 1)
            );
            assert_eq!(
                bank.entry_bytes_budget().reserve(1),
                Err(EntryBytesReserveError::ExceedsSlotLimit)
            );
        } else {
            assert_matches!(
                commit_transactions_result,
                Err(PohRecorderError::MaxHeightReached)
            );
            assert_eq!(retryable_transaction_indexes.len(), transactions.len());
            assert!(
                retryable_transaction_indexes
                    .iter()
                    .enumerate()
                    .all(|(index, retryable)| retryable.index == index
                        && retryable.immediately_retryable)
            );
            assert!(record_receiver.try_recv().is_err());
            assert_eq!(bank.get_balance(&mint_keypair.pubkey()), mint_balance);
            assert!(
                recipients
                    .iter()
                    .all(|recipient| bank.get_balance(recipient) == 0)
            );
            assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), block_cost);
        }
    }

    #[test]
    fn test_tx_compute_reservation_exceeds_drops_bundle() {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let transactions = sanitize_transactions(vec![
            transfer(&mint_keypair, &kp1.pubkey(), 1, genesis_config.hash()),
            transfer(&kp1, &kp2.pubkey(), 1, genesis_config.hash()),
            transfer(&kp2, &new_rand(), 1, genesis_config.hash()),
        ]);

        let txs_costs: Vec<_> = transactions
            .iter()
            .map(|tx| CostModel::calculate_cost(tx, &bank.feature_set))
            .collect();
        let total_cost: u64 = txs_costs.iter().map(|cost| cost.sum()).sum();
        bank.write_cost_tracker().unwrap().set_limits(
            solana_cost_model::cost_tracker::CostTrackerLimits::new(
                u64::MAX,
                total_cost - 1,
                u64::MAX,
            ),
        );

        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 1);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 3);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_matches!(
            commit_transactions_result[1],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_matches!(
            commit_transactions_result[2],
            CommitTransactionDetails::NotCommitted(TransactionError::WouldExceedMaxBlockCostLimit)
        );
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);

        // A throttled bundle doesn't wait for an account lock it also conflicts on.
        let held = sanitize_transactions(vec![transfer(
            &mint_keypair,
            &new_rand(),
            1,
            genesis_config.hash(),
        )]);
        let _batch = bank.prepare_sanitized_batch(&held);
        let start = Instant::now();
        let output = consumer.process_and_record_aged_transactions(
            &bank,
            &transactions,
            &[MaxAge::MAX; 3],
            Duration::from_secs(30),
        );
        assert!(start.elapsed() < Duration::from_secs(10));
        assert_matches!(
            output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .unwrap()[0],
            CommitTransactionDetails::NotCommitted(TransactionError::AccountInUse)
        );
    }

    #[test]
    fn test_transaction_already_processed_fails() {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();

        let transactions = sanitize_transactions(vec![transfer(
            &mint_keypair,
            &new_rand(),
            1,
            genesis_config.hash(),
        )]);

        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 1);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::Committed { result: Ok(_), .. }
        );

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 1);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::NotCommitted(TransactionError::AlreadyProcessed)
        );
    }

    #[test]
    fn test_non_fee_payer_failure_tx_reverts() {
        agave_logger::setup();
        let (genesis_config, mint_keypair, bank, _bank_forks) = test_bank();

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let transactions = sanitize_transactions(vec![
            transfer(&mint_keypair, &kp1.pubkey(), 1, genesis_config.hash()),
            transfer(&kp1, &kp2.pubkey(), 10, genesis_config.hash()),
        ]);

        let (consumer, _record_receiver, _replay_vote_receiver) = new_test_consumer(&bank);

        let (commit_transactions_result, cost_model_throttled_transactions_count) =
            process_bundle(&consumer, &bank, &transactions);
        assert_eq!(cost_model_throttled_transactions_count, 0);
        assert!(commit_transactions_result.is_ok());
        let commit_transactions_result = commit_transactions_result.unwrap();
        assert_eq!(commit_transactions_result.len(), 2);
        assert_matches!(
            commit_transactions_result[0],
            CommitTransactionDetails::NotCommitted(TransactionError::CommitCancelled)
        );
        assert_matches!(
            commit_transactions_result[1],
            CommitTransactionDetails::NotCommitted(TransactionError::InstructionError(
                0,
                InstructionError::Custom(1)
            ))
        );
    }
}
