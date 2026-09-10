use {
    super::{
        BankingStageStats, SLOT_BOUNDARY_CHECK_PERIOD,
        consumer::Consumer,
        decision_maker::{BufferedPacketsDecision, DecisionMaker},
        latest_validator_vote_packet::VoteSource,
        leader_slot_metrics::{
            CommittedTransactionsCounts, LeaderSlotMetricsTracker, ProcessTransactionsSummary,
        },
        vote_packet_receiver::VotePacketReceiver,
        vote_storage::VoteStorage,
    },
    crate::{
        banking_stage::{
            consumer::{ExecuteAndCommitTransactionsOutput, ProcessTransactionBatchOutput},
            transaction_scheduler::transaction_state_container::RuntimeTransactionView,
        },
        bundle_stage::bundle_account_locker::BundleAccountLocker,
    },
    agave_transaction_view::{
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    crossbeam_channel::RecvTimeoutError,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_clock::FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
    solana_measure::{measure::Measure, measure_us},
    solana_perf::packet::bytes::Bytes,
    solana_poh::poh_recorder::PohRecorderError,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::TransactionMeta,
        transaction_with_meta::TransactionWithMeta,
    },
    solana_svm::{
        account_loader::TransactionCheckResult, transaction_error_metrics::TransactionErrorMetrics,
    },
    solana_svm_transaction::svm_message::SVMMessage,
    solana_time_utils::timestamp,
    solana_transaction::sanitized::MessageHash,
    solana_transaction_error::TransactionError,
    std::{
        collections::VecDeque,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        time::Instant,
    },
    tokio_util::sync::CancellationToken,
};

mod transaction {
    pub use solana_transaction_error::TransactionResult as Result;
}

pub struct VoteWorker {
    exit: Arc<AtomicBool>,
    shutdown_signal: CancellationToken,
    decision_maker: DecisionMaker,
    tpu_receiver: VotePacketReceiver,
    gossip_receiver: VotePacketReceiver,
    storage: VoteStorage,
    bank_forks: Arc<RwLock<BankForks>>,
    consumer: Consumer,
    bundle_account_locker: BundleAccountLocker,
}

impl VoteWorker {
    pub fn new(
        exit: Arc<AtomicBool>,
        shutdown_signal: CancellationToken,
        decision_maker: DecisionMaker,
        tpu_receiver: VotePacketReceiver,
        gossip_receiver: VotePacketReceiver,
        storage: VoteStorage,
        bank_forks: Arc<RwLock<BankForks>>,
        consumer: Consumer,
        bundle_account_locker: BundleAccountLocker,
    ) -> Self {
        Self {
            exit,
            shutdown_signal,
            decision_maker,
            tpu_receiver,
            gossip_receiver,
            storage,
            bank_forks,
            consumer,
            bundle_account_locker,
        }
    }

    pub fn run(mut self) {
        let mut banking_stage_stats = BankingStageStats::new();
        let mut slot_metrics_tracker = LeaderSlotMetricsTracker::default();

        let mut last_metrics_update = Instant::now();

        while !self.exit.load(Ordering::Relaxed) {
            if !self.storage.is_empty()
                || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
            {
                let (_, process_buffered_packets_us) =
                    measure_us!(self.process_buffered_packets(
                        &mut banking_stage_stats,
                        &mut slot_metrics_tracker
                    ));
                slot_metrics_tracker
                    .increment_process_buffered_packets_us(process_buffered_packets_us);
                last_metrics_update = Instant::now();
            }

            // Check for new packets from the tpu receiver
            match self.tpu_receiver.receive_and_buffer_packets(
                &mut self.storage,
                &mut banking_stage_stats,
                &mut slot_metrics_tracker,
                VoteSource::Tpu,
            ) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => {
                    self.shutdown_signal.cancel();

                    break;
                }
            }
            // Check for new packets from the gossip receiver
            match self.gossip_receiver.receive_and_buffer_packets(
                &mut self.storage,
                &mut banking_stage_stats,
                &mut slot_metrics_tracker,
                VoteSource::Gossip,
            ) {
                Ok(()) | Err(RecvTimeoutError::Timeout) => (),
                Err(RecvTimeoutError::Disconnected) => {
                    self.shutdown_signal.cancel();

                    break;
                }
            }
            banking_stage_stats.report(1000);
        }
    }

    fn process_buffered_packets(
        &mut self,
        banking_stage_stats: &mut BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        let (decision, make_decision_us) =
            measure_us!(self.decision_maker.make_consume_or_forward_decision());
        let metrics_action = slot_metrics_tracker.check_leader_slot_boundary(decision.bank());
        slot_metrics_tracker.increment_make_decision_us(make_decision_us);

        // Take metrics action before processing packets (potentially resetting the
        // slot metrics tracker to the next slot) so that we don't count the
        // packet processing metrics from the next slot towards the metrics
        // of the previous slot
        slot_metrics_tracker.apply_action(metrics_action);

        match decision {
            BufferedPacketsDecision::Consume(bank) => {
                let (_, consume_buffered_packets_us) = measure_us!(self.consume_buffered_packets(
                    &bank,
                    banking_stage_stats,
                    slot_metrics_tracker,
                ));
                slot_metrics_tracker
                    .increment_consume_buffered_packets_us(consume_buffered_packets_us);
            }
            BufferedPacketsDecision::Forward => {
                // get current working bank from bank_forks, use it to sanitize transaction and
                // load all accounts from address loader;
                let current_bank = self.bank_forks.read().unwrap().working_bank();
                self.storage.cache_epoch_boundary_info(&current_bank);
                self.storage.clear();
            }
            BufferedPacketsDecision::ForwardAndHold => {
                // get current working bank from bank_forks, use it to sanitize transaction and
                // load all accounts from address loader;
                let current_bank = self.bank_forks.read().unwrap().working_bank();
                self.storage.cache_epoch_boundary_info(&current_bank);
            }
            BufferedPacketsDecision::Hold => {}
        }
    }

    fn consume_buffered_packets(
        &mut self,
        bank: &Bank,
        banking_stage_stats: &BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) {
        let restored_vote_count = self.storage.restore_taken_votes_for_bank(bank);
        if self.storage.is_empty() {
            return;
        }

        let mut consumed_buffered_packets_count = 0;
        let mut rebuffered_packet_count = restored_vote_count;
        let mut proc_start = Measure::start("consume_buffered_process");
        let num_packets_to_process = self.storage.len();

        let reached_end_of_slot = self.process_packets(
            bank,
            &mut consumed_buffered_packets_count,
            &mut rebuffered_packet_count,
            banking_stage_stats,
            slot_metrics_tracker,
        );

        if reached_end_of_slot {
            slot_metrics_tracker.set_end_of_slot_unprocessed_buffer_len(self.storage.len() as u64);
        }

        proc_start.stop();
        debug!(
            "@{:?} done processing buffered batches: {} time: {:?}ms tx count: {} tx/s: {}",
            timestamp(),
            num_packets_to_process,
            proc_start.as_ms(),
            consumed_buffered_packets_count,
            (consumed_buffered_packets_count as f32) / (proc_start.as_s())
        );

        banking_stage_stats
            .consume_buffered_packets_elapsed
            .fetch_add(proc_start.as_us(), Ordering::Relaxed);
        banking_stage_stats
            .rebuffered_packets_count
            .fetch_add(rebuffered_packet_count, Ordering::Relaxed);
        banking_stage_stats
            .consumed_buffered_packets_count
            .fetch_add(consumed_buffered_packets_count, Ordering::Relaxed);
    }

    // returns `true` if the end of slot is reached
    fn process_packets(
        &mut self,
        bank: &Bank,
        consumed_buffered_packets_count: &mut usize,
        rebuffered_packet_count: &mut usize,
        banking_stage_stats: &BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> bool {
        // Based on the stake distribution present in the supplied bank, drain the unprocessed votes
        // from each validator using a weighted random ordering. Votes from validators with
        // 0 stake are ignored.
        let (all_vote_packets, deferred_restore_count) =
            self.storage.drain_unprocessed_with_deferred_restores(bank);
        *rebuffered_packet_count += deferred_restore_count;
        let mut all_vote_packets = VecDeque::from(all_vote_packets);
        let mut error_counters: TransactionErrorMetrics = TransactionErrorMetrics::default();
        // Process one vote at a time to avoid over-reserving block CUs during packing.
        // This also keeps each recorded vote batch small, which favors entry/FEC-set packing.
        while let Some((vote_pubkey, packet)) = all_vote_packets.pop_front() {
            let Some(sanitized_transaction) =
                consume_scan_should_process_packet(bank, packet, &mut error_counters)
            else {
                if let Some(retained_vote) = self.storage.take_deferred_retained_vote(vote_pubkey) {
                    *rebuffered_packet_count += 1;
                    all_vote_packets.push_front(retained_vote);
                }
                continue;
            };

            let (process_transactions_summary, process_packets_transactions_us) =
                measure_us!(self.process_packets_transactions(
                    bank,
                    std::slice::from_ref(&sanitized_transaction),
                    banking_stage_stats,
                    slot_metrics_tracker,
                ));
            slot_metrics_tracker
                .increment_process_packets_transactions_us(process_packets_transactions_us);

            let ProcessTransactionsSummary {
                reached_max_poh_height,
                transaction_counts,
                retryable_transaction_indexes: retryable_vote_indices,
                ..
            } = process_transactions_summary;

            let retryable_vote_count = retryable_vote_indices.len();
            let vote_succeeded = transaction_counts
                .committed_transactions_with_successful_result_count
                .0
                == 1
                || (retryable_vote_count == 0
                    && bank.get_signature_status(&sanitized_transaction.signatures()[0])
                        == Some(Ok(())));
            *consumed_buffered_packets_count += usize::from(retryable_vote_count == 0);
            *rebuffered_packet_count += retryable_vote_count;

            slot_metrics_tracker.increment_retryable_packets_count(retryable_vote_count as u64);

            if vote_succeeded {
                self.storage.retain_processed_vote(
                    vote_pubkey,
                    sanitized_transaction
                        .into_inner_transaction()
                        .into_view()
                        .into_inner_data(),
                );
            } else if retryable_vote_count != 0 {
                // vote is processed one at a time, so the only valid retryable index is 0
                assert_eq!(retryable_vote_indices.as_slice(), &[0]);

                self.storage.reinsert_packets(std::iter::once(
                    sanitized_transaction.into_inner_transaction().into_view(),
                ));
            } else if let Some(retained_vote) =
                self.storage.take_deferred_retained_vote(vote_pubkey)
            {
                *rebuffered_packet_count += 1;
                all_vote_packets.push_front(retained_vote);
            }

            if has_reached_end_of_slot(reached_max_poh_height, bank) {
                self.storage
                    .reinsert_packets(all_vote_packets.into_iter().map(|(_, vote)| vote));
                return true;
            }
        }

        false
    }

    fn process_packets_transactions(
        &self,
        bank: &Bank,
        sanitized_transactions: &[impl TransactionWithMeta],
        banking_stage_stats: &BankingStageStats,
        slot_metrics_tracker: &mut LeaderSlotMetricsTracker,
    ) -> ProcessTransactionsSummary {
        let (mut process_transactions_summary, process_transactions_us) =
            measure_us!(Self::process_transactions(
                &self.consumer,
                bank,
                sanitized_transactions,
                &self.bundle_account_locker
            ));
        slot_metrics_tracker.increment_process_transactions_us(process_transactions_us);
        banking_stage_stats
            .transaction_processing_elapsed
            .fetch_add(process_transactions_us, Ordering::Relaxed);

        let ProcessTransactionsSummary {
            ref retryable_transaction_indexes,
            ref error_counters,
            ..
        } = process_transactions_summary;

        slot_metrics_tracker.accumulate_process_transactions_summary(&process_transactions_summary);
        slot_metrics_tracker.accumulate_transaction_errors(error_counters);

        // Filter out the retryable transactions that are too old
        let (filtered_retryable_transaction_indexes, filter_retryable_packets_us) =
            measure_us!(Self::filter_pending_packets_from_pending_txs(
                bank,
                sanitized_transactions,
                retryable_transaction_indexes,
            ));
        slot_metrics_tracker.increment_filter_retryable_packets_us(filter_retryable_packets_us);
        banking_stage_stats
            .filter_pending_packets_elapsed
            .fetch_add(filter_retryable_packets_us, Ordering::Relaxed);

        let retryable_packets_filtered_count = retryable_transaction_indexes
            .len()
            .saturating_sub(filtered_retryable_transaction_indexes.len());
        slot_metrics_tracker
            .increment_retryable_packets_filtered_count(retryable_packets_filtered_count as u64);

        banking_stage_stats
            .dropped_forward_packets_count
            .fetch_add(retryable_packets_filtered_count, Ordering::Relaxed);

        process_transactions_summary.retryable_transaction_indexes =
            filtered_retryable_transaction_indexes;
        process_transactions_summary
    }

    /// Sends transactions to the bank.
    ///
    /// Returns the number of transactions successfully processed by the bank, which may be less
    /// than the total number if max PoH height was reached and the bank halted
    #[cfg_attr(test, qualifier_attr::qualifiers(pub(crate)))]
    fn process_transactions(
        consumer: &Consumer,
        bank: &Bank,
        transactions: &[impl TransactionWithMeta],
        bundle_account_locker: &BundleAccountLocker,
    ) -> ProcessTransactionsSummary {
        let process_transaction_batch_output = consumer
            .process_and_record_transactions_with_policy(
                bank,
                transactions,
                Some(bundle_account_locker),
                false,
            );

        let ProcessTransactionBatchOutput {
            cost_model_throttled_transactions_count,
            cost_model_us,
            execute_and_commit_transactions_output,
        } = process_transaction_batch_output;

        let ExecuteAndCommitTransactionsOutput {
            transaction_counts,
            retryable_transaction_indexes,
            commit_transactions_result,
            execute_and_commit_timings,
            error_counters,
            ..
        } = execute_and_commit_transactions_output;

        let mut total_transaction_counts = CommittedTransactionsCounts::default();
        total_transaction_counts
            .accumulate(&transaction_counts, commit_transactions_result.is_ok());

        let reached_max_poh_height = matches!(
            commit_transactions_result,
            Err(PohRecorderError::MaxHeightReached)
        );

        if reached_max_poh_height {
            info!(
                "process transactions: max height reached slot: {} height: {}",
                bank.slot(),
                bank.tick_height()
            );
        }

        ProcessTransactionsSummary {
            reached_max_poh_height,
            transaction_counts: total_transaction_counts,
            retryable_transaction_indexes: retryable_transaction_indexes
                .into_iter()
                .map(|retryable_index| retryable_index.index)
                .collect(),
            cost_model_throttled_transactions_count,
            cost_model_us,
            execute_and_commit_timings,
            error_counters,
        }
    }

    /// This function filters pending packets that are still valid
    /// # Arguments
    /// * `transactions` - a batch of transactions deserialized from packets
    /// * `pending_indexes` - identifies which indexes in the `transactions` list are still pending
    fn filter_pending_packets_from_pending_txs(
        bank: &Bank,
        transactions: &[impl TransactionWithMeta],
        pending_indexes: &[usize],
    ) -> Vec<usize> {
        let filter =
            Self::prepare_filter_for_pending_transactions(transactions.len(), pending_indexes);

        let results = bank.check_transactions_with_forwarding_delay(
            transactions,
            &filter,
            FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET,
        );

        Self::filter_valid_transaction_indexes(&results)
    }

    /// This function creates a filter of transaction results with Ok() for every pending
    /// transaction. The non-pending transactions are marked with TransactionError
    fn prepare_filter_for_pending_transactions(
        transactions_len: usize,
        pending_tx_indexes: &[usize],
    ) -> Vec<transaction::Result<()>> {
        let mut mask = vec![Err(TransactionError::BlockhashNotFound); transactions_len];
        pending_tx_indexes.iter().for_each(|x| mask[*x] = Ok(()));
        mask
    }

    /// This function returns a vector containing index of all valid transactions. A valid
    /// transaction has result Ok() as the value
    fn filter_valid_transaction_indexes(valid_txs: &[TransactionCheckResult]) -> Vec<usize> {
        valid_txs
            .iter()
            .enumerate()
            .filter_map(|(index, res)| res.as_ref().ok().map(|_| index))
            .collect()
    }
}

fn consume_scan_should_process_packet(
    bank: &Bank,
    packet: SanitizedTransactionView<Bytes>,
    error_counters: &mut TransactionErrorMetrics,
) -> Option<RuntimeTransactionView> {
    // Construct the RuntimeTransaction.
    let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_new(
        packet,
        MessageHash::Compute,
        None,
    ) else {
        return None;
    };

    // Filter invalid votes (should never be triggered).
    if !view.is_simple_vote_transaction() {
        return None;
    }

    // Resolve the transaction (votes do not have LUTs).
    debug_assert!(!matches!(view.version(), TransactionVersion::V0));
    let Ok(view) = RuntimeTransactionView::try_new(view, None, bank.get_reserved_account_keys())
    else {
        return None;
    };

    // Check the number of locks and whether there are duplicates
    if validate_account_locks(
        view.account_keys(),
        bank.get_transaction_account_lock_limit(),
    )
    .is_err()
    {
        return None;
    }

    if Consumer::check_fee_payer_unlocked(bank, &view, error_counters).is_err() {
        return None;
    }

    Some(view)
}

fn has_reached_end_of_slot(reached_max_poh_height: bool, bank: &Bank) -> bool {
    reached_max_poh_height || bank.is_complete()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::{
            committer::Committer,
            tests::{create_slow_genesis_config, sanitize_transactions},
            vote_storage::tests::to_sanitized_view,
        },
        crossbeam_channel::{bounded, never},
        solana_account::WritableAccount,
        solana_clock::MAX_TRANSACTION_FORWARDING_DELAY,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_perf::packet::BytesPacket,
        solana_poh::{
            poh_recorder::{LeaderState, SharedLeaderState},
            record_channels::{RecordReceiver, record_channels},
        },
        solana_signer::Signer,
        solana_svm::account_loader::CheckedTransactionDetails,
        solana_system_transaction as system_transaction,
        solana_transaction::Transaction,
        solana_vote::vote_transaction::new_tower_sync_transaction,
        solana_vote_program::vote_state::TowerSync,
    };

    #[test]
    fn test_bank_prepare_filter_for_pending_transaction() {
        assert_eq!(
            VoteWorker::prepare_filter_for_pending_transactions(6, &[2, 4, 5]),
            vec![
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(())
            ]
        );

        assert_eq!(
            VoteWorker::prepare_filter_for_pending_transactions(6, &[0, 2, 3]),
            vec![
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Ok(()),
                Ok(()),
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
            ]
        );
    }

    #[test]
    fn test_bank_filter_valid_transaction_indexes() {
        assert_eq!(
            VoteWorker::filter_valid_transaction_indexes(&[
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
                Ok(CheckedTransactionDetails::default()),
                Err(TransactionError::BlockhashNotFound),
                Ok(CheckedTransactionDetails::default()),
                Ok(CheckedTransactionDetails::default()),
            ]),
            [2, 4, 5]
        );

        assert_eq!(
            VoteWorker::filter_valid_transaction_indexes(&[
                Ok(CheckedTransactionDetails::default()),
                Err(TransactionError::BlockhashNotFound),
                Err(TransactionError::BlockhashNotFound),
                Ok(CheckedTransactionDetails::default()),
                Ok(CheckedTransactionDetails::default()),
                Ok(CheckedTransactionDetails::default()),
            ]),
            [0, 3, 4, 5]
        );
    }

    #[test]
    fn test_has_reached_end_of_slot() {
        let GenesisConfigInfo { genesis_config, .. } = create_slow_genesis_config(10_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        assert!(!has_reached_end_of_slot(false, &bank));
        assert!(has_reached_end_of_slot(true, &bank));

        bank.fill_bank_with_ticks_for_tests();
        assert!(bank.is_complete());

        assert!(has_reached_end_of_slot(false, &bank));
        assert!(has_reached_end_of_slot(true, &bank));
    }

    #[test]
    fn test_should_bank_still_be_processing_txs() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10_000);
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        // Sanity.
        assert!(!bank.is_complete());

        // Set up Consumer infrastructure to process a transaction
        let (record_sender, mut record_receiver) = record_channels(false);
        let recorder = solana_poh::transaction_recorder::TransactionRecorder::new(record_sender);
        record_receiver.restart(bank.bank_id());

        let (replay_vote_sender, _replay_vote_receiver) = bounded(1024);
        let committer = Committer::new(None, replay_vote_sender, None);
        let consumer = Consumer::new(committer, recorder, None);

        // Create and process a simple transfer transaction
        let pubkey = solana_pubkey::new_rand();
        let transactions = sanitize_transactions(vec![system_transaction::transfer(
            &mint_keypair,
            &pubkey,
            1,
            bank.last_blockhash(),
        )]);

        // Process some transactions on a bank that hasn't finished.
        let summary = VoteWorker::process_transactions(
            &consumer,
            &bank,
            &transactions,
            &BundleAccountLocker::default(),
        );

        // Assert - Transaction were prcoessed.
        assert!(summary.transaction_counts.committed_transactions_count.0 > 0);

        // Assert - We have not yet reached max_poh_height.
        assert!(!summary.reached_max_poh_height);
    }

    /// Exercises the worker contract with legacy-voting banks. Manually replacing
    /// these banks does not establish that this ordering is reachable in production.
    struct VoteRestorationFixture {
        worker: VoteWorker,
        root_bank: Arc<Bank>,
        bank: Arc<Bank>,
        shared_leader_state: SharedLeaderState,
        record_receiver: RecordReceiver,
        mint_keypair: Keypair,
        voting_keypair: Keypair,
        vote_a: Transaction,
        stats: BankingStageStats,
        metrics: LeaderSlotMetricsTracker,
    }

    impl VoteRestorationFixture {
        fn new() -> Self {
            let GenesisConfigInfo {
                genesis_config,
                mint_keypair,
                voting_keypair,
                ..
            } = create_slow_genesis_config(10_000);
            let (root_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
            let bank = Arc::new(Bank::new_from_parent(
                root_bank.clone(),
                SlotLeader::new_unique(),
                1,
            ));
            assert!(!root_bank.is_alpenglow());
            assert!(!bank.is_alpenglow());
            let (record_sender, mut record_receiver) = record_channels(false);
            record_receiver.restart(bank.bank_id());
            let mut shared_leader_state = SharedLeaderState::new(0, None, None);
            shared_leader_state.store(Arc::new(LeaderState::new(
                Some(bank.clone()),
                bank.tick_height(),
                None,
                None,
            )));
            let worker = VoteWorker::new(
                Arc::new(AtomicBool::new(false)),
                CancellationToken::new(),
                DecisionMaker::new(shared_leader_state.clone()),
                VotePacketReceiver::new(never(), Arc::default()),
                VotePacketReceiver::new(never(), Arc::default()),
                VoteStorage::new(&bank),
                bank_forks,
                Consumer::new(
                    Committer::new(None, bounded(1).0, None),
                    solana_poh::transaction_recorder::TransactionRecorder::new(record_sender),
                    None,
                ),
                BundleAccountLocker::default(),
            );
            let mut tower = TowerSync::from(vec![(0, 1)]);
            tower.hash = root_bank.hash();
            let vote_a = new_tower_sync_transaction(
                tower,
                root_bank.last_blockhash(),
                &mint_keypair,
                &voting_keypair,
                &voting_keypair,
                None,
            );
            let mut fixture = Self {
                worker,
                root_bank,
                bank,
                shared_leader_state,
                record_receiver,
                mint_keypair,
                voting_keypair,
                vote_a,
                stats: BankingStageStats::new(),
                metrics: LeaderSlotMetricsTracker::default(),
            };
            fixture.insert(&fixture.vote_a.clone());
            fixture.consume();
            fixture.assert_committed(&fixture.vote_a);
            fixture
        }

        fn insert(&mut self, transaction: &Transaction) {
            transaction.verify().unwrap();
            self.worker.storage.insert_packet(
                VoteSource::Tpu,
                to_sanitized_view(BytesPacket::from_data(transaction).unwrap()),
            );
        }

        fn consume(&mut self) {
            self.worker
                .process_buffered_packets(&mut self.stats, &mut self.metrics);
        }

        fn replace_bank(&mut self) {
            assert_eq!(self.record_receiver.drain().count(), 0);
            self.record_receiver.shutdown();
            self.bank.quiesce_transaction_execution();
            assert!(self.record_receiver.is_safe_to_restart());
            self.root_bank
                .remove_unrooted_slots(&[(self.bank.slot(), self.bank.bank_id())]);
            self.root_bank.clear_slot_signatures(self.bank.slot());
            // Construct only after purging the old bank's accounts and signatures.
            let replacement = Arc::new(Bank::new_from_parent(
                self.root_bank.clone(),
                SlotLeader::new_unique(),
                self.bank.slot(),
            ));
            assert_ne!(replacement.bank_id(), self.bank.bank_id());
            assert!(!replacement.is_alpenglow());
            self.record_receiver.restart(replacement.bank_id());
            self.shared_leader_state.store(Arc::new(LeaderState::new(
                Some(replacement.clone()),
                replacement.tick_height(),
                None,
                None,
            )));
            self.bank = replacement;
            assert_eq!(
                self.bank.get_signature_status(&self.vote_a.signatures[0]),
                None
            );
        }

        fn successor(&self, timestamp: i64, recent_blockhash: Hash) -> Transaction {
            let mut tower = TowerSync::from(vec![(0, 1)]);
            tower.hash = self.root_bank.hash();
            tower.timestamp = Some(timestamp);
            new_tower_sync_transaction(
                tower,
                recent_blockhash,
                &self.mint_keypair,
                &self.voting_keypair,
                &self.voting_keypair,
                None,
            )
        }

        fn assert_committed(&self, transaction: &Transaction) {
            let records: Vec<_> = self.record_receiver.drain().collect();
            assert_eq!(records.len(), 1);
            assert_eq!(records[0].bank_id, self.bank.bank_id());
            assert_eq!(records[0].transactions.len(), 1);
            assert_eq!(
                records[0].transactions[0].signatures,
                transaction.signatures
            );
            assert_eq!(
                self.bank.get_signature_status(&transaction.signatures[0]),
                Some(Ok(())),
            );
            assert_eq!(self.worker.storage.len(), 0);
        }

        fn assert_idle(&mut self) {
            self.consume();
            assert_eq!(self.record_receiver.drain().count(), 0);
            assert_eq!(self.worker.storage.len(), 0);
        }

        /// The caller holds real account locks. Check the raw execution result
        /// separately from the forwarding-age filter used by the worker.
        fn assert_account_conflict(&self, transaction: &Transaction, final_retry: &[usize]) {
            let transaction = RuntimeTransaction::from_transaction_for_tests(transaction.clone());
            let transactions = std::slice::from_ref(&transaction);
            let summary = VoteWorker::process_transactions(
                &self.worker.consumer,
                &self.bank,
                transactions,
                &self.worker.bundle_account_locker,
            );
            assert_eq!(summary.error_counters.account_in_use.0, 1);
            assert_eq!(summary.retryable_transaction_indexes, [0]);
            assert_eq!(summary.transaction_counts.committed_transactions_count.0, 0);
            assert_eq!(
                VoteWorker::filter_pending_packets_from_pending_txs(
                    &self.bank,
                    transactions,
                    &summary.retryable_transaction_indexes,
                ),
                final_retry,
            );
            assert_eq!(self.record_receiver.drain().count(), 0);
        }

        fn assert_blockhash_rejected(&self, transaction: &Transaction) {
            let transaction = RuntimeTransaction::from_transaction_for_tests(transaction.clone());
            let summary = VoteWorker::process_transactions(
                &self.worker.consumer,
                &self.bank,
                std::slice::from_ref(&transaction),
                &self.worker.bundle_account_locker,
            );
            assert_eq!(summary.error_counters.blockhash_not_found.0, 1);
            assert_eq!(summary.error_counters.account_in_use.0, 0);
            assert!(summary.retryable_transaction_indexes.is_empty());
            assert_eq!(summary.transaction_counts.committed_transactions_count.0, 0);
            assert_eq!(self.record_receiver.drain().count(), 0);
        }

        fn rebuffered_count(&self) -> usize {
            self.stats.rebuffered_packets_count.load(Ordering::Relaxed)
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum RetrySuccessor {
        None,
        IncompatibleFork,
        InvalidBlockhash,
        Valid,
    }

    #[test]
    fn test_restored_retry_worker_keeps_fallback() {
        for successor in [
            RetrySuccessor::None,
            RetrySuccessor::IncompatibleFork,
            RetrySuccessor::InvalidBlockhash,
            RetrySuccessor::Valid,
        ] {
            let mut fixture = VoteRestorationFixture::new();
            fixture.replace_bank();
            let bank = fixture.bank.clone();
            let transaction =
                RuntimeTransaction::from_transaction_for_tests(fixture.vote_a.clone());
            {
                let held_accounts =
                    bank.prepare_sanitized_batch(std::slice::from_ref(&transaction));
                assert_eq!(held_accounts.lock_results(), &[Ok(())]);
                fixture.assert_account_conflict(&fixture.vote_a, &[0]);
                let rebuffered_before = fixture.rebuffered_count();
                let consumed_before = fixture
                    .stats
                    .consumed_buffered_packets_count
                    .load(Ordering::Relaxed);
                fixture.consume();
                // One direct restoration and one retry; no terminal outcome.
                assert_eq!(fixture.rebuffered_count() - rebuffered_before, 2);
                assert_eq!(
                    fixture
                        .stats
                        .consumed_buffered_packets_count
                        .load(Ordering::Relaxed),
                    consumed_before
                );
                assert_eq!(fixture.record_receiver.drain().count(), 0);
                assert_eq!(
                    bank.get_signature_status(&fixture.vote_a.signatures[0]),
                    None
                );
                assert_eq!(fixture.worker.storage.len(), 1);
            }

            let vote_b = match successor {
                RetrySuccessor::None => None,
                RetrySuccessor::IncompatibleFork => {
                    // Newer by slot, unlike the same-slot timestamp successors.
                    let mut tower = TowerSync::from(vec![(1, 1)]);
                    tower.hash = Hash::new_unique();
                    Some(new_tower_sync_transaction(
                        tower,
                        bank.last_blockhash(),
                        &fixture.mint_keypair,
                        &fixture.voting_keypair,
                        &fixture.voting_keypair,
                        None,
                    ))
                }
                RetrySuccessor::InvalidBlockhash => Some(fixture.successor(1, Hash::new_unique())),
                RetrySuccessor::Valid => Some(fixture.successor(1, bank.last_blockhash())),
            };
            if let Some(vote_b) = &vote_b {
                fixture.insert(vote_b);
                assert_eq!(fixture.worker.storage.len(), 1);
            }
            let rebuffered_before = fixture.rebuffered_count();
            fixture.consume();
            let succeeded = if matches!(successor, RetrySuccessor::Valid) {
                assert_eq!(
                    bank.get_signature_status(&fixture.vote_a.signatures[0]),
                    None
                );
                vote_b.as_ref().unwrap().clone()
            } else {
                if let Some(vote_b) = &vote_b {
                    assert_eq!(bank.get_signature_status(&vote_b.signatures[0]), None);
                }
                fixture.vote_a.clone()
            };
            fixture.assert_committed(&succeeded);
            assert_eq!(
                fixture.rebuffered_count() - rebuffered_before,
                usize::from(matches!(
                    successor,
                    RetrySuccessor::IncompatibleFork | RetrySuccessor::InvalidBlockhash
                )),
                "successor={successor:?}",
            );
            fixture.assert_idle();

            // Duplicate delivery after successful restoration must not replenish it.
            fixture.insert(&succeeded);
            assert_eq!(fixture.worker.storage.len(), 0);
            fixture.assert_idle();

            if matches!(successor, RetrySuccessor::Valid) {
                let vote_c = fixture.successor(2, Hash::new_unique());
                fixture.insert(&vote_c);
                fixture.assert_idle();
                assert_eq!(
                    bank.get_signature_status(&fixture.vote_a.signatures[0]),
                    None
                );
                // A second replacement proves successful B replaced retained A.
                fixture.replace_bank();
                fixture.consume();
                fixture.assert_committed(&succeeded);
                assert_eq!(
                    fixture
                        .bank
                        .get_signature_status(&fixture.vote_a.signatures[0]),
                    None
                );
                fixture.assert_idle();
            }
        }
    }

    #[test]
    fn test_retained_vote_after_direct_bank_replacement() {
        let mut fixture = VoteRestorationFixture::new();
        let vote_b = fixture.successor(1, Hash::new_unique());
        fixture.insert(&vote_b);
        fixture.replace_bank();
        fixture.consume();
        fixture.assert_committed(&fixture.vote_a);
        assert_eq!(
            fixture.bank.get_signature_status(&vote_b.signatures[0]),
            None
        );
        fixture.assert_idle();
    }

    #[test]
    fn test_restored_retry_worker_deferred_fallback_survives_successor() {
        let mut fixture = VoteRestorationFixture::new();
        // B must be fork-compatible at replacement: otherwise A is directly
        // restored, and the Deferred -> Restored transition is never exercised.
        let vote_b = fixture.successor(1, Hash::new_unique());
        fixture.insert(&vote_b);
        fixture.replace_bank();
        assert_eq!(
            fixture
                .worker
                .storage
                .restore_taken_votes_for_bank(&fixture.bank),
            0
        );
        assert_eq!(fixture.worker.storage.len(), 1);
        let bank = fixture.bank.clone();
        let transaction = RuntimeTransaction::from_transaction_for_tests(fixture.vote_a.clone());
        {
            let held_accounts = bank.prepare_sanitized_batch(std::slice::from_ref(&transaction));
            assert_eq!(held_accounts.lock_results(), &[Ok(())]);
            // Consumer rejects B's blockhash before locking accounts. Deferred A
            // then reaches the held locks and remains retryable after age filtering.
            fixture.assert_blockhash_rejected(&vote_b);
            fixture.assert_account_conflict(&fixture.vote_a, &[0]);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.consume();
            // Exactly one deferred materialization and one retry of that A.
            assert_eq!(fixture.rebuffered_count() - rebuffered_before, 2);
            assert_eq!(
                fixture
                    .stats
                    .dropped_forward_packets_count
                    .load(Ordering::Relaxed),
                0
            );
            assert_eq!(fixture.record_receiver.drain().count(), 0);
            assert_eq!(
                bank.get_signature_status(&fixture.vote_a.signatures[0]),
                None
            );
            assert_eq!(bank.get_signature_status(&vote_b.signatures[0]), None);
            assert_eq!(fixture.worker.storage.len(), 1);
        }
        let vote_c = fixture.successor(2, Hash::new_unique());
        fixture.insert(&vote_c);
        assert_eq!(fixture.worker.storage.len(), 1);
        let rebuffered_before = fixture.rebuffered_count();
        fixture.consume();
        assert_eq!(fixture.rebuffered_count() - rebuffered_before, 1);
        fixture.assert_committed(&fixture.vote_a);
        assert_eq!(bank.get_signature_status(&vote_c.signatures[0]), None);
        fixture.assert_idle();
    }

    #[test]
    fn test_restored_retry_worker_terminal_execution_failure() {
        for deferred in [false, true] {
            let mut fixture = VoteRestorationFixture::new();
            if deferred {
                let vote_b = fixture.successor(1, Hash::new_unique());
                fixture.insert(&vote_b);
            }
            fixture.replace_bank();
            for _ in 0..=fixture.bank.max_processing_age() {
                fixture.bank.register_unique_recent_blockhash_for_test();
            }
            fixture.assert_blockhash_rejected(&fixture.vote_a);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.consume();
            // Direct or deferred A is materialized once, then fails terminally.
            assert_eq!(fixture.rebuffered_count() - rebuffered_before, 1);
            fixture.assert_idle();
            let vote_c = fixture.successor(2, Hash::new_unique());
            fixture.insert(&vote_c);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.assert_idle();
            assert_eq!(fixture.rebuffered_count(), rebuffered_before);
            assert_eq!(
                fixture
                    .bank
                    .get_signature_status(&fixture.vote_a.signatures[0]),
                None
            );
            assert!(
                fixture
                    .worker
                    .storage
                    .take_deferred_retained_vote(fixture.voting_keypair.pubkey())
                    .is_none()
            );
        }
    }

    #[test]
    fn test_restored_retry_worker_terminal_pre_execution_failure() {
        for deferred in [false, true] {
            let mut fixture = VoteRestorationFixture::new();
            if deferred {
                let vote_b = fixture.successor(1, Hash::new_unique());
                fixture.insert(&vote_b);
            }
            fixture.replace_bank();
            let payer_pubkey = fixture.mint_keypair.pubkey();
            let payer_account = fixture.bank.get_account(&payer_pubkey).unwrap();
            let mut invalid_payer = payer_account.clone();
            invalid_payer.set_owner(solana_pubkey::new_rand());
            fixture.bank.store_account(&payer_pubkey, &invalid_payer);
            let mut errors = TransactionErrorMetrics::default();
            assert!(
                consume_scan_should_process_packet(
                    &fixture.bank,
                    to_sanitized_view(BytesPacket::from_data(&fixture.vote_a).unwrap()),
                    &mut errors,
                )
                .is_none()
            );
            assert_eq!(errors.invalid_account_for_fee.0, 1);
            let consumed_before = fixture
                .stats
                .consumed_buffered_packets_count
                .load(Ordering::Relaxed);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.consume();
            assert_eq!(fixture.rebuffered_count() - rebuffered_before, 1);
            // Neither candidate reaches the execution call after fee-payer rejection.
            assert_eq!(
                fixture
                    .stats
                    .consumed_buffered_packets_count
                    .load(Ordering::Relaxed),
                consumed_before
            );
            fixture.assert_idle();
            fixture.bank.store_account(&payer_pubkey, &payer_account);
            // A would now execute successfully if a later B could re-arm it.
            let vote_c = fixture.successor(2, Hash::new_unique());
            fixture.insert(&vote_c);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.assert_idle();
            assert_eq!(fixture.rebuffered_count(), rebuffered_before);
            assert_eq!(
                fixture
                    .bank
                    .get_signature_status(&fixture.vote_a.signatures[0]),
                None
            );
        }
    }

    #[test]
    fn test_restored_retry_worker_expired_retry_is_terminal() {
        let mut fixture = VoteRestorationFixture::new();
        fixture.replace_bank();
        let bank = fixture.bank.clone();
        let retry_max_age = bank
            .max_processing_age()
            .saturating_sub(MAX_TRANSACTION_FORWARDING_DELAY)
            .saturating_sub(FORWARD_TRANSACTIONS_TO_LEADER_AT_SLOT_OFFSET as usize);
        for _ in 0..=retry_max_age {
            bank.register_unique_recent_blockhash_for_test();
        }
        let transaction = RuntimeTransaction::from_transaction_for_tests(fixture.vote_a.clone());
        assert!(
            bank.check_transactions(
                std::slice::from_ref(&transaction),
                &[Ok(())],
                bank.max_processing_age(),
                false,
                &mut TransactionErrorMetrics::default(),
            )[0]
            .is_ok()
        );
        {
            let held_accounts = bank.prepare_sanitized_batch(std::slice::from_ref(&transaction));
            assert_eq!(held_accounts.lock_results(), &[Ok(())]);
            fixture.assert_account_conflict(&fixture.vote_a, &[]);
            let rebuffered_before = fixture.rebuffered_count();
            fixture.consume();
            // The raw retry is dropped, leaving only the direct restoration count.
            assert_eq!(fixture.rebuffered_count() - rebuffered_before, 1);
            assert_eq!(
                fixture
                    .stats
                    .dropped_forward_packets_count
                    .load(Ordering::Relaxed),
                1
            );
            assert_eq!(fixture.worker.storage.len(), 0);
            assert_eq!(fixture.record_receiver.drain().count(), 0);
        }
        // A is still executable after unlocking, so recovery would expose a re-arm.
        let vote_b = fixture.successor(1, Hash::new_unique());
        fixture.insert(&vote_b);
        let rebuffered_before = fixture.rebuffered_count();
        fixture.assert_idle();
        assert_eq!(fixture.rebuffered_count(), rebuffered_before);
        assert_eq!(
            bank.get_signature_status(&fixture.vote_a.signatures[0]),
            None
        );
    }
}
