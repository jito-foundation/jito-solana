use {
    crate::banking_stage::{
        committer::CommitTransactionDetails,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    itertools::Itertools,
    solana_bundle::bundle_execution::{BundleTransactionsOutput, LoadAndExecuteBundleOutput},
    solana_cost_model::cost_model::CostModel,
    solana_ledger::{
        blockstore_processor::TransactionStatusSender,
        transaction_balances::compile_collected_balances,
    },
    solana_measure::measure_us,
    solana_runtime::{
        bank::Bank, bank_utils, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_with_meta::TransactionWithMeta,
    },
    solana_svm::{
        transaction_balances::BalanceCollector, transaction_commit_result::TransactionCommitResult,
    },
    solana_transaction::sanitized::SanitizedTransaction,
    std::{num::Saturating, ops::AddAssign, sync::Arc},
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitBundleDetails {
    pub commit_transaction_details: Vec<Vec<CommitTransactionDetails>>,
}

pub struct Committer {
    transaction_status_sender: Option<TransactionStatusSender>,
    replay_vote_sender: ReplayVoteSender,
    prioritization_fee_cache: Arc<PrioritizationFeeCache>,
}

impl Committer {
    pub fn new(
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        prioritization_fee_cache: Arc<PrioritizationFeeCache>,
    ) -> Self {
        Self {
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache,
        }
    }

    pub(crate) fn transaction_status_sender_enabled(&self) -> bool {
        self.transaction_status_sender.is_some()
    }

    /// Very similar to Committer::commit_transactions, but works with bundles.
    /// The main difference is there's multiple non-parallelizable transaction vectors to commit
    /// and post-balances are collected after execution instead of from the bank in Self::collect_balances_and_send_status_batch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn commit_bundle(
        &self,
        bundle_execution_output: LoadAndExecuteBundleOutput,
        mut starting_transaction_index: Option<usize>,
        bank: &Arc<Bank>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
    ) -> (u64, CommitBundleDetails) {
        let (commit_transaction_details, commit_times): (Vec<_>, Vec<_>) = bundle_execution_output
            .bundle_transaction_results
            .into_iter()
            .map(
                |BundleTransactionsOutput {
                     transactions,
                     load_and_execute_transactions_output,
                     ..
                 }| {
                    let (commit_results, commit_time_us) = measure_us!(bank.commit_transactions(
                        transactions,
                        load_and_execute_transactions_output.processing_results,
                        &load_and_execute_transactions_output.processed_counts,
                        &mut execute_and_commit_timings.execute_timings,
                    ));
                    execute_and_commit_timings
                        .commit_us
                        .add_assign(&commit_time_us);

                    let commit_transaction_statuses: Vec<CommitTransactionDetails> = commit_results
                        .iter()
                        .map(|commit_result| match commit_result {
                            // reports actual execution CUs, and actual loaded accounts size for
                            // transaction committed to block. qos_service uses these information to adjust
                            // reserved block space.
                            Ok(committed_tx) => CommitTransactionDetails::Committed {
                                compute_units: committed_tx.executed_units,
                                loaded_accounts_data_size: committed_tx
                                    .loaded_account_stats
                                    .loaded_accounts_data_size,
                            },
                            Err(_) => CommitTransactionDetails::NotCommitted,
                        })
                        .collect();
                    let num_committed = commit_transaction_statuses
                        .iter()
                        .filter(|status| {
                            matches!(status, CommitTransactionDetails::Committed { .. })
                        })
                        .count();

                    let ((), find_and_send_votes_us) = measure_us!({
                        bank_utils::find_and_send_votes(
                            transactions,
                            &commit_results,
                            Some(&self.replay_vote_sender),
                        );

                        self.prioritization_fee_cache
                            .update(bank, transactions.iter());

                        self.collect_balances_and_send_status_batch(
                            commit_results,
                            bank,
                            transactions,
                            load_and_execute_transactions_output.balance_collector,
                            starting_transaction_index,
                        );

                        // NOTE: we're doing batched records, so we need to increment the poh starting_transaction_index
                        // by number committed so the next batch will have the correct starting_transaction_index
                        starting_transaction_index =
                            starting_transaction_index.map(|starting_transaction_index| {
                                starting_transaction_index.saturating_add(num_committed)
                            });
                    });
                    execute_and_commit_timings
                        .find_and_send_votes_us
                        .add_assign(find_and_send_votes_us);

                    (commit_transaction_statuses, commit_time_us)
                },
            )
            .unzip();

        (
            commit_times.iter().sum(),
            CommitBundleDetails {
                commit_transaction_details,
            },
        )
    }

    fn collect_balances_and_send_status_batch(
        &self,
        commit_results: Vec<TransactionCommitResult>,
        bank: &Arc<Bank>,
        sanitized_transactions: &[RuntimeTransaction<SanitizedTransaction>],
        balance_collector: Option<BalanceCollector>,
        starting_transaction_index: Option<usize>,
    ) {
        if let Some(transaction_status_sender) = &self.transaction_status_sender {
            // Clone `SanitizedTransaction` out of `RuntimeTransaction`, this is
            // done to send over the status sender.
            let txs = sanitized_transactions
                .iter()
                .map(|tx| tx.as_sanitized_transaction().into_owned())
                .collect_vec();

            let mut transaction_index = Saturating(starting_transaction_index.unwrap_or_default());
            let (batch_transaction_indexes, tx_costs): (Vec<_>, Vec<_>) = commit_results
                .iter()
                .zip(sanitized_transactions.iter())
                .map(|(commit_result, tx)| {
                    if let Ok(committed_tx) = commit_result {
                        let Saturating(this_transaction_index) = transaction_index;
                        transaction_index += 1;

                        let tx_cost = Some(
                            CostModel::calculate_cost_for_executed_transaction(
                                tx,
                                committed_tx.executed_units,
                                committed_tx.loaded_account_stats.loaded_accounts_data_size,
                                &bank.feature_set,
                            )
                            .sum(),
                        );

                        (this_transaction_index, tx_cost)
                    } else {
                        (0, Some(0))
                    }
                })
                .unzip();

            // There are two cases where balance_collector could be None:
            // * Balance recording is disabled. If that were the case, there would
            //   be no TransactionStatusSender, and we would not be in this branch.
            // * The batch was aborted in its entirety in SVM. In that case, there
            //   would be zero processed transactions, and commit_transactions()
            //   would not have been called at all.
            // Therefore this should always be true.
            debug_assert!(balance_collector.is_some());

            let (balances, token_balances) =
                compile_collected_balances(balance_collector.unwrap_or_default());

            transaction_status_sender.send_transaction_status_batch(
                bank.slot(),
                txs,
                commit_results,
                balances,
                token_balances,
                tx_costs,
                batch_transaction_indexes,
            );
        }
    }
}
