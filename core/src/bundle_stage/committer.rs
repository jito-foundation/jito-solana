use {
    crate::banking_stage::{
        committer::CommitTransactionDetails,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    solana_bundle::bundle_execution::LoadAndExecuteBundleOutput,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure_us,
    solana_runtime::{
        bank::{Bank, TransactionBalances, TransactionBalancesSet},
        bank_utils,
        prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_sdk::{saturating_add_assign, transaction::SanitizedTransaction},
    solana_svm::transaction_commit_result::TransactionCommitResult,
    solana_transaction_status::{
        token_balances::{TransactionTokenBalances, TransactionTokenBalancesSet},
        PreBalanceInfo,
    },
    std::{ops::Deref, sync::Arc},
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
    pub(crate) fn commit_bundle<'a>(
        &self,
        bundle_execution_output: &'a mut LoadAndExecuteBundleOutput<'a>,
        mut starting_transaction_index: Option<usize>,
        bank: &Arc<Bank>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
    ) -> (u64, CommitBundleDetails) {
        let transaction_output = bundle_execution_output.bundle_transaction_results_mut();

        let (commit_transaction_details, commit_times): (Vec<_>, Vec<_>) = transaction_output
            .iter_mut()
            .map(|bundle_results| {
                let execution_results = bundle_results.execution_results().to_vec();

                let (commit_results, commit_time_us) = measure_us!(bank.commit_transactions(
                    bundle_results.transactions(),
                    execution_results,
                    &bundle_results
                        .load_and_execute_transactions_output()
                        .processed_counts,
                    &mut execute_and_commit_timings.execute_timings,
                ));
                saturating_add_assign!(execute_and_commit_timings.commit_us, commit_time_us);

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
                    .filter(|status| matches!(status, CommitTransactionDetails::Committed { .. }))
                    .count();

                let ((), find_and_send_votes_us) = measure_us!({
                    bank_utils::find_and_send_votes(
                        bundle_results.transactions(),
                        &commit_results,
                        Some(&self.replay_vote_sender),
                    );

                    let post_balance_info = bundle_results.post_balance_info().clone();

                    self.collect_balances_and_send_status_batch(
                        commit_results,
                        bank,
                        bundle_results.transactions(),
                        &mut bundle_results.pre_balance_info().clone(),
                        post_balance_info,
                        starting_transaction_index,
                    );

                    // NOTE: we're doing batched records, so we need to increment the poh starting_transaction_index
                    // by number committed so the next batch will have the correct starting_transaction_index
                    starting_transaction_index =
                        starting_transaction_index.map(|starting_transaction_index| {
                            starting_transaction_index.saturating_add(num_committed)
                        });

                    self.prioritization_fee_cache
                        .update(bank, bundle_results.executed_transactions().into_iter());
                });
                saturating_add_assign!(
                    execute_and_commit_timings.find_and_send_votes_us,
                    find_and_send_votes_us
                );

                (commit_transaction_statuses, commit_time_us)
            })
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
        pre_balance_info: &mut PreBalanceInfo,
        (post_balances, post_token_balances): (TransactionBalances, TransactionTokenBalances),
        starting_transaction_index: Option<usize>,
    ) {
        if let Some(transaction_status_sender) = &self.transaction_status_sender {
            let mut transaction_index = starting_transaction_index.unwrap_or_default();
            let batch_transaction_indexes: Vec<_> = commit_results
                .iter()
                .map(|result| {
                    if result.is_ok() {
                        let this_transaction_index = transaction_index;
                        saturating_add_assign!(transaction_index, 1);
                        this_transaction_index
                    } else {
                        0
                    }
                })
                .collect();
            transaction_status_sender.send_transaction_status_batch(
                bank.slot(),
                sanitized_transactions
                    .iter()
                    .map(|s| s.deref().clone())
                    .collect(),
                commit_results,
                TransactionBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.native),
                    post_balances,
                ),
                TransactionTokenBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.token),
                    post_token_balances,
                ),
                batch_transaction_indexes,
            );
        }
    }
}
