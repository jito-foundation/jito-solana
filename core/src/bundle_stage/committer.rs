use {
    crate::banking_stage::{
        committer::CommitTransactionDetails,
        leader_slot_timing_metrics::LeaderExecuteAndCommitTimings,
    },
    solana_accounts_db::transaction_results::TransactionResults,
    solana_bundle::bundle_execution::LoadAndExecuteBundleOutput,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure_us,
    solana_runtime::{
        bank::{Bank, CommitTransactionCounts, TransactionBalances, TransactionBalancesSet},
        bank_utils,
        prioritization_fee_cache::PrioritizationFeeCache,
    },
    solana_sdk::{hash::Hash, saturating_add_assign, transaction::SanitizedTransaction},
    solana_transaction_status::{
        token_balances::{TransactionTokenBalances, TransactionTokenBalancesSet},
        PreBalanceInfo,
    },
    solana_vote::vote_sender_types::ReplayVoteSender,
    std::sync::Arc,
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
        last_blockhash: Hash,
        lamports_per_signature: u64,
        mut starting_transaction_index: Option<usize>,
        bank: &Arc<Bank>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
    ) -> (u64, CommitBundleDetails) {
        let transaction_output = bundle_execution_output.bundle_transaction_results_mut();

        let (commit_transaction_details, commit_times): (Vec<_>, Vec<_>) = transaction_output
            .iter_mut()
            .map(|bundle_results| {
                let committed_transactions_count = bundle_results
                    .load_and_execute_transactions_output()
                    .executed_transactions_count
                    as u64;

                let committed_non_vote_transactions_count = bundle_results
                    .load_and_execute_transactions_output()
                    .executed_non_vote_transactions_count
                    as u64;

                let committed_with_failure_result_count = bundle_results
                    .load_and_execute_transactions_output()
                    .executed_transactions_count
                    .saturating_sub(
                        bundle_results
                            .load_and_execute_transactions_output()
                            .executed_with_successful_result_count,
                    ) as u64;

                let signature_count = bundle_results
                    .load_and_execute_transactions_output()
                    .signature_count;

                let sanitized_transactions = bundle_results.transactions().to_vec();
                let execution_results = bundle_results.execution_results().to_vec();

                let loaded_transactions = bundle_results.loaded_transactions_mut();
                debug!("loaded_transactions: {:?}", loaded_transactions);

                let (tx_results, commit_time_us) = measure_us!(bank.commit_transactions(
                    &sanitized_transactions,
                    loaded_transactions,
                    execution_results,
                    last_blockhash,
                    lamports_per_signature,
                    CommitTransactionCounts {
                        committed_transactions_count,
                        committed_non_vote_transactions_count,
                        committed_with_failure_result_count,
                        signature_count,
                    },
                    &mut execute_and_commit_timings.execute_timings,
                ));

                let commit_transaction_statuses: Vec<_> = tx_results
                    .execution_results
                    .iter()
                    .map(|execution_result| match execution_result.details() {
                        Some(details) => CommitTransactionDetails::Committed {
                            compute_units: details.executed_units,
                        },
                        None => CommitTransactionDetails::NotCommitted,
                    })
                    .collect();

                let ((), find_and_send_votes_us) = measure_us!({
                    bank_utils::find_and_send_votes(
                        &sanitized_transactions,
                        &tx_results,
                        Some(&self.replay_vote_sender),
                    );

                    let post_balance_info = bundle_results.post_balance_info().clone();
                    let pre_balance_info = bundle_results.pre_balance_info();

                    let num_committed = tx_results
                        .execution_results
                        .iter()
                        .filter(|r| r.was_executed())
                        .count();

                    self.collect_balances_and_send_status_batch(
                        tx_results,
                        bank,
                        sanitized_transactions,
                        pre_balance_info,
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
        tx_results: TransactionResults,
        bank: &Arc<Bank>,
        sanitized_transactions: Vec<SanitizedTransaction>,
        pre_balance_info: &mut PreBalanceInfo,
        (post_balances, post_token_balances): (TransactionBalances, TransactionTokenBalances),
        starting_transaction_index: Option<usize>,
    ) {
        if let Some(transaction_status_sender) = &self.transaction_status_sender {
            let mut transaction_index = starting_transaction_index.unwrap_or_default();
            let batch_transaction_indexes: Vec<_> = tx_results
                .execution_results
                .iter()
                .map(|result| {
                    if result.was_executed() {
                        let this_transaction_index = transaction_index;
                        saturating_add_assign!(transaction_index, 1);
                        this_transaction_index
                    } else {
                        0
                    }
                })
                .collect();
            transaction_status_sender.send_transaction_status_batch(
                bank.clone(),
                sanitized_transactions,
                tx_results.execution_results,
                TransactionBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.native),
                    post_balances,
                ),
                TransactionTokenBalancesSet::new(
                    std::mem::take(&mut pre_balance_info.token),
                    post_token_balances,
                ),
                tx_results.rent_debits,
                batch_transaction_indexes,
            );
        }
    }
}
