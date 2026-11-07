use super::*;

impl Bank {
    /// Simulates transactions against a potentially unfrozen bank with pre-execution accounts
    pub fn simulate_transactions_unchecked_with_pre_accounts<Tx: TransactionWithMeta>(
        &self,
        transactions: &[Tx],
        pre_accounts: &Vec<Vec<Pubkey>>,
        post_accounts: &Vec<Vec<Pubkey>>,
        log_messages_bytes_limit: Option<usize>,
    ) -> Vec<(
        Vec<KeyedAccountSharedData>, /* pre-accounts */
        TransactionSimulationResult, /* post-simulation result, which also contains the accounts */
        Vec<KeyedAccountSharedData>, /* post-accounts; results are stored in the simulation result, but there's no requirement for the tx being present*/
    )> {
        let mut simulation_results = Vec::with_capacity(transactions.len());

        let mut account_overrides = AccountOverrides::default();
        let mut program_cache_for_tx_batch = ProgramCacheForTxBatch::new(self.slot);

        // Pre-load all the account state into account overrides
        for transaction in transactions {
            let account_keys = transaction.account_keys();
            account_overrides.merge(self.get_account_overrides_for_simulation(&account_keys));
            for account in account_keys.iter() {
                if !account_overrides.accounts().contains_key(account)
                    && let Some(account_shared_data) = self.get_account_shared_data(account)
                {
                    account_overrides.set_account(account, Some(account_shared_data));
                }
            }
        }

        // execute each transaction (this could be faster, but the dumb pre-execution accounts logic makes it difficult)
        for (transaction, pre_accounts, post_accounts) in
            izip!(transactions, pre_accounts, post_accounts)
        {
            let mut accounts_pre_loaded = Vec::with_capacity(pre_accounts.len());

            // fill out the pre-accounts from the account overrides or bank
            // shouldn't need to hit the bank unless pre_account isn't in transaction keys
            for pubkey in pre_accounts {
                if let Some(account) = account_overrides.get(pubkey) {
                    accounts_pre_loaded.push((*pubkey, account.clone()));
                } else if let Some(account_shared_data) = self.get_account_shared_data(pubkey) {
                    accounts_pre_loaded.push((*pubkey, account_shared_data));
                } else {
                    accounts_pre_loaded.push((*pubkey, AccountSharedData::default()));
                }
            }

            let number_of_accounts = transaction.account_keys().len();

            let batch = self.prepare_unlocked_batch_from_single_tx(transaction);

            let LoadAndExecuteTransactionsOutput {
                processing_results,
                balance_collector,
                ..
            } = self.load_and_execute_transactions_with_program_cache(
                &batch,
                MAX_PROCESSING_AGE - MAX_TRANSACTION_FORWARDING_DELAY,
                &mut ExecuteTimings::default(),
                &mut TransactionErrorMetrics::default(),
                TransactionProcessingConfig {
                    account_overrides: Some(&account_overrides),
                    log_messages_bytes_limit,
                    limit_to_load_programs: true,
                    recording_config: ExecutionRecordingConfig {
                        enable_cpi_recording: false,
                        enable_log_recording: true,
                        enable_return_data_recording: true,
                        enable_transaction_balance_recording: true,
                    },
                    drop_on_failure: true,
                    all_or_nothing: true,
                    strict_nonce_size_check: true,
                    drop_noop_transactions: true,
                    drop_bail_out_transactions: true,
                },
                &mut program_cache_for_tx_batch,
            );

            // Later transactions read these overrides, so write them before the result is consumed.
            match processing_results.last() {
                Some(Ok(ProcessedTransaction::Executed(executed_tx))) => {
                    for (pubkey, account) in executed_tx.loaded_transaction.accounts.iter() {
                        account_overrides.set_account(pubkey, Some(account.clone()));
                    }
                }
                Some(Ok(ProcessedTransaction::FeesOnly(fees_only_tx))) => {
                    for (pubkey, account) in fees_only_tx.rollback_accounts.iter() {
                        account_overrides.set_account(pubkey, Some(account.clone()));
                    }
                }
                Some(Ok(ProcessedTransaction::NoOp(_)) | Err(_)) | None => {}
            }
            let simulation_result =
                Self::simulation_result(processing_results, balance_collector, number_of_accounts);

            let mut accounts_post_loaded = Vec::with_capacity(post_accounts.len());
            for pubkey in post_accounts {
                if let Some(account) = account_overrides.get(pubkey) {
                    accounts_post_loaded.push((*pubkey, account.clone()));
                } else if let Some(account_shared_data) = self.get_account_shared_data(pubkey) {
                    accounts_post_loaded.push((*pubkey, account_shared_data));
                } else {
                    accounts_post_loaded.push((*pubkey, AccountSharedData::default()));
                }
            }

            let is_execution_result_err = simulation_result.result.is_err();
            simulation_results.push((accounts_pre_loaded, simulation_result, accounts_post_loaded));

            // bail out early if the execution result is an error
            if is_execution_result_err {
                break;
            }
        }

        simulation_results
    }
}
