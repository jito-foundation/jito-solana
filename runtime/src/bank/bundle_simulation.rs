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
        if transactions.is_empty() {
            return vec![];
        }
        let mut simulation_results = Vec::with_capacity(transactions.len());

        let mut account_overrides = AccountOverrides::default();
        let mut program_cache_for_tx_batch = ProgramCacheForTxBatch::new(self.slot);

        // Pre-load all the account state into account overrides
        for transaction in transactions {
            let account_keys = transaction.account_keys();
            account_overrides.merge(self.get_account_overrides_for_simulation(&account_keys));
            for account in account_keys.iter() {
                if !account_overrides.accounts().contains_key(account)
                    && let Some((account_shared_data, _slot)) =
                        self.get_account_shared_data(account)
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
                } else if let Some((account_shared_data, _slot)) =
                    self.get_account_shared_data(pubkey)
                {
                    accounts_pre_loaded.push((*pubkey, account_shared_data));
                } else {
                    accounts_pre_loaded.push((*pubkey, AccountSharedData::default()));
                }
            }

            let number_of_accounts = transaction.account_keys().len();

            let batch = self.prepare_unlocked_batch_from_single_tx(transaction);
            program_cache_for_tx_batch.hit_max_limit = false;
            program_cache_for_tx_batch.loaded_missing = false;
            program_cache_for_tx_batch.merged_modified = false;

            let LoadAndExecuteTransactionsOutput {
                mut processing_results,
                balance_collector,
                ..
            } = self.load_and_execute_transactions_with_program_cache(
                &batch,
                MAX_PROCESSING_AGE - MAX_TRANSACTION_FORWARDING_DELAY,
                &mut ExecuteTimings::default(),
                &mut TransactionErrorMetrics::default(),
                TransactionProcessingConfig {
                    account_overrides: Some(&account_overrides),
                    check_program_deployment_slot: self.check_program_deployment_slot,
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
                },
                &mut program_cache_for_tx_batch,
                false,
            );

            let processing_result = processing_results
                .pop()
                .unwrap_or(Err(TransactionError::InvalidProgramForExecution));
            let (
                post_simulation_accounts,
                result,
                fee,
                logs,
                return_data,
                inner_instructions,
                units_consumed,
                loaded_accounts_data_size,
            ) = match processing_result {
                Ok(processed_tx) => {
                    let executed_units = processed_tx.executed_units();
                    let loaded_accounts_data_size = processed_tx.loaded_accounts_data_size();

                    match processed_tx {
                        ProcessedTransaction::Executed(executed_tx) => {
                            // write accounts into the account overrides
                            for (pubkey, account) in executed_tx.loaded_transaction.accounts.iter()
                            {
                                account_overrides.set_account(pubkey, Some(account.clone()));
                            }

                            let details = executed_tx.execution_details;
                            let post_simulation_accounts = executed_tx
                                .loaded_transaction
                                .accounts
                                .into_iter()
                                .take(number_of_accounts)
                                .collect::<Vec<_>>();
                            (
                                post_simulation_accounts,
                                details.status,
                                Some(executed_tx.loaded_transaction.fee_details.total_fee()),
                                details.log_messages,
                                details.return_data,
                                details.inner_instructions,
                                executed_units,
                                loaded_accounts_data_size,
                            )
                        }
                        ProcessedTransaction::FeesOnly(fees_only_tx) => {
                            // write accounts into the account overrides
                            match fees_only_tx.rollback_accounts {
                                RollbackAccounts::FeePayerOnly { fee_payer } => {
                                    account_overrides
                                        .set_account(&fee_payer.0, Some(fee_payer.1.clone()));
                                }
                                RollbackAccounts::SameNonceAndFeePayer { nonce } => {
                                    account_overrides.set_account(&nonce.0, Some(nonce.1.clone()));
                                }
                                RollbackAccounts::SeparateNonceAndFeePayer { nonce, fee_payer } => {
                                    account_overrides.set_account(&nonce.0, Some(nonce.1.clone()));
                                    account_overrides
                                        .set_account(&fee_payer.0, Some(fee_payer.1.clone()));
                                }
                            }

                            (
                                vec![],
                                Err(fees_only_tx.load_error),
                                Some(fees_only_tx.fee_details.total_fee()),
                                None,
                                None,
                                None,
                                executed_units,
                                loaded_accounts_data_size,
                            )
                        }
                        ProcessedTransaction::NoOp(no_op_tx) => (
                            vec![],
                            Err(no_op_tx.validation_error),
                            None,
                            None,
                            None,
                            None,
                            executed_units,
                            loaded_accounts_data_size,
                        ),
                    }
                }
                Err(error) => (vec![], Err(error), None, None, None, None, 0, 0),
            };
            let logs = logs.unwrap_or_default();

            let (pre_balances, post_balances, pre_token_balances, post_token_balances) =
                match balance_collector {
                    Some(balance_collector) => {
                        let (mut native_pre, mut native_post, mut token_pre, mut token_post) =
                            balance_collector.into_vecs();

                        (
                            native_pre.pop(),
                            native_post.pop(),
                            token_pre.pop(),
                            token_post.pop(),
                        )
                    }
                    None => (None, None, None, None),
                };

            let mut accounts_post_loaded = Vec::with_capacity(post_accounts.len());
            for pubkey in post_accounts {
                if let Some(account) = account_overrides.get(pubkey) {
                    accounts_post_loaded.push((*pubkey, account.clone()));
                } else if let Some((account_shared_data, _slot)) =
                    self.get_account_shared_data(pubkey)
                {
                    accounts_post_loaded.push((*pubkey, account_shared_data));
                } else {
                    accounts_post_loaded.push((*pubkey, AccountSharedData::default()));
                }
            }

            let is_execution_result_err = result.is_err();
            simulation_results.push((
                accounts_pre_loaded,
                TransactionSimulationResult {
                    result,
                    logs,
                    post_simulation_accounts,
                    units_consumed,
                    loaded_accounts_data_size,
                    return_data,
                    inner_instructions,
                    fee,
                    pre_balances,
                    post_balances,
                    pre_token_balances,
                    post_token_balances,
                },
                accounts_post_loaded,
            ));

            // bail out early if the execution result is an error
            if is_execution_result_err {
                break;
            }
        }

        simulation_results
    }
}
