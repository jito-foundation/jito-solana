use {
    itertools::izip,
    log::*,
    solana_accounts_db::{
        account_overrides::AccountOverrides, accounts::TransactionLoadResult,
        transaction_results::TransactionExecutionResult,
    },
    solana_ledger::token_balances::collect_token_balances,
    solana_measure::{measure::Measure, measure_us},
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        bank::{Bank, LoadAndExecuteTransactionsOutput, TransactionBalances},
        transaction_batch::TransactionBatch,
    },
    solana_sdk::{
        account::AccountSharedData,
        bundle::SanitizedBundle,
        pubkey::Pubkey,
        saturating_add_assign,
        signature::Signature,
        transaction::{SanitizedTransaction, TransactionError, VersionedTransaction},
    },
    solana_transaction_status::{token_balances::TransactionTokenBalances, PreBalanceInfo},
    std::{
        cmp::{max, min},
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Clone, Default)]
pub struct BundleExecutionMetrics {
    pub num_retries: u64,
    pub collect_balances_us: u64,
    pub load_execute_us: u64,
    pub collect_pre_post_accounts_us: u64,
    pub cache_accounts_us: u64,
    pub execute_timings: ExecuteTimings,
}

/// Contains the results from executing each TransactionBatch with a final result associated with it
/// Note that if !result.is_ok(), bundle_transaction_results will not contain the output for every transaction.
pub struct LoadAndExecuteBundleOutput<'a> {
    bundle_transaction_results: Vec<BundleTransactionsOutput<'a>>,
    result: LoadAndExecuteBundleResult<()>,
    metrics: BundleExecutionMetrics,
}

impl<'a> LoadAndExecuteBundleOutput<'a> {
    pub fn executed_ok(&self) -> bool {
        self.result.is_ok()
    }

    pub fn result(&self) -> &LoadAndExecuteBundleResult<()> {
        &self.result
    }

    pub fn bundle_transaction_results_mut(&mut self) -> &'a mut [BundleTransactionsOutput] {
        &mut self.bundle_transaction_results
    }

    pub fn bundle_transaction_results(&self) -> &'a [BundleTransactionsOutput] {
        &self.bundle_transaction_results
    }

    pub fn executed_transaction_batches(&self) -> Vec<Vec<VersionedTransaction>> {
        self.bundle_transaction_results
            .iter()
            .map(|br| br.executed_versioned_transactions())
            .collect()
    }

    pub fn metrics(&self) -> BundleExecutionMetrics {
        self.metrics.clone()
    }
}

#[derive(Clone, Debug, Error)]
pub enum LoadAndExecuteBundleError {
    #[error("Bundle execution timed out")]
    ProcessingTimeExceeded(Duration),

    #[error(
        "A transaction in the bundle encountered a lock error: [signature={:?}, transaction_error={:?}]",
        signature,
        transaction_error
    )]
    LockError {
        signature: Signature,
        transaction_error: TransactionError,
    },

    #[error(
        "A transaction in the bundle failed to execute: [signature={:?}, execution_result={:?}",
        signature,
        execution_result
    )]
    TransactionError {
        signature: Signature,
        // Box reduces the size between variants in the Error
        execution_result: Box<TransactionExecutionResult>,
    },

    #[error("Invalid pre or post accounts")]
    InvalidPreOrPostAccounts,
}

pub struct BundleTransactionsOutput<'a> {
    transactions: &'a [SanitizedTransaction],
    load_and_execute_transactions_output: LoadAndExecuteTransactionsOutput,
    pre_balance_info: PreBalanceInfo,
    post_balance_info: (TransactionBalances, TransactionTokenBalances),
    // the length of the outer vector should be the same as transactions.len()
    // for indices that didn't get executed, expect a None.
    pre_tx_execution_accounts: Vec<Option<Vec<(Pubkey, AccountSharedData)>>>,
    post_tx_execution_accounts: Vec<Option<Vec<(Pubkey, AccountSharedData)>>>,
}

impl<'a> BundleTransactionsOutput<'a> {
    pub fn executed_versioned_transactions(&self) -> Vec<VersionedTransaction> {
        self.transactions
            .iter()
            .zip(
                self.load_and_execute_transactions_output
                    .execution_results
                    .iter(),
            )
            .filter_map(|(tx, exec_result)| {
                exec_result
                    .was_executed()
                    .then_some(tx.to_versioned_transaction())
            })
            .collect()
    }

    pub fn executed_transactions(&self) -> Vec<&'a SanitizedTransaction> {
        self.transactions
            .iter()
            .zip(
                self.load_and_execute_transactions_output
                    .execution_results
                    .iter(),
            )
            .filter_map(|(tx, exec_result)| exec_result.was_executed().then_some(tx))
            .collect()
    }

    pub fn load_and_execute_transactions_output(&self) -> &LoadAndExecuteTransactionsOutput {
        &self.load_and_execute_transactions_output
    }

    pub fn transactions(&self) -> &[SanitizedTransaction] {
        self.transactions
    }

    pub fn loaded_transactions_mut(&mut self) -> &mut [TransactionLoadResult] {
        &mut self
            .load_and_execute_transactions_output
            .loaded_transactions
    }

    pub fn execution_results(&self) -> &[TransactionExecutionResult] {
        &self.load_and_execute_transactions_output.execution_results
    }

    pub fn pre_balance_info(&mut self) -> &mut PreBalanceInfo {
        &mut self.pre_balance_info
    }

    pub fn post_balance_info(&self) -> &(TransactionBalances, TransactionTokenBalances) {
        &self.post_balance_info
    }

    pub fn pre_tx_execution_accounts(&self) -> &Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
        &self.pre_tx_execution_accounts
    }

    pub fn post_tx_execution_accounts(&self) -> &Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
        &self.post_tx_execution_accounts
    }
}

pub type LoadAndExecuteBundleResult<T> = Result<T, LoadAndExecuteBundleError>;

/// Return an Error if a transaction was executed and reverted
/// NOTE: `execution_results` are zipped with `sanitized_txs` so it's expected a sanitized tx at
/// position i has a corresponding execution result at position i within the `execution_results`
/// slice
pub fn check_bundle_execution_results<'a>(
    execution_results: &'a [TransactionExecutionResult],
    sanitized_txs: &'a [SanitizedTransaction],
) -> Result<(), (&'a SanitizedTransaction, &'a TransactionExecutionResult)> {
    for (exec_results, sanitized_tx) in execution_results.iter().zip(sanitized_txs) {
        match exec_results {
            TransactionExecutionResult::Executed { details, .. } => {
                if details.status.is_err() {
                    return Err((sanitized_tx, exec_results));
                }
            }
            TransactionExecutionResult::NotExecuted(e) => {
                if !matches!(e, TransactionError::AccountInUse) {
                    return Err((sanitized_tx, exec_results));
                }
            }
        }
    }
    Ok(())
}

/// Executing a bundle is somewhat complicated compared to executing single transactions. In order to
/// avoid duplicate logic for execution and simulation, this function can be leveraged.
///
/// Assumptions for the caller:
/// - all transactions were signed properly
/// - user has deduplicated transactions inside the bundle
///
/// TODO (LB):
/// - given a bundle with 3 transactions that write lock the following accounts: [A, B, C], on failure of B
///   we should add in the BundleTransactionsOutput of A and C and return the error for B.
#[allow(clippy::too_many_arguments)]
pub fn load_and_execute_bundle<'a>(
    bank: &Bank,
    bundle: &'a SanitizedBundle,
    // Max blockhash age
    max_age: usize,
    // Upper bound on execution time for a bundle
    max_processing_time: &Duration,
    // Execution data logging
    enable_cpi_recording: bool,
    enable_log_recording: bool,
    enable_return_data_recording: bool,
    enable_balance_recording: bool,
    log_messages_bytes_limit: &Option<usize>,
    // simulation will not use the Bank's account locks when building the TransactionBatch
    // if simulating on an unfrozen bank, this is helpful to avoid stalling replay and use whatever
    // state the accounts are in at the current time
    is_simulation: bool,
    account_overrides: Option<&mut AccountOverrides>,
    // these must be the same length as the bundle's transactions
    // allows one to read account state before and after execution of each transaction in the bundle
    // will use AccountsOverride + Bank
    pre_execution_accounts: &Vec<Option<Vec<Pubkey>>>,
    post_execution_accounts: &Vec<Option<Vec<Pubkey>>>,
) -> LoadAndExecuteBundleOutput<'a> {
    if pre_execution_accounts.len() != post_execution_accounts.len()
        || post_execution_accounts.len() != bundle.transactions.len()
    {
        return LoadAndExecuteBundleOutput {
            bundle_transaction_results: vec![],
            result: Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts),
            metrics: BundleExecutionMetrics::default(),
        };
    }
    let mut binding = AccountOverrides::default();
    let account_overrides = account_overrides.unwrap_or(&mut binding);

    let mut chunk_start = 0;
    let start_time = Instant::now();

    let mut bundle_transaction_results = vec![];
    let mut metrics = BundleExecutionMetrics::default();

    while chunk_start != bundle.transactions.len() {
        if start_time.elapsed() > *max_processing_time {
            trace!("bundle: {} took too long to execute", bundle.bundle_id);
            return LoadAndExecuteBundleOutput {
                bundle_transaction_results,
                metrics,
                result: Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(
                    start_time.elapsed(),
                )),
            };
        }

        let chunk_end = min(bundle.transactions.len(), chunk_start.saturating_add(128));
        let chunk = &bundle.transactions[chunk_start..chunk_end];

        // Note: these batches are dropped after execution and before record/commit, which is atypical
        // compared to BankingStage which holds account locks until record + commit to avoid race conditions with
        // other BankingStage threads. However, the caller of this method, BundleConsumer, will use BundleAccountLocks
        // to hold RW locks across all transactions in a bundle until its processed.
        let batch = if is_simulation {
            bank.prepare_sequential_sanitized_batch_with_results_for_simulation(chunk)
        } else {
            bank.prepare_sequential_sanitized_batch_with_results(chunk)
        };

        debug!(
            "bundle: {} batch num locks ok: {}",
            bundle.bundle_id,
            batch.lock_results().iter().filter(|lr| lr.is_ok()).count()
        );

        // Ensures that bundle lock results only return either:
        // Ok(()) | Err(TransactionError::AccountInUse)
        // If the error isn't one of those, then error out
        if let Some((transaction, lock_failure)) = batch.check_bundle_lock_results() {
            debug!(
                "bundle: {} lock error; signature: {} error: {}",
                bundle.bundle_id,
                transaction.signature(),
                lock_failure
            );
            return LoadAndExecuteBundleOutput {
                bundle_transaction_results,
                metrics,
                result: Err(LoadAndExecuteBundleError::LockError {
                    signature: *transaction.signature(),
                    transaction_error: lock_failure.clone(),
                }),
            };
        }

        let mut pre_balance_info = PreBalanceInfo::default();
        let (_, collect_balances_us) = measure_us!({
            if enable_balance_recording {
                pre_balance_info.native =
                    bank.collect_balances_with_cache(&batch, Some(account_overrides));
                pre_balance_info.token = collect_token_balances(
                    bank,
                    &batch,
                    &mut pre_balance_info.mint_decimals,
                    Some(account_overrides),
                );
            }
        });
        saturating_add_assign!(metrics.collect_balances_us, collect_balances_us);

        let end = min(
            chunk_start.saturating_add(batch.sanitized_transactions().len()),
            pre_execution_accounts.len(),
        );

        let m = Measure::start("accounts");
        let accounts_requested = &pre_execution_accounts[chunk_start..end];
        let pre_tx_execution_accounts =
            get_account_transactions(bank, account_overrides, accounts_requested, &batch);
        saturating_add_assign!(metrics.collect_pre_post_accounts_us, m.end_as_us());

        let (mut load_and_execute_transactions_output, load_execute_us) = measure_us!(bank
            .load_and_execute_transactions(
                &batch,
                max_age,
                enable_cpi_recording,
                enable_log_recording,
                enable_return_data_recording,
                &mut metrics.execute_timings,
                Some(account_overrides),
                *log_messages_bytes_limit,
            ));
        debug!(
            "bundle id: {} loaded_transactions: {:?}",
            bundle.bundle_id, load_and_execute_transactions_output.loaded_transactions
        );
        saturating_add_assign!(metrics.load_execute_us, load_execute_us);

        // All transactions within a bundle are expected to be executable + not fail
        // If there's any transactions that executed and failed or didn't execute due to
        // unexpected failures (not locking related), bail out of bundle execution early.
        if let Err((failing_tx, exec_result)) = check_bundle_execution_results(
            load_and_execute_transactions_output
                .execution_results
                .as_slice(),
            batch.sanitized_transactions(),
        ) {
            // TODO (LB): we should try to return partial results here for successful bundles in a parallel batch.
            //  given a bundle that write locks the following accounts [[A], [B], [C]]
            //  when B fails, we could return the execution results for A and C, but leave B out.
            //  however, if we have bundle that write locks accounts [[A_1], [A_2], [B], [C]] and B fails
            //  we'll get the results for A_1 but not [A_2], [B], [C] due to the way this loop executes.
            debug!(
                "bundle: {} execution error; signature: {} error: {:?}",
                bundle.bundle_id,
                failing_tx.signature(),
                exec_result
            );
            return LoadAndExecuteBundleOutput {
                bundle_transaction_results,
                metrics,
                result: Err(LoadAndExecuteBundleError::TransactionError {
                    signature: *failing_tx.signature(),
                    execution_result: Box::new(exec_result.clone()),
                }),
            };
        }

        // If none of the transactions were executed, most likely an AccountInUse error
        // need to retry to ensure that all transactions in the bundle are executed.
        if !load_and_execute_transactions_output
            .execution_results
            .iter()
            .any(|r| r.was_executed())
        {
            saturating_add_assign!(metrics.num_retries, 1);
            debug!(
                "bundle: {} no transaction executed, retrying",
                bundle.bundle_id
            );
            continue;
        }

        // Cache accounts so next iterations of loop can load cached state instead of using
        // AccountsDB, which will contain stale account state because results aren't committed
        // to the bank yet.
        // NOTE: Bank::collect_accounts_to_store does not handle any state changes related to
        // failed, non-nonce transactions.
        let m = Measure::start("cache");
        let accounts = bank.collect_accounts_to_store(
            batch.sanitized_transactions(),
            &load_and_execute_transactions_output.execution_results,
            &mut load_and_execute_transactions_output.loaded_transactions,
        );
        for (pubkey, data) in accounts {
            account_overrides.set_account(pubkey, Some(data.clone()));
        }
        saturating_add_assign!(metrics.cache_accounts_us, m.end_as_us());

        let end = max(
            chunk_start.saturating_add(batch.sanitized_transactions().len()),
            post_execution_accounts.len(),
        );

        let m = Measure::start("accounts");
        let accounts_requested = &post_execution_accounts[chunk_start..end];
        let post_tx_execution_accounts =
            get_account_transactions(bank, account_overrides, accounts_requested, &batch);
        saturating_add_assign!(metrics.collect_pre_post_accounts_us, m.end_as_us());

        let ((post_balances, post_token_balances), collect_balances_us) =
            measure_us!(if enable_balance_recording {
                let post_balances =
                    bank.collect_balances_with_cache(&batch, Some(account_overrides));
                let post_token_balances = collect_token_balances(
                    bank,
                    &batch,
                    &mut pre_balance_info.mint_decimals,
                    Some(account_overrides),
                );
                (post_balances, post_token_balances)
            } else {
                (
                    TransactionBalances::default(),
                    TransactionTokenBalances::default(),
                )
            });
        saturating_add_assign!(metrics.collect_balances_us, collect_balances_us);

        let processing_end = batch.lock_results().iter().position(|lr| lr.is_err());
        if let Some(end) = processing_end {
            chunk_start = chunk_start.saturating_add(end);
        } else {
            chunk_start = chunk_end;
        }

        bundle_transaction_results.push(BundleTransactionsOutput {
            transactions: chunk,
            load_and_execute_transactions_output,
            pre_balance_info,
            post_balance_info: (post_balances, post_token_balances),
            pre_tx_execution_accounts,
            post_tx_execution_accounts,
        });
    }

    LoadAndExecuteBundleOutput {
        bundle_transaction_results,
        metrics,
        result: Ok(()),
    }
}

fn get_account_transactions(
    bank: &Bank,
    account_overrides: &AccountOverrides,
    accounts: &[Option<Vec<Pubkey>>],
    batch: &TransactionBatch,
) -> Vec<Option<Vec<(Pubkey, AccountSharedData)>>> {
    let iter = izip!(batch.lock_results().iter(), accounts.iter());

    iter.map(|(lock_result, accounts_requested)| {
        if lock_result.is_ok() {
            accounts_requested.as_ref().map(|accounts_requested| {
                accounts_requested
                    .iter()
                    .map(|a| match account_overrides.get(a) {
                        None => (*a, bank.get_account(a).unwrap_or_default()),
                        Some(data) => (*a, data.clone()),
                    })
                    .collect()
            })
        } else {
            None
        }
    })
    .collect()
}

#[cfg(test)]
mod tests {
    use {
        crate::bundle_execution::{load_and_execute_bundle, LoadAndExecuteBundleError},
        assert_matches::assert_matches,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            bundle::{derive_bundle_id_from_sanitized_transactions, SanitizedBundle},
            clock::MAX_PROCESSING_AGE,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::{SanitizedTransaction, Transaction, TransactionError},
        },
        std::{
            sync::{Arc, Barrier},
            thread::{sleep, spawn},
            time::Duration,
        },
    };

    const MAX_PROCESSING_TIME: Duration = Duration::from_secs(1);
    const LOG_MESSAGE_BYTES_LIMITS: Option<usize> = Some(100_000);
    const MINT_AMOUNT_LAMPORTS: u64 = 1_000_000;

    fn create_simple_test_bank(lamports: u64) -> (GenesisConfigInfo, Arc<Bank>) {
        let genesis_config_info = create_genesis_config(lamports);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));
        (genesis_config_info, bank)
    }

    fn make_bundle(txs: &[Transaction]) -> SanitizedBundle {
        let transactions: Vec<_> = txs
            .iter()
            .map(|tx| SanitizedTransaction::try_from_legacy_transaction(tx.clone()).unwrap())
            .collect();

        let bundle_id = derive_bundle_id_from_sanitized_transactions(&transactions);

        SanitizedBundle {
            transactions,
            bundle_id,
        }
    }

    fn find_account_index(tx: &Transaction, account: &Pubkey) -> Option<usize> {
        tx.message
            .account_keys
            .iter()
            .position(|pubkey| account == pubkey)
    }

    /// A single, valid bundle shall execute successfully and return the correct BundleTransactionsOutput content
    #[test]
    fn test_single_transaction_bundle_success() {
        const TRANSFER_AMOUNT: u64 = 1_000;
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);
        let lamports_per_signature = bank
            .get_lamports_per_signature_for_blockhash(&genesis_config_info.genesis_config.hash())
            .unwrap();

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            TRANSFER_AMOUNT,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions);
        let default_accounts = vec![None; bundle.transactions.len()];

        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            true,
            true,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        // make sure the bundle succeeded
        assert!(execution_result.result.is_ok());

        // check to make sure there was one batch returned with one transaction that was the same that was put in
        assert_eq!(execution_result.bundle_transaction_results.len(), 1);
        let tx_result = execution_result.bundle_transaction_results.get(0).unwrap();
        assert_eq!(tx_result.transactions.len(), 1);
        assert_eq!(tx_result.transactions[0], bundle.transactions[0]);

        // make sure the transaction executed successfully
        assert_eq!(
            tx_result
                .load_and_execute_transactions_output
                .execution_results
                .len(),
            1
        );
        let execution_result = tx_result
            .load_and_execute_transactions_output
            .execution_results
            .get(0)
            .unwrap();
        assert!(execution_result.was_executed());
        assert!(execution_result.was_executed_successfully());

        // Make sure the post-balances are correct
        assert_eq!(tx_result.pre_balance_info.native.len(), 1);
        let post_tx_sol_balances = tx_result.post_balance_info.0.get(0).unwrap();

        let minter_message_index =
            find_account_index(&transactions[0], &genesis_config_info.mint_keypair.pubkey())
                .unwrap();
        let receiver_message_index = find_account_index(&transactions[0], &kp.pubkey()).unwrap();

        assert_eq!(
            post_tx_sol_balances[minter_message_index],
            MINT_AMOUNT_LAMPORTS - lamports_per_signature - TRANSFER_AMOUNT
        );
        assert_eq!(
            post_tx_sol_balances[receiver_message_index],
            TRANSFER_AMOUNT
        );
    }

    /// Test a simple failure
    #[test]
    fn test_single_transaction_bundle_fail() {
        const TRANSFER_AMOUNT: u64 = 1_000;
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        // kp has no funds, transfer will fail
        let kp = Keypair::new();
        let transactions = vec![transfer(
            &kp,
            &kp.pubkey(),
            TRANSFER_AMOUNT,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            true,
            true,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        assert_eq!(execution_result.bundle_transaction_results.len(), 0);

        assert!(execution_result.result.is_err());

        match execution_result.result.unwrap_err() {
            LoadAndExecuteBundleError::ProcessingTimeExceeded(_)
            | LoadAndExecuteBundleError::LockError { .. }
            | LoadAndExecuteBundleError::InvalidPreOrPostAccounts => {
                unreachable!();
            }
            LoadAndExecuteBundleError::TransactionError {
                signature,
                execution_result,
            } => {
                assert_eq!(signature, *bundle.transactions[0].signature());
                assert!(!execution_result.was_executed());
            }
        }
    }

    /// Tests a multi-tx bundle that succeeds. Checks the returned results
    #[test]
    fn test_multi_transaction_bundle_success() {
        const TRANSFER_AMOUNT_1: u64 = 100_000;
        const TRANSFER_AMOUNT_2: u64 = 50_000;
        const TRANSFER_AMOUNT_3: u64 = 10_000;
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);
        let lamports_per_signature = bank
            .get_lamports_per_signature_for_blockhash(&genesis_config_info.genesis_config.hash())
            .unwrap();

        // mint transfers 100k to 1
        // 1 transfers 50k to 2
        // 2 transfers 10k to 3
        // should get executed in 3 batches [[1], [2], [3]]
        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let kp3 = Keypair::new();
        let transactions = vec![
            transfer(
                &genesis_config_info.mint_keypair,
                &kp1.pubkey(),
                TRANSFER_AMOUNT_1,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp1,
                &kp2.pubkey(),
                TRANSFER_AMOUNT_2,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp2,
                &kp3.pubkey(),
                TRANSFER_AMOUNT_3,
                genesis_config_info.genesis_config.hash(),
            ),
        ];
        let bundle = make_bundle(&transactions);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            true,
            true,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        assert!(execution_result.result.is_ok());
        assert_eq!(execution_result.bundle_transaction_results.len(), 3);

        // first batch contains the first tx that was executed
        assert_eq!(
            execution_result.bundle_transaction_results[0].transactions,
            bundle.transactions
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .execution_results
                .len(),
            3
        );
        assert!(execution_result.bundle_transaction_results[0]
            .load_and_execute_transactions_output
            .execution_results[0]
            .was_executed_successfully());
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .execution_results[1]
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .load_and_execute_transactions_output
                .execution_results[2]
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .pre_balance_info
                .native
                .len(),
            3
        );
        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .post_balance_info
                .0
                .len(),
            3
        );

        let minter_index =
            find_account_index(&transactions[0], &genesis_config_info.mint_keypair.pubkey())
                .unwrap();
        let kp1_index = find_account_index(&transactions[0], &kp1.pubkey()).unwrap();

        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .post_balance_info
                .0[0][minter_index],
            MINT_AMOUNT_LAMPORTS - lamports_per_signature - TRANSFER_AMOUNT_1
        );

        assert_eq!(
            execution_result.bundle_transaction_results[0]
                .post_balance_info
                .0[0][kp1_index],
            TRANSFER_AMOUNT_1
        );

        // in the second batch, the second transaction was executed
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .transactions
                .to_owned(),
            bundle.transactions[1..]
        );
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .load_and_execute_transactions_output
                .execution_results
                .len(),
            2
        );
        assert!(execution_result.bundle_transaction_results[1]
            .load_and_execute_transactions_output
            .execution_results[0]
            .was_executed_successfully());
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .load_and_execute_transactions_output
                .execution_results[1]
                .flattened_result(),
            Err(TransactionError::AccountInUse)
        );

        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .pre_balance_info
                .native
                .len(),
            2
        );
        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .post_balance_info
                .0
                .len(),
            2
        );

        let kp1_index = find_account_index(&transactions[1], &kp1.pubkey()).unwrap();
        let kp2_index = find_account_index(&transactions[1], &kp2.pubkey()).unwrap();

        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .post_balance_info
                .0[0][kp1_index],
            TRANSFER_AMOUNT_1 - lamports_per_signature - TRANSFER_AMOUNT_2
        );

        assert_eq!(
            execution_result.bundle_transaction_results[1]
                .post_balance_info
                .0[0][kp2_index],
            TRANSFER_AMOUNT_2
        );

        // in the third batch, the third transaction was executed
        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .transactions
                .to_owned(),
            bundle.transactions[2..]
        );
        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .load_and_execute_transactions_output
                .execution_results
                .len(),
            1
        );
        assert!(execution_result.bundle_transaction_results[2]
            .load_and_execute_transactions_output
            .execution_results[0]
            .was_executed_successfully());

        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .pre_balance_info
                .native
                .len(),
            1
        );
        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .post_balance_info
                .0
                .len(),
            1
        );

        let kp2_index = find_account_index(&transactions[2], &kp2.pubkey()).unwrap();
        let kp3_index = find_account_index(&transactions[2], &kp3.pubkey()).unwrap();

        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .post_balance_info
                .0[0][kp2_index],
            TRANSFER_AMOUNT_2 - lamports_per_signature - TRANSFER_AMOUNT_3
        );

        assert_eq!(
            execution_result.bundle_transaction_results[2]
                .post_balance_info
                .0[0][kp3_index],
            TRANSFER_AMOUNT_3
        );
    }

    /// Tests a multi-tx bundle with the middle transaction failing.
    #[test]
    fn test_multi_transaction_bundle_fails() {
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();
        let kp3 = Keypair::new();
        let transactions = vec![
            transfer(
                &genesis_config_info.mint_keypair,
                &kp1.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp2,
                &kp3.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
            transfer(
                &kp1,
                &kp2.pubkey(),
                100_000,
                genesis_config_info.genesis_config.hash(),
            ),
        ];
        let bundle = make_bundle(&transactions);

        let default_accounts = vec![None; bundle.transactions.len()];
        let execution_result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &MAX_PROCESSING_TIME,
            true,
            true,
            true,
            true,
            &LOG_MESSAGE_BYTES_LIMITS,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );
        match execution_result.result.as_ref().unwrap_err() {
            LoadAndExecuteBundleError::ProcessingTimeExceeded(_)
            | LoadAndExecuteBundleError::LockError { .. }
            | LoadAndExecuteBundleError::InvalidPreOrPostAccounts => {
                unreachable!();
            }

            LoadAndExecuteBundleError::TransactionError {
                signature,
                execution_result: tx_failure,
            } => {
                assert_eq!(signature, bundle.transactions[1].signature());
                assert_eq!(
                    tx_failure.flattened_result(),
                    Err(TransactionError::AccountNotFound)
                );
                assert_eq!(execution_result.bundle_transaction_results().len(), 0);
            }
        }
    }

    /// Tests that when the max processing time is exceeded, the bundle is an error
    #[test]
    fn test_bundle_max_processing_time_exceeded() {
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions);

        let locked_transfer = vec![SanitizedTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        // locks it and prevents execution bc write lock on genesis_config_info.mint_keypair + kp.pubkey() held
        let _batch = bank.prepare_sanitized_batch(&locked_transfer);

        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            false,
            false,
            false,
            &None,
            false,
            None,
            &default,
            &default,
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::ProcessingTimeExceeded(_))
        );
    }

    #[test]
    fn test_simulate_bundle_with_locked_account_works() {
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions);

        let locked_transfer = vec![SanitizedTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        let _batch = bank.prepare_sanitized_batch(&locked_transfer);

        // simulation ignores account locks so you can simulate bundles on unfrozen banks
        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            false,
            false,
            false,
            &None,
            true,
            None,
            &default,
            &default,
        );
        assert!(result.result.is_ok());
    }

    /// Creates a multi-tx bundle and temporarily locks the accounts for one of the transactions in a bundle.
    /// Ensures the result is what's expected
    #[test]
    fn test_bundle_works_with_released_account_locks() {
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);
        let barrier = Arc::new(Barrier::new(2));

        let kp = Keypair::new();

        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = Arc::new(make_bundle(&transactions));

        let locked_transfer = vec![SanitizedTransaction::from_transaction_for_tests(transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            2,
            genesis_config_info.genesis_config.hash(),
        ))];

        // background thread locks the accounts for a bit then unlocks them
        let thread = {
            let barrier = barrier.clone();
            let bank = bank.clone();
            spawn(move || {
                let batch = bank.prepare_sanitized_batch(&locked_transfer);
                barrier.wait();
                sleep(Duration::from_millis(500));
                drop(batch);
            })
        };

        let _ = barrier.wait();

        // load_and_execute_bundle should spin for a bit then process after the 500ms sleep is over
        let default = vec![None; bundle.transactions.len()];
        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_secs(2),
            false,
            false,
            false,
            false,
            &None,
            false,
            None,
            &default,
            &default,
        );
        assert!(result.result.is_ok());

        thread.join().unwrap();
    }

    /// Tests that when the max processing time is exceeded, the bundle is an error
    #[test]
    fn test_bundle_bad_pre_post_accounts() {
        let (genesis_config_info, bank) = create_simple_test_bank(MINT_AMOUNT_LAMPORTS);

        let kp = Keypair::new();
        let transactions = vec![transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config_info.genesis_config.hash(),
        )];
        let bundle = make_bundle(&transactions);

        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            false,
            false,
            false,
            &None,
            false,
            None,
            &vec![None; 2],
            &vec![None; bundle.transactions.len()],
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts)
        );

        let result = load_and_execute_bundle(
            &bank,
            &bundle,
            MAX_PROCESSING_AGE,
            &Duration::from_millis(100),
            false,
            false,
            false,
            false,
            &None,
            false,
            None,
            &vec![None; bundle.transactions.len()],
            &vec![None; 2],
        );
        assert_matches!(
            result.result,
            Err(LoadAndExecuteBundleError::InvalidPreOrPostAccounts)
        );
    }
}
