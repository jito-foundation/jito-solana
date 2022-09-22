//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::{BatchedTransactionDetails, CommitTransactionDetails},
        bundle_account_locker::{BundleAccountLocker, BundleAccountLockerResult, LockedBundle},
        bundle_sanitizer::get_sanitized_bundle,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        packet_bundle::PacketBundle,
        qos_service::QosService,
        tip_manager::TipManager,
    },
    crossbeam_channel::{Receiver, RecvError},
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
    },
    solana_measure::measure::Measure,
    solana_poh::poh_recorder::{
        BankStart, PohRecorder,
        PohRecorderError::{self},
        TransactionRecorder,
    },
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        account_overrides::AccountOverrides,
        accounts::TransactionLoadResult,
        bank::{
            Bank, CommitTransactionCounts, LoadAndExecuteTransactionsOutput, TransactionBalances,
            TransactionBalancesSet, TransactionExecutionResult,
        },
        bank_utils,
        cost_model::{CostModel, TransactionCost},
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        bundle::{
            error::BundleExecutionError, sanitized::SanitizedBundle,
            utils::check_bundle_lock_results,
        },
        clock::{Epoch, Slot, DEFAULT_TICKS_PER_SLOT, MAX_PROCESSING_AGE},
        hash::Hash,
        pubkey::Pubkey,
        saturating_add_assign,
        transaction::{self, SanitizedTransaction, TransactionError, VersionedTransaction},
    },
    solana_transaction_status::token_balances::{
        TransactionTokenBalances, TransactionTokenBalancesSet,
    },
    std::{
        collections::{HashMap, HashSet, VecDeque},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(10);

type BundleExecutionResult<T> = Result<T, BundleExecutionError>;

struct AllExecutionResults {
    pub load_and_execute_tx_output: LoadAndExecuteTransactionsOutput,
    pub sanitized_txs: Vec<SanitizedTransaction>,
    pub pre_balances: (TransactionBalances, TransactionTokenBalances),
    pub post_balances: (TransactionBalances, TransactionTokenBalances),
}

pub struct BundleStage {
    bundle_thread: JoinHandle<()>,
}

impl BundleStage {
    #[allow(clippy::new_ret_no_self)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
    ) -> Self {
        Self::start_bundle_thread(
            cluster_info,
            poh_recorder,
            transaction_status_sender,
            gossip_vote_sender,
            cost_model,
            bundle_receiver,
            exit,
            tip_manager,
            bundle_account_locker,
            MAX_BUNDLE_RETRY_DURATION,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
    ) -> Self {
        let poh_recorder = poh_recorder.clone();
        let cluster_info = cluster_info.clone();

        let bundle_thread = Builder::new()
            .name("solana-bundle-stage".to_string())
            .spawn(move || {
                let transaction_status_sender = transaction_status_sender.clone();
                Self::bundle_stage(
                    cluster_info,
                    &poh_recorder,
                    transaction_status_sender,
                    bundle_receiver,
                    gossip_vote_sender,
                    0,
                    cost_model,
                    exit,
                    tip_manager,
                    bundle_account_locker,
                    max_bundle_retry_duration,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    // rollup transaction cost details, eg signature_cost, write_lock_cost, data_bytes_cost and
    // execution_cost from the batch of transactions selected for block.
    fn accumulate_batched_transaction_costs<'a>(
        transactions_costs: impl Iterator<Item = &'a TransactionCost>,
        transaction_results: impl Iterator<Item = &'a transaction::Result<()>>,
    ) -> BatchedTransactionDetails {
        let mut batched_transaction_details = BatchedTransactionDetails::default();
        transactions_costs
            .zip(transaction_results)
            .for_each(|(cost, result)| match result {
                Ok(_) => {
                    saturating_add_assign!(
                        batched_transaction_details.costs.batched_signature_cost,
                        cost.signature_cost
                    );
                    saturating_add_assign!(
                        batched_transaction_details.costs.batched_write_lock_cost,
                        cost.write_lock_cost
                    );
                    saturating_add_assign!(
                        batched_transaction_details.costs.batched_data_bytes_cost,
                        cost.data_bytes_cost
                    );
                    saturating_add_assign!(
                        batched_transaction_details
                            .costs
                            .batched_builtins_execute_cost,
                        cost.builtins_execution_cost
                    );
                    saturating_add_assign!(
                        batched_transaction_details.costs.batched_bpf_execute_cost,
                        cost.bpf_execution_cost
                    );
                }
                Err(transaction_error) => match transaction_error {
                    TransactionError::WouldExceedMaxBlockCostLimit => {
                        saturating_add_assign!(
                            batched_transaction_details
                                .errors
                                .batched_retried_txs_per_block_limit_count,
                            1
                        );
                    }
                    TransactionError::WouldExceedMaxVoteCostLimit => {
                        saturating_add_assign!(
                            batched_transaction_details
                                .errors
                                .batched_retried_txs_per_vote_limit_count,
                            1
                        );
                    }
                    TransactionError::WouldExceedMaxAccountCostLimit => {
                        saturating_add_assign!(
                            batched_transaction_details
                                .errors
                                .batched_retried_txs_per_account_limit_count,
                            1
                        );
                    }
                    TransactionError::WouldExceedAccountDataBlockLimit => {
                        saturating_add_assign!(
                            batched_transaction_details
                                .errors
                                .batched_retried_txs_per_account_data_block_limit_count,
                            1
                        );
                    }
                    TransactionError::WouldExceedAccountDataTotalLimit => {
                        saturating_add_assign!(
                            batched_transaction_details
                                .errors
                                .batched_dropped_txs_per_account_data_total_limit_count,
                            1
                        );
                    }
                    _ => {}
                },
            });
        batched_transaction_details
    }

    /// Calculates QoS and reserves compute space for the bundle. If the bundle succeeds, commits
    /// the results to the cost tracker. If the bundle fails, rolls back any QoS changes made.
    /// Ensure that SanitizedBundle was returned by BundleAccountLocker to avoid parallelism issues
    /// with banking stage
    fn update_qos_and_execute_record_commit_bundle(
        sanitized_bundle: &SanitizedBundle,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        bank_start: &BankStart,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        max_bundle_retry_duration: &Duration,
    ) -> BundleExecutionResult<()> {
        let tx_costs = qos_service.compute_transaction_costs(sanitized_bundle.transactions.iter());
        let (transactions_qos_results, num_included) = qos_service.select_transactions_per_cost(
            sanitized_bundle.transactions.iter(),
            tx_costs.iter(),
            &bank_start.working_bank,
        );

        // qos rate-limited a tx in here, drop the bundle
        if sanitized_bundle.transactions.len() != num_included {
            QosService::remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                &bank_start.working_bank,
            );
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        // accumulates QoS to metrics
        qos_service.accumulate_estimated_transaction_costs(
            &Self::accumulate_batched_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
            ),
        );

        return match Self::execute_record_commit_bundle(
            sanitized_bundle,
            recorder,
            transaction_status_sender,
            gossip_vote_sender,
            bank_start,
            execute_and_commit_timings,
            max_bundle_retry_duration,
        ) {
            Ok(commit_transaction_details) => {
                // NOTE: Assumptions made on the QoS transaction costs:
                // - commit_transaction_details are returned in the same ordering as the transactions
                //   in the sanitized_bundle, which is the same ordering as tx_costs.
                // - all contents in the bundle are committed (it's executed all or nothing).
                // When fancier execution algorithms are made that may execute transactions out of
                // order (but resulting in same result as if they were executed sequentially), or
                // allow failures in bundles, one should revisit this and the code that returns
                // commit_transaction_details.
                QosService::update_or_remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    Some(&commit_transaction_details),
                    &bank_start.working_bank,
                );
                let (cu, us) = Self::accumulate_execute_units_and_time(
                    &execute_and_commit_timings.execute_timings,
                );
                qos_service.accumulate_actual_execute_cu(cu);
                qos_service.accumulate_actual_execute_time(us);
                qos_service.report_metrics(bank_start.working_bank.clone());
                Ok(())
            }
            Err(e) => {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    &bank_start.working_bank,
                );
                qos_service.report_metrics(bank_start.working_bank.clone());
                Err(e)
            }
        };
    }

    fn execute_bundle(
        sanitized_bundle: &SanitizedBundle,
        transaction_status_sender: &Option<TransactionStatusSender>,
        bank_start: &BankStart,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        max_bundle_retry_duration: &Duration,
    ) -> BundleExecutionResult<Vec<AllExecutionResults>> {
        let mut account_overrides = AccountOverrides::default();

        let mut execution_results = Vec::new();
        let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

        let BankStart {
            working_bank: bank,
            bank_creation_time,
        } = bank_start;

        let mut chunk_start = 0;
        let start_time = Instant::now();
        while chunk_start != sanitized_bundle.transactions.len() {
            if !Bank::should_bank_still_be_processing_txs(bank_creation_time, bank.ns_per_slot) {
                return Err(BundleExecutionError::PohMaxHeightError);
            }

            // ************************************************************************
            // Build a TransactionBatch that ensures transactions in the bundle
            // are executed sequentially.
            // NOTE: The TransactionBatch is dropped before the results are committed, which
            // would normally open up race conditions between this stage and BankingStage where
            // a transaction here could read and execute state on a transaction and BankingStage
            // could read-execute-store, invaliding the state produced by the bundle.
            // Assuming the SanitizedBundle was locked with the BundleAccountLocker, that race
            // condition shall be prevented as it holds an extra set of locks until the entire
            // bundle is processed.
            // ************************************************************************
            let chunk_end = std::cmp::min(sanitized_bundle.transactions.len(), chunk_start + 128);
            let chunk = &sanitized_bundle.transactions[chunk_start..chunk_end];
            let batch = bank.prepare_sequential_sanitized_batch_with_results(chunk, None);

            // Ensures that bundle lock results only return either:
            //  Ok(())
            //  Err(TransactionError::AccountInUse)
            //  Err(TransactionError::BundleNotContinuous)
            // if unexpected failure case, the bundle can't be executed
            // NOTE: previous logging around batch here caused issues with
            // unit tests failing due to PoH hitting max height. Unknown why. Be advised.
            if let Some((e, _)) = check_bundle_lock_results(batch.lock_results()) {
                return Err(e.into());
            }

            let (pre_balances, pre_token_balances) = Self::collect_balances(
                bank,
                &batch,
                &account_overrides,
                transaction_status_sender,
                &mut mint_decimals,
            );

            let mut load_execute_time = Measure::start("load_execute");
            let mut load_and_execute_transactions_output = bank.load_and_execute_transactions(
                &batch,
                MAX_PROCESSING_AGE,
                transaction_status_sender.is_some(),
                transaction_status_sender.is_some(),
                &mut execute_and_commit_timings.execute_timings,
                Some(&account_overrides),
            );
            load_execute_time.stop();

            execute_and_commit_timings.load_execute_us = load_execute_time.as_us();

            // Return error if executed and failed or didn't execute because of an unexpected reason.
            // The only acceptable reasons for not executing would be failure to lock errors from:
            //  Ok(())
            //  Err(TransactionError::AccountInUse)
            //  Err(TransactionError::BundleNotContinuous)
            // If there's another error (AlreadyProcessed, InsufficientFundsForFee, etc.), bail out
            if let Err((e, _)) = TransactionExecutionResult::check_bundle_execution_results(
                load_and_execute_transactions_output
                    .execution_results
                    .as_slice(),
                batch.sanitized_transactions(),
            ) {
                return Err(e);
            }

            // The errors have been checked above, now check to see if any were executed at all
            // If none were executed, check to see if the bundle timed out and if so, return timeout
            // error
            if !load_and_execute_transactions_output
                .execution_results
                .iter()
                .any(|r| r.was_executed())
            {
                let bundle_execution_elapsed = start_time.elapsed();
                if bundle_execution_elapsed >= *max_bundle_retry_duration {
                    warn!("bundle timed out: {:?}", sanitized_bundle);
                    return Err(BundleExecutionError::MaxRetriesExceeded(
                        bundle_execution_elapsed,
                    ));
                }
                continue;
            }

            // *********************************************************************************
            // Cache results so next iterations of bundle execution can load cached state
            // instead of using AccountsDB which contains stale execution data.
            // *********************************************************************************
            Self::cache_accounts(
                bank,
                batch.sanitized_transactions(),
                &load_and_execute_transactions_output.execution_results,
                &mut load_and_execute_transactions_output.loaded_transactions,
                &mut account_overrides,
            );

            let (post_balances, post_token_balances) = Self::collect_balances(
                bank,
                &batch,
                &account_overrides,
                transaction_status_sender,
                &mut mint_decimals,
            );

            execution_results.push(AllExecutionResults {
                load_and_execute_tx_output: load_and_execute_transactions_output,
                sanitized_txs: batch.sanitized_transactions().to_vec(),
                pre_balances: (pre_balances, pre_token_balances),
                post_balances: (post_balances, post_token_balances),
            });

            // start at the next available transaction in the batch that threw an error
            let processing_end = batch.lock_results().iter().position(|lr| lr.is_err());
            if let Some(end) = processing_end {
                chunk_start += end;
            } else {
                chunk_start = chunk_end;
            }

            drop(batch);
        }
        Ok(execution_results)
    }

    /// Executes a bundle, where all transactions in the bundle are executed all-or-nothing.
    /// Executes all transactions until the end or the first failure. The account state between
    /// iterations is cached to a temporary HashMap to be used on successive runs
    #[allow(clippy::too_many_arguments)]
    fn execute_record_commit_bundle(
        sanitized_bundle: &SanitizedBundle,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        bank_start: &BankStart,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        max_bundle_retry_duration: &Duration,
    ) -> BundleExecutionResult<Vec<CommitTransactionDetails>> {
        let execution_results = Self::execute_bundle(
            sanitized_bundle,
            transaction_status_sender,
            bank_start,
            execute_and_commit_timings,
            max_bundle_retry_duration,
        )?;
        // in order for bundle to succeed, it most have something to record + commit
        assert!(!execution_results.is_empty());

        Self::record_commit_bundle(
            execution_results,
            &bank_start.working_bank,
            recorder,
            execute_and_commit_timings,
            transaction_status_sender,
            gossip_vote_sender,
        )
    }

    /// Records the entire bundle to PoH and if successful, commits all transactions to the Bank
    /// Note that the BundleAccountLocker still has a lock on these accounts in the bank
    fn record_commit_bundle(
        execution_results: Vec<AllExecutionResults>,
        bank: &Arc<Bank>,
        recorder: &TransactionRecorder,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
    ) -> BundleExecutionResult<Vec<CommitTransactionDetails>> {
        // *********************************************************************************
        // All transactions are executed in the bundle.
        // Record to PoH and send the saved execution results to the Bank.
        // Note: Ensure that bank.commit_transactions is called on a per-batch basis and
        // not all together
        // *********************************************************************************

        let _freeze_lock = bank.freeze_lock();
        let (slot, mixins) = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        Self::try_record(recorder, slot, mixins)?;

        let mut commit_transaction_details = Vec::new();
        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            let (last_blockhash, lamports_per_signature) =
                bank.last_blockhash_and_lamports_per_signature();
            let transaction_results = bank.commit_transactions(
                &sanitized_txs,
                &mut output.loaded_transactions,
                output.execution_results.clone(),
                last_blockhash,
                lamports_per_signature,
                CommitTransactionCounts {
                    committed_transactions_count: output.executed_transactions_count as u64,
                    committed_with_failure_result_count: output
                        .executed_transactions_count
                        .saturating_sub(output.executed_with_successful_result_count)
                        as u64,
                    signature_count: output.signature_count,
                },
                &mut execute_and_commit_timings.execute_timings,
            );

            {
                bank_utils::find_and_send_votes(
                    &sanitized_txs,
                    &transaction_results,
                    Some(gossip_vote_sender),
                );
                if let Some(transaction_status_sender) = transaction_status_sender {
                    transaction_status_sender.send_transaction_status_batch(
                        bank.clone(),
                        sanitized_txs,
                        output.execution_results,
                        TransactionBalancesSet::new(r.pre_balances.0, r.post_balances.0),
                        TransactionTokenBalancesSet::new(r.pre_balances.1, r.post_balances.1),
                        transaction_results.rent_debits.clone(),
                    );
                }
            }

            for tx_results in transaction_results.execution_results {
                if let Some(details) = tx_results.details() {
                    commit_transaction_details.push(CommitTransactionDetails::Committed {
                        compute_units: details.executed_units,
                    });
                }
            }
        }
        Ok(commit_transaction_details)
    }

    /// Returns true if any of the transactions in a bundle mention one of the tip PDAs
    fn bundle_touches_tip_pdas(
        transactions: &[SanitizedTransaction],
        tip_pdas: &HashSet<Pubkey>,
    ) -> bool {
        let mut bundle_touches_tip_pdas = false;
        for tx in transactions {
            if tx
                .message()
                .account_keys()
                .iter()
                .any(|a| tip_pdas.contains(a))
            {
                bundle_touches_tip_pdas = true;
                break;
            }
        }
        bundle_touches_tip_pdas
    }

    fn accumulate_execute_units_and_time(execute_timings: &ExecuteTimings) -> (u64, u64) {
        let (units, times): (Vec<_>, Vec<_>) = execute_timings
            .details
            .per_program_timings
            .iter()
            .map(|(_program_id, program_timings)| {
                (
                    program_timings.accumulated_units,
                    program_timings.accumulated_us,
                )
            })
            .unzip();
        (units.iter().sum(), times.iter().sum())
    }

    fn cache_accounts(
        bank: &Arc<Bank>,
        txs: &[SanitizedTransaction],
        res: &[TransactionExecutionResult],
        loaded: &mut [TransactionLoadResult],
        cached_accounts: &mut AccountOverrides,
    ) {
        let accounts = bank.collect_accounts_to_store(txs, res, loaded);
        for (pubkey, data) in accounts {
            cached_accounts.set_account(pubkey, Some(data.clone()));
        }
    }

    fn collect_balances(
        bank: &Arc<Bank>,
        batch: &TransactionBatch,
        cached_accounts: &AccountOverrides,
        transaction_status_sender: &Option<TransactionStatusSender>,
        mint_decimals: &mut HashMap<Pubkey, u8>,
    ) -> (TransactionBalances, TransactionTokenBalances) {
        if transaction_status_sender.is_some() {
            let balances = bank.collect_balances_with_cache(batch, Some(cached_accounts));
            let token_balances =
                collect_token_balances(bank, batch, mint_decimals, Some(cached_accounts));
            (balances, token_balances)
        } else {
            (vec![], vec![])
        }
    }

    /// When executed the first time, there's some accounts that need to be initialized.
    /// This is only helpful for local testing, on testnet and mainnet these will never be executed.
    /// TODO (LB): consider removing this for mainnet/testnet and move to program deployment?
    fn get_initialize_tip_accounts_transactions(
        bank: &Bank,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
    ) -> BundleExecutionResult<Vec<SanitizedTransaction>> {
        let maybe_init_tip_payment_config_tx =
            if tip_manager.should_initialize_tip_payment_program(bank) {
                info!("initializing tip-payment program config");
                Some(tip_manager.initialize_tip_payment_program_tx(
                    bank.last_blockhash(),
                    &cluster_info.keypair(),
                ))
            } else {
                None
            };

        let maybe_init_tip_distro_config_tx =
            if tip_manager.should_initialize_tip_distribution_config(bank) {
                info!("initializing tip-distribution program config");
                Some(tip_manager.initialize_tip_distribution_config_tx(
                    bank.last_blockhash(),
                    &cluster_info.keypair(),
                ))
            } else {
                None
            };

        let maybe_init_tip_distro_account_tx = if tip_manager
            .should_init_tip_distribution_account(bank)
        {
            info!("initializing my [TipDistributionAccount]");
            Some(tip_manager.init_tip_distribution_account_tx(bank.last_blockhash(), bank.epoch()))
        } else {
            None
        };

        let transactions = [
            maybe_init_tip_payment_config_tx,
            maybe_init_tip_distro_config_tx,
            maybe_init_tip_distro_account_tx,
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<SanitizedTransaction>>();

        Ok(transactions)
    }

    /// Execute all unprocessed bundles until no more left or POH max tick height is reached.
    /// For any bundles that didn't execute due to POH max tick height reached, add them
    /// back onto the front of unprocessed_bundles in reverse order to preserve original ordering
    #[allow(clippy::too_many_arguments)]
    fn execute_bundles_until_empty_or_end_of_slot(
        bundle_account_locker: &mut BundleAccountLocker,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        blacklisted_accounts: &HashSet<Pubkey>,
        bank_start: &BankStart,
        consensus_accounts_cache: &HashSet<Pubkey>,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        last_tip_update_slot: &mut Slot,
    ) -> BundleExecutionResult<()> {
        // Drain all unprocessed bundles, turn to sanitized_bundles, lock them all, then process
        // until max proof-of-history tick
        let sanitized_bundles: VecDeque<(PacketBundle, SanitizedBundle)> = unprocessed_bundles
            .drain(..)
            .into_iter()
            .filter_map(|packet_bundle| {
                match get_sanitized_bundle(
                    &packet_bundle,
                    &bank_start.working_bank,
                    consensus_accounts_cache,
                    blacklisted_accounts,
                ) {
                    Ok(sanitized_bundle) => Some((packet_bundle, sanitized_bundle)),
                    Err(e) => {
                        warn!(
                            "failed to sanitize bundle uuid: {:?} error: {:?}",
                            packet_bundle.uuid, e
                        );
                        None
                    }
                }
            })
            .collect();

        let tip_pdas = tip_manager.get_tip_accounts();

        // Prepare locked bundles, which will RW lock accounts in sanitized_bundles so
        // BankingStage can't lock them. This adds a layer of protection since a transaction in a bundle
        // will not hold the AccountLocks through TransactionBatch across load-execute-commit cycle.
        // We collect here to ensure that all of the bundles are locked ahead of time for priority over
        // BankingStage
        let locked_bundles: Vec<BundleAccountLockerResult<LockedBundle>> = sanitized_bundles
            .iter()
            .map(|(_, sanitized_bundle)| {
                bundle_account_locker
                    .prepare_locked_bundle(sanitized_bundle, &bank_start.working_bank)
            })
            .collect();

        let execution_results = locked_bundles.into_iter().map(|maybe_locked_bundle| {
            let locked_bundle = maybe_locked_bundle.map_err(|_| BundleExecutionError::LockError)?;
            if !Bank::should_bank_still_be_processing_txs(
                &bank_start.bank_creation_time,
                bank_start.working_bank.ns_per_slot,
            ) {
                Err(BundleExecutionError::PohMaxHeightError)
            } else {
                let sanitized_bundle = locked_bundle.sanitized_bundle();

                // if bundle touches tip account, need to make sure the tip-related accounts are initialized
                // and the tip receiver is set correctly so tips in any bundles executed go to this leader
                // instead of the last.
                if Self::bundle_touches_tip_pdas(&sanitized_bundle.transactions, &tip_pdas)
                    && bank_start.working_bank.slot() != *last_tip_update_slot
                {
                    let initialize_tip_accounts_bundle = SanitizedBundle {
                        transactions: Self::get_initialize_tip_accounts_transactions(
                            &bank_start.working_bank,
                            tip_manager,
                            cluster_info,
                        )?,
                    };
                    {
                        let locked_init_tip_bundle = bundle_account_locker
                            .prepare_locked_bundle(
                                &initialize_tip_accounts_bundle,
                                &bank_start.working_bank,
                            )
                            .map_err(|_| BundleExecutionError::LockError)?;
                        Self::update_qos_and_execute_record_commit_bundle(
                            &locked_init_tip_bundle.sanitized_bundle(),
                            recorder,
                            transaction_status_sender,
                            gossip_vote_sender,
                            qos_service,
                            bank_start,
                            execute_and_commit_timings,
                            max_bundle_retry_duration,
                        )?;
                    }

                    // change tip receiver, draining the previous tip_receiver in the process
                    // note that this needs to happen after the above tip-related bundle executes
                    // because get_configured_tip_receiver reads an account in from the bank.
                    let configured_tip_receiver =
                        tip_manager.get_configured_tip_receiver(&bank_start.working_bank)?;
                    let my_tip_distribution_pda =
                        tip_manager.get_my_tip_distribution_pda(bank_start.working_bank.epoch());
                    if configured_tip_receiver != my_tip_distribution_pda {
                        info!(
                            "changing tip receiver from {} to {}",
                            configured_tip_receiver, my_tip_distribution_pda
                        );

                        let change_tip_receiver_bundle = SanitizedBundle {
                            transactions: vec![tip_manager.change_tip_receiver_tx(
                                &my_tip_distribution_pda,
                                &bank_start.working_bank,
                                &cluster_info.keypair(),
                            )?],
                        };
                        let locked_change_tip_receiver_bundle = bundle_account_locker
                            .prepare_locked_bundle(
                                &change_tip_receiver_bundle,
                                &bank_start.working_bank,
                            )
                            .map_err(|_| BundleExecutionError::LockError)?;
                        Self::update_qos_and_execute_record_commit_bundle(
                            &locked_change_tip_receiver_bundle.sanitized_bundle(),
                            recorder,
                            transaction_status_sender,
                            gossip_vote_sender,
                            qos_service,
                            bank_start,
                            execute_and_commit_timings,
                            max_bundle_retry_duration,
                        )?;
                    }

                    *last_tip_update_slot = bank_start.working_bank.slot();
                }
                Self::update_qos_and_execute_record_commit_bundle(
                    sanitized_bundle,
                    recorder,
                    transaction_status_sender,
                    gossip_vote_sender,
                    qos_service,
                    bank_start,
                    execute_and_commit_timings,
                    max_bundle_retry_duration,
                )
            }
        });

        execution_results
            .into_iter()
            .zip(sanitized_bundles.iter())
            .rev()
            .for_each(|(bundle_execution_result, (packet_bundle, _))| {
                if matches!(
                    bundle_execution_result,
                    Err(BundleExecutionError::PohMaxHeightError)
                ) {
                    unprocessed_bundles.push_front(packet_bundle.clone());
                }
            });

        Ok(())
    }

    /// Updates consensus-related accounts on epoch boundaries
    /// Bundles must not contain any consensus related accounts in order to prevent starvation
    /// of voting related transactions
    fn maybe_update_consensus_cache(
        bank: &Arc<Bank>,
        consensus_accounts_cache: &mut HashSet<Pubkey>,
        last_consensus_update: &mut Epoch,
    ) {
        if bank.epoch() > *last_consensus_update {
            *consensus_accounts_cache = Self::get_consensus_accounts(bank);
            *last_consensus_update = bank.epoch();
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn bundle_stage(
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        mut bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
    ) {
        const DROP_BUNDLE_SLOT_OFFSET: u64 = 4;

        let recorder = poh_recorder.lock().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        // Bundles can't mention any accounts related to consensus
        let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
        let mut last_consensus_update = Epoch::default();
        let mut last_tip_update_slot = Slot::default();

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        // Bundles can't mention the tip payment program to ensure that a malicious entity doesn't
        // steal tips mid-slot
        let blacklisted_accounts = HashSet::from_iter([tip_manager.tip_payment_program_id()]);

        let mut unprocessed_bundles: VecDeque<PacketBundle> = VecDeque::with_capacity(1000);
        while !exit.load(Ordering::Relaxed) {
            match bundle_receiver.recv() {
                Ok(bundles) => {
                    let l_poh_recorder = poh_recorder.lock().unwrap();
                    let poh_recorder_bank = l_poh_recorder.get_poh_recorder_bank();
                    let working_bank_start = poh_recorder_bank.working_bank_start();
                    let would_be_leader_soon = l_poh_recorder
                        .would_be_leader(DROP_BUNDLE_SLOT_OFFSET * DEFAULT_TICKS_PER_SLOT);
                    drop(l_poh_recorder);

                    match (working_bank_start, would_be_leader_soon) {
                        // leader now, insert new read bundles + as many as can read then return bank
                        (Some(bank_start), _) => {
                            unprocessed_bundles.extend(bundles);
                            unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
                            Self::maybe_update_consensus_cache(
                                &bank_start.working_bank,
                                &mut consensus_accounts_cache,
                                &mut last_consensus_update,
                            );
                            match Self::execute_bundles_until_empty_or_end_of_slot(
                                &mut bundle_account_locker,
                                &mut unprocessed_bundles,
                                &blacklisted_accounts,
                                bank_start,
                                &consensus_accounts_cache,
                                &cluster_info,
                                &recorder,
                                &transaction_status_sender,
                                &gossip_vote_sender,
                                &qos_service,
                                &tip_manager,
                                &max_bundle_retry_duration,
                                &mut execute_and_commit_timings,
                                &mut last_tip_update_slot,
                            ) {
                                Ok(_) => {
                                    // keep going
                                }
                                Err(BundleExecutionError::Shutdown) => {
                                    return;
                                }
                                Err(e) => {
                                    error!("error executing all bundles: {:?}", e);
                                }
                            }
                        }
                        // leader soon, insert new read bundles
                        (None, true) => {
                            unprocessed_bundles.extend(bundles);
                            unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
                        }
                        // not leader now and not soon, clear bundles
                        (None, false) => {
                            let new_dropped_bundles: Vec<_> =
                                bundles.iter().map(|p| p.uuid).collect();
                            let old_dropped_bundles: Vec<_> =
                                unprocessed_bundles.iter().map(|p| p.uuid).collect();
                            unprocessed_bundles.clear();

                            info!("dropping new bundles: {:?}", new_dropped_bundles);
                            info!("dropping old bundles: {:?}", old_dropped_bundles);
                        }
                    }
                }
                Err(RecvError) => {
                    error!("shutting down bundle_stage");
                    return;
                }
            }
        }
    }

    /// Builds a HashSet of all consensus related accounts for the Bank's epoch
    fn get_consensus_accounts(bank: &Arc<Bank>) -> HashSet<Pubkey> {
        let mut consensus_accounts: HashSet<Pubkey> = HashSet::new();
        if let Some(epoch_stakes) = bank.epoch_stakes(bank.epoch()) {
            // votes use the following accounts:
            // - vote_account pubkey: writeable
            // - authorized_voter_pubkey: read-only
            // - node_keypair pubkey: payer (writeable)
            let node_id_vote_accounts = epoch_stakes.node_id_to_vote_accounts();

            let vote_accounts = node_id_vote_accounts
                .values()
                .into_iter()
                .flat_map(|v| v.vote_accounts.clone());

            // vote_account
            consensus_accounts.extend(vote_accounts.into_iter());
            // authorized_voter_pubkey
            consensus_accounts.extend(epoch_stakes.epoch_authorized_voters().keys().into_iter());
            // node_keypair
            consensus_accounts.extend(epoch_stakes.node_id_to_vote_accounts().keys().into_iter());
        }
        consensus_accounts
    }

    fn prepare_poh_record_bundle(
        bank_slot: &Slot,
        execution_results_txs: &[AllExecutionResults],
    ) -> (Slot, Vec<(Hash, Vec<VersionedTransaction>)>) {
        let mixins_txs = execution_results_txs
            .iter()
            .map(|r| {
                let processed_transactions: Vec<VersionedTransaction> = r
                    .load_and_execute_tx_output
                    .execution_results
                    .iter()
                    .zip(r.sanitized_txs.iter())
                    .filter_map(|(execution_result, tx)| {
                        if execution_result.was_executed() {
                            Some(tx.to_versioned_transaction())
                        } else {
                            None
                        }
                    })
                    .collect();
                let hash = hash_transactions(&processed_transactions[..]);
                (hash, processed_transactions)
            })
            .collect();
        (*bank_slot, mixins_txs)
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    fn try_record(
        recorder: &TransactionRecorder,
        bank_slot: Slot,
        mixins_txs: Vec<(Hash, Vec<VersionedTransaction>)>,
    ) -> BundleExecutionResult<()> {
        match recorder.record(bank_slot, mixins_txs) {
            Ok(()) => Ok(()),
            Err(PohRecorderError::MaxHeightReached) => Err(BundleExecutionError::PohMaxHeightError),
            Err(e) => panic!("Poh recorder returned unexpected error: {:?}", e),
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::bundle_stage::tests::TestOption::{
            AssertDuplicateInBundleDropped, AssertNonZeroCostModel, AssertZeroedCostModel,
            LowComputeBudget,
        },
        crossbeam_channel::unbounded,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path_auto_delete,
        },
        solana_perf::packet::PacketBatch,
        solana_poh::poh_recorder::create_test_recorder,
        solana_sdk::{
            bundle::error::BundleExecutionError::{
                ExceedsCostModel, PohMaxHeightError, TransactionFailure,
            },
            compute_budget::ComputeBudgetInstruction,
            genesis_config::GenesisConfig,
            instruction::InstructionError,
            message::Message,
            packet::Packet,
            poh_config::PohConfig,
            signature::{Keypair, Signer},
            system_instruction,
            system_transaction::{self, transfer},
            transaction::{
                Transaction,
                TransactionError::{self, AccountNotFound},
            },
        },
        std::{collections::HashSet, sync::atomic::Ordering},
        uuid::Uuid,
    };
    const TEST_MAX_RETRY_DURATION: Duration = Duration::from_millis(500);

    enum TestOption {
        LowComputeBudget,
        AssertZeroedCostModel,
        AssertNonZeroCostModel,
        AssertDuplicateInBundleDropped,
    }

    #[cfg(test)]
    fn test_single_bundle(
        genesis_config: GenesisConfig,
        bundle: PacketBundle,
        options: Option<Vec<TestOption>>,
    ) -> Result<(), BundleExecutionError> {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
        // start a banking_stage to eat verified receiver
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        if options.is_some()
            && options
                .as_ref()
                .unwrap()
                .iter()
                .any(|option| matches!(option, LowComputeBudget))
        {
            bank.write_cost_tracker().unwrap().set_limits(1, 1, 1);
        }
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let poh_config = PohConfig {
            // limit tick count to avoid clearing working_bank at
            // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
            target_tick_count: Some(bank.max_tick_height() - 1), // == 1, only enough for ticks, not txs
            ..PohConfig::default()
        };
        let (exit, poh_recorder, poh_service, _entry_receiver) =
            create_test_recorder(&bank, &blockstore, Some(poh_config), None);
        let recorder = poh_recorder.lock().unwrap().recorder();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let bank_start = poh_recorder.lock().unwrap().bank_start().unwrap();
        let sanitized_bundle =
            get_sanitized_bundle(&bundle, &bank, &HashSet::default(), &HashSet::default()).unwrap();

        let results = BundleStage::update_qos_and_execute_record_commit_bundle(
            &sanitized_bundle,
            &recorder,
            &None,
            &gossip_vote_sender,
            &qos_service,
            &bank_start,
            &mut execute_and_commit_timings,
            &TEST_MAX_RETRY_DURATION,
        );

        // This is ugly, not really an option for testing but a test itself.
        // Still preferable to duplicating the entirety of this method
        // just to test duplicate txs are dropped.
        if options.is_some()
            && options
                .as_ref()
                .unwrap()
                .iter()
                .any(|option| matches!(option, AssertDuplicateInBundleDropped))
        {
            assert_eq!(results, Ok(()));
            assert!(
                get_sanitized_bundle(&bundle, &bank, &HashSet::default(), &HashSet::default())
                    .is_err()
            );
        }

        // Transaction rolled back successfully if
        // cost tracker has 0 transaction count
        // cost tracker as 0 block cost
        if options.is_some()
            && options
                .as_ref()
                .unwrap()
                .iter()
                .any(|option| matches!(option, AssertZeroedCostModel))
        {
            assert_eq!(bank.read_cost_tracker().unwrap().transaction_count(), 0);
            assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);
        }

        if options.is_some()
            && options
                .as_ref()
                .unwrap()
                .iter()
                .any(|option| matches!(option, AssertNonZeroCostModel))
        {
            assert_ne!(bank.read_cost_tracker().unwrap().transaction_count(), 0);
            assert_ne!(bank.read_cost_tracker().unwrap().block_cost(), 0);
        }

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        results
    }

    #[test]
    fn test_successful_bundle() {
        let (genesis_config, bundle) = setup_successful_tx();
        assert_eq!(
            test_single_bundle(genesis_config, bundle, Some(vec![AssertNonZeroCostModel])),
            Ok(())
        );
    }

    #[test]
    fn test_bundle_contains_processed_transaction() {
        let (genesis_config, bundle) = setup_successful_tx();
        assert_eq!(
            test_single_bundle(
                genesis_config,
                bundle,
                Some(vec![AssertDuplicateInBundleDropped]),
            ),
            Ok(())
        );
    }

    #[cfg(test)]
    fn setup_successful_tx() -> (GenesisConfig, PacketBundle) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(5);

        let kp_a = Keypair::new();
        let kp_b = Keypair::new();
        let ix_mint_a = system_instruction::transfer(&mint_keypair.pubkey(), &kp_a.pubkey(), 1);
        let ix_mint_b = system_instruction::transfer(&mint_keypair.pubkey(), &kp_b.pubkey(), 1);
        let message = Message::new(&[ix_mint_a, ix_mint_b], Some(&mint_keypair.pubkey()));
        let tx = Transaction::new(&[&mint_keypair], message, genesis_config.hash());
        let packet = Packet::from_data(None, tx).unwrap();

        (
            genesis_config,
            PacketBundle {
                batch: PacketBatch::new(vec![packet]),
                uuid: Uuid::new_v4(),
            },
        )
    }

    #[test]
    fn test_txs_exceed_cost_model() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(5);

        let kp = Keypair::new();
        let instruction = system_instruction::transfer(&mint_keypair.pubkey(), &kp.pubkey(), 1);
        let message = Message::new(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(1),
                instruction,
            ],
            Some(&mint_keypair.pubkey()),
        );
        let tx = Transaction::new(&[&mint_keypair], message, genesis_config.hash());
        let packet = Packet::from_data(None, tx).unwrap();

        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };
        assert_eq!(
            test_single_bundle(genesis_config, bundle, Some(vec![LowComputeBudget])),
            Err(ExceedsCostModel)
        );
    }

    #[test]
    fn test_nonce_tx_failure() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);

        let kp_a = Keypair::new();
        let kp_nonce = Keypair::new();
        let kp_nonce_authority = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::nonced_transfer(
                &mint_keypair,
                &kp_a.pubkey(),
                1,
                &kp_nonce.pubkey(),
                &kp_nonce_authority,
                genesis_config.hash(),
            ),
        )
        .unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        assert_eq!(
            test_single_bundle(genesis_config, bundle, None),
            Err(TransactionFailure(TransactionError::InstructionError(
                0,
                InstructionError::InvalidAccountData,
            )))
        );
    }

    #[test]
    fn test_qos_rollback() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);

        let kp_a = Keypair::new();
        let kp_b = Keypair::new();
        let successful_packet = Packet::from_data(
            None,
            system_transaction::transfer(&mint_keypair, &kp_b.pubkey(), 1, genesis_config.hash()),
        )
        .unwrap();
        let failed_packet = Packet::from_data(
            None,
            system_transaction::transfer(&kp_a, &kp_b.pubkey(), 1, genesis_config.hash()),
        )
        .unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![successful_packet, failed_packet]),
            uuid: Uuid::new_v4(),
        };

        assert_eq!(
            test_single_bundle(genesis_config, bundle, Some(vec![AssertZeroedCostModel])),
            Err(TransactionFailure(AccountNotFound))
        );
    }

    #[test]
    fn test_zero_balance_account() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair: _,
            ..
        } = create_genesis_config(4);

        let kp_a = Keypair::new();
        let kp_b = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::transfer(&kp_a, &kp_b.pubkey(), 1, genesis_config.hash()),
        )
        .unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        assert_eq!(
            test_single_bundle(genesis_config, bundle, None),
            Err(TransactionFailure(AccountNotFound))
        );
    }

    #[test]
    fn test_bundle_fails_poh_record() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);
        genesis_config.ticks_per_slot = 1; // Reduce ticks so that POH fails

        let kp_b = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::transfer(&mint_keypair, &kp_b.pubkey(), 1, genesis_config.hash()),
        )
        .unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };
        assert_eq!(
            test_single_bundle(genesis_config, bundle, None),
            Err(PohMaxHeightError)
        );
    }

    #[test]
    fn test_bundle_max_retries() {
        solana_logger::setup_with_default("INFO");

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(100_000_000);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(u64::MAX, u64::MAX, u64::MAX);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let poh_config = PohConfig {
            // limit tick count to avoid clearing working_bank at
            // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
            target_tick_count: Some(bank.max_tick_height() - 1), // == 1, only enough for ticks, not txs
            ..PohConfig::default()
        };
        let (exit, poh_recorder, poh_service, _entry_receiver) =
            create_test_recorder(&bank, &blockstore, Some(poh_config), None);
        let recorder = poh_recorder.lock().unwrap().recorder();
        let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let bank_start = poh_recorder.lock().unwrap().bank_start().unwrap();

        // Create two transfers
        // 0. mint_keypair -> keypair0
        // 1. keypair0 -> keypair 1
        // Lock the accounts through the bank for tx1 and try to process tx0.
        // It should timeout because BundleStage will continue to fail to get locks on keypair0.

        let keypair0 = Keypair::new();
        let keypair1 = Keypair::new();

        let tx0 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &keypair0.pubkey(),
            100_000,
            genesis_config.hash(),
        ));

        let tx1 = transfer(&keypair0, &keypair1.pubkey(), 50_000, genesis_config.hash());
        let sanitized_txs_1 = vec![SanitizedTransaction::from_transaction_for_tests(tx1)];

        // grab lock on tx1
        let _batch = bank.prepare_sanitized_batch(&sanitized_txs_1);

        // push and pop tx0
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
            uuid: Uuid::new_v4(),
        };
        info!("test_bundle_max_retries uuid: {:?}", bundle.uuid);

        let sanitized_bundle =
            get_sanitized_bundle(&bundle, &bank, &HashSet::default(), &HashSet::default()).unwrap();

        let result = BundleStage::update_qos_and_execute_record_commit_bundle(
            &sanitized_bundle,
            &recorder,
            &None,
            &gossip_vote_sender,
            &qos_service,
            &bank_start,
            &mut execute_and_commit_timings,
            &TEST_MAX_RETRY_DURATION,
        );
        info!("test_bundle_max_retries result: {:?}", result);
        assert!(matches!(
            result,
            Err(BundleExecutionError::MaxRetriesExceeded(_))
        ));

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }
}
