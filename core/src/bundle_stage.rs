//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::{BatchedTransactionDetails, CommitTransactionDetails},
        bundle::PacketBundle,
        bundle_account_locker::BundleAccountLocker,
        bundle_sanitizer::BundleSanitizer,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::QosService,
        tip_manager::TipManager,
    },
    crossbeam_channel::Receiver,
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore_processor::TransactionStatusSender,
        token_balances::{collect_balances_with_cache, collect_token_balances},
    },
    solana_measure::measure,
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
    uuid::Uuid,
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
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
    ) -> Self {
        const NUM_BUNDLES_PRELOCK: usize = 3;

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
            NUM_BUNDLES_PRELOCK,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
        max_bundle_retry_duration: Duration,
        num_bundles_prelock: usize,
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
                    num_bundles_prelock,
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
        let mut account_overrides = AccountOverrides {
            accounts: HashMap::with_capacity(20),
        };

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

            let ((pre_balances, pre_token_balances), _) = measure!(
                Self::collect_balances(
                    bank,
                    &batch,
                    &account_overrides,
                    transaction_status_sender,
                    &mut mint_decimals,
                ),
                "collect_balances",
            );

            let (mut load_and_execute_transactions_output, load_execute_time) = measure!(
                bank.load_and_execute_transactions(
                    &batch,
                    MAX_PROCESSING_AGE,
                    transaction_status_sender.is_some(),
                    transaction_status_sender.is_some(),
                    transaction_status_sender.is_some(),
                    &mut execute_and_commit_timings.execute_timings,
                    Some(&account_overrides),
                    None,
                ),
                "load_execute",
            );
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
                warn!("none of the transactions were executed");
                let bundle_execution_elapsed = start_time.elapsed();
                if bundle_execution_elapsed >= *max_bundle_retry_duration {
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

            let ((post_balances, post_token_balances), _) = measure!(
                Self::collect_balances(
                    bank,
                    &batch,
                    &account_overrides,
                    transaction_status_sender,
                    &mut mint_decimals,
                ),
                "collect_balances",
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
        // TODO: do we really want assertions in production code?

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

        let (_freeze_lock, _freeze_lock_time) = measure!(bank.freeze_lock(), "freeze_lock");

        let (slot, mixins) = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        let mut transaction_index = Self::try_record(recorder, slot, mixins)?.unwrap_or_default();

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

            let (_, _) = measure!(
                {
                    bank_utils::find_and_send_votes(
                        &sanitized_txs,
                        &transaction_results,
                        Some(gossip_vote_sender),
                    );
                    if let Some(transaction_status_sender) = transaction_status_sender {
                        let batch_transaction_indexes: Vec<_> = transaction_results
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
                            sanitized_txs,
                            output.execution_results,
                            TransactionBalancesSet::new(r.pre_balances.0, r.post_balances.0),
                            TransactionTokenBalancesSet::new(r.pre_balances.1, r.post_balances.1),
                            transaction_results.rent_debits.clone(),
                            batch_transaction_indexes,
                        );
                    }
                },
                "find_and_send_votes",
            );

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
            let balances = collect_balances_with_cache(batch, bank, Some(cached_accounts));
            let token_balances =
                collect_token_balances(bank, batch, mint_decimals, Some(cached_accounts));
            (balances, token_balances)
        } else {
            (vec![], vec![])
        }
    }

    /// Schedules bundles until the validator is a leader, at which point it returns a bank and
    /// bundle_locker_sanitizer with >= 1 bundle to be executed
    fn schedule_bundles_until_leader(
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> BundleExecutionResult<BankStart> {
        // drop bundles if not within this slot range
        const DROP_BUNDLE_SLOT_OFFSET: u64 = 4;
        while let Ok(bundles) = bundle_receiver.recv() {
            let r_poh_recorder = poh_recorder.read().unwrap();
            let poh_recorder_bank = r_poh_recorder.get_poh_recorder_bank();
            let working_bank_start = poh_recorder_bank.working_bank_start();
            let would_be_leader_soon =
                r_poh_recorder.would_be_leader(DROP_BUNDLE_SLOT_OFFSET * DEFAULT_TICKS_PER_SLOT);
            let is_leader_now =
                PohRecorder::get_working_bank_if_not_expired(&working_bank_start).is_some();
            drop(r_poh_recorder);

            match (is_leader_now, would_be_leader_soon) {
                // leader now, insert new read bundles + as many as can read then return bank
                (true, _) => {
                    unprocessed_bundles.extend(bundles);
                    unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
                    return Ok(working_bank_start.unwrap().clone());
                }
                // leader soon, insert new read bundles
                (false, true) => {
                    unprocessed_bundles.extend(bundles);
                    unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
                }
                // not leader now and not soon, clear bundles
                (false, false) => {
                    let new_dropped_bundles: Vec<_> = bundles.iter().map(|p| p.uuid).collect();
                    let old_dropped_bundles: Vec<_> =
                        unprocessed_bundles.iter().map(|p| p.uuid).collect();
                    unprocessed_bundles.clear();
                    info!("dropping new bundles: {:?}", new_dropped_bundles);
                    info!("dropping old bundles: {:?}", old_dropped_bundles);
                }
            }
        }
        return Err(BundleExecutionError::Shutdown);
    }

    /// Initializes the tip config, as well as the tip_receiver iff the epoch has changed, then
    /// sets the tip_receiver if needed and executes them as a bundle.
    fn maybe_initialize_and_change_tip_receiver(
        bank_start: &BankStart,
        tip_manager: &TipManager,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        gossip_vote_sender: &ReplayVoteSender,
        cluster_info: &Arc<ClusterInfo>,
        max_bundle_retry_duration: &Duration,
    ) -> BundleExecutionResult<()> {
        let BankStart {
            working_bank: bank, ..
        } = bank_start;

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

        if !transactions.is_empty() {
            info!("executing init txs");
            Self::update_qos_and_execute_record_commit_bundle(
                &SanitizedBundle {
                    transactions,
                    uuid: Uuid::new_v4(),
                },
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                bank_start,
                execute_and_commit_timings,
                max_bundle_retry_duration,
            )?;
            info!("successfully executed init txs");
        }

        let configured_tip_receiver = tip_manager.get_configured_tip_receiver(bank)?;
        let my_tip_distribution_pda = tip_manager.get_my_tip_distribution_pda(bank.epoch());

        if configured_tip_receiver != my_tip_distribution_pda {
            info!(
                "changing tip receiver from {:?} to {:?}",
                configured_tip_receiver, my_tip_distribution_pda
            );
            let sanitized_bundle = SanitizedBundle {
                transactions: vec![tip_manager.change_tip_receiver_tx(
                    &my_tip_distribution_pda,
                    bank,
                    &cluster_info.keypair(),
                )?],
                uuid: Uuid::new_v4(),
            };

            Self::update_qos_and_execute_record_commit_bundle(
                &sanitized_bundle,
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                bank_start,
                execute_and_commit_timings,
                max_bundle_retry_duration,
            )?;
        }
        Ok(())
    }

    /// Handles tip account management and executing, recording, and committing bundles
    #[allow(clippy::too_many_arguments)]
    fn handle_tip_and_execute_record_commit_bundle(
        sanitized_bundle: &SanitizedBundle,
        tip_manager: &TipManager,
        bank_start: &BankStart,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        cluster_info: &Arc<ClusterInfo>,
        max_bundle_retry_duration: &Duration,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
    ) -> BundleExecutionResult<()> {
        let _lock = tip_manager.lock();
        let tip_pdas = tip_manager.get_tip_accounts();
        if Self::bundle_touches_tip_pdas(&sanitized_bundle.transactions, &tip_pdas) {
            let _ = Self::maybe_initialize_and_change_tip_receiver(
                bank_start,
                tip_manager,
                qos_service,
                recorder,
                transaction_status_sender,
                execute_and_commit_timings,
                gossip_vote_sender,
                cluster_info,
                max_bundle_retry_duration,
            )?;
            info!("successfully changed tip receiver");
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

    /// Executes as many bundles as possible until there's no more bundles to execute or the slot
    /// is finished.
    #[allow(clippy::too_many_arguments)]
    fn execute_bundles_until_empty_or_end_of_slot(
        bundle_account_locker: &Arc<Mutex<BundleAccountLocker>>,
        bundle_sanitizer: &BundleSanitizer,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        locked_bundles: &mut VecDeque<(PacketBundle, SanitizedBundle)>,
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        bank_start: BankStart,
        consensus_accounts_cache: &HashSet<Pubkey>,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        num_bundles_prelock: &usize,
    ) -> BundleExecutionResult<()> {
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        loop {
            // ensure locked_bundles contains at least num_bundles_prelock bundles so bundles
            // tradeoff here to be made with respect to how many bundles are prelocked.
            // too many locks and bundle_stage will rip, but may also starve banking stage for too
            // long. too few locks and bundle_stage might starve waiting for banking stage to finish
            // processing
            unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
            while locked_bundles.len() <= *num_bundles_prelock && !unprocessed_bundles.is_empty() {
                let packet_bundle = unprocessed_bundles.pop_front().unwrap();
                match bundle_sanitizer.get_sanitized_bundle(
                    &packet_bundle,
                    &bank_start.working_bank,
                    consensus_accounts_cache,
                ) {
                    Ok(sanitized_bundle) => {
                        let mut bundle_account_locker_l = bundle_account_locker.lock().unwrap();
                        match bundle_account_locker_l.lock_bundle_accounts(&sanitized_bundle) {
                            Ok(_) => {
                                locked_bundles.push_back((packet_bundle, sanitized_bundle));
                            }
                            Err(e) => {
                                error!("error locking bundle account: {:?}", e);
                            }
                        }
                    }
                    Err(e) => {
                        error!("sanitizing bundle error: {:?}", e);
                    }
                }
            }

            if let Some((packet_bundle, sanitized_bundle)) = locked_bundles.pop_front() {
                let result = Self::handle_tip_and_execute_record_commit_bundle(
                    &sanitized_bundle,
                    tip_manager,
                    &bank_start,
                    qos_service,
                    recorder,
                    transaction_status_sender,
                    gossip_vote_sender,
                    cluster_info,
                    max_bundle_retry_duration,
                    &mut execute_and_commit_timings,
                );

                let _ = bundle_account_locker
                    .lock()
                    .unwrap()
                    .unlock_bundle_accounts(&sanitized_bundle);

                match result {
                    Ok(_) => {}
                    Err(BundleExecutionError::PohMaxHeightError) => {
                        warn!("poh max height reached uuid: {:?}", sanitized_bundle.uuid);
                        // push back on the front to be rescheduled
                        // note that we already unlocked the bundle account above, so don't need to
                        // worry about double locking here
                        locked_bundles.push_front((packet_bundle, sanitized_bundle));
                        return Err(BundleExecutionError::PohMaxHeightError);
                    }
                    Err(BundleExecutionError::TransactionFailure(e)) => {
                        warn!("tx failure uuid: {:?} e: {:?}", sanitized_bundle.uuid, e);
                    }
                    Err(BundleExecutionError::ExceedsCostModel) => {
                        warn!("tx exceeds cost model uuid: {:?}", sanitized_bundle.uuid,);
                    }
                    Err(BundleExecutionError::TipError(e)) => {
                        warn!(
                            "tx fails from tip error uuid: {:?} error: {:?}",
                            sanitized_bundle.uuid, e
                        );
                    }
                    Err(BundleExecutionError::Shutdown) => {
                        return Err(BundleExecutionError::Shutdown);
                    }
                    Err(BundleExecutionError::MaxRetriesExceeded(dur)) => {
                        warn!(
                            "tx exceeded max execution duration uuid: {:?} dur: {:?}",
                            sanitized_bundle.uuid, dur
                        );
                    }
                }
            } else {
                break;
            }
        }
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
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
        max_bundle_retry_duration: Duration,
        num_bundles_prelock: usize,
    ) {
        let recorder = poh_recorder.read().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
        let mut last_consensus_update = Epoch::default();

        let mut unprocessed_bundles: VecDeque<PacketBundle> = VecDeque::with_capacity(1000);
        let mut locked_bundles: VecDeque<(PacketBundle, SanitizedBundle)> =
            VecDeque::with_capacity(num_bundles_prelock + 1);

        let bundle_sanitizer = BundleSanitizer::new(&tip_manager.tip_payment_program_id());

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            match Self::schedule_bundles_until_leader(
                &bundle_receiver,
                &mut unprocessed_bundles,
                poh_recorder,
            ) {
                Ok(bank_start) => {
                    Self::maybe_update_consensus_cache(
                        &bank_start.working_bank,
                        &mut consensus_accounts_cache,
                        &mut last_consensus_update,
                    );

                    match Self::execute_bundles_until_empty_or_end_of_slot(
                        &bundle_account_locker,
                        &bundle_sanitizer,
                        &mut unprocessed_bundles,
                        &mut locked_bundles,
                        &bundle_receiver,
                        bank_start,
                        &consensus_accounts_cache,
                        &cluster_info,
                        &recorder,
                        &transaction_status_sender,
                        &gossip_vote_sender,
                        &qos_service,
                        &tip_manager,
                        &max_bundle_retry_duration,
                        &num_bundles_prelock,
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
                Err(BundleExecutionError::Shutdown) => {
                    return;
                }
                Err(e) => {
                    error!("error scheduling bundles: {:?}", e);
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
    ) -> BundleExecutionResult<Option<usize>> {
        return match recorder.record(bank_slot, mixins_txs) {
            Ok(maybe_tx_index) => Ok(maybe_tx_index),
            Err(PohRecorderError::MaxHeightReached) => Err(BundleExecutionError::PohMaxHeightError),
            Err(e) => panic!("Poh recorder returned unexpected error: {:?}", e),
        };
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
        let recorder = poh_recorder.read().unwrap().recorder();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let bundle_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
        let sanitized_bundle = bundle_sanitizer
            .get_sanitized_bundle(&bundle, &bank, &HashSet::default())
            .unwrap();

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
            assert!(bundle_sanitizer
                .get_sanitized_bundle(&bundle, &bank, &HashSet::default())
                .is_err());
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
    fn test_schedule_bundles_until_leader() {
        solana_logger::setup();
        let (bundle_sender, bundle_receiver) = unbounded();
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

        let bundle = vec![PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        }];
        assert!(bundle_sender.send(bundle).is_ok());

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
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
        let bundle_locker_sanitizer =
            Arc::new(Mutex::new(BundleSanitizer::new(&Pubkey::new_unique())));

        let mut unprocessed_bundles = VecDeque::new();

        let maybe_bank_start = BundleStage::schedule_bundles_until_leader(
            &bundle_receiver,
            &mut unprocessed_bundles,
            &poh_recorder,
        );
        assert!(maybe_bank_start.is_ok());
        assert_eq!(unprocessed_bundles.len(), 1);
        let sanitized_bundle = bundle_locker_sanitizer
            .lock()
            .unwrap()
            .get_sanitized_bundle(
                &unprocessed_bundles.pop_front().unwrap(),
                &bank,
                &HashSet::default(),
            );
        assert!(sanitized_bundle.is_ok());
        // TODO: the logic around working bank makes it difficult to test bundles are returned
        // when leader
        // let scheduled_bundles = BundleStage::schedule_bundles_until_leader(&bundle_receiver, &bundle_locker_sanitizer, &poh_recorder);
        // assert!(scheduled_bundles.is_ok());
        // assert_eq!(bundle_locker_sanitizer.lock().unwrap().num_bundles(), 0);
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
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
        let recorder = poh_recorder.read().unwrap().recorder();
        let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

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

        let bundle_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        // grab lock on tx1
        let _batch = bank.prepare_sanitized_batch(&sanitized_txs_1);

        // push and pop tx0
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, tx0).unwrap()]),
            uuid: Uuid::new_v4(),
        };
        info!("test_bundle_max_retries uuid: {:?}", bundle.uuid);

        let sanitized_bundle = bundle_sanitizer
            .get_sanitized_bundle(&bundle, &bank, &HashSet::default())
            .unwrap();

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

    // TODO (LB): add TX v2 test here
}
