//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::BatchedTransactionDetails,
        bundle::PacketBundle,
        bundle_account_locker::BundleAccountLocker,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::{CommitTransactionDetails, QosService},
        tip_manager::TipManager,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, TryRecvError},
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure,
    solana_poh::poh_recorder::{
        BankStart, PohRecorder,
        PohRecorderError::{self},
        Record, TransactionRecorder,
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
        clock::{Epoch, Slot, MAX_PROCESSING_AGE},
        pubkey::Pubkey,
        saturating_add_assign,
        signature::Signer,
        transaction::{self, SanitizedTransaction, TransactionError, VersionedTransaction},
    },
    solana_transaction_status::token_balances::{
        collect_balances_with_cache, collect_token_balances, TransactionTokenBalances,
        TransactionTokenBalancesSet,
    },
    std::{
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    uuid::Uuid,
};

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
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
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
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
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
    ) -> BundleExecutionResult<Vec<AllExecutionResults>> {
        let mut account_overrides = AccountOverrides {
            slot_history: None,
            cached_accounts_with_rent: HashMap::with_capacity(20),
        };

        let mut execution_results = Vec::new();
        let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

        let BankStart {
            working_bank: bank,
            bank_creation_time,
        } = bank_start;

        let mut chunk_start = 0;
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
            error!("chunk_start: {:?}, batch: {:?}", chunk_start, batch);
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
                ),
                "load_execute",
            );
            execute_and_commit_timings.load_execute_us = load_execute_time.as_us();

            // Return error if executed and failed or didn't execute because of an unexpected reason
            // (AlreadyProcessed, InsufficientFundsForFee, etc.)
            if let Err((e, _)) = TransactionExecutionResult::check_bundle_execution_results(
                load_and_execute_transactions_output
                    .execution_results
                    .as_slice(),
                batch.sanitized_transactions(),
            ) {
                return Err(e);
            }

            // if none executed okay, nothing to record so try again
            if !load_and_execute_transactions_output
                .execution_results
                .iter()
                .any(|r| r.was_executed())
            {
                warn!("none of the transactions executed, trying again");
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
    ) -> BundleExecutionResult<Vec<CommitTransactionDetails>> {
        let execution_results = Self::execute_bundle(
            sanitized_bundle,
            transaction_status_sender,
            bank_start,
            execute_and_commit_timings,
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

        let (_freeze_lock, _freeze_lock_time) = measure!(bank.freeze_lock(), "freeze_lock");

        let record = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        Self::try_record(recorder, record)?;

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
                        transaction_status_sender.send_transaction_status_batch(
                            bank.clone(),
                            sanitized_txs,
                            output.execution_results,
                            TransactionBalancesSet::new(r.pre_balances.0, r.post_balances.0),
                            TransactionTokenBalancesSet::new(r.pre_balances.1, r.post_balances.1),
                            transaction_results.rent_debits.clone(),
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

    /// Returns false if:
    ///     bundle is empty
    ///     bundle packet length is not equal to transaction length
    ///     bundle contains duplicate transactions
    ///     TODO: bundle contains previously processed transactions (lucas said some bank function can check this)
    fn valid_bundle(transactions: &Vec<SanitizedTransaction>, bundle: Bundle) -> bool {
        if transactions.is_empty() || bundle.batch.packets.len() != transactions.len() {
            return false;
        }
        let mut previous_transactions = HashSet::new();
        for tx in transactions {
            if previous_transactions.contains(&tx.signatures()[0]) {
                return false;
            }
            previous_transactions.insert(tx.signatures()[0]);
        }
        return true;
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
            cached_accounts.put(*pubkey, data.clone());
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

    /// Read as many bundles off the chanel without blocking
    fn try_recv_bundles(
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        bundle_account_locker: &Arc<Mutex<BundleAccountLocker>>,
    ) -> BundleExecutionResult<()> {
        loop {
            match bundle_receiver.try_recv() {
                Ok(bundles) => {
                    bundle_account_locker.lock().unwrap().push(bundles);
                }
                Err(TryRecvError::Disconnected) => {
                    return Err(BundleExecutionError::Shutdown);
                }
                Err(TryRecvError::Empty) => return Ok(()),
            }
        }
    }

    /// Schedules bundles until the validator is a leader, at which point it returns a bank and
    /// bundle_account_locker with >= 1 bundle to be executed
    fn schedule_bundles_until_leader(
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        bundle_account_locker: &Arc<Mutex<BundleAccountLocker>>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
    ) -> BundleExecutionResult<BankStart> {
        // TODO (LB): drop bundle here if not leader anymore
        loop {
            let poh_recorder_bank = poh_recorder.lock().unwrap().get_poh_recorder_bank();
            let working_bank_start = poh_recorder_bank.working_bank_start();
            if PohRecorder::get_working_bank_if_not_expired(&working_bank_start).is_some()
                && bundle_account_locker.lock().unwrap().num_bundles() > 0
            {
                Self::try_recv_bundles(bundle_receiver, bundle_account_locker)?;
                return Ok(working_bank_start.unwrap().clone());
            } else {
                // don't block too long because we want to pick up the working bank as fast as possible
                // if we're transitioning from not leader -> leader and there's no new bundles to read
                // but bundles already pushed into the account locker
                match bundle_receiver.recv_timeout(Duration::from_millis(10)) {
                    Ok(bundles) => {
                        bundle_account_locker.lock().unwrap().push(bundles);
                        Self::try_recv_bundles(bundle_receiver, bundle_account_locker)?;
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(BundleExecutionError::Shutdown);
                    }
                }
            }
        }
    }

    /// Initializes the tip config and changes tip receiver if needed and executes them as a bundle
    fn maybe_initialize_and_change_tip_receiver(
        bank_start: &BankStart,
        tip_manager: &TipManager,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        gossip_vote_sender: &ReplayVoteSender,
        cluster_info: &Arc<ClusterInfo>,
    ) -> BundleExecutionResult<()> {
        let BankStart {
            working_bank: bank,
            bank_creation_time: _,
        } = bank_start;

        if bank.get_account(&tip_manager.config_pubkey()).is_none() {
            info!("initializing tip program config");
            let sanitized_bundle = SanitizedBundle {
                transactions: vec![tip_manager
                    .initialize_config_tx(&bank.last_blockhash(), &cluster_info.keypair())],
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
            )?;
        }

        let current_tip_receiver = tip_manager.get_current_tip_receiver(bank)?;
        let my_kp = cluster_info.keypair();
        if current_tip_receiver != my_kp.pubkey() {
            info!(
                "changing tip receiver from {:?} to {:?}",
                current_tip_receiver,
                my_kp.pubkey()
            );
            let sanitized_bundle = SanitizedBundle {
                transactions: vec![tip_manager.change_tip_receiver_tx(
                    &my_kp.pubkey(),
                    bank,
                    &my_kp,
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
            )?;
        }
        Ok(())
    }

    /// Executes as many bundles as possible until there's no more bundles to execute or the slot
    /// is finished.
    #[allow(clippy::too_many_arguments)]
    fn execute_bundles_until_empty_or_end_of_slot(
        bundle_account_locker: &Arc<Mutex<BundleAccountLocker>>,
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        bank_start: BankStart,
        consensus_accounts_cache: &HashSet<Pubkey>,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
    ) -> BundleExecutionResult<()> {
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        loop {
            // ensure bundle_account_locker is topped off so it can lock accounts ahead of time
            Self::try_recv_bundles(bundle_receiver, bundle_account_locker)?;
            let maybe_bundle = bundle_account_locker
                .lock()
                .unwrap()
                .pop(&bank_start.working_bank, consensus_accounts_cache);
            match maybe_bundle {
                None => {
                    break;
                }
                Some(locked_bundle) => {
                    let tip_pdas = tip_manager.get_tip_accounts();
                    if Self::bundle_touches_tip_pdas(
                        &locked_bundle.sanitized_bundle().transactions,
                        &tip_pdas,
                    ) {
                        let _lock = tip_manager.lock();
                        match Self::maybe_initialize_and_change_tip_receiver(
                            &bank_start,
                            tip_manager,
                            qos_service,
                            recorder,
                            transaction_status_sender,
                            &mut execute_and_commit_timings,
                            gossip_vote_sender,
                            cluster_info,
                        ) {
                            Ok(_) => {
                                info!("successfully changed tip receiver");
                            }
                            Err(e) => {
                                error!("error changing tip receiver, rescheduling bundle: {:?}", e);
                                bundle_account_locker
                                    .lock()
                                    .unwrap()
                                    .push_front(locked_bundle);
                                return Err(e);
                            }
                        }
                    }
                    match Self::update_qos_and_execute_record_commit_bundle(
                        locked_bundle.sanitized_bundle(),
                        recorder,
                        transaction_status_sender,
                        gossip_vote_sender,
                        qos_service,
                        &bank_start,
                        &mut execute_and_commit_timings,
                    ) {
                        Ok(_) => {
                            bundle_account_locker
                                .lock()
                                .unwrap()
                                .unlock_bundle_accounts(locked_bundle);
                        }
                        Err(BundleExecutionError::PohMaxHeightError) => {
                            // re-schedule bundle to be executed after grabbing a new bank
                            bundle_account_locker
                                .lock()
                                .unwrap()
                                .push_front(locked_bundle);
                            return Err(BundleExecutionError::PohMaxHeightError);
                        }
                        Err(e) => {
                            // keep going
                            bundle_account_locker
                                .lock()
                                .unwrap()
                                .unlock_bundle_accounts(locked_bundle);
                            error!("error executing bundle {:?}", e);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Updates consensus-related accounts on epoch boundaries
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
        bundle_account_locker: Arc<Mutex<BundleAccountLocker>>,
    ) {
        let recorder = poh_recorder.lock().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
        let mut last_consensus_update = Epoch::default();

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            match Self::schedule_bundles_until_leader(
                &bundle_receiver,
                &bundle_account_locker,
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
                        &bundle_receiver,
                        bank_start,
                        &consensus_accounts_cache,
                        &cluster_info,
                        &recorder,
                        &transaction_status_sender,
                        &gossip_vote_sender,
                        &qos_service,
                        &tip_manager,
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
    ) -> Record {
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
        Record {
            mixins_txs,
            slot: *bank_slot,
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    fn try_record(recorder: &TransactionRecorder, record: Record) -> BundleExecutionResult<()> {
        return match recorder.record(record) {
            Ok(()) => Ok(()),
            Err(PohRecorderError::MaxHeightReached) => Err(BundleExecutionError::PohMaxHeightError),
            Err(e) => panic!("Poh recorder returned unexpected error: {:?}", e),
        };
    }
}

#[cfg(test)]
mod tests {
    use etcd_client::SortTarget::Key;
    use {
        super::*,
        crossbeam_channel::unbounded,
        solana_address_lookup_table_program::state::{AddressLookupTable, LookupTableMeta},
        solana_entry::entry::{next_entry, next_versioned_entry, EntrySlice},
        solana_gossip::{cluster_info::Node, contact_info::ContactInfo},
        solana_ledger::{
            blockstore::{entries_to_test_shreds, Blockstore},
            genesis_utils::{create_genesis_config, GenesisConfigInfo},
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::{to_packet_batches, PacketFlags},
        solana_poh::{
            poh_recorder::{create_test_recorder, RecordReceiver, WorkingBankEntry},
            poh_service::PohService,
        },
        solana_program_runtime::timings::ProgramTiming,
        solana_rpc::transaction_status_service::TransactionStatusService,
        solana_sdk::{
            account::AccountSharedData,
            hash::Hash,
            instruction::InstructionError,
            message::{
                v0::{self, MessageAddressTableLookup},
                MessageHeader, VersionedMessage,
            },
            packet::Packet,
            poh_config::PohConfig,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::{
                MessageHash, SimpleAddressLoader, Transaction, TransactionError,
                VersionedTransaction,
            },
        },
        solana_streamer::{recvmmsg::recv_mmsg, socket::SocketAddrSpace},
        solana_transaction_status::{TransactionStatusMeta, VersionedTransactionWithStatusMeta},
        solana_vote_program::vote_transaction,
        std::{
            borrow::{Borrow, Cow},
            collections::HashSet,
            path::Path,
            sync::atomic::{AtomicBool, Ordering},
            thread::sleep,
        },
    };
    use solana_sdk::bundle::error::BundleExecutionError::{ExceedsCostModel, InvalidBundle, PohMaxHeightError, TransactionFailure};
    use solana_sdk::compute_budget::ComputeBudgetInstruction;
    use solana_sdk::genesis_config::GenesisConfig;
    use solana_sdk::message::Message;
    use solana_sdk::system_instruction;
    use solana_sdk::system_instruction::create_nonce_account;
    use solana_sdk::transaction::TransactionError::{AccountNotFound, AlreadyProcessed, BlockhashNotFound};

    // Test cases to cover:
//    Test bundle fails to record to PoH (X)
//    Try to whitebox test the caching, rent, and fee things // TODO: tested cost model exceeded...what else?
//    Test nonce transaction success and failure (1/2)
//    Test multiple of the same transaction in a bundle (X)
//    Test 0 balance fee payer in bundle (X)
//    Test invalid blockhash (X)
//    Ensure QoS/cost-model is calculated properly // TODO: how? push through invalid bundle and see the QoS is the same
//    Some creative tests if i can think of more edge cases after the above
    // ideas for these: unsigned tx?
    // QOS is reverted on failure
    // tip payer -> allow serializations but return error in the valid_bundle method

    #[cfg(test)]
    fn test_single_bundle_failure(genesis_config: GenesisConfig, bundle: Bundle) -> Result<(), BundleExecutionError> {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let results;
        {
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            results = {
                // start a banking_stage to eat verified receiver
                let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
                bank.write_cost_tracker().unwrap().set_limits(1, 1, 1);
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
                let (exit, poh_recorder, poh_service, entry_receiver) =
                    create_test_recorder(&bank, &blockstore, Some(poh_config), None); // (2) (7)
                let cluster_info = ClusterInfo::new(
                    Node::new_localhost().info,
                    Arc::new(Keypair::new()),
                    SocketAddrSpace::Unspecified,
                );
                let cluster_info = Arc::new(cluster_info); // (1)
                let tip_manager = Arc::new(Mutex::new(TipManager::new(Keypair::new().pubkey())));
                let recorder = poh_recorder.lock().unwrap().recorder();
                let cost_model = Arc::new(RwLock::new(CostModel::default()));
                let qos_service = QosService::new(cost_model, 0);

                let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
                let mut last_consensus_update = Epoch::default();

                let results = BundleStage::execute_bundle(
                    &cluster_info,
                    bundle,
                    &poh_recorder,
                    &recorder,
                    &None,
                    &gossip_vote_sender,
                    &qos_service,
                    &tip_manager,
                    &mut consensus_accounts_cache,
                    &mut last_consensus_update,
                );
                exit.store(true, Ordering::Relaxed);
                poh_service.join().unwrap();
                results
            };
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
        return results;
    }

    #[test]
    fn test_qos_rollback() {
        // This test first executes a bundle so cost tracker number of transactions is non-zero, then
        // executes a bundle with a few successful txs followed by a failed tx. This will force cost
        // model rollback. We then check the number of transactions is the same as after the first bundle
        // to confirm.
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(100);
        genesis_config.ticks_per_slot *= 8; // Increase ticks so bank doesn't reset
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
            {
                // start a banking_stage to eat verified receiver
                let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
                error!("bank: {:?}", bank.read_cost_tracker().unwrap());
                let blockstore = Arc::new(
                    Blockstore::open(ledger_path.path())
                        .expect("Expected to be able to open database ledger"),
                );
                let poh_config = PohConfig {
                    // limit tick count to avoid clearing working_bank at
                    // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
                    target_tick_count: Some(bank.max_tick_height() - 1),
                    ..PohConfig::default()
                };
                let (exit, poh_recorder, poh_service, entry_receiver) =
                    create_test_recorder(&bank, &blockstore, Some(poh_config), None); // (2) (7)
                let cluster_info = ClusterInfo::new(
                    Node::new_localhost().info,
                    Arc::new(Keypair::new()),
                    SocketAddrSpace::Unspecified,
                );
                let cluster_info = Arc::new(cluster_info); // (1)
                let tip_manager = Arc::new(Mutex::new(TipManager::new(Keypair::new().pubkey())));
                let recorder = poh_recorder.lock().unwrap().recorder();
                let cost_model = Arc::new(RwLock::new(CostModel::default()));
                let qos_service = QosService::new(cost_model, 0);

                let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
                let mut last_consensus_update = Epoch::default();

                let kp_a = Keypair::new();
                let kp_b = Keypair::new();
                error!("mint_pubkey: {:?}, a_pubkey: {:?}, b_pubkey: {:?}", mint_keypair.pubkey(), kp_a.pubkey(), kp_b.pubkey());
                let a = Packet::from_data(
                    None,
                    system_transaction::transfer(&mint_keypair, &kp_a.pubkey(), 10, genesis_config.hash()),
                ).unwrap();
                let b = Packet::from_data(
                    None,
                    system_transaction::transfer(&mint_keypair, &kp_b.pubkey(), 10, genesis_config.hash()),
                ).unwrap();
                let c = Packet::from_data(
                    None,
                    system_transaction::transfer(&kp_a, &kp_b.pubkey(), 1, genesis_config.hash()),
                ).unwrap();
                let bundle = Bundle {batch: PacketBatch::new(vec![a, b, c])};

                // Execute a bundle successfully to force cost model update
                assert_eq!(BundleStage::execute_bundle(
                    &cluster_info,
                    bundle,
                    &poh_recorder,
                    &recorder,
                    &None,
                    &gossip_vote_sender,
                    &qos_service,
                    &tip_manager,
                    &mut consensus_accounts_cache,
                    &mut last_consensus_update,
                ), Ok(()));
                info!("bank slot: {}", bank.slot());
                error!("bank: {:?}", bank.read_cost_tracker().unwrap());
                let expected_num_tx = bank.read_cost_tracker().unwrap().transaction_count();

                // Confirm we have non-zero expected txs
                assert_ne!(expected_num_tx, 0);

                let kp_c = Keypair::new();
                let kp_d = Keypair::new();
                let d = Packet::from_data(
                    None,
                    system_transaction::transfer(&mint_keypair, &kp_c.pubkey(), 10, genesis_config.hash()),
                ).unwrap();
                let e = Packet::from_data(
                    None,
                    system_transaction::transfer(&mint_keypair, &kp_d.pubkey(), 10, genesis_config.hash()),
                ).unwrap();
                // This tx should fail due to invalid blockhash
                let f = Packet::from_data(
                    None,
                    system_transaction::transfer(&kp_c, &kp_d.pubkey(), 10, Hash::new_unique()),
                ).unwrap();
                let bundle = Bundle {batch: PacketBatch::new(vec![d, e, f])};

                // Execute a bundle that partially fails to test rollback of QoS
                assert_eq!(BundleStage::execute_bundle(
                    &cluster_info,
                    bundle,
                    &poh_recorder,
                    &recorder,
                    &None,
                    &gossip_vote_sender,
                    &qos_service,
                    &tip_manager,
                    &mut consensus_accounts_cache,
                    &mut last_consensus_update,
                ), Err(TransactionFailure(BlockhashNotFound)));

                // Transaction count of cost tracker should be equivalent to before
                assert_eq!(expected_num_tx, bank.read_cost_tracker().unwrap().transaction_count());

                exit.store(true, Ordering::Relaxed);
                poh_service.join().unwrap();
            };
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
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
        let tx = Transaction::new(&[&mint_keypair, &kp], message, genesis_config.hash());
        let packet = Packet::from_data(
            None,
            tx,
        ).unwrap();

        let bundle = Bundle {batch: PacketBatch::new(vec![packet])};
        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(ExceedsCostModel));
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
            system_transaction::nonced_transfer(&mint_keypair, &kp_a.pubkey(), 1, &kp_nonce.pubkey(), &kp_nonce_authority, genesis_config.hash()),
        ).unwrap();
        let bundle = Bundle {batch: PacketBatch::new(vec![packet])};

        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(TransactionFailure(TransactionError::InstructionError(0, InstructionError::InvalidAccountData))));
    }

    #[test]
    fn test_invalid_blockhash() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);

        let kp_a = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::transfer(&mint_keypair, &kp_a.pubkey(), 1, Hash::new_unique()),
        ).unwrap();
        let bundle = Bundle {batch: PacketBatch::new(vec![packet])};

        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(TransactionFailure(BlockhashNotFound)));
    }

    #[test]
    fn test_zero_balance_fee_payer() { // TODO: this tests zero account not fee payer balance
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
        ).unwrap();

        let bundle = Bundle {batch: PacketBatch::new(vec![packet])};
        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(TransactionFailure(AccountNotFound)));
    }

    #[test]
    fn test_bundle_fails_poh_record() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);
        genesis_config.ticks_per_slot = 2; // Reduce ticks so that POH fails

        let kp_b = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::transfer(&mint_keypair, &kp_b.pubkey(), 1, genesis_config.hash()),
        ).unwrap();
        let bundle = Bundle {batch: PacketBatch::new(vec![packet])};
        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(PohMaxHeightError));
    }

    #[test]
    fn test_nonce_tx_success() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);


        let kp_a = Keypair::new();
        let kp_nonce = Keypair::new();

        let create_nonce_ix = create_nonce_account(&mint_keypair.pubkey(), &kp_nonce.pubkey(), &kp_a.pubkey(), 2);
        let mut create_nonce_tx = Transaction::new_with_payer(&create_nonce_ix, Some(&kp_a.pubkey()));
        create_nonce_tx.sign(&[&mint_keypair], genesis_config.hash());
        let create_nonce_packet = Packet::from_data(
            None,
            create_nonce_tx,
        ).unwrap();

        let packet = Packet::from_data(
            None,
            system_transaction::nonced_transfer(&mint_keypair, &kp_a.pubkey(), 1, &kp_nonce.pubkey(), &kp_a, Hash::new_unique()),
        ).unwrap();

        let bundle = Bundle {batch: PacketBatch::new(vec![create_nonce_packet])};

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let results = {
                // start a banking_stage to eat verified receiver
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
                let (exit, poh_recorder, poh_service, entry_receiver) =
                    create_test_recorder(&bank, &blockstore, Some(poh_config), None); // (2) (7)
                let cluster_info = ClusterInfo::new(
                    Node::new_localhost().info,
                    Arc::new(Keypair::new()),
                    SocketAddrSpace::Unspecified,
                );
                let cluster_info = Arc::new(cluster_info); // (1)
                let tip_manager = Arc::new(Mutex::new(TipManager::new(Keypair::new().pubkey())));
                let recorder = poh_recorder.lock().unwrap().recorder();
                let cost_model = Arc::new(RwLock::new(CostModel::default()));
                let qos_service = QosService::new(cost_model, 0);

                let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
                let mut last_consensus_update = Epoch::default();

                let results = BundleStage::execute_bundle(
                    &cluster_info,
                    bundle,
                    &poh_recorder,
                    &recorder,
                    &None,
                    &gossip_vote_sender,
                    &qos_service,
                    &tip_manager,
                    &mut consensus_accounts_cache,
                    &mut last_consensus_update,
                );
                exit.store(true, Ordering::Relaxed);
                poh_service.join().unwrap();
                results
            };
            assert_eq!(results, Ok(()));
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }

    #[test]
    fn test_tx_duplicates_fail() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);

        let kp = Keypair::new();
        let packet = Packet::from_data(
            None,
            system_transaction::transfer(&mint_keypair, &kp.pubkey(), 1, genesis_config.hash()),
        )
        .unwrap();
        let bundle = Bundle{
            batch: PacketBatch::new(vec![packet.clone(), packet]),
        };

        assert_eq!(test_single_bundle_failure(genesis_config, bundle), Err(InvalidBundle));
    }

    #[test]
    fn test_bundle_stage_happy_path() {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(4);
        genesis_config.ticks_per_slot *= 8; // Extend ticks so we don't run out in 1 slot test

        let (bundle_sender, bundle_receiver) = unbounded();

        // Create bundle with transaction chain mint->B->C
        let kp_b = Keypair::new();
        let kp_c = Keypair::new();
        let tx1 =
            system_transaction::transfer(&mint_keypair, &kp_b.pubkey(), 3, genesis_config.hash());
        let mut p1 = Packet::from_data(None, &tx1).unwrap();

        let tx2 = system_transaction::transfer(&kp_b, &kp_c.pubkey(), 2, genesis_config.hash());
        let mut p2 = Packet::from_data(None, &tx2).unwrap();

        bundle_sender.send(vec![Bundle {
            batch: PacketBatch::new(vec![p1, p2]),
        }]);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        {
            let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();

            let entry_receiver = {
                // start a banking_stage to eat verified receiver
                let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));
                let blockstore = Arc::new(
                    Blockstore::open(ledger_path.path())
                        .expect("Expected to be able to open database ledger"),
                );
                let poh_config = PohConfig {
                    // limit tick count to avoid clearing working_bank at
                    // PohRecord then PohRecorderError(MaxHeightReached) at BankingStage
                    target_tick_count: Some(bank.max_tick_height() - 1),
                    ..PohConfig::default()
                };
                let (exit, poh_recorder, poh_service, entry_receiver) =
                    create_test_recorder(&bank, &blockstore, Some(poh_config), None); // (2) (7)
                let cluster_info = ClusterInfo::new(
                    Node::new_localhost().info,
                    Arc::new(Keypair::new()),
                    SocketAddrSpace::Unspecified,
                );
                let cluster_info = Arc::new(cluster_info); // (1)
                let tip_manager = Arc::new(Mutex::new(TipManager::new(Keypair::new().pubkey())));
                let _bundle_stage = BundleStage::new(
                    &cluster_info,
                    &poh_recorder,
                    None,
                    gossip_vote_sender,
                    Arc::new(RwLock::new(CostModel::default())),
                    bundle_receiver,
                    exit.clone(),
                    tip_manager,
                );

                // wait for banking_stage to eat the packets
                while bank.get_balance(&kp_b.pubkey()) < 1 {
                    sleep(Duration::from_millis(10));
                }
                while bank.get_balance(&kp_c.pubkey()) < 1 {
                    sleep(Duration::from_millis(10));
                }
                exit.store(true, Ordering::Relaxed);
                poh_service.join().unwrap();
                entry_receiver // we return the entry_receiver here so we can drop everything else
                               // and have the banking_stage exit fully after record
            };
            drop(bundle_sender);

            // consume the entire entry_receiver
            let entries: Vec<_> = entry_receiver
                .iter()
                .flat_map(
                    |WorkingBankEntry {
                         bank: _,
                         entries_ticks,
                     }| entries_ticks.into_iter().map(|e| e.0),
                )
                .collect();

            // create a new bank to replay entries and confirm balances post transfer
            let bank = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
            for entry in entries {
                bank.process_entry_transactions(entry.transactions)
                    .iter()
                    .for_each(|x| assert_eq!(*x, Ok(())));
            }

            assert_eq!(bank.get_balance(&kp_b.pubkey()), 1);
            assert_eq!(bank.get_balance(&kp_c.pubkey()), 2);
        }
        Blockstore::destroy(ledger_path.path()).unwrap();
    }
}
