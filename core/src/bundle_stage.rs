//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::BatchedTransactionDetails,
        bundle::Bundle,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::{CommitTransactionDetails, QosService},
        tip_manager::TipManager,
        unprocessed_packet_batches::{self, *},
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure,
    solana_perf::packet::PacketBatch,
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
            TransactionBalancesSet, TransactionExecutionResult, TransactionResults,
        },
        bank_utils,
        cost_model::{CostModel, TransactionCost},
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        bpf_loader_upgradeable,
        bundle::{
            error::BundleExecutionError,
            utils::{check_bundle_lock_results, BundleExecutionResult},
        },
        clock::{Epoch, Slot, MAX_PROCESSING_AGE},
        feature_set,
        pubkey::Pubkey,
        saturating_add_assign,
        signature::Signer,
        transaction::{
            self, AddressLoader, MessageHash, SanitizedTransaction, Transaction, TransactionError,
            VersionedTransaction,
        },
    },
    solana_transaction_status::token_balances::{
        collect_balances_with_cache, collect_token_balances, TransactionTokenBalances,
        TransactionTokenBalancesSet,
    },
    std::{
        borrow::Cow,
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

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
        bundle_receiver: Receiver<Vec<Bundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: Arc<Mutex<TipManager>>,
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
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<Bundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: Arc<Mutex<TipManager>>,
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

    /// Executes a bundle, where all transactions in the bundle are executed all-or-nothing.
    ///
    /// Notes:
    /// - Transactions are streamed out in the form of entries as they're produced instead of
    /// all-at-once like other blockchains.
    /// - There might be multiple entries for a given bundle because there might be shared state that's
    /// written to in multiple transactions, which can't be parallelized.
    /// - We have some knowledge of when the slot will end, but it's not known how long ahead of
    /// time it'll take to execute a bundle.
    ///
    /// Assumptions:
    /// - Bundles are executed all-or-nothing. There shall never be a situation where the back half
    ///   of a bundle is dropped.
    /// - Bundles can contain any number of transactions, potentially from multiple signers.
    /// - Bundles are not executed across block boundaries.
    /// - All other non-vote pipelines are locked while bundles are running. (Note: this might
    ///   interfere with bundles that contain vote accounts bc the cache state might be incorrect).
    ///
    /// Given the above, there's a few things we need to do unique to normal transaction processing:
    /// - Execute all transactions in the bundle before sending to PoH.
    /// - In order to prevent being on a fork, we need to make sure that the transactions are
    ///   recorded to PoH successfully before committing them to the bank.
    /// - All of the records produced are batch sent to PoH and PoH will report back success only
    ///   if all transactions are recorded. If all transactions are recorded according to PoH, only
    ///   then will the transactions be committed to the bank.
    /// - When executing a transaction, the account state is loaded from accountsdb. Transaction n
    ///   might depend on state from transaction n-1, but that state isn't committed to the bank yet
    ///   because it hasn't run through poh yet. Therefore, we need to add the concept of an accounts
    ///   cache that saves the state of accounts from transaction n-1. When loading accounts in the
    ///   bundle execution stage, it'll bias towards loading from the cache to have the most recent
    ///   state.
    fn execute_bundle(
        cluster_info: &Arc<ClusterInfo>,
        bundle: Bundle,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &Arc<Mutex<TipManager>>,
        consensus_accounts_cache: &mut HashSet<Pubkey>,
        last_consensus_update: &mut Epoch,
    ) -> BundleExecutionResult<()> {
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let mut account_overrides = AccountOverrides {
            slot_history: None,
            cached_accounts_with_rent: HashMap::with_capacity(20),
        };

        let mut execution_results = Vec::new();
        let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

        let tip_program_id = tip_manager.lock().unwrap().program_id();
        let tip_pdas = tip_manager.lock().unwrap().get_tip_accounts();

        // ************************************************************************
        // Ensure validator is leader according to PoH
        // ************************************************************************
        let poh_recorder_bank = poh_recorder.lock().unwrap().get_poh_recorder_bank();
        let working_bank_start = poh_recorder_bank.working_bank_start();
        if PohRecorder::get_working_bank_if_not_expired(&working_bank_start).is_none() {
            return Err(BundleExecutionError::NotLeaderYet);
        }

        let BankStart {
            working_bank: bank,
            bank_creation_time,
        } = &*working_bank_start.unwrap();

        // ************************************************************************
        // Update consensus-related accounts on epoch boundaries to avoid
        // locking those accounts
        // ************************************************************************
        if bank.epoch() > last_consensus_update.to_owned() {
            *consensus_accounts_cache = Self::get_consensus_accounts(bank);
            *last_consensus_update = bank.epoch();
        }

        let transactions =
            Self::get_bundle_txs(&bundle, bank, &tip_program_id, consensus_accounts_cache);
        if transactions.is_empty() || bundle.batch.packets.len() != transactions.len() {
            return Err(BundleExecutionError::InvalidBundle);
        }

        // ************************************************************************
        // Quality-of-service and block size check
        // ************************************************************************
        let tx_costs = qos_service.compute_transaction_costs(transactions.iter());
        let (transactions_qos_results, num_included) =
            qos_service.select_transactions_per_cost(transactions.iter(), tx_costs.iter(), bank);

        // qos rate-limited a tx in here, drop the bundle
        if transactions.len().saturating_sub(num_included) > 0 {
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        qos_service.accumulate_estimated_transaction_costs(
            &Self::accumulate_batched_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
            ),
        );

        if Self::bundle_touches_tip_pdas(&transactions, &tip_pdas) {
            // NOTE: ensure that tip_manager locked while TX is executing to avoid any race conditions with bundle_stage
            // writing to the account
            let tip_manager_l = tip_manager.lock().unwrap();
            // TODO: remove initializing the tip program when done testing
            Self::maybe_initialize_config_account(
                bank,
                &tip_manager_l,
                qos_service,
                recorder,
                transaction_status_sender,
                &mut execute_and_commit_timings,
                gossip_vote_sender,
                cluster_info,
            )?;
            Self::maybe_change_tip_receiver(
                bank,
                &tip_manager_l,
                qos_service,
                recorder,
                transaction_status_sender,
                &mut execute_and_commit_timings,
                gossip_vote_sender,
                cluster_info,
            )?;
        }

        let mut chunk_start = 0;
        while chunk_start != transactions.len() {
            if !Bank::should_bank_still_be_processing_txs(bank_creation_time, bank.ns_per_slot) {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
                return Err(BundleExecutionError::PohMaxHeightError);
            }

            // ************************************************************************
            // Build a TransactionBatch that ensures transactions in the bundle
            // are executed sequentially.
            // ************************************************************************
            let chunk_end = std::cmp::min(transactions.len(), chunk_start + 128);
            let chunk = &transactions[chunk_start..chunk_end];
            let batch = bank.prepare_sequential_sanitized_batch_with_results(chunk, None);
            if let Some((e, _)) = check_bundle_lock_results(batch.lock_results()) {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
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

            if let Err((e, _)) = TransactionExecutionResult::check_bundle_execution_results(
                load_and_execute_transactions_output
                    .execution_results
                    .as_slice(),
                batch.sanitized_transactions(),
            ) {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
                return Err(e);
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

        assert!(!execution_results.is_empty());

        // *********************************************************************************
        // All transactions are executed in the bundle.
        // Record to PoH and send the saved execution results to the Bank.
        // Note: Ensure that bank.commit_transactions is called on a per-batch basis and
        // not all together
        // *********************************************************************************

        let (freeze_lock, _freeze_lock_time) = measure!(bank.freeze_lock(), "freeze_lock");

        let record = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        Self::try_record(recorder, record).map_err(|e| {
            QosService::remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                bank,
            );
            e
        })?;

        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            // TODO: @buffalu_ double check: e66ea7cb6ac1b9881b11c81ecfec287af6a59754
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

            let commit_transaction_statuses = transaction_results
                .execution_results
                .iter()
                .map(|tx_results| match tx_results.details() {
                    Some(details) => CommitTransactionDetails::Committed {
                        compute_units: details.executed_units,
                    },
                    None => CommitTransactionDetails::NotCommitted,
                })
                .collect();

            QosService::update_or_remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                Some(&commit_transaction_statuses),
                bank,
            );
            let (cu, us) = Self::accumulate_execute_units_and_time(
                &execute_and_commit_timings.execute_timings,
            );
            qos_service.accumulate_actual_execute_cu(cu);
            qos_service.accumulate_actual_execute_time(us);
        }

        // reports qos service stats for this batch
        qos_service.report_metrics(bank.clone());

        drop(freeze_lock);

        Ok(())
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

    fn maybe_initialize_config_account(
        bank: &Arc<Bank>,
        tip_manager_l: &TipManager,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        gossip_vote_sender: &ReplayVoteSender,
        cluster_info: &Arc<ClusterInfo>,
    ) -> BundleExecutionResult<()> {
        if bank.get_account(&tip_manager_l.config_pubkey()).is_none() {
            let init_tx =
                tip_manager_l.build_initialize_tx(&bank.last_blockhash(), &cluster_info.keypair());
            if let Err(e) = Self::execute_record_and_commit(
                init_tx,
                bank,
                qos_service,
                recorder,
                transaction_status_sender,
                execute_and_commit_timings,
                gossip_vote_sender,
            ) {
                warn!("error initializing the tip program!!! error: {}", e);
                return Err(e);
            } else {
                info!("initialized tip program");
            }
        }
        Ok(())
    }

    fn maybe_change_tip_receiver(
        bank: &Arc<Bank>,
        tip_manager_l: &TipManager,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        gossip_vote_sender: &ReplayVoteSender,
        cluster_info: &Arc<ClusterInfo>,
    ) -> BundleExecutionResult<()> {
        let current_tip_receiver = tip_manager_l.get_current_tip_receiver(bank)?;
        let my_kp = cluster_info.keypair();

        if current_tip_receiver != my_kp.pubkey() {
            match tip_manager_l.build_change_tip_receiver_tx(&my_kp.pubkey(), bank, &my_kp) {
                Ok(tx) => {
                    if let Err(e) = Self::execute_record_and_commit(
                        tx,
                        bank,
                        qos_service,
                        recorder,
                        transaction_status_sender,
                        execute_and_commit_timings,
                        gossip_vote_sender,
                    ) {
                        warn!("error changing tip receiver!!! error: {}", e);
                        return Err(e);
                    } else {
                        info!("tip receiver changed!")
                    }
                }
                Err(e) => {
                    error!("error build tip tx! error: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Executes transactions, records them to PoH, and commits them to the bank
    fn execute_record_and_commit(
        tx: Transaction,
        bank: &Arc<Bank>,
        qos_service: &QosService,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        gossip_vote_sender: &ReplayVoteSender,
    ) -> BundleExecutionResult<()> {
        let account_overrides = AccountOverrides::default();
        let mut mint_decimals = HashMap::new();

        let sanitized_txs = vec![SanitizedTransaction::try_create(
            VersionedTransaction::from(tx),
            MessageHash::Compute,
            None,
            bank.as_ref(),
            bank.feature_set
                .is_active(&feature_set::require_static_program_ids_in_transaction::ID),
        )?];

        let tx_costs = qos_service.compute_transaction_costs(sanitized_txs.iter());
        let (transactions_qos_results, num_included) =
            qos_service.select_transactions_per_cost(sanitized_txs.iter(), tx_costs.iter(), bank);
        if sanitized_txs.len().saturating_sub(num_included) > 0 {
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        qos_service.accumulate_estimated_transaction_costs(
            &Self::accumulate_batched_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
            ),
        );

        let batch = TransactionBatch::new(vec![Ok(())], bank, Cow::from(sanitized_txs));

        let (pre_balances, _) = measure!(
            Self::collect_balances(
                bank,
                &batch,
                &account_overrides,
                transaction_status_sender,
                &mut mint_decimals,
            ),
            "collect_balances",
        );

        let (load_and_execute_tx_output, load_execute_time) = measure!(
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

        if let Err((e, _)) = TransactionExecutionResult::check_bundle_execution_results(
            load_and_execute_tx_output.execution_results.as_slice(),
            batch.sanitized_transactions(),
        ) {
            QosService::remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                bank,
            );
            return Err(e);
        }

        let (post_balances, _) = measure!(
            Self::collect_balances(
                bank,
                &batch,
                &account_overrides,
                transaction_status_sender,
                &mut mint_decimals,
            ),
            "collect_balances",
        );

        let execution_results = vec![AllExecutionResults {
            load_and_execute_tx_output,
            sanitized_txs: batch.sanitized_transactions().to_vec(),
            pre_balances,
            post_balances,
        }];

        Self::record_and_commit_and_handle_qos(
            bank,
            execution_results,
            recorder,
            execute_and_commit_timings,
            transaction_status_sender,
            gossip_vote_sender,
            tx_costs,
            transactions_qos_results,
            qos_service,
        )?;

        Ok(())
    }

    fn record_and_commit_and_handle_qos(
        bank: &Arc<Bank>,
        execution_results: Vec<AllExecutionResults>,
        recorder: &TransactionRecorder,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        tx_costs: Vec<TransactionCost>,
        transactions_qos_results: Vec<transaction::Result<()>>,
        qos_service: &QosService,
    ) -> BundleExecutionResult<()> {
        match Self::record_and_commit_results(
            bank,
            execution_results,
            recorder,
            execute_and_commit_timings,
            transaction_status_sender,
            gossip_vote_sender,
        ) {
            Err(e) => {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
                return Err(e);
            }
            Ok(transaction_results) => {
                transaction_results.iter().for_each(|results| {
                    let commit_transaction_statuses = results
                        .execution_results
                        .iter()
                        .map(|tx_results| match tx_results.details() {
                            Some(details) => CommitTransactionDetails::Committed {
                                compute_units: details.executed_units,
                            },
                            None => CommitTransactionDetails::NotCommitted,
                        })
                        .collect();
                    QosService::update_or_remove_transaction_costs(
                        tx_costs.iter(),
                        transactions_qos_results.iter(),
                        Some(&commit_transaction_statuses),
                        bank,
                    );
                    let (cu, us) = Self::accumulate_execute_units_and_time(
                        &execute_and_commit_timings.execute_timings,
                    );
                    qos_service.accumulate_actual_execute_cu(cu);
                    qos_service.accumulate_actual_execute_time(us);
                });
                qos_service.report_metrics(bank.clone());
            }
        }
        Ok(())
    }

    fn record_and_commit_results(
        bank: &Arc<Bank>,
        execution_results: Vec<AllExecutionResults>,
        recorder: &TransactionRecorder,
        execute_and_commit_timings: &mut LeaderExecuteAndCommitTimings,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
    ) -> BundleExecutionResult<Vec<TransactionResults>> {
        let (freeze_lock, _freeze_lock_time) = measure!(bank.freeze_lock(), "freeze_lock");

        let record = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        Self::try_record(recorder, record)?;

        let mut transaction_results = Vec::new();
        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            // TODO: @buffalu_ double check, see above
            let (last_blockhash, lamports_per_signature) =
                bank.last_blockhash_and_lamports_per_signature();
            let results = bank.commit_transactions(
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
                        &results,
                        Some(gossip_vote_sender),
                    );
                    if let Some(transaction_status_sender) = transaction_status_sender {
                        transaction_status_sender.send_transaction_status_batch(
                            bank.clone(),
                            sanitized_txs,
                            output.execution_results,
                            TransactionBalancesSet::new(r.pre_balances.0, r.post_balances.0),
                            TransactionTokenBalancesSet::new(r.pre_balances.1, r.post_balances.1),
                            results.rent_debits.clone(),
                        );
                    }
                },
                "find_and_send_votes",
            );
            transaction_results.push(results);
        }

        drop(freeze_lock);
        Ok(transaction_results)
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

    fn bundle_stage(
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_receiver: Receiver<Vec<Bundle>>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
        exit: Arc<AtomicBool>,
        tip_manager: Arc<Mutex<TipManager>>,
    ) {
        let recorder = poh_recorder.lock().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        let mut consensus_accounts_cache: HashSet<Pubkey> = HashSet::new();
        let mut last_consensus_update = Epoch::default();

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            let bundles = {
                match bundle_receiver.recv_timeout(Duration::from_millis(100)) {
                    Ok(bundle) => bundle,
                    Err(RecvTimeoutError::Timeout) => {
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        break;
                    }
                }
            };

            for bundle in bundles {
                match Self::execute_bundle(
                    &cluster_info,
                    bundle,
                    poh_recorder,
                    &recorder,
                    &transaction_status_sender,
                    &gossip_vote_sender,
                    &qos_service,
                    &tip_manager,
                    &mut consensus_accounts_cache,
                    &mut last_consensus_update,
                ) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("error recording bundle {:?}", e);
                    }
                }
            }
        }
    }

    fn get_consensus_accounts(bank: &Arc<Bank>) -> HashSet<Pubkey> {
        let mut consensus_accounts: HashSet<Pubkey> = HashSet::new();
        bank.epoch_stakes(bank.epoch()).map(|epoch_stakes| {
            // votes use the following accounts:
            // - vote_account pubkey: writeable
            // - authorized_voter_pubkey: read-only
            // - node_keypair pubkey: payer (writeable)
            let node_id_vote_accounts = epoch_stakes.node_id_to_vote_accounts();

            let vote_accounts: Vec<Pubkey> = node_id_vote_accounts
                .values()
                .into_iter()
                .map(|v| v.vote_accounts.clone())
                .flatten()
                .collect();

            // vote_account
            consensus_accounts.extend(vote_accounts.into_iter());
            // authorized_voter_pubkey
            consensus_accounts.extend(epoch_stakes.epoch_authorized_voters().keys().into_iter());
            // node_keypair
            consensus_accounts.extend(epoch_stakes.node_id_to_vote_accounts().keys().into_iter());
        });
        consensus_accounts
    }

    fn get_bundle_txs(
        bundle: &Bundle,
        bank: &Arc<Bank>,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &mut HashSet<Pubkey>,
    ) -> Vec<SanitizedTransaction> {
        let packet_indexes = Self::generate_packet_indexes(&bundle.batch);
        let deserialized_packets =
            unprocessed_packet_batches::deserialize_packets(&bundle.batch, &packet_indexes);
        deserialized_packets
            .filter_map(|p| {
                let immutable_packet = p.immutable_section().clone();
                Self::transaction_from_deserialized_packet(
                    &immutable_packet,
                    &bank.feature_set,
                    bank.vote_only_bank(),
                    bank.as_ref(),
                    tip_program_id,
                    &consensus_accounts_cache,
                )
            })
            .collect()
    }

    fn generate_packet_indexes(batch: &PacketBatch) -> Vec<usize> {
        batch
            .iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
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

    // This function deserializes packets into transactions, computes the blake3 hash of transaction
    // messages, and verifies secp256k1 instructions. A list of sanitized transactions are returned
    // with their packet indexes.
    #[allow(clippy::needless_collect)]
    fn transaction_from_deserialized_packet(
        deserialized_packet: &ImmutableDeserializedPacket,
        feature_set: &Arc<feature_set::FeatureSet>,
        votes_only: bool,
        address_loader: impl AddressLoader,
        tip_program_id: &Pubkey,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Option<SanitizedTransaction> {
        if votes_only && !deserialized_packet.is_simple_vote() {
            return None;
        }

        let tx = SanitizedTransaction::try_new(
            deserialized_packet.transaction().clone(),
            *deserialized_packet.message_hash(),
            deserialized_packet.is_simple_vote(),
            address_loader,
        )
        .ok()?;
        tx.verify_precompiles(feature_set).ok()?;

        // Prevent transactions from mentioning the tip program to avoid getting the tip_receiver
        // changed mid-slot and the rest of the tips stolen.
        // NOTE: if this is a weak assumption helpful for testing deployment,
        // before production it shall only be the tip program
        let tx_accounts = tx.message().account_keys();
        if tx_accounts.iter().any(|a| a == tip_program_id)
            && !tx_accounts
                .iter()
                .any(|a| a == &bpf_loader_upgradeable::id())
        {
            warn!("someone attempted to change the tip program!! tx: {:?}", tx);
            return None;
        }

        // NOTE: may want to revisit this as it may reduce use cases of locking accounts
        // used for legitimate cases in bundles.
        if tx_accounts
            .iter()
            .any(|a| consensus_accounts_cache.contains(a))
        {
            warn!(
                "someone attempted to lock a consensus related account!! tx: {:?}",
                tx
            );
            return None;
        }

        Some(tx)
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
