//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use crate::qos_service::CommitTransactionDetails;
use solana_program_runtime::timings::ExecuteTimings;
use {
    crate::{
        banking_stage::BatchedTransactionDetails,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::QosService, unprocessed_packet_batches::*,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_entry::entry::hash_transactions,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure::Measure,
    solana_mev::bundle::Bundle,
    solana_perf::{
        cuda_runtime::PinnedVec,
        packet::{limited_deserialize, Packet, PacketBatch},
    },
    solana_poh::poh_recorder::{
        BankStart, PohRecorder,
        PohRecorderError::{self},
        Record, TransactionRecorder,
    },
    solana_runtime::{
        account_overrides::AccountOverrides,
        accounts::TransactionLoadResult,
        bank::{
            Bank, LoadAndExecuteTransactionsOutput, TransactionBalances, TransactionBalancesSet,
            TransactionExecutionResult,
        },
        bank_utils,
        cost_model::{CostModel, TransactionCost},
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE},
        feature_set,
        message::Message,
        pubkey::Pubkey,
        saturating_add_assign,
        transaction::{
            self, AddressLoader, SanitizedTransaction, TransactionError, VersionedTransaction,
        },
    },
    solana_transaction_status::token_balances::{
        collect_balances_with_cache, collect_token_balances, TransactionTokenBalances,
        TransactionTokenBalancesSet,
    },
    std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    thiserror::Error,
};

#[derive(Error, Debug, Clone)]
pub enum BundleExecutionError {
    #[error("Bank is not processing transactions.")]
    BankNotProcessingTransactions,

    #[error("Bundle is invalid")]
    InvalidBundle,

    #[error("PoH max height reached in the middle of a bundle.")]
    PohError(#[from] PohRecorderError),

    #[error("No records to record to PoH")]
    NoRecordsToRecord,

    #[error("A transaction in the bundle failed")]
    TransactionFailure(#[from] TransactionError),

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,

    #[error("The validator is not a leader yet, dropping")]
    NotLeaderYet,
}

type BundleExecutionResult<T> = std::result::Result<T, BundleExecutionError>;

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
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Bundle>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        Self::start_bundle_thread(
            poh_recorder,
            transaction_status_sender,
            gossip_vote_sender,
            cost_model,
            bundle_receiver,
            exit,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Bundle>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let poh_recorder = poh_recorder.clone();

        let bundle_thread = Builder::new()
            .name("solana-bundle-stage".to_string())
            .spawn(move || {
                let transaction_status_sender = transaction_status_sender.clone();
                Self::bundle_stage(
                    &poh_recorder,
                    transaction_status_sender,
                    bundle_receiver,
                    gossip_vote_sender,
                    0,
                    cost_model,
                    exit,
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
        bundle: Bundle,
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
    ) -> BundleExecutionResult<()> {
        let mut chunk_start = 0;

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();
        let mut cached_accounts = AccountOverrides {
            slot_history: None,
            cached_accounts_with_rent: HashMap::with_capacity(20),
        };

        let mut execution_results = Vec::new();
        let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

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

        let transactions = Self::get_bundle_txs(&bundle, bank);
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

        while chunk_start != transactions.len() {
            if !Bank::should_bank_still_be_processing_txs(bank_creation_time, bank.ns_per_slot) {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
                return Err(PohRecorderError::MaxHeightReached.into());
            }

            // ************************************************************************
            // Build a TransactionBatch that ensures transactions in the bundle
            // are executed sequentially.
            // ************************************************************************
            let chunk_end = std::cmp::min(transactions.len(), chunk_start + 128);
            let chunk = &transactions[chunk_start..chunk_end];
            let batch = bank.prepare_sequential_sanitized_batch_with_results(chunk);
            if let Err(e) = Self::check_bundle_batch_ok(&batch) {
                QosService::remove_transaction_costs(
                    tx_costs.iter(),
                    transactions_qos_results.iter(),
                    bank,
                );
                return Err(e);
            }

            let ((pre_balances, pre_token_balances), _) = Measure::this(
                |_| {
                    Self::collect_balances(
                        bank,
                        &batch,
                        &cached_accounts,
                        transaction_status_sender,
                        &mut mint_decimals,
                    )
                },
                (),
                "collect_balances",
            );

            let (mut load_and_execute_transactions_output, load_execute_time) = Measure::this(
                |_| {
                    bank.load_and_execute_transactions(
                        &batch,
                        MAX_PROCESSING_AGE,
                        transaction_status_sender.is_some(),
                        transaction_status_sender.is_some(),
                        transaction_status_sender.is_some(),
                        &mut execute_and_commit_timings.execute_timings,
                        Some(&cached_accounts),
                    )
                },
                (),
                "load_execute",
            );
            execute_and_commit_timings.load_execute_us = load_execute_time.as_us();

            if let Err(e) = Self::check_all_executed_ok(
                &load_and_execute_transactions_output
                    .execution_results
                    .as_slice(),
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
                &batch.sanitized_transactions(),
                &load_and_execute_transactions_output.execution_results,
                &mut load_and_execute_transactions_output.loaded_transactions,
                &mut cached_accounts,
            );

            let ((post_balances, post_token_balances), _) = Measure::this(
                |_| {
                    Self::collect_balances(
                        bank,
                        &batch,
                        &cached_accounts,
                        transaction_status_sender,
                        &mut mint_decimals,
                    )
                },
                (),
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

        let (freeze_lock, _freeze_lock_time) =
            Measure::this(|_| bank.freeze_lock(), (), "freeze_lock");

        let record = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        if let Err(e) = recorder.record(record) {
            QosService::remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                bank,
            );
            return Err(e.into());
        }

        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            let transaction_results = bank.commit_transactions(
                &sanitized_txs,
                &mut output.loaded_transactions,
                output.execution_results.clone(),
                output.executed_transactions_count as u64,
                output
                    .executed_transactions_count
                    .saturating_sub(output.executed_with_successful_result_count)
                    as u64,
                output.signature_count,
                &mut execute_and_commit_timings.execute_timings,
            );

            let (_, _) = Measure::this(
                |_| {
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
                (),
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
        // info!("caching accounts {:?}", accounts);
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
            let balances = collect_balances_with_cache(&batch, bank, Some(&cached_accounts));
            let token_balances =
                collect_token_balances(bank, &batch, mint_decimals, Some(&cached_accounts));
            (balances, token_balances)
        } else {
            (vec![], vec![])
        }
    }

    /// Return an Error if a transaction wasn't executed
    fn check_all_executed_ok(
        execution_results: &[TransactionExecutionResult],
    ) -> BundleExecutionResult<()> {
        let maybe_err = execution_results
            .iter()
            .find(|er| er.was_executed() && !er.was_executed_successfully());
        if let Some(TransactionExecutionResult::Executed(details)) = maybe_err {
            match &details.status {
                Ok(_) => {
                    unreachable!();
                }
                Err(e) => return Err(e.clone().into()),
            }
        }
        Ok(())
    }

    /// Checks that preparing a bundle gives an acceptable batch back
    fn check_bundle_batch_ok(batch: &TransactionBatch) -> BundleExecutionResult<()> {
        for r in batch.lock_results() {
            match r {
                Ok(())
                | Err(TransactionError::AccountInUse)
                | Err(TransactionError::BundleNotContinuous) => {}
                Err(e) => {
                    return Err(e.clone().into());
                }
            }
        }
        Ok(())
    }

    fn bundle_stage(
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_receiver: Receiver<Bundle>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
        exit: Arc<AtomicBool>,
    ) {
        let recorder = poh_recorder.lock().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            let bundle = {
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

            match Self::execute_bundle(
                bundle,
                poh_recorder,
                &recorder,
                &transaction_status_sender,
                &gossip_vote_sender,
                &qos_service,
            ) {
                Ok(_) => {
                    info!("bundle processed ok");
                }
                Err(e) => {
                    error!("error recording bundle {:?}", e);
                }
            }
        }
    }

    fn get_bundle_txs(bundle: &Bundle, bank: &Arc<Bank>) -> Vec<SanitizedTransaction> {
        let packet_indexes = Self::generate_packet_indexes(&bundle.batch.packets);
        let (transactions, _) = Self::transactions_from_packets(
            &bundle.batch,
            &packet_indexes,
            &bank.feature_set,
            bank.vote_only_bank(),
            bank.as_ref(),
        );
        transactions
    }

    fn generate_packet_indexes(vers: &PinnedVec<Packet>) -> Vec<usize> {
        vers.iter()
            .enumerate()
            .filter(|(_, pkt)| !pkt.meta.discard())
            .map(|(index, _)| index)
            .collect()
    }

    fn prepare_poh_record_bundle(
        bank_slot: &Slot,
        execution_results_txs: &Vec<AllExecutionResults>,
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
    fn transactions_from_packets(
        packet_batch: &PacketBatch,
        transaction_indexes: &[usize],
        feature_set: &Arc<feature_set::FeatureSet>,
        votes_only: bool,
        address_loader: impl AddressLoader,
    ) -> (Vec<SanitizedTransaction>, Vec<usize>) {
        transaction_indexes
            .iter()
            .filter_map(|tx_index| {
                let p = &packet_batch.packets[*tx_index];
                if votes_only && !p.meta.is_simple_vote_tx() {
                    return None;
                }

                let tx: VersionedTransaction = limited_deserialize(&p.data[0..p.meta.size]).ok()?;
                let message_bytes = DeserializedPacketBatch::packet_message(p)?;
                let message_hash = Message::hash_raw_message(message_bytes);
                let tx = SanitizedTransaction::try_create(
                    tx,
                    message_hash,
                    Some(p.meta.is_simple_vote_tx()),
                    address_loader.clone(),
                )
                .ok()?;
                tx.verify_precompiles(feature_set).ok()?;
                Some((tx, *tx_index))
            })
            .unzip()
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }
}
