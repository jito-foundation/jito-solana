//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::BatchedTransactionDetails,
        leader_slot_banking_stage_metrics::LeaderSlotMetricsTracker,
        leader_slot_banking_stage_timing_metrics::LeaderExecuteAndCommitTimings,
        qos_service::QosService, unprocessed_packet_batches::*,
    },
    solana_entry::entry::hash_transactions,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure::Measure,
    solana_mev::{bundle::Bundle, bundle_scheduler::BundleScheduler},
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
        sync::{Arc, Mutex, RwLock},
        thread::{self, sleep, Builder, JoinHandle},
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Error, Debug, Clone)]
pub enum BundleExecutionError {
    #[error("Bank is not processing transactions.")]
    BankNotProcessingTransactions,

    #[error("PoH max height reached in the middle of a bundle.")]
    PohError(#[from] PohRecorderError),

    #[error("No records to record to PoH")]
    NoRecordsToRecord,

    #[error("A transaction in the bundle failed")]
    TransactionFailure(#[from] TransactionError),

    #[error("The bundle exceeds the cost model")]
    ExceedsCostModel,
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
        bundle_scheduler: Arc<Mutex<BundleScheduler>>,
    ) -> Self {
        Self::start_bundle_thread(
            poh_recorder,
            transaction_status_sender,
            gossip_vote_sender,
            cost_model,
            bundle_scheduler,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_scheduler: Arc<Mutex<BundleScheduler>>,
    ) -> Self {
        let poh_recorder = poh_recorder.clone();

        let bundle_thread = Builder::new()
            .name("solana-bundle-stage".to_string())
            .spawn(move || {
                let transaction_status_sender = transaction_status_sender.clone();
                Self::bundle_stage(
                    &poh_recorder,
                    transaction_status_sender,
                    bundle_scheduler,
                    gossip_vote_sender,
                    0,
                    cost_model,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    fn get_transaction_qos_results(
        qos_service: &QosService,
        txs: &[SanitizedTransaction],
        bank: &Arc<Bank>,
    ) -> (Vec<Result<(), TransactionError>>, usize) {
        let tx_costs = qos_service.compute_transaction_costs(txs.iter());

        let (transactions_qos_results, num_included) =
            qos_service.select_transactions_per_cost(txs.iter(), tx_costs.iter(), bank);

        let cost_model_throttled_transactions_count = txs.len().saturating_sub(num_included);

        qos_service.accumulate_estimated_transaction_costs(
            &Self::accumulate_batched_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
            ),
        );
        (
            transactions_qos_results,
            cost_model_throttled_transactions_count,
        )
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

    /// Returns the bundle batch, where a batch must be a list of transactions locked sequentially
    /// and contiguously. If there are any irrecoverable errors that would result in a piece of the
    /// bundle being dropped, drop the entire thing.
    ///
    /// Assumptions:
    /// - transactions_qos_results is all Ok(()) since anything that exceeds block limit in a bundle
    ///   will cause the entire bundle to be dropped.
    /// - AccountLoadedTwice or TooManyAccountLocks are irrecoverable errors and the entire bundle
    ///   should be dropped. Ideally this is caught upstream of here too.
    fn get_bundle_batch<'a>(
        bank: &'a Arc<Bank>,
        chunk: &'a [SanitizedTransaction],
        transactions_qos_results: Vec<Result<(), TransactionError>>,
    ) -> BundleExecutionResult<TransactionBatch<'a, 'a>> {
        // lock results can be: Ok, AccountInUse, BundleNotContinuous, AccountLoadedTwice, or TooManyAccountLocks
        // AccountLoadedTwice and TooManyAccountLocks are irrecoverable errors
        // AccountInUse and BundleNotContinuous are acceptable
        let batch = bank.prepare_sequential_sanitized_batch_with_results(
            chunk,
            transactions_qos_results.into_iter(),
        );

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
        Ok(batch)
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
        transactions: Vec<SanitizedTransaction>,
        bank: &Arc<Bank>,
        bank_creation_time: &Arc<Instant>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        _slot_metrics_tracker: &LeaderSlotMetricsTracker,
    ) -> BundleExecutionResult<()> {
        let mut chunk_start = 0;
        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        let mut cached_accounts = AccountOverrides {
            slot_history: None,
            cached_accounts_with_rent: HashMap::with_capacity(20),
        };

        let mut execution_results = Vec::new();
        let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

        while chunk_start != transactions.len() {
            if !Bank::should_bank_still_be_processing_txs(bank_creation_time, bank.ns_per_slot) {
                return Err(PohRecorderError::MaxHeightReached.into());
            }

            // *********************************************************************************
            // Prepare batch for execution
            // *********************************************************************************
            let chunk_end = std::cmp::min(transactions.len(), chunk_start + 128);
            let chunk = &transactions[chunk_start..chunk_end];

            let (
                (transactions_qos_results, cost_model_throttled_transactions_count),
                _cost_model_time,
            ) = Measure::this(
                |_| Self::get_transaction_qos_results(qos_service, chunk, bank),
                (),
                "cost_model",
            );

            if cost_model_throttled_transactions_count > 0 {
                return Err(BundleExecutionError::ExceedsCostModel);
            }

            // NOTE: accounts are unlocked when batch is dropped (if method exits out early)
            let batch = Self::get_bundle_batch(bank, chunk, transactions_qos_results)?;
            Self::check_bundle_batch_ok(&batch)?;

            let processing_end = batch.lock_results().iter().position(|lr| lr.is_err());
            if let Some(end) = processing_end {
                chunk_start += end;
            } else {
                chunk_start = chunk_end;
            }

            // *********************************************************************************
            // Execute batch
            // *********************************************************************************

            let ((pre_balances, pre_token_balances), _) = Measure::this(
                |_| {
                    // Use a shorter maximum age when adding transactions into the pipeline.  This will reduce
                    // the likelihood of any single thread getting starved and processing old ids.
                    // TODO: Banking stage threads should be prioritized to complete faster then this queue
                    // expires.
                    let pre_balances = if transaction_status_sender.is_some() {
                        collect_balances_with_cache(&batch, bank, Some(&cached_accounts))
                    } else {
                        vec![]
                    };

                    let pre_token_balances = if transaction_status_sender.is_some() {
                        collect_token_balances(
                            bank,
                            &batch,
                            &mut mint_decimals,
                            Some(&cached_accounts),
                        )
                    } else {
                        vec![]
                    };

                    (pre_balances, pre_token_balances)
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

            // TODO (LB): rollback QoS for non-executed transactions

            Self::check_all_executed_ok(
                &load_and_execute_transactions_output
                    .execution_results
                    .as_slice(),
            )?;

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
                    let pre_balances = if transaction_status_sender.is_some() {
                        collect_balances_with_cache(&batch, bank, Some(&cached_accounts))
                    } else {
                        vec![]
                    };

                    let pre_token_balances = if transaction_status_sender.is_some() {
                        collect_token_balances(
                            bank,
                            &batch,
                            &mut mint_decimals,
                            Some(&cached_accounts),
                        )
                    } else {
                        vec![]
                    };

                    (pre_balances, pre_token_balances)
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
            drop(batch);
        }

        if execution_results.is_empty() {
            return Err(BundleExecutionError::NoRecordsToRecord);
        }

        // *********************************************************************************
        // All transactions are executed in the bundle.
        // Record to PoH and send the saved execution results to the Bank.
        // Note: Ensure that bank.commit_transactions is called on a per-batch basis and
        // not all together
        // *********************************************************************************

        let (freeze_lock, _freeze_lock_time) =
            Measure::this(|_| bank.freeze_lock(), (), "freeze_lock");

        let record = Self::prepare_poh_record_bundle(&bank.slot(), &execution_results);
        recorder.record(record)?;

        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            let commit_result = bank.commit_transactions(
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
                        &commit_result,
                        Some(gossip_vote_sender),
                    );
                    if let Some(transaction_status_sender) = transaction_status_sender {
                        transaction_status_sender.send_transaction_status_batch(
                            bank.clone(),
                            sanitized_txs,
                            output.execution_results,
                            TransactionBalancesSet::new(r.pre_balances.0, r.post_balances.0),
                            TransactionTokenBalancesSet::new(r.pre_balances.1, r.post_balances.1),
                            commit_result.rent_debits,
                        );
                    }
                },
                (),
                "find_and_send_votes",
            );
        }

        drop(freeze_lock);

        Ok(())
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

    /// Return an Error if a transaction was executed
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
        // Ok, AccountInUse, BundleNotContinuous, AccountLoadedTwice, or TooManyAccountLocks
        let maybe_err = batch.lock_results().iter().find(|lr| {
            !matches!(
                lr,
                Ok(())
                    | Err(TransactionError::AccountInUse)
                    | Err(TransactionError::BundleNotContinuous)
            )
        });
        if let Some(Err(e)) = maybe_err {
            Err(e.clone().into())
        } else {
            Ok(())
        }
    }

    fn bundle_stage(
        poh_recorder: &Arc<Mutex<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_scheduler: Arc<Mutex<BundleScheduler>>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
    ) {
        let recorder = poh_recorder.lock().unwrap().recorder();
        let slot_metrics_tracker = LeaderSlotMetricsTracker::new(id);
        let qos_service = QosService::new(cost_model, id);

        loop {
            let bundle = {
                if let Some(bundle) = bundle_scheduler.lock().unwrap().pop() {
                    bundle
                } else {
                    sleep(Duration::from_millis(1));
                    continue;
                }
            };
            let bundles = vec![bundle];

            // Skip execution of bundles if not leader yet
            let poh_recorder_bank = poh_recorder.lock().unwrap().get_poh_recorder_bank();
            let working_bank_start = poh_recorder_bank.working_bank_start();
            if PohRecorder::get_working_bank_if_not_expired(&working_bank_start).is_none() {
                error!(
                    "dropping bundle, not leader yet [num_bundles={}]",
                    bundles.len()
                );
                continue;
            }

            let BankStart {
                working_bank,
                bank_creation_time,
            } = &*working_bank_start.unwrap();

            // Process + send one bundle at a time
            // TODO (LB): probably want to lock all the other tx procesing pipelines (except votes) until this is done
            for bundle in bundles {
                let bundle_txs = Self::get_bundle_txs(bundle.clone(), working_bank);
                match Self::execute_bundle(
                    bundle_txs,
                    working_bank,
                    bank_creation_time,
                    &recorder,
                    &transaction_status_sender,
                    &gossip_vote_sender,
                    &qos_service,
                    &slot_metrics_tracker,
                ) {
                    Ok(_) => {
                        info!("bundle processed ok");
                    }
                    Err(BundleExecutionError::BankNotProcessingTransactions) => {
                        error!("bank not processing txs");
                    }
                    Err(BundleExecutionError::PohError(err)) => {
                        error!("poh err: {:?}", err);
                        // TODO (LB): might need bundles to be re-staged? TBD
                        if matches!(err, PohRecorderError::MaxHeightReached) {
                            error!("dropping the rest of the bundles");
                            break;
                        }
                    }
                    Err(BundleExecutionError::NoRecordsToRecord) => {
                        error!("no records to record");
                    }
                    Err(BundleExecutionError::TransactionFailure(e)) => {
                        error!("transaction in bundle failed to execute: {}", e);
                    }
                    Err(BundleExecutionError::ExceedsCostModel) => {
                        error!("bundle exceeded cost model");
                    }
                }
            }
        }
    }

    fn get_bundle_txs(bundle: Bundle, bank: &Arc<Bank>) -> Vec<SanitizedTransaction> {
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
