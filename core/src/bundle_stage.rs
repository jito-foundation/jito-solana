//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to contruct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.
use {
    crate::{
        banking_stage::{BatchedTransactionDetails, CommitTransactionDetails},
        bundle_account_locker::{BundleAccountLocker, BundleAccountLockerResult, LockedBundle},
        bundle_sanitizer::{get_sanitized_bundle, BundleSanitizerError},
        bundle_stage_leader_stats::{BundleStageLeaderSlotTrackingMetrics, BundleStageLeaderStats},
        consensus_cache_updater::ConsensusCacheUpdater,
        leader_slot_banking_stage_timing_metrics::RecordTransactionsTimings,
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        qos_service::QosService,
        tip_manager::TipManager,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_entry::entry::hash_transactions,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::{
        blockstore_processor::TransactionStatusSender, token_balances::collect_token_balances,
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
        block_cost_limits::MAX_BLOCK_UNITS,
        cost_model::{CostModel, TransactionCost},
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        bundle::{
            error::BundleExecutionError, sanitized::SanitizedBundle,
            utils::check_bundle_lock_results,
        },
        clock::{Slot, DEFAULT_TICKS_PER_SLOT, MAX_PROCESSING_AGE},
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
const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

type BundleStageResult<T> = Result<T, BundleExecutionError>;

// Stats emitted periodically
struct BundleStageLoopStats {
    last_report: Instant,

    num_bundles_received: u64,
    num_bundles_dropped: u64,
    receive_and_buffer_bundles_elapsed_us: u64,
    process_buffered_bundles_elapsed_us: u64,
}

impl Default for BundleStageLoopStats {
    fn default() -> Self {
        BundleStageLoopStats {
            last_report: Instant::now(),
            num_bundles_received: 0,
            num_bundles_dropped: 0,
            receive_and_buffer_bundles_elapsed_us: 0,
            process_buffered_bundles_elapsed_us: 0,
        }
    }
}

impl BundleStageLoopStats {
    fn maybe_report(&mut self, id: u32, period: Duration) {
        if self.last_report.elapsed() > period {
            datapoint_info!(
                "bundle_stage-loop_stats",
                ("id", id, i64),
                ("num_bundles_received", self.num_bundles_received, i64),
                ("num_bundles_dropped", self.num_bundles_dropped, i64),
                (
                    "receive_and_buffer_bundles_elapsed_us",
                    self.receive_and_buffer_bundles_elapsed_us,
                    i64
                ),
                (
                    "process_buffered_bundles_elapsed_us",
                    self.process_buffered_bundles_elapsed_us,
                    i64
                ),
            );
            *self = BundleStageLoopStats::default();
        }
    }
}

struct AllExecutionResults {
    pub load_and_execute_tx_output: LoadAndExecuteTransactionsOutput,
    pub sanitized_txs: Vec<SanitizedTransaction>,
    pub pre_balances: (TransactionBalances, TransactionTokenBalances),
    pub post_balances: (TransactionBalances, TransactionTokenBalances),
}

struct BundleReservedSpace {
    current_tx_block_limit: u64,
    current_bundle_block_limit: u64,
    initial_allocated_cost: u64,
    unreserved_ticks: u64,
}

impl BundleReservedSpace {
    fn reset_reserved_cost(&mut self, working_bank: &Arc<Bank>) {
        self.current_tx_block_limit = self
            .current_bundle_block_limit
            .saturating_sub(self.initial_allocated_cost);

        working_bank
            .write_cost_tracker()
            .unwrap()
            .set_block_cost_limit(self.current_tx_block_limit);

        debug!(
            "slot: {}. cost limits reset. bundle: {}, txn: {}",
            working_bank.slot(),
            self.current_bundle_block_limit,
            self.current_tx_block_limit,
        );
    }

    fn bundle_block_limit(&self) -> u64 {
        self.current_bundle_block_limit
    }

    fn tx_block_limit(&self) -> u64 {
        self.current_tx_block_limit
    }

    fn update_reserved_cost(&mut self, working_bank: &Arc<Bank>) {
        if self.current_tx_block_limit != self.current_bundle_block_limit
            && working_bank
                .max_tick_height()
                .saturating_sub(working_bank.tick_height())
                < self.unreserved_ticks
        {
            self.current_tx_block_limit = self.current_bundle_block_limit;
            working_bank
                .write_cost_tracker()
                .unwrap()
                .set_block_cost_limit(self.current_tx_block_limit);
            debug!(
                "slot: {}. increased tx cost limit to {}",
                working_bank.slot(),
                self.current_tx_block_limit
            );
        }
    }
}

pub struct BundleStage {
    bundle_thread: JoinHandle<()>,
}

impl BundleStage {
    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        gossip_vote_sender: ReplayVoteSender,
        cost_model: Arc<RwLock<CostModel>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        preallocated_bundle_cost: u64,
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
            block_builder_fee_info,
            preallocated_bundle_cost,
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
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        preallocated_bundle_cost: u64,
    ) -> Self {
        const BUNDLE_STAGE_ID: u32 = 10_000;
        let poh_recorder = poh_recorder.clone();
        let cluster_info = cluster_info.clone();
        let block_builder_fee_info = block_builder_fee_info.clone();

        let bundle_thread = Builder::new()
            .name("solana-bundle-stage".to_string())
            .spawn(move || {
                Self::process_loop(
                    cluster_info,
                    &poh_recorder,
                    transaction_status_sender,
                    bundle_receiver,
                    gossip_vote_sender,
                    BUNDLE_STAGE_ID,
                    cost_model,
                    exit,
                    tip_manager,
                    bundle_account_locker,
                    max_bundle_retry_duration,
                    block_builder_fee_info,
                    preallocated_bundle_cost,
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
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        max_bundle_retry_duration: &Duration,
        reserved_space: &mut BundleReservedSpace,
    ) -> BundleStageResult<()> {
        if sanitized_bundle.transactions.is_empty() {
            return Ok(());
        }

        // Try to fit bundle into block using modified limits (scoped to guarantee cost_tracker is dropped)
        let (tx_costs, transactions_qos_results, num_included) = {
            let tx_costs =
                qos_service.compute_transaction_costs(sanitized_bundle.transactions.iter());

            let mut cost_tracker = bank_start.working_bank.write_cost_tracker().unwrap();

            // Increase block cost limit for bundles
            debug!(
                "increasing cost limit for bundles: {}",
                reserved_space.bundle_block_limit()
            );
            cost_tracker.set_block_cost_limit(reserved_space.bundle_block_limit());
            let (transactions_qos_results, num_included) = qos_service
                .select_transactions_per_cost(
                    sanitized_bundle.transactions.iter(),
                    tx_costs.iter(),
                    bank_start.working_bank.slot(),
                    &mut cost_tracker,
                );
            debug!(
                "resetting cost limit for normal transactions: {}",
                reserved_space.tx_block_limit()
            );

            // Reset block cost limit for normal txs
            cost_tracker.set_block_cost_limit(reserved_space.tx_block_limit());

            (tx_costs, transactions_qos_results, num_included)
        };

        // accumulates QoS to metrics
        qos_service.accumulate_estimated_transaction_costs(
            &Self::accumulate_batched_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
            ),
        );

        // qos rate-limited a tx in here, drop the bundle
        if sanitized_bundle.transactions.len() != num_included {
            QosService::remove_transaction_costs(
                tx_costs.iter(),
                transactions_qos_results.iter(),
                &bank_start.working_bank,
            );
            warn!(
                "bundle dropped, qos rate limit. bundle_id: {} bundle_cost: {},  block_cost: {}",
                sanitized_bundle.bundle_id,
                tx_costs.iter().map(|c| c.sum()).sum::<u64>(),
                &bank_start
                    .working_bank
                    .read_cost_tracker()
                    .unwrap()
                    .block_cost()
            );
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        match Self::execute_record_commit_bundle(
            sanitized_bundle,
            recorder,
            transaction_status_sender,
            gossip_vote_sender,
            bank_start,
            bundle_stage_leader_stats,
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
                    &bundle_stage_leader_stats
                        .execute_and_commit_timings()
                        .execute_timings,
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
        }
    }

    fn execute_bundle(
        sanitized_bundle: &SanitizedBundle,
        transaction_status_sender: &Option<TransactionStatusSender>,
        bank_start: &BankStart,
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        max_bundle_retry_duration: &Duration,
    ) -> BundleStageResult<Vec<AllExecutionResults>> {
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

            let ((pre_balances, pre_token_balances), collect_balances_elapsed) = measure!(
                Self::collect_balances(
                    bank,
                    &batch,
                    &account_overrides,
                    transaction_status_sender,
                    &mut mint_decimals,
                ),
                "collect_balances",
            );
            saturating_add_assign!(
                bundle_stage_leader_stats
                    .execute_and_commit_timings()
                    .collect_balances_us,
                collect_balances_elapsed.as_us()
            );

            let (mut load_and_execute_transactions_output, load_execute_time) = measure!(
                bank.load_and_execute_transactions(
                    &batch,
                    MAX_PROCESSING_AGE,
                    transaction_status_sender.is_some(),
                    transaction_status_sender.is_some(),
                    transaction_status_sender.is_some(),
                    &mut bundle_stage_leader_stats
                        .execute_and_commit_timings()
                        .execute_timings,
                    Some(&account_overrides),
                    None,
                ),
                "load_execute",
            );

            saturating_add_assign!(
                bundle_stage_leader_stats
                    .execute_and_commit_timings()
                    .load_execute_us,
                load_execute_time.as_us()
            );
            bundle_stage_leader_stats
                .transaction_errors()
                .accumulate(&load_and_execute_transactions_output.error_counters);

            debug!(
                "execution results: {:?}",
                load_and_execute_transactions_output.execution_results
            );
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
                debug!("execution error");
                bundle_stage_leader_stats
                    .bundle_stage_stats()
                    .increment_num_execution_failures(1);

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
                debug!("retrying bundle");

                let bundle_execution_elapsed = start_time.elapsed();
                if bundle_execution_elapsed >= *max_bundle_retry_duration {
                    warn!("bundle timed out: {:?}", sanitized_bundle);
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_execution_timeouts(1);
                    return Err(BundleExecutionError::MaxRetriesExceeded(
                        bundle_execution_elapsed,
                    ));
                }

                bundle_stage_leader_stats
                    .bundle_stage_stats()
                    .increment_num_execution_retries(1);
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

            let ((post_balances, post_token_balances), collect_balances_elapsed) = measure!(
                Self::collect_balances(
                    bank,
                    &batch,
                    &account_overrides,
                    transaction_status_sender,
                    &mut mint_decimals,
                ),
                "collect_balances",
            );

            saturating_add_assign!(
                bundle_stage_leader_stats
                    .execute_and_commit_timings()
                    .collect_balances_us,
                collect_balances_elapsed.as_us()
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
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        max_bundle_retry_duration: &Duration,
    ) -> BundleStageResult<Vec<CommitTransactionDetails>> {
        let execution_results = Self::execute_bundle(
            sanitized_bundle,
            transaction_status_sender,
            bank_start,
            bundle_stage_leader_stats,
            max_bundle_retry_duration,
        )?;
        // in order for bundle to succeed, it most have something to record + commit
        assert!(!execution_results.is_empty());

        Self::record_commit_bundle(
            execution_results,
            &bank_start.working_bank,
            recorder,
            bundle_stage_leader_stats,
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
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
    ) -> BundleStageResult<Vec<CommitTransactionDetails>> {
        // *********************************************************************************
        // All transactions are executed in the bundle.
        // Record to PoH and send the saved execution results to the Bank.
        // Note: Ensure that bank.commit_transactions is called on a per-batch basis and
        // not all together
        // *********************************************************************************
        debug!("grabbing freeze lock");
        let (_freeze_lock, freeze_lock_time) = measure!(bank.freeze_lock(), "freeze_lock");
        saturating_add_assign!(
            bundle_stage_leader_stats
                .execute_and_commit_timings()
                .freeze_lock_us,
            freeze_lock_time.as_us()
        );

        let (slot, mixins) = Self::prepare_poh_record_bundle(
            &bank.slot(),
            &execution_results,
            &mut bundle_stage_leader_stats
                .execute_and_commit_timings()
                .record_transactions_timings,
        );

        debug!("recording bundle");
        let (mut transaction_index, record_elapsed) = measure!(
            Self::try_record(recorder, slot, mixins)
                .map_err(|e| {
                    error!("error recording bundle: {:?}", e);
                    e
                })?
                .unwrap_or_default(),
            "record_elapsed"
        );
        debug!("bundle recorded");

        saturating_add_assign!(
            bundle_stage_leader_stats
                .execute_and_commit_timings()
                .record_us,
            record_elapsed.as_us()
        );
        bundle_stage_leader_stats
            .execute_and_commit_timings()
            .record_transactions_timings
            .accumulate(&RecordTransactionsTimings {
                execution_results_to_transactions_us: 0,
                hash_us: 0,
                poh_record_us: record_elapsed.as_us(),
            });

        let mut commit_transaction_details = Vec::new();
        for r in execution_results {
            let mut output = r.load_and_execute_tx_output;
            let sanitized_txs = r.sanitized_txs;

            let (last_blockhash, lamports_per_signature) =
                bank.last_blockhash_and_lamports_per_signature();

            let (transaction_results, commit_elapsed) = measure!(
                bank.commit_transactions(
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
                    &mut bundle_stage_leader_stats
                        .execute_and_commit_timings()
                        .execute_timings,
                ),
                "commit_elapsed"
            );
            saturating_add_assign!(
                bundle_stage_leader_stats
                    .execute_and_commit_timings()
                    .commit_us,
                commit_elapsed.as_us()
            );

            let (_, find_and_send_votes_elapsed) = measure!(
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

            saturating_add_assign!(
                bundle_stage_leader_stats
                    .execute_and_commit_timings()
                    .find_and_send_votes_us,
                find_and_send_votes_elapsed.as_us()
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
    ) -> BundleStageResult<Vec<SanitizedTransaction>> {
        let maybe_init_tip_payment_config_tx =
            if tip_manager.should_initialize_tip_payment_program(bank) {
                info!("building initialize_tip_payment_program_tx");
                Some(tip_manager.initialize_tip_payment_program_tx(
                    bank.last_blockhash(),
                    &cluster_info.keypair(),
                ))
            } else {
                None
            };

        let maybe_init_tip_distro_config_tx =
            if tip_manager.should_initialize_tip_distribution_config(bank) {
                info!("building initialize_tip_distribution_config_tx");
                Some(
                    tip_manager
                        .initialize_tip_distribution_config_tx(bank.last_blockhash(), cluster_info),
                )
            } else {
                None
            };

        let maybe_init_tip_distro_account_tx =
            if tip_manager.should_init_tip_distribution_account(bank) {
                info!("building initialize_tip_distribution_account tx");
                Some(tip_manager.initialize_tip_distribution_account_tx(
                    bank.last_blockhash(),
                    bank.epoch(),
                    cluster_info,
                ))
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
        bundle_account_locker: &BundleAccountLocker,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        cost_model_failed_bundles: &mut VecDeque<PacketBundle>,
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
        last_tip_update_slot: &mut Slot,
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        reserved_space: &mut BundleReservedSpace,
    ) {
        let (sanitized_bundles, sanitized_bundle_elapsed) = measure!(
            unprocessed_bundles
                .drain(..)
                .into_iter()
                .filter_map(|packet_bundle| {
                    match get_sanitized_bundle(
                        &packet_bundle,
                        &bank_start.working_bank,
                        consensus_accounts_cache,
                        blacklisted_accounts,
                        bundle_stage_leader_stats.transaction_errors(),
                    ) {
                        Ok(sanitized_bundle) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_ok(1);
                            Some((packet_bundle, sanitized_bundle))
                        }
                        Err(BundleSanitizerError::VoteOnlyMode) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_vote_only_mode(1);
                            None
                        }
                        Err(BundleSanitizerError::FailedPacketBatchPreCheck) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_failed_precheck(1);
                            None
                        }
                        Err(BundleSanitizerError::BlacklistedAccount) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_blacklisted_account(1);
                            None
                        }
                        Err(BundleSanitizerError::FailedToSerializeTransaction) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_failed_to_serialize(1);
                            None
                        }
                        Err(BundleSanitizerError::DuplicateTransaction) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_duplicate_transaction(1);
                            None
                        }
                        Err(BundleSanitizerError::FailedCheckTransactions) => {
                            bundle_stage_leader_stats
                                .bundle_stage_stats()
                                .increment_sanitize_transaction_failed_check(1);
                            None
                        }
                    }
                })
                .collect::<VecDeque<(PacketBundle, SanitizedBundle)>>(),
            "sanitized_bundle_elapsed"
        );
        bundle_stage_leader_stats
            .bundle_stage_stats()
            .increment_sanitize_bundle_elapsed_us(sanitized_bundle_elapsed.as_us());

        // Prepare locked bundles, which will RW lock accounts in sanitized_bundles so
        // BankingStage can't lock them. This adds a layer of protection since a transaction in a bundle
        // will not hold the AccountLocks through TransactionBatch across load-execute-commit cycle.
        // We collect here to ensure that all of the bundles are locked ahead of time for priority over
        // BankingStage
        #[allow(clippy::needless_collect)]
        let (locked_bundles, locked_bundles_elapsed) = measure!(
            sanitized_bundles
                .iter()
                .map(|(_, sanitized_bundle)| {
                    bundle_account_locker
                        .prepare_locked_bundle(sanitized_bundle, &bank_start.working_bank)
                })
                .collect::<Vec<BundleAccountLockerResult<LockedBundle>>>(),
            "locked_bundles_elapsed"
        );
        bundle_stage_leader_stats
            .bundle_stage_stats()
            .increment_locked_bundle_elapsed_us(locked_bundles_elapsed.as_us());

        let (execution_results, execute_locked_bundles_elapsed) = measure!(
            Self::execute_locked_bundles(
                bundle_account_locker,
                locked_bundles,
                bank_start,
                cluster_info,
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                tip_manager,
                max_bundle_retry_duration,
                last_tip_update_slot,
                bundle_stage_leader_stats,
                block_builder_fee_info,
                reserved_space,
            ),
            "execute_locked_bundles_elapsed"
        );

        bundle_stage_leader_stats
            .bundle_stage_stats()
            .increment_execute_locked_bundles_elapsed_us(execute_locked_bundles_elapsed.as_us());

        execution_results
            .into_iter()
            .zip(sanitized_bundles.into_iter())
            .for_each(
                |(bundle_execution_result, (packet_bundle, _))| match bundle_execution_result {
                    Ok(_) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_ok(1);
                    }
                    Err(BundleExecutionError::PohMaxHeightError) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_poh_max_height(1);
                        // retry the bundle
                        unprocessed_bundles.push_back(packet_bundle);
                    }
                    Err(BundleExecutionError::TransactionFailure(_)) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_transaction_failures(1);
                    }
                    Err(BundleExecutionError::ExceedsCostModel) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_exceeds_cost_model(1);
                        // retry the bundle
                        cost_model_failed_bundles.push_back(packet_bundle);
                    }
                    Err(BundleExecutionError::TipError(_)) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_tip_errors(1);
                    }
                    Err(BundleExecutionError::Shutdown) => {}
                    Err(BundleExecutionError::MaxRetriesExceeded(_)) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_max_retries(1);
                    }
                    Err(BundleExecutionError::LockError) => {
                        bundle_stage_leader_stats
                            .bundle_stage_stats()
                            .increment_execution_results_lock_errors(1);
                    }
                },
            );
    }

    /// This only needs to be done once on program initialization
    /// TODO (LB): may make sense to remove this and move to program deployment instead, but helpful
    ///  during development
    #[allow(clippy::too_many_arguments)]
    fn maybe_initialize_tip_accounts(
        bundle_account_locker: &BundleAccountLocker,
        bank_start: &BankStart,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        reserved_space: &mut BundleReservedSpace,
    ) -> BundleStageResult<()> {
        let initialize_tip_accounts_bundle = SanitizedBundle {
            transactions: Self::get_initialize_tip_accounts_transactions(
                &bank_start.working_bank,
                tip_manager,
                cluster_info,
            )?,
            bundle_id: String::default(),
        };
        if !initialize_tip_accounts_bundle.transactions.is_empty() {
            debug!("initialize tip account");

            let locked_init_tip_bundle = bundle_account_locker
                .prepare_locked_bundle(&initialize_tip_accounts_bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::LockError)?;
            let result = Self::update_qos_and_execute_record_commit_bundle(
                locked_init_tip_bundle.sanitized_bundle(),
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                bank_start,
                bundle_stage_leader_stats,
                max_bundle_retry_duration,
                reserved_space,
            );

            match &result {
                Ok(_) => {
                    debug!("initialize tip account: success");
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_init_tip_account_ok(1);
                }
                Err(e) => {
                    error!("initialize tip account error: {:?}", e);
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_init_tip_account_errors(1);
                }
            }
            result
        } else {
            Ok(())
        }
    }

    /// change tip receiver, draining tips to the previous tip_receiver in the process
    /// note that this needs to happen after the above tip-related bundle initializes
    /// config accounts because get_configured_tip_receiver relies on an account
    /// existing in the bank
    #[allow(clippy::too_many_arguments)]
    fn maybe_change_tip_receiver(
        bundle_account_locker: &BundleAccountLocker,
        bank_start: &BankStart,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        reserved_space: &mut BundleReservedSpace,
    ) -> BundleStageResult<()> {
        let start_handle_tips = Instant::now();

        let configured_tip_receiver =
            tip_manager.get_configured_tip_receiver(&bank_start.working_bank)?;
        let my_tip_distribution_pda =
            tip_manager.get_my_tip_distribution_pda(bank_start.working_bank.epoch());
        if configured_tip_receiver != my_tip_distribution_pda {
            info!(
                "changing tip receiver from {} to {}",
                configured_tip_receiver, my_tip_distribution_pda
            );

            let bb_info = block_builder_fee_info.lock().unwrap();
            let change_tip_receiver_tx = tip_manager.change_tip_receiver_and_block_builder_tx(
                &my_tip_distribution_pda,
                &bank_start.working_bank,
                &cluster_info.keypair(),
                &bb_info.block_builder,
                bb_info.block_builder_commission,
            )?;

            let change_tip_receiver_bundle = SanitizedBundle {
                transactions: vec![change_tip_receiver_tx],
                bundle_id: String::default(),
            };
            let locked_change_tip_receiver_bundle = bundle_account_locker
                .prepare_locked_bundle(&change_tip_receiver_bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::LockError)?;
            let result = Self::update_qos_and_execute_record_commit_bundle(
                locked_change_tip_receiver_bundle.sanitized_bundle(),
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                bank_start,
                bundle_stage_leader_stats,
                max_bundle_retry_duration,
                reserved_space,
            );

            bundle_stage_leader_stats
                .bundle_stage_stats()
                .increment_change_tip_receiver_elapsed_us(
                    start_handle_tips.elapsed().as_micros() as u64
                );

            match &result {
                Ok(_) => {
                    debug!("change tip receiver: success");
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_change_tip_receiver_ok(1);
                }
                Err(e) => {
                    error!("change tip receiver: error {:?}", e);
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_change_tip_receiver_errors(1);
                }
            }
            result
        } else {
            Ok(())
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_locked_bundles(
        bundle_account_locker: &BundleAccountLocker,
        locked_bundles: Vec<BundleAccountLockerResult<LockedBundle>>,
        bank_start: &BankStart,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        last_tip_update_slot: &mut Slot,
        bundle_stage_leader_stats: &mut BundleStageLeaderStats,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        reserved_space: &mut BundleReservedSpace,
    ) -> Vec<BundleStageResult<()>> {
        let tip_pdas = tip_manager.get_tip_accounts();

        // make sure each locked_bundle is dropped after processing to unlock BankingStage
        locked_bundles
            .into_iter()
            .map(|maybe_locked_bundle| {
                let locked_bundle = maybe_locked_bundle.as_ref().map_err(|_| {
                    bundle_stage_leader_stats
                        .bundle_stage_stats()
                        .increment_num_lock_errors(1);

                    BundleExecutionError::LockError
                })?;

                if !Bank::should_bank_still_be_processing_txs(
                    &bank_start.bank_creation_time,
                    bank_start.working_bank.ns_per_slot,
                ) {
                    Err(BundleExecutionError::PohMaxHeightError)
                } else {
                    let sanitized_bundle = locked_bundle.sanitized_bundle();

                    if Self::bundle_touches_tip_pdas(&sanitized_bundle.transactions, &tip_pdas)
                        && bank_start.working_bank.slot() != *last_tip_update_slot
                    {
                        Self::maybe_initialize_tip_accounts(
                            bundle_account_locker,
                            bank_start,
                            cluster_info,
                            recorder,
                            transaction_status_sender,
                            gossip_vote_sender,
                            qos_service,
                            tip_manager,
                            max_bundle_retry_duration,
                            bundle_stage_leader_stats,
                            reserved_space,
                        )?;

                        Self::maybe_change_tip_receiver(
                            bundle_account_locker,
                            bank_start,
                            cluster_info,
                            recorder,
                            transaction_status_sender,
                            gossip_vote_sender,
                            qos_service,
                            tip_manager,
                            max_bundle_retry_duration,
                            bundle_stage_leader_stats,
                            block_builder_fee_info,
                            reserved_space,
                        )?;

                        *last_tip_update_slot = bank_start.working_bank.slot();
                    }

                    Self::update_qos_and_execute_record_commit_bundle(
                        sanitized_bundle,
                        recorder,
                        transaction_status_sender,
                        gossip_vote_sender,
                        qos_service,
                        bank_start,
                        bundle_stage_leader_stats,
                        max_bundle_retry_duration,
                        reserved_space,
                    )
                }
            })
            .collect()
    }

    fn receive_and_buffer_bundles(
        bundle_receiver: &Receiver<Vec<PacketBundle>>,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        timeout: Duration,
    ) -> Result<usize, RecvTimeoutError> {
        let bundles = bundle_receiver.recv_timeout(timeout)?;
        let num_bundles_before = unprocessed_bundles.len();
        unprocessed_bundles.extend(bundles);
        unprocessed_bundles.extend(bundle_receiver.try_iter().flatten());
        let num_bundles_after = unprocessed_bundles.len();
        Ok(num_bundles_after - num_bundles_before)
    }

    #[allow(clippy::too_many_arguments)]
    fn process_buffered_bundles(
        bundle_account_locker: &BundleAccountLocker,
        unprocessed_bundles: &mut VecDeque<PacketBundle>,
        cost_model_failed_bundles: &mut VecDeque<PacketBundle>,
        blacklisted_accounts: &HashSet<Pubkey>,
        consensus_cache_updater: &mut ConsensusCacheUpdater,
        cluster_info: &Arc<ClusterInfo>,
        recorder: &TransactionRecorder,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: &Option<TransactionStatusSender>,
        gossip_vote_sender: &ReplayVoteSender,
        qos_service: &QosService,
        tip_manager: &TipManager,
        max_bundle_retry_duration: &Duration,
        last_tip_update_slot: &mut u64,
        bundle_stage_leader_stats: &mut BundleStageLeaderSlotTrackingMetrics,
        bundle_stage_stats: &mut BundleStageLoopStats,
        id: u32,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        reserved_space: &mut BundleReservedSpace,
    ) {
        const DROP_BUNDLE_SLOT_OFFSET: u64 = 4;

        let r_poh_recorder = poh_recorder.read().unwrap();
        let poh_recorder_bank = r_poh_recorder.get_poh_recorder_bank();
        let working_bank_start = poh_recorder_bank.working_bank_start();
        let would_be_leader_soon =
            r_poh_recorder.would_be_leader(DROP_BUNDLE_SLOT_OFFSET * DEFAULT_TICKS_PER_SLOT);
        drop(r_poh_recorder);

        let last_slot = bundle_stage_leader_stats.current_slot;
        bundle_stage_leader_stats.maybe_report(id, &working_bank_start);

        if !would_be_leader_soon {
            saturating_add_assign!(
                bundle_stage_stats.num_bundles_dropped,
                unprocessed_bundles.len() as u64 + cost_model_failed_bundles.len() as u64
            );

            unprocessed_bundles.clear();
            cost_model_failed_bundles.clear();
            return;
        }

        // leader now, insert new read bundles + as many as can read then return bank
        if let Some(bank_start) = working_bank_start {
            consensus_cache_updater.maybe_update(&bank_start.working_bank);

            let is_new_slot = match (last_slot, bundle_stage_leader_stats.current_slot) {
                (Some(last_slot), Some(current_slot)) => last_slot != current_slot,
                (None, Some(_)) => true,
                (_, _) => false,
            };

            if is_new_slot {
                reserved_space.reset_reserved_cost(&bank_start.working_bank);
                // Re-Buffer any bundles that didn't fit into last block
                if !cost_model_failed_bundles.is_empty() {
                    info!(
                        "slot {}: re-buffering {} bundles that failed cost model.",
                        &bank_start.working_bank.slot(),
                        cost_model_failed_bundles.len()
                    );
                    unprocessed_bundles.extend(cost_model_failed_bundles.drain(..));
                }
            } else {
                reserved_space.update_reserved_cost(&bank_start.working_bank);
            }

            Self::execute_bundles_until_empty_or_end_of_slot(
                bundle_account_locker,
                unprocessed_bundles,
                cost_model_failed_bundles,
                blacklisted_accounts,
                bank_start,
                consensus_cache_updater.consensus_accounts_cache(),
                cluster_info,
                recorder,
                transaction_status_sender,
                gossip_vote_sender,
                qos_service,
                tip_manager,
                max_bundle_retry_duration,
                last_tip_update_slot,
                bundle_stage_leader_stats.bundle_stage_leader_stats(),
                block_builder_fee_info,
                reserved_space,
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        cluster_info: Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        gossip_vote_sender: ReplayVoteSender,
        id: u32,
        cost_model: Arc<RwLock<CostModel>>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        preallocated_bundle_cost: u64,
    ) {
        const LOOP_STATS_METRICS_PERIOD: Duration = Duration::from_secs(1);

        let ticks_per_slot = poh_recorder.read().unwrap().ticks_per_slot();
        let recorder = poh_recorder.read().unwrap().recorder();
        let qos_service = QosService::new(cost_model, id);

        // Bundles can't mention any accounts related to consensus
        let mut consensus_cache_updater = ConsensusCacheUpdater::default();
        let mut last_tip_update_slot = Slot::default();

        let mut last_leader_slots_update_time = Instant::now();
        let mut bundle_stage_leader_stats = BundleStageLeaderSlotTrackingMetrics::default();
        let mut bundle_stage_stats = BundleStageLoopStats::default();

        // Bundles can't mention the tip payment program to ensure that a malicious entity doesn't
        // steal tips mid-slot
        let blacklisted_accounts = HashSet::from_iter([tip_manager.tip_payment_program_id()]);

        let mut unprocessed_bundles: VecDeque<PacketBundle> = VecDeque::with_capacity(1000);
        let mut cost_model_failed_bundles: VecDeque<PacketBundle> = VecDeque::with_capacity(1000);
        // Initialize block limits and open up last 20% of ticks to non-bundle transactions
        let mut reserved_space = BundleReservedSpace {
            current_bundle_block_limit: MAX_BLOCK_UNITS,
            current_tx_block_limit: MAX_BLOCK_UNITS.saturating_sub(preallocated_bundle_cost),
            initial_allocated_cost: preallocated_bundle_cost,
            unreserved_ticks: ticks_per_slot.saturating_div(5), // 20% for non-bundles
        };
        debug!(
            "initialize bundled reserved space: {preallocated_bundle_cost} cu for {} ticks",
            ticks_per_slot.saturating_sub(reserved_space.unreserved_ticks)
        );

        while !exit.load(Ordering::Relaxed) {
            if !unprocessed_bundles.is_empty()
                || last_leader_slots_update_time.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
            {
                let (_, process_buffered_bundles_elapsed) = measure!(
                    Self::process_buffered_bundles(
                        &bundle_account_locker,
                        &mut unprocessed_bundles,
                        &mut cost_model_failed_bundles,
                        &blacklisted_accounts,
                        &mut consensus_cache_updater,
                        &cluster_info,
                        &recorder,
                        poh_recorder,
                        &transaction_status_sender,
                        &gossip_vote_sender,
                        &qos_service,
                        &tip_manager,
                        &max_bundle_retry_duration,
                        &mut last_tip_update_slot,
                        &mut bundle_stage_leader_stats,
                        &mut bundle_stage_stats,
                        id,
                        &block_builder_fee_info,
                        &mut reserved_space,
                    ),
                    "process_buffered_bundles_elapsed"
                );

                saturating_add_assign!(
                    bundle_stage_stats.process_buffered_bundles_elapsed_us,
                    process_buffered_bundles_elapsed.as_us()
                );
                last_leader_slots_update_time = Instant::now();
            }

            bundle_stage_stats.maybe_report(id, LOOP_STATS_METRICS_PERIOD);

            // ensure bundle stage can run immediately if bundles to process, otherwise okay
            // chilling for a few
            let sleep_time = if !unprocessed_bundles.is_empty() {
                Duration::from_millis(0)
            } else {
                Duration::from_millis(10)
            };

            let (res, receive_and_buffer_elapsed) = measure!(
                Self::receive_and_buffer_bundles(
                    &bundle_receiver,
                    &mut unprocessed_bundles,
                    sleep_time,
                ),
                "receive_and_buffer_elapsed"
            );
            saturating_add_assign!(
                bundle_stage_stats.receive_and_buffer_bundles_elapsed_us,
                receive_and_buffer_elapsed.as_us()
            );

            match res {
                Ok(num_bundles_received) => {
                    saturating_add_assign!(
                        bundle_stage_stats.num_bundles_received,
                        num_bundles_received as u64
                    );
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }

    fn prepare_poh_record_bundle(
        bank_slot: &Slot,
        execution_results_txs: &[AllExecutionResults],
        record_transactions_timings: &mut RecordTransactionsTimings,
    ) -> (Slot, Vec<(Hash, Vec<VersionedTransaction>)>) {
        let mut new_record_transaction_timings = RecordTransactionsTimings::default();

        let mixins_txs = execution_results_txs
            .iter()
            .map(|r| {
                let (processed_transactions, results_to_transactions_elapsed) = measure!(
                    {
                        r.load_and_execute_tx_output
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
                            .collect::<Vec<VersionedTransaction>>()
                    },
                    "results_to_transactions_elapsed"
                );

                let (hash, hash_elapsed) = measure!(
                    hash_transactions(&processed_transactions[..]),
                    "hash_elapsed"
                );

                saturating_add_assign!(
                    new_record_transaction_timings.execution_results_to_transactions_us,
                    results_to_transactions_elapsed.as_us()
                );
                saturating_add_assign!(
                    new_record_transaction_timings.hash_us,
                    hash_elapsed.as_us()
                );

                (hash, processed_transactions)
            })
            .collect();

        record_transactions_timings.accumulate(&new_record_transaction_timings);

        (*bank_slot, mixins_txs)
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    fn try_record(
        recorder: &TransactionRecorder,
        bank_slot: Slot,
        mixins_txs: Vec<(Hash, Vec<VersionedTransaction>)>,
    ) -> BundleStageResult<Option<usize>> {
        match recorder.record(bank_slot, mixins_txs) {
            Ok(maybe_tx_index) => Ok(maybe_tx_index),
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
            bundle::{
                error::BundleExecutionError::{
                    ExceedsCostModel, PohMaxHeightError, TransactionFailure,
                },
                sanitized::derive_bundle_id,
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
        let current_block_cost_limit = bank.read_cost_tracker().unwrap().block_cost_limit();
        debug!("current block cost limit: {current_block_cost_limit}");
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
        let ticks_per_slot = poh_recorder.read().unwrap().ticks_per_slot();
        let recorder = poh_recorder.read().unwrap().recorder();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let mut bundle_stage_leader_stats = BundleStageLeaderStats::default();
        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();
        let sanitized_bundle = get_sanitized_bundle(
            &bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            bundle_stage_leader_stats.transaction_errors(),
        )
        .unwrap();

        let results = BundleStage::update_qos_and_execute_record_commit_bundle(
            &sanitized_bundle,
            &recorder,
            &None,
            &gossip_vote_sender,
            &qos_service,
            &bank_start,
            &mut bundle_stage_leader_stats,
            &TEST_MAX_RETRY_DURATION,
            &mut BundleReservedSpace {
                current_tx_block_limit: current_block_cost_limit,
                current_bundle_block_limit: current_block_cost_limit,
                initial_allocated_cost: 0,
                unreserved_ticks: ticks_per_slot,
            },
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
            assert!(get_sanitized_bundle(
                &bundle,
                &bank,
                &HashSet::default(),
                &HashSet::default(),
                bundle_stage_leader_stats.transaction_errors(),
            )
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
        let tx = VersionedTransaction::from(Transaction::new(
            &[&mint_keypair],
            message,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        (
            genesis_config,
            PacketBundle {
                batch: PacketBatch::new(vec![packet]),
                bundle_id,
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
        let tx = VersionedTransaction::from(Transaction::new(
            &[&mint_keypair],
            message,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();

        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id: derive_bundle_id(&[tx]),
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
        let tx = VersionedTransaction::from(system_transaction::nonced_transfer(
            &mint_keypair,
            &kp_a.pubkey(),
            1,
            &kp_nonce.pubkey(),
            &kp_nonce_authority,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id: derive_bundle_id(&[tx]),
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

        let successful_tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp_b.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let failed_tx =
            VersionedTransaction::from(transfer(&kp_a, &kp_b.pubkey(), 1, genesis_config.hash()));

        let successful_packet = Packet::from_data(None, &successful_tx).unwrap();
        let failed_packet = Packet::from_data(None, &failed_tx).unwrap();

        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![successful_packet, failed_packet]),
            bundle_id: derive_bundle_id(&[successful_tx, failed_tx]),
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
        let tx =
            VersionedTransaction::from(transfer(&kp_a, &kp_b.pubkey(), 1, genesis_config.hash()));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id: derive_bundle_id(&[tx]),
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
        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp_b.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id: derive_bundle_id(&[tx]),
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
        let current_block_cost_limit = bank.read_cost_tracker().unwrap().block_cost_limit();
        debug!("current block cost limit: {current_block_cost_limit}");
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
        let ticks_per_slot = poh_recorder.read().unwrap().ticks_per_slot();
        let recorder = poh_recorder.read().unwrap().recorder();
        let (gossip_vote_sender, _gossip_vote_receiver) = unbounded();
        let cost_model = Arc::new(RwLock::new(CostModel::default()));
        let qos_service = QosService::new(cost_model, 0);
        let mut bundle_stage_leader_stats = BundleStageLeaderStats::default();
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

        // grab lock on tx1
        let _batch = bank.prepare_sanitized_batch(&sanitized_txs_1);

        // push and pop tx0
        let bundle = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx0).unwrap()]),
            bundle_id: derive_bundle_id(&[tx0]),
        };
        info!("test_bundle_max_retries uuid: {:?}", bundle.bundle_id);

        let sanitized_bundle = get_sanitized_bundle(
            &bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            bundle_stage_leader_stats.transaction_errors(),
        )
        .unwrap();

        let result = BundleStage::update_qos_and_execute_record_commit_bundle(
            &sanitized_bundle,
            &recorder,
            &None,
            &gossip_vote_sender,
            &qos_service,
            &bank_start,
            &mut bundle_stage_leader_stats,
            &TEST_MAX_RETRY_DURATION,
            &mut BundleReservedSpace {
                current_tx_block_limit: current_block_cost_limit,
                current_bundle_block_limit: current_block_cost_limit,
                initial_allocated_cost: 0,
                unreserved_ticks: ticks_per_slot,
            },
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
