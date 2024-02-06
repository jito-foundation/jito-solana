use {
    crate::{
        banking_stage::{
            committer::CommitTransactionDetails, leader_slot_metrics::ProcessTransactionsSummary,
            leader_slot_timing_metrics::LeaderExecuteAndCommitTimings, qos_service::QosService,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_account_locker::{BundleAccountLocker, LockedBundle},
            bundle_reserved_space_manager::BundleReservedSpaceManager,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics,
            committer::Committer,
        },
        consensus_cache_updater::ConsensusCacheUpdater,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    solana_bundle::{
        bundle_execution::{load_and_execute_bundle, BundleExecutionMetrics},
        BundleExecutionError, BundleExecutionResult, TipError,
    },
    solana_cost_model::transaction_cost::TransactionCost,
    solana_gossip::cluster_info::ClusterInfo,
    solana_measure::{measure, measure_us},
    solana_poh::poh_recorder::{BankStart, RecordTransactionsSummary, TransactionRecorder},
    solana_runtime::bank::Bank,
    solana_sdk::{
        bundle::SanitizedBundle,
        clock::{Slot, MAX_PROCESSING_AGE},
        feature_set,
        pubkey::Pubkey,
        transaction::{self},
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        collections::HashSet,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

pub struct ExecuteRecordCommitResult {
    commit_transaction_details: Vec<CommitTransactionDetails>,
    result: BundleExecutionResult<()>,
    execution_metrics: BundleExecutionMetrics,
    execute_and_commit_timings: LeaderExecuteAndCommitTimings,
    transaction_error_counter: TransactionErrorMetrics,
}

pub struct BundleConsumer {
    committer: Committer,
    transaction_recorder: TransactionRecorder,
    qos_service: QosService,
    log_messages_bytes_limit: Option<usize>,

    consensus_cache_updater: ConsensusCacheUpdater,

    tip_manager: TipManager,
    last_tip_update_slot: Slot,

    blacklisted_accounts: HashSet<Pubkey>,

    // Manages account locks across multiple transactions within a bundle to prevent race conditions
    // with BankingStage
    bundle_account_locker: BundleAccountLocker,

    block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,

    max_bundle_retry_duration: Duration,

    cluster_info: Arc<ClusterInfo>,

    reserved_space: BundleReservedSpaceManager,
}

impl BundleConsumer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        committer: Committer,
        transaction_recorder: TransactionRecorder,
        qos_service: QosService,
        log_messages_bytes_limit: Option<usize>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        max_bundle_retry_duration: Duration,
        cluster_info: Arc<ClusterInfo>,
        reserved_space: BundleReservedSpaceManager,
    ) -> Self {
        Self {
            committer,
            transaction_recorder,
            qos_service,
            log_messages_bytes_limit,
            consensus_cache_updater: ConsensusCacheUpdater::default(),
            tip_manager,
            // MAX because sending tips during slot 0 in tests doesn't work
            last_tip_update_slot: u64::MAX,
            blacklisted_accounts: HashSet::default(),
            bundle_account_locker,
            block_builder_fee_info,
            max_bundle_retry_duration,
            cluster_info,
            reserved_space,
        }
    }

    // A bundle is a series of transactions to be executed sequentially, atomically, and all-or-nothing.
    // Sequentially:
    //  - Transactions are executed in order
    // Atomically:
    //  - All transactions in a bundle get recoded to PoH and committed to the bank in the same slot. Account locks
    //  for all accounts in all transactions in a bundle are held during the entire execution to remove POH record race conditions
    //  with transactions in BankingStage.
    // All-or-nothing:
    //  - All transactions are committed or none. Modified state for the entire bundle isn't recorded to PoH and committed to the
    //  bank until all transactions in the bundle have executed.
    //
    // Some corner cases to be aware of when working with BundleStage:
    // A bundle is not allowed to call the Tip Payment program in a bundle (or BankingStage).
    // - This is to avoid stealing of tips by malicious parties with bundles that crank the tip
    // payment program and set the tip receiver to themself.
    // A bundle is not allowed to touch consensus-related accounts
    //  - This is to avoid stalling the voting BankingStage threads.
    pub fn consume_buffered_bundles(
        &mut self,
        bank_start: &BankStart,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) {
        self.maybe_update_blacklist(bank_start);
        self.reserved_space.tick(&bank_start.working_bank);

        let reached_end_of_slot = unprocessed_transaction_storage.process_bundles(
            bank_start.working_bank.clone(),
            bundle_stage_leader_metrics,
            &self.blacklisted_accounts,
            |bundles, bundle_stage_leader_metrics| {
                Self::do_process_bundles(
                    &self.bundle_account_locker,
                    &self.tip_manager,
                    &mut self.last_tip_update_slot,
                    &self.cluster_info,
                    &self.block_builder_fee_info,
                    &self.committer,
                    &self.transaction_recorder,
                    &self.qos_service,
                    &self.log_messages_bytes_limit,
                    self.max_bundle_retry_duration,
                    &self.reserved_space,
                    bundles,
                    bank_start,
                    bundle_stage_leader_metrics,
                )
            },
        );

        if reached_end_of_slot {
            bundle_stage_leader_metrics
                .leader_slot_metrics_tracker()
                .set_end_of_slot_unprocessed_buffer_len(
                    unprocessed_transaction_storage.len() as u64
                );
        }
    }

    /// Blacklist is updated with the tip payment program + any consensus accounts.
    fn maybe_update_blacklist(&mut self, bank_start: &BankStart) {
        if self
            .consensus_cache_updater
            .maybe_update(&bank_start.working_bank)
        {
            self.blacklisted_accounts = self
                .consensus_cache_updater
                .consensus_accounts_cache()
                .union(&HashSet::from_iter([self
                    .tip_manager
                    .tip_payment_program_id()]))
                .cloned()
                .collect();

            debug!(
                "updated blacklist with {} accounts",
                self.blacklisted_accounts.len()
            );
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn do_process_bundles(
        bundle_account_locker: &BundleAccountLocker,
        tip_manager: &TipManager,
        last_tip_updated_slot: &mut Slot,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        bundles: &[(ImmutableDeserializedBundle, SanitizedBundle)],
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Vec<Result<(), BundleExecutionError>> {
        // BundleAccountLocker holds RW locks for ALL accounts in ALL transactions within a single bundle.
        // By pre-locking bundles before they're ready to be processed, it will prevent BankingStage from
        // grabbing those locks so BundleStage can process as fast as possible.
        // A LockedBundle is similar to TransactionBatch; once its dropped the locks are released.
        #[allow(clippy::needless_collect)]
        let (locked_bundle_results, locked_bundles_elapsed) = measure!(
            bundles
                .iter()
                .map(|(_, sanitized_bundle)| {
                    bundle_account_locker
                        .prepare_locked_bundle(sanitized_bundle, &bank_start.working_bank)
                })
                .collect::<Vec<_>>(),
            "locked_bundles_elapsed"
        );
        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_locked_bundle_elapsed_us(locked_bundles_elapsed.as_us());

        let (execution_results, execute_locked_bundles_elapsed) = measure!(locked_bundle_results
            .into_iter()
            .map(|r| match r {
                Ok(locked_bundle) => {
                    let (r, measure) = measure_us!(Self::process_bundle(
                        bundle_account_locker,
                        tip_manager,
                        last_tip_updated_slot,
                        cluster_info,
                        block_builder_fee_info,
                        committer,
                        recorder,
                        qos_service,
                        log_messages_bytes_limit,
                        max_bundle_retry_duration,
                        reserved_space,
                        &locked_bundle,
                        bank_start,
                        bundle_stage_leader_metrics,
                    ));
                    bundle_stage_leader_metrics
                        .leader_slot_metrics_tracker()
                        .increment_process_packets_transactions_us(measure);
                    r
                }
                Err(_) => {
                    Err(BundleExecutionError::LockError)
                }
            })
            .collect::<Vec<_>>());

        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_execute_locked_bundles_elapsed_us(execute_locked_bundles_elapsed.as_us());
        execution_results.iter().for_each(|result| {
            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_bundle_execution_result(result);
        });

        execution_results
    }

    #[allow(clippy::too_many_arguments)]
    fn process_bundle(
        bundle_account_locker: &BundleAccountLocker,
        tip_manager: &TipManager,
        last_tip_updated_slot: &mut Slot,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        locked_bundle: &LockedBundle,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), BundleExecutionError> {
        if !Bank::should_bank_still_be_processing_txs(
            &bank_start.bank_creation_time,
            bank_start.working_bank.ns_per_slot,
        ) {
            return Err(BundleExecutionError::BankProcessingTimeLimitReached);
        }

        if bank_start.working_bank.slot() != *last_tip_updated_slot
            && Self::bundle_touches_tip_pdas(
                locked_bundle.sanitized_bundle(),
                &tip_manager.get_tip_accounts(),
            )
        {
            let start = Instant::now();
            let result = Self::handle_tip_programs(
                bundle_account_locker,
                tip_manager,
                cluster_info,
                block_builder_fee_info,
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                reserved_space,
                bank_start,
                bundle_stage_leader_metrics,
            );

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_change_tip_receiver_elapsed_us(start.elapsed().as_micros() as u64);

            result?;

            *last_tip_updated_slot = bank_start.working_bank.slot();
        }

        Self::update_qos_and_execute_record_commit_bundle(
            committer,
            recorder,
            qos_service,
            log_messages_bytes_limit,
            max_bundle_retry_duration,
            reserved_space,
            locked_bundle.sanitized_bundle(),
            bank_start,
            bundle_stage_leader_metrics,
        )?;

        Ok(())
    }

    /// The validator needs to manage state on two programs related to tips
    #[allow(clippy::too_many_arguments)]
    fn handle_tip_programs(
        bundle_account_locker: &BundleAccountLocker,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), BundleExecutionError> {
        debug!("handle_tip_programs");

        // This will setup the tip payment and tip distribution program if they haven't been
        // initialized yet, which is typically helpful for local validators. On mainnet and testnet,
        // this code should never run.
        let keypair = cluster_info.keypair().clone();
        let initialize_tip_programs_bundle =
            tip_manager.get_initialize_tip_programs_bundle(&bank_start.working_bank, &keypair);
        if let Some(bundle) = initialize_tip_programs_bundle {
            debug!(
                "initializing tip programs with {} transactions, bundle id: {}",
                bundle.transactions.len(),
                bundle.bundle_id
            );

            let locked_init_tip_programs_bundle = bundle_account_locker
                .prepare_locked_bundle(&bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

            Self::update_qos_and_execute_record_commit_bundle(
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                reserved_space,
                locked_init_tip_programs_bundle.sanitized_bundle(),
                bank_start,
                bundle_stage_leader_metrics,
            )
            .map_err(|e| {
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_num_init_tip_account_errors(1);
                error!(
                    "bundle: {} error initializing tip programs: {:?}",
                    locked_init_tip_programs_bundle.sanitized_bundle().bundle_id,
                    e
                );
                BundleExecutionError::TipError(TipError::InitializeProgramsError)
            })?;

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_num_init_tip_account_ok(1);
        }

        // There are two frequently run internal cranks inside the jito-solana validator that have to do with managing MEV tips.
        // One is initialize the TipDistributionAccount, which is a validator's "tip piggy bank" for an epoch
        // The other is ensuring the tip_receiver is configured correctly to ensure tips are routed to the correct
        // address. The validator must drain the tip accounts to the previous tip receiver before setting the tip receiver to
        // themselves.

        let kp = cluster_info.keypair().clone();
        let tip_crank_bundle = tip_manager.get_tip_programs_crank_bundle(
            &bank_start.working_bank,
            &kp,
            &block_builder_fee_info.lock().unwrap(),
        )?;
        debug!("tip_crank_bundle is_some: {}", tip_crank_bundle.is_some());

        if let Some(bundle) = tip_crank_bundle {
            info!(
                "bundle id: {} cranking tip programs with {} transactions",
                bundle.bundle_id,
                bundle.transactions.len()
            );

            let locked_tip_crank_bundle = bundle_account_locker
                .prepare_locked_bundle(&bundle, &bank_start.working_bank)
                .map_err(|_| BundleExecutionError::TipError(TipError::LockError))?;

            Self::update_qos_and_execute_record_commit_bundle(
                committer,
                recorder,
                qos_service,
                log_messages_bytes_limit,
                max_bundle_retry_duration,
                reserved_space,
                locked_tip_crank_bundle.sanitized_bundle(),
                bank_start,
                bundle_stage_leader_metrics,
            )
            .map_err(|e| {
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_num_change_tip_receiver_errors(1);
                error!(
                    "bundle: {} error cranking tip programs: {:?}",
                    locked_tip_crank_bundle.sanitized_bundle().bundle_id,
                    e
                );
                BundleExecutionError::TipError(TipError::CrankTipError)
            })?;

            bundle_stage_leader_metrics
                .bundle_stage_metrics_tracker()
                .increment_num_change_tip_receiver_ok(1);
        }

        Ok(())
    }

    /// Reserves space for the entire bundle up-front to ensure the entire bundle can execute.
    /// Rolls back the reserved space if there's not enough blockspace for all transactions in the bundle.
    fn reserve_bundle_blockspace(
        qos_service: &QosService,
        reserved_space: &BundleReservedSpaceManager,
        sanitized_bundle: &SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> BundleExecutionResult<(Vec<transaction::Result<TransactionCost>>, usize)> {
        let mut write_cost_tracker = bank.write_cost_tracker().unwrap();

        // set the block cost limit to the original block cost limit, run the select + accumulate
        // then reset back to the expected block cost limit. this allows bundle stage to potentially
        // increase block_compute_limits, allocate the space, and reset the block_cost_limits to
        // the reserved space without BankingStage racing to allocate this extra reserved space
        write_cost_tracker.set_block_cost_limit(reserved_space.block_cost_limit());
        let (transaction_qos_cost_results, cost_model_throttled_transactions_count) = qos_service
            .select_and_accumulate_transaction_costs(
                bank,
                &mut write_cost_tracker,
                &sanitized_bundle.transactions,
                std::iter::repeat(Ok(())),
            );
        write_cost_tracker.set_block_cost_limit(reserved_space.expected_block_cost_limits(bank));
        drop(write_cost_tracker);

        // rollback all transaction costs if it can't fit and
        if transaction_qos_cost_results.iter().any(|c| c.is_err()) {
            QosService::remove_costs(transaction_qos_cost_results.iter(), None, bank);
            return Err(BundleExecutionError::ExceedsCostModel);
        }

        Ok((
            transaction_qos_cost_results,
            cost_model_throttled_transactions_count,
        ))
    }

    fn update_qos_and_execute_record_commit_bundle(
        committer: &Committer,
        recorder: &TransactionRecorder,
        qos_service: &QosService,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        reserved_space: &BundleReservedSpaceManager,
        sanitized_bundle: &SanitizedBundle,
        bank_start: &BankStart,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> BundleExecutionResult<()> {
        debug!(
            "bundle: {} reserving blockspace for {} transactions",
            sanitized_bundle.bundle_id,
            sanitized_bundle.transactions.len()
        );

        let (
            (transaction_qos_cost_results, _cost_model_throttled_transactions_count),
            cost_model_elapsed_us,
        ) = measure_us!(Self::reserve_bundle_blockspace(
            qos_service,
            reserved_space,
            sanitized_bundle,
            &bank_start.working_bank
        )?);

        debug!(
            "bundle: {} executing, recording, and committing",
            sanitized_bundle.bundle_id
        );

        let (result, process_transactions_us) = measure_us!(Self::execute_record_commit_bundle(
            committer,
            recorder,
            log_messages_bytes_limit,
            max_bundle_retry_duration,
            sanitized_bundle,
            bank_start,
        ));

        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_num_execution_retries(result.execution_metrics.num_retries);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_transaction_errors(&result.transaction_error_counter);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_process_transactions_us(process_transactions_us);

        let (cu, us) = result
            .execute_and_commit_timings
            .execute_timings
            .accumulate_execute_units_and_time();
        qos_service.accumulate_actual_execute_cu(cu);
        qos_service.accumulate_actual_execute_time(us);

        let num_committed = result
            .commit_transaction_details
            .iter()
            .filter(|c| matches!(c, CommitTransactionDetails::Committed { .. }))
            .count();
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_process_transactions_summary(&ProcessTransactionsSummary {
                reached_max_poh_height: matches!(
                    result.result,
                    Err(BundleExecutionError::BankProcessingTimeLimitReached)
                        | Err(BundleExecutionError::PohRecordError(_))
                ),
                transactions_attempted_execution_count: sanitized_bundle.transactions.len(),
                committed_transactions_count: num_committed,
                // NOTE: this assumes that bundles are committed all-or-nothing
                committed_transactions_with_successful_result_count: num_committed,
                failed_commit_count: 0,
                retryable_transaction_indexes: vec![],
                cost_model_throttled_transactions_count: 0,
                cost_model_us: cost_model_elapsed_us,
                execute_and_commit_timings: result.execute_and_commit_timings,
                error_counters: result.transaction_error_counter,
                min_prioritization_fees: 0, // TODO (LB)
                max_prioritization_fees: 0, // TODO (LB)
            });

        match result.result {
            Ok(_) => {
                // it's assumed that all transactions in the bundle executed, can update QoS
                if !bank_start
                    .working_bank
                    .feature_set
                    .is_active(&feature_set::apply_cost_tracker_during_replay::id())
                {
                    QosService::update_costs(
                        transaction_qos_cost_results.iter(),
                        Some(&result.commit_transaction_details),
                        &bank_start.working_bank,
                    );
                }

                qos_service.report_metrics(bank_start.working_bank.slot());
                Ok(())
            }
            Err(e) => {
                // on bundle failure, none of the transactions are committed, so need to revert
                // all compute reserved
                QosService::remove_costs(
                    transaction_qos_cost_results.iter(),
                    None,
                    &bank_start.working_bank,
                );
                qos_service.report_metrics(bank_start.working_bank.slot());

                Err(e)
            }
        }
    }

    fn execute_record_commit_bundle(
        committer: &Committer,
        recorder: &TransactionRecorder,
        log_messages_bytes_limit: &Option<usize>,
        max_bundle_retry_duration: Duration,
        sanitized_bundle: &SanitizedBundle,
        bank_start: &BankStart,
    ) -> ExecuteRecordCommitResult {
        let transaction_status_sender_enabled = committer.transaction_status_sender_enabled();

        let mut execute_and_commit_timings = LeaderExecuteAndCommitTimings::default();

        debug!("bundle: {} executing", sanitized_bundle.bundle_id);
        let default_accounts = vec![None; sanitized_bundle.transactions.len()];
        let mut bundle_execution_results = load_and_execute_bundle(
            &bank_start.working_bank,
            sanitized_bundle,
            MAX_PROCESSING_AGE,
            &max_bundle_retry_duration,
            transaction_status_sender_enabled,
            transaction_status_sender_enabled,
            transaction_status_sender_enabled,
            transaction_status_sender_enabled,
            log_messages_bytes_limit,
            false,
            None,
            &default_accounts,
            &default_accounts,
        );

        let execution_metrics = bundle_execution_results.metrics();

        execute_and_commit_timings.collect_balances_us = execution_metrics.collect_balances_us;
        execute_and_commit_timings.load_execute_us = execution_metrics.load_execute_us;
        execute_and_commit_timings
            .execute_timings
            .accumulate(&execution_metrics.execute_timings);

        let mut transaction_error_counter = TransactionErrorMetrics::default();
        bundle_execution_results
            .bundle_transaction_results()
            .iter()
            .for_each(|r| {
                transaction_error_counter
                    .accumulate(&r.load_and_execute_transactions_output().error_counters);
            });

        debug!(
            "bundle: {} executed, is_ok: {}",
            sanitized_bundle.bundle_id,
            bundle_execution_results.result().is_ok()
        );

        // don't commit bundle if failure executing any part of the bundle
        if let Err(e) = bundle_execution_results.result() {
            return ExecuteRecordCommitResult {
                commit_transaction_details: vec![],
                result: Err(e.clone().into()),
                execution_metrics,
                execute_and_commit_timings,
                transaction_error_counter,
            };
        }

        let (executed_batches, execution_results_to_transactions_us) =
            measure_us!(bundle_execution_results.executed_transaction_batches());

        debug!(
            "bundle: {} recording {} batches of {:?} transactions",
            sanitized_bundle.bundle_id,
            executed_batches.len(),
            executed_batches
                .iter()
                .map(|b| b.len())
                .collect::<Vec<usize>>()
        );

        let (freeze_lock, freeze_lock_us) = measure_us!(bank_start.working_bank.freeze_lock());
        execute_and_commit_timings.freeze_lock_us = freeze_lock_us;

        let (last_blockhash, lamports_per_signature) = bank_start
            .working_bank
            .last_blockhash_and_lamports_per_signature();

        let (
            RecordTransactionsSummary {
                result: record_transactions_result,
                record_transactions_timings,
                starting_transaction_index,
            },
            record_us,
        ) = measure_us!(
            recorder.record_transactions(bank_start.working_bank.slot(), executed_batches)
        );

        execute_and_commit_timings.record_us = record_us;
        execute_and_commit_timings.record_transactions_timings = record_transactions_timings;
        execute_and_commit_timings
            .record_transactions_timings
            .execution_results_to_transactions_us = execution_results_to_transactions_us;

        debug!(
            "bundle: {} record result: {}",
            sanitized_bundle.bundle_id,
            record_transactions_result.is_ok()
        );

        // don't commit bundle if failed to record
        if let Err(e) = record_transactions_result {
            return ExecuteRecordCommitResult {
                commit_transaction_details: vec![],
                result: Err(e.into()),
                execution_metrics,
                execute_and_commit_timings,
                transaction_error_counter,
            };
        }

        // note: execute_and_commit_timings.commit_us handled inside this function
        let (commit_us, commit_bundle_details) = committer.commit_bundle(
            &mut bundle_execution_results,
            last_blockhash,
            lamports_per_signature,
            starting_transaction_index,
            &bank_start.working_bank,
            &mut execute_and_commit_timings,
        );
        execute_and_commit_timings.commit_us = commit_us;

        drop(freeze_lock);

        // commit_bundle_details contains transactions that were and were not committed
        // given the current implementation only executes, records, and commits bundles
        // where all transactions executed, we can filter out the non-committed
        // TODO (LB): does this make more sense in commit_bundle for future when failing bundles are accepted?
        let commit_transaction_details = commit_bundle_details
            .commit_transaction_details
            .into_iter()
            .flat_map(|commit_details| {
                commit_details
                    .into_iter()
                    .filter(|d| matches!(d, CommitTransactionDetails::Committed { .. }))
            })
            .collect();
        debug!(
            "bundle: {} commit details: {:?}",
            sanitized_bundle.bundle_id, commit_transaction_details
        );

        ExecuteRecordCommitResult {
            commit_transaction_details,
            result: Ok(()),
            execution_metrics,
            execute_and_commit_timings,
            transaction_error_counter,
        }
    }

    /// Returns true if any of the transactions in a bundle mention one of the tip PDAs
    fn bundle_touches_tip_pdas(bundle: &SanitizedBundle, tip_pdas: &HashSet<Pubkey>) -> bool {
        bundle.transactions.iter().any(|tx| {
            tx.message()
                .account_keys()
                .iter()
                .any(|a| tip_pdas.contains(a))
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            bundle_stage::{
                bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
                bundle_packet_deserializer::BundlePacketDeserializer,
                bundle_reserved_space_manager::BundleReservedSpaceManager,
                bundle_stage_leader_metrics::BundleStageLeaderMetrics, committer::Committer,
                QosService, UnprocessedTransactionStorage,
            },
            packet_bundle::PacketBundle,
            proxy::block_engine_stage::BlockBuilderFeeInfo,
            tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
        },
        crossbeam_channel::{unbounded, Receiver},
        jito_tip_distribution::sdk::derive_tip_distribution_account_address,
        rand::{thread_rng, RngCore},
        solana_cost_model::{block_cost_limits::MAX_BLOCK_UNITS, cost_model::CostModel},
        solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::create_genesis_config,
            get_tmp_ledger_path_auto_delete, leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::PacketBatch,
        solana_poh::{
            poh_recorder::{PohRecorder, Record, WorkingBankEntry},
            poh_service::PohService,
        },
        solana_program_test::programs::spl_programs,
        solana_runtime::{
            bank::Bank,
            genesis_utils::{create_genesis_config_with_leader_ex, GenesisConfigInfo},
            installed_scheduler_pool::BankWithScheduler,
            prioritization_fee_cache::PrioritizationFeeCache,
        },
        solana_sdk::{
            bundle::{derive_bundle_id, SanitizedBundle},
            clock::MAX_PROCESSING_AGE,
            fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
            genesis_config::ClusterType,
            hash::Hash,
            native_token::sol_to_lamports,
            packet::Packet,
            poh_config::PohConfig,
            pubkey::Pubkey,
            rent::Rent,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::{SanitizedTransaction, TransactionError, VersionedTransaction},
            vote::state::VoteState,
        },
        solana_streamer::socket::SocketAddrSpace,
        solana_svm::{
            account_loader::TransactionCheckResult,
            transaction_error_metrics::TransactionErrorMetrics,
        },
        std::{
            collections::HashSet,
            str::FromStr,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc, Mutex, RwLock,
            },
            thread::{Builder, JoinHandle},
            time::Duration,
        },
    };

    struct TestFixture {
        genesis_config_info: GenesisConfigInfo,
        leader_keypair: Keypair,
        bank: Arc<Bank>,
        exit: Arc<AtomicBool>,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        poh_simulator: JoinHandle<()>,
        entry_receiver: Receiver<WorkingBankEntry>,
    }

    pub(crate) fn simulate_poh(
        record_receiver: Receiver<Record>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
    ) -> JoinHandle<()> {
        let poh_recorder = poh_recorder.clone();
        let is_exited = poh_recorder.read().unwrap().is_exited.clone();
        let tick_producer = Builder::new()
            .name("solana-simulate_poh".to_string())
            .spawn(move || loop {
                PohService::read_record_receiver_and_process(
                    &poh_recorder,
                    &record_receiver,
                    Duration::from_millis(10),
                );
                if is_exited.load(Ordering::Relaxed) {
                    break;
                }
            });
        tick_producer.unwrap()
    }

    pub fn create_test_recorder(
        bank: &Arc<Bank>,
        blockstore: Arc<Blockstore>,
        poh_config: Option<PohConfig>,
        leader_schedule_cache: Option<Arc<LeaderScheduleCache>>,
    ) -> (
        Arc<AtomicBool>,
        Arc<RwLock<PohRecorder>>,
        JoinHandle<()>,
        Receiver<WorkingBankEntry>,
    ) {
        let leader_schedule_cache = match leader_schedule_cache {
            Some(provided_cache) => provided_cache,
            None => Arc::new(LeaderScheduleCache::new_from_bank(bank)),
        };
        let exit = Arc::new(AtomicBool::new(false));
        let poh_config = poh_config.unwrap_or_default();
        let (mut poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            &Pubkey::default(),
            blockstore,
            &leader_schedule_cache,
            &poh_config,
            exit.clone(),
        );
        poh_recorder.set_bank(
            BankWithScheduler::new_without_scheduler(bank.clone()),
            false,
        );

        let poh_recorder = Arc::new(RwLock::new(poh_recorder));
        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        (exit, poh_recorder, poh_simulator, entry_receiver)
    }

    fn create_test_fixture(mint_sol: u64) -> TestFixture {
        let mint_keypair = Keypair::new();
        let leader_keypair = Keypair::new();
        let voting_keypair = Keypair::new();

        let rent = Rent::default();

        let mut genesis_config = create_genesis_config_with_leader_ex(
            sol_to_lamports(mint_sol as f64),
            &mint_keypair.pubkey(),
            &leader_keypair.pubkey(),
            &voting_keypair.pubkey(),
            &solana_sdk::pubkey::new_rand(),
            rent.minimum_balance(VoteState::size_of()) + sol_to_lamports(1_000_000.0),
            sol_to_lamports(1_000_000.0),
            FeeRateGovernor {
                // Initialize with a non-zero fee
                lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
                ..FeeRateGovernor::default()
            },
            rent.clone(), // most tests don't expect rent
            ClusterType::Development,
            spl_programs(&rent),
        );
        genesis_config.ticks_per_slot *= 8;

        // workaround for https://github.com/solana-labs/solana/issues/30085
        // the test can deploy and use spl_programs in the genensis slot without waiting for the next one
        let (bank, _) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (exit, poh_recorder, poh_simulator, entry_receiver) =
            create_test_recorder(&bank, blockstore, Some(PohConfig::default()), None);

        let validator_pubkey = voting_keypair.pubkey();
        TestFixture {
            genesis_config_info: GenesisConfigInfo {
                genesis_config,
                mint_keypair,
                voting_keypair,
                validator_pubkey,
            },
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
        }
    }

    fn make_random_overlapping_bundles(
        mint_keypair: &Keypair,
        num_bundles: usize,
        num_packets_per_bundle: usize,
        hash: Hash,
        max_transfer_amount: u64,
    ) -> Vec<PacketBundle> {
        let mut rng = thread_rng();

        (0..num_bundles)
            .map(|_| {
                let transfers: Vec<_> = (0..num_packets_per_bundle)
                    .map(|_| {
                        VersionedTransaction::from(transfer(
                            mint_keypair,
                            &mint_keypair.pubkey(),
                            rng.next_u64() % max_transfer_amount,
                            hash,
                        ))
                    })
                    .collect();
                let bundle_id = derive_bundle_id(&transfers);

                PacketBundle {
                    batch: PacketBatch::new(
                        transfers
                            .iter()
                            .map(|tx| Packet::from_data(None, tx).unwrap())
                            .collect(),
                    ),
                    bundle_id,
                }
            })
            .collect()
    }

    fn get_tip_manager(vote_account: &Pubkey) -> TipManager {
        TipManager::new(TipManagerConfig {
            tip_payment_program_id: Pubkey::from_str("T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt")
                .unwrap(),
            tip_distribution_program_id: Pubkey::from_str(
                "4R3gSG8BpU4t19KYj8CfnbtRpnT8gtk4dvTHxVRwc2r7",
            )
            .unwrap(),
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority: Pubkey::new_unique(),
                vote_account: *vote_account,
                commission_bps: 10,
            },
        })
    }

    /// Happy-path bundle execution w/ no tip management
    #[test]
    fn test_bundle_no_tip_success() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
        } = create_test_fixture(1_000_000);
        let recorder = poh_recorder.read().unwrap().new_recorder();

        let status = poh_recorder.read().unwrap().reached_leader_slot();
        info!("status: {:?}", status);

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let block_builder_pubkey = Pubkey::new_unique();
        let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));

        let mut consumer = BundleConsumer::new(
            committer,
            recorder,
            QosService::new(1),
            None,
            tip_manager,
            BundleAccountLocker::default(),
            block_builder_info,
            Duration::from_secs(10),
            cluster_info,
            BundleReservedSpaceManager::new(
                MAX_BLOCK_UNITS,
                3_000_000,
                poh_recorder
                    .read()
                    .unwrap()
                    .ticks_per_slot()
                    .saturating_mul(8)
                    .saturating_div(10),
            ),
        );

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let mut bundle_storage = UnprocessedTransactionStorage::new_bundle_storage();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);

        let mut packet_bundles = make_random_overlapping_bundles(
            &genesis_config_info.mint_keypair,
            1,
            3,
            genesis_config_info.genesis_config.hash(),
            10_000,
        );
        let deserialized_bundle = BundlePacketDeserializer::deserialize_bundle(
            packet_bundles.get_mut(0).unwrap(),
            false,
            None,
        )
        .unwrap();
        let mut error_metrics = TransactionErrorMetrics::default();
        let sanitized_bundle = deserialized_bundle
            .build_sanitized_bundle(
                &bank_start.working_bank,
                &HashSet::default(),
                &mut error_metrics,
            )
            .unwrap();

        let summary = bundle_storage.insert_bundles(vec![deserialized_bundle]);
        assert_eq!(
            summary.num_packets_inserted,
            sanitized_bundle.transactions.len()
        );
        assert_eq!(summary.num_bundles_dropped, 0);
        assert_eq!(summary.num_bundles_inserted, 1);

        consumer.consume_buffered_bundles(
            &bank_start,
            &mut bundle_storage,
            &mut bundle_stage_leader_metrics,
        );

        let mut transactions = Vec::new();
        while let Ok(WorkingBankEntry {
            bank: wbe_bank,
            entries_ticks,
        }) = entry_receiver.recv()
        {
            assert_eq!(bank.slot(), wbe_bank.slot());
            for (entry, _) in entries_ticks {
                if !entry.transactions.is_empty() {
                    // transactions in this test are all overlapping, so each entry will contain 1 transaction
                    assert_eq!(entry.transactions.len(), 1);
                    transactions.extend(entry.transactions);
                }
            }
            if transactions.len() == sanitized_bundle.transactions.len() {
                break;
            }
        }

        let bundle_versioned_transactions: Vec<_> = sanitized_bundle
            .transactions
            .iter()
            .map(|tx| tx.to_versioned_transaction())
            .collect();
        assert_eq!(transactions, bundle_versioned_transactions);

        let check_results = bank.check_transactions(
            &sanitized_bundle.transactions,
            &vec![Ok(()); sanitized_bundle.transactions.len()],
            MAX_PROCESSING_AGE,
            &mut error_metrics,
        );

        let expected_result: Vec<TransactionCheckResult> =
            vec![
                (Err(TransactionError::AlreadyProcessed), None, None);
                sanitized_bundle.transactions.len()
            ];

        assert_eq!(check_results, expected_result);

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
        // TODO (LB): cleanup blockstore
    }

    /// Happy-path bundle execution to ensure tip management works.
    /// Tip management involves cranking setup bundles before executing the test bundle
    #[test]
    fn test_bundle_tip_program_setup_success() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
        } = create_test_fixture(1_000_000);
        let recorder = poh_recorder.read().unwrap().new_recorder();

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let block_builder_pubkey = Pubkey::new_unique();
        let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));

        let mut consumer = BundleConsumer::new(
            committer,
            recorder,
            QosService::new(1),
            None,
            tip_manager.clone(),
            BundleAccountLocker::default(),
            block_builder_info,
            Duration::from_secs(10),
            cluster_info.clone(),
            BundleReservedSpaceManager::new(
                MAX_BLOCK_UNITS,
                3_000_000,
                poh_recorder
                    .read()
                    .unwrap()
                    .ticks_per_slot()
                    .saturating_mul(8)
                    .saturating_div(10),
            ),
        );

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let mut bundle_storage = UnprocessedTransactionStorage::new_bundle_storage();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
        // MAIN LOGIC

        // a bundle that tips the tip program
        let tip_accounts = tip_manager.get_tip_accounts();
        let tip_account = tip_accounts.iter().collect::<Vec<_>>()[0];
        let mut packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(
                None,
                transfer(
                    &genesis_config_info.mint_keypair,
                    tip_account,
                    1,
                    genesis_config_info.genesis_config.hash(),
                ),
            )
            .unwrap()]),
            bundle_id: "test_transfer".to_string(),
        };

        let deserialized_bundle =
            BundlePacketDeserializer::deserialize_bundle(&mut packet_bundle, false, None).unwrap();
        let mut error_metrics = TransactionErrorMetrics::default();
        let sanitized_bundle = deserialized_bundle
            .build_sanitized_bundle(
                &bank_start.working_bank,
                &HashSet::default(),
                &mut error_metrics,
            )
            .unwrap();

        let summary = bundle_storage.insert_bundles(vec![deserialized_bundle]);
        assert_eq!(summary.num_bundles_inserted, 1);
        assert_eq!(summary.num_packets_inserted, 1);
        assert_eq!(summary.num_bundles_dropped, 0);

        consumer.consume_buffered_bundles(
            &bank_start,
            &mut bundle_storage,
            &mut bundle_stage_leader_metrics,
        );

        // its expected there are 3 transactions. One to initialize the tip program configuration, one to change the tip receiver,
        // and another with the tip

        let mut transactions = Vec::new();
        while let Ok(WorkingBankEntry {
            bank: wbe_bank,
            entries_ticks,
        }) = entry_receiver.recv()
        {
            assert_eq!(bank.slot(), wbe_bank.slot());
            transactions.extend(entries_ticks.into_iter().flat_map(|(e, _)| e.transactions));
            if transactions.len() == 5 {
                break;
            }
        }

        // tip management on the first bundle involves:
        // calling initialize on the tip payment and tip distribution programs
        // creating the tip distribution account for this validator's epoch (the MEV piggy bank)
        // changing the tip receiver and block builder tx
        // the original transfer that was sent
        let keypair = cluster_info.keypair().clone();

        assert_eq!(
            transactions[0],
            tip_manager
                .initialize_tip_payment_program_tx(bank.last_blockhash(), &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[1],
            tip_manager
                .initialize_tip_distribution_config_tx(bank.last_blockhash(), &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[2],
            tip_manager
                .initialize_tip_distribution_account_tx(
                    bank.last_blockhash(),
                    bank.epoch(),
                    &keypair
                )
                .to_versioned_transaction()
        );
        // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
        // TipPayment program during initialization
        assert_eq!(
            transactions[3],
            tip_manager
                .build_change_tip_receiver_and_block_builder_tx(
                    &keypair.pubkey(),
                    &derive_tip_distribution_account_address(
                        &tip_manager.tip_distribution_program_id(),
                        &genesis_config_info.validator_pubkey,
                        bank_start.working_bank.epoch()
                    )
                    .0,
                    &bank_start.working_bank,
                    &keypair,
                    &keypair.pubkey(),
                    &block_builder_pubkey,
                    10
                )
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[4],
            sanitized_bundle.transactions[0].to_versioned_transaction()
        );

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }

    #[test]
    fn test_handle_tip_programs() {
        solana_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            exit,
            poh_recorder,
            poh_simulator,
            entry_receiver,
        } = create_test_fixture(1_000_000);
        let recorder = poh_recorder.read().unwrap().new_recorder();

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::new(0u64)),
        );

        let block_builder_pubkey = Pubkey::new_unique();
        let tip_manager = get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let block_builder_info = Arc::new(Mutex::new(BlockBuilderFeeInfo {
            block_builder: block_builder_pubkey,
            block_builder_commission: 10,
        }));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new(leader_keypair.pubkey(), 0, 0),
            Arc::new(leader_keypair),
            SocketAddrSpace::new(true),
        ));

        let bank_start = poh_recorder.read().unwrap().bank_start().unwrap();

        let reserved_ticks = bank.max_tick_height().saturating_mul(8).saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space =
            BundleReservedSpaceManager::new(MAX_BLOCK_UNITS, 3_000_000, reserved_ticks);
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(1);
        assert_matches!(
            BundleConsumer::handle_tip_programs(
                &BundleAccountLocker::default(),
                &tip_manager,
                &cluster_info,
                &block_builder_info,
                &committer,
                &recorder,
                &QosService::new(1),
                &None,
                Duration::from_secs(10),
                &reserved_space,
                &bank_start,
                &mut bundle_stage_leader_metrics
            ),
            Ok(())
        );

        let mut transactions = Vec::new();
        while let Ok(WorkingBankEntry {
            bank: wbe_bank,
            entries_ticks,
        }) = entry_receiver.recv()
        {
            assert_eq!(bank.slot(), wbe_bank.slot());
            transactions.extend(entries_ticks.into_iter().flat_map(|(e, _)| e.transactions));
            if transactions.len() == 4 {
                break;
            }
        }

        let keypair = cluster_info.keypair().clone();
        // expect to see initialize tip payment program, tip distribution program, initialize tip distribution account, change tip receiver + change block builder
        assert_eq!(
            transactions[0],
            tip_manager
                .initialize_tip_payment_program_tx(bank.last_blockhash(), &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[1],
            tip_manager
                .initialize_tip_distribution_config_tx(bank.last_blockhash(), &keypair)
                .to_versioned_transaction()
        );
        assert_eq!(
            transactions[2],
            tip_manager
                .initialize_tip_distribution_account_tx(
                    bank.last_blockhash(),
                    bank.epoch(),
                    &keypair
                )
                .to_versioned_transaction()
        );
        // the first tip receiver + block builder are the initializer (keypair.pubkey()) as set by the
        // TipPayment program during initialization
        assert_eq!(
            transactions[3],
            tip_manager
                .build_change_tip_receiver_and_block_builder_tx(
                    &keypair.pubkey(),
                    &derive_tip_distribution_account_address(
                        &tip_manager.tip_distribution_program_id(),
                        &genesis_config_info.validator_pubkey,
                        bank_start.working_bank.epoch()
                    )
                    .0,
                    &bank_start.working_bank,
                    &keypair,
                    &keypair.pubkey(),
                    &block_builder_pubkey,
                    10
                )
                .to_versioned_transaction()
        );

        poh_recorder
            .write()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        exit.store(true, Ordering::Relaxed);
        poh_simulator.join().unwrap();
    }

    #[test]
    fn test_reserve_bundle_blockspace_success() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let transfer_tx = SanitizedTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            1,
            bank.parent_hash(),
        ));
        let sanitized_bundle = SanitizedBundle {
            transactions: vec![transfer_tx],
            bundle_id: String::default(),
        };

        let transfer_cost =
            CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);

        let qos_service = QosService::new(1);
        let reserved_ticks = bank.max_tick_height().saturating_mul(8).saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space =
            BundleReservedSpaceManager::new(MAX_BLOCK_UNITS, 3_000_000, reserved_ticks);

        assert!(BundleConsumer::reserve_bundle_blockspace(
            &qos_service,
            &reserved_space,
            &sanitized_bundle,
            &bank
        )
        .is_ok());
        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost(),
            transfer_cost.sum()
        );
    }

    #[test]
    fn test_reserve_bundle_blockspace_failure() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();
        let transfer_tx1 = SanitizedTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            1,
            bank.parent_hash(),
        ));
        let transfer_tx2 = SanitizedTransaction::from_transaction_for_tests(transfer(
            &keypair1,
            &keypair2.pubkey(),
            2,
            bank.parent_hash(),
        ));
        let sanitized_bundle = SanitizedBundle {
            transactions: vec![transfer_tx1, transfer_tx2],
            bundle_id: String::default(),
        };

        // set block cost limit to 1 transfer transaction, try to process 2, should return an error
        // and rollback block cost added
        let transfer_cost =
            CostModel::calculate_cost(&sanitized_bundle.transactions[0], &bank.feature_set);
        bank.write_cost_tracker()
            .unwrap()
            .set_block_cost_limit(transfer_cost.sum());

        let qos_service = QosService::new(1);
        let reserved_ticks = bank.max_tick_height().saturating_mul(8).saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space = BundleReservedSpaceManager::new(
            bank.read_cost_tracker().unwrap().block_cost(),
            50,
            reserved_ticks,
        );

        assert!(BundleConsumer::reserve_bundle_blockspace(
            &qos_service,
            &reserved_space,
            &sanitized_bundle,
            &bank
        )
        .is_err());
        assert_eq!(bank.read_cost_tracker().unwrap().block_cost(), 0);
        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            bank.read_cost_tracker()
                .unwrap()
                .block_cost_limit()
                .saturating_sub(50)
        );
    }
}
