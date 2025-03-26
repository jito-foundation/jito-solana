#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::{field_qualifiers, qualifiers};
use {
    crate::{
        account_loader::{
            collect_rent_from_account, load_transaction, validate_fee_payer, AccountLoader,
            CheckedTransactionDetails, LoadedTransaction, TransactionCheckResult,
            TransactionLoadResult, ValidatedTransactionDetails,
        },
        account_overrides::AccountOverrides,
        message_processor::process_message,
        nonce_info::NonceInfo,
        program_loader::{get_program_modification_slot, load_program_with_pubkey},
        rollback_accounts::RollbackAccounts,
        transaction_account_state_info::TransactionAccountStateInfo,
        transaction_error_metrics::TransactionErrorMetrics,
        transaction_execution_result::{ExecutedTransaction, TransactionExecutionDetails},
        transaction_processing_callback::TransactionProcessingCallback,
        transaction_processing_result::{ProcessedTransaction, TransactionProcessingResult},
    },
    log::debug,
    percentage::Percentage,
    solana_account::{state_traits::StateMut, AccountSharedData, ReadableAccount, PROGRAM_OWNERS},
    solana_bpf_loader_program::syscalls::{
        create_program_runtime_environment_v1, create_program_runtime_environment_v2,
    },
    solana_clock::{Epoch, Slot},
    solana_compute_budget::compute_budget::ComputeBudget,
    solana_feature_set::{
        enable_transaction_loading_failure_fees, remove_accounts_executable_flag_checks, FeatureSet,
    },
    solana_fee_structure::{FeeBudgetLimits, FeeDetails, FeeStructure},
    solana_hash::Hash,
    solana_instruction::TRANSACTION_LEVEL_STACK_HEIGHT,
    solana_log_collector::LogCollector,
    solana_measure::{measure::Measure, measure_us},
    solana_message::compiled_instruction::CompiledInstruction,
    solana_nonce::{
        state::{DurableNonce, State as NonceState},
        versions::Versions as NonceVersions,
    },
    solana_program_runtime::{
        invoke_context::{EnvironmentConfig, InvokeContext},
        loaded_programs::{
            ForkGraph, ProgramCache, ProgramCacheEntry, ProgramCacheForTxBatch,
            ProgramCacheMatchCriteria, ProgramRuntimeEnvironment,
        },
        solana_sbpf::{program::BuiltinProgram, vm::Config as VmConfig},
        sysvar_cache::SysvarCache,
    },
    solana_pubkey::Pubkey,
    solana_sdk::{
        inner_instruction::{InnerInstruction, InnerInstructionsList},
        rent_collector::RentCollector,
    },
    solana_sdk_ids::{native_loader, system_program},
    solana_svm_rent_collector::svm_rent_collector::SVMRentCollector,
    solana_svm_transaction::{svm_message::SVMMessage, svm_transaction::SVMTransaction},
    solana_timings::{ExecuteTimingType, ExecuteTimings},
    solana_transaction_context::{ExecutionRecord, TransactionContext},
    solana_transaction_error::{TransactionError, TransactionResult},
    solana_type_overrides::sync::{atomic::Ordering, Arc, RwLock, RwLockReadGuard},
    std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        fmt::{Debug, Formatter},
        rc::Rc,
        sync::Weak,
    },
};

/// A list of log messages emitted during a transaction
pub type TransactionLogMessages = Vec<String>;

/// The output of the transaction batch processor's
/// `load_and_execute_sanitized_transactions` method.
pub struct LoadAndExecuteSanitizedTransactionsOutput {
    /// Error metrics for transactions that were processed.
    pub error_metrics: TransactionErrorMetrics,
    /// Timings for transaction batch execution.
    pub execute_timings: ExecuteTimings,
    /// Vector of results indicating whether a transaction was processed or
    /// could not be processed. Note processed transactions can still have a
    /// failure result meaning that the transaction will be rolled back.
    pub processing_results: Vec<TransactionProcessingResult>,
}

/// Configuration of the recording capabilities for transaction execution
#[derive(Copy, Clone, Default)]
pub struct ExecutionRecordingConfig {
    pub enable_cpi_recording: bool,
    pub enable_log_recording: bool,
    pub enable_return_data_recording: bool,
}

impl ExecutionRecordingConfig {
    pub fn new_single_setting(option: bool) -> Self {
        ExecutionRecordingConfig {
            enable_return_data_recording: option,
            enable_log_recording: option,
            enable_cpi_recording: option,
        }
    }
}

/// Configurations for processing transactions.
#[derive(Default)]
pub struct TransactionProcessingConfig<'a> {
    /// Encapsulates overridden accounts, typically used for transaction
    /// simulation.
    pub account_overrides: Option<&'a AccountOverrides>,
    /// Whether or not to check a program's modification slot when replenishing
    /// a program cache instance.
    pub check_program_modification_slot: bool,
    /// The compute budget to use for transaction execution.
    pub compute_budget: Option<ComputeBudget>,
    /// The maximum number of bytes that log messages can consume.
    pub log_messages_bytes_limit: Option<usize>,
    /// Whether to limit the number of programs loaded for the transaction
    /// batch.
    pub limit_to_load_programs: bool,
    /// Recording capabilities for transaction execution.
    pub recording_config: ExecutionRecordingConfig,
    /// The max number of accounts that a transaction may lock.
    pub transaction_account_lock_limit: Option<usize>,
}

/// Runtime environment for transaction batch processing.
pub struct TransactionProcessingEnvironment<'a> {
    /// The blockhash to use for the transaction batch.
    pub blockhash: Hash,
    /// Lamports per signature that corresponds to this blockhash.
    ///
    /// Note: This value is primarily used for nonce accounts. If set to zero,
    /// it will disable transaction fees. However, any non-zero value will not
    /// change transaction fees. For this reason, it is recommended to use the
    /// `fee_per_signature` field to adjust transaction fees.
    pub blockhash_lamports_per_signature: u64,
    /// The total stake for the current epoch.
    pub epoch_total_stake: u64,
    /// Runtime feature set to use for the transaction batch.
    pub feature_set: Arc<FeatureSet>,
    /// Transaction fee to charge per signature, in lamports.
    pub fee_lamports_per_signature: u64,
    /// Rent collector to use for the transaction batch.
    pub rent_collector: Option<&'a dyn SVMRentCollector>,
}

impl Default for TransactionProcessingEnvironment<'_> {
    fn default() -> Self {
        Self {
            blockhash: Hash::default(),
            blockhash_lamports_per_signature: 0,
            epoch_total_stake: 0,
            feature_set: Arc::<FeatureSet>::default(),
            fee_lamports_per_signature: FeeStructure::default().lamports_per_signature, // <-- Default fee.
            rent_collector: None,
        }
    }
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[cfg_attr(
    feature = "dev-context-only-utils",
    field_qualifiers(slot(pub), epoch(pub))
)]
pub struct TransactionBatchProcessor<FG: ForkGraph> {
    /// Bank slot (i.e. block)
    slot: Slot,

    /// Bank epoch
    epoch: Epoch,

    /// SysvarCache is a collection of system variables that are
    /// accessible from on chain programs. It is passed to SVM from
    /// client code (e.g. Bank) and forwarded to process_message.
    sysvar_cache: RwLock<SysvarCache>,

    /// Programs required for transaction batch processing
    pub program_cache: Arc<RwLock<ProgramCache<FG>>>,

    /// Builtin program ids
    pub builtin_program_ids: RwLock<HashSet<Pubkey>>,
}

impl<FG: ForkGraph> Debug for TransactionBatchProcessor<FG> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionBatchProcessor")
            .field("slot", &self.slot)
            .field("epoch", &self.epoch)
            .field("sysvar_cache", &self.sysvar_cache)
            .field("program_cache", &self.program_cache)
            .finish()
    }
}

impl<FG: ForkGraph> Default for TransactionBatchProcessor<FG> {
    fn default() -> Self {
        Self {
            slot: Slot::default(),
            epoch: Epoch::default(),
            sysvar_cache: RwLock::<SysvarCache>::default(),
            program_cache: Arc::new(RwLock::new(ProgramCache::new(
                Slot::default(),
                Epoch::default(),
            ))),
            builtin_program_ids: RwLock::new(HashSet::new()),
        }
    }
}

impl<FG: ForkGraph> TransactionBatchProcessor<FG> {
    /// Create a new, uninitialized `TransactionBatchProcessor`.
    ///
    /// In this context, uninitialized means that the `TransactionBatchProcessor`
    /// has been initialized with an empty program cache. The cache contains no
    /// programs (including builtins) and has not been configured with a valid
    /// fork graph.
    ///
    /// When using this method, it's advisable to call `set_fork_graph_in_program_cache`
    /// as well as `add_builtin` to configure the cache before using the processor.
    pub fn new_uninitialized(slot: Slot, epoch: Epoch) -> Self {
        Self {
            slot,
            epoch,
            program_cache: Arc::new(RwLock::new(ProgramCache::new(slot, epoch))),
            ..Self::default()
        }
    }

    /// Create a new `TransactionBatchProcessor`.
    ///
    /// The created processor's program cache is initialized with the provided
    /// fork graph and loaders. If any loaders are omitted, a default "empty"
    /// loader (no syscalls) will be used.
    ///
    /// The cache will still not contain any builtin programs. It's advisable to
    /// call `add_builtin` to add the required builtins before using the processor.
    pub fn new(
        slot: Slot,
        epoch: Epoch,
        fork_graph: Weak<RwLock<FG>>,
        program_runtime_environment_v1: Option<ProgramRuntimeEnvironment>,
        program_runtime_environment_v2: Option<ProgramRuntimeEnvironment>,
    ) -> Self {
        let processor = Self::new_uninitialized(slot, epoch);
        {
            let mut program_cache = processor.program_cache.write().unwrap();
            program_cache.set_fork_graph(fork_graph);
            processor.configure_program_runtime_environments_inner(
                &mut program_cache,
                program_runtime_environment_v1,
                program_runtime_environment_v2,
            );
        }
        processor
    }

    /// Create a new `TransactionBatchProcessor` from the current instance, but
    /// with the provided slot and epoch.
    ///
    /// * Inherits the program cache and builtin program ids from the current
    ///   instance.
    /// * Resets the sysvar cache.
    pub fn new_from(&self, slot: Slot, epoch: Epoch) -> Self {
        Self {
            slot,
            epoch,
            sysvar_cache: RwLock::<SysvarCache>::default(),
            program_cache: self.program_cache.clone(),
            builtin_program_ids: RwLock::new(self.builtin_program_ids.read().unwrap().clone()),
        }
    }

    fn configure_program_runtime_environments_inner(
        &self,
        program_cache: &mut ProgramCache<FG>,
        program_runtime_environment_v1: Option<ProgramRuntimeEnvironment>,
        program_runtime_environment_v2: Option<ProgramRuntimeEnvironment>,
    ) {
        let empty_loader = || Arc::new(BuiltinProgram::new_loader(VmConfig::default()));

        program_cache.latest_root_slot = self.slot;
        program_cache.latest_root_epoch = self.epoch;
        program_cache.environments.program_runtime_v1 =
            program_runtime_environment_v1.unwrap_or(empty_loader());
        program_cache.environments.program_runtime_v2 =
            program_runtime_environment_v2.unwrap_or(empty_loader());
    }

    /// Configures the program runtime environments (loaders) in the
    /// transaction processor's program cache.
    pub fn configure_program_runtime_environments(
        &self,
        program_runtime_environment_v1: Option<ProgramRuntimeEnvironment>,
        program_runtime_environment_v2: Option<ProgramRuntimeEnvironment>,
    ) {
        self.configure_program_runtime_environments_inner(
            &mut self.program_cache.write().unwrap(),
            program_runtime_environment_v1,
            program_runtime_environment_v2,
        );
    }

    /// Returns the current environments depending on the given epoch
    /// Returns None if the call could result in a deadlock
    #[cfg(feature = "dev-context-only-utils")]
    pub fn get_environments_for_epoch(
        &self,
        epoch: Epoch,
    ) -> Option<solana_program_runtime::loaded_programs::ProgramRuntimeEnvironments> {
        self.program_cache
            .try_read()
            .ok()
            .map(|cache| cache.get_environments_for_epoch(epoch))
    }

    pub fn sysvar_cache(&self) -> RwLockReadGuard<SysvarCache> {
        self.sysvar_cache.read().unwrap()
    }

    /// Main entrypoint to the SVM.
    pub fn load_and_execute_sanitized_transactions<CB: TransactionProcessingCallback>(
        &self,
        callbacks: &CB,
        sanitized_txs: &[impl SVMTransaction],
        check_results: Vec<TransactionCheckResult>,
        environment: &TransactionProcessingEnvironment,
        config: &TransactionProcessingConfig,
    ) -> LoadAndExecuteSanitizedTransactionsOutput {
        // If `check_results` does not have the same length as `sanitized_txs`,
        // transactions could be truncated as a result of `.iter().zip()` in
        // many of the below methods.
        // See <https://doc.rust-lang.org/std/iter/trait.Iterator.html#method.zip>.
        debug_assert_eq!(
            sanitized_txs.len(),
            check_results.len(),
            "Length of check_results does not match length of sanitized_txs"
        );

        // Initialize metrics.
        let mut error_metrics = TransactionErrorMetrics::default();
        let mut execute_timings = ExecuteTimings::default();
        let mut processing_results = Vec::with_capacity(sanitized_txs.len());

        let native_loader = native_loader::id();
        let (program_accounts_map, filter_executable_us) = measure_us!({
            let mut program_accounts_map = Self::filter_executable_program_accounts(
                callbacks,
                sanitized_txs,
                &check_results,
                PROGRAM_OWNERS,
            );
            for builtin_program in self.builtin_program_ids.read().unwrap().iter() {
                program_accounts_map.insert(*builtin_program, (&native_loader, 0));
            }
            program_accounts_map
        });

        let (mut program_cache_for_tx_batch, program_cache_us) = measure_us!({
            let program_cache_for_tx_batch = self.replenish_program_cache(
                callbacks,
                &program_accounts_map,
                &mut execute_timings,
                config.check_program_modification_slot,
                config.limit_to_load_programs,
            );

            if program_cache_for_tx_batch.hit_max_limit {
                return LoadAndExecuteSanitizedTransactionsOutput {
                    error_metrics,
                    execute_timings,
                    processing_results: (0..sanitized_txs.len())
                        .map(|_| Err(TransactionError::ProgramCacheHitMaxLimit))
                        .collect(),
                };
            }

            program_cache_for_tx_batch
        });

        // Determine a capacity for the internal account cache. This
        // over-allocates but avoids ever reallocating, and spares us from
        // deduplicating the account keys lists.
        let account_keys_in_batch: usize =
            sanitized_txs.iter().map(|tx| tx.account_keys().len()).sum();

        // Create the account loader, which wraps all external account fetching.
        let mut account_loader = AccountLoader::new_with_account_cache_capacity(
            config.account_overrides,
            callbacks,
            environment.feature_set.clone(),
            account_keys_in_batch.saturating_add(
                config
                    .account_overrides
                    .map(|a| a.len())
                    .unwrap_or_default(),
            ),
        );

        let enable_transaction_loading_failure_fees = environment
            .feature_set
            .is_active(&enable_transaction_loading_failure_fees::id());

        let (mut validate_fees_us, mut load_us, mut execution_us): (u64, u64, u64) = (0, 0, 0);

        // Validate, execute, and collect results from each transaction in order.
        // With SIMD83, transactions must be executed in order, because transactions
        // in the same batch may modify the same accounts. Transaction order is
        // preserved within entries written to the ledger.
        for (tx, check_result) in sanitized_txs.iter().zip(check_results) {
            let (validate_result, single_validate_fees_us) =
                measure_us!(check_result.and_then(|tx_details| {
                    Self::validate_transaction_nonce_and_fee_payer(
                        &mut account_loader,
                        tx,
                        tx_details,
                        &environment.blockhash,
                        environment.fee_lamports_per_signature,
                        environment
                            .rent_collector
                            .unwrap_or(&RentCollector::default()),
                        &mut error_metrics,
                        callbacks,
                    )
                }));
            validate_fees_us = validate_fees_us.saturating_add(single_validate_fees_us);

            let (load_result, single_load_us) = measure_us!(load_transaction(
                &mut account_loader,
                tx,
                validate_result,
                &mut error_metrics,
                environment
                    .rent_collector
                    .unwrap_or(&RentCollector::default()),
            ));
            load_us = load_us.saturating_add(single_load_us);

            let (processing_result, single_execution_us) = measure_us!(match load_result {
                TransactionLoadResult::NotLoaded(err) => Err(err),
                TransactionLoadResult::FeesOnly(fees_only_tx) => {
                    if enable_transaction_loading_failure_fees {
                        // Update loaded accounts cache with nonce and fee-payer
                        account_loader
                            .update_accounts_for_failed_tx(tx, &fees_only_tx.rollback_accounts);

                        Ok(ProcessedTransaction::FeesOnly(Box::new(fees_only_tx)))
                    } else {
                        Err(fees_only_tx.load_error)
                    }
                }
                TransactionLoadResult::Loaded(loaded_transaction) => {
                    let executed_tx = self.execute_loaded_transaction(
                        callbacks,
                        tx,
                        loaded_transaction,
                        &mut execute_timings,
                        &mut error_metrics,
                        &mut program_cache_for_tx_batch,
                        environment,
                        config,
                    );

                    // Update loaded accounts cache with account states which might have changed.
                    // Also update local program cache with modifications made by the transaction,
                    // if it executed successfully.
                    account_loader.update_accounts_for_executed_tx(tx, &executed_tx);
                    if executed_tx.was_successful() {
                        program_cache_for_tx_batch.merge(&executed_tx.programs_modified_by_tx);
                    }

                    Ok(ProcessedTransaction::Executed(Box::new(executed_tx)))
                }
            });
            execution_us = execution_us.saturating_add(single_execution_us);

            processing_results.push(processing_result);
        }

        // Skip eviction when there's no chance this particular tx batch has increased the size of
        // ProgramCache entries. Note that loaded_missing is deliberately defined, so that there's
        // still at least one other batch, which will evict the program cache, even after the
        // occurrences of cooperative loading.
        if program_cache_for_tx_batch.loaded_missing || program_cache_for_tx_batch.merged_modified {
            const SHRINK_LOADED_PROGRAMS_TO_PERCENTAGE: u8 = 90;
            self.program_cache
                .write()
                .unwrap()
                .evict_using_2s_random_selection(
                    Percentage::from(SHRINK_LOADED_PROGRAMS_TO_PERCENTAGE),
                    self.slot,
                );
        }

        debug!(
            "load: {}us execute: {}us txs_len={}",
            load_us,
            execution_us,
            sanitized_txs.len(),
        );

        execute_timings
            .saturating_add_in_place(ExecuteTimingType::ValidateFeesUs, validate_fees_us);
        execute_timings
            .saturating_add_in_place(ExecuteTimingType::FilterExecutableUs, filter_executable_us);
        execute_timings
            .saturating_add_in_place(ExecuteTimingType::ProgramCacheUs, program_cache_us);
        execute_timings.saturating_add_in_place(ExecuteTimingType::LoadUs, load_us);
        execute_timings.saturating_add_in_place(ExecuteTimingType::ExecuteUs, execution_us);

        LoadAndExecuteSanitizedTransactionsOutput {
            error_metrics,
            execute_timings,
            processing_results,
        }
    }

    fn validate_transaction_nonce_and_fee_payer<CB: TransactionProcessingCallback>(
        account_loader: &mut AccountLoader<CB>,
        message: &impl SVMMessage,
        checked_details: CheckedTransactionDetails,
        environment_blockhash: &Hash,
        fee_lamports_per_signature: u64,
        rent_collector: &dyn SVMRentCollector,
        error_counters: &mut TransactionErrorMetrics,
        callbacks: &CB,
    ) -> TransactionResult<ValidatedTransactionDetails> {
        // If this is a nonce transaction, validate the nonce info.
        // This must be done for every transaction to support SIMD83 because
        // it may have changed due to use, authorization, or deallocation.
        // This function is a successful no-op if given a blockhash transaction.
        if let CheckedTransactionDetails {
            nonce: Some(ref nonce_info),
            lamports_per_signature: _,
            compute_budget_limits: _,
        } = checked_details
        {
            let next_durable_nonce = DurableNonce::from_blockhash(environment_blockhash);
            Self::validate_transaction_nonce(
                account_loader,
                message,
                nonce_info,
                &next_durable_nonce,
                error_counters,
            )?;
        }

        // Now validate the fee-payer for the transaction unconditionally.
        Self::validate_transaction_fee_payer(
            account_loader,
            message,
            checked_details,
            fee_lamports_per_signature,
            rent_collector,
            error_counters,
            callbacks,
        )
    }

    // Loads transaction fee payer, collects rent if necessary, then calculates
    // transaction fees, and deducts them from the fee payer balance. If the
    // account is not found or has insufficient funds, an error is returned.
    fn validate_transaction_fee_payer<CB: TransactionProcessingCallback>(
        account_loader: &mut AccountLoader<CB>,
        message: &impl SVMMessage,
        checked_details: CheckedTransactionDetails,
        fee_lamports_per_signature: u64,
        rent_collector: &dyn SVMRentCollector,
        error_counters: &mut TransactionErrorMetrics,
        callbacks: &CB,
    ) -> TransactionResult<ValidatedTransactionDetails> {
        let CheckedTransactionDetails {
            nonce,
            lamports_per_signature,
            compute_budget_limits,
        } = checked_details;

        let compute_budget_limits = compute_budget_limits.inspect_err(|_err| {
            error_counters.invalid_compute_budget += 1;
        })?;

        let fee_payer_address = message.fee_payer();

        let Some(mut loaded_fee_payer) = account_loader.load_account(fee_payer_address, true)
        else {
            error_counters.account_not_found += 1;
            return Err(TransactionError::AccountNotFound);
        };

        let fee_payer_loaded_rent_epoch = loaded_fee_payer.account.rent_epoch();
        loaded_fee_payer.rent_collected = collect_rent_from_account(
            &account_loader.feature_set,
            rent_collector,
            fee_payer_address,
            &mut loaded_fee_payer.account,
        )
        .rent_amount;

        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let fee_details = if lamports_per_signature == 0 {
            FeeDetails::default()
        } else {
            callbacks.calculate_fee(
                message,
                fee_lamports_per_signature,
                fee_budget_limits.prioritization_fee,
                account_loader.feature_set.as_ref(),
            )
        };

        let fee_payer_index = 0;
        validate_fee_payer(
            fee_payer_address,
            &mut loaded_fee_payer.account,
            fee_payer_index,
            error_counters,
            rent_collector,
            fee_details.total_fee(),
        )?;

        // Capture fee-subtracted fee payer account and next nonce account state
        // to commit if transaction execution fails.
        let rollback_accounts = RollbackAccounts::new(
            nonce,
            *fee_payer_address,
            loaded_fee_payer.account.clone(),
            loaded_fee_payer.rent_collected,
            fee_payer_loaded_rent_epoch,
        );

        Ok(ValidatedTransactionDetails {
            fee_details,
            rollback_accounts,
            compute_budget_limits,
            loaded_fee_payer_account: loaded_fee_payer,
        })
    }

    fn validate_transaction_nonce<CB: TransactionProcessingCallback>(
        account_loader: &mut AccountLoader<CB>,
        message: &impl SVMMessage,
        nonce_info: &NonceInfo,
        next_durable_nonce: &DurableNonce,
        error_counters: &mut TransactionErrorMetrics,
    ) -> TransactionResult<()> {
        // When SIMD83 is enabled, if the nonce has been used in this batch already, we must drop
        // the transaction. This is the same as if it was used in different batches in the same slot.
        // If the nonce account was closed in the batch, we error as if the blockhash didn't validate.
        // We must validate the account in case it was reopened, either as a normal system account,
        // or a fake nonce account. We must also check the signer in case the authority was changed.
        //
        // Note these checks are *not* obviated by fee-only transactions.
        let nonce_is_valid = account_loader
            .load_account(nonce_info.address(), true)
            .and_then(|loaded_nonce| {
                let current_nonce_account = &loaded_nonce.account;
                system_program::check_id(current_nonce_account.owner()).then_some(())?;
                StateMut::<NonceVersions>::state(current_nonce_account).ok()
            })
            .and_then(
                |current_nonce_versions| match current_nonce_versions.state() {
                    NonceState::Initialized(ref current_nonce_data) => {
                        let nonce_can_be_advanced =
                            &current_nonce_data.durable_nonce != next_durable_nonce;

                        let nonce_authority_is_valid = message
                            .account_keys()
                            .iter()
                            .enumerate()
                            .any(|(i, address)| {
                                address == &current_nonce_data.authority && message.is_signer(i)
                            });

                        if nonce_authority_is_valid {
                            Some(nonce_can_be_advanced)
                        } else {
                            None
                        }
                    }
                    _ => None,
                },
            );

        match nonce_is_valid {
            None => {
                error_counters.blockhash_not_found += 1;
                Err(TransactionError::BlockhashNotFound)
            }
            Some(false) => {
                error_counters.account_not_found += 1;
                Err(TransactionError::AccountNotFound)
            }
            Some(true) => Ok(()),
        }
    }

    /// Returns a map from executable program accounts (all accounts owned by any loader)
    /// to their usage counters, for the transactions with a valid blockhash or nonce.
    fn filter_executable_program_accounts<'a, CB: TransactionProcessingCallback>(
        callbacks: &CB,
        txs: &[impl SVMMessage],
        check_results: &[TransactionCheckResult],
        program_owners: &'a [Pubkey],
    ) -> HashMap<Pubkey, (&'a Pubkey, u64)> {
        let mut result: HashMap<Pubkey, (&'a Pubkey, u64)> = HashMap::new();
        check_results.iter().zip(txs).for_each(|etx| {
            if let (Ok(_), tx) = etx {
                tx.account_keys()
                    .iter()
                    .for_each(|key| match result.entry(*key) {
                        Entry::Occupied(mut entry) => {
                            let (_, count) = entry.get_mut();
                            *count = count.saturating_add(1);
                        }
                        Entry::Vacant(entry) => {
                            if let Some(index) =
                                callbacks.account_matches_owners(key, program_owners)
                            {
                                if let Some(owner) = program_owners.get(index) {
                                    entry.insert((owner, 1));
                                }
                            }
                        }
                    });
            }
        });
        result
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn replenish_program_cache<CB: TransactionProcessingCallback>(
        &self,
        callback: &CB,
        program_accounts_map: &HashMap<Pubkey, (&Pubkey, u64)>,
        execute_timings: &mut ExecuteTimings,
        check_program_modification_slot: bool,
        limit_to_load_programs: bool,
    ) -> ProgramCacheForTxBatch {
        let mut missing_programs: Vec<(Pubkey, (ProgramCacheMatchCriteria, u64))> =
            program_accounts_map
                .iter()
                .map(|(pubkey, (_, count))| {
                    let match_criteria = if check_program_modification_slot {
                        get_program_modification_slot(callback, pubkey)
                            .map_or(ProgramCacheMatchCriteria::Tombstone, |slot| {
                                ProgramCacheMatchCriteria::DeployedOnOrAfterSlot(slot)
                            })
                    } else {
                        ProgramCacheMatchCriteria::NoCriteria
                    };
                    (*pubkey, (match_criteria, *count))
                })
                .collect();

        let mut loaded_programs_for_txs: Option<ProgramCacheForTxBatch> = None;
        loop {
            let (program_to_store, task_cookie, task_waiter) = {
                // Lock the global cache.
                let program_cache = self.program_cache.read().unwrap();
                // Initialize our local cache.
                let is_first_round = loaded_programs_for_txs.is_none();
                if is_first_round {
                    loaded_programs_for_txs = Some(ProgramCacheForTxBatch::new_from_cache(
                        self.slot,
                        self.epoch,
                        &program_cache,
                    ));
                }
                // Figure out which program needs to be loaded next.
                let program_to_load = program_cache.extract(
                    &mut missing_programs,
                    loaded_programs_for_txs.as_mut().unwrap(),
                    is_first_round,
                );

                let program_to_store = program_to_load.map(|(key, count)| {
                    // Load, verify and compile one program.
                    let program = load_program_with_pubkey(
                        callback,
                        &program_cache.get_environments_for_epoch(self.epoch),
                        &key,
                        self.slot,
                        execute_timings,
                        false,
                    )
                    .expect("called load_program_with_pubkey() with nonexistent account");
                    program.tx_usage_counter.store(count, Ordering::Relaxed);
                    (key, program)
                });

                let task_waiter = Arc::clone(&program_cache.loading_task_waiter);
                (program_to_store, task_waiter.cookie(), task_waiter)
                // Unlock the global cache again.
            };

            if let Some((key, program)) = program_to_store {
                loaded_programs_for_txs.as_mut().unwrap().loaded_missing = true;
                let mut program_cache = self.program_cache.write().unwrap();
                // Submit our last completed loading task.
                if program_cache.finish_cooperative_loading_task(self.slot, key, program)
                    && limit_to_load_programs
                {
                    // This branch is taken when there is an error in assigning a program to a
                    // cache slot. It is not possible to mock this error for SVM unit
                    // tests purposes.
                    let mut ret = ProgramCacheForTxBatch::new_from_cache(
                        self.slot,
                        self.epoch,
                        &program_cache,
                    );
                    ret.hit_max_limit = true;
                    return ret;
                }
            } else if missing_programs.is_empty() {
                break;
            } else {
                // Sleep until the next finish_cooperative_loading_task() call.
                // Once a task completes we'll wake up and try to load the
                // missing programs inside the tx batch again.
                let _new_cookie = task_waiter.wait(task_cookie);
            }
        }

        loaded_programs_for_txs.unwrap()
    }

    pub fn prepare_program_cache_for_upcoming_feature_set<CB: TransactionProcessingCallback>(
        &self,
        callbacks: &CB,
        upcoming_feature_set: &FeatureSet,
        compute_budget: &ComputeBudget,
        slot_index: u64,
        slots_in_epoch: u64,
    ) {
        // Recompile loaded programs one at a time before the next epoch hits
        let slots_in_recompilation_phase =
            (solana_program_runtime::loaded_programs::MAX_LOADED_ENTRY_COUNT as u64)
                .min(slots_in_epoch)
                .checked_div(2)
                .unwrap();
        let mut program_cache = self.program_cache.write().unwrap();
        if program_cache.upcoming_environments.is_some() {
            if let Some((key, program_to_recompile)) = program_cache.programs_to_recompile.pop() {
                let effective_epoch = program_cache.latest_root_epoch.saturating_add(1);
                drop(program_cache);
                let environments_for_epoch = self
                    .program_cache
                    .read()
                    .unwrap()
                    .get_environments_for_epoch(effective_epoch);
                if let Some(recompiled) = load_program_with_pubkey(
                    callbacks,
                    &environments_for_epoch,
                    &key,
                    self.slot,
                    &mut ExecuteTimings::default(),
                    false,
                ) {
                    recompiled.tx_usage_counter.fetch_add(
                        program_to_recompile
                            .tx_usage_counter
                            .load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                    recompiled.ix_usage_counter.fetch_add(
                        program_to_recompile
                            .ix_usage_counter
                            .load(Ordering::Relaxed),
                        Ordering::Relaxed,
                    );
                    let mut program_cache = self.program_cache.write().unwrap();
                    program_cache.assign_program(key, recompiled);
                }
            }
        } else if self.epoch != program_cache.latest_root_epoch
            || slot_index.saturating_add(slots_in_recompilation_phase) >= slots_in_epoch
        {
            // Anticipate the upcoming program runtime environment for the next epoch,
            // so we can try to recompile loaded programs before the feature transition hits.
            drop(program_cache);
            let mut program_cache = self.program_cache.write().unwrap();
            let program_runtime_environment_v1 = create_program_runtime_environment_v1(
                upcoming_feature_set,
                compute_budget,
                false, /* deployment */
                false, /* debugging_features */
            )
            .unwrap();
            let program_runtime_environment_v2 = create_program_runtime_environment_v2(
                compute_budget,
                false, /* debugging_features */
            );
            let mut upcoming_environments = program_cache.environments.clone();
            let changed_program_runtime_v1 =
                *upcoming_environments.program_runtime_v1 != program_runtime_environment_v1;
            let changed_program_runtime_v2 =
                *upcoming_environments.program_runtime_v2 != program_runtime_environment_v2;
            if changed_program_runtime_v1 {
                upcoming_environments.program_runtime_v1 = Arc::new(program_runtime_environment_v1);
            }
            if changed_program_runtime_v2 {
                upcoming_environments.program_runtime_v2 = Arc::new(program_runtime_environment_v2);
            }
            program_cache.upcoming_environments = Some(upcoming_environments);
            program_cache.programs_to_recompile = program_cache
                .get_flattened_entries(changed_program_runtime_v1, changed_program_runtime_v2);
            program_cache
                .programs_to_recompile
                .sort_by_cached_key(|(_id, program)| program.decayed_usage_counter(self.slot));
        }
    }

    /// Execute a transaction using the provided loaded accounts and update
    /// the executors cache if the transaction was successful.
    #[allow(clippy::too_many_arguments)]
    fn execute_loaded_transaction<CB: TransactionProcessingCallback>(
        &self,
        callback: &CB,
        tx: &impl SVMTransaction,
        mut loaded_transaction: LoadedTransaction,
        execute_timings: &mut ExecuteTimings,
        error_metrics: &mut TransactionErrorMetrics,
        program_cache_for_tx_batch: &mut ProgramCacheForTxBatch,
        environment: &TransactionProcessingEnvironment,
        config: &TransactionProcessingConfig,
    ) -> ExecutedTransaction {
        let transaction_accounts = std::mem::take(&mut loaded_transaction.accounts);

        // Ensure the length of accounts matches the expected length from tx.account_keys().
        // This is a sanity check in case that someone starts adding some additional accounts
        // since this has been done before. See discussion in PR #4497 for details
        debug_assert!(transaction_accounts.len() == tx.account_keys().len());

        fn transaction_accounts_lamports_sum(
            accounts: &[(Pubkey, AccountSharedData)],
        ) -> Option<u128> {
            accounts.iter().try_fold(0u128, |sum, (_, account)| {
                sum.checked_add(u128::from(account.lamports()))
            })
        }

        let default_rent_collector = RentCollector::default();
        let rent_collector = environment
            .rent_collector
            .unwrap_or(&default_rent_collector);

        let lamports_before_tx =
            transaction_accounts_lamports_sum(&transaction_accounts).unwrap_or(0);

        let compute_budget = config
            .compute_budget
            .unwrap_or_else(|| ComputeBudget::from(loaded_transaction.compute_budget_limits));

        let mut transaction_context = TransactionContext::new(
            transaction_accounts,
            rent_collector.get_rent().clone(),
            compute_budget.max_instruction_stack_depth,
            compute_budget.max_instruction_trace_length,
        );
        transaction_context.set_remove_accounts_executable_flag_checks(
            environment
                .feature_set
                .is_active(&remove_accounts_executable_flag_checks::id()),
        );
        #[cfg(debug_assertions)]
        transaction_context.set_signature(tx.signature());

        let pre_account_state_info =
            TransactionAccountStateInfo::new(&transaction_context, tx, rent_collector);

        let log_collector = if config.recording_config.enable_log_recording {
            match config.log_messages_bytes_limit {
                None => Some(LogCollector::new_ref()),
                Some(log_messages_bytes_limit) => Some(LogCollector::new_ref_with_limit(Some(
                    log_messages_bytes_limit,
                ))),
            }
        } else {
            None
        };

        let mut executed_units = 0u64;
        let sysvar_cache = &self.sysvar_cache.read().unwrap();
        let epoch_vote_account_stake_callback =
            |pubkey| callback.get_current_epoch_vote_account_stake(pubkey);

        let mut invoke_context = InvokeContext::new(
            &mut transaction_context,
            program_cache_for_tx_batch,
            EnvironmentConfig::new(
                environment.blockhash,
                environment.blockhash_lamports_per_signature,
                environment.epoch_total_stake,
                &epoch_vote_account_stake_callback,
                Arc::clone(&environment.feature_set),
                sysvar_cache,
            ),
            log_collector.clone(),
            compute_budget,
        );

        let mut process_message_time = Measure::start("process_message_time");
        let process_result = process_message(
            tx,
            &loaded_transaction.program_indices,
            &mut invoke_context,
            execute_timings,
            &mut executed_units,
        );
        process_message_time.stop();

        drop(invoke_context);

        execute_timings.execute_accessories.process_message_us += process_message_time.as_us();

        let mut status = process_result
            .and_then(|info| {
                let post_account_state_info =
                    TransactionAccountStateInfo::new(&transaction_context, tx, rent_collector);
                TransactionAccountStateInfo::verify_changes(
                    &pre_account_state_info,
                    &post_account_state_info,
                    &transaction_context,
                    rent_collector,
                )
                .map(|_| info)
            })
            .map_err(|err| {
                match err {
                    TransactionError::InvalidRentPayingAccount
                    | TransactionError::InsufficientFundsForRent { .. } => {
                        error_metrics.invalid_rent_paying_account += 1;
                    }
                    TransactionError::InvalidAccountIndex => {
                        error_metrics.invalid_account_index += 1;
                    }
                    _ => {
                        error_metrics.instruction_error += 1;
                    }
                }
                err
            });

        let log_messages: Option<TransactionLogMessages> =
            log_collector.and_then(|log_collector| {
                Rc::try_unwrap(log_collector)
                    .map(|log_collector| log_collector.into_inner().into_messages())
                    .ok()
            });

        let inner_instructions = if config.recording_config.enable_cpi_recording {
            Some(Self::inner_instructions_list_from_instruction_trace(
                &transaction_context,
            ))
        } else {
            None
        };

        let ExecutionRecord {
            accounts,
            return_data,
            touched_account_count,
            accounts_resize_delta: accounts_data_len_delta,
        } = transaction_context.into();

        if status.is_ok()
            && transaction_accounts_lamports_sum(&accounts)
                .filter(|lamports_after_tx| lamports_before_tx == *lamports_after_tx)
                .is_none()
        {
            status = Err(TransactionError::UnbalancedTransaction);
        }
        let status = status.map(|_| ());

        loaded_transaction.accounts = accounts;
        execute_timings.details.total_account_count += loaded_transaction.accounts.len() as u64;
        execute_timings.details.changed_account_count += touched_account_count;

        let return_data = if config.recording_config.enable_return_data_recording
            && !return_data.data.is_empty()
        {
            Some(return_data)
        } else {
            None
        };

        ExecutedTransaction {
            execution_details: TransactionExecutionDetails {
                status,
                log_messages,
                inner_instructions,
                return_data,
                executed_units,
                accounts_data_len_delta,
            },
            loaded_transaction,
            programs_modified_by_tx: program_cache_for_tx_batch.drain_modified_entries(),
        }
    }

    /// Extract the InnerInstructionsList from a TransactionContext
    fn inner_instructions_list_from_instruction_trace(
        transaction_context: &TransactionContext,
    ) -> InnerInstructionsList {
        debug_assert!(transaction_context
            .get_instruction_context_at_index_in_trace(0)
            .map(|instruction_context| instruction_context.get_stack_height()
                == TRANSACTION_LEVEL_STACK_HEIGHT)
            .unwrap_or(true));
        let mut outer_instructions = Vec::new();
        for index_in_trace in 0..transaction_context.get_instruction_trace_length() {
            if let Ok(instruction_context) =
                transaction_context.get_instruction_context_at_index_in_trace(index_in_trace)
            {
                let stack_height = instruction_context.get_stack_height();
                if stack_height == TRANSACTION_LEVEL_STACK_HEIGHT {
                    outer_instructions.push(Vec::new());
                } else if let Some(inner_instructions) = outer_instructions.last_mut() {
                    let stack_height = u8::try_from(stack_height).unwrap_or(u8::MAX);
                    let instruction = CompiledInstruction::new_from_raw_parts(
                        instruction_context
                            .get_index_of_program_account_in_transaction(
                                instruction_context
                                    .get_number_of_program_accounts()
                                    .saturating_sub(1),
                            )
                            .unwrap_or_default() as u8,
                        instruction_context.get_instruction_data().to_vec(),
                        (0..instruction_context.get_number_of_instruction_accounts())
                            .map(|instruction_account_index| {
                                instruction_context
                                    .get_index_of_instruction_account_in_transaction(
                                        instruction_account_index,
                                    )
                                    .unwrap_or_default() as u8
                            })
                            .collect(),
                    );
                    inner_instructions.push(InnerInstruction {
                        instruction,
                        stack_height,
                    });
                } else {
                    debug_assert!(false);
                }
            } else {
                debug_assert!(false);
            }
        }
        outer_instructions
    }

    pub fn fill_missing_sysvar_cache_entries<CB: TransactionProcessingCallback>(
        &self,
        callbacks: &CB,
    ) {
        let mut sysvar_cache = self.sysvar_cache.write().unwrap();
        sysvar_cache.fill_missing_entries(|pubkey, set_sysvar| {
            if let Some(account) = callbacks.get_account_shared_data(pubkey) {
                set_sysvar(account.data());
            }
        });
    }

    pub fn reset_sysvar_cache(&self) {
        let mut sysvar_cache = self.sysvar_cache.write().unwrap();
        sysvar_cache.reset();
    }

    pub fn get_sysvar_cache_for_tests(&self) -> SysvarCache {
        self.sysvar_cache.read().unwrap().clone()
    }

    /// Add a built-in program
    pub fn add_builtin<CB: TransactionProcessingCallback>(
        &self,
        callbacks: &CB,
        program_id: Pubkey,
        name: &str,
        builtin: ProgramCacheEntry,
    ) {
        debug!("Adding program {} under {:?}", name, program_id);
        callbacks.add_builtin_account(name, &program_id);
        self.builtin_program_ids.write().unwrap().insert(program_id);
        self.program_cache
            .write()
            .unwrap()
            .assign_program(program_id, Arc::new(builtin));
        debug!("Added program {} under {:?}", name, program_id);
    }

    #[cfg(feature = "dev-context-only-utils")]
    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn writable_sysvar_cache(&self) -> &RwLock<SysvarCache> {
        &self.sysvar_cache
    }
}

#[cfg(test)]
mod tests {
    #[allow(deprecated)]
    use solana_sysvar::fees::Fees;
    use {
        super::*,
        crate::{
            account_loader::{LoadedTransactionAccount, ValidatedTransactionDetails},
            nonce_info::NonceInfo,
            rollback_accounts::RollbackAccounts,
            transaction_processing_callback::AccountState,
        },
        solana_account::{create_account_shared_data_for_test, WritableAccount},
        solana_clock::Clock,
        solana_compute_budget::compute_budget_limits::ComputeBudgetLimits,
        solana_compute_budget_instruction::instructions_processor::process_compute_budget_instructions,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_epoch_schedule::EpochSchedule,
        solana_feature_set::FeatureSet,
        solana_fee_calculator::FeeCalculator,
        solana_fee_structure::{FeeDetails, FeeStructure},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{LegacyMessage, Message, MessageHeader, SanitizedMessage},
        solana_nonce as nonce,
        solana_program_runtime::loaded_programs::{BlockRelation, ProgramCacheEntryType},
        solana_rent::Rent,
        solana_rent_debits::RentDebits,
        solana_reserved_account_keys::ReservedAccountKeys,
        solana_sdk::rent_collector::{RentCollector, RENT_EXEMPT_RENT_EPOCH},
        solana_sdk_ids::{bpf_loader, system_program, sysvar},
        solana_signature::Signature,
        solana_transaction::{sanitized::SanitizedTransaction, Transaction},
        solana_transaction_context::TransactionContext,
        solana_transaction_error::TransactionError,
        test_case::test_case,
    };

    fn new_unchecked_sanitized_message(message: Message) -> SanitizedMessage {
        SanitizedMessage::Legacy(LegacyMessage::new(
            message,
            &ReservedAccountKeys::empty_key_set(),
        ))
    }

    struct TestForkGraph {}

    impl ForkGraph for TestForkGraph {
        fn relationship(&self, _a: Slot, _b: Slot) -> BlockRelation {
            BlockRelation::Unknown
        }
    }

    #[derive(Default, Clone)]
    struct MockBankCallback {
        account_shared_data: Arc<RwLock<HashMap<Pubkey, AccountSharedData>>>,
        #[allow(clippy::type_complexity)]
        inspected_accounts:
            Arc<RwLock<HashMap<Pubkey, Vec<(Option<AccountSharedData>, /* is_writable */ bool)>>>>,
    }

    impl TransactionProcessingCallback for MockBankCallback {
        fn account_matches_owners(&self, account: &Pubkey, owners: &[Pubkey]) -> Option<usize> {
            if let Some(data) = self.account_shared_data.read().unwrap().get(account) {
                if data.lamports() == 0 {
                    None
                } else {
                    owners.iter().position(|entry| data.owner() == entry)
                }
            } else {
                None
            }
        }

        fn get_account_shared_data(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
            self.account_shared_data
                .read()
                .unwrap()
                .get(pubkey)
                .cloned()
        }

        fn add_builtin_account(&self, name: &str, program_id: &Pubkey) {
            let mut account_data = AccountSharedData::default();
            account_data.set_data(name.as_bytes().to_vec());
            self.account_shared_data
                .write()
                .unwrap()
                .insert(*program_id, account_data);
        }

        fn inspect_account(
            &self,
            address: &Pubkey,
            account_state: AccountState,
            is_writable: bool,
        ) {
            let account = match account_state {
                AccountState::Dead => None,
                AccountState::Alive(account) => Some(account.clone()),
            };
            self.inspected_accounts
                .write()
                .unwrap()
                .entry(*address)
                .or_default()
                .push((account, is_writable));
        }

        fn calculate_fee(
            &self,
            message: &impl SVMMessage,
            lamports_per_signature: u64,
            prioritization_fee: u64,
            _feature_set: &FeatureSet,
        ) -> FeeDetails {
            let signature_count = message
                .num_transaction_signatures()
                .saturating_add(message.num_ed25519_signatures())
                .saturating_add(message.num_secp256k1_signatures())
                .saturating_add(message.num_secp256r1_signatures());

            FeeDetails::new(
                signature_count.saturating_mul(lamports_per_signature),
                prioritization_fee,
            )
        }
    }

    impl<'a> From<&'a MockBankCallback> for AccountLoader<'a, MockBankCallback> {
        fn from(callbacks: &'a MockBankCallback) -> AccountLoader<'a, MockBankCallback> {
            AccountLoader::new_with_account_cache_capacity(
                None,
                callbacks,
                Arc::<FeatureSet>::default(),
                0,
            )
        }
    }

    #[test_case(1; "Check results too small")]
    #[test_case(3; "Check results too large")]
    #[should_panic(expected = "Length of check_results does not match length of sanitized_txs")]
    fn test_check_results_txs_length_mismatch(check_results_len: usize) {
        let sanitized_message = new_unchecked_sanitized_message(Message {
            account_keys: vec![Pubkey::new_from_array([0; 32])],
            header: MessageHeader::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![],
            }],
            recent_blockhash: Hash::default(),
        });

        // Transactions, length 2.
        let sanitized_txs = vec![
            SanitizedTransaction::new_for_tests(
                sanitized_message,
                vec![Signature::new_unique()],
                false,
            );
            2
        ];

        let check_results = vec![
            TransactionCheckResult::Ok(CheckedTransactionDetails::default());
            check_results_len
        ];

        let batch_processor = TransactionBatchProcessor::<TestForkGraph>::default();
        let callback = MockBankCallback::default();

        batch_processor.load_and_execute_sanitized_transactions(
            &callback,
            &sanitized_txs,
            check_results,
            &TransactionProcessingEnvironment::default(),
            &TransactionProcessingConfig::default(),
        );
    }

    #[test]
    fn test_inner_instructions_list_from_instruction_trace() {
        let instruction_trace = [1, 2, 1, 1, 2, 3, 2];
        let mut transaction_context =
            TransactionContext::new(vec![], Rent::default(), 3, instruction_trace.len());
        for (index_in_trace, stack_height) in instruction_trace.into_iter().enumerate() {
            while stack_height <= transaction_context.get_instruction_context_stack_height() {
                transaction_context.pop().unwrap();
            }
            if stack_height > transaction_context.get_instruction_context_stack_height() {
                transaction_context
                    .get_next_instruction_context()
                    .unwrap()
                    .configure(&[], &[], &[index_in_trace as u8]);
                transaction_context.push().unwrap();
            }
        }
        let inner_instructions =
            TransactionBatchProcessor::<TestForkGraph>::inner_instructions_list_from_instruction_trace(
                &transaction_context,
            );

        assert_eq!(
            inner_instructions,
            vec![
                vec![InnerInstruction {
                    instruction: CompiledInstruction::new_from_raw_parts(0, vec![1], vec![]),
                    stack_height: 2,
                }],
                vec![],
                vec![
                    InnerInstruction {
                        instruction: CompiledInstruction::new_from_raw_parts(0, vec![4], vec![]),
                        stack_height: 2,
                    },
                    InnerInstruction {
                        instruction: CompiledInstruction::new_from_raw_parts(0, vec![5], vec![]),
                        stack_height: 3,
                    },
                    InnerInstruction {
                        instruction: CompiledInstruction::new_from_raw_parts(0, vec![6], vec![]),
                        stack_height: 2,
                    },
                ]
            ]
        );
    }

    #[test]
    fn test_execute_loaded_transaction_recordings() {
        // Setting all the arguments correctly is too burdensome for testing
        // execute_loaded_transaction separately.This function will be tested in an integration
        // test with load_and_execute_sanitized_transactions
        let message = Message {
            account_keys: vec![Pubkey::new_from_array([0; 32])],
            header: MessageHeader::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![],
            }],
            recent_blockhash: Hash::default(),
        };

        let sanitized_message = new_unchecked_sanitized_message(message);
        let mut program_cache_for_tx_batch = ProgramCacheForTxBatch::default();
        let batch_processor = TransactionBatchProcessor::<TestForkGraph>::default();

        let sanitized_transaction = SanitizedTransaction::new_for_tests(
            sanitized_message,
            vec![Signature::new_unique()],
            false,
        );

        let loaded_transaction = LoadedTransaction {
            accounts: vec![(Pubkey::new_unique(), AccountSharedData::default())],
            program_indices: vec![vec![0]],
            fee_details: FeeDetails::default(),
            rollback_accounts: RollbackAccounts::default(),
            compute_budget_limits: ComputeBudgetLimits::default(),
            rent: 0,
            rent_debits: RentDebits::default(),
            loaded_accounts_data_size: 32,
        };

        let processing_environment = TransactionProcessingEnvironment::default();

        let mut processing_config = TransactionProcessingConfig::default();
        processing_config.recording_config.enable_log_recording = true;

        let mock_bank = MockBankCallback::default();

        let executed_tx = batch_processor.execute_loaded_transaction(
            &mock_bank,
            &sanitized_transaction,
            loaded_transaction.clone(),
            &mut ExecuteTimings::default(),
            &mut TransactionErrorMetrics::default(),
            &mut program_cache_for_tx_batch,
            &processing_environment,
            &processing_config,
        );
        assert!(executed_tx.execution_details.log_messages.is_some());

        processing_config.log_messages_bytes_limit = Some(2);

        let executed_tx = batch_processor.execute_loaded_transaction(
            &mock_bank,
            &sanitized_transaction,
            loaded_transaction.clone(),
            &mut ExecuteTimings::default(),
            &mut TransactionErrorMetrics::default(),
            &mut program_cache_for_tx_batch,
            &processing_environment,
            &processing_config,
        );
        assert!(executed_tx.execution_details.log_messages.is_some());
        assert!(executed_tx.execution_details.inner_instructions.is_none());

        processing_config.recording_config.enable_log_recording = false;
        processing_config.recording_config.enable_cpi_recording = true;
        processing_config.log_messages_bytes_limit = None;

        let executed_tx = batch_processor.execute_loaded_transaction(
            &mock_bank,
            &sanitized_transaction,
            loaded_transaction,
            &mut ExecuteTimings::default(),
            &mut TransactionErrorMetrics::default(),
            &mut program_cache_for_tx_batch,
            &processing_environment,
            &processing_config,
        );

        assert!(executed_tx.execution_details.log_messages.is_none());
        assert!(executed_tx.execution_details.inner_instructions.is_some());
    }

    #[test]
    fn test_execute_loaded_transaction_error_metrics() {
        // Setting all the arguments correctly is too burdensome for testing
        // execute_loaded_transaction separately.This function will be tested in an integration
        // test with load_and_execute_sanitized_transactions
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let message = Message {
            account_keys: vec![key1, key2],
            header: MessageHeader::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![2],
                data: vec![],
            }],
            recent_blockhash: Hash::default(),
        };

        let sanitized_message = new_unchecked_sanitized_message(message);
        let mut program_cache_for_tx_batch = ProgramCacheForTxBatch::default();
        let batch_processor = TransactionBatchProcessor::<TestForkGraph>::default();

        let sanitized_transaction = SanitizedTransaction::new_for_tests(
            sanitized_message,
            vec![Signature::new_unique()],
            false,
        );

        let mut account_data = AccountSharedData::default();
        account_data.set_owner(bpf_loader::id());
        let loaded_transaction = LoadedTransaction {
            accounts: vec![
                (key1, AccountSharedData::default()),
                (key2, AccountSharedData::default()),
            ],
            program_indices: vec![vec![0]],
            fee_details: FeeDetails::default(),
            rollback_accounts: RollbackAccounts::default(),
            compute_budget_limits: ComputeBudgetLimits::default(),
            rent: 0,
            rent_debits: RentDebits::default(),
            loaded_accounts_data_size: 0,
        };

        let processing_config = TransactionProcessingConfig {
            recording_config: ExecutionRecordingConfig::new_single_setting(false),
            ..Default::default()
        };
        let mut error_metrics = TransactionErrorMetrics::new();
        let mock_bank = MockBankCallback::default();

        let _ = batch_processor.execute_loaded_transaction(
            &mock_bank,
            &sanitized_transaction,
            loaded_transaction,
            &mut ExecuteTimings::default(),
            &mut error_metrics,
            &mut program_cache_for_tx_batch,
            &TransactionProcessingEnvironment::default(),
            &processing_config,
        );

        assert_eq!(error_metrics.instruction_error.0, 1);
    }

    #[test]
    #[should_panic = "called load_program_with_pubkey() with nonexistent account"]
    fn test_replenish_program_cache_with_nonexistent_accounts() {
        let mock_bank = MockBankCallback::default();
        let fork_graph = Arc::new(RwLock::new(TestForkGraph {}));
        let batch_processor =
            TransactionBatchProcessor::new(0, 0, Arc::downgrade(&fork_graph), None, None);
        let key = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let mut account_maps: HashMap<Pubkey, (&Pubkey, u64)> = HashMap::new();
        account_maps.insert(key, (&owner, 4));

        batch_processor.replenish_program_cache(
            &mock_bank,
            &account_maps,
            &mut ExecuteTimings::default(),
            false,
            true,
        );
    }

    #[test]
    fn test_replenish_program_cache() {
        let mock_bank = MockBankCallback::default();
        let fork_graph = Arc::new(RwLock::new(TestForkGraph {}));
        let batch_processor =
            TransactionBatchProcessor::new(0, 0, Arc::downgrade(&fork_graph), None, None);
        let key = Pubkey::new_unique();
        let owner = Pubkey::new_unique();

        let mut account_data = AccountSharedData::default();
        account_data.set_owner(bpf_loader::id());
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(key, account_data);

        let mut account_maps: HashMap<Pubkey, (&Pubkey, u64)> = HashMap::new();
        account_maps.insert(key, (&owner, 4));
        let mut loaded_missing = 0;

        for limit_to_load_programs in [false, true] {
            let result = batch_processor.replenish_program_cache(
                &mock_bank,
                &account_maps,
                &mut ExecuteTimings::default(),
                false,
                limit_to_load_programs,
            );
            assert!(!result.hit_max_limit);
            if result.loaded_missing {
                loaded_missing += 1;
            }

            let program = result.find(&key).unwrap();
            assert!(matches!(
                program.program,
                ProgramCacheEntryType::FailedVerification(_)
            ));
        }
        assert!(loaded_missing > 0);
    }

    #[test]
    fn test_filter_executable_program_accounts() {
        let mock_bank = MockBankCallback::default();
        let key1 = Pubkey::new_unique();
        let owner1 = Pubkey::new_unique();

        let mut data = AccountSharedData::default();
        data.set_owner(owner1);
        data.set_lamports(93);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(key1, data);

        let message = Message {
            account_keys: vec![key1],
            header: MessageHeader::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![],
            }],
            recent_blockhash: Hash::default(),
        };

        let sanitized_message = new_unchecked_sanitized_message(message);

        let sanitized_transaction_1 = SanitizedTransaction::new_for_tests(
            sanitized_message,
            vec![Signature::new_unique()],
            false,
        );

        let key2 = Pubkey::new_unique();
        let owner2 = Pubkey::new_unique();

        let mut account_data = AccountSharedData::default();
        account_data.set_owner(owner2);
        account_data.set_lamports(90);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(key2, account_data);

        let message = Message {
            account_keys: vec![key1, key2],
            header: MessageHeader::default(),
            instructions: vec![CompiledInstruction {
                program_id_index: 0,
                accounts: vec![],
                data: vec![],
            }],
            recent_blockhash: Hash::default(),
        };

        let sanitized_message = new_unchecked_sanitized_message(message);

        let sanitized_transaction_2 = SanitizedTransaction::new_for_tests(
            sanitized_message,
            vec![Signature::new_unique()],
            false,
        );

        let transactions = vec![
            sanitized_transaction_1.clone(),
            sanitized_transaction_2.clone(),
            sanitized_transaction_1,
        ];
        let check_results = vec![
            Ok(CheckedTransactionDetails::default()),
            Ok(CheckedTransactionDetails::default()),
            Err(TransactionError::ProgramAccountNotFound),
        ];
        let owners = vec![owner1, owner2];

        let result = TransactionBatchProcessor::<TestForkGraph>::filter_executable_program_accounts(
            &mock_bank,
            &transactions,
            &check_results,
            &owners,
        );

        assert_eq!(result.len(), 2);
        assert_eq!(result[&key1], (&owner1, 2));
        assert_eq!(result[&key2], (&owner2, 1));
    }

    #[test]
    fn test_filter_executable_program_accounts_no_errors() {
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let non_program_pubkey1 = Pubkey::new_unique();
        let non_program_pubkey2 = Pubkey::new_unique();
        let program1_pubkey = Pubkey::new_unique();
        let program2_pubkey = Pubkey::new_unique();
        let account1_pubkey = Pubkey::new_unique();
        let account2_pubkey = Pubkey::new_unique();
        let account3_pubkey = Pubkey::new_unique();
        let account4_pubkey = Pubkey::new_unique();

        let account5_pubkey = Pubkey::new_unique();

        let bank = MockBankCallback::default();
        bank.account_shared_data.write().unwrap().insert(
            non_program_pubkey1,
            AccountSharedData::new(1, 10, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            non_program_pubkey2,
            AccountSharedData::new(1, 10, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            program1_pubkey,
            AccountSharedData::new(40, 1, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            program2_pubkey,
            AccountSharedData::new(40, 1, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            account1_pubkey,
            AccountSharedData::new(1, 10, &non_program_pubkey1),
        );
        bank.account_shared_data.write().unwrap().insert(
            account2_pubkey,
            AccountSharedData::new(1, 10, &non_program_pubkey2),
        );
        bank.account_shared_data.write().unwrap().insert(
            account3_pubkey,
            AccountSharedData::new(40, 1, &program1_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            account4_pubkey,
            AccountSharedData::new(40, 1, &program2_pubkey),
        );

        let tx1 = Transaction::new_with_compiled_instructions(
            &[&keypair1],
            &[non_program_pubkey1],
            Hash::new_unique(),
            vec![account1_pubkey, account2_pubkey, account3_pubkey],
            vec![CompiledInstruction::new(1, &(), vec![0])],
        );
        let sanitized_tx1 = SanitizedTransaction::from_transaction_for_tests(tx1);

        let tx2 = Transaction::new_with_compiled_instructions(
            &[&keypair2],
            &[non_program_pubkey2],
            Hash::new_unique(),
            vec![account4_pubkey, account3_pubkey, account2_pubkey],
            vec![CompiledInstruction::new(1, &(), vec![0])],
        );
        let sanitized_tx2 = SanitizedTransaction::from_transaction_for_tests(tx2);

        let owners = &[program1_pubkey, program2_pubkey];
        let programs =
            TransactionBatchProcessor::<TestForkGraph>::filter_executable_program_accounts(
                &bank,
                &[sanitized_tx1, sanitized_tx2],
                &[
                    Ok(CheckedTransactionDetails::default()),
                    Ok(CheckedTransactionDetails::default()),
                ],
                owners,
            );

        // The result should contain only account3_pubkey, and account4_pubkey as the program accounts
        assert_eq!(programs.len(), 2);
        assert_eq!(
            programs
                .get(&account3_pubkey)
                .expect("failed to find the program account"),
            &(&program1_pubkey, 2)
        );
        assert_eq!(
            programs
                .get(&account4_pubkey)
                .expect("failed to find the program account"),
            &(&program2_pubkey, 1)
        );
    }

    #[test]
    fn test_filter_executable_program_accounts_invalid_blockhash() {
        let keypair1 = Keypair::new();
        let keypair2 = Keypair::new();

        let non_program_pubkey1 = Pubkey::new_unique();
        let non_program_pubkey2 = Pubkey::new_unique();
        let program1_pubkey = Pubkey::new_unique();
        let program2_pubkey = Pubkey::new_unique();
        let account1_pubkey = Pubkey::new_unique();
        let account2_pubkey = Pubkey::new_unique();
        let account3_pubkey = Pubkey::new_unique();
        let account4_pubkey = Pubkey::new_unique();

        let account5_pubkey = Pubkey::new_unique();

        let bank = MockBankCallback::default();
        bank.account_shared_data.write().unwrap().insert(
            non_program_pubkey1,
            AccountSharedData::new(1, 10, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            non_program_pubkey2,
            AccountSharedData::new(1, 10, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            program1_pubkey,
            AccountSharedData::new(40, 1, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            program2_pubkey,
            AccountSharedData::new(40, 1, &account5_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            account1_pubkey,
            AccountSharedData::new(1, 10, &non_program_pubkey1),
        );
        bank.account_shared_data.write().unwrap().insert(
            account2_pubkey,
            AccountSharedData::new(1, 10, &non_program_pubkey2),
        );
        bank.account_shared_data.write().unwrap().insert(
            account3_pubkey,
            AccountSharedData::new(40, 1, &program1_pubkey),
        );
        bank.account_shared_data.write().unwrap().insert(
            account4_pubkey,
            AccountSharedData::new(40, 1, &program2_pubkey),
        );

        let tx1 = Transaction::new_with_compiled_instructions(
            &[&keypair1],
            &[non_program_pubkey1],
            Hash::new_unique(),
            vec![account1_pubkey, account2_pubkey, account3_pubkey],
            vec![CompiledInstruction::new(1, &(), vec![0])],
        );
        let sanitized_tx1 = SanitizedTransaction::from_transaction_for_tests(tx1);

        let tx2 = Transaction::new_with_compiled_instructions(
            &[&keypair2],
            &[non_program_pubkey2],
            Hash::new_unique(),
            vec![account4_pubkey, account3_pubkey, account2_pubkey],
            vec![CompiledInstruction::new(1, &(), vec![0])],
        );
        // Let's not register blockhash from tx2. This should cause the tx2 to fail
        let sanitized_tx2 = SanitizedTransaction::from_transaction_for_tests(tx2);

        let owners = &[program1_pubkey, program2_pubkey];
        let check_results = vec![
            Ok(CheckedTransactionDetails::default()),
            Err(TransactionError::BlockhashNotFound),
        ];
        let programs =
            TransactionBatchProcessor::<TestForkGraph>::filter_executable_program_accounts(
                &bank,
                &[sanitized_tx1, sanitized_tx2],
                &check_results,
                owners,
            );

        // The result should contain only account3_pubkey as the program accounts
        assert_eq!(programs.len(), 1);
        assert_eq!(
            programs
                .get(&account3_pubkey)
                .expect("failed to find the program account"),
            &(&program1_pubkey, 1)
        );
    }

    #[test]
    #[allow(deprecated)]
    fn test_sysvar_cache_initialization1() {
        let mock_bank = MockBankCallback::default();

        let clock = Clock {
            slot: 1,
            epoch_start_timestamp: 2,
            epoch: 3,
            leader_schedule_epoch: 4,
            unix_timestamp: 5,
        };
        let clock_account = create_account_shared_data_for_test(&clock);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::clock::id(), clock_account);

        let epoch_schedule = EpochSchedule::custom(64, 2, true);
        let epoch_schedule_account = create_account_shared_data_for_test(&epoch_schedule);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::epoch_schedule::id(), epoch_schedule_account);

        let fees = Fees {
            fee_calculator: FeeCalculator {
                lamports_per_signature: 123,
            },
        };
        let fees_account = create_account_shared_data_for_test(&fees);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::fees::id(), fees_account);

        let rent = Rent::with_slots_per_epoch(2048);
        let rent_account = create_account_shared_data_for_test(&rent);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::rent::id(), rent_account);

        let transaction_processor = TransactionBatchProcessor::<TestForkGraph>::default();
        transaction_processor.fill_missing_sysvar_cache_entries(&mock_bank);

        let sysvar_cache = transaction_processor.sysvar_cache.read().unwrap();
        let cached_clock = sysvar_cache.get_clock();
        let cached_epoch_schedule = sysvar_cache.get_epoch_schedule();
        let cached_fees = sysvar_cache.get_fees();
        let cached_rent = sysvar_cache.get_rent();

        assert_eq!(
            cached_clock.expect("clock sysvar missing in cache"),
            clock.into()
        );
        assert_eq!(
            cached_epoch_schedule.expect("epoch_schedule sysvar missing in cache"),
            epoch_schedule.into()
        );
        assert_eq!(
            cached_fees.expect("fees sysvar missing in cache"),
            fees.into()
        );
        assert_eq!(
            cached_rent.expect("rent sysvar missing in cache"),
            rent.into()
        );
        assert!(sysvar_cache.get_slot_hashes().is_err());
        assert!(sysvar_cache.get_epoch_rewards().is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn test_reset_and_fill_sysvar_cache() {
        let mock_bank = MockBankCallback::default();

        let clock = Clock {
            slot: 1,
            epoch_start_timestamp: 2,
            epoch: 3,
            leader_schedule_epoch: 4,
            unix_timestamp: 5,
        };
        let clock_account = create_account_shared_data_for_test(&clock);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::clock::id(), clock_account);

        let epoch_schedule = EpochSchedule::custom(64, 2, true);
        let epoch_schedule_account = create_account_shared_data_for_test(&epoch_schedule);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::epoch_schedule::id(), epoch_schedule_account);

        let fees = Fees {
            fee_calculator: FeeCalculator {
                lamports_per_signature: 123,
            },
        };
        let fees_account = create_account_shared_data_for_test(&fees);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::fees::id(), fees_account);

        let rent = Rent::with_slots_per_epoch(2048);
        let rent_account = create_account_shared_data_for_test(&rent);
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(sysvar::rent::id(), rent_account);

        let transaction_processor = TransactionBatchProcessor::<TestForkGraph>::default();
        // Fill the sysvar cache
        transaction_processor.fill_missing_sysvar_cache_entries(&mock_bank);
        // Reset the sysvar cache
        transaction_processor.reset_sysvar_cache();

        {
            let sysvar_cache = transaction_processor.sysvar_cache.read().unwrap();
            // Test that sysvar cache is empty and none of the values are found
            assert!(sysvar_cache.get_clock().is_err());
            assert!(sysvar_cache.get_epoch_schedule().is_err());
            assert!(sysvar_cache.get_fees().is_err());
            assert!(sysvar_cache.get_epoch_rewards().is_err());
            assert!(sysvar_cache.get_rent().is_err());
            assert!(sysvar_cache.get_epoch_rewards().is_err());
        }

        // Refill the cache and test the values are available.
        transaction_processor.fill_missing_sysvar_cache_entries(&mock_bank);

        let sysvar_cache = transaction_processor.sysvar_cache.read().unwrap();
        let cached_clock = sysvar_cache.get_clock();
        let cached_epoch_schedule = sysvar_cache.get_epoch_schedule();
        let cached_fees = sysvar_cache.get_fees();
        let cached_rent = sysvar_cache.get_rent();

        assert_eq!(
            cached_clock.expect("clock sysvar missing in cache"),
            clock.into()
        );
        assert_eq!(
            cached_epoch_schedule.expect("epoch_schedule sysvar missing in cache"),
            epoch_schedule.into()
        );
        assert_eq!(
            cached_fees.expect("fees sysvar missing in cache"),
            fees.into()
        );
        assert_eq!(
            cached_rent.expect("rent sysvar missing in cache"),
            rent.into()
        );
        assert!(sysvar_cache.get_slot_hashes().is_err());
        assert!(sysvar_cache.get_epoch_rewards().is_err());
    }

    #[test]
    fn test_add_builtin() {
        let mock_bank = MockBankCallback::default();
        let fork_graph = Arc::new(RwLock::new(TestForkGraph {}));
        let batch_processor =
            TransactionBatchProcessor::new(0, 0, Arc::downgrade(&fork_graph), None, None);

        let key = Pubkey::new_unique();
        let name = "a_builtin_name";
        let program = ProgramCacheEntry::new_builtin(
            0,
            name.len(),
            |_invoke_context, _param0, _param1, _param2, _param3, _param4| {},
        );

        batch_processor.add_builtin(&mock_bank, key, name, program);

        assert_eq!(
            mock_bank.account_shared_data.read().unwrap()[&key].data(),
            name.as_bytes()
        );

        let mut loaded_programs_for_tx_batch = ProgramCacheForTxBatch::new_from_cache(
            0,
            0,
            &batch_processor.program_cache.read().unwrap(),
        );
        batch_processor.program_cache.write().unwrap().extract(
            &mut vec![(key, (ProgramCacheMatchCriteria::NoCriteria, 1))],
            &mut loaded_programs_for_tx_batch,
            true,
        );
        let entry = loaded_programs_for_tx_batch.find(&key).unwrap();

        // Repeating code because ProgramCacheEntry does not implement clone.
        let program = ProgramCacheEntry::new_builtin(
            0,
            name.len(),
            |_invoke_context, _param0, _param1, _param2, _param3, _param4| {},
        );
        assert_eq!(entry, Arc::new(program));
    }

    #[test]
    fn test_validate_transaction_fee_payer_exact_balance() {
        let lamports_per_signature = 5000;
        let message = new_unchecked_sanitized_message(Message::new_with_blockhash(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(2000u32),
                ComputeBudgetInstruction::set_compute_unit_price(1_000_000_000),
            ],
            Some(&Pubkey::new_unique()),
            &Hash::new_unique(),
        ));
        let compute_budget_limits = process_compute_budget_instructions(
            SVMMessage::program_instructions_iter(&message),
            &FeatureSet::default(),
        )
        .unwrap();
        let fee_payer_address = message.fee_payer();
        let current_epoch = 42;
        let rent_collector = RentCollector {
            epoch: current_epoch,
            ..RentCollector::default()
        };
        let min_balance = rent_collector
            .rent
            .minimum_balance(nonce::state::State::size());
        let transaction_fee = lamports_per_signature;
        let priority_fee = 2_000_000u64;
        let starting_balance = transaction_fee + priority_fee;
        assert!(
            starting_balance > min_balance,
            "we're testing that a rent exempt fee payer can be fully drained, \
        so ensure that the starting balance is more than the min balance"
        );

        let fee_payer_rent_epoch = current_epoch;
        let fee_payer_rent_debit = 0;
        let fee_payer_account = AccountSharedData::new_rent_epoch(
            starting_balance,
            0,
            &Pubkey::default(),
            fee_payer_rent_epoch,
        );
        let mut mock_accounts = HashMap::new();
        mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
        let mock_bank = MockBankCallback {
            account_shared_data: Arc::new(RwLock::new(mock_accounts)),
            ..Default::default()
        };
        let mut account_loader = (&mock_bank).into();

        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(compute_budget_limits),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &rent_collector,
                &mut error_counters,
                &mock_bank,
            );

        let post_validation_fee_payer_account = {
            let mut account = fee_payer_account.clone();
            account.set_rent_epoch(RENT_EXEMPT_RENT_EPOCH);
            account.set_lamports(0);
            account
        };

        assert_eq!(
            result,
            Ok(ValidatedTransactionDetails {
                rollback_accounts: RollbackAccounts::new(
                    None, // nonce
                    *fee_payer_address,
                    post_validation_fee_payer_account.clone(),
                    fee_payer_rent_debit,
                    fee_payer_rent_epoch
                ),
                compute_budget_limits,
                fee_details: FeeDetails::new(transaction_fee, priority_fee),
                loaded_fee_payer_account: LoadedTransactionAccount {
                    loaded_size: fee_payer_account.data().len(),
                    account: post_validation_fee_payer_account,
                    rent_collected: fee_payer_rent_debit,
                },
            })
        );
    }

    #[test]
    fn test_validate_transaction_fee_payer_rent_paying() {
        let lamports_per_signature = 5000;
        let message = new_unchecked_sanitized_message(Message::new_with_blockhash(
            &[],
            Some(&Pubkey::new_unique()),
            &Hash::new_unique(),
        ));
        let compute_budget_limits = process_compute_budget_instructions(
            SVMMessage::program_instructions_iter(&message),
            &FeatureSet::default(),
        )
        .unwrap();
        let fee_payer_address = message.fee_payer();
        let mut rent_collector = RentCollector::default();
        rent_collector.rent.lamports_per_byte_year = 1_000_000;
        let min_balance = rent_collector.rent.minimum_balance(0);
        let transaction_fee = lamports_per_signature;
        let starting_balance = min_balance - 1;
        let fee_payer_account = AccountSharedData::new(starting_balance, 0, &Pubkey::default());
        let fee_payer_rent_debit = rent_collector
            .get_rent_due(
                fee_payer_account.lamports(),
                fee_payer_account.data().len(),
                fee_payer_account.rent_epoch(),
            )
            .lamports();
        assert!(fee_payer_rent_debit > 0);

        let mut mock_accounts = HashMap::new();
        mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
        let mock_bank = MockBankCallback {
            account_shared_data: Arc::new(RwLock::new(mock_accounts)),
            ..Default::default()
        };
        let mut account_loader = (&mock_bank).into();

        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(compute_budget_limits),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &rent_collector,
                &mut error_counters,
                &mock_bank,
            );

        let post_validation_fee_payer_account = {
            let mut account = fee_payer_account.clone();
            account.set_rent_epoch(1);
            account.set_lamports(starting_balance - transaction_fee - fee_payer_rent_debit);
            account
        };

        assert_eq!(
            result,
            Ok(ValidatedTransactionDetails {
                rollback_accounts: RollbackAccounts::new(
                    None, // nonce
                    *fee_payer_address,
                    post_validation_fee_payer_account.clone(),
                    fee_payer_rent_debit,
                    0, // rent epoch
                ),
                compute_budget_limits,
                fee_details: FeeDetails::new(transaction_fee, 0),
                loaded_fee_payer_account: LoadedTransactionAccount {
                    loaded_size: fee_payer_account.data().len(),
                    account: post_validation_fee_payer_account,
                    rent_collected: fee_payer_rent_debit,
                }
            })
        );
    }

    #[test]
    fn test_validate_transaction_fee_payer_not_found() {
        let lamports_per_signature = 5000;
        let message =
            new_unchecked_sanitized_message(Message::new(&[], Some(&Pubkey::new_unique())));

        let mock_bank = MockBankCallback::default();
        let mut account_loader = (&mock_bank).into();
        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(ComputeBudgetLimits::default()),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &RentCollector::default(),
                &mut error_counters,
                &mock_bank,
            );

        assert_eq!(error_counters.account_not_found.0, 1);
        assert_eq!(result, Err(TransactionError::AccountNotFound));
    }

    #[test]
    fn test_validate_transaction_fee_payer_insufficient_funds() {
        let lamports_per_signature = 5000;
        let message =
            new_unchecked_sanitized_message(Message::new(&[], Some(&Pubkey::new_unique())));
        let fee_payer_address = message.fee_payer();
        let fee_payer_account = AccountSharedData::new(1, 0, &Pubkey::default());
        let mut mock_accounts = HashMap::new();
        mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
        let mock_bank = MockBankCallback {
            account_shared_data: Arc::new(RwLock::new(mock_accounts)),
            ..Default::default()
        };
        let mut account_loader = (&mock_bank).into();

        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(ComputeBudgetLimits::default()),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &RentCollector::default(),
                &mut error_counters,
                &mock_bank,
            );

        assert_eq!(error_counters.insufficient_funds.0, 1);
        assert_eq!(result, Err(TransactionError::InsufficientFundsForFee));
    }

    #[test]
    fn test_validate_transaction_fee_payer_insufficient_rent() {
        let lamports_per_signature = 5000;
        let message =
            new_unchecked_sanitized_message(Message::new(&[], Some(&Pubkey::new_unique())));
        let fee_payer_address = message.fee_payer();
        let transaction_fee = lamports_per_signature;
        let rent_collector = RentCollector::default();
        let min_balance = rent_collector.rent.minimum_balance(0);
        let starting_balance = min_balance + transaction_fee - 1;
        let fee_payer_account = AccountSharedData::new(starting_balance, 0, &Pubkey::default());
        let mut mock_accounts = HashMap::new();
        mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
        let mock_bank = MockBankCallback {
            account_shared_data: Arc::new(RwLock::new(mock_accounts)),
            ..Default::default()
        };
        let mut account_loader = (&mock_bank).into();

        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(ComputeBudgetLimits::default()),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &rent_collector,
                &mut error_counters,
                &mock_bank,
            );

        assert_eq!(
            result,
            Err(TransactionError::InsufficientFundsForRent { account_index: 0 })
        );
    }

    #[test]
    fn test_validate_transaction_fee_payer_invalid() {
        let lamports_per_signature = 5000;
        let message =
            new_unchecked_sanitized_message(Message::new(&[], Some(&Pubkey::new_unique())));
        let fee_payer_address = message.fee_payer();
        let fee_payer_account = AccountSharedData::new(1_000_000, 0, &Pubkey::new_unique());
        let mut mock_accounts = HashMap::new();
        mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
        let mock_bank = MockBankCallback {
            account_shared_data: Arc::new(RwLock::new(mock_accounts)),
            ..Default::default()
        };
        let mut account_loader = (&mock_bank).into();

        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(
                    None,
                    lamports_per_signature,
                    Ok(ComputeBudgetLimits::default()),
                ),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &RentCollector::default(),
                &mut error_counters,
                &mock_bank,
            );

        assert_eq!(error_counters.invalid_account_for_fee.0, 1);
        assert_eq!(result, Err(TransactionError::InvalidAccountForFee));
    }

    #[test]
    fn test_validate_transaction_fee_payer_invalid_compute_budget() {
        let lamports_per_signature = 5000;
        let message = new_unchecked_sanitized_message(Message::new(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(2000u32),
                ComputeBudgetInstruction::set_compute_unit_limit(42u32),
            ],
            Some(&Pubkey::new_unique()),
        ));
        let compute_budget_limits = process_compute_budget_instructions(
            SVMMessage::program_instructions_iter(&message),
            &FeatureSet::default(),
        );

        let mock_bank = MockBankCallback::default();
        let mut account_loader = (&mock_bank).into();
        let mut error_counters = TransactionErrorMetrics::default();
        let result =
            TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(None, lamports_per_signature, compute_budget_limits),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &RentCollector::default(),
                &mut error_counters,
                &mock_bank,
            );

        assert_eq!(error_counters.invalid_compute_budget.0, 1);
        assert_eq!(result, Err(TransactionError::DuplicateInstruction(1u8)));
    }

    #[test]
    fn test_validate_transaction_fee_payer_is_nonce() {
        let lamports_per_signature = 5000;
        let rent_collector = RentCollector::default();
        let compute_unit_limit = 2 * solana_compute_budget_program::DEFAULT_COMPUTE_UNITS;
        let last_blockhash = Hash::new_unique();
        let message = new_unchecked_sanitized_message(Message::new_with_blockhash(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit as u32),
                ComputeBudgetInstruction::set_compute_unit_price(1_000_000),
            ],
            Some(&Pubkey::new_unique()),
            &last_blockhash,
        ));
        let compute_budget_limits = process_compute_budget_instructions(
            SVMMessage::program_instructions_iter(&message),
            &FeatureSet::default(),
        )
        .unwrap();
        let fee_payer_address = message.fee_payer();
        let min_balance = Rent::default().minimum_balance(nonce::state::State::size());
        let transaction_fee = lamports_per_signature;
        let priority_fee = compute_unit_limit;

        // Sufficient Fees
        {
            let fee_payer_account = AccountSharedData::new_data(
                min_balance + transaction_fee + priority_fee,
                &nonce::versions::Versions::new(nonce::state::State::Initialized(
                    nonce::state::Data::new(
                        *fee_payer_address,
                        DurableNonce::default(),
                        lamports_per_signature,
                    ),
                )),
                &system_program::id(),
            )
            .unwrap();

            let mut mock_accounts = HashMap::new();
            mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
            let mock_bank = MockBankCallback {
                account_shared_data: Arc::new(RwLock::new(mock_accounts)),
                ..Default::default()
            };
            let mut account_loader = (&mock_bank).into();

            let mut error_counters = TransactionErrorMetrics::default();

            let environment_blockhash = Hash::new_unique();
            let next_durable_nonce = DurableNonce::from_blockhash(&environment_blockhash);
            let mut future_nonce = NonceInfo::new(*fee_payer_address, fee_payer_account.clone());
            future_nonce
                .try_advance_nonce(next_durable_nonce, lamports_per_signature)
                .unwrap();

            let tx_details = CheckedTransactionDetails::new(
                Some(future_nonce.clone()),
                lamports_per_signature,
                Ok(compute_budget_limits),
            );

            let result = TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                tx_details,
                &environment_blockhash,
                FeeStructure::default().lamports_per_signature,
                &rent_collector,
                &mut error_counters,
                &mock_bank,
            );

            let post_validation_fee_payer_account = {
                let mut account = fee_payer_account.clone();
                account.set_rent_epoch(RENT_EXEMPT_RENT_EPOCH);
                account.set_lamports(min_balance);
                account
            };

            assert_eq!(
                result,
                Ok(ValidatedTransactionDetails {
                    rollback_accounts: RollbackAccounts::new(
                        Some(future_nonce),
                        *fee_payer_address,
                        post_validation_fee_payer_account.clone(),
                        0, // fee_payer_rent_debit
                        0, // fee_payer_rent_epoch
                    ),
                    compute_budget_limits,
                    fee_details: FeeDetails::new(transaction_fee, priority_fee),
                    loaded_fee_payer_account: LoadedTransactionAccount {
                        loaded_size: fee_payer_account.data().len(),
                        account: post_validation_fee_payer_account,
                        rent_collected: 0,
                    }
                })
            );
        }

        // Insufficient Fees
        {
            let fee_payer_account = AccountSharedData::new_data(
                transaction_fee + priority_fee, // no min_balance this time
                &nonce::versions::Versions::new(nonce::state::State::Initialized(
                    nonce::state::Data::default(),
                )),
                &system_program::id(),
            )
            .unwrap();

            let mut mock_accounts = HashMap::new();
            mock_accounts.insert(*fee_payer_address, fee_payer_account.clone());
            let mock_bank = MockBankCallback {
                account_shared_data: Arc::new(RwLock::new(mock_accounts)),
                ..Default::default()
            };
            let mut account_loader = (&mock_bank).into();

            let mut error_counters = TransactionErrorMetrics::default();
            let result = TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
                &mut account_loader,
                &message,
                CheckedTransactionDetails::new(None, lamports_per_signature, Ok(compute_budget_limits)),
                &Hash::default(),
                FeeStructure::default().lamports_per_signature,
                &rent_collector,
                &mut error_counters,
                &mock_bank,
            );

            assert_eq!(error_counters.insufficient_funds.0, 1);
            assert_eq!(result, Err(TransactionError::InsufficientFundsForFee));
        }
    }

    // Ensure `TransactionProcessingCallback::inspect_account()` is called when
    // validating the fee payer, since that's when the fee payer account is loaded.
    #[test]
    fn test_inspect_account_fee_payer() {
        let fee_payer_address = Pubkey::new_unique();
        let fee_payer_account = AccountSharedData::new_rent_epoch(
            123_000_000_000,
            0,
            &Pubkey::default(),
            RENT_EXEMPT_RENT_EPOCH,
        );
        let mock_bank = MockBankCallback::default();
        mock_bank
            .account_shared_data
            .write()
            .unwrap()
            .insert(fee_payer_address, fee_payer_account.clone());
        let mut account_loader = (&mock_bank).into();

        let message = new_unchecked_sanitized_message(Message::new_with_blockhash(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(2000u32),
                ComputeBudgetInstruction::set_compute_unit_price(1_000_000_000),
            ],
            Some(&fee_payer_address),
            &Hash::new_unique(),
        ));
        let compute_budget_limits = process_compute_budget_instructions(
            SVMMessage::program_instructions_iter(&message),
            &FeatureSet::default(),
        )
        .unwrap();
        TransactionBatchProcessor::<TestForkGraph>::validate_transaction_nonce_and_fee_payer(
            &mut account_loader,
            &message,
            CheckedTransactionDetails::new(None, 5000, Ok(compute_budget_limits)),
            &Hash::default(),
            FeeStructure::default().lamports_per_signature,
            &RentCollector::default(),
            &mut TransactionErrorMetrics::default(),
            &mock_bank,
        )
        .unwrap();

        // ensure the fee payer is an inspected account
        let actual_inspected_accounts: Vec<_> = mock_bank
            .inspected_accounts
            .read()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect();
        assert_eq!(
            actual_inspected_accounts.as_slice(),
            &[(fee_payer_address, vec![(Some(fee_payer_account), true)])],
        );
    }
}
