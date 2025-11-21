// // //! The `bundle_stage` processes bundles, which are list of transactions to be executed
// // //! sequentially and atomically.

// use std::{num::Saturating, sync::RwLock, time::Instant};

// use solana_poh::poh_recorder::PohRecorder;

// use crate::{
//     banking_stage::decision_maker::DecisionMaker,
//     bundle_stage::bundle_stage_leader_metrics::BundleStageLeaderMetrics,
// };

use {
    crate::{
        banking_stage::{
            committer::{CommitTransactionDetails, Committer},
            consumer::ProcessTransactionBatchOutput,
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            qos_service::QosService,
            scheduler_messages::MaxAge,
        },
        bundle_stage::{
            bundle_account_locker::BundleAccountLocker,
            bundle_consumer::BundleConsumer,
            bundle_storage::{BundleStorage, BundleStorageEntry, BundleStorageError},
        },
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::{self, TipManager},
    },
    ahash::HashSet,
    anchor_lang::solana_program::clock::Slot,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure_us,
    solana_poh::{poh_recorder::PohRecorder, transaction_recorder::TransactionRecorder},
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_transaction::TransactionError,
    std::{
        collections::VecDeque,
        num::{NonZeroUsize, Saturating},
        ops::Deref,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub mod bundle_account_locker;
mod bundle_consumer;
mod bundle_packet_deserializer;
mod bundle_storage;
const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(40);
const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

// Stats emitted periodically
pub struct BundleStageLoopMetrics {
    last_report: Instant,
    id: u32,

    // total received
    num_bundles_received: Saturating<u64>,
    num_packets_received: Saturating<u64>,

    // newly buffered
    newly_buffered_bundles_count: Saturating<u64>,

    // currently buffered
    current_buffered_bundles_count: Saturating<u64>,
    current_buffered_packets_count: Saturating<u64>,

    // buffered due to cost model
    cost_model_buffered_bundles_count: Saturating<u64>,

    // number of bundles dropped during insertion
    num_bundles_dropped: Saturating<u64>,

    // timings
    receive_and_buffer_bundles_elapsed_us: Saturating<u64>,
    process_buffered_bundles_elapsed_us: Saturating<u64>,

    num_bundles_dropped_empty_batch: Saturating<u64>,
    num_bundles_dropped_container_full: Saturating<u64>,
    num_bundles_dropped_packet_marked_discard: Saturating<u64>,
    num_bundles_dropped_packet_filter_error: Saturating<u64>,
    num_bundles_dropped_bundle_too_large: Saturating<u64>,
    num_bundles_dropped_duplicate_transaction: Saturating<u64>,
}

impl Default for BundleStageLoopMetrics {
    fn default() -> Self {
        BundleStageLoopMetrics {
            last_report: Instant::now(),
            id: 0,
            num_bundles_received: Saturating(0),
            num_packets_received: Saturating(0),
            newly_buffered_bundles_count: Saturating(0),
            current_buffered_bundles_count: Saturating(0),
            current_buffered_packets_count: Saturating(0),
            cost_model_buffered_bundles_count: Saturating(0),
            num_bundles_dropped: Saturating(0),
            receive_and_buffer_bundles_elapsed_us: Saturating(0),
            process_buffered_bundles_elapsed_us: Saturating(0),
            num_bundles_dropped_empty_batch: Saturating(0),
            num_bundles_dropped_container_full: Saturating(0),
            num_bundles_dropped_packet_marked_discard: Saturating(0),
            num_bundles_dropped_packet_filter_error: Saturating(0),
            num_bundles_dropped_bundle_too_large: Saturating(0),
            num_bundles_dropped_duplicate_transaction: Saturating(0),
        }
    }
}

impl BundleStageLoopMetrics {
    pub fn increment_num_bundles_received(&mut self, count: u64) {
        self.num_bundles_received += count;
    }

    pub fn increment_num_packets_received(&mut self, count: u64) {
        self.num_packets_received += count;
    }

    pub fn increment_newly_buffered_bundles_count(&mut self, count: u64) {
        self.newly_buffered_bundles_count += count;
    }

    pub fn set_current_buffered_bundles_count(&mut self, count: u64) {
        self.current_buffered_bundles_count = Saturating(count);
    }

    pub fn set_current_buffered_packets_count(&mut self, count: u64) {
        self.current_buffered_packets_count = Saturating(count);
    }

    pub fn set_cost_model_buffered_bundles_count(&mut self, count: u64) {
        self.cost_model_buffered_bundles_count = Saturating(count);
    }

    pub fn increment_num_bundles_dropped(&mut self, count: u64) {
        self.num_bundles_dropped += count;
    }

    pub fn increment_bundle_dropped_error(&mut self, error: BundleStorageError) {
        self.num_bundles_dropped += 1;
        match error {
            BundleStorageError::EmptyBatch => {
                self.num_bundles_dropped_empty_batch += 1;
            }
            BundleStorageError::ContainerFull => {
                self.num_bundles_dropped_container_full += 1;
            }
            BundleStorageError::PacketMarkedDiscard(_) => {
                self.num_bundles_dropped_packet_marked_discard += 1;
            }
            BundleStorageError::PacketFilterError(_) => {
                self.num_bundles_dropped_packet_filter_error += 1;
            }
            BundleStorageError::BundleTooLarge => {
                self.num_bundles_dropped_bundle_too_large += 1;
            }
            BundleStorageError::DuplicateTransaction => {
                self.num_bundles_dropped_duplicate_transaction += 1;
            }
        }
    }

    pub fn increment_receive_and_buffer_bundles_elapsed_us(&mut self, count: u64) {
        self.receive_and_buffer_bundles_elapsed_us += count;
    }

    pub fn increment_process_buffered_bundles_elapsed_us(&mut self, count: u64) {
        self.process_buffered_bundles_elapsed_us += count;
    }

    fn maybe_report(&mut self, report_interval_ms: u64) {
        if self.last_report.elapsed().as_millis() >= report_interval_ms as u128 {
            datapoint_info!(
                "bundle_stage-loop_stats",
                ("id", self.id, i64),
                (
                    "num_bundles_received",
                    self.num_bundles_received.0 as i64,
                    i64
                ),
                (
                    "num_packets_received",
                    self.num_packets_received.0 as i64,
                    i64
                ),
                (
                    "newly_buffered_bundles_count",
                    self.newly_buffered_bundles_count.0 as i64,
                    i64
                ),
                (
                    "current_buffered_bundles_count",
                    self.current_buffered_bundles_count.0 as i64,
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped",
                    self.num_bundles_dropped.0 as i64,
                    i64
                ),
                (
                    "receive_and_buffer_bundles_elapsed_us",
                    self.receive_and_buffer_bundles_elapsed_us.0 as i64,
                    i64
                ),
                (
                    "process_buffered_bundles_elapsed_us",
                    self.process_buffered_bundles_elapsed_us.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_empty_batch",
                    self.num_bundles_dropped_empty_batch.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_container_full",
                    self.num_bundles_dropped_container_full.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_packet_marked_discard",
                    self.num_bundles_dropped_packet_marked_discard.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_packet_filter_error",
                    self.num_bundles_dropped_packet_filter_error.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_bundle_too_large",
                    self.num_bundles_dropped_bundle_too_large.0 as i64,
                    i64
                ),
                (
                    "num_bundles_dropped_duplicate_transaction",
                    self.num_bundles_dropped_duplicate_transaction.0 as i64,
                    i64
                ),
            );

            self.last_report = Instant::now();
            self.clear();
        }
    }

    fn clear(&mut self) {
        self.num_bundles_received = Saturating(0);
        self.num_packets_received = Saturating(0);
        self.newly_buffered_bundles_count = Saturating(0);
        self.current_buffered_bundles_count = Saturating(0);
        self.current_buffered_packets_count = Saturating(0);
        self.cost_model_buffered_bundles_count = Saturating(0);
        self.num_bundles_dropped = Saturating(0);
        self.receive_and_buffer_bundles_elapsed_us = Saturating(0);
        self.process_buffered_bundles_elapsed_us = Saturating(0);
        self.num_bundles_dropped_empty_batch = Saturating(0);
        self.num_bundles_dropped_container_full = Saturating(0);
        self.num_bundles_dropped_packet_marked_discard = Saturating(0);
        self.num_bundles_dropped_packet_filter_error = Saturating(0);
        self.num_bundles_dropped_bundle_too_large = Saturating(0);
        self.num_bundles_dropped_duplicate_transaction = Saturating(0);
    }
}

pub struct BundleStage {
    bundle_thread: JoinHandle<()>,
}

impl BundleStage {
    const BUNDLE_STAGE_ID: u32 = 10_000;

    #[allow(clippy::new_ret_no_self)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        Self::start_bundle_thread(
            cluster_info,
            bank_forks,
            poh_recorder,
            transaction_recorder,
            bundle_receiver,
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            exit,
            tip_manager,
            bundle_account_locker,
            MAX_BUNDLE_RETRY_DURATION,
            block_builder_fee_info,
            prioritization_fee_cache,
            blacklisted_accounts,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<BankForks>>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_message_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache.clone(),
        );
        let decision_maker = DecisionMaker::from(poh_recorder.read().unwrap().deref());

        let consumer = BundleConsumer::new(
            committer,
            transaction_recorder,
            QosService::new(Self::BUNDLE_STAGE_ID),
            log_message_bytes_limit,
        );

        let bundle_thread = Builder::new()
            .name("solBundleStgTx".to_string())
            .spawn(move || {
                Self::process_loop(
                    bank_forks,
                    bundle_receiver,
                    decision_maker,
                    consumer,
                    exit,
                    blacklisted_accounts,
                    bundle_account_locker,
                    tip_manager,
                    block_builder_fee_info,
                    &cluster_info,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        bank_forks: Arc<RwLock<BankForks>>,
        mut bundle_receiver: Receiver<Vec<PacketBundle>>,
        mut decision_maker: DecisionMaker,
        mut consumer: BundleConsumer,
        exit: Arc<AtomicBool>,
        blacklisted_accounts: HashSet<Pubkey>,
        bundle_account_locker: BundleAccountLocker,
        tip_manager: TipManager,
        block_builder_fee_info: Arc<Mutex<BlockBuilderFeeInfo>>,
        cluster_info: Arc<ClusterInfo>,
    ) {
        let mut last_metrics_update = Instant::now();
        let mut bundle_storage = BundleStorage::with_capacity(2_000);

        let mut bundle_stage_metrics = BundleStageLoopMetrics::default();
        // let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(id);

        let mut last_tip_update_slot = Slot::MAX;

        while !exit.load(Ordering::Relaxed) {
            if bundle_storage.unprocessed_bundles_len() > 0
                || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
            {
                let (_, process_buffered_packets_time_us) =
                    measure_us!(Self::process_buffered_bundles(
                        &mut decision_maker,
                        &mut consumer,
                        &mut bundle_storage,
                        &bundle_account_locker,
                        &mut bundle_stage_metrics,
                        &mut last_tip_update_slot,
                        &block_builder_fee_info,
                        &tip_manager,
                        &cluster_info,
                    ));
                // bundle_stage_leader_metrics
                //         .leader_slot_metrics_tracker()
                //         .increment_process_buffered_packets_us(process_buffered_packets_time_us);
                last_metrics_update = Instant::now();
            }

            let start = Instant::now();
            if let Err(RecvTimeoutError::Disconnected) = Self::receive_and_buffer_bundles(
                &bank_forks,
                &mut bundle_receiver,
                &mut bundle_storage,
                &blacklisted_accounts,
                &mut bundle_stage_metrics,
            ) {
                break;
            }
            let elapsed = start.elapsed();

            bundle_stage_metrics
                .increment_receive_and_buffer_bundles_elapsed_us(elapsed.as_micros() as u64);

            bundle_stage_metrics.set_current_buffered_bundles_count(
                bundle_storage.unprocessed_bundles_len() as u64,
            );
            bundle_stage_metrics
                .set_current_buffered_packets_count(bundle_storage.num_packets_buffered() as u64);
            bundle_stage_metrics.set_cost_model_buffered_bundles_count(
                bundle_storage.cost_model_buffered_bundles_len() as u64,
            );

            bundle_stage_metrics.maybe_report(1_000);
        }
    }

    fn receive_and_buffer_bundles(
        bank_forks: &Arc<RwLock<BankForks>>,
        bundle_receiver: &mut Receiver<Vec<PacketBundle>>,
        bundle_storage: &mut BundleStorage,
        blacklisted_accounts: &HashSet<Pubkey>,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
    ) -> Result<(), RecvTimeoutError> {
        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };

        let recv_timeout = if bundle_storage.unprocessed_bundles_len() > 0 {
            Duration::from_millis(10)
        } else {
            Duration::from_millis(0)
        };

        let bundles = bundle_receiver.recv_timeout(recv_timeout)?;
        for bundle in bundles {
            Self::insert_bundle(
                bundle_storage,
                bundle,
                &root_bank,
                &working_bank,
                &blacklisted_accounts,
                bundle_stage_metrics,
            );
        }

        while let Ok(bundles) = bundle_receiver.try_recv() {
            for bundle in bundles {
                Self::insert_bundle(
                    bundle_storage,
                    bundle,
                    &root_bank,
                    &working_bank,
                    &blacklisted_accounts,
                    bundle_stage_metrics,
                );
            }
        }

        Ok(())
    }

    fn insert_bundle(
        bundle_storage: &mut BundleStorage,
        bundle: PacketBundle,
        root_bank: &Arc<Bank>,
        working_bank: &Arc<Bank>,
        blacklisted_accounts: &HashSet<Pubkey>,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
    ) {
        let num_packets = bundle.batch().len();

        bundle_stage_metrics.increment_num_bundles_received(1);
        bundle_stage_metrics.increment_num_packets_received(num_packets as u64);

        match bundle_storage.insert_bundle(bundle, &root_bank, &working_bank, &blacklisted_accounts)
        {
            Ok(_) => {
                bundle_stage_metrics.increment_newly_buffered_bundles_count(1);
            }
            Err(e) => {
                bundle_stage_metrics.increment_bundle_dropped_error(e);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_buffered_bundles(
        decision_maker: &mut DecisionMaker,
        consumer: &mut BundleConsumer,
        bundle_storage: &mut BundleStorage,
        // bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        bundle_account_locker: &BundleAccountLocker,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        last_tip_update_slot: &mut Slot,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
    ) {
        match decision_maker.make_consume_or_forward_decision() {
            // BufferedPacketsDecision::Consume means this leader is scheduled to be running at the moment.
            // Execute, record, and commit as many bundles possible given time, compute, and other constraints.
            BufferedPacketsDecision::Consume(bank) => {
                Self::consume_bundles(
                    &bank,
                    bundle_storage,
                    bundle_account_locker,
                    consumer,
                    bundle_stage_metrics,
                    last_tip_update_slot,
                    block_builder_fee_info,
                    tip_manager,
                    cluster_info,
                );
            }
            // BufferedPacketsDecision::Forward means the leader is slot is far away.
            // Bundles aren't forwarded because it breaks atomicity guarantees, so just drop them.
            BufferedPacketsDecision::Forward => {
                bundle_storage.clear();
            }
            // BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold means the validator
            // is approaching the leader slot, hold bundles. Also, bundles aren't forwarded because it breaks
            // atomicity guarantees
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {}
        }
    }

    fn consume_bundles(
        bank: &Arc<Bank>,
        bundle_storage: &mut BundleStorage,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        last_tip_update_slot: &mut Slot,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
    ) {
        const BUNDLE_WINDOW_SIZE: NonZeroUsize = NonZeroUsize::new(10).unwrap();

        let mut bundles = VecDeque::with_capacity(BUNDLE_WINDOW_SIZE.get());

        if bank.slot() != *last_tip_update_slot {
            let output = Self::handle_tip_programs(
                bank,
                bundle_account_locker,
                consumer,
                tip_manager,
                cluster_info,
                block_builder_fee_info,
            );

            *last_tip_update_slot = bank.slot();
        }

        // This loop shall:
        // - Pop a bundle from the bundle storage
        // - Try to pre-lock the bundle with the bundle account locker, destorying the bundle if it fails
        // - Add the bundle to the back of the bundles deque
        // - If the bundles deque is full, process the bundle at the front of the deque.
        // - After processing it, check to see if it's a retryable error.
        // - If it's a retryable error, add the bundle back to the back of the bundles deque
        // - If it's not a retryable error, destroy the bundle
        //
        // The BUNDLE_WINDOW_SIZE should be chosen such that it's not holding too many bundles pre-locks at once
        // to prevent BankingStage from being starved of transactions to process.
        //
        // Requirements:
        // - Any bundle that gets popped must be destoryed
        // - Any bundle that gets locked with the bundle account locker shall be destroyed
        while let Some(bundle) = bundle_storage.pop_bundle(bank.slot()) {
            if bundle_account_locker
                .lock_bundle(&bundle.transactions, bank)
                .is_err()
            {
                bundle_storage.destroy_bundle(bundle);
                continue;
            }

            bundles.push_back(bundle);
            if bundles.len() == BUNDLE_WINDOW_SIZE.get() {
                // unwrwap safe here because of the length check
                let bundle = bundles.pop_front().unwrap();
                let _output = Self::process_bundle(
                    bank,
                    bundle,
                    bundle_storage,
                    bundle_account_locker,
                    consumer,
                );
            }
        }

        while let Some(bundle) = bundles.pop_front() {
            let _output = Self::process_bundle(
                bank,
                bundle,
                bundle_storage,
                bundle_account_locker,
                consumer,
            );
        }

        debug_assert!(
            bundle_account_locker
                .account_locks()
                .read_locks()
                .is_empty(),
            "bundle account read locks should be empty"
        );
        debug_assert!(
            bundle_account_locker
                .account_locks()
                .write_locks()
                .is_empty(),
            "bundle account write locks should be empty"
        );
    }

    fn handle_tip_programs(
        bank: &Arc<Bank>,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
    ) -> Option<ProcessTransactionBatchOutput> {
        let keypair = cluster_info.keypair();
        let initialize_tip_programs_bundle =
            tip_manager.get_initialize_tip_programs_bundle(bank, &keypair);
        if initialize_tip_programs_bundle.is_some() {
            info!("initialize tip program bundle to process");
        }

        let bundle = match tip_manager.get_tip_programs_crank_bundle(
            bank,
            &keypair,
            &block_builder_fee_info.lock().unwrap(),
        ) {
            Ok(maybe_bundle) => maybe_bundle,
            Err(e) => {
                // Only returns an error if there's an issue parsing the Configuration account, which should never happen.
                error!("error getting tip programs crank bundle: {:?}", e);
                return None;
            }
        };
        if bundle.is_some() {
            info!("tip program bundle to process");
        }

        let mut transactions = Vec::new();
        if let Some(bundle) = bundle {
            transactions.extend(bundle.into_iter());
        }
        if let Some(bundle) = initialize_tip_programs_bundle {
            transactions.extend(bundle.into_iter());
        }

        if !transactions.is_empty() {
            info!(
                "cranking tip programs with {} transactions",
                transactions.len()
            );
            let max_ages = vec![
                MaxAge {
                    sanitized_epoch: bank.epoch(),
                    alt_invalidation_slot: bank.slot(),
                };
                transactions.len()
            ];
            let _ = bundle_account_locker.lock_bundle(&transactions, bank);

            let output = consumer.process_and_record_aged_transactions(
                &bank,
                &transactions,
                &max_ages,
                MAX_BUNDLE_RETRY_DURATION,
            );
            let _ = bundle_account_locker.unlock_bundle_accounts(&transactions, &bank);
            return Some(output);
        }
        return None;
    }

    fn process_bundle(
        bank: &Arc<Bank>,
        bundle: BundleStorageEntry,
        bundle_storage: &mut BundleStorage,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
    ) -> Option<ProcessTransactionBatchOutput> {
        if bank.is_complete() {
            let _ = bundle_account_locker.unlock_bundle_accounts(&bundle.transactions, &bank);
            bundle_storage.retry_bundle(bundle);
            return None;
        } else {
            let output = consumer.process_and_record_aged_transactions(
                &bank,
                &bundle.transactions,
                &bundle.max_ages,
                MAX_BUNDLE_RETRY_DURATION,
            );

            let _ = bundle_account_locker.unlock_bundle_accounts(&bundle.transactions, &bank);

            if Self::is_retryable_error(&output) {
                bundle_storage.retry_bundle(bundle);
            } else {
                bundle_storage.destroy_bundle(bundle);
            }
            return Some(output);
        }
    }

    /// A bundle is retryable if:
    /// - The cost model throttled transactions count is greater than 0
    /// - The commit transactions result is an error
    /// - The commit transactions result contains a not committed error
    /// - The commit transactions result contains a not committed error that is an account in use error
    fn is_retryable_error(output: &ProcessTransactionBatchOutput) -> bool {
        output.cost_model_throttled_transactions_count > 0
            || output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .is_err()
            || output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .as_ref()
                .map(|results| {
                    results.iter().any(|result| {
                        matches!(
                            result,
                            CommitTransactionDetails::NotCommitted(TransactionError::AccountInUse)
                        )
                    })
                })
                .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_process_buffered_bundles() {
        panic!("Not implemented");
    }

    #[test]
    fn test_tip_payment_once_per_slot() {
        panic!("Not implemented");
    }
}
