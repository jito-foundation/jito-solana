//! The `bundle_stage` processes bundles, which are list of transactions to be executed
//! sequentially and atomically.

use {
    crate::{
        banking_stage::{
            committer::{CommitTransactionDetails, Committer},
            consume_worker::ConsumeWorkerMetrics,
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
        packet_bundle::VerifiedPacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    ahash::HashSet,
    arc_swap::ArcSwap,
    crossbeam_channel::{Receiver, RecvTimeoutError},
    smallvec::SmallVec,
    solana_clock::Slot,
    solana_gossip::cluster_info::ClusterInfo,
    solana_keypair::Keypair,
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
            Arc, RwLock,
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

    tip_programs_error: Saturating<u64>,
    bundle_lock_errors: Saturating<u64>,
    bundles_processed: Saturating<u64>,
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
            tip_programs_error: Saturating(0),
            bundle_lock_errors: Saturating(0),
            bundles_processed: Saturating(0),
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

    pub fn increment_tip_programs_error(&mut self, count: u64) {
        self.tip_programs_error += count;
    }

    pub fn increment_bundle_lock_errors(&mut self, count: u64) {
        self.bundle_lock_errors += count;
    }

    pub fn increment_bundles_processed(&mut self, count: u64) {
        self.bundles_processed += count;
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
        if self.last_report.elapsed().as_millis() >= report_interval_ms as u128 && self.has_data() {
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
                ("tip_programs_error", self.tip_programs_error.0 as i64, i64),
                ("bundle_lock_errors", self.bundle_lock_errors.0 as i64, i64),
                ("bundles_processed", self.bundles_processed.0 as i64, i64),
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
        self.tip_programs_error = Saturating(0);
        self.bundle_lock_errors = Saturating(0);
        self.bundles_processed = Saturating(0);
    }

    pub fn has_data(&self) -> bool {
        self.num_bundles_received.0 > 0
            || self.num_packets_received.0 > 0
            || self.newly_buffered_bundles_count.0 > 0
            || self.current_buffered_bundles_count.0 > 0
            || self.current_buffered_packets_count.0 > 0
            || self.cost_model_buffered_bundles_count.0 > 0
            || self.num_bundles_dropped.0 > 0
            || self.num_bundles_dropped_empty_batch.0 > 0
            || self.num_bundles_dropped_container_full.0 > 0
            || self.num_bundles_dropped_packet_marked_discard.0 > 0
            || self.num_bundles_dropped_packet_filter_error.0 > 0
            || self.num_bundles_dropped_bundle_too_large.0 > 0
            || self.num_bundles_dropped_duplicate_transaction.0 > 0
            || self.tip_programs_error.0 > 0
            || self.bundle_lock_errors.0 > 0
            || self.bundles_processed.0 > 0
    }
}

type BundleExecutionResult<T> = Result<T, BundleExecutionError>;

#[derive(Debug)]
enum BundleExecutionError {
    TipError,
    ErrorRetryable,
    ErrorNonRetryable,
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
        bundle_receiver: Receiver<VerifiedPacketBundle>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
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
        bundle_receiver: Receiver<VerifiedPacketBundle>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_message_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
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

        let block_builder_fee_info = block_builder_fee_info.clone();
        let cluster_info = cluster_info.clone();
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
                    cluster_info,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    #[allow(clippy::too_many_arguments)]
    fn process_loop(
        bank_forks: Arc<RwLock<BankForks>>,
        mut bundle_receiver: Receiver<VerifiedPacketBundle>,
        mut decision_maker: DecisionMaker,
        mut consumer: BundleConsumer,
        exit: Arc<AtomicBool>,
        blacklisted_accounts: HashSet<Pubkey>,
        bundle_account_locker: BundleAccountLocker,
        tip_manager: TipManager,
        block_builder_fee_info: Arc<ArcSwap<BlockBuilderFeeInfo>>,
        cluster_info: Arc<ClusterInfo>,
    ) {
        let mut last_metrics_update = Instant::now();
        let mut bundle_storage = BundleStorage::with_capacity(2_000);

        let mut bundle_stage_metrics = BundleStageLoopMetrics::default();
        let consume_worker_metrics = ConsumeWorkerMetrics::new(10_000);

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
                        &consume_worker_metrics
                    ));
                bundle_stage_metrics.increment_process_buffered_bundles_elapsed_us(
                    process_buffered_packets_time_us,
                );
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

            bundle_stage_metrics.maybe_report(20);
        }
    }

    fn receive_and_buffer_bundles(
        bank_forks: &Arc<RwLock<BankForks>>,
        bundle_receiver: &mut Receiver<VerifiedPacketBundle>,
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
            Duration::from_millis(0)
        } else {
            Duration::from_millis(10)
        };

        let bundle = bundle_receiver.recv_timeout(recv_timeout)?;
        Self::insert_bundle(
            bundle_storage,
            bundle,
            &root_bank,
            &working_bank,
            blacklisted_accounts,
            bundle_stage_metrics,
        );

        while let Ok(bundle) = bundle_receiver.try_recv() {
            Self::insert_bundle(
                bundle_storage,
                bundle,
                &root_bank,
                &working_bank,
                blacklisted_accounts,
                bundle_stage_metrics,
            );
        }

        Ok(())
    }

    fn insert_bundle(
        bundle_storage: &mut BundleStorage,
        bundle: VerifiedPacketBundle,
        root_bank: &Arc<Bank>,
        working_bank: &Arc<Bank>,
        blacklisted_accounts: &HashSet<Pubkey>,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
    ) {
        let num_packets = bundle.batch().len();

        bundle_stage_metrics.increment_num_bundles_received(1);
        bundle_stage_metrics.increment_num_packets_received(num_packets as u64);

        match bundle_storage.insert_bundle(bundle, root_bank, working_bank, blacklisted_accounts) {
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
        bundle_account_locker: &BundleAccountLocker,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        last_tip_update_slot: &mut Slot,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
        consume_worker_metrics: &ConsumeWorkerMetrics,
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
                    consume_worker_metrics,
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

    #[allow(clippy::too_many_arguments)]
    fn consume_bundles(
        bank: &Arc<Bank>,
        bundle_storage: &mut BundleStorage,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        last_tip_update_slot: &mut Slot,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        tip_manager: &TipManager,
        cluster_info: &Arc<ClusterInfo>,
        consume_worker_metrics: &ConsumeWorkerMetrics,
    ) {
        const BUNDLE_WINDOW_SIZE: NonZeroUsize = NonZeroUsize::new(10).unwrap();

        let mut bundles = VecDeque::with_capacity(BUNDLE_WINDOW_SIZE.get());

        if bank.slot() != *last_tip_update_slot {
            if Self::handle_tip_programs(
                bank,
                bundle_account_locker,
                consumer,
                tip_manager,
                cluster_info,
                block_builder_fee_info,
                consume_worker_metrics,
            )
            .is_err()
            {
                bundle_stage_metrics.increment_tip_programs_error(1);
                error!("tip programs error, not processing bundles");
                return;
            }

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
                bundle_stage_metrics.increment_bundle_lock_errors(1);

                bundle_storage.destroy_bundle(bundle);
                continue;
            }

            bundles.push_back(bundle);
            if bundles.len() == BUNDLE_WINDOW_SIZE.get() {
                // unwrwap safe here because of the length check
                let bundle = bundles.pop_front().unwrap();
                if let Some(output) = Self::process_bundle(
                    bank,
                    bundle,
                    bundle_storage,
                    bundle_account_locker,
                    consumer,
                    consume_worker_metrics,
                ) {
                    consume_worker_metrics.update_for_consume(&output);
                    consume_worker_metrics.set_has_data(true);
                    bundle_stage_metrics.increment_bundles_processed(1);
                }
            }

            consume_worker_metrics.maybe_report_and_reset();
        }

        while let Some(bundle) = bundles.pop_front() {
            if let Some(output) = Self::process_bundle(
                bank,
                bundle,
                bundle_storage,
                bundle_account_locker,
                consumer,
                consume_worker_metrics,
            ) {
                consume_worker_metrics.update_for_consume(&output);
                consume_worker_metrics.set_has_data(true);
                bundle_stage_metrics.increment_bundles_processed(1);
            }
            consume_worker_metrics.maybe_report_and_reset();
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
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        consume_worker_metrics: &ConsumeWorkerMetrics,
    ) -> BundleExecutionResult<()> {
        let keypair = cluster_info.keypair();

        Self::handle_initialize_tip_programs(
            bank,
            bundle_account_locker,
            consumer,
            tip_manager,
            consume_worker_metrics,
            &keypair,
        )?;

        Self::handle_crank_tip_programs(
            bank,
            bundle_account_locker,
            consumer,
            tip_manager,
            block_builder_fee_info,
            consume_worker_metrics,
            &keypair,
        )?;

        Ok(())
    }

    fn handle_initialize_tip_programs(
        bank: &Arc<Bank>,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        tip_manager: &TipManager,
        consume_worker_metrics: &ConsumeWorkerMetrics,
        keypair: &Keypair,
    ) -> BundleExecutionResult<()> {
        let initialize_tip_program_transactions = tip_manager
            .get_initialize_tip_programs_bundle(bank, keypair)
            .map_err(|e| {
                warn!("tip programs initialize error: {e:?}");
                BundleExecutionError::TipError
            })?;
        if initialize_tip_program_transactions.is_empty() {
            return Ok(());
        }

        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        let max_ages: SmallVec<[MaxAge; 2]> =
            SmallVec::from_elem(max_age, initialize_tip_program_transactions.len());
        let _ = bundle_account_locker.lock_bundle(&initialize_tip_program_transactions, bank);
        let output = consumer.process_and_record_aged_transactions(
            bank,
            &initialize_tip_program_transactions,
            &max_ages,
            MAX_BUNDLE_RETRY_DURATION,
        );
        let _ = bundle_account_locker.unlock_bundle(&initialize_tip_program_transactions, bank);

        consume_worker_metrics.update_for_consume(&output);
        consume_worker_metrics.set_has_data(true);

        let bundle_result = Self::to_bundle_result(&output);

        debug!(
            "initialize tip program output: {:?}",
            output
                .execute_and_commit_transactions_output
                .commit_transactions_result
        );
        info!("initialize tip program output: {bundle_result:?}");

        bundle_result
    }

    fn handle_crank_tip_programs(
        bank: &Arc<Bank>,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        tip_manager: &TipManager,
        block_builder_fee_info: &Arc<ArcSwap<BlockBuilderFeeInfo>>,
        consume_worker_metrics: &ConsumeWorkerMetrics,
        keypair: &Keypair,
    ) -> BundleExecutionResult<()> {
        let block_builder_fee_info = block_builder_fee_info.load();
        let crank_tip_program_transactions = tip_manager
            .get_tip_programs_crank_bundle(bank, keypair, &block_builder_fee_info)
            .map_err(|e| {
                warn!("tip programs crank error: {e:?}");
                BundleExecutionError::TipError
            })?;

        if crank_tip_program_transactions.is_empty() {
            return Ok(());
        }

        let max_age = MaxAge {
            sanitized_epoch: bank.epoch(),
            alt_invalidation_slot: bank.slot(),
        };
        let max_ages: SmallVec<[MaxAge; 2]> =
            SmallVec::from_elem(max_age, crank_tip_program_transactions.len());
        let _ = bundle_account_locker.lock_bundle(&crank_tip_program_transactions, bank);
        let output = consumer.process_and_record_aged_transactions(
            bank,
            &crank_tip_program_transactions,
            &max_ages,
            MAX_BUNDLE_RETRY_DURATION,
        );
        let _ = bundle_account_locker.unlock_bundle(&crank_tip_program_transactions, bank);

        consume_worker_metrics.update_for_consume(&output);
        consume_worker_metrics.set_has_data(true);
        let bundle_result = Self::to_bundle_result(&output);
        debug!(
            "crank tip program output: {:?}",
            output
                .execute_and_commit_transactions_output
                .commit_transactions_result
        );
        info!("crank tip program output: {bundle_result:?}");
        bundle_result
    }

    fn to_bundle_result(output: &ProcessTransactionBatchOutput) -> BundleExecutionResult<()> {
        // If the commit transactions result is ok and all the commit transactions results are committed without an error, return ok.
        if output
            .execute_and_commit_transactions_output
            .commit_transactions_result
            .is_ok()
            && output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .as_ref()
                .unwrap()
                .iter()
                .all(|r| matches!(r, CommitTransactionDetails::Committed { result: Ok(_), .. }))
        {
            return Ok(());
        }
        // If there were any cost model throttled transactions, account in use, or PoH record error, it's retryable
        else if output.cost_model_throttled_transactions_count > 0
            || output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .is_err()
            || output
                .execute_and_commit_transactions_output
                .commit_transactions_result
                .as_ref()
                .unwrap()
                .iter()
                .any(|r| {
                    matches!(
                        r,
                        CommitTransactionDetails::NotCommitted(TransactionError::AccountInUse)
                    )
                })
        {
            return Err(BundleExecutionError::ErrorRetryable);
        }

        // If there were any other errors, it's non-retryable
        Err(BundleExecutionError::ErrorNonRetryable)
    }

    fn process_bundle(
        bank: &Arc<Bank>,
        bundle: BundleStorageEntry,
        bundle_storage: &mut BundleStorage,
        bundle_account_locker: &BundleAccountLocker,
        consumer: &mut BundleConsumer,
        consume_worker_metrics: &ConsumeWorkerMetrics,
    ) -> Option<ProcessTransactionBatchOutput> {
        if bank.is_complete() {
            let _ = bundle_account_locker.unlock_bundle(&bundle.transactions, bank);
            bundle_storage.retry_bundle(bundle);
            None
        } else {
            let output = consumer.process_and_record_aged_transactions(
                bank,
                &bundle.transactions,
                &bundle.max_ages,
                MAX_BUNDLE_RETRY_DURATION,
            );

            let _ = bundle_account_locker.unlock_bundle(&bundle.transactions, bank);
            consume_worker_metrics.update_for_consume(&output);

            let result = Self::to_bundle_result(&output);

            if log::log_enabled!(log::Level::Debug) {
                let signatures: Vec<_> = bundle
                    .transactions
                    .iter()
                    .map(|tx| tx.signatures())
                    .collect();
                debug!(
                    "execution results: bundle signatures: {:?}, result: {:?} \
                     cost_model_throttled_transactions_count: {:?} output: {:?}",
                    signatures,
                    result,
                    output.cost_model_throttled_transactions_count,
                    output
                        .execute_and_commit_transactions_output
                        .commit_transactions_result
                );
            }

            match result {
                Ok(_) | Err(BundleExecutionError::ErrorNonRetryable) => {
                    bundle_storage.destroy_bundle(bundle);
                }
                Err(BundleExecutionError::ErrorRetryable) => {
                    bundle_storage.retry_bundle(bundle);
                }
                Err(BundleExecutionError::TipError) => {
                    return None;
                }
            }

            Some(output)
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::tip_manager::{
            tip_distribution::{JitoTipDistributionConfig, TipDistributionAccount},
            tip_payment::JitoTipPaymentConfig,
            TipDistributionAccountConfig, TipManagerConfig,
        },
        agave_feature_set::FeatureSet,
        crossbeam_channel::{bounded, unbounded},
        solana_cluster_type::ClusterType,
        solana_fee_calculator::{FeeRateGovernor, DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE},
        solana_gossip::contact_info::ContactInfo,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::Blockstore, genesis_utils::GenesisConfigInfo,
            get_tmp_ledger_path_auto_delete,
        },
        solana_native_token::LAMPORTS_PER_SOL,
        solana_perf::{
            packet::{BytesPacket, PacketBatch},
            test_tx::test_tx,
        },
        solana_poh::poh_recorder::create_test_recorder,
        solana_program_binaries::{jito_tip_distribution, jito_tip_payment, spl_programs},
        solana_rent::Rent,
        solana_runtime::genesis_utils::create_genesis_config_with_leader_ex,
        solana_signer::Signer,
        solana_streamer::socket::SocketAddrSpace,
        solana_system_transaction::transfer,
        solana_time_utils::timestamp,
        solana_vote_interface::state::vote_state_v4::VoteStateV4,
    };

    struct TestFixture {
        genesis_config_info: GenesisConfigInfo,
        leader_keypair: Keypair,
    }

    fn create_genesis_config(mint_sol: u64) -> TestFixture {
        let mint_keypair = Keypair::new();
        let leader_keypair = Keypair::new();
        let voting_keypair = Keypair::new();

        let rent = Rent::default();

        let mut genesis_config = create_genesis_config_with_leader_ex(
            mint_sol * LAMPORTS_PER_SOL,
            &mint_keypair.pubkey(),
            &leader_keypair.pubkey(),
            &voting_keypair.pubkey(),
            &Pubkey::new_unique(),
            None,
            rent.minimum_balance(VoteStateV4::size_of()) + (LAMPORTS_PER_SOL * 1_000_000),
            LAMPORTS_PER_SOL * 1_000_000,
            FeeRateGovernor {
                // Initialize with a non-zero fee
                lamports_per_signature: DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE / 2,
                ..FeeRateGovernor::default()
            },
            rent.clone(), // most tests don't expect rent
            ClusterType::Development,
            &FeatureSet::all_enabled(),
            spl_programs(&rent),
        );
        genesis_config.ticks_per_slot *= 8;

        TestFixture {
            genesis_config_info: GenesisConfigInfo {
                genesis_config,
                mint_keypair,
                voting_keypair,
                validator_pubkey: leader_keypair.pubkey(),
            },
            leader_keypair,
        }
    }

    #[test]
    fn test_tip_programs_initialized_with_no_bundles() {
        agave_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
        } = create_genesis_config(2);
        let (bank, bank_forks) =
            Bank::new_no_wallclock_throttle_for_tests(&genesis_config_info.genesis_config);

        let bank = Bank::new_from_parent(bank, &Pubkey::new_unique(), 1);
        bank_forks.write().unwrap().insert(bank);

        let bank = bank_forks.read().unwrap().working_bank();
        assert_eq!(bank.slot(), 1);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiever,
        ) = create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&leader_keypair.pubkey(), timestamp()),
            Arc::new(leader_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));

        let (verified_bundle_sender, verified_bundle_receiver) = bounded(1024);
        let bundle_stage = BundleStage::new(
            &cluster_info,
            bank_forks,
            &poh_recorder,
            transaction_recorder,
            verified_bundle_receiver,
            None,
            replay_vote_sender,
            None,
            exit.clone(),
            TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::from(jito_tip_payment::id().to_bytes()),
                tip_distribution_program_id: Pubkey::from(jito_tip_distribution::id().to_bytes()),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: genesis_config_info.voting_keypair.pubkey(),
                    commission_bps: 10,
                },
            }),
            BundleAccountLocker::default(),
            &Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder: genesis_config_info.validator_pubkey,
                block_builder_commission: 10,
            })),
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
        );

        // initialize tip distribution config
        // initialize tip payment config
        // initialize tip distribution account
        // change tip receiver and block builder
        const NUM_TXS_EXPECTED: usize = 4;
        let mut tx_count = 0;
        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
            }
            assert!(tx_count <= NUM_TXS_EXPECTED, "tx_count: {tx_count}");
        }

        let tip_payment_config_account = bank
            .get_account(&JitoTipPaymentConfig::find_program_address(&jito_tip_payment::id()).0)
            .unwrap();
        let tip_distribution_config_account = bank
            .get_account(
                &JitoTipDistributionConfig::find_program_address(&jito_tip_distribution::id()).0,
            )
            .unwrap();

        let tip_payment_config = JitoTipPaymentConfig::from_account_shared_data(
            &tip_payment_config_account,
            &jito_tip_payment::id(),
        )
        .unwrap();
        let tip_distribution_config = JitoTipDistributionConfig::from_account_shared_data(
            &tip_distribution_config_account,
            &jito_tip_distribution::id(),
        )
        .unwrap();

        assert_eq!(
            tip_payment_config.tip_receiver(),
            TipDistributionAccount::find_program_address(
                &jito_tip_distribution::id(),
                &genesis_config_info.voting_keypair.pubkey(),
                bank.epoch()
            )
            .0
        );
        assert_eq!(
            tip_payment_config.block_builder(),
            genesis_config_info.validator_pubkey
        );
        assert_eq!(tip_payment_config.block_builder_commission_pct(), 10);

        assert_eq!(
            tip_distribution_config.authority(),
            genesis_config_info.validator_pubkey
        );
        assert_eq!(
            tip_distribution_config.expired_funds_account(),
            genesis_config_info.validator_pubkey
        );
        assert_eq!(tip_distribution_config.num_epochs_valid(), 10);
        assert_eq!(
            tip_distribution_config.max_validator_commission_bps(),
            10000
        );

        drop(verified_bundle_sender);
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        bundle_stage.join().unwrap();
    }

    #[test]
    fn test_process_bad_bundle() {
        agave_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
        } = create_genesis_config(2);
        let (bank, bank_forks) =
            Bank::new_no_wallclock_throttle_for_tests(&genesis_config_info.genesis_config);

        let bank = Bank::new_from_parent(bank, &Pubkey::new_unique(), 1);
        bank_forks.write().unwrap().insert(bank);

        let bank = bank_forks.read().unwrap().working_bank();
        assert_eq!(bank.slot(), 1);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiever,
        ) = create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&leader_keypair.pubkey(), timestamp()),
            Arc::new(leader_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));

        let (verified_bundle_sender, verified_bundle_receiver) = bounded(1024);
        let bundle_stage = BundleStage::new(
            &cluster_info,
            bank_forks,
            &poh_recorder,
            transaction_recorder,
            verified_bundle_receiver,
            None,
            replay_vote_sender,
            None,
            exit.clone(),
            TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::from(jito_tip_payment::id().to_bytes()),
                tip_distribution_program_id: Pubkey::from(jito_tip_distribution::id().to_bytes()),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: genesis_config_info.voting_keypair.pubkey(),
                    commission_bps: 10,
                },
            }),
            BundleAccountLocker::default(),
            &Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder: genesis_config_info.validator_pubkey,
                block_builder_commission: 10,
            })),
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
        );

        let verified_bundle =
            VerifiedPacketBundle::new(PacketBatch::from(vec![BytesPacket::from_data(
                None,
                test_tx(),
            )
            .unwrap()]));
        verified_bundle_sender.send(verified_bundle).unwrap();

        let start = Instant::now();
        const MAX_EXPECTED_TXS: usize = 4;
        let mut tx_count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
                assert!(tx_count <= MAX_EXPECTED_TXS, "tx_count: {tx_count}");
            }
        }

        exit.store(true, Ordering::Relaxed);
        bundle_stage.join().unwrap();
        poh_service.join().unwrap();
        drop(verified_bundle_sender);
    }

    #[test]
    fn test_process_sequential_bundles() {
        agave_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
        } = create_genesis_config(2);
        let (bank, bank_forks) =
            Bank::new_no_wallclock_throttle_for_tests(&genesis_config_info.genesis_config);

        let bank = Bank::new_from_parent(bank, &Pubkey::new_unique(), 1);
        bank_forks.write().unwrap().insert(bank);

        let bank = bank_forks.read().unwrap().working_bank();
        assert_eq!(bank.slot(), 1);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiever,
        ) = create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&leader_keypair.pubkey(), timestamp()),
            Arc::new(leader_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));

        let (verified_bundle_sender, verified_bundle_receiver) = bounded(1024);
        let bundle_stage = BundleStage::new(
            &cluster_info,
            bank_forks,
            &poh_recorder,
            transaction_recorder,
            verified_bundle_receiver,
            None,
            replay_vote_sender,
            None,
            exit.clone(),
            TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::from(jito_tip_payment::id().to_bytes()),
                tip_distribution_program_id: Pubkey::from(jito_tip_distribution::id().to_bytes()),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: genesis_config_info.voting_keypair.pubkey(),
                    commission_bps: 10,
                },
            }),
            BundleAccountLocker::default(),
            &Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder: genesis_config_info.validator_pubkey,
                block_builder_commission: 10,
            })),
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
        );

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();

        // mint seeds kp1 which transfers to kp2
        let verified_bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(
                None,
                transfer(
                    &genesis_config_info.mint_keypair,
                    &kp1.pubkey(),
                    (genesis_config_info.genesis_config.rent.minimum_balance(0) * 2) + 5_000,
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
            BytesPacket::from_data(
                None,
                transfer(
                    &kp1,
                    &kp2.pubkey(),
                    genesis_config_info.genesis_config.rent.minimum_balance(0),
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
        ]));
        verified_bundle_sender.send(verified_bundle).unwrap();

        let start = Instant::now();
        const MAX_EXPECTED_TXS: usize = 6; // 4 initial for tips + 2 transfers
        let mut tx_count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
                assert!(tx_count <= MAX_EXPECTED_TXS, "tx_count: {tx_count}");
            }
        }

        let balance = bank.get_balance(&kp1.pubkey());
        assert_eq!(
            balance,
            genesis_config_info.genesis_config.rent.minimum_balance(0)
        );
        let balance = bank.get_balance(&kp2.pubkey());
        assert_eq!(
            balance,
            genesis_config_info.genesis_config.rent.minimum_balance(0)
        );

        exit.store(true, Ordering::Relaxed);
        bundle_stage.join().unwrap();
        poh_service.join().unwrap();
        drop(verified_bundle_sender);
    }

    #[test]
    fn test_partial_revert_bundle() {
        agave_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
        } = create_genesis_config(2);
        let (bank, bank_forks) =
            Bank::new_no_wallclock_throttle_for_tests(&genesis_config_info.genesis_config);

        let bank = Bank::new_from_parent(bank, &Pubkey::new_unique(), 1);
        bank_forks.write().unwrap().insert(bank);

        let bank = bank_forks.read().unwrap().working_bank();
        assert_eq!(bank.slot(), 1);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiever,
        ) = create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&leader_keypair.pubkey(), timestamp()),
            Arc::new(leader_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));

        let (verified_bundle_sender, verified_bundle_receiver) = bounded(1024);
        let bundle_stage = BundleStage::new(
            &cluster_info,
            bank_forks,
            &poh_recorder,
            transaction_recorder,
            verified_bundle_receiver,
            None,
            replay_vote_sender,
            None,
            exit.clone(),
            TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::from(jito_tip_payment::id().to_bytes()),
                tip_distribution_program_id: Pubkey::from(jito_tip_distribution::id().to_bytes()),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: genesis_config_info.voting_keypair.pubkey(),
                    commission_bps: 10,
                },
            }),
            BundleAccountLocker::default(),
            &Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder: genesis_config_info.validator_pubkey,
                block_builder_commission: 10,
            })),
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
        );

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();

        // mint seeds kp1 which transfers to kp2
        let verified_bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(
                None,
                transfer(
                    &genesis_config_info.mint_keypair,
                    &kp1.pubkey(),
                    (genesis_config_info.genesis_config.rent.minimum_balance(0) * 2) + 5_000,
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
            BytesPacket::from_data(
                None,
                transfer(
                    &kp1,
                    &kp2.pubkey(),
                    genesis_config_info.genesis_config.rent.minimum_balance(0),
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
            BytesPacket::from_data(
                None,
                transfer(
                    &Keypair::new(), // no funds
                    &kp1.pubkey(),
                    genesis_config_info.genesis_config.rent.minimum_balance(0),
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
        ]));

        verified_bundle_sender.send(verified_bundle).unwrap();

        let start = Instant::now();
        const MAX_EXPECTED_TXS: usize = 4; // 4 initial for tips
        let mut tx_count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
                assert!(tx_count <= MAX_EXPECTED_TXS, "tx_count: {tx_count}");
            }
        }

        exit.store(true, Ordering::Relaxed);
        bundle_stage.join().unwrap();
        poh_service.join().unwrap();
        drop(verified_bundle_sender);
    }

    #[test]
    fn test_already_processed_tx_drops_bundle() {
        agave_logger::setup();
        let TestFixture {
            genesis_config_info,
            leader_keypair,
        } = create_genesis_config(2);
        let (bank, bank_forks) =
            Bank::new_no_wallclock_throttle_for_tests(&genesis_config_info.genesis_config);

        let bank = Bank::new_from_parent(bank, &Pubkey::new_unique(), 1);
        bank_forks.write().unwrap().insert(bank);

        let bank = bank_forks.read().unwrap().working_bank();
        assert_eq!(bank.slot(), 1);

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (
            exit,
            poh_recorder,
            _poh_controller,
            transaction_recorder,
            poh_service,
            entry_receiever,
        ) = create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&leader_keypair.pubkey(), timestamp()),
            Arc::new(leader_keypair.insecure_clone()),
            SocketAddrSpace::Unspecified,
        ));

        let (verified_bundle_sender, verified_bundle_receiver) = bounded(1024);
        let bundle_stage = BundleStage::new(
            &cluster_info,
            bank_forks,
            &poh_recorder,
            transaction_recorder,
            verified_bundle_receiver,
            None,
            replay_vote_sender,
            None,
            exit.clone(),
            TipManager::new(TipManagerConfig {
                tip_payment_program_id: Pubkey::from(jito_tip_payment::id().to_bytes()),
                tip_distribution_program_id: Pubkey::from(jito_tip_distribution::id().to_bytes()),
                tip_distribution_account_config: TipDistributionAccountConfig {
                    merkle_root_upload_authority: Pubkey::new_unique(),
                    vote_account: genesis_config_info.voting_keypair.pubkey(),
                    commission_bps: 10,
                },
            }),
            BundleAccountLocker::default(),
            &Arc::new(ArcSwap::from_pointee(BlockBuilderFeeInfo {
                block_builder: genesis_config_info.validator_pubkey,
                block_builder_commission: 10,
            })),
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
        );

        let kp = Keypair::new();
        let tx = transfer(
            &genesis_config_info.mint_keypair,
            &kp.pubkey(),
            genesis_config_info.genesis_config.rent.minimum_balance(0) * 10,
            bank.last_blockhash(),
        );

        let verified_bundle =
            VerifiedPacketBundle::new(PacketBatch::from(vec![BytesPacket::from_data(
                None,
                tx.clone(),
            )
            .unwrap()]));

        verified_bundle_sender.send(verified_bundle).unwrap();

        let start = Instant::now();
        const MAX_EXPECTED_TXS: usize = 5; // 4 initial for tips
        let mut tx_count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
                assert!(tx_count <= MAX_EXPECTED_TXS, "tx_count: {tx_count}");
            }
        }

        assert_eq!(
            bank.get_balance(&kp.pubkey()),
            (genesis_config_info.genesis_config.rent.minimum_balance(0) * 10)
        );

        // stick it second in a valid bundle
        let verified_bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(
                None,
                transfer(
                    &genesis_config_info.mint_keypair,
                    &kp.pubkey(),
                    genesis_config_info.genesis_config.rent.minimum_balance(0),
                    bank.last_blockhash(),
                ),
            )
            .unwrap(),
            BytesPacket::from_data(None, tx).unwrap(),
        ]));
        verified_bundle_sender.send(verified_bundle).unwrap();

        let start = Instant::now();
        let mut tx_count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            if let Ok((_bank, (entry, _tick_height))) =
                entry_receiever.recv_timeout(Duration::from_millis(1))
            {
                tx_count += entry.transactions.len();
                assert_eq!(tx_count, 0, "tx_count: {tx_count}");
            }
        }

        assert_eq!(
            bank.get_balance(&kp.pubkey()),
            genesis_config_info.genesis_config.rent.minimum_balance(0) * 10
        );

        exit.store(true, Ordering::Relaxed);
        bundle_stage.join().unwrap();
        poh_service.join().unwrap();
        drop(verified_bundle_sender);
    }
}
