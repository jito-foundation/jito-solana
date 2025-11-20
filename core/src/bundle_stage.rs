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
            committer::Committer,
            decision_maker::{BufferedPacketsDecision, DecisionMaker},
            qos_service::QosService,
        },
        bundle_stage::{
            bundle_account_locker::BundleAccountLocker,
            bundle_consumer::BundleConsumer,
            bundle_storage::{BundleStorage, BundleStorageError},
        },
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        tip_manager::TipManager,
    },
    ahash::HashSet,
    crossbeam_channel::Receiver,
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure_us,
    solana_poh::{poh_recorder::PohRecorder, transaction_recorder::TransactionRecorder},
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    std::{
        num::Saturating,
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
// // mod bundle_packet_receiver;
// pub(crate) mod bundle_stage_leader_metrics;
mod bundle_storage;
// // // mod committer;
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
            tip_manager,
            block_builder_fee_info.clone(),
            max_bundle_retry_duration,
            cluster_info.clone(),
        );

        let bundle_thread = Builder::new()
            .name("solBundleStgTx".to_string())
            .spawn(move || {
                Self::process_loop(
                    bank_forks,
                    bundle_receiver,
                    decision_maker,
                    consumer,
                    // BUNDLE_STAGE_ID,
                    exit,
                    blacklisted_accounts,
                    bundle_account_locker,
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
        consumer: BundleConsumer,
        // id: u32,
        exit: Arc<AtomicBool>,
        blacklisted_accounts: HashSet<Pubkey>,
        bundle_account_locker: BundleAccountLocker,
    ) {
        let mut last_metrics_update = Instant::now();
        let mut bundle_storage = BundleStorage::with_capacity(2_000);

        let mut bundle_stage_metrics = BundleStageLoopMetrics::default();
        // let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(id);

        while !exit.load(Ordering::Relaxed) {
            if bundle_storage.unprocessed_bundles_len() > 0
                || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
            {
                let (_, process_buffered_packets_time_us) =
                    measure_us!(Self::process_buffered_bundles(
                        &mut decision_maker,
                        &mut bundle_storage,
                        &bundle_account_locker
                    ));
                // bundle_stage_leader_metrics
                //         .leader_slot_metrics_tracker()
                //         .increment_process_buffered_packets_us(process_buffered_packets_time_us);
                last_metrics_update = Instant::now();
            }

            // Buffer the rest
            let (root_bank, working_bank) = {
                let bank_forks = bank_forks.read().unwrap();
                let root_bank = bank_forks.root_bank();
                let working_bank = bank_forks.working_bank();
                (root_bank, working_bank)
            };

            let start = Instant::now();
            while let Ok(bundles) = bundle_receiver.try_recv() {
                for bundle in bundles {
                    let num_packets = bundle.batch().len();

                    bundle_stage_metrics.increment_num_bundles_received(1);
                    bundle_stage_metrics.increment_num_packets_received(num_packets as u64);

                    match bundle_storage.insert_bundle(
                        bundle,
                        &root_bank,
                        &working_bank,
                        &blacklisted_accounts,
                    ) {
                        Ok(_) => {
                            bundle_stage_metrics.increment_newly_buffered_bundles_count(1);
                        }
                        Err(e) => {
                            bundle_stage_metrics.increment_bundle_dropped_error(e);
                        }
                    }
                }
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

    #[allow(clippy::too_many_arguments)]
    fn process_buffered_bundles(
        decision_maker: &mut DecisionMaker,
        // consumer: &mut BundleConsumer,
        bundle_storage: &mut BundleStorage,
        // bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        bundle_account_locker: &BundleAccountLocker,
    ) {
        match decision_maker.make_consume_or_forward_decision() {
            // BufferedPacketsDecision::Consume means this leader is scheduled to be running at the moment.
            // Execute, record, and commit as many bundles possible given time, compute, and other constraints.
            BufferedPacketsDecision::Consume(bank) => {
                while !bank.is_complete() {
                    let Some(bundle) = bundle_storage.pop_bundle(bank.slot()) else {
                        break;
                    };

                    // let locked_bundle =
                    //     bundle_account_locker.prepare_locked_bundle(sanitized_bundle, &bank);

                    bundle_storage.destroy_bundle(bundle);
                }
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
}
