//! The `banking_stage` processes Transaction messages. It is intended to be used
//! to construct a software pipeline. The stage uses all available CPU cores and
//! can do its processing in parallel with signature verification on the GPU.

#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    self::{
        committer::Committer, consumer::Consumer, decision_maker::DecisionMaker,
        packet_receiver::PacketReceiver, qos_service::QosService, vote_storage::VoteStorage,
    },
    crate::{
        banking_stage::{
            consume_worker::ConsumeWorker,
            packet_deserializer::PacketDeserializer,
            transaction_scheduler::{
                prio_graph_scheduler::PrioGraphScheduler,
                scheduler_controller::SchedulerController, scheduler_error::SchedulerError,
            },
        },
        bundle_stage::bundle_account_locker::BundleAccountLocker,
        validator::{BlockProductionMethod, TransactionStructure},
    },
    agave_banking_stage_ingress_types::BankingPacketReceiver,
    conditional_mod::conditional_vis_mod,
    crossbeam_channel::{unbounded, Receiver, Sender},
    histogram::Histogram,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfoQuery},
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_perf::packet::PACKETS_PER_BATCH,
    solana_poh::{poh_recorder::PohRecorder, transaction_recorder::TransactionRecorder},
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, prioritization_fee_cache::PrioritizationFeeCache,
        vote_sender_types::ReplayVoteSender,
    },
    solana_time_utils::AtomicInterval,
    std::{
        cmp,
        collections::HashSet,
        env,
        num::Saturating,
        ops::Deref,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
    transaction_scheduler::{
        greedy_scheduler::{GreedyScheduler, GreedySchedulerConfig},
        prio_graph_scheduler::PrioGraphSchedulerConfig,
        receive_and_buffer::{
            ReceiveAndBuffer, SanitizedTransactionReceiveAndBuffer, TransactionViewReceiveAndBuffer,
        },
        transaction_state_container::TransactionStateContainer,
    },
    vote_worker::VoteWorker,
};

// Below modules are pub to allow use by banking_stage bench
pub mod committer;
pub mod consumer;
pub mod leader_slot_metrics;
pub mod qos_service;
pub mod vote_storage;

mod consume_worker;
pub mod decision_maker;
pub(crate) mod immutable_deserialized_packet;
mod latest_validator_vote_packet;
pub(crate) mod leader_slot_timing_metrics;
mod vote_worker;
conditional_vis_mod!(packet_deserializer, feature = "dev-context-only-utils", pub);
mod packet_receiver;
mod read_write_account_set;
conditional_vis_mod!(scheduler_messages, feature = "dev-context-only-utils", pub);
conditional_vis_mod!(
    transaction_scheduler,
    feature = "dev-context-only-utils",
    pub
);
conditional_vis_mod!(unified_scheduler, feature = "dev-context-only-utils", pub, pub(crate));

pub const DEFAULT_NUM_WORKERS: u32 = 4;
pub const NUM_THREADS: u32 = {
    DEFAULT_NUM_WORKERS
    + 1 // scheduler thread
    + 1 // vote-thread
};

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
const TOTAL_BUFFERED_PACKETS: usize = 100_000;

const NUM_VOTE_PROCESSING_THREADS: u32 = 2;
const MIN_THREADS_BANKING: u32 = 1;
const MIN_TOTAL_THREADS: u32 = NUM_VOTE_PROCESSING_THREADS + MIN_THREADS_BANKING;

const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

#[derive(Debug, Default)]
pub struct BankingStageStats {
    last_report: AtomicInterval,
    tpu_counts: VoteSourceCounts,
    gossip_counts: VoteSourceCounts,
    pub(crate) dropped_duplicated_packets_count: AtomicUsize,
    dropped_forward_packets_count: AtomicUsize,
    current_buffered_packets_count: AtomicUsize,
    rebuffered_packets_count: AtomicUsize,
    consumed_buffered_packets_count: AtomicUsize,
    batch_packet_indexes_len: Histogram,

    // Timing
    consume_buffered_packets_elapsed: AtomicU64,
    receive_and_buffer_packets_elapsed: AtomicU64,
    filter_pending_packets_elapsed: AtomicU64,
    pub(crate) packet_conversion_elapsed: AtomicU64,
    transaction_processing_elapsed: AtomicU64,
}

#[derive(Debug, Default)]
struct VoteSourceCounts {
    receive_and_buffer_packets_count: AtomicUsize,
    dropped_packets_count: AtomicUsize,
    newly_buffered_packets_count: AtomicUsize,
    newly_buffered_forwarded_packets_count: AtomicUsize,
}

impl VoteSourceCounts {
    fn is_empty(&self) -> bool {
        0 == self
            .receive_and_buffer_packets_count
            .load(Ordering::Relaxed)
            + self.dropped_packets_count.load(Ordering::Relaxed)
            + self.newly_buffered_packets_count.load(Ordering::Relaxed)
            + self
                .newly_buffered_forwarded_packets_count
                .load(Ordering::Relaxed)
    }
}

impl BankingStageStats {
    pub fn new() -> Self {
        BankingStageStats {
            batch_packet_indexes_len: Histogram::configure()
                .max_value(PACKETS_PER_BATCH as u64)
                .build()
                .unwrap(),
            ..BankingStageStats::default()
        }
    }

    fn is_empty(&self) -> bool {
        self.gossip_counts.is_empty()
            && self.tpu_counts.is_empty()
            && 0 == self
                .dropped_duplicated_packets_count
                .load(Ordering::Relaxed) as u64
                + self.dropped_forward_packets_count.load(Ordering::Relaxed) as u64
                + self.current_buffered_packets_count.load(Ordering::Relaxed) as u64
                + self.rebuffered_packets_count.load(Ordering::Relaxed) as u64
                + self.consumed_buffered_packets_count.load(Ordering::Relaxed) as u64
                + self
                    .consume_buffered_packets_elapsed
                    .load(Ordering::Relaxed)
                + self
                    .receive_and_buffer_packets_elapsed
                    .load(Ordering::Relaxed)
                + self.filter_pending_packets_elapsed.load(Ordering::Relaxed)
                + self.packet_conversion_elapsed.load(Ordering::Relaxed)
                + self.transaction_processing_elapsed.load(Ordering::Relaxed)
                + self.batch_packet_indexes_len.entries()
    }

    fn report(&mut self, report_interval_ms: u64) {
        // skip reporting metrics if stats is empty
        if self.is_empty() {
            return;
        }
        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "banking_stage-vote_loop_stats",
                (
                    "tpu_receive_and_buffer_packets_count",
                    self.tpu_counts
                        .receive_and_buffer_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "tpu_dropped_packets_count",
                    self.tpu_counts
                        .dropped_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "tpu_newly_buffered_packets_count",
                    self.tpu_counts
                        .newly_buffered_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "tpu_newly_buffered_forwarded_packets_count",
                    self.tpu_counts
                        .newly_buffered_forwarded_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "gossip_receive_and_buffer_packets_count",
                    self.gossip_counts
                        .receive_and_buffer_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "gossip_dropped_packets_count",
                    self.gossip_counts
                        .dropped_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "gossip_newly_buffered_packets_count",
                    self.gossip_counts
                        .newly_buffered_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "gossip_newly_buffered_forwarded_packets_count",
                    self.gossip_counts
                        .newly_buffered_forwarded_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dropped_duplicated_packets_count",
                    self.dropped_duplicated_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dropped_forward_packets_count",
                    self.dropped_forward_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "rebuffered_packets_count",
                    self.rebuffered_packets_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "consumed_buffered_packets_count",
                    self.consumed_buffered_packets_count
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "consume_buffered_packets_elapsed",
                    self.consume_buffered_packets_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "receive_and_buffer_packets_elapsed",
                    self.receive_and_buffer_packets_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "filter_pending_packets_elapsed",
                    self.filter_pending_packets_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "packet_conversion_elapsed",
                    self.packet_conversion_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "transaction_processing_elapsed",
                    self.transaction_processing_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "packet_batch_indices_len_min",
                    self.batch_packet_indexes_len.minimum().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_max",
                    self.batch_packet_indexes_len.maximum().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_mean",
                    self.batch_packet_indexes_len.mean().unwrap_or(0),
                    i64
                ),
                (
                    "packet_batch_indices_len_90pct",
                    self.batch_packet_indexes_len.percentile(90.0).unwrap_or(0),
                    i64
                )
            );
            self.batch_packet_indexes_len.clear();
        }
    }
}

#[derive(Debug, Default)]
pub struct BatchedTransactionDetails {
    pub costs: BatchedTransactionCostDetails,
    pub errors: BatchedTransactionErrorDetails,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionCostDetails {
    pub batched_signature_cost: Saturating<u64>,
    pub batched_write_lock_cost: Saturating<u64>,
    pub batched_data_bytes_cost: Saturating<u64>,
    pub batched_loaded_accounts_data_size_cost: Saturating<u64>,
    pub batched_programs_execute_cost: Saturating<u64>,
}

#[derive(Debug, Default)]
pub struct BatchedTransactionErrorDetails {
    pub batched_retried_txs_per_block_limit_count: Saturating<u64>,
    pub batched_retried_txs_per_vote_limit_count: Saturating<u64>,
    pub batched_retried_txs_per_account_limit_count: Saturating<u64>,
    pub batched_retried_txs_per_account_data_block_limit_count: Saturating<u64>,
    pub batched_dropped_txs_per_account_data_total_limit_count: Saturating<u64>,
}

pub struct BankingStage {
    vote_thread_hdl: JoinHandle<()>,
    non_vote_thread_hdls: Vec<JoinHandle<()>>,
}

pub trait LikeClusterInfo: Send + Sync + 'static + Clone {
    fn id(&self) -> Pubkey;

    fn lookup_contact_info<R>(&self, id: &Pubkey, query: impl ContactInfoQuery<R>) -> Option<R>;
}

impl LikeClusterInfo for Arc<ClusterInfo> {
    fn id(&self) -> Pubkey {
        self.deref().id()
    }

    fn lookup_contact_info<R>(&self, id: &Pubkey, query: impl ContactInfoQuery<R>) -> Option<R> {
        self.deref().lookup_contact_info(id, query)
    }
}

impl BankingStage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        block_production_method: BlockProductionMethod,
        transaction_struct: TransactionStructure,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        non_vote_receiver: BankingPacketReceiver,
        tpu_vote_receiver: BankingPacketReceiver,
        gossip_vote_receiver: BankingPacketReceiver,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        blacklisted_accounts: HashSet<Pubkey>,
        bundle_account_locker: BundleAccountLocker,
        // callback function for compute space reservation for BundleStage
        block_cost_limit_block_cost_limit_reservation_cb: impl Fn(&Bank) -> u64 + Clone + Send + 'static,
    ) -> Self {
        Self::new_num_threads(
            block_production_method,
            transaction_struct,
            poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            Self::default_or_env_num_workers(),
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            bank_forks,
            prioritization_fee_cache,
            blacklisted_accounts,
            bundle_account_locker,
            block_cost_limit_block_cost_limit_reservation_cb,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_num_threads(
        block_production_method: BlockProductionMethod,
        transaction_struct: TransactionStructure,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        transaction_recorder: TransactionRecorder,
        non_vote_receiver: BankingPacketReceiver,
        tpu_vote_receiver: BankingPacketReceiver,
        gossip_vote_receiver: BankingPacketReceiver,
        num_workers: u32,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
        blacklisted_accounts: HashSet<Pubkey>,
        bundle_account_locker: BundleAccountLocker,
        block_cost_limit_reservation_cb: impl Fn(&Bank) -> u64 + Clone + Send + 'static,
    ) -> Self {
        let committer = Committer::new(
            transaction_status_sender.clone(),
            replay_vote_sender.clone(),
            prioritization_fee_cache.clone(),
        );
        let vote_thread_hdl = Self::spawn_vote_worker(
            tpu_vote_receiver,
            gossip_vote_receiver,
            transaction_recorder.clone(),
            poh_recorder.clone(),
            bank_forks.clone(),
            committer.clone(),
            log_messages_bytes_limit,
            VoteStorage::new(&bank_forks.read().unwrap().working_bank()),
            bundle_account_locker.clone(),
            block_cost_limit_reservation_cb.clone(),
        );

        let use_greedy_scheduler = matches!(
            block_production_method,
            BlockProductionMethod::CentralSchedulerGreedy
        );
        let non_vote_thread_hdls = Self::new_central_scheduler(
            transaction_struct,
            use_greedy_scheduler,
            num_workers,
            non_vote_receiver,
            transaction_recorder.clone(),
            poh_recorder.clone(),
            bank_forks.clone(),
            committer.clone(),
            log_messages_bytes_limit,
            bundle_account_locker.clone(),
            block_cost_limit_reservation_cb.clone(),
            blacklisted_accounts,
        );

        Self {
            vote_thread_hdl,
            non_vote_thread_hdls,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn new_central_scheduler(
        transaction_struct: TransactionStructure,
        use_greedy_scheduler: bool,
        num_workers: u32,
        non_vote_receiver: BankingPacketReceiver,
        transaction_recorder: TransactionRecorder,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        committer: Committer,
        log_messages_bytes_limit: Option<usize>,
        bundle_account_locker: BundleAccountLocker,
        block_cost_limit_reservation_cb: impl Fn(&Bank) -> u64 + Clone + Send + 'static,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Vec<JoinHandle<()>> {
        match transaction_struct {
            TransactionStructure::Sdk => {
                let receive_and_buffer = SanitizedTransactionReceiveAndBuffer::new(
                    PacketDeserializer::new(non_vote_receiver),
                    bank_forks.clone(),
                    blacklisted_accounts,
                );
                Self::spawn_scheduler_and_workers(
                    receive_and_buffer,
                    use_greedy_scheduler,
                    num_workers,
                    transaction_recorder,
                    poh_recorder,
                    bank_forks,
                    committer,
                    log_messages_bytes_limit,
                    bundle_account_locker,
                    block_cost_limit_reservation_cb,
                )
            }
            TransactionStructure::View => {
                let receive_and_buffer = TransactionViewReceiveAndBuffer {
                    receiver: non_vote_receiver,
                    bank_forks: bank_forks.clone(),
                    blacklisted_accounts,
                };
                Self::spawn_scheduler_and_workers(
                    receive_and_buffer,
                    use_greedy_scheduler,
                    num_workers,
                    transaction_recorder,
                    poh_recorder,
                    bank_forks,
                    committer,
                    log_messages_bytes_limit,
                    bundle_account_locker,
                    block_cost_limit_reservation_cb,
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_scheduler_and_workers<R: ReceiveAndBuffer + Send + Sync + 'static>(
        receive_and_buffer: R,
        use_greedy_scheduler: bool,
        num_workers: u32,
        transaction_recorder: TransactionRecorder,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        committer: Committer,
        log_messages_bytes_limit: Option<usize>,
        bundle_account_locker: BundleAccountLocker,
        block_cost_limit_reservation_cb: impl Fn(&Bank) -> u64 + Clone + Send + 'static,
    ) -> Vec<JoinHandle<()>> {
        // + 1 for scheduler thread
        let mut thread_hdls = Vec::with_capacity(num_workers as usize + 1);

        // Create channels for communication between scheduler and workers
        let (work_senders, work_receivers): (Vec<Sender<_>>, Vec<Receiver<_>>) =
            (0..num_workers).map(|_| unbounded()).unzip();
        let (finished_work_sender, finished_work_receiver) = unbounded();

        // Spawn the worker threads
        let decision_maker = DecisionMaker::new(poh_recorder.clone());
        let mut worker_metrics = Vec::with_capacity(num_workers as usize);
        for (index, work_receiver) in work_receivers.into_iter().enumerate() {
            let id = (index as u32).saturating_add(NUM_VOTE_PROCESSING_THREADS);
            let consume_worker = ConsumeWorker::new(
                id,
                work_receiver,
                Consumer::new(
                    committer.clone(),
                    transaction_recorder.clone(),
                    QosService::new(id),
                    log_messages_bytes_limit,
                    bundle_account_locker.clone(),
                ),
                finished_work_sender.clone(),
                poh_recorder.read().unwrap().shared_working_bank(),
            );

            worker_metrics.push(consume_worker.metrics_handle());
            let cb = block_cost_limit_reservation_cb.clone();
            thread_hdls.push(
                Builder::new()
                    .name(format!("solCoWorker{id:02}"))
                    .spawn(move || {
                        let _ = consume_worker.run(cb);
                    })
                    .unwrap(),
            )
        }

        // Macro to spawn the scheduler. Different type on `scheduler` and thus
        // scheduler_controller mean we cannot have an easy if for `scheduler`
        // assignment without introducing `dyn`.
        macro_rules! spawn_scheduler {
            ($scheduler:ident) => {
                thread_hdls.push(
                    Builder::new()
                        .name("solBnkTxSched".to_string())
                        .spawn(move || {
                            let scheduler_controller = SchedulerController::new(
                                decision_maker.clone(),
                                receive_and_buffer,
                                bank_forks,
                                $scheduler,
                                worker_metrics,
                            );

                            match scheduler_controller.run() {
                                Ok(_) => {}
                                Err(SchedulerError::DisconnectedRecvChannel(_)) => {}
                                Err(SchedulerError::DisconnectedSendChannel(_)) => {
                                    warn!("Unexpected worker disconnect from scheduler")
                                }
                            }
                        })
                        .unwrap(),
                );
            };
        }

        // Spawn the central scheduler thread
        if use_greedy_scheduler {
            let scheduler = GreedyScheduler::new(
                work_senders,
                finished_work_receiver,
                GreedySchedulerConfig::default(),
            );
            spawn_scheduler!(scheduler);
        } else {
            let scheduler = PrioGraphScheduler::new(
                work_senders,
                finished_work_receiver,
                PrioGraphSchedulerConfig::default(),
            );
            spawn_scheduler!(scheduler);
        }

        thread_hdls
    }

    #[allow(clippy::too_many_arguments)]
    fn spawn_vote_worker(
        tpu_receiver: BankingPacketReceiver,
        gossip_receiver: BankingPacketReceiver,
        transaction_recorder: TransactionRecorder,
        poh_recorder: Arc<RwLock<PohRecorder>>,
        bank_forks: Arc<RwLock<BankForks>>,
        committer: Committer,
        log_messages_bytes_limit: Option<usize>,
        vote_storage: VoteStorage,
        bundle_account_locker: BundleAccountLocker,
        block_cost_limit_reservation_cb: impl Fn(&Bank) -> u64 + Clone + Send + 'static,
    ) -> JoinHandle<()> {
        let tpu_receiver = PacketReceiver::new(tpu_receiver);
        let gossip_receiver = PacketReceiver::new(gossip_receiver);
        let consumer = Consumer::new(
            committer,
            transaction_recorder,
            QosService::new(0),
            log_messages_bytes_limit,
            bundle_account_locker.clone(),
        );
        let decision_maker = DecisionMaker::new(poh_recorder);

        Builder::new()
            .name("solBanknStgVote".to_string())
            .spawn(move || {
                VoteWorker::new(
                    decision_maker,
                    tpu_receiver,
                    gossip_receiver,
                    vote_storage,
                    bank_forks,
                    consumer,
                )
                .run(block_cost_limit_reservation_cb)
            })
            .unwrap()
    }

    pub fn default_or_env_num_workers() -> u32 {
        cmp::max(
            env::var("SOLANA_BANKING_THREADS")
                .map(|x| x.parse().unwrap_or(NUM_THREADS))
                .unwrap_or(NUM_THREADS)
                .saturating_sub(2), // - 2 for vote and scheduler threads
            MIN_TOTAL_THREADS,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.vote_thread_hdl.join()?;
        for bank_thread_hdl in self.non_vote_thread_hdls {
            bank_thread_hdl.join()?;
        }
        Ok(())
    }
}

#[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
pub(crate) fn update_bank_forks_and_poh_recorder_for_new_tpu_bank(
    bank_forks: &RwLock<BankForks>,
    poh_recorder: &RwLock<PohRecorder>,
    tpu_bank: Bank,
) {
    let tpu_bank = bank_forks.write().unwrap().insert(tpu_bank);
    poh_recorder.write().unwrap().set_bank(tpu_bank);
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_trace::{BankingTracer, Channels},
        agave_banking_stage_ingress_types::BankingPacketBatch,
        crossbeam_channel::{unbounded, Receiver},
        itertools::Itertools,
        solana_entry::entry::{self, Entry, EntrySlice},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::{
            blockstore::Blockstore,
            genesis_utils::{
                create_genesis_config, create_genesis_config_with_leader, GenesisConfigInfo,
            },
            get_tmp_ledger_path_auto_delete,
            leader_schedule_cache::LeaderScheduleCache,
        },
        solana_perf::packet::to_packet_batches,
        solana_poh::{
            poh_recorder::{create_test_recorder, PohRecorderError, Record},
            poh_service::PohService,
            transaction_recorder::RecordTransactionsSummary,
        },
        solana_poh_config::PohConfig,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, genesis_utils::bootstrap_validator_stake_lamports},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_system_transaction as system_transaction,
        solana_transaction::{sanitized::SanitizedTransaction, Transaction},
        solana_vote::vote_transaction::new_tower_sync_transaction,
        solana_vote_program::vote_state::TowerSync,
        std::{
            sync::atomic::{AtomicBool, Ordering},
            thread::sleep,
            time::Instant,
        },
        strum::IntoEnumIterator,
        test_case::test_case,
    };

    pub(crate) fn sanitize_transactions(
        txs: Vec<Transaction>,
    ) -> Vec<RuntimeTransaction<SanitizedTransaction>> {
        txs.into_iter()
            .map(RuntimeTransaction::from_transaction_for_tests)
            .collect()
    }

    #[test_case(TransactionStructure::Sdk)]
    #[test_case(TransactionStructure::View)]
    fn test_banking_stage_shutdown1(transaction_struct: TransactionStructure) {
        let genesis_config = create_genesis_config(2).genesis_config;
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let (exit, poh_recorder, transaction_recorder, poh_service, _entry_receiever) =
            create_test_recorder(bank, blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new(
            BlockProductionMethod::CentralScheduler,
            transaction_struct,
            &poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            None,
            replay_vote_sender,
            None,
            bank_forks,
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
            BundleAccountLocker::default(),
            |_| 0,
        );
        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        exit.store(true, Ordering::Relaxed);
        banking_stage.join().unwrap();
        poh_service.join().unwrap();
    }

    #[test_case(TransactionStructure::Sdk)]
    #[test_case(TransactionStructure::View)]
    fn test_banking_stage_tick(transaction_struct: TransactionStructure) {
        solana_logger::setup();
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(2);
        genesis_config.ticks_per_slot = 4;
        let num_extra_ticks = 2;
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let start_hash = bank.last_blockhash();
        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let poh_config = PohConfig {
            target_tick_count: Some(bank.max_tick_height() + num_extra_ticks),
            ..PohConfig::default()
        };
        let (exit, poh_recorder, transaction_recorder, poh_service, entry_receiver) =
            create_test_recorder(bank.clone(), blockstore, Some(poh_config), None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new(
            BlockProductionMethod::CentralScheduler,
            transaction_struct,
            &poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            None,
            replay_vote_sender,
            None,
            bank_forks,
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
            BundleAccountLocker::default(),
            |_| 0,
        );
        trace!("sending bank");
        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        drop(poh_recorder);

        trace!("getting entries");
        let entries: Vec<_> = entry_receiver
            .iter()
            .map(|(_bank, (entry, _tick_height))| entry)
            .collect();
        trace!("done");
        assert_eq!(entries.len(), genesis_config.ticks_per_slot as usize);
        assert!(entries.verify(&start_hash, &entry::thread_pool_for_tests()));
        assert_eq!(entries[entries.len() - 1].hash, bank.last_blockhash());
        banking_stage.join().unwrap();
    }

    fn test_banking_stage_entries_only(
        block_production_method: BlockProductionMethod,
        transaction_struct: TransactionStructure,
    ) {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10);
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let start_hash = bank.last_blockhash();
        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let (exit, poh_recorder, transaction_recorder, poh_service, entry_receiver) =
            create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new(
            block_production_method,
            transaction_struct,
            &poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            None,
            replay_vote_sender,
            None,
            bank_forks.clone(), // keep a local-copy of bank-forks so worker threads do not lose weak access to bank-forks
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
            BundleAccountLocker::default(),
            |_| 0,
        );

        // fund another account so we can send 2 good transactions in a single batch.
        let keypair = Keypair::new();
        let fund_tx = system_transaction::transfer(&mint_keypair, &keypair.pubkey(), 2, start_hash);
        bank.process_transaction(&fund_tx).unwrap();

        // good tx, but no verify
        let to = solana_pubkey::new_rand();
        let tx_no_ver = system_transaction::transfer(&keypair, &to, 2, start_hash);

        // good tx
        let to2 = solana_pubkey::new_rand();
        let tx = system_transaction::transfer(&mint_keypair, &to2, 1, start_hash);

        // bad tx, AccountNotFound
        let keypair = Keypair::new();
        let to3 = solana_pubkey::new_rand();
        let tx_anf = system_transaction::transfer(&keypair, &to3, 1, start_hash);

        // send 'em over
        let mut packet_batches = to_packet_batches(&[tx_no_ver, tx_anf, tx], 3);
        packet_batches[0]
            .first_mut()
            .unwrap()
            .meta_mut()
            .set_discard(true); // set discard on `tx_no_ver`

        // glad they all fit
        assert_eq!(packet_batches.len(), 1);

        non_vote_sender // no_ver, anf, tx
            .send(BankingPacketBatch::new(packet_batches))
            .unwrap();

        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);
        // wait until banking_stage to finish up all packets
        banking_stage.join().unwrap();

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
        drop(poh_recorder);

        let mut blockhash = start_hash;
        let (bank, _bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        bank.process_transaction(&fund_tx).unwrap();
        //receive entries + ticks
        loop {
            let entries: Vec<Entry> = entry_receiver
                .iter()
                .map(|(_bank, (entry, _tick_height))| entry)
                .collect();

            assert!(entries.verify(&blockhash, &entry::thread_pool_for_tests()));
            if !entries.is_empty() {
                blockhash = entries.last().unwrap().hash;
                for entry in entries {
                    bank.process_entry_transactions(entry.transactions)
                        .iter()
                        .for_each(|x| assert_eq!(*x, Ok(())));
                }
            }

            if bank.get_balance(&to2) == 1 {
                break;
            }

            sleep(Duration::from_millis(200));
        }

        assert_eq!(bank.get_balance(&to2), 1);
        assert_eq!(bank.get_balance(&to), 0);

        drop(entry_receiver);
    }

    #[test_case(TransactionStructure::Sdk)]
    #[test_case(TransactionStructure::View)]
    fn test_banking_stage_entries_only_central_scheduler(transaction_struct: TransactionStructure) {
        test_banking_stage_entries_only(
            BlockProductionMethod::CentralScheduler,
            transaction_struct,
        );
    }

    #[test_case(TransactionStructure::Sdk)]
    #[test_case(TransactionStructure::View)]
    fn test_banking_stage_entryfication(transaction_struct: TransactionStructure) {
        solana_logger::setup();
        // In this attack we'll demonstrate that a verifier can interpret the ledger
        // differently if either the server doesn't signal the ledger to add an
        // Entry OR if the verifier tries to parallelize across multiple Entries.
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(2);
        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);

        // Process a batch that includes a transaction that receives two lamports.
        let alice = Keypair::new();
        let tx =
            system_transaction::transfer(&mint_keypair, &alice.pubkey(), 2, genesis_config.hash());

        let packet_batches = to_packet_batches(&[tx], 1);
        non_vote_sender
            .send(BankingPacketBatch::new(packet_batches))
            .unwrap();

        // Process a second batch that uses the same from account, so conflicts with above TX
        let tx =
            system_transaction::transfer(&mint_keypair, &alice.pubkey(), 1, genesis_config.hash());
        let packet_batches = to_packet_batches(&[tx], 1);
        non_vote_sender
            .send(BankingPacketBatch::new(packet_batches))
            .unwrap();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );

        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let entry_receiver = {
            // start a banking_stage to eat verified receiver
            let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
            let (exit, poh_recorder, transaction_recorder, poh_service, entry_receiver) =
                create_test_recorder(bank.clone(), blockstore, None, None);
            let _banking_stage = BankingStage::new(
                BlockProductionMethod::CentralScheduler,
                transaction_struct,
                &poh_recorder,
                transaction_recorder,
                non_vote_receiver,
                tpu_vote_receiver,
                gossip_vote_receiver,
                None,
                replay_vote_sender,
                None,
                bank_forks,
                &Arc::new(PrioritizationFeeCache::new(0u64)),
                HashSet::default(),
                BundleAccountLocker::default(),
                |_| 0,
            );

            // wait for banking_stage to eat the packets
            const TIMEOUT: Duration = Duration::from_secs(10);
            let start = Instant::now();
            while bank.get_balance(&alice.pubkey()) < 1 {
                if start.elapsed() > TIMEOUT {
                    panic!("banking stage took too long to process transactions");
                }
                sleep(Duration::from_millis(10));
            }
            exit.store(true, Ordering::Relaxed);
            poh_service.join().unwrap();
            entry_receiver
        };
        drop(non_vote_sender);
        drop(tpu_vote_sender);
        drop(gossip_vote_sender);

        // consume the entire entry_receiver, feed it into a new bank
        // check that the balance is what we expect.
        let entries: Vec<_> = entry_receiver
            .iter()
            .map(|(_bank, (entry, _tick_height))| entry)
            .collect();

        let (bank, _bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        for entry in entries {
            let _ = bank
                .try_process_entry_transactions(entry.transactions)
                .expect("All transactions should be processed");
        }

        // Assert the user doesn't hold three lamports. If the stage only outputs one
        // entry, then one of the transactions will be rejected, because it drives
        // the account balance below zero before the credit is added.
        assert!(bank.get_balance(&alice.pubkey()) != 3);
    }

    #[test]
    fn test_bank_record_transactions() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (bank, _bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let (poh_recorder, entry_receiver) = PohRecorder::new(
            // TODO use record_receiver
            bank.tick_height(),
            bank.last_blockhash(),
            bank.clone(),
            None,
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        let (record_sender, record_receiver) = unbounded();
        let recorder = TransactionRecorder::new(record_sender, poh_recorder.is_exited.clone());
        let poh_recorder = Arc::new(RwLock::new(poh_recorder));

        let poh_simulator = simulate_poh(record_receiver, &poh_recorder);

        poh_recorder
            .write()
            .unwrap()
            .set_bank_for_test(bank.clone());
        let pubkey = solana_pubkey::new_rand();
        let keypair2 = Keypair::new();
        let pubkey2 = solana_pubkey::new_rand();

        let txs = vec![
            system_transaction::transfer(&mint_keypair, &pubkey, 1, genesis_config.hash()).into(),
            system_transaction::transfer(&keypair2, &pubkey2, 1, genesis_config.hash()).into(),
        ];

        let _ = recorder.record_transactions(bank.slot(), txs.clone());
        let (_bank, (entry, _tick_height)) = entry_receiver.recv().unwrap();
        assert_eq!(entry.transactions, txs);

        // Once bank is set to a new bank (setting bank.slot() + 1 in record_transactions),
        // record_transactions should throw MaxHeightReached
        let next_slot = bank.slot() + 1;
        let RecordTransactionsSummary { result, .. } = recorder.record_transactions(next_slot, txs);
        assert_matches!(result, Err(PohRecorderError::MaxHeightReached));
        // Should receive nothing from PohRecorder b/c record failed
        assert!(entry_receiver.try_recv().is_err());

        poh_recorder
            .read()
            .unwrap()
            .is_exited
            .store(true, Ordering::Relaxed);
        let _ = poh_simulator.join();
    }

    pub(crate) fn create_slow_genesis_config(lamports: u64) -> GenesisConfigInfo {
        create_slow_genesis_config_with_leader(lamports, &solana_pubkey::new_rand())
    }

    pub(crate) fn create_slow_genesis_config_with_leader(
        lamports: u64,
        validator_pubkey: &Pubkey,
    ) -> GenesisConfigInfo {
        let mut config_info = create_genesis_config_with_leader(
            lamports,
            validator_pubkey,
            // See solana_ledger::genesis_utils::create_genesis_config.
            bootstrap_validator_stake_lamports(),
        );

        // For these tests there's only 1 slot, don't want to run out of ticks
        config_info.genesis_config.ticks_per_slot *= 1024;
        config_info
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

    #[test_case(TransactionStructure::Sdk)]
    #[test_case(TransactionStructure::View)]
    fn test_vote_storage_full_send(transaction_struct: TransactionStructure) {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(10000);
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        let start_hash = bank.last_blockhash();
        let banking_tracer = BankingTracer::new_disabled();
        let Channels {
            non_vote_sender,
            non_vote_receiver,
            tpu_vote_sender,
            tpu_vote_receiver,
            gossip_vote_sender,
            gossip_vote_receiver,
        } = banking_tracer.create_channels(false);
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Arc::new(
            Blockstore::open(ledger_path.path())
                .expect("Expected to be able to open database ledger"),
        );
        let (exit, poh_recorder, transaction_recorder, poh_service, _entry_receiver) =
            create_test_recorder(bank.clone(), blockstore, None, None);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();

        let banking_stage = BankingStage::new(
            BlockProductionMethod::CentralScheduler,
            transaction_struct,
            &poh_recorder,
            transaction_recorder,
            non_vote_receiver,
            tpu_vote_receiver,
            gossip_vote_receiver,
            None,
            replay_vote_sender,
            None,
            bank_forks,
            &Arc::new(PrioritizationFeeCache::new(0u64)),
            HashSet::default(),
            BundleAccountLocker::default(),
            |_| 0,
        );

        let keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
        let vote_keypairs = (0..100).map(|_| Keypair::new()).collect_vec();
        for keypair in keypairs.iter() {
            bank.process_transaction(&system_transaction::transfer(
                &mint_keypair,
                &keypair.pubkey(),
                20,
                start_hash,
            ))
            .unwrap();
        }

        // Send a bunch of votes and transfers
        let tpu_votes = (0..100_usize)
            .map(|i| {
                new_tower_sync_transaction(
                    TowerSync::from(vec![(0, 8), (1, 7), (i as u64 + 10, 6), (i as u64 + 11, 1)]),
                    Hash::new_unique(),
                    &keypairs[i],
                    &vote_keypairs[i],
                    &vote_keypairs[i],
                    None,
                )
            })
            .collect_vec();
        let gossip_votes = (0..100_usize)
            .map(|i| {
                new_tower_sync_transaction(
                    TowerSync::from(vec![(0, 9), (1, 8), (i as u64 + 5, 6), (i as u64 + 63, 1)]),
                    Hash::new_unique(),
                    &keypairs[i],
                    &vote_keypairs[i],
                    &vote_keypairs[i],
                    None,
                )
            })
            .collect_vec();
        let txs = (0..100_usize)
            .map(|i| {
                system_transaction::transfer(
                    &keypairs[i],
                    &keypairs[(i + 1) % 100].pubkey(),
                    10,
                    start_hash,
                );
            })
            .collect_vec();

        let non_vote_packet_batches = to_packet_batches(&txs, 10);
        let tpu_packet_batches = to_packet_batches(&tpu_votes, 10);
        let gossip_packet_batches = to_packet_batches(&gossip_votes, 10);

        // Send em all
        [
            (non_vote_packet_batches, non_vote_sender),
            (tpu_packet_batches, tpu_vote_sender),
            (gossip_packet_batches, gossip_vote_sender),
        ]
        .into_iter()
        .map(|(packet_batches, sender)| {
            Builder::new()
                .spawn(move || {
                    sender
                        .send(BankingPacketBatch::new(packet_batches))
                        .unwrap()
                })
                .unwrap()
        })
        .for_each(|handle| handle.join().unwrap());

        banking_stage.join().unwrap();
        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[test]
    fn test_blacklisted_accounts() {
        solana_logger::setup();

        for block_production_method in BlockProductionMethod::iter() {
            for transaction_struct in TransactionStructure::iter() {
                let GenesisConfigInfo {
                    genesis_config,
                    mint_keypair,
                    ..
                } = create_slow_genesis_config(10);
                let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
                let start_hash = bank.last_blockhash();
                let banking_tracer = BankingTracer::new_disabled();
                let Channels {
                    non_vote_sender,
                    non_vote_receiver,
                    tpu_vote_sender,
                    tpu_vote_receiver,
                    gossip_vote_sender,
                    gossip_vote_receiver,
                } = banking_tracer.create_channels(false);

                let ledger_path = get_tmp_ledger_path_auto_delete!();
                {
                    let blockstore = Arc::new(
                        Blockstore::open(ledger_path.path())
                            .expect("Expected to be able to open database ledger"),
                    );
                    let (exit, poh_recorder, transaction_recorder, poh_service, entry_receiver) =
                        create_test_recorder(bank.clone(), blockstore, None, None);

                    let (replay_vote_sender, _replay_vote_receiver) = unbounded();

                    let blacklisted_keypair = Keypair::new();

                    let banking_stage = BankingStage::new(
                        block_production_method.clone(),
                        transaction_struct.clone(),
                        &poh_recorder,
                        transaction_recorder,
                        non_vote_receiver,
                        tpu_vote_receiver,
                        gossip_vote_receiver,
                        None,
                        replay_vote_sender,
                        None,
                        bank_forks.clone(), // keep a local-copy of bank-forks so worker threads do not lose weak access to bank-forks
                        &Arc::new(PrioritizationFeeCache::new(0u64)),
                        HashSet::from_iter([blacklisted_keypair.pubkey()]),
                        BundleAccountLocker::default(),
                        |_| 0,
                    );

                    // bad tx
                    let blacklisted_tx = system_transaction::transfer(
                        &mint_keypair,
                        &blacklisted_keypair.pubkey(),
                        2,
                        start_hash,
                    );

                    // good tx
                    let good_keypair = Keypair::new();
                    let ok_tx = system_transaction::transfer(
                        &mint_keypair,
                        &good_keypair.pubkey(),
                        2,
                        start_hash,
                    );

                    // send 'em over
                    let packet_batches =
                        to_packet_batches(&[blacklisted_tx.clone(), ok_tx.clone()], 2);

                    // glad they all fit
                    assert_eq!(packet_batches.len(), 1);
                    non_vote_sender
                        .send(BankingPacketBatch::new(packet_batches))
                        .unwrap();

                    // wait for 512 ticks or 8 leader slots to pass before checking state
                    while let Ok((_bank, (_entry, tick))) = entry_receiver.recv() {
                        if tick == 511 {
                            break;
                        }
                    }

                    drop(non_vote_sender);
                    drop(tpu_vote_sender);
                    drop(gossip_vote_sender);
                    exit.store(true, Ordering::Relaxed);
                    poh_service.join().unwrap();
                    banking_stage.join().unwrap();

                    assert_eq!(bank.get_balance(&good_keypair.pubkey()), 2);
                    assert!(bank.has_signature(&ok_tx.signatures[0]));

                    assert_eq!(
                        bank.get_balance(&blacklisted_keypair.pubkey()),
                        0,
                        "test failed with config: {}_{}",
                        block_production_method,
                        transaction_struct
                    );
                    assert!(!bank.has_signature(&blacklisted_tx.signatures[0]));
                }
                Blockstore::destroy(ledger_path.path()).unwrap();
            }
        }
    }
}
