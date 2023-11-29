//! The `bundle_stage` processes bundles, which are list of transactions to be executed
//! sequentially and atomically.
use {
    crate::{
        banking_stage::decision_maker::{BufferedPacketsDecision, DecisionMaker},
        bundle_stage::{
            bundle_account_locker::BundleAccountLocker, bundle_consumer::BundleConsumer,
            bundle_packet_receiver::BundleReceiver,
            bundle_reserved_space_manager::BundleReservedSpaceManager,
            bundle_stage_leader_metrics::BundleStageLeaderMetrics, committer::Committer,
        },
        packet_bundle::PacketBundle,
        proxy::block_engine_stage::BlockBuilderFeeInfo,
        qos_service::QosService,
        tip_manager::TipManager,
        unprocessed_transaction_storage::UnprocessedTransactionStorage,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_gossip::cluster_info::ClusterInfo,
    solana_ledger::blockstore_processor::TransactionStatusSender,
    solana_measure::measure,
    solana_poh::poh_recorder::PohRecorder,
    solana_runtime::{
        bank_forks::BankForks, block_cost_limits::MAX_BLOCK_UNITS,
        prioritization_fee_cache::PrioritizationFeeCache, vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::timing::AtomicInterval,
    std::{
        collections::VecDeque,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex, RwLock,
        },
        thread::{self, Builder, JoinHandle},
        time::{Duration, Instant},
    },
};

pub mod bundle_account_locker;
mod bundle_consumer;
mod bundle_packet_deserializer;
mod bundle_packet_receiver;
mod bundle_reserved_space_manager;
pub(crate) mod bundle_stage_leader_metrics;
mod committer;

const MAX_BUNDLE_RETRY_DURATION: Duration = Duration::from_millis(10);
const SLOT_BOUNDARY_CHECK_PERIOD: Duration = Duration::from_millis(10);

// Stats emitted periodically
#[derive(Default)]
pub struct BundleStageLoopMetrics {
    last_report: AtomicInterval,
    id: u32,

    // total received
    num_bundles_received: AtomicU64,
    num_packets_received: AtomicU64,

    // newly buffered
    newly_buffered_bundles_count: AtomicU64,

    // currently buffered
    current_buffered_bundles_count: AtomicU64,
    current_buffered_packets_count: AtomicU64,

    // buffered due to cost model
    cost_model_buffered_bundles_count: AtomicU64,
    cost_model_buffered_packets_count: AtomicU64,

    // number of bundles dropped during insertion
    num_bundles_dropped: AtomicU64,

    // timings
    receive_and_buffer_bundles_elapsed_us: AtomicU64,
    process_buffered_bundles_elapsed_us: AtomicU64,
}

impl BundleStageLoopMetrics {
    fn new(id: u32) -> Self {
        BundleStageLoopMetrics {
            id,
            ..BundleStageLoopMetrics::default()
        }
    }

    pub fn increment_num_bundles_received(&mut self, count: u64) {
        self.num_bundles_received
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_num_packets_received(&mut self, count: u64) {
        self.num_packets_received
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_newly_buffered_bundles_count(&mut self, count: u64) {
        self.newly_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_current_buffered_bundles_count(&mut self, count: u64) {
        self.current_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_current_buffered_packets_count(&mut self, count: u64) {
        self.current_buffered_packets_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_cost_model_buffered_bundles_count(&mut self, count: u64) {
        self.cost_model_buffered_bundles_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_cost_model_buffered_packets_count(&mut self, count: u64) {
        self.cost_model_buffered_packets_count
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_num_bundles_dropped(&mut self, count: u64) {
        self.num_bundles_dropped.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_receive_and_buffer_bundles_elapsed_us(&mut self, count: u64) {
        self.receive_and_buffer_bundles_elapsed_us
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_process_buffered_bundles_elapsed_us(&mut self, count: u64) {
        self.process_buffered_bundles_elapsed_us
            .fetch_add(count, Ordering::Relaxed);
    }
}

impl BundleStageLoopMetrics {
    fn maybe_report(&mut self, report_interval_ms: u64) {
        if self.last_report.should_update(report_interval_ms) {
            datapoint_info!(
                "bundle_stage-loop_stats",
                ("id", self.id, i64),
                (
                    "num_bundles_received",
                    self.num_bundles_received.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "num_packets_received",
                    self.num_packets_received.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "newly_buffered_bundles_count",
                    self.newly_buffered_bundles_count.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "current_buffered_bundles_count",
                    self.current_buffered_bundles_count
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "current_buffered_packets_count",
                    self.current_buffered_packets_count
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "num_bundles_dropped",
                    self.num_bundles_dropped.swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "receive_and_buffer_bundles_elapsed_us",
                    self.receive_and_buffer_bundles_elapsed_us
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
                (
                    "process_buffered_bundles_elapsed_us",
                    self.process_buffered_bundles_elapsed_us
                        .swap(0, Ordering::Acquire) as i64,
                    i64
                ),
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
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_messages_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        preallocated_bundle_cost: u64,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
    ) -> Self {
        Self::start_bundle_thread(
            cluster_info,
            poh_recorder,
            bundle_receiver,
            transaction_status_sender,
            replay_vote_sender,
            log_messages_bytes_limit,
            exit,
            tip_manager,
            bundle_account_locker,
            MAX_BUNDLE_RETRY_DURATION,
            block_builder_fee_info,
            preallocated_bundle_cost,
            bank_forks,
            prioritization_fee_cache,
        )
    }

    pub fn join(self) -> thread::Result<()> {
        self.bundle_thread.join()
    }

    #[allow(clippy::too_many_arguments)]
    fn start_bundle_thread(
        cluster_info: &Arc<ClusterInfo>,
        poh_recorder: &Arc<RwLock<PohRecorder>>,
        bundle_receiver: Receiver<Vec<PacketBundle>>,
        transaction_status_sender: Option<TransactionStatusSender>,
        replay_vote_sender: ReplayVoteSender,
        log_message_bytes_limit: Option<usize>,
        exit: Arc<AtomicBool>,
        tip_manager: TipManager,
        bundle_account_locker: BundleAccountLocker,
        max_bundle_retry_duration: Duration,
        block_builder_fee_info: &Arc<Mutex<BlockBuilderFeeInfo>>,
        preallocated_bundle_cost: u64,
        bank_forks: Arc<RwLock<BankForks>>,
        prioritization_fee_cache: &Arc<PrioritizationFeeCache>,
    ) -> Self {
        const BUNDLE_STAGE_ID: u32 = 10_000;
        let poh_recorder = poh_recorder.clone();
        let cluster_info = cluster_info.clone();

        let mut bundle_receiver =
            BundleReceiver::new(BUNDLE_STAGE_ID, bundle_receiver, bank_forks, Some(5));

        let committer = Committer::new(
            transaction_status_sender,
            replay_vote_sender,
            prioritization_fee_cache.clone(),
        );
        let decision_maker = DecisionMaker::new(cluster_info.id(), poh_recorder.clone());

        let mut unprocessed_bundle_storage = UnprocessedTransactionStorage::new_bundle_storage(
            VecDeque::with_capacity(1_000),
            VecDeque::with_capacity(1_000),
        );

        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space = BundleReservedSpaceManager::new(
            MAX_BLOCK_UNITS,
            preallocated_bundle_cost,
            reserved_ticks,
        );

        let mut consumer = BundleConsumer::new(
            committer,
            poh_recorder.read().unwrap().new_recorder(),
            QosService::new(BUNDLE_STAGE_ID),
            log_message_bytes_limit,
            tip_manager,
            bundle_account_locker,
            block_builder_fee_info.clone(),
            max_bundle_retry_duration,
            cluster_info,
            reserved_space,
        );

        let bundle_thread = Builder::new()
            .name("solBundleStgTx".to_string())
            .spawn(move || {
                Self::process_bundles_loop(
                    &mut bundle_receiver,
                    &decision_maker,
                    &mut consumer,
                    BUNDLE_STAGE_ID,
                    &mut unprocessed_bundle_storage,
                    &exit,
                );
            })
            .unwrap();

        Self { bundle_thread }
    }

    /// Reads bundles off `bundle_receiver`, buffering in [UnprocessedTransactionStorage::BundleStorage]
    fn process_bundles_loop(
        bundle_receiver: &mut BundleReceiver,
        decision_maker: &DecisionMaker,
        consumer: &mut BundleConsumer,
        id: u32,
        unprocessed_bundle_storage: &mut UnprocessedTransactionStorage,
        exit: &Arc<AtomicBool>,
    ) {
        let mut last_metrics_update = Instant::now();

        let mut bundle_stage_metrics = BundleStageLoopMetrics::new(id);
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(id);

        while !exit.load(Ordering::Relaxed) {
            if let Err(e) = Self::process_bundles(
                bundle_receiver,
                decision_maker,
                consumer,
                unprocessed_bundle_storage,
                &mut last_metrics_update,
                &mut bundle_stage_metrics,
                &mut bundle_stage_leader_metrics,
            ) {
                error!("Bundle stage error: {e:?}");
                break;
            }
        }
    }

    fn process_bundles(
        bundle_receiver: &mut BundleReceiver,
        decision_maker: &DecisionMaker,
        consumer: &mut BundleConsumer,
        unprocessed_bundle_storage: &mut UnprocessedTransactionStorage,
        last_metrics_update: &mut Instant,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), RecvTimeoutError> {
        if !unprocessed_bundle_storage.is_empty()
            || last_metrics_update.elapsed() >= SLOT_BOUNDARY_CHECK_PERIOD
        {
            let (_, process_buffered_packets_time) = measure!(
                Self::process_buffered_bundles(
                    decision_maker,
                    consumer,
                    unprocessed_bundle_storage,
                    bundle_stage_leader_metrics,
                ),
                "process_buffered_packets",
            );
            bundle_stage_leader_metrics
                .leader_slot_metrics_tracker()
                .increment_process_buffered_packets_us(process_buffered_packets_time.as_us());
            *last_metrics_update = Instant::now();
        }

        if let Err(RecvTimeoutError::Disconnected) = bundle_receiver.receive_and_buffer_bundles(
            unprocessed_bundle_storage,
            bundle_stage_metrics,
            bundle_stage_leader_metrics,
        ) {
            return Err(RecvTimeoutError::Disconnected);
        }

        let bundle_storage = unprocessed_bundle_storage.bundle_storage().unwrap();
        bundle_stage_metrics.increment_current_buffered_bundles_count(
            bundle_storage.unprocessed_bundles_len() as u64,
        );
        bundle_stage_metrics.increment_current_buffered_packets_count(
            bundle_storage.unprocessed_packets_len() as u64,
        );
        bundle_stage_metrics.increment_cost_model_buffered_bundles_count(
            bundle_storage.cost_model_buffered_bundles_len() as u64,
        );
        bundle_stage_metrics.increment_cost_model_buffered_packets_count(
            bundle_storage.cost_model_buffered_packets_len() as u64,
        );
        bundle_stage_metrics.maybe_report(1_000);

        Ok(())
    }

    fn process_buffered_bundles(
        decision_maker: &DecisionMaker,
        consumer: &mut BundleConsumer,
        unprocessed_bundle_storage: &mut UnprocessedTransactionStorage,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) {
        let (decision, make_decision_time) =
            measure!(decision_maker.make_consume_or_forward_decision());

        let (metrics_action, banking_stage_metrics_action) =
            bundle_stage_leader_metrics.check_leader_slot_boundary(decision.bank_start());
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_make_decision_us(make_decision_time.as_us());

        match decision {
            // BufferedPacketsDecision::Consume means this leader is scheduled to be running at the moment.
            // Execute, record, and commit as many bundles possible given time, compute, and other constraints.
            BufferedPacketsDecision::Consume(bank_start) => {
                // Take metrics action before consume packets (potentially resetting the
                // slot metrics tracker to the next slot) so that we don't count the
                // packet processing metrics from the next slot towards the metrics
                // of the previous slot
                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);

                let (_, consume_buffered_packets_time) = measure!(
                    consumer.consume_buffered_bundles(
                        &bank_start,
                        unprocessed_bundle_storage,
                        bundle_stage_leader_metrics,
                    ),
                    "consume_buffered_bundles",
                );
                bundle_stage_leader_metrics
                    .leader_slot_metrics_tracker()
                    .increment_consume_buffered_packets_us(consume_buffered_packets_time.as_us());
            }
            // BufferedPacketsDecision::Forward means the leader is slot is far away.
            // Bundles aren't forwarded because it breaks atomicity guarantees, so just drop them.
            BufferedPacketsDecision::Forward => {
                let (_num_bundles_cleared, _num_cost_model_buffered_bundles) =
                    unprocessed_bundle_storage.bundle_storage().unwrap().reset();

                // TODO (LB): add metrics here for how many bundles were cleared

                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);
            }
            // BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold means the validator
            // is approaching the leader slot, hold bundles. Also, bundles aren't forwarded because it breaks
            // atomicity guarantees
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Hold => {
                bundle_stage_leader_metrics
                    .apply_action(metrics_action, banking_stage_metrics_action);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        itertools::Itertools,
        serial_test::serial,
        solana_gossip::cluster_info::Node,
        solana_perf::packet::PacketBatch,
        solana_poh::poh_recorder::create_test_recorder,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_sdk::{
            packet::Packet,
            poh_config::PohConfig,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_instruction::{self, MAX_PERMITTED_DATA_LENGTH},
            system_transaction,
            transaction::Transaction,
        },
        solana_streamer::socket::SocketAddrSpace,
    };

    pub(crate) fn new_test_cluster_info(keypair: Option<Arc<Keypair>>) -> (Node, ClusterInfo) {
        let keypair = keypair.unwrap_or_else(|| Arc::new(Keypair::new()));
        let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
        let cluster_info =
            ClusterInfo::new(node.info.clone(), keypair, SocketAddrSpace::Unspecified);
        (node, cluster_info)
    }

    #[test]
    #[serial]
    fn test_basic_bundle() {
        solana_logger::setup();
        const BUNDLE_STAGE_ID: u32 = 10_000;
        let bundle_consumer::tests::TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            blockstore: _,
            exit: _,
            poh_recorder,
            poh_simulator: _,
            entry_receiver: _entry_receiver,
        } = bundle_consumer::tests::create_test_fixture(10_000_000);
        let tip_manager =
            bundle_consumer::tests::get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let mint_keypair = genesis_config_info.mint_keypair;
        let bank_forks = Arc::new(RwLock::new(BankForks::new_from_banks(
            &[bank.clone()],
            bank.slot(),
        )));
        let (_, cluster_info) = new_test_cluster_info(Some(Arc::new(leader_keypair)));
        let cluster_info = Arc::new(cluster_info);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let (bundle_sender, bundle_receiver) = unbounded();
        let mut bundle_receiver =
            BundleReceiver::new(BUNDLE_STAGE_ID, bundle_receiver, bank_forks, Some(5));
        // Queue the bundles
        let recent_blockhash = genesis_config_info.genesis_config.hash();
        let kp = Keypair::new();
        let txn0 =
            system_transaction::transfer(&mint_keypair, &kp.pubkey(), 1_000_000, recent_blockhash);
        let txn1 =
            system_transaction::transfer(&mint_keypair, &kp.pubkey(), 2_000_000, recent_blockhash);

        bundle_sender
            .send(vec![PacketBundle {
                batch: PacketBatch::new(vec![
                    Packet::from_data(None, txn0).unwrap(),
                    Packet::from_data(None, txn1).unwrap(),
                ]),
                bundle_id: String::default(),
            }])
            .unwrap();
        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::default()),
        );

        let mut unprocessed_transaction_storage = UnprocessedTransactionStorage::new_bundle_storage(
            VecDeque::with_capacity(1_000),
            VecDeque::with_capacity(1_000),
        );

        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space =
            BundleReservedSpaceManager::new(MAX_BLOCK_UNITS, 3_000_000, reserved_ticks);

        let mut consumer = BundleConsumer::new(
            committer,
            poh_recorder.read().unwrap().new_recorder(),
            QosService::new(BUNDLE_STAGE_ID),
            None,
            tip_manager,
            BundleAccountLocker::default(),
            Arc::new(Mutex::new(BlockBuilderFeeInfo {
                block_builder: cluster_info.keypair().pubkey(),
                block_builder_commission: 0,
            })),
            MAX_BUNDLE_RETRY_DURATION,
            cluster_info.clone(),
            reserved_space,
        );

        // sanity check
        assert_eq!(bank.get_balance(&kp.pubkey()), 0);
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_unprocessed_bundle_storage_len(),
            0
        );
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_cost_model_buffered_bundle_storage_len(),
            0
        );

        let mut last_metrics_update = Instant::now();
        let mut bundle_stage_metrics = BundleStageLoopMetrics::new(BUNDLE_STAGE_ID);
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(BUNDLE_STAGE_ID);

        // first run, just buffers off receiver, does not process them
        BundleStage::process_bundles(
            &mut bundle_receiver,
            &DecisionMaker::new(cluster_info.id(), poh_recorder.clone()),
            &mut consumer,
            &mut unprocessed_transaction_storage,
            &mut last_metrics_update,
            &mut bundle_stage_metrics,
            &mut bundle_stage_leader_metrics,
        )
        .unwrap();

        // now process
        BundleStage::process_bundles(
            &mut bundle_receiver,
            &DecisionMaker::new(cluster_info.id(), poh_recorder.clone()),
            &mut consumer,
            &mut unprocessed_transaction_storage,
            &mut last_metrics_update,
            &mut bundle_stage_metrics,
            &mut bundle_stage_leader_metrics,
        )
        .unwrap();

        assert_eq!(bank.get_balance(&kp.pubkey()), 3_000_000);
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_unprocessed_bundle_storage_len(),
            0
        );
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_cost_model_buffered_bundle_storage_len(),
            0
        )
    }

    #[test]
    #[serial]
    fn test_rebuffer_exceed_max_attempts() {
        solana_logger::setup();
        const BUNDLE_STAGE_ID: u32 = 10_000;
        const BUNDLE_RETRY_ATTEMPTS: u8 = 2;

        let bundle_consumer::tests::TestFixture {
            genesis_config_info,
            leader_keypair,
            bank,
            blockstore,
            exit: _,
            poh_recorder,
            poh_simulator: _,
            entry_receiver: _entry_receiver,
        } = bundle_consumer::tests::create_test_fixture(10_000_000);
        let tip_manager =
            bundle_consumer::tests::get_tip_manager(&genesis_config_info.voting_keypair.pubkey());
        let mint_keypair = genesis_config_info.mint_keypair;
        let bank_forks = Arc::new(RwLock::new(BankForks::new_from_banks(
            &[bank.clone()],
            bank.slot(),
        )));
        let (_, cluster_info) = new_test_cluster_info(Some(Arc::new(leader_keypair)));
        let cluster_info = Arc::new(cluster_info);
        let (replay_vote_sender, _replay_vote_receiver) = unbounded();
        let (bundle_sender, bundle_receiver) = unbounded();
        let mut bundle_receiver =
            BundleReceiver::new(BUNDLE_STAGE_ID, bundle_receiver, bank_forks, Some(5));
        let rent = genesis_config_info
            .genesis_config
            .rent
            .minimum_balance(MAX_PERMITTED_DATA_LENGTH as usize);
        let test_keypairs = (0..10).map(|_| Keypair::new()).collect_vec();
        let ixs = test_keypairs
            .iter()
            .map(|kp| {
                system_instruction::create_account(
                    &mint_keypair.pubkey(),
                    &kp.pubkey(),
                    rent,
                    MAX_PERMITTED_DATA_LENGTH,
                    &solana_sdk::system_program::ID,
                )
            })
            .collect_vec();
        let recent_blockhash = bank.last_blockhash();
        let txn0 = Transaction::new_signed_with_payer(
            ixs.iter().take(5).cloned().collect_vec().as_slice(),
            Some(&mint_keypair.pubkey()),
            std::iter::once(&mint_keypair)
                .chain(test_keypairs.iter().take(5))
                .collect_vec()
                .as_slice(),
            recent_blockhash,
        );
        let txn1 = Transaction::new_signed_with_payer(
            ixs.iter().skip(5).cloned().collect_vec().as_slice(),
            Some(&mint_keypair.pubkey()),
            std::iter::once(&mint_keypair)
                .chain(test_keypairs.iter().skip(5))
                .collect_vec()
                .as_slice(),
            recent_blockhash,
        );
        bundle_sender
            .send(vec![PacketBundle {
                batch: PacketBatch::new(vec![
                    Packet::from_data(None, txn0).unwrap(),
                    Packet::from_data(None, txn1).unwrap(),
                ]),
                bundle_id: String::default(),
            }])
            .unwrap();

        let committer = Committer::new(
            None,
            replay_vote_sender,
            Arc::new(PrioritizationFeeCache::default()),
        );

        let mut unprocessed_transaction_storage =
            UnprocessedTransactionStorage::new_bundle_storage_ttl(
                VecDeque::with_capacity(1_000),
                VecDeque::with_capacity(1_000),
                BUNDLE_RETRY_ATTEMPTS,
            );

        let reserved_ticks = poh_recorder
            .read()
            .unwrap()
            .ticks_per_slot()
            .saturating_mul(8)
            .saturating_div(10);

        // The first 80% of the block, based on poh ticks, has `preallocated_bundle_cost` less compute units.
        // The last 20% has has full compute so blockspace is maximized if BundleStage is idle.
        let reserved_space =
            BundleReservedSpaceManager::new(MAX_BLOCK_UNITS, 3_000_000, reserved_ticks);

        let mut consumer = BundleConsumer::new(
            committer,
            poh_recorder.read().unwrap().new_recorder(),
            QosService::new(BUNDLE_STAGE_ID),
            None,
            tip_manager,
            BundleAccountLocker::default(),
            Arc::new(Mutex::new(BlockBuilderFeeInfo {
                block_builder: cluster_info.keypair().pubkey(),
                block_builder_commission: 0,
            })),
            MAX_BUNDLE_RETRY_DURATION,
            cluster_info.clone(),
            reserved_space,
        );

        // sanity check
        assert!(test_keypairs
            .iter()
            .all(|kp| bank.get_balance(&kp.pubkey()) == 0));
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_unprocessed_bundle_storage_len(),
            0
        );
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_cost_model_buffered_bundle_storage_len(),
            0
        );

        let mut last_metrics_update = Instant::now();
        let mut bundle_stage_metrics = BundleStageLoopMetrics::new(BUNDLE_STAGE_ID);
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(BUNDLE_STAGE_ID);

        // first run, just buffers off receiver, does not process them
        BundleStage::process_bundles(
            &mut bundle_receiver,
            &DecisionMaker::new(cluster_info.id(), poh_recorder.clone()),
            &mut consumer,
            &mut unprocessed_transaction_storage,
            &mut last_metrics_update,
            &mut bundle_stage_metrics,
            &mut bundle_stage_leader_metrics,
        )
        .unwrap();
        assert!(test_keypairs
            .iter()
            .all(|kp| bank.get_balance(&kp.pubkey()) == 0));
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_unprocessed_bundle_storage_len(),
            1
        );
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_cost_model_buffered_bundle_storage_len(),
            0
        );

        let mut curr_bank = bank;
        // retry until reached, but not exceeding max attempts
        (1..BUNDLE_RETRY_ATTEMPTS + 1).for_each(|_i| {
            // advance the slot so we can evaluate cost_model_buffered_bundle_storage
            let new_bank = Arc::new(Bank::new_from_parent(
                &curr_bank,
                &Pubkey::default(),
                curr_bank.slot() + 1,
            ));
            let (_exit, poh_recorder, _poh_simulator, _entry_receiver) = create_test_recorder(
                &new_bank,
                blockstore.clone(),
                Some(PohConfig::default()),
                None,
            );
            curr_bank = new_bank.clone();
            BundleStage::process_bundles(
                &mut bundle_receiver,
                &DecisionMaker::new(cluster_info.id(), poh_recorder),
                &mut consumer,
                &mut unprocessed_transaction_storage,
                &mut last_metrics_update,
                &mut bundle_stage_metrics,
                &mut bundle_stage_leader_metrics,
            )
            .unwrap();
            assert!(test_keypairs
                .iter()
                .all(|kp| new_bank.get_balance(&kp.pubkey()) == 0));
            assert_eq!(
                unprocessed_transaction_storage
                    .bundle_storage()
                    .unwrap()
                    .get_unprocessed_bundle_storage_len(),
                0
            );
            assert_eq!(
                unprocessed_transaction_storage
                    .bundle_storage()
                    .unwrap()
                    .get_cost_model_buffered_bundle_storage_len(),
                1
            );
        });

        // exceed attempt limit, bundle should be removed from buffers
        BundleStage::process_bundles(
            &mut bundle_receiver,
            &DecisionMaker::new(cluster_info.id(), poh_recorder),
            &mut consumer,
            &mut unprocessed_transaction_storage,
            &mut last_metrics_update,
            &mut bundle_stage_metrics,
            &mut bundle_stage_leader_metrics,
        )
        .unwrap();
        assert!(test_keypairs
            .iter()
            .all(|kp| curr_bank.get_balance(&kp.pubkey()) == 0));
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_unprocessed_bundle_storage_len(),
            0
        );
        assert_eq!(
            unprocessed_transaction_storage
                .bundle_storage()
                .unwrap()
                .get_cost_model_buffered_bundle_storage_len(),
            0
        );
    }
}
