/// A Scheduler implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
///
use crate::banking_stage::transaction_scheduler::scheduler::PreLockFilterAction;
use {
    super::{
        bam_receive_and_buffer::priority_to_seq_id,
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::{
        bam_dependencies::BamOutboundMessage,
        banking_stage::{
            decision_maker::BufferedPacketsDecision,
            scheduler_messages::{
                ConsumeWork, FinishedConsumeWork, NotCommittedReason, TransactionBatchId,
                TransactionResult,
            },
            transaction_scheduler::{
                bam_utils::convert_txn_error_to_proto, scheduler_common::SchedulingCommon,
                transaction_state::TransactionState,
            },
        },
    },
    crossbeam_channel::{Receiver, Sender},
    histogram::Histogram,
    itertools::Itertools,
    jito_protos::proto::bam_types::{
        atomic_txn_batch_result, not_committed::Reason, SchedulingError,
    },
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    smallvec::SmallVec,
    solana_clock::{Slot, MAX_PROCESSING_AGE},
    solana_nohash_hasher::IntMap,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction_error::TransactionError,
    std::{
        borrow::Borrow,
        sync::{Arc, RwLock},
        time::Instant,
    },
};

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

#[inline(always)]
fn passthrough_priority(
    id: &TransactionPriorityId,
    _graph_node: &GraphNode<TransactionPriorityId>,
) -> TransactionPriorityId {
    *id
}

pub const MAX_PACKETS_PER_BUNDLE: usize = 5; // copied from BundleStorage::MAX_PACKETS_PER_BUNDLE

pub struct BamScheduler<Tx: TransactionWithMeta> {
    consume_work_sender: Sender<ConsumeWork<Tx>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: Sender<BamOutboundMessage>,

    next_batch_id: u64,
    inflight_batch_info: IntMap<TransactionBatchId, InflightBatchInfo>,
    prio_graph: SchedulerPrioGraph,
    /// seq_id is the key
    insertion_to_prio_graph_time: IntMap<u32, Instant>,
    time_in_priograph_us: Histogram,
    time_in_worker_us: Histogram,
    time_between_schedule_us: Histogram,
    last_schedule_time: Instant,
    slot: Option<Slot>,

    // Reusable objects to avoid allocations
    reusable_consume_work: Vec<ConsumeWork<Tx>>,
    reusable_priority_ids: Vec<SmallVec<[TransactionPriorityId; 1]>>,

    extra_checks_enabled: bool,
    bank_forks: Arc<RwLock<BankForks>>,
}

// A structure to hold information about inflight batches.
// A batch can either be one 'revert_on_error' batch or multiple
// 'non-revert_on_error' batches that are scheduled together.
struct InflightBatchInfo {
    pub schedule_time: Instant,
    pub batch_priority_ids: SmallVec<[TransactionPriorityId; 1]>,
    pub slot: Slot,
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_sender: Sender<ConsumeWork<Tx>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: Sender<BamOutboundMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
    ) -> Self {
        Self {
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            next_batch_id: 0,
            inflight_batch_info: IntMap::default(),
            prio_graph: PrioGraph::new(passthrough_priority),
            insertion_to_prio_graph_time: IntMap::default(),
            time_in_priograph_us: Histogram::new(),
            time_in_worker_us: Histogram::new(),
            time_between_schedule_us: Histogram::new(),
            last_schedule_time: Instant::now(),
            slot: None,
            reusable_consume_work: Vec::new(),
            reusable_priority_ids: Vec::new(),
            extra_checks_enabled: true,
            bank_forks,
        }
    }

    /// Gets accessed accounts (resources) for use in `PrioGraph`.
    fn get_transactions_account_access<'a>(
        transactions: impl Iterator<Item = &'a (impl SVMMessage + 'a)> + 'a,
    ) -> impl Iterator<Item = (Pubkey, AccessKind)> + 'a {
        transactions.flat_map(|txn| {
            txn.account_keys().iter().enumerate().map(|(index, key)| {
                if txn.is_writable(index) {
                    (*key, AccessKind::Write)
                } else {
                    (*key, AccessKind::Read)
                }
            })
        })
    }

    /// Insert all incoming transactions into the `PrioGraph`.
    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        let Some(slot) = self.slot else {
            warn!("Slot is not set, cannot pull transactions into prio-graph");
            return;
        };

        let working_bank = self.bank_forks.read().unwrap().working_bank();

        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, _, max_schedule_slot)) = container.get_batch(next_batch_id.id)
            else {
                error!("Batch {} not found in container", next_batch_id.id);
                continue;
            };

            if max_schedule_slot < slot {
                // If the slot has changed, we cannot schedule this batch
                let seq_id = priority_to_seq_id(next_batch_id.priority);
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(next_batch_id.id);
                continue;
            }

            let txns = batch_ids
                .iter()
                .filter_map(|txn_id| container.get_transaction(*txn_id))
                .collect::<SmallVec<[&Tx; MAX_PACKETS_PER_BUNDLE]>>();

            if self.extra_checks_enabled {
                let lock_results: SmallVec<
                    [solana_transaction_error::TransactionResult<()>; MAX_PACKETS_PER_BUNDLE],
                > = SmallVec::from_elem(Ok(()), txns.len());
                let check_result = working_bank.check_transactions::<Tx>(
                    &txns,
                    &lock_results,
                    MAX_PROCESSING_AGE,
                    &mut TransactionErrorMetrics::default(),
                );
                if let Some((index, err)) = check_result
                    .iter()
                    .find_position(|res| res.is_err())
                    .map(|(i, res)| (i, res.as_ref().err().unwrap().clone()))
                {
                    drop(txns);
                    container.remove_by_id(next_batch_id.id);

                    let seq_id = priority_to_seq_id(next_batch_id.priority);
                    let result = atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Self::convert_reason_to_proto(
                                index,
                                NotCommittedReason::Error(err),
                            )),
                        },
                    );
                    self.send_back_result(seq_id, result);
                    continue;
                };
            }

            self.insertion_to_prio_graph_time
                .insert(priority_to_seq_id(next_batch_id.priority), Instant::now());
            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(txns.into_iter()),
            );
        }
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        num_scheduled: &mut usize,
    ) {
        let Some(slot) = self.slot else {
            warn!("Slot is not set, cannot schedule transactions");
            return;
        };

        let now = Instant::now();
        let working_bank = self.bank_forks.read().unwrap().working_bank();
        while let Some(id) = self.prio_graph.pop() {
            let (batch_ids, revert_on_error, max_schedule_slot) =
                container.get_batch(id.id).unwrap();

            // Update time in prio-graph metric
            if let Some(insertion_time) = self
                .insertion_to_prio_graph_time
                .remove(&priority_to_seq_id(id.priority))
            {
                let _ = self
                    .time_in_priograph_us
                    .increment(now.duration_since(insertion_time).as_micros() as u64);
            };

            // Filter on slot
            if max_schedule_slot < slot {
                self.prio_graph.unblock(&id);
                let seq_id = priority_to_seq_id(id.priority);
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(id.id);
                continue;
            }

            // Filter on check_transactions
            if self.extra_checks_enabled {
                let mut sanitized_txs: SmallVec<[&Tx; MAX_PACKETS_PER_BUNDLE]> = SmallVec::new();
                let mut lock_results: SmallVec<
                    [solana_transaction_error::TransactionResult<()>; MAX_PACKETS_PER_BUNDLE],
                > = SmallVec::new();
                for txn_id in batch_ids.iter() {
                    if let Some(txn) = container.get_transaction(*txn_id) {
                        sanitized_txs.push(txn.borrow());
                        lock_results.push(Ok(()));
                    }
                }
                let check_result = working_bank.check_transactions::<Tx>(
                    &sanitized_txs,
                    &lock_results,
                    MAX_PROCESSING_AGE,
                    &mut TransactionErrorMetrics::default(),
                );
                if let Some((index, err)) = check_result
                    .iter()
                    .find_position(|res| res.is_err())
                    .map(|(i, res)| (i, res.as_ref().err().unwrap().clone()))
                {
                    drop(sanitized_txs);
                    container.remove_by_id(id.id);
                    self.prio_graph.unblock(&id);

                    let seq_id = priority_to_seq_id(id.priority);
                    let result = atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Self::convert_reason_to_proto(
                                index,
                                NotCommittedReason::Error(err),
                            )),
                        },
                    );
                    self.send_back_result(seq_id, result);
                    continue;
                };
            }

            // Schedule it
            let mut work = self.get_or_create_work_object();
            let batch_id = self.get_next_schedule_id();
            *num_scheduled += batch_ids.len();
            Self::generate_work(&mut work, batch_id, &[id], revert_on_error, container, slot);
            self.send_to_worker(SmallVec::from([id]), work, slot);
        }
    }

    fn send_to_worker(
        &mut self,
        priority_ids: SmallVec<[TransactionPriorityId; 1]>,
        work: ConsumeWork<Tx>,
        slot: Slot,
    ) {
        let batch_id = work.batch_id;
        let _ = self.consume_work_sender.send(work);
        self.inflight_batch_info.insert(
            batch_id,
            InflightBatchInfo {
                schedule_time: Instant::now(),
                batch_priority_ids: priority_ids,
                slot,
            },
        );
    }

    fn get_next_schedule_id(&mut self) -> TransactionBatchId {
        let result = TransactionBatchId::new(self.next_batch_id);
        self.next_batch_id += 1;
        result
    }

    fn get_or_create_work_object(&mut self) -> ConsumeWork<Tx> {
        if let Some(work) = self.reusable_consume_work.pop() {
            work
        } else {
            // These values will be overwritten by `generate_work`
            ConsumeWork {
                batch_id: TransactionBatchId::new(0),
                ids: Vec::with_capacity(1),
                transactions: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                max_ages: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                revert_on_error: false,
                respond_with_extra_info: false,
                max_schedule_slot: None,
            }
        }
    }

    fn recycle_work_object(&mut self, mut work: ConsumeWork<Tx>) {
        // Just in case, clear the work object
        work.ids.clear();
        work.transactions.clear();
        work.max_ages.clear();
        self.reusable_consume_work.push(work);
    }

    fn recycle_priority_ids(&mut self, mut priority_ids: SmallVec<[TransactionPriorityId; 1]>) {
        priority_ids.clear();
        self.reusable_priority_ids.push(priority_ids);
    }

    fn generate_work(
        output: &mut ConsumeWork<Tx>,
        batch_id: TransactionBatchId,
        priority_ids: &[TransactionPriorityId],
        revert_on_error: bool,
        container: &mut impl StateContainer<Tx>,
        slot: Slot,
    ) {
        output.ids.clear();
        output.ids.extend(
            priority_ids
                .iter()
                .filter_map(|priority_id| container.get_batch(priority_id.id))
                .flat_map(|(batch_ids, _, _)| batch_ids.into_iter())
                .copied(),
        );

        output.transactions.clear();
        output.max_ages.clear();
        for (txn, max_age) in output.ids.iter().filter_map(|txn_id| {
            let result = container.get_mut_transaction_state(*txn_id)?;
            let result = result.take_transaction_for_scheduling();
            Some(result)
        }) {
            output.transactions.push(txn);
            output.max_ages.push(max_age);
        }

        output.batch_id = batch_id;
        output.revert_on_error = revert_on_error;
        output.max_schedule_slot = Some(slot);
        output.respond_with_extra_info = true;
    }

    fn send_no_leader_slot_bundle_result(&self, seq_id: u32) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            ));
    }

    fn send_back_result(&self, seq_id: u32, result: atomic_txn_batch_result::Result) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(result),
                },
            ));
    }

    /// Generates a `bundle_result::Result` based on the processed results for 'revert_on_error' batches.
    fn generate_revert_on_error_bundle_result(
        processed_results: &[TransactionResult],
    ) -> atomic_txn_batch_result::Result {
        if processed_results
            .iter()
            .all(|result| matches!(result, TransactionResult::Committed(_)))
        {
            let transaction_results = processed_results
                .iter()
                .filter_map(|result| {
                    if let TransactionResult::Committed(processed) = result {
                        Some(processed.clone())
                    } else {
                        None
                    }
                })
                .collect();
            atomic_txn_batch_result::Result::Committed(jito_protos::proto::bam_types::Committed {
                transaction_results,
            })
        } else {
            let mut index = 0;
            let mut not_commit_reason = NotCommittedReason::PohTimeout;
            for (i, result) in processed_results.iter().enumerate() {
                match result {
                    TransactionResult::NotCommitted(NotCommittedReason::Error(err)) => {
                        // TransactionError::CommitCancelled used to indicate that another transaction in this bundle errored out
                        if *err != TransactionError::CommitCancelled {
                            index = i;
                            not_commit_reason = NotCommittedReason::Error(err.clone());
                            break;
                        }
                    }
                    TransactionResult::NotCommitted(NotCommittedReason::PohTimeout) => {
                        index = i;
                        not_commit_reason = NotCommittedReason::PohTimeout;
                        break;
                    }
                    _ => {}
                }
            }

            atomic_txn_batch_result::Result::NotCommitted(
                jito_protos::proto::bam_types::NotCommitted {
                    reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
                },
            )
        }
    }

    /// Generates a `bundle_result::Result` based on the processed result of a single transaction.
    fn generate_bundle_result(processed: &TransactionResult) -> atomic_txn_batch_result::Result {
        match processed {
            TransactionResult::Committed(result) => atomic_txn_batch_result::Result::Committed(
                jito_protos::proto::bam_types::Committed {
                    transaction_results: vec![result.clone()],
                },
            ),
            TransactionResult::NotCommitted(reason) => {
                let (index, not_commit_reason) = match reason {
                    NotCommittedReason::PohTimeout => (0, NotCommittedReason::PohTimeout),
                    NotCommittedReason::Error(err) => (0, NotCommittedReason::Error(err.clone())),
                };
                atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(Self::convert_reason_to_proto(index, not_commit_reason)),
                    },
                )
            }
        }
    }

    fn convert_reason_to_proto(
        index: usize,
        reason: NotCommittedReason,
    ) -> jito_protos::proto::bam_types::not_committed::Reason {
        match reason {
            NotCommittedReason::PohTimeout => {
                jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                    SchedulingError::PohTimeout as i32,
                )
            }
            NotCommittedReason::Error(err) => {
                jito_protos::proto::bam_types::not_committed::Reason::TransactionError(
                    jito_protos::proto::bam_types::TransactionError {
                        index: index as u32,
                        reason: convert_txn_error_to_proto(err) as i32,
                    },
                )
            }
        }
    }

    fn maybe_bank_boundary_actions(
        &mut self,
        decision: &BufferedPacketsDecision,
        container: &mut impl StateContainer<Tx>,
    ) {
        // Check if no bank or slot has changed
        let maybe_bank = decision.bank();
        if maybe_bank.map(|bank| bank.slot()) == self.slot {
            return;
        }
        let prev_slot = self.slot;
        if let Some(bank) = maybe_bank {
            info!(
                "Bank boundary detected: slot changed from {:?} to {:?}",
                self.slot,
                bank.slot()
            );
            self.slot = Some(bank.slot());
        } else {
            info!("Bank boundary detected: slot changed to None");
            self.slot = None;
        }

        // Drain container and send back 'retryable'
        if self.slot.is_none() {
            while let Some(next_batch_id) = container.pop() {
                let seq_id = priority_to_seq_id(next_batch_id.priority);
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(next_batch_id.id);
            }
        }

        // Unblock all transactions blocked by inflight batches
        // and then drain the prio-graph
        for (_, inflight_info) in self.inflight_batch_info.iter() {
            for priority_id in &inflight_info.batch_priority_ids {
                if prev_slot == Some(inflight_info.slot) {
                    self.prio_graph.unblock(priority_id);
                }
            }
        }
        let now = Instant::now();
        while let Some((next_batch_id, _)) = self.prio_graph.pop_and_unblock() {
            if let Some(insertion_time) = self
                .insertion_to_prio_graph_time
                .remove(&priority_to_seq_id(next_batch_id.priority))
            {
                let _ = self
                    .time_in_priograph_us
                    .increment(now.duration_since(insertion_time).as_micros() as u64);
            };

            let seq_id = priority_to_seq_id(next_batch_id.priority);
            self.send_no_leader_slot_bundle_result(seq_id);
            container.remove_by_id(next_batch_id.id);
        }

        self.prio_graph.clear();
        self.insertion_to_prio_graph_time.clear();

        // Only report timing metrics when slot has ended
        if self.slot.is_none() {
            self.report_histogram_metrics();
        }

        self.last_schedule_time = Instant::now();
    }

    fn report_histogram_metrics(&mut self) {
        datapoint_info!(
            "bam_scheduler_bank_boundary-metrics",
            (
                "time_in_priograph_us_p50",
                self.time_in_priograph_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_in_priograph_us_p75",
                self.time_in_priograph_us
                    .percentile(75.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_in_priograph_us_p90",
                self.time_in_priograph_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_in_priograph_us_p99",
                self.time_in_priograph_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_in_priograph_us_max",
                self.time_in_priograph_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_in_priograph_us.clear();

        datapoint_info!(
            "bam_scheduler_worker_time_metrics",
            (
                "time_in_worker_us_p50",
                self.time_in_worker_us.percentile(50.0).unwrap_or_default(),
                i64
            ),
            (
                "time_in_worker_us_p75",
                self.time_in_worker_us.percentile(75.0).unwrap_or_default(),
                i64
            ),
            (
                "time_in_worker_us_p90",
                self.time_in_worker_us.percentile(90.0).unwrap_or_default(),
                i64
            ),
            (
                "time_in_worker_us_p99",
                self.time_in_worker_us.percentile(99.0).unwrap_or_default(),
                i64
            ),
            (
                "time_in_worker_us_max",
                self.time_in_worker_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_in_worker_us.clear();

        datapoint_info!(
            "bam_scheduler_time_between_schedules_metrics",
            (
                "time_between_schedule_us_p50",
                self.time_between_schedule_us
                    .percentile(50.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_between_schedule_us_p75",
                self.time_between_schedule_us
                    .percentile(75.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_between_schedule_us_p90",
                self.time_between_schedule_us
                    .percentile(90.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_between_schedule_us_p99",
                self.time_between_schedule_us
                    .percentile(99.0)
                    .unwrap_or_default(),
                i64
            ),
            (
                "time_between_schedule_us_max",
                self.time_between_schedule_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_between_schedule_us.clear();
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for BamScheduler<Tx> {
    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        _budget: u64,
        _pre_graph_filter: impl Fn(&[&Tx], &mut [bool]),
        _pre_lock_filter: impl Fn(&TransactionState<Tx>) -> PreLockFilterAction,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let starting_queue_size = container.queue_size();
        let starting_buffer_size = container.buffer_size();

        let start_time = Instant::now();
        let time_since_last_schedule = start_time.duration_since(self.last_schedule_time);
        self.last_schedule_time = start_time;
        let _ = self
            .time_between_schedule_us
            .increment(time_since_last_schedule.as_micros() as u64);

        let mut num_scheduled = 0;

        self.pull_into_prio_graph(container);
        self.send_to_workers(container, &mut num_scheduled);

        // TODO(seg): Double check the zeros here
        Ok(SchedulingSummary {
            starting_queue_size,
            starting_buffer_size,
            num_scheduled,
            num_unschedulable_conflicts: 0,
            num_filtered_out: 0,
            filter_time_us: start_time.elapsed().as_micros() as u64,
            num_unschedulable_threads: 0,
        })
    }

    /// Receive completed batches of transactions without blocking.
    /// This also handles checking if the slot has ended and if so, it will
    /// drain the container and prio-graph, sending back 'retryable' results
    /// back to BAM.
    fn receive_completed(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        decision: &BufferedPacketsDecision,
    ) -> Result<(usize, usize), SchedulerError> {
        // Check if the slot/bank has changed; do what must be done
        // IMPORTANT: This must be called before the receiving code below
        self.maybe_bank_boundary_actions(decision, container);

        let mut num_transactions = 0;
        let now = Instant::now();
        while let Ok(result) = self.finished_consume_work_receiver.try_recv() {
            num_transactions += result.work.ids.len();
            let batch_id = result.work.batch_id;
            let revert_on_error = result.work.revert_on_error;
            self.recycle_work_object(result.work);

            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                continue;
            };

            let _ = self.time_in_worker_us.increment(
                now.duration_since(inflight_batch_info.schedule_time)
                    .as_micros() as u64,
            );

            // Should never not be 1; but just in case
            let len = if revert_on_error {
                1
            } else {
                inflight_batch_info.batch_priority_ids.len()
            };
            for (i, priority_id) in inflight_batch_info
                .batch_priority_ids
                .iter()
                .enumerate()
                .take(len)
            {
                // If we got extra info, we can send back the result
                if let Some(extra_info) = result.extra_info.as_ref() {
                    let bundle_result = if revert_on_error {
                        Self::generate_revert_on_error_bundle_result(&extra_info.processed_results)
                    } else {
                        let Some(txn_result) = extra_info.processed_results.get(i) else {
                            warn!(
                                "Processed results for batch {} are missing for index {}",
                                batch_id.0, i
                            );
                            continue;
                        };
                        Self::generate_bundle_result(txn_result)
                    };
                    self.send_back_result(priority_to_seq_id(priority_id.priority), bundle_result);
                }

                // If in the same slot, unblock the transaction
                if Some(inflight_batch_info.slot) == self.slot {
                    self.prio_graph.unblock(priority_id);
                }

                // Remove the transaction from the container
                container.remove_by_id(priority_id.id);
            }
            self.recycle_priority_ids(inflight_batch_info.batch_priority_ids);
        }

        Ok((num_transactions, 0))
    }

    fn scheduling_common_mut(&mut self) -> &mut SchedulingCommon<Tx> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            bam_dependencies::BamOutboundMessage,
            banking_stage::{
                decision_maker::BufferedPacketsDecision,
                scheduler_messages::{
                    ConsumeWork, FinishedConsumeWork, MaxAge, NotCommittedReason, TransactionResult,
                },
                tests::create_slow_genesis_config,
                transaction_scheduler::{
                    bam_receive_and_buffer::seq_id_to_priority,
                    bam_scheduler::{BamScheduler, MAX_PACKETS_PER_BUNDLE},
                    scheduler::{PreLockFilterAction, Scheduler},
                    transaction_state_container::{StateContainer, TransactionStateContainer},
                },
            },
        },
        crossbeam_channel::unbounded,
        itertools::Itertools,
        jito_protos::proto::bam_types::{
            atomic_txn_batch_result::Result::{Committed, NotCommitted},
            TransactionCommittedResult,
        },
        smallvec::SmallVec,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_system_interface::instruction::transfer_many,
        solana_transaction::{sanitized::SanitizedTransaction, Transaction},
        std::{
            borrow::Borrow,
            sync::{Arc, RwLock},
        },
    };

    struct TestScheduler {
        scheduler: BamScheduler<RuntimeTransaction<SanitizedTransaction>>,
        consume_work_receivers:
            Vec<crossbeam_channel::Receiver<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>>,
        finished_consume_work_sender: crossbeam_channel::Sender<
            FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>,
        >,
        response_receiver: crossbeam_channel::Receiver<BamOutboundMessage>,
    }

    fn create_test_scheduler(
        num_threads: usize,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> TestScheduler {
        let (consume_work_sender, consume_work_receiver) = unbounded();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = unbounded();
        test_bank_forks();
        let scheduler = BamScheduler::new(
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            bank_forks.clone(),
        );
        TestScheduler {
            scheduler,
            consume_work_receivers: (0..num_threads)
                .map(|_| consume_work_receiver.clone())
                .collect(),
            finished_consume_work_sender,
            response_receiver,
        }
    }

    fn prioritized_tranfers(
        from_keypair: &Keypair,
        to_pubkeys: impl IntoIterator<Item = impl Borrow<Pubkey>>,
        lamports: u64,
        priority: u64,
    ) -> RuntimeTransaction<SanitizedTransaction> {
        let to_pubkeys_lamports = to_pubkeys
            .into_iter()
            .map(|pubkey| *pubkey.borrow())
            .zip(std::iter::repeat(lamports))
            .collect_vec();
        let mut ixs = transfer_many(&from_keypair.pubkey(), &to_pubkeys_lamports);
        let prioritization = ComputeBudgetInstruction::set_compute_unit_price(priority);
        ixs.push(prioritization);
        let message = Message::new(&ixs, Some(&from_keypair.pubkey()));
        let tx = Transaction::new(&[from_keypair], message, Hash::default());
        RuntimeTransaction::from_transaction_for_tests(tx)
    }

    fn create_container(
        tx_infos: impl IntoIterator<
            Item = (
                impl Borrow<Keypair>,
                impl IntoIterator<Item = impl Borrow<Pubkey>>,
                u64,
                u64,
                u64,
            ),
        >,
    ) -> TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>> {
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (from_keypair, to_pubkeys, lamports, compute_unit_price, max_schedule_slot) in
            tx_infos.into_iter()
        {
            let transaction = prioritized_tranfers(
                from_keypair.borrow(),
                to_pubkeys,
                lamports,
                compute_unit_price,
            );
            const TEST_TRANSACTION_COST: u64 = 5000;
            let mut txns_max_age: SmallVec<
                [(RuntimeTransaction<SanitizedTransaction>, MaxAge); MAX_PACKETS_PER_BUNDLE],
            > = SmallVec::new();
            txns_max_age.push((transaction, MaxAge::MAX));
            container.insert_new_batch(
                txns_max_age,
                compute_unit_price,
                TEST_TRANSACTION_COST,
                false,
                max_schedule_slot,
            );
        }

        container
    }

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    #[test]
    fn test_scheduler_empty() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers: _,
            finished_consume_work_sender: _,
            response_receiver: _,
        } = create_test_scheduler(4, &bank_forks);

        let mut container = TransactionStateContainer::with_capacity(100);
        let result = scheduler
            .schedule(
                &mut container,
                0,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 0);
    }

    #[test]
    fn test_scheduler_basic() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
            response_receiver,
        } = create_test_scheduler(4, &bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();

        let first_recipient = Pubkey::new_unique();
        let second_recipient = Pubkey::new_unique();

        let mut container = create_container(vec![
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1000,
                seq_id_to_priority(1),
                u64::MAX,
            ),
            (
                &keypair_a,
                vec![first_recipient],
                1500,
                seq_id_to_priority(0),
                u64::MAX,
            ),
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1500,
                seq_id_to_priority(2),
                u64::MAX,
            ),
            (
                &Keypair::new(),
                vec![second_recipient],
                2000,
                seq_id_to_priority(3),
                u64::MAX,
            ),
        ]);

        assert!(
            scheduler.slot.is_none(),
            "Scheduler slot should be None initially"
        );

        let decision = BufferedPacketsDecision::Consume(bank_forks.read().unwrap().working_bank());

        // Init scheduler with bank start info
        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        assert!(
            scheduler.slot.is_some(),
            "Scheduler slot should be set after receiving bank start"
        );

        // Schedule the transactions
        let result = scheduler
            .schedule(
                &mut container,
                0,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();

        // Only two should have been scheduled as one is blocked
        assert_eq!(result.num_scheduled, 2);

        // Receive the scheduled work
        let work_1 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_1.ids.len(), 1);
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Check that the first transaction is from keypair_a and first recipient is the first recipient
        assert_eq!(
            work_1.transactions[0].message().account_keys()[0],
            keypair_a.pubkey()
        );
        assert_eq!(
            work_1.transactions[0].message().account_keys()[1],
            first_recipient
        );

        // Check that the second transaction is from the other keypair
        assert_ne!(
            work_2.transactions[0].message().account_keys()[0],
            keypair_a.pubkey(),
        );
        assert_eq!(
            work_2.transactions[0].message().account_keys()[1],
            second_recipient
        );

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(
                &mut container,
                0,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 0);

        // Respond with finished work
        let responses = [
            (
                work_1,
                TransactionResult::Committed(TransactionCommittedResult {
                    cus_consumed: 100,
                    feepayer_balance_lamports: 1000,
                    loaded_accounts_data_size: 10,
                    execution_success: true,
                }),
            ), // Committed
            (
                work_2,
                TransactionResult::NotCommitted(NotCommittedReason::PohTimeout),
            ), // Not committed
        ];
        for (work, response) in responses.into_iter() {
            let finished_work = FinishedConsumeWork {
                work,
                retryable_indexes: vec![],
                extra_info: Some(
                    crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
                        processed_results: vec![response],
                    },
                ),
            };
            let _ = finished_consume_work_sender.send(finished_work);
        }

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 2);

        // Check the responses
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 0);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(committed) => {
                assert_eq!(committed.transaction_results.len(), 1);
                assert_eq!(committed.transaction_results[0].cus_consumed, 100);
            }
            NotCommitted(not_committed) => {
                panic!("Expected Committed result, got NotCommitted: {not_committed:?}");
            }
        }

        // Check the response for the second transaction (not committed)
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 3);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(_) => {
                panic!("Expected NotCommitted result, got Committed");
            }
            NotCommitted(not_committed) => {
                assert!(
                    not_committed.reason.is_some(),
                    "NotCommitted reason should be present"
                );
                let reason = not_committed.reason.unwrap();
                assert_eq!(
                    reason,
                    jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                        jito_protos::proto::bam_types::SchedulingError::PohTimeout as i32
                    )
                );
            }
        }

        // Now try scheduling again; should schedule the remaining transaction
        let result = scheduler
            .schedule(
                &mut container,
                0,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler
            .schedule(
                &mut container,
                0,
                |_, _| {},
                |_| PreLockFilterAction::AttemptToSchedule,
            )
            .unwrap();
        assert_eq!(result.num_scheduled, 0);

        // Send back the finished work for the second transaction
        let finished_work = FinishedConsumeWork {
            work: work_2,
            retryable_indexes: vec![],
            extra_info: Some(
                crate::banking_stage::scheduler_messages::FinishedConsumeWorkExtraInfo {
                    processed_results: vec![TransactionResult::Committed(
                        TransactionCommittedResult {
                            cus_consumed: 1500,
                            feepayer_balance_lamports: 1500,
                            loaded_accounts_data_size: 20,
                            execution_success: true,
                        },
                    )],
                },
            ),
        };
        let _ = finished_consume_work_sender.send(finished_work);

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 1);

        // Check the response for the next transaction
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 1);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(committed) => {
                assert_eq!(committed.transaction_results.len(), 1);
                assert_eq!(committed.transaction_results[0].cus_consumed, 1500);
            }
            NotCommitted(not_committed) => {
                panic!("Expected Committed result, got NotCommitted: {not_committed:?}");
            }
        }

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
            .unwrap();
        assert_eq!(num_transactions, 0);

        // Check that container + prio-graph are empty
        assert!(
            container.pop().is_none(),
            "Container should be empty after processing all transactions"
        );
        assert!(
            scheduler.prio_graph.is_empty(),
            "Prio-graph should be empty after processing all transactions"
        );

        // Receive the NotCommitted Result
        let response = response_receiver.try_recv().unwrap();
        let BamOutboundMessage::AtomicTxnBatchResult(bundle_result) = response else {
            panic!("Expected AtomicTxnBatchResult message");
        };
        assert_eq!(bundle_result.seq_id, 2);
        assert!(
            bundle_result.result.is_some(),
            "Bundle result should be present"
        );
        let result = bundle_result.result.unwrap();
        match result {
            Committed(_) => {
                panic!("Expected NotCommitted result, got Committed");
            }
            NotCommitted(not_committed) => {
                assert!(
                    not_committed.reason.is_some(),
                    "NotCommitted reason should be present"
                );
                let reason = not_committed.reason.unwrap();
                assert_eq!(
                    reason,
                    jito_protos::proto::bam_types::not_committed::Reason::SchedulingError(
                        jito_protos::proto::bam_types::SchedulingError::OutsideLeaderSlot as i32
                    )
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "node must exist")]
    fn test_prio_graph_clears_on_slot_boundary() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers: _,
            finished_consume_work_sender: _,
            response_receiver: _,
        } = create_test_scheduler(4, &bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();
        let keypair_b = Keypair::new();

        let bank = bank_forks.read().unwrap().working_bank();

        // Set initial slot with bank start
        let mut container = create_container(vec![(
            &keypair_a,
            vec![Pubkey::new_unique()],
            1000,
            seq_id_to_priority(0),
            u64::MAX,
        )]);
        let decision = BufferedPacketsDecision::Consume(bank.clone());

        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(scheduler.slot, Some(bank.slot()));

        // Pull transactions into prio_graph
        // Create container with some transactions
        let mut container = create_container(vec![
            (
                &keypair_a,
                vec![Pubkey::new_unique()],
                1000,
                seq_id_to_priority(0),
                u64::MAX,
            ),
            (
                &keypair_b,
                vec![Pubkey::new_unique()],
                2000,
                seq_id_to_priority(1),
                u64::MAX,
            ),
        ]);
        scheduler.pull_into_prio_graph(&mut container);
        assert!(
            !scheduler.prio_graph.is_empty(),
            "Prio graph should have transactions"
        );

        // Store transaction IDs that are currently in the prio_graph
        let mut stored_txn_ids = Vec::new();
        while let Some(txn_id) = scheduler.prio_graph.pop() {
            stored_txn_ids.push(txn_id);
            // Unblock to allow the next transaction to be popped
            scheduler.prio_graph.unblock(&txn_id);
        }

        // Re-insert the transactions back into prio_graph for testing
        for txn_id in &stored_txn_ids {
            // Get transaction from container to re-insert
            if let Some((batch_ids, _, _)) = container.get_batch(txn_id.id) {
                let txns = batch_ids
                    .iter()
                    .filter_map(|id| container.get_transaction(*id));
                scheduler.prio_graph.insert_transaction(
                    *txn_id,
                    BamScheduler::<RuntimeTransaction<SanitizedTransaction>>::get_transactions_account_access(txns.into_iter()),
                );
            }
        }

        // Simulate slot boundary change by changing to no bank (None)
        let decision_no_bank = BufferedPacketsDecision::Forward;
        scheduler
            .receive_completed(&mut container, &decision_no_bank)
            .unwrap();

        assert_eq!(scheduler.slot, None);

        // This should panic because the prio_graph has been cleared
        // and the transaction ID no longer exists in the graph
        if let Some(first_id) = stored_txn_ids.first() {
            scheduler.prio_graph.unblock(first_id);
        }
    }
}
