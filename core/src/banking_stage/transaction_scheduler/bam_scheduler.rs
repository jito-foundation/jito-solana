/// A Scheduler implementation that pulls batches off the container, and then
/// schedules them to workers in a FIFO, account-aware manner. This is facilitated by the
/// `PrioGraph` data structure, which is a directed graph that tracks the dependencies.
///
use {
    super::{
        scheduler::{Scheduler, SchedulingSummary},
        scheduler_error::SchedulerError,
        transaction_priority_id::TransactionPriorityId,
        transaction_state_container::StateContainer,
    },
    crate::{
        bam_dependencies::BamOutboundMessage,
        banking_stage::{
            consume_worker::active_leader_state,
            decision_maker::BufferedPacketsDecision,
            qos_service::QosService,
            scheduler_messages::{
                ConsumeWork, FinishedConsumeWork, NotCommittedReason, TransactionBatchId,
                TransactionResult,
            },
            transaction_scheduler::{
                bam_utils::convert_txn_error_to_proto, scheduler_common::SchedulingCommon,
            },
        },
    },
    crossbeam_channel::{Receiver, Sender},
    histogram::Histogram,
    jito_protos::proto::bam_types::{
        AtomicTxnBatchResult, Committed, NotCommitted, SchedulingError, atomic_txn_batch_result,
        not_committed::Reason,
    },
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    smallvec::SmallVec,
    solana_clock::{BankId, MAX_PROCESSING_AGE, Slot},
    solana_measure::measure_us,
    solana_nohash_hasher::IntMap,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, bank_forks::BankForks},
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_transaction_error::TransactionError,
    std::{
        borrow::Borrow,
        sync::{Arc, RwLock},
        time::Instant,
    },
    tokio::sync::mpsc::Sender as TokioSender,
};

type SchedulerPrioGraph = PrioGraph<
    TransactionPriorityId,
    Pubkey,
    TransactionPriorityId,
    fn(&TransactionPriorityId, &GraphNode<TransactionPriorityId>) -> TransactionPriorityId,
>;

pub const MAX_PACKETS_PER_BUNDLE: usize = 5; // copied from BundleStorage::MAX_PACKETS_PER_BUNDLE

// Sized from 30 days of mainnet data ending 2026-08-18. Atomic-only ClickHouse counts of distinct
// `(source, seq_id)` batches per validator-slot had p99=149, p99.9=236, 99.94% <=256, and max=540.
// The full-slot count conservatively upper-bounds the pre-ParentReady subset stored here.
// Mainnet-validator Influx `bam_connection-metrics.bundle_received` active 25 ms samples, which
// also include non-atomic batches, had p99=183, p99.9=272, 99.86% <=256, and max=881. Thus 256
// keeps the atomic p99.9 inline in 4 KiB while rarer bursts spill safely.
const DEFERRED_ATOMIC_BATCHES_INLINE_CAPACITY: usize = 256;

pub struct BamScheduler<Tx: TransactionWithMeta> {
    consume_work_sender: Sender<ConsumeWork<Tx>>,
    finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
    response_sender: TokioSender<BamOutboundMessage>,

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
    deferred_atomic_batches:
        SmallVec<[TransactionPriorityId; DEFERRED_ATOMIC_BATCHES_INLINE_CAPACITY]>,

    extra_checks_enabled: bool,
    bank_forks: Arc<RwLock<BankForks>>,
    shared_leader_state: SharedLeaderState,

    /// Bank whose cost tracker holds the current reservations.
    admission_bank: Option<(BankId, Slot)>,
    /// Estimated cost reserved on `admission_bank` by dispatched work that has not settled.
    inflight_reserved_cost: u64,
    /// Deferred head-of-line batch and the inflight estimate at its last attempt.
    pending_admission: Option<(TransactionPriorityId, u64)>,
    admission_us: Histogram,
}

// Each work item contains one BAM batch, which may contain multiple transactions.
struct InflightBatchInfo {
    schedule_time: Instant,
    priority_id: TransactionPriorityId,
    seq_id: u32,
    /// Estimate the scheduler reserved for this work, held until completion.
    reserved_cost: u64,
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_sender: Sender<ConsumeWork<Tx>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: TokioSender<BamOutboundMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
        shared_leader_state: SharedLeaderState,
    ) -> Self {
        Self {
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            next_batch_id: 0,
            inflight_batch_info: IntMap::default(),
            prio_graph: PrioGraph::new(|id, _| *id),
            insertion_to_prio_graph_time: IntMap::default(),
            time_in_priograph_us: Histogram::new(),
            time_in_worker_us: Histogram::new(),
            time_between_schedule_us: Histogram::new(),
            last_schedule_time: Instant::now(),
            slot: None,
            reusable_consume_work: Vec::new(),
            deferred_atomic_batches: SmallVec::new(),
            extra_checks_enabled: true,
            bank_forks,
            shared_leader_state,
            admission_bank: None,
            inflight_reserved_cost: 0,
            pending_admission: None,
            admission_us: Histogram::new(),
        }
    }

    #[inline]
    fn check_transactions(
        &self,
        bank: &Bank,
        txns: &[impl Borrow<Tx>],
    ) -> Option<(usize, TransactionError)> {
        if !self.extra_checks_enabled {
            return None;
        }
        let lock_results = SmallVec::<[_; MAX_PACKETS_PER_BUNDLE]>::from_elem(Ok(()), txns.len());
        bank.check_transactions::<Tx>(
            txns,
            &lock_results,
            MAX_PROCESSING_AGE,
            true,
            &mut TransactionErrorMetrics::default(),
        )
        .into_iter()
        .enumerate()
        .find_map(|(i, res)| res.err().map(|err| (i, err)))
    }

    /// Insert all incoming transactions into the `PrioGraph`.
    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        let Some(slot) = self.slot else {
            return;
        };

        let working_bank = self.bank_forks.read().unwrap().working_bank();
        let atomic_batches_enabled = self.shared_leader_state.load().atomic_batches_enabled();

        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, revert_on_error, max_schedule_slot, seq_id)) =
                container.get_batch(next_batch_id.id)
            else {
                error!("Batch {} not found in container", next_batch_id.id);
                container.remove_by_id(next_batch_id.id);
                continue;
            };

            if revert_on_error && !atomic_batches_enabled {
                self.deferred_atomic_batches.push(next_batch_id);
                continue;
            }

            if max_schedule_slot < slot {
                // If the slot has changed, we cannot schedule this batch
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(next_batch_id.id);
                continue;
            }

            let txns = batch_ids
                .iter()
                .filter_map(|txn_id| container.get_transaction(*txn_id))
                .collect::<SmallVec<[&Tx; MAX_PACKETS_PER_BUNDLE]>>();

            if let Some((index, err)) = self.check_transactions(&working_bank, &txns) {
                drop(txns);
                container.remove_by_id(next_batch_id.id);

                self.send_back_result(
                    seq_id,
                    Self::not_committed_result(index, NotCommittedReason::Error(err)),
                );
                continue;
            }

            self.insertion_to_prio_graph_time
                .insert(seq_id, Instant::now());
            self.prio_graph.insert_transaction(
                next_batch_id,
                txns.into_iter().flat_map(|txn| {
                    txn.account_keys().iter().enumerate().map(|(index, key)| {
                        if txn.is_writable(index) {
                            (*key, AccessKind::Write)
                        } else {
                            (*key, AccessKind::Read)
                        }
                    })
                }),
            );
        }

        // Atomic batches must wait until ParentReady confirms or replaces the provisional bank.
        // Non-atomic batches can still be rescheduled after sad handover.
        container.push_ids_into_queue(self.deferred_atomic_batches.drain(..));
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
    ) -> Result<usize, SchedulerError> {
        let Some(slot) = self.slot else {
            return Ok(0);
        };

        let Some(leader_state) = active_leader_state(&self.shared_leader_state) else {
            return Ok(0);
        };
        let Some(admission_bank) = leader_state
            .working_bank()
            .filter(|bank| bank.slot() == slot)
        else {
            return Ok(0);
        };

        if self.admission_bank != Some((admission_bank.bank_id(), slot)) {
            // Work admitted before a replacement may admit locally there in worker order.
            // Keep later work behind it.
            if !self.inflight_batch_info.is_empty() {
                return Ok(0);
            }
            self.admission_bank = Some((admission_bank.bank_id(), slot));
        }

        let now = Instant::now();
        let mut num_scheduled = 0;
        loop {
            // A deferred batch holds the head of the line until work on its bank settles or the
            // bank itself changes; either way it gets the next attempt before anything else.
            let id = if let Some((id, attempted_inflight_cost)) = self.pending_admission {
                if attempted_inflight_cost == self.inflight_reserved_cost {
                    return Ok(num_scheduled);
                }
                self.pending_admission = None;
                id
            } else {
                let Some(id) = self.prio_graph.pop() else {
                    return Ok(num_scheduled);
                };
                id
            };

            let (batch_ids, revert_on_error, max_schedule_slot, seq_id) =
                container.get_batch(id.id).unwrap();

            // Update time in prio-graph metric
            if let Some(insertion_time) = self.insertion_to_prio_graph_time.remove(&seq_id) {
                let _ = self
                    .time_in_priograph_us
                    .increment(now.duration_since(insertion_time).as_micros() as u64);
            };

            // Filter on slot
            if max_schedule_slot < slot {
                self.prio_graph.unblock(&id);
                self.send_no_leader_slot_bundle_result(seq_id);
                container.remove_by_id(id.id);
                continue;
            }

            let mut work = self
                .reusable_consume_work
                .pop()
                .unwrap_or_else(|| ConsumeWork {
                    batch_id: TransactionBatchId::new(0),
                    ids: Vec::with_capacity(1),
                    transactions: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                    max_ages: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                    revert_on_error: false,
                    respond_with_extra_info: true,
                    target_slot: 0,
                    max_schedule_slot: None,
                    admission: None,
                });
            work.ids.extend(batch_ids);
            for txn_id in &work.ids {
                let (transaction, max_age) = container
                    .get_mut_transaction_state(*txn_id)
                    .unwrap()
                    .take_transaction_for_scheduling();
                work.transactions.push(transaction);
                work.max_ages.push(max_age);
            }

            // Validate the work directly, before reserving its cost.
            if let Some((index, err)) = self.check_transactions(admission_bank, &work.transactions)
            {
                self.recycle_work_object(work);
                container.remove_by_id(id.id);
                self.prio_graph.unblock(&id);

                self.send_back_result(
                    seq_id,
                    Self::not_committed_result(index, NotCommittedReason::Error(err)),
                );
                continue;
            }

            // Admit cost here, in pop order, so eight workers racing for the cost tracker cannot
            // reorder it.
            let (attempt, admission_us) = measure_us!(QosService::try_admit_transactions(
                admission_bank,
                &work.transactions,
                work.transactions
                    .iter()
                    .zip(&work.max_ages)
                    .map(|(tx, max_age)| {
                        admission_bank.resanitize_transaction_minimally(
                            tx,
                            max_age.sanitized_epoch,
                            max_age.alt_invalidation_slot,
                        )
                    }),
                self.inflight_reserved_cost,
            ));
            let _ = self.admission_us.increment(admission_us);
            let Some((results, reserved_cost)) = attempt else {
                debug!(
                    "deferring batch {seq_id}: {} in flight",
                    self.inflight_reserved_cost
                );
                // The retry takes the transactions out again; hand them back until then.
                for (txn_id, transaction) in work.ids.iter().zip(work.transactions.drain(..)) {
                    container
                        .get_mut_transaction_state(*txn_id)
                        .unwrap()
                        .retry_transaction(transaction);
                }
                self.recycle_work_object(work);
                self.pending_admission = Some((id, self.inflight_reserved_cost));
                return Ok(num_scheduled);
            };
            let batch_id = TransactionBatchId::new(self.next_batch_id);
            self.next_batch_id += 1;
            work.batch_id = batch_id;
            work.revert_on_error = revert_on_error;
            work.target_slot = slot;
            work.max_schedule_slot = Some(slot);
            work.admission = Some((Arc::clone(admission_bank), results));
            num_scheduled += work.ids.len();
            if let Err(err) = self.consume_work_sender.send(work) {
                self.recycle_work_object(err.0);
                return Err(SchedulerError::DisconnectedSendChannel(
                    "BAM worker disconnected",
                ));
            }
            self.inflight_reserved_cost += reserved_cost;
            self.inflight_batch_info.insert(
                batch_id,
                InflightBatchInfo {
                    schedule_time: Instant::now(),
                    priority_id: id,
                    seq_id,
                    reserved_cost,
                },
            );
        }
    }

    fn recycle_work_object(&mut self, mut work: ConsumeWork<Tx>) {
        if let Some((bank, results)) = work.admission.take() {
            let costs = QosService::compute_transaction_costs(
                &bank.feature_set,
                work.transactions.iter(),
                results.into_iter(),
            );
            QosService::remove_or_update_costs(costs.iter(), None, &bank);
        }
        work.ids.clear();
        work.transactions.clear();
        work.max_ages.clear();
        self.reusable_consume_work.push(work);
    }

    fn send_no_leader_slot_bundle_result(&self, seq_id: u32) {
        self.send_back_result(
            seq_id,
            atomic_txn_batch_result::Result::NotCommitted(NotCommitted {
                reason: Some(Reason::SchedulingError(
                    SchedulingError::OutsideLeaderSlot as i32,
                )),
            }),
        );
    }

    fn send_back_result(&self, seq_id: u32, result: atomic_txn_batch_result::Result) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                AtomicTxnBatchResult {
                    seq_id,
                    result: Some(result),
                },
            ));
    }

    /// Generates a `bundle_result::Result` based on the processed results for 'revert_on_error' batches.
    fn generate_revert_on_error_bundle_result(
        processed_results: impl IntoIterator<Item = TransactionResult>,
    ) -> atomic_txn_batch_result::Result {
        let mut saw_commit_cancelled = false;
        let processed_results = processed_results.into_iter();
        let mut transaction_results = Vec::with_capacity(processed_results.size_hint().0);
        for (i, result) in processed_results.enumerate() {
            match result {
                TransactionResult::Committed(processed) => transaction_results.push(processed),
                // TransactionError::CommitCancelled indicates another transaction in this bundle errored out.
                TransactionResult::NotCommitted(NotCommittedReason::Error(
                    TransactionError::CommitCancelled,
                )) => {
                    saw_commit_cancelled = true;
                }
                TransactionResult::NotCommitted(reason) => {
                    return Self::not_committed_result(i, reason);
                }
            }
        }

        if saw_commit_cancelled {
            return Self::not_committed_result(0, NotCommittedReason::PohTimeout);
        }

        atomic_txn_batch_result::Result::Committed(Committed {
            transaction_results,
        })
    }

    fn not_committed_result(
        index: usize,
        reason: NotCommittedReason,
    ) -> atomic_txn_batch_result::Result {
        let reason = match reason {
            NotCommittedReason::PohTimeout => {
                Reason::SchedulingError(SchedulingError::PohTimeout as i32)
            }
            NotCommittedReason::Error(err) => {
                Reason::TransactionError(jito_protos::proto::bam_types::TransactionError {
                    index: index as u32,
                    reason: convert_txn_error_to_proto(err) as i32,
                })
            }
        };
        atomic_txn_batch_result::Result::NotCommitted(NotCommitted {
            reason: Some(reason),
        })
    }

    fn maybe_bank_boundary_actions(
        &mut self,
        decision: &BufferedPacketsDecision,
        container: &mut impl StateContainer<Tx>,
    ) {
        // Check if no bank or slot has changed
        let bank_slot = decision.bank().map(|bank| bank.slot());
        if bank_slot == self.slot {
            return;
        }
        // A bankless boundary already unblocked and cleared the old graph. Keep its slot
        // inactive until old completions drain so a same-slot replacement cannot unblock twice.
        if self.slot.is_none() && !self.inflight_batch_info.is_empty() {
            return;
        }
        let prev_slot = self.slot;
        match bank_slot {
            Some(bank_slot) => {
                debug!("Bank boundary detected: slot changed from {prev_slot:?} to {bank_slot}")
            }
            None => debug!("Bank boundary detected: slot changed to None"),
        }
        self.slot = bank_slot;

        // Drain container and send back 'retryable'
        if self.slot.is_none() {
            while let Some(next_batch_id) = container.pop() {
                if let Some((_, _, _, seq_id)) = container.get_batch(next_batch_id.id) {
                    self.send_no_leader_slot_bundle_result(seq_id);
                }
                container.remove_by_id(next_batch_id.id);
            }
        }

        // A batch deferred on cost admission was popped from the prio-graph but never
        // dispatched. It gets the same result as everything still queued, and it must be
        // unblocked so the drain below reaches its dependents.
        if let Some((pending_id, _)) = self.pending_admission.take() {
            self.prio_graph.unblock(&pending_id);
            if let Some((_, _, _, seq_id)) = container.get_batch(pending_id.id) {
                self.send_no_leader_slot_bundle_result(seq_id);
            }
            container.remove_by_id(pending_id.id);
        }
        // Unblock all transactions blocked by inflight batches
        // and then drain the prio-graph
        if prev_slot == self.admission_bank.map(|(_, slot)| slot) {
            for inflight_info in self.inflight_batch_info.values() {
                self.prio_graph.unblock(&inflight_info.priority_id);
            }
        }
        let now = Instant::now();
        while let Some((next_batch_id, _)) = self.prio_graph.pop_and_unblock() {
            let Some((_, _, _, seq_id)) = container.get_batch(next_batch_id.id) else {
                container.remove_by_id(next_batch_id.id);
                continue;
            };
            if let Some(insertion_time) = self.insertion_to_prio_graph_time.remove(&seq_id) {
                let _ = self
                    .time_in_priograph_us
                    .increment(now.duration_since(insertion_time).as_micros() as u64);
            };

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
        let percentile = |p| self.time_in_priograph_us.percentile(p).unwrap_or_default();
        datapoint_info!(
            "bam_scheduler_bank_boundary-metrics",
            ("time_in_priograph_us_p50", percentile(50.0), i64),
            ("time_in_priograph_us_p75", percentile(75.0), i64),
            ("time_in_priograph_us_p90", percentile(90.0), i64),
            ("time_in_priograph_us_p99", percentile(99.0), i64),
            (
                "time_in_priograph_us_max",
                self.time_in_priograph_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_in_priograph_us.clear();

        let percentile = |p| self.time_in_worker_us.percentile(p).unwrap_or_default();
        datapoint_info!(
            "bam_scheduler_worker_time_metrics",
            ("time_in_worker_us_p50", percentile(50.0), i64),
            ("time_in_worker_us_p75", percentile(75.0), i64),
            ("time_in_worker_us_p90", percentile(90.0), i64),
            ("time_in_worker_us_p99", percentile(99.0), i64),
            (
                "time_in_worker_us_max",
                self.time_in_worker_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_in_worker_us.clear();

        let percentile = |p| {
            self.time_between_schedule_us
                .percentile(p)
                .unwrap_or_default()
        };
        datapoint_info!(
            "bam_scheduler_time_between_schedules_metrics",
            ("time_between_schedule_us_p50", percentile(50.0), i64),
            ("time_between_schedule_us_p75", percentile(75.0), i64),
            ("time_between_schedule_us_p90", percentile(90.0), i64),
            ("time_between_schedule_us_p99", percentile(99.0), i64),
            (
                "time_between_schedule_us_max",
                self.time_between_schedule_us.maximum().unwrap_or_default(),
                i64
            ),
            (
                "admission_us_p99",
                self.admission_us.percentile(99.0).unwrap_or_default(),
                i64
            ),
            (
                "admission_us_max",
                self.admission_us.maximum().unwrap_or_default(),
                i64
            ),
        );
        self.time_between_schedule_us.clear();
        self.admission_us.clear();
    }
}

impl<Tx: TransactionWithMeta> Scheduler<Tx> for BamScheduler<Tx> {
    fn has_in_flight_transactions(&self) -> bool {
        !self.inflight_batch_info.is_empty()
    }

    fn schedule<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        _slot: Slot,
        _budget: u64,
    ) -> Result<SchedulingSummary, SchedulerError> {
        let starting_queue_size = container.queue_size();
        let starting_buffer_size = container.buffer_size();

        let start_time = Instant::now();
        let time_since_last_schedule = start_time.duration_since(self.last_schedule_time);
        self.last_schedule_time = start_time;
        let _ = self
            .time_between_schedule_us
            .increment(time_since_last_schedule.as_micros() as u64);

        self.pull_into_prio_graph(container);
        let num_scheduled = self.send_to_workers(container)?;

        // TODO(seg): Double check the zeros here
        Ok(SchedulingSummary {
            starting_queue_size,
            starting_buffer_size,
            num_scheduled,
            num_unschedulable_conflicts: 0,
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
            let FinishedConsumeWork {
                work, extra_info, ..
            } = result;
            num_transactions += work.ids.len();
            let batch_id = work.batch_id;
            let revert_on_error = work.revert_on_error;
            self.recycle_work_object(work);

            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                continue;
            };

            // Settled work may have freed budget for a batch deferred on this bank's block limit.
            // Dispatch is held across a bank change until the old work drains, so everything in
            // flight was admitted on `admission_bank`.
            self.inflight_reserved_cost = self
                .inflight_reserved_cost
                .saturating_sub(inflight_batch_info.reserved_cost);

            let _ = self.time_in_worker_us.increment(
                now.duration_since(inflight_batch_info.schedule_time)
                    .as_micros() as u64,
            );
            if let Some(extra_info) = extra_info {
                let mut processed_results = extra_info.into_iter();
                let bundle_result = if revert_on_error {
                    Self::generate_revert_on_error_bundle_result(processed_results)
                } else {
                    match processed_results.next() {
                        Some(TransactionResult::Committed(result)) => {
                            atomic_txn_batch_result::Result::Committed(Committed {
                                transaction_results: vec![result],
                            })
                        }
                        Some(TransactionResult::NotCommitted(reason)) => {
                            Self::not_committed_result(0, reason)
                        }
                        None => {
                            warn!("Processed results for batch {batch_id} are missing for index 0");
                            continue;
                        }
                    }
                };
                self.send_back_result(inflight_batch_info.seq_id, bundle_result);
            }

            // The admission bank stays unchanged until every inflight batch returns.
            if self.admission_bank.map(|(_, slot)| slot) == self.slot {
                self.prio_graph.unblock(&inflight_batch_info.priority_id);
            }
            container.remove_by_id(inflight_batch_info.priority_id.id);
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
                consumer::RetryableIndex,
                decision_maker::BufferedPacketsDecision,
                qos_service::QosService,
                scheduler_messages::{
                    ConsumeWork, FinishedConsumeWork, MaxAge, NotCommittedReason, TransactionResult,
                },
                tests::create_slow_genesis_config,
                transaction_scheduler::{
                    bam_scheduler::BamScheduler,
                    scheduler::Scheduler,
                    transaction_state_container::{StateContainer, TransactionStateContainer},
                },
            },
        },
        crossbeam_channel::unbounded,
        itertools::Itertools,
        jito_protos::proto::bam_types::{
            SchedulingError, TransactionCommittedResult,
            atomic_txn_batch_result::{
                self,
                Result::{Committed, NotCommitted},
            },
            not_committed::Reason,
        },
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_cost_model::cost_tracker::{CostTrackerError, CostTrackerLimits},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_message::Message,
        solana_poh::poh_recorder::{LeaderState, SharedLeaderState},
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_system_interface::instruction::{transfer, transfer_many},
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        solana_transaction_error::TransactionError,
        std::sync::{Arc, RwLock},
    };

    type Tx = RuntimeTransaction<SanitizedTransaction>;

    struct TestScheduler {
        scheduler: BamScheduler<Tx>,
        consume_work_receiver: crossbeam_channel::Receiver<ConsumeWork<Tx>>,
        finished_consume_work_sender: crossbeam_channel::Sender<FinishedConsumeWork<Tx>>,
        response_receiver: tokio::sync::mpsc::Receiver<BamOutboundMessage>,
    }

    fn create_test_scheduler(bank_forks: &Arc<RwLock<BankForks>>) -> TestScheduler {
        let (consume_work_sender, consume_work_receiver) = unbounded();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(100);
        let scheduler = BamScheduler::new(
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            bank_forks.clone(),
            SharedLeaderState::new(0, None, None),
        );
        TestScheduler {
            scheduler,
            consume_work_receiver,
            finished_consume_work_sender,
            response_receiver,
        }
    }

    /// Makes `bank` the bank workers would execute on, as `PohRecorder` does for the leader.
    fn set_leader_bank(shared_leader_state: &mut SharedLeaderState, bank: &Arc<Bank>) {
        shared_leader_state.store(Arc::new(LeaderState::new(
            Some(bank.clone()),
            bank.tick_height(),
            None,
            None,
        )));
    }

    fn set_block_cost_limit(bank: &Bank, block_cost: u64) {
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(CostTrackerLimits::new(u64::MAX, block_cost, u64::MAX));
    }

    /// Estimated cost of one `prioritized_tranfers` transaction; every test transaction has the
    /// same shape, so this is the reservation each batch makes.
    fn estimated_cost(bank: &Bank) -> u64 {
        let tx = prioritized_tranfers(&Keypair::new(), vec![Pubkey::new_unique()], 1000, 0);
        QosService::compute_transaction_costs(
            &bank.feature_set,
            std::iter::once(&tx),
            std::iter::once(Ok(())),
        )[0]
        .as_ref()
        .unwrap()
        .sum()
    }

    fn block_cost_and_in_flight(bank: &Bank) -> (u64, usize) {
        let tracker = bank.read_cost_tracker().unwrap();
        (tracker.block_cost(), tracker.in_flight_transaction_count())
    }

    /// What a worker does after committing `work` on `bank` with `actual_units` consumed:
    /// settle the reservation the scheduler made and take the admission out of the work.
    fn settle_committed(bank: &Bank, work: &mut ConsumeWork<Tx>, actual_units: u64) {
        let costs = QosService::compute_transaction_costs(
            &bank.feature_set,
            work.transactions.iter(),
            std::iter::repeat(Ok(())),
        );
        let mut tracker = bank.write_cost_tracker().unwrap();
        for cost in costs.iter().flatten() {
            tracker.update_execution_cost(cost, actual_units, 0);
        }
        tracker.sub_transactions_in_flight(costs.len());
        drop(tracker);
        work.admission = None;
    }

    fn finish_committed(
        test: &mut TestScheduler,
        container: &mut TransactionStateContainer<Tx>,
        decision: &BufferedPacketsDecision,
        work: ConsumeWork<Tx>,
        cus_consumed: u32,
    ) {
        let processed_results = vec![
            TransactionResult::Committed(TransactionCommittedResult {
                cus_consumed,
                feepayer_balance_lamports: 0,
                loaded_accounts_data_size: 0,
                execution_success: true,
            });
            work.transactions.len()
        ];
        test.finished_consume_work_sender
            .send(FinishedConsumeWork {
                work,
                retryable_indexes: vec![],
                extra_info: Some(processed_results),
            })
            .unwrap();
        test.scheduler
            .receive_completed(container, decision)
            .unwrap();
    }

    fn next_result(
        response_receiver: &mut tokio::sync::mpsc::Receiver<BamOutboundMessage>,
    ) -> (u32, atomic_txn_batch_result::Result) {
        let BamOutboundMessage::AtomicTxnBatchResult(result) = response_receiver
            .try_recv()
            .expect("a result should be queued")
        else {
            panic!("expected AtomicTxnBatchResult message");
        };
        (result.seq_id, result.result.expect("result should be set"))
    }

    fn prioritized_tranfers(
        from_keypair: &Keypair,
        to_pubkeys: impl IntoIterator<Item = Pubkey>,
        lamports: u64,
        priority: u64,
    ) -> Tx {
        let to_pubkeys_lamports = to_pubkeys
            .into_iter()
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
        tx_infos: Vec<(&Keypair, Vec<Pubkey>, u64, u32, u64)>,
    ) -> TransactionStateContainer<Tx> {
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (fifo_index, (from_keypair, to_pubkeys, lamports, seq_id, max_schedule_slot)) in
            tx_infos.into_iter().enumerate()
        {
            let transaction =
                prioritized_tranfers(from_keypair, to_pubkeys, lamports, u64::from(seq_id));
            container.insert_new_batch(
                std::iter::once((transaction, MaxAge::MAX)).collect(),
                u64::MAX.saturating_sub(fifo_index as u64),
                false,
                max_schedule_slot,
                seq_id,
            );
        }

        container
    }

    fn test_bank_forks() -> Arc<RwLock<BankForks>> {
        let genesis_config = create_slow_genesis_config(u64::MAX).genesis_config;
        Bank::new_with_bank_forks_for_tests(&genesis_config).1
    }

    fn admission_scheduler() -> (TestScheduler, Arc<Bank>) {
        let bank_forks = test_bank_forks();
        let mut test = create_test_scheduler(&bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let bank = Arc::new(Bank::new_from_parent(
            bank_forks.read().unwrap().working_bank(),
            SlotLeader::new_unique(),
            1,
        ));
        set_leader_bank(&mut test.scheduler.shared_leader_state, &bank);
        (test, bank)
    }

    #[test]
    fn test_scheduler_empty() {
        let bank_forks = test_bank_forks();
        let TestScheduler { mut scheduler, .. } = create_test_scheduler(&bank_forks);

        let mut container = TransactionStateContainer::with_capacity(100);
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(result.num_scheduled, 0);
    }

    #[test]
    fn test_scheduler_basic() {
        let bank_forks = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receiver,
            finished_consume_work_sender,
            mut response_receiver,
        } = create_test_scheduler(&bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();

        let first_fifo_recipient = Pubkey::new_unique();
        let blocked_recipient = Pubkey::new_unique();
        let second_recipient = Pubkey::new_unique();

        // First two batches conflict on fee payer and span the seq_id wrap boundary.
        // FIFO should schedule u32::MAX before 0.
        let mut container = create_container(vec![
            (
                &keypair_a,
                vec![first_fifo_recipient],
                1000,
                u32::MAX,
                u64::MAX,
            ),
            (&keypair_a, vec![blocked_recipient], 1500, 0, u64::MAX),
            (&keypair_a, vec![Pubkey::new_unique()], 1500, 2, u64::MAX),
            (&Keypair::new(), vec![second_recipient], 2000, 3, u64::MAX),
        ]);

        assert!(
            scheduler.slot.is_none(),
            "Scheduler slot should be None initially"
        );

        let bank = bank_forks.read().unwrap().working_bank();
        let decision = BufferedPacketsDecision::Consume(bank);

        // Init scheduler with bank start info
        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        assert!(
            scheduler.slot.is_some(),
            "Scheduler slot should be set after receiving bank start"
        );

        // A stale Consume decision must not dispatch without an active leader Bank.
        assert_eq!(
            scheduler
                .schedule(&mut container, 0, 0)
                .unwrap()
                .num_scheduled,
            0
        );
        assert!(consume_work_receiver.try_recv().is_err());
        set_leader_bank(&mut scheduler.shared_leader_state, decision.bank().unwrap());

        // Schedule the transactions
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();

        // Only two should have been scheduled as one is blocked
        assert_eq!(result.num_scheduled, 2);

        // Receive the scheduled work
        let work_1 = consume_work_receiver.try_recv().unwrap();
        assert_eq!(work_1.ids.len(), 1);
        assert!(work_1.admission.is_some());
        let work_2 = consume_work_receiver.try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);
        assert!(work_2.admission.is_some());

        // Check that the first transaction is from keypair_a and first recipient is the first recipient
        assert_eq!(
            work_1.transactions[0].message().account_keys()[0],
            keypair_a.pubkey()
        );
        assert_eq!(
            work_1.transactions[0].message().account_keys()[1],
            first_fifo_recipient
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
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
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
                extra_info: Some(vec![response]),
            };
            finished_consume_work_sender.send(finished_work).unwrap();
        }

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 2);

        // Check the responses
        let (seq_id, Committed(committed)) = next_result(&mut response_receiver) else {
            panic!("expected Committed result");
        };
        assert_eq!(seq_id, u32::MAX);
        assert_eq!(committed.transaction_results.len(), 1);
        assert_eq!(committed.transaction_results[0].cus_consumed, 100);

        // Check the response for the second transaction (not committed)
        let (seq_id, NotCommitted(not_committed)) = next_result(&mut response_receiver) else {
            panic!("expected NotCommitted result");
        };
        assert_eq!(seq_id, 3);
        assert_eq!(
            not_committed.reason,
            Some(Reason::SchedulingError(SchedulingError::PohTimeout as i32))
        );

        // Now try scheduling again; should schedule the remaining transaction
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receiver.try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(result.num_scheduled, 0);

        // Send back the finished work for the second transaction
        let finished_work = FinishedConsumeWork {
            work: work_2,
            retryable_indexes: vec![],
            extra_info: Some(vec![TransactionResult::Committed(
                TransactionCommittedResult {
                    cus_consumed: 1500,
                    feepayer_balance_lamports: 1500,
                    loaded_accounts_data_size: 20,
                    execution_success: true,
                },
            )]),
        };
        finished_consume_work_sender.send(finished_work).unwrap();

        // Receive the finished work
        let (num_transactions, _) = scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(num_transactions, 1);

        // Check the response for the next transaction
        let (seq_id, Committed(committed)) = next_result(&mut response_receiver) else {
            panic!("expected Committed result");
        };
        assert_eq!(seq_id, 0);
        assert_eq!(committed.transaction_results.len(), 1);
        assert_eq!(committed.transaction_results[0].cus_consumed, 1500);

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
        let (seq_id, NotCommitted(not_committed)) = next_result(&mut response_receiver) else {
            panic!("expected NotCommitted result");
        };
        assert_eq!(seq_id, 2);
        assert_eq!(
            not_committed.reason,
            Some(Reason::SchedulingError(
                SchedulingError::OutsideLeaderSlot as i32
            ))
        );
    }

    #[test]
    #[should_panic(expected = "node must exist")]
    fn test_prio_graph_clears_on_slot_boundary() {
        let bank_forks = test_bank_forks();
        let TestScheduler { mut scheduler, .. } = create_test_scheduler(&bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();
        let keypair_b = Keypair::new();

        let bank = bank_forks.read().unwrap().working_bank();

        // Set initial slot with bank start
        let mut container = create_container(vec![
            (&keypair_a, vec![Pubkey::new_unique()], 1000, 0, u64::MAX),
            (&keypair_b, vec![Pubkey::new_unique()], 2000, 1, u64::MAX),
        ]);
        let decision = BufferedPacketsDecision::Consume(bank.clone());

        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(scheduler.slot, Some(bank.slot()));

        // Pull transactions into prio_graph
        let txn_id = *container.recheck_iter(None).next().unwrap();
        scheduler.pull_into_prio_graph(&mut container);
        assert!(
            !scheduler.prio_graph.is_empty(),
            "Prio graph should have transactions"
        );

        // Simulate slot boundary change by changing to no bank (None)
        let decision_no_bank = BufferedPacketsDecision::Forward;
        scheduler
            .receive_completed(&mut container, &decision_no_bank)
            .unwrap();

        assert_eq!(scheduler.slot, None);

        // This should panic because the prio_graph has been cleared
        // and the transaction ID no longer exists in the graph
        scheduler.prio_graph.unblock(&txn_id);
    }

    /// Regression test for the `solBamSched` "blocking node must exist" panic.
    ///
    /// A bundle is inserted as one `PrioGraph` node, so two transactions sharing
    /// a writable account (the common fee payer here) make the node reference
    /// the same resource twice. prio-graph 0.3.0 tolerates this (its
    /// `insert_transaction` skips a blocker equal to the node itself); 0.1.0
    /// lacked that guard and panicked. Guards against regressing to a version
    /// without it.
    #[test]
    fn test_pull_bundle_with_shared_writable_account_does_not_panic() {
        let (mut test, bank) = admission_scheduler();
        let scheduler = &mut test.scheduler;

        // One batch, two transactions sharing a writable account: both are
        // signed by `keypair_a`, so both write its fee-payer account (index 0).
        let keypair_a = Keypair::new();
        let priority = u64::MAX;
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        container.insert_new_batch(
            [1000, 2000]
                .into_iter()
                .map(|lamports| {
                    (
                        prioritized_tranfers(
                            &keypair_a,
                            [Pubkey::new_unique()],
                            lamports,
                            priority,
                        ),
                        MaxAge::MAX,
                    )
                })
                .collect(),
            priority,
            false,
            u64::MAX,
            0,
        );
        scheduler
            .receive_completed(
                &mut container,
                &BufferedPacketsDecision::Consume(bank.clone()),
            )
            .unwrap();

        // Must not panic; the bundle becomes a single schedulable node.
        scheduler.pull_into_prio_graph(&mut container);

        assert!(
            !scheduler.prio_graph.is_empty(),
            "bundle sharing a writable account should be inserted and schedulable"
        );

        // A dispatch-time recheck rejects the default blockhash after taking the transactions.
        // It must recycle the work, clear the graph node, and leave no reservation behind.
        scheduler.extra_checks_enabled = true;
        assert_eq!(scheduler.send_to_workers(&mut container).unwrap(), 0);
        assert!(scheduler.prio_graph.is_empty());
        assert_eq!(container.buffer_size(), 0);
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
        assert!(scheduler.reusable_consume_work[0].transactions.is_empty());
    }

    // ---- scheduler-side cost admission (JSA-72) ----

    /// Two independent batches plus a bank the scheduler can admit on, with `slot` set.
    fn setup_two_batches(
        second_batch_size: usize,
    ) -> (
        TestScheduler,
        TransactionStateContainer<Tx>,
        Arc<Bank>,
        BufferedPacketsDecision,
    ) {
        let (mut test, bank) = admission_scheduler();
        let mut container = TransactionStateContainer::with_capacity(8);
        for (seq_id, size) in [1, second_batch_size].into_iter().enumerate() {
            container.insert_new_batch(
                (0..size)
                    .map(|_| {
                        (
                            prioritized_tranfers(&Keypair::new(), [Pubkey::new_unique()], 1000, 0),
                            MaxAge::MAX,
                        )
                    })
                    .collect(),
                u64::MAX - seq_id as u64,
                size > 1,
                u64::MAX,
                seq_id as u32,
            );
        }
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        (test, container, bank, decision)
    }

    /// Deterministic scheduler-level version of the JSA-72 live PoC.
    ///
    /// The PoC pauses the earlier, high-CU transaction after dequeue so a later cheap transfer
    /// can reserve the Bank budget first. Scheduler-side admission makes that worker timing
    /// irrelevant: only the high-priority work can be dispatched until its estimate settles.
    #[test]
    fn test_jsa72_poc_priority_survives_inverted_worker_timing() {
        let (mut test, bank) = admission_scheduler();

        // Match the live PoC shapes: the earlier transaction requests 200k CUs, while the later
        // transaction is a plain transfer. Distinct payers and recipients keep the batches
        // independent in the priority graph.
        let poc_transfer = |compute_unit_limit: Option<u32>| {
            let from = Keypair::new();
            let mut instructions = compute_unit_limit
                .map(ComputeBudgetInstruction::set_compute_unit_limit)
                .into_iter()
                .collect_vec();
            instructions.push(transfer(&from.pubkey(), &Pubkey::new_unique(), 1));
            RuntimeTransaction::from_transaction_for_tests(Transaction::new(
                &[&from],
                Message::new(&instructions, Some(&from.pubkey())),
                Hash::default(),
            ))
        };
        let high_transaction = poc_transfer(Some(200_000));
        let low_transaction = poc_transfer(None);
        let transaction_costs = QosService::compute_transaction_costs(
            &bank.feature_set,
            [&high_transaction, &low_transaction].into_iter(),
            std::iter::repeat(Ok(())),
        );
        let high_cost = transaction_costs[0].as_ref().unwrap();
        let low_cost = transaction_costs[1].as_ref().unwrap();
        let high_estimate = high_cost.sum();
        let low_estimate = low_cost.sum();

        // As in the PoC, the block limit is exactly the high transaction's reservation. If the
        // low transaction reserved first, the high transaction would be rejected even though
        // both fit after the high estimate settles to actual execution cost.
        set_block_cost_limit(&bank, high_estimate);
        {
            let mut tracker = bank.write_cost_tracker().unwrap();
            tracker.try_add(low_cost).unwrap();
            assert!(matches!(
                tracker.try_add(high_cost),
                Err(CostTrackerError::WouldExceedBlockMaxLimit)
            ));
            assert_eq!(tracker.block_cost(), low_estimate);
            assert!(tracker.block_cost() < tracker.block_cost_limit());
            tracker.remove(low_cost);
        }
        drop(transaction_costs);

        let mut container = TransactionStateContainer::with_capacity(8);
        for (transaction, priority, seq_id) in [(high_transaction, 2, 0), (low_transaction, 1, 1)] {
            assert!(
                container
                    .insert_new_batch(
                        std::iter::once((transaction, MaxAge::MAX)).collect(),
                        priority,
                        false,
                        u64::MAX,
                        seq_id,
                    )
                    .is_some()
            );
        }
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let mut high_work = test.consume_work_receiver.try_recv().unwrap();
        let high_info = &test.scheduler.inflight_batch_info[&high_work.batch_id];
        assert_eq!(high_info.seq_id, 0);
        assert!(test.scheduler.pending_admission.is_some());
        assert_eq!(block_cost_and_in_flight(&bank), (high_estimate, 1));
        assert!(test.consume_work_receiver.try_recv().is_err());
        assert_eq!(
            test.scheduler
                .schedule(&mut container, 0, 0)
                .unwrap()
                .num_scheduled,
            0,
            "a pending batch must not spin or retry before a completion"
        );

        // Complete the high transaction below its estimate. This is the event the live PoC
        // delayed; here it is explicit, so the test has no sleeps or scheduling races.
        settle_committed(&bank, &mut high_work, 150);
        let settled_high_cost = bank.read_cost_tracker().unwrap().block_cost();
        assert!(settled_high_cost + low_estimate <= high_estimate);
        finish_committed(&mut test, &mut container, &decision, high_work, 150);
        let (seq_id, result) = next_result(&mut test.response_receiver);
        assert_eq!(seq_id, 0);
        assert!(matches!(result, Committed(_)));

        // Only after the earlier reservation settles can the lower-priority work be dispatched.
        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let mut low_work = test.consume_work_receiver.try_recv().unwrap();
        let low_info = &test.scheduler.inflight_batch_info[&low_work.batch_id];
        assert_eq!(low_info.seq_id, 1);
        assert_eq!(
            block_cost_and_in_flight(&bank),
            (settled_high_cost + low_estimate, 1)
        );

        settle_committed(&bank, &mut low_work, 150);
        finish_committed(&mut test, &mut container, &decision, low_work, 150);
        let (seq_id, result) = next_result(&mut test.response_receiver);
        assert_eq!(seq_id, 1);
        assert!(matches!(result, Committed(_)));
        assert_eq!(block_cost_and_in_flight(&bank).1, 0);
        assert!(test.scheduler.inflight_batch_info.is_empty());
    }

    #[test_case::test_case(1; "single_transaction")]
    #[test_case::test_case(2; "partial_batch_rollback")]
    fn test_deferred_batch_is_final_once_inflight_settles_without_freeing_budget(
        second_batch_size: usize,
    ) {
        let (mut test, mut container, bank, decision) = setup_two_batches(second_batch_size);
        let estimate = estimated_cost(&bank);
        set_block_cost_limit(&bank, estimate * second_batch_size as u64 + estimate / 2);

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let mut work_a = test.consume_work_receiver.try_recv().unwrap();
        assert!(test.scheduler.pending_admission.is_some());
        // B must roll back any earlier admissions in its batch without touching A's reservation.
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));
        assert!(test.consume_work_receiver.try_recv().is_err());

        // A commits at exactly its estimate: nothing is freed. The worker settled it.
        bank.write_cost_tracker()
            .unwrap()
            .sub_transactions_in_flight(1);
        work_a.admission = None;
        finish_committed(
            &mut test,
            &mut container,
            &decision,
            work_a,
            estimate as u32,
        );

        // Nothing inflight can cover the shortfall any more, so B is dispatched with the final
        // per-transaction error, exactly as the worker would have produced it.
        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert!(test.scheduler.pending_admission.is_none());
        let work_b = test.consume_work_receiver.try_recv().unwrap();
        assert_eq!(
            work_b.admission.as_ref().unwrap().1,
            std::iter::repeat_n(Ok(()), second_batch_size - 1)
                .chain([Err(TransactionError::WouldExceedMaxBlockCostLimit)])
                .collect_vec()
        );
        assert_eq!(
            block_cost_and_in_flight(&bank),
            (estimate * second_batch_size as u64, second_batch_size - 1)
        );
        test.scheduler.recycle_work_object(work_b);
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 0));
    }

    #[test]
    fn test_deferred_batch_and_its_dependents_are_drained_at_slot_boundary() {
        let (mut test, bank) = admission_scheduler();
        let estimate = estimated_cost(&bank);
        set_block_cost_limit(&bank, estimate + estimate / 2);

        // A and B are independent; C shares B's fee payer and is blocked behind it.
        let keypair_b = Keypair::new();
        let mut container = create_container(vec![
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                1000,
                0,
                u64::MAX,
            ),
            (&keypair_b, vec![Pubkey::new_unique()], 1000, 1, u64::MAX),
            (&keypair_b, vec![Pubkey::new_unique()], 1000, 2, u64::MAX),
        ]);
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        test.consume_work_receiver.try_recv().unwrap();
        assert!(test.scheduler.pending_admission.is_some());

        // Slot ends: the deferred batch and the batch it was blocking both go back to BAM.
        test.scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
            .unwrap();
        assert!(test.scheduler.pending_admission.is_none());
        assert!(test.scheduler.prio_graph.is_empty());
        assert!(container.pop().is_none());

        let mut seq_ids = vec![];
        for _ in 0..2 {
            let (seq_id, result) = next_result(&mut test.response_receiver);
            assert!(matches!(
                result,
                NotCommitted(not_committed)
                    if not_committed.reason == Some(Reason::SchedulingError(
                        SchedulingError::OutsideLeaderSlot as i32
                    ))
            ));
            seq_ids.push(seq_id);
        }
        seq_ids.sort_unstable();
        assert_eq!(seq_ids, vec![1, 2]);
        assert!(test.response_receiver.try_recv().is_err());
        // A is still inflight; its result arrives with the worker's response as before.
        assert_eq!(test.scheduler.inflight_batch_info.len(), 1);
    }

    #[test]
    fn test_bank_replacement_within_slot_restarts_admission_on_new_bank() {
        let (mut test, mut container, bank_1, _) = setup_two_batches(1);
        let estimate = estimated_cost(&bank_1);
        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let work_a = test.consume_work_receiver.try_recv().unwrap();
        let work_b = test.consume_work_receiver.try_recv().unwrap();
        assert_eq!(block_cost_and_in_flight(&bank_1), (estimate * 2, 2));

        // The bankless handover clears the graph. Work arriving in the gap must remain queued.
        test.scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
            .unwrap();
        assert_eq!(test.scheduler.slot, None);
        let transaction = prioritized_tranfers(&Keypair::new(), [Pubkey::new_unique()], 1000, 0);
        assert!(
            container
                .insert_new_batch(
                    std::iter::once((transaction, MaxAge::MAX)).collect(),
                    u64::MAX,
                    false,
                    u64::MAX,
                    2,
                )
                .is_some()
        );

        // ParentReady installs a replacement for the same slot. Do not adopt it or dispatch C
        // until both old-bank batches have returned.
        let bank_1b = Arc::new(Bank::new_from_parent(
            bank_1.parent().unwrap(),
            SlotLeader::new_unique(),
            bank_1.slot(),
        ));
        assert_ne!(bank_1b.bank_id(), bank_1.bank_id());
        set_leader_bank(&mut test.scheduler.shared_leader_state, &bank_1b);
        let decision = BufferedPacketsDecision::Consume(bank_1b.clone());
        // Old work returned with its admission attached must drain before C can dispatch.
        for (work, remaining) in [(work_a, 1), (work_b, 0)] {
            test.scheduler
                .receive_completed(&mut container, &decision)
                .unwrap();
            test.scheduler.schedule(&mut container, 0, 0).unwrap();
            assert!(test.consume_work_receiver.try_recv().is_err());
            assert_eq!(test.scheduler.slot, None);
            finish_committed(&mut test, &mut container, &decision, work, 150);
            assert_eq!(
                block_cost_and_in_flight(&bank_1),
                (estimate * remaining as u64, remaining)
            );
        }

        // With both old-bank batches drained, adopt the new bank and admit C.
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let work_c = test.consume_work_receiver.try_recv().unwrap();
        let admission = work_c.admission.as_ref().unwrap();
        assert_eq!(admission.0.bank_id(), bank_1b.bank_id());
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(block_cost_and_in_flight(&bank_1b), (estimate, 1));
    }

    #[test_case::test_case(false; "returned_work")]
    #[test_case::test_case(true; "disconnected_worker")]
    fn test_unprocessed_work_releases_reservation_on_its_bank(disconnected: bool) {
        let (mut test, mut container, bank, decision) = setup_two_batches(1);
        let estimate = estimated_cost(&bank);
        set_block_cost_limit(&bank, estimate);

        if disconnected {
            drop(test.consume_work_receiver);
            assert!(matches!(
                test.scheduler.schedule(&mut container, 0, 0),
                Err(super::SchedulerError::DisconnectedSendChannel(_))
            ));
            assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
            assert_eq!(test.scheduler.inflight_reserved_cost, 0);
            assert!(!test.scheduler.has_in_flight_transactions());
            return;
        }

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let work_a = test.consume_work_receiver.try_recv().unwrap();
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));

        // The worker found the bank complete and returned A untouched, admission attached.
        test.finished_consume_work_sender
            .send(FinishedConsumeWork {
                work: work_a,
                retryable_indexes: vec![RetryableIndex::new(0, true)],
                extra_info: Some(vec![TransactionResult::NotCommitted(
                    NotCommittedReason::PohTimeout,
                )]),
            })
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        // The released admission must not keep the bank alive from the reuse pool.
        assert!(
            test.scheduler
                .reusable_consume_work
                .iter()
                .all(|work| work.admission.is_none())
        );
        assert!(matches!(
            next_result(&mut test.response_receiver),
            (0, NotCommitted(not_committed))
                if not_committed.reason
                    == Some(Reason::SchedulingError(SchedulingError::PohTimeout as i32))
        ));
    }
}
