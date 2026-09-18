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
            consumer::{Consumer, TipProcessingDependencies},
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
        SchedulingError, atomic_txn_batch_result, not_committed::Reason,
    },
    prio_graph::{AccessKind, GraphNode, PrioGraph},
    smallvec::SmallVec,
    solana_clock::{BankId, MAX_PROCESSING_AGE, Slot},
    solana_nohash_hasher::IntMap,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction_error::TransactionError,
    std::{
        borrow::Borrow,
        collections::BTreeMap,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::mpsc::Sender as TokioSender,
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

    extra_checks_enabled: bool,
    shared_leader_state: SharedLeaderState,
    tip_processing: Option<(Consumer, TipProcessingDependencies)>,
    tip_retry_at: Option<(BankId, Instant)>,

    /// Prepared Bank whose cost tracker holds the current reservations.
    admission_bank: Option<(BankId, Slot)>,
    /// Estimated cost reserved on `admission_bank` by dispatched work that has not settled.
    inflight_reserved_cost: u64,
    /// Deferred or returned batches in original dispatch order, with their last attempted estimate.
    pending_admission: BTreeMap<u64, (TransactionPriorityId, Option<u64>)>,
}

// A structure to hold information about inflight batches.
// A batch can either be one 'revert_on_error' batch or multiple
// 'non-revert_on_error' batches that are scheduled together.
struct InflightBatchInfo {
    pub schedule_time: Instant,
    // SmallVec 1: each scheduled work item typically corresponds to one batch id.
    pub batch_priority_ids: SmallVec<[(TransactionPriorityId, u32 /* seq_id */); 1]>,
    pub slot: Slot,
    /// Estimate the scheduler reserved for this work, held until completion.
    pub reserved_cost: u64,
}

impl<Tx: TransactionWithMeta> BamScheduler<Tx> {
    pub fn new(
        consume_work_sender: Sender<ConsumeWork<Tx>>,
        finished_consume_work_receiver: Receiver<FinishedConsumeWork<Tx>>,
        response_sender: TokioSender<BamOutboundMessage>,
        shared_leader_state: SharedLeaderState,
        tip_processing: Option<(Consumer, TipProcessingDependencies)>,
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
            extra_checks_enabled: true,
            shared_leader_state,
            tip_processing,
            tip_retry_at: None,
            admission_bank: None,
            inflight_reserved_cost: 0,
            pending_admission: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn has_in_flight_transactions(&self) -> bool {
        !self.inflight_batch_info.is_empty()
    }

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
    fn pull_into_prio_graph<S: StateContainer<Tx>>(
        &mut self,
        container: &mut S,
        working_bank: &Bank,
    ) {
        let slot = working_bank.slot();

        while let Some(next_batch_id) = container.pop() {
            let Some((batch_ids, _, max_schedule_slot, seq_id)) =
                container.get_batch(next_batch_id.id)
            else {
                error!("Batch {} not found in container", next_batch_id.id);
                container.remove_by_id(next_batch_id.id);
                continue;
            };

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

            if self.extra_checks_enabled {
                let lock_results: SmallVec<
                    [solana_transaction_error::TransactionResult<()>; MAX_PACKETS_PER_BUNDLE],
                > = SmallVec::from_elem(Ok(()), txns.len());
                let check_result = working_bank.check_transactions::<Tx>(
                    &txns,
                    &lock_results,
                    MAX_PROCESSING_AGE,
                    true,
                    &mut TransactionErrorMetrics::default(),
                );
                if let Some((index, err)) = check_result
                    .iter()
                    .enumerate()
                    .find_map(|(i, res)| res.as_ref().err().cloned().map(|err| (i, err)))
                {
                    drop(txns);
                    container.remove_by_id(next_batch_id.id);

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
                .insert(seq_id, Instant::now());
            self.prio_graph.insert_transaction(
                next_batch_id,
                Self::get_transactions_account_access(txns.into_iter()),
            );
        }
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        admission_bank: &Arc<Bank>,
    ) -> Result<usize, SchedulerError> {
        let slot = admission_bank.slot();

        if self.admission_bank != Some((admission_bank.bank_id(), slot)) {
            // Drain old admissions before retrying returned work on a replacement Bank.
            if !self.inflight_batch_info.is_empty() {
                return Ok(0);
            }
            if self.tip_retry_at.is_some_and(|(bank_id, deadline)| {
                bank_id == admission_bank.bank_id() && Instant::now() < deadline
            }) {
                return Ok(0);
            }
            if let Some((consumer, tips)) = &self.tip_processing
                && !tips.process_tip_programs(consumer, admission_bank)
            {
                // The controller busy-polls; do not sign/execute a failing crank every poll.
                self.tip_retry_at = Some((
                    admission_bank.bank_id(),
                    Instant::now() + Duration::from_millis(1),
                ));
                return Ok(0);
            }
            self.tip_retry_at = None;
            // Equal inflight totals on a different Bank do not describe the same admission attempt.
            for (_, attempted_cost) in self.pending_admission.values_mut() {
                *attempted_cost = None;
            }
            self.admission_bank = Some((admission_bank.bank_id(), slot));
        }

        if self.prio_graph.is_empty() && self.pending_admission.is_empty() {
            return Ok(0);
        }

        let now = Instant::now();
        let mut num_scheduled = 0;
        loop {
            // A deferred batch holds the head of the line until work on its bank settles or the
            // bank itself changes; either way it gets the next attempt before anything else.
            let (batch_id, id) = if let Some((&batch_id, &(id, attempted_inflight_cost))) =
                self.pending_admission.first_key_value()
            {
                if attempted_inflight_cost == Some(self.inflight_reserved_cost) {
                    return Ok(num_scheduled);
                }
                self.pending_admission.pop_first();
                (TransactionBatchId::new(batch_id), id)
            } else {
                let Some(id) = self.prio_graph.pop() else {
                    return Ok(num_scheduled);
                };
                (self.get_next_schedule_id(), id)
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
                let check_result = admission_bank.check_transactions::<Tx>(
                    &sanitized_txs,
                    &lock_results,
                    MAX_PROCESSING_AGE,
                    true,
                    &mut TransactionErrorMetrics::default(),
                );
                if let Some((index, err)) = check_result
                    .iter()
                    .enumerate()
                    .find_map(|(i, res)| res.as_ref().err().cloned().map(|err| (i, err)))
                {
                    drop(sanitized_txs);
                    container.remove_by_id(id.id);
                    self.prio_graph.unblock(&id);

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

            let mut work = self.get_or_create_work_object();
            Self::populate_consume_work(
                &mut work,
                batch_id,
                &[id],
                revert_on_error,
                container,
                slot,
            );

            // Admit cost here, in pop order, so eight workers racing for the cost tracker cannot
            // reorder it.
            let attempt = QosService::try_admit_transactions(
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
                revert_on_error,
            );
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
                self.pending_admission
                    .insert(batch_id.0, (id, Some(self.inflight_reserved_cost)));
                return Ok(num_scheduled);
            };
            work.admission = Some((Arc::clone(admission_bank), results));
            num_scheduled += work.ids.len();
            self.send_to_worker(SmallVec::from([(id, seq_id)]), work, slot, reserved_cost)?;
        }
    }

    fn send_to_worker(
        &mut self,
        // SmallVec 1: scheduler currently sends a single batch id per work item.
        priority_ids: SmallVec<[(TransactionPriorityId, u32); 1]>,
        work: ConsumeWork<Tx>,
        slot: Slot,
        reserved_cost: u64,
    ) -> Result<(), SchedulerError> {
        let batch_id = work.batch_id;
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
                batch_priority_ids: priority_ids,
                slot,
                reserved_cost,
            },
        );
        Ok(())
    }

    fn get_next_schedule_id(&mut self) -> TransactionBatchId {
        let result = TransactionBatchId::new(self.next_batch_id);
        self.next_batch_id += 1;
        result
    }

    fn get_or_create_work_object(&mut self) -> ConsumeWork<Tx> {
        self.reusable_consume_work.pop().unwrap_or_else(|| {
            // These values will be overwritten by `populate_consume_work`
            ConsumeWork {
                batch_id: TransactionBatchId::new(0),
                ids: Vec::with_capacity(1),
                transactions: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                max_ages: Vec::with_capacity(MAX_PACKETS_PER_BUNDLE),
                revert_on_error: false,
                respond_with_extra_info: false,
                max_schedule_slot: None,
                admission: None,
            }
        })
    }

    fn release_admission(work: &mut ConsumeWork<Tx>) {
        if let Some((bank, results)) = work.admission.take() {
            let costs = QosService::compute_transaction_costs(
                &bank.feature_set,
                work.transactions.iter(),
                results.into_iter(),
            );
            QosService::remove_or_update_costs(costs.iter(), None, &bank);
        }
    }

    fn recycle_work_object(&mut self, mut work: ConsumeWork<Tx>) {
        Self::release_admission(&mut work);
        work.ids.clear();
        work.transactions.clear();
        work.max_ages.clear();
        self.reusable_consume_work.push(work);
    }

    /// Populates a reusable `ConsumeWork` from scheduled `priority_ids` and stamps
    /// scheduling metadata for worker execution.
    fn populate_consume_work(
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
                .flat_map(|(batch_ids, _, _, _)| batch_ids.into_iter())
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
    fn generate_revert_on_error_bundle_result<I: IntoIterator<Item = TransactionResult>>(
        processed_results: I,
    ) -> atomic_txn_batch_result::Result {
        let mut saw_commit_cancelled = false;
        let processed_results = processed_results.into_iter();
        let mut transaction_results = Vec::with_capacity(processed_results.size_hint().0);
        for (i, result) in processed_results.enumerate() {
            match result {
                TransactionResult::Committed(processed) => transaction_results.push(processed),
                // TransactionError::CommitCancelled indicates another transaction in this bundle errored out.
                TransactionResult::NotCommitted(NotCommittedReason::Error(err))
                    if err != TransactionError::CommitCancelled =>
                {
                    return atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Self::convert_reason_to_proto(
                                i,
                                NotCommittedReason::Error(err),
                            )),
                        },
                    );
                }
                TransactionResult::NotCommitted(NotCommittedReason::PohTimeout) => {
                    return atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Self::convert_reason_to_proto(
                                i,
                                NotCommittedReason::PohTimeout,
                            )),
                        },
                    );
                }
                TransactionResult::NotCommitted(NotCommittedReason::Error(_)) => {
                    saw_commit_cancelled = true;
                }
            }
        }

        if saw_commit_cancelled {
            return atomic_txn_batch_result::Result::NotCommitted(
                jito_protos::proto::bam_types::NotCommitted {
                    reason: Some(Self::convert_reason_to_proto(
                        0,
                        NotCommittedReason::PohTimeout,
                    )),
                },
            );
        }

        atomic_txn_batch_result::Result::Committed(jito_protos::proto::bam_types::Committed {
            transaction_results,
        })
    }

    /// Generates a `bundle_result::Result` based on the processed result of a single transaction.
    fn generate_bundle_result(processed: TransactionResult) -> atomic_txn_batch_result::Result {
        match processed {
            TransactionResult::Committed(result) => atomic_txn_batch_result::Result::Committed(
                jito_protos::proto::bam_types::Committed {
                    transaction_results: vec![result],
                },
            ),
            TransactionResult::NotCommitted(reason) => {
                atomic_txn_batch_result::Result::NotCommitted(
                    jito_protos::proto::bam_types::NotCommitted {
                        reason: Some(Self::convert_reason_to_proto(0, reason)),
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
        let bank_slot = decision.bank().map(|bank| bank.slot()).or_else(|| {
            matches!(decision, BufferedPacketsDecision::Hold)
                .then(|| self.shared_leader_state.load().bank_slot())
                .flatten()
        });
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

        // Pending admissions still hold their popped graph nodes. Unblock them so the drain
        // below reaches their dependents, and report the same result as other queued work.
        while let Some((_, (pending_id, _))) = self.pending_admission.pop_first() {
            self.prio_graph.unblock(&pending_id);
            if let Some((_, _, _, seq_id)) = container.get_batch(pending_id.id) {
                self.send_no_leader_slot_bundle_result(seq_id);
            }
            container.remove_by_id(pending_id.id);
        }
        // Unblock all transactions blocked by inflight batches
        // and then drain the prio-graph
        for inflight_info in self.inflight_batch_info.values() {
            for (priority_id, _) in &inflight_info.batch_priority_ids {
                if prev_slot == Some(inflight_info.slot) {
                    self.prio_graph.unblock(priority_id);
                }
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

        // Pin both validation passes to the same resolved bank, even if BankForks advances.
        let leader_state = self.shared_leader_state.load();
        if leader_state.atomic_batches_enabled()
            && let Some(bank) = leader_state.working_bank()
            && !bank.is_complete()
            && Some(bank.slot()) == self.slot
        {
            self.pull_into_prio_graph(container, bank);
            num_scheduled = self.send_to_workers(container, bank)?;
        }

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
                mut work,
                retryable_indexes,
                extra_info,
            } = result;
            num_transactions += work.ids.len();
            let batch_id = work.batch_id;
            let revert_on_error = work.revert_on_error;
            let Some(inflight_batch_info) = self.inflight_batch_info.remove(&batch_id) else {
                self.recycle_work_object(work);
                continue;
            };

            // Settled work may have freed budget for a batch deferred on this bank's block limit.
            // Dispatch is held across a bank change until the old work drains, so everything in
            // flight was admitted on `admission_bank`.
            self.inflight_reserved_cost = self
                .inflight_reserved_cost
                .saturating_sub(inflight_batch_info.reserved_cost);

            let retry_on_replacement = work.admission.as_ref().is_some_and(|(owner, _)| {
                let leader_state = self.shared_leader_state.load();
                Some(inflight_batch_info.slot) == self.slot
                    && leader_state.bank_slot() == self.slot
                    && retryable_indexes.len() == work.transactions.len()
                    && leader_state
                        .working_bank()
                        .is_none_or(|bank| bank.bank_id() != owner.bank_id())
            });
            if retry_on_replacement {
                Self::release_admission(&mut work);
                for (id, transaction) in work.ids.iter().zip(work.transactions.drain(..)) {
                    container
                        .get_mut_transaction_state(*id)
                        .unwrap()
                        .retry_transaction(transaction);
                }
                // Keep the graph node blocked and its original dispatch ID across replacements.
                self.pending_admission.insert(
                    batch_id.0,
                    (inflight_batch_info.batch_priority_ids[0].0, None),
                );
                self.recycle_work_object(work);
                continue;
            }
            self.recycle_work_object(work);

            let _ = self.time_in_worker_us.increment(
                now.duration_since(inflight_batch_info.schedule_time)
                    .as_micros() as u64,
            );
            let mut processed_results = extra_info.map(|info| info.processed_results.into_iter());

            // Should never not be 1; but just in case
            let len = if revert_on_error {
                1
            } else {
                inflight_batch_info.batch_priority_ids.len()
            };
            for (i, (priority_id, seq_id)) in inflight_batch_info
                .batch_priority_ids
                .iter()
                .copied()
                .enumerate()
                .take(len)
            {
                // If we got extra info, we can send back the result
                if revert_on_error {
                    if let Some(processed_results) = processed_results.take() {
                        let bundle_result =
                            Self::generate_revert_on_error_bundle_result(processed_results);
                        self.send_back_result(seq_id, bundle_result);
                    }
                } else if let Some(processed_results) = processed_results.as_mut() {
                    let Some(txn_result) = processed_results.next() else {
                        warn!(
                            "Processed results for batch {} are missing for index {i}",
                            batch_id.0
                        );
                        continue;
                    };
                    let bundle_result = Self::generate_bundle_result(txn_result);
                    self.send_back_result(seq_id, bundle_result);
                }

                // If in the same slot, unblock the transaction
                if Some(inflight_batch_info.slot) == self.slot {
                    self.prio_graph.unblock(&priority_id);
                }

                // Remove the transaction from the container
                container.remove_by_id(priority_id.id);
            }
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
                    ConsumeWork, FinishedConsumeWork, FinishedConsumeWorkExtraInfo, MaxAge,
                    NotCommittedReason, TransactionResult,
                },
                tests::create_slow_genesis_config,
                transaction_scheduler::{
                    bam_receive_and_buffer::tests::set_leader_bank,
                    bam_scheduler::{BamScheduler, MAX_PACKETS_PER_BUNDLE},
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
        smallvec::SmallVec,
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_cost_model::cost_tracker::{CostTrackerError, CostTrackerLimits},
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_poh::poh_recorder::{LeaderState, SharedLeaderState},
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, bank_forks::BankForks},
        solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
        solana_signer::Signer,
        solana_system_interface::instruction::{transfer, transfer_many},
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        solana_transaction_error::TransactionError,
        std::{
            borrow::Borrow,
            sync::{Arc, RwLock},
            time::{Duration, Instant},
        },
    };

    type Tx = RuntimeTransaction<SanitizedTransaction>;

    struct TestScheduler {
        // Banks hold only a weak reference to the program cache fork graph.
        _bank_forks: Arc<RwLock<BankForks>>,
        scheduler: BamScheduler<RuntimeTransaction<SanitizedTransaction>>,
        consume_work_receivers:
            Vec<crossbeam_channel::Receiver<ConsumeWork<RuntimeTransaction<SanitizedTransaction>>>>,
        finished_consume_work_sender: crossbeam_channel::Sender<
            FinishedConsumeWork<RuntimeTransaction<SanitizedTransaction>>,
        >,
        response_receiver: tokio::sync::mpsc::Receiver<BamOutboundMessage>,
    }

    fn create_test_scheduler(
        num_threads: usize,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> TestScheduler {
        let (consume_work_sender, consume_work_receiver) = unbounded();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(100);
        let mut shared_leader_state = SharedLeaderState::new(0, None, None);
        set_leader_bank(
            &mut shared_leader_state,
            Some(bank_forks.read().unwrap().working_bank()),
        );
        let scheduler = BamScheduler::new(
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            shared_leader_state,
            None,
        );
        TestScheduler {
            _bank_forks: bank_forks.clone(),
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
                u32,
                u64,
            ),
        >,
    ) -> TransactionStateContainer<RuntimeTransaction<SanitizedTransaction>> {
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (fifo_index, (from_keypair, to_pubkeys, lamports, seq_id, max_schedule_slot)) in
            tx_infos.into_iter().enumerate()
        {
            let transaction = prioritized_tranfers(
                from_keypair.borrow(),
                to_pubkeys,
                lamports,
                u64::from(seq_id),
            );
            let mut txns_max_age: SmallVec<
                [(RuntimeTransaction<SanitizedTransaction>, MaxAge); MAX_PACKETS_PER_BUNDLE],
            > = SmallVec::new();
            txns_max_age.push((transaction, MaxAge::MAX));
            let priority = u64::MAX.saturating_sub(fifo_index as u64);
            container.insert_new_batch(txns_max_age, priority, false, max_schedule_slot, seq_id);
        }

        container
    }

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    #[test]
    fn test_scheduler_empty() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler { mut scheduler, .. } = create_test_scheduler(4, &bank_forks);

        let mut container = TransactionStateContainer::with_capacity(100);
        let result = scheduler.schedule(&mut container, 0).unwrap();
        assert_eq!(result.num_scheduled, 0);
    }

    #[test]
    fn test_unresolved_bank_behavior() {
        let (bank_forks, _) = test_bank_forks();
        let bank = bank_forks.read().unwrap().working_bank();
        let mut shared_leader_state = SharedLeaderState::new(0, None, None);
        shared_leader_state.store(Arc::new(LeaderState::new_with_atomic_batches_enabled(
            Some(bank.clone()),
            0,
            None,
            None,
            false,
        )));
        let mut test = create_test_scheduler(1, &bank_forks);
        test.scheduler.shared_leader_state = shared_leader_state.clone();
        test.scheduler.extra_checks_enabled = false;
        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        for (fifo_index, (revert_on_error, seq_id)) in
            [(true, 71), (false, 72)].into_iter().enumerate()
        {
            let transaction = prioritized_tranfers(
                &Keypair::new(),
                [Pubkey::new_unique()],
                1_000,
                u64::from(seq_id),
            );
            container
                .insert_new_batch(
                    smallvec::smallvec![(transaction, MaxAge::MAX)],
                    u64::MAX - fifo_index as u64,
                    revert_on_error,
                    bank.slot(),
                    seq_id,
                )
                .unwrap();
        }
        test.scheduler
            .receive_completed(
                &mut container,
                &BufferedPacketsDecision::Consume(bank.clone()),
            )
            .unwrap();

        test.scheduler.schedule(&mut container, 0).unwrap();
        assert_eq!(container.queue_size(), 2);
        assert!(test.consume_work_receivers[0].try_recv().is_err());
        assert!(test.response_receiver.try_recv().is_err());

        // A stale Consume decision must not dispatch through the no-bank replacement gap.
        shared_leader_state.set_bank_replacement();
        assert_eq!(
            test.scheduler
                .schedule(&mut container, 0)
                .unwrap()
                .num_scheduled,
            0
        );
        assert_eq!(container.queue_size(), 2);

        // A held controller iteration preserves work through resolution.
        set_leader_bank(&mut shared_leader_state, Some(bank.clone()));
        test.scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert_eq!(test.scheduler.slot, Some(bank.slot()));
        assert_eq!(container.queue_size(), 2);

        test.scheduler.schedule(&mut container, 0).unwrap();
        assert_eq!(container.queue_size(), 0);
        let atomic_work = test.consume_work_receivers[0].try_recv().unwrap();
        let non_atomic_work = test.consume_work_receivers[0].try_recv().unwrap();
        assert!(atomic_work.revert_on_error);
        assert!(!non_atomic_work.revert_on_error);
        assert!(test.consume_work_receivers[0].try_recv().is_err());
        assert!(test.response_receiver.try_recv().is_err());
    }

    #[test]
    fn test_scheduler_validates_selected_leader_bank() {
        let (bank_forks, mint) = test_bank_forks();
        let bank = Arc::new(Bank::new_from_parent(
            bank_forks.read().unwrap().working_bank(),
            solana_leader_schedule::SlotLeader::new_unique(),
            1,
        ));
        bank.register_unique_recent_blockhash_for_test();
        let mut test = create_test_scheduler(1, &bank_forks);
        set_leader_bank(&mut test.scheduler.shared_leader_state, Some(bank.clone()));
        let transaction = solana_system_transaction::transfer(
            &mint,
            &Pubkey::new_unique(),
            1,
            bank.last_blockhash(),
        );
        let mut container = TransactionStateContainer::with_capacity(10);
        container
            .insert_new_batch(
                smallvec::smallvec![(
                    RuntimeTransaction::from_transaction_for_tests(transaction),
                    MaxAge::MAX
                )],
                u64::MAX,
                false,
                bank.slot(),
                1,
            )
            .unwrap();
        test.scheduler
            .receive_completed(
                &mut container,
                &BufferedPacketsDecision::Consume(bank.clone()),
            )
            .unwrap();
        // BankForks still exposes a different bank which cannot validate this blockhash.
        assert_ne!(
            bank.bank_id(),
            bank_forks.read().unwrap().working_bank().bank_id()
        );
        assert_eq!(
            test.scheduler
                .schedule(&mut container, 0)
                .unwrap()
                .num_scheduled,
            1
        );
        assert_eq!(
            test.consume_work_receivers[0]
                .try_recv()
                .unwrap()
                .max_schedule_slot,
            Some(bank.slot())
        );
        assert!(test.response_receiver.try_recv().is_err());
    }

    #[test]
    fn test_scheduler_basic() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler {
            mut scheduler,
            consume_work_receivers,
            finished_consume_work_sender,
            mut response_receiver,
            ..
        } = create_test_scheduler(4, &bank_forks);
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

        let decision = BufferedPacketsDecision::Consume(bank_forks.read().unwrap().working_bank());

        // Init scheduler with bank start info
        scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        assert!(
            scheduler.slot.is_some(),
            "Scheduler slot should be set after receiving bank start"
        );

        // A stale Consume decision must not dispatch without an active leader Bank.
        scheduler.shared_leader_state = SharedLeaderState::new(0, None, None);
        assert_eq!(
            scheduler.schedule(&mut container, 0).unwrap().num_scheduled,
            0
        );
        assert!(consume_work_receivers[0].try_recv().is_err());
        set_leader_bank(
            &mut scheduler.shared_leader_state,
            Some(decision.bank().unwrap().clone()),
        );

        // Schedule the transactions
        let result = scheduler.schedule(&mut container, 0).unwrap();

        // Only two should have been scheduled as one is blocked
        assert_eq!(result.num_scheduled, 2);

        // Receive the scheduled work
        let work_1 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_1.ids.len(), 1);
        assert!(work_1.admission.is_some());
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
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
        let result = scheduler.schedule(&mut container, 0).unwrap();
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
        assert_eq!(bundle_result.seq_id, u32::MAX);
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
        let result = scheduler.schedule(&mut container, 0).unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler.schedule(&mut container, 0).unwrap();
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
        assert_eq!(bundle_result.seq_id, 0);
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
    fn test_prio_graph_clears_on_slot_boundary() {
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler { mut scheduler, .. } = create_test_scheduler(4, &bank_forks);
        scheduler.extra_checks_enabled = false;

        let keypair_a = Keypair::new();
        let keypair_b = Keypair::new();

        let bank = bank_forks.read().unwrap().working_bank();

        // Set initial slot with bank start
        let mut container = create_container(vec![(
            &keypair_a,
            vec![Pubkey::new_unique()],
            1000,
            0,
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
            (&keypair_a, vec![Pubkey::new_unique()], 1000, 0, u64::MAX),
            (&keypair_b, vec![Pubkey::new_unique()], 2000, 1, u64::MAX),
        ]);
        scheduler.pull_into_prio_graph(&mut container, &bank);
        assert!(
            !scheduler.prio_graph.is_empty(),
            "Prio graph should have transactions"
        );

        set_leader_bank(&mut scheduler.shared_leader_state, Some(bank));
        scheduler.shared_leader_state.set_bank_replacement();
        scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();
        assert!(!scheduler.prio_graph.is_empty());

        // Simulate slot boundary change by changing to no bank (None)
        let decision_no_bank = BufferedPacketsDecision::Forward;
        scheduler
            .receive_completed(&mut container, &decision_no_bank)
            .unwrap();

        assert_eq!(scheduler.slot, None);
        assert!(scheduler.prio_graph.is_empty());
        assert!(container.is_empty());
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
        let (bank_forks, _) = test_bank_forks();
        let TestScheduler { mut scheduler, .. } = create_test_scheduler(4, &bank_forks);
        scheduler.extra_checks_enabled = false;

        let bank = bank_forks.read().unwrap().working_bank();

        // Set the scheduler's slot via a Consume decision.
        let mut slot_container = create_container(vec![(
            &Keypair::new(),
            vec![Pubkey::new_unique()],
            1000,
            0,
            u64::MAX,
        )]);
        scheduler
            .receive_completed(
                &mut slot_container,
                &BufferedPacketsDecision::Consume(bank.clone()),
            )
            .unwrap();
        assert_eq!(scheduler.slot, Some(bank.slot()));

        // One batch, two transactions sharing a writable account: both are
        // signed by `keypair_a`, so both write its fee-payer account (index 0).
        let keypair_a = Keypair::new();
        let priority = u64::MAX;
        let mut txns_max_age: SmallVec<
            [(RuntimeTransaction<SanitizedTransaction>, MaxAge); MAX_PACKETS_PER_BUNDLE],
        > = SmallVec::new();
        txns_max_age.push((
            prioritized_tranfers(&keypair_a, vec![Pubkey::new_unique()], 1000, priority),
            MaxAge::MAX,
        ));
        txns_max_age.push((
            prioritized_tranfers(&keypair_a, vec![Pubkey::new_unique()], 2000, priority),
            MaxAge::MAX,
        ));

        let mut container = TransactionStateContainer::with_capacity(10 * 1024);
        container.insert_new_batch(txns_max_age, priority, false, u64::MAX, 0);

        // Must not panic; the bundle becomes a single schedulable node.
        scheduler.pull_into_prio_graph(&mut container, &bank);

        assert!(
            !scheduler.prio_graph.is_empty(),
            "bundle sharing a writable account should be inserted and schedulable"
        );

        // A dispatch-time recheck rejects the default blockhash without reserving any cost.
        set_leader_bank(&mut scheduler.shared_leader_state, Some(bank.clone()));
        scheduler.extra_checks_enabled = true;
        assert_eq!(scheduler.send_to_workers(&mut container, &bank).unwrap(), 0);
        assert!(scheduler.prio_graph.is_empty());
        assert_eq!(container.buffer_size(), 0);
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
    }

    impl TestScheduler {
        fn schedule(&mut self, container: &mut TransactionStateContainer<Tx>) -> usize {
            self.scheduler.schedule(container, 0).unwrap().num_scheduled
        }

        fn receive_completed(
            &mut self,
            container: &mut TransactionStateContainer<Tx>,
            decision: &BufferedPacketsDecision,
        ) {
            self.scheduler
                .receive_completed(container, decision)
                .unwrap();
        }
    }

    fn insert_admission_batch(
        container: &mut TransactionStateContainer<Tx>,
        transactions: impl IntoIterator<Item = Tx>,
        seq_id: u32,
    ) {
        let transactions: SmallVec<_> = transactions
            .into_iter()
            .map(|tx| (tx, MaxAge::MAX))
            .collect();
        let revert_on_error = transactions.len() > 1;
        container
            .insert_new_batch(
                transactions,
                u64::MAX - u64::from(seq_id),
                revert_on_error,
                u64::MAX,
                seq_id,
            )
            .unwrap();
    }

    fn set_block_cost_limit(bank: &Bank, block_cost: u64) {
        bank.write_cost_tracker()
            .unwrap()
            .set_limits(CostTrackerLimits::new(u64::MAX, block_cost, u64::MAX));
    }

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
                extra_info: Some(FinishedConsumeWorkExtraInfo { processed_results }),
            })
            .unwrap();
        test.receive_completed(container, decision);
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

    fn admission_scheduler() -> (TestScheduler, Arc<Bank>) {
        let (bank_forks, _) = test_bank_forks();
        let mut test = create_test_scheduler(1, &bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let bank = Arc::new(Bank::new_from_parent(
            bank_forks.read().unwrap().working_bank(),
            SlotLeader::new_unique(),
            1,
        ));
        set_leader_bank(&mut test.scheduler.shared_leader_state, Some(bank.clone()));
        (test, bank)
    }

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
            insert_admission_batch(
                &mut container,
                (0..size).map(|_| {
                    prioritized_tranfers(&Keypair::new(), [Pubkey::new_unique()], 1000, 0)
                }),
                seq_id as u32,
            );
        }
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.receive_completed(&mut container, &decision);
        (test, container, bank, decision)
    }

    /// JSA-72: later cheap work must wait for the earlier high-CU reservation to settle.
    #[test]
    fn test_jsa72_poc_priority_survives_inverted_worker_timing() {
        let (mut test, bank) = admission_scheduler();

        // Match the reported transaction shapes with independent payers and recipients.
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

        // The lower-priority reservation would reject the earlier work at this limit.
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
        for (seq_id, transaction) in [high_transaction, low_transaction].into_iter().enumerate() {
            insert_admission_batch(&mut container, [transaction], seq_id as u32);
        }
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.receive_completed(&mut container, &decision);

        test.schedule(&mut container);
        let mut high_work = test.consume_work_receivers[0].try_recv().unwrap();
        let high_info = &test.scheduler.inflight_batch_info[&high_work.batch_id];
        assert_eq!(high_info.batch_priority_ids[0].1, 0);
        assert!(!test.scheduler.pending_admission.is_empty());
        assert!(test.scheduler.prio_graph.is_empty());
        assert_eq!(block_cost_and_in_flight(&bank), (high_estimate, 1));
        assert!(test.consume_work_receivers[0].try_recv().is_err());
        assert_eq!(
            test.schedule(&mut container),
            0,
            "a pending batch must not spin or retry before a completion"
        );

        // Settle explicitly below the estimate, without sleeps or worker timing dependencies.
        settle_committed(&bank, &mut high_work, 150);
        let settled_high_cost = bank.read_cost_tracker().unwrap().block_cost();
        assert!(settled_high_cost + low_estimate <= high_estimate);
        finish_committed(&mut test, &mut container, &decision, high_work, 150);
        let (seq_id, result) = next_result(&mut test.response_receiver);
        assert_eq!(seq_id, 0);
        assert!(matches!(result, Committed(_)));

        // Only after the earlier reservation settles can the lower-priority work be dispatched.
        test.schedule(&mut container);
        let mut low_work = test.consume_work_receivers[0].try_recv().unwrap();
        let low_info = &test.scheduler.inflight_batch_info[&low_work.batch_id];
        assert_eq!(low_info.batch_priority_ids[0].1, 1);
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
    #[test_case::test_case(2; "atomic_prefix_rollback")]
    fn test_deferred_batch_is_final_once_inflight_settles_without_freeing_budget(
        second_batch_size: usize,
    ) {
        let (mut test, mut container, bank, decision) = setup_two_batches(second_batch_size);
        let estimate = estimated_cost(&bank);
        set_block_cost_limit(&bank, estimate * second_batch_size as u64 + estimate / 2);

        test.schedule(&mut container);
        let mut work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert!(!test.scheduler.pending_admission.is_empty());
        // B must roll back any earlier admissions in its batch without touching A's reservation.
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));
        assert!(test.consume_work_receivers[0].try_recv().is_err());

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
        test.schedule(&mut container);
        assert!(test.scheduler.pending_admission.is_empty());
        let work_b = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(
            work_b.admission.as_ref().unwrap().1.as_slice(),
            std::iter::repeat_n(
                Err(TransactionError::CommitCancelled),
                second_batch_size - 1,
            )
            .chain([Err(TransactionError::WouldExceedMaxBlockCostLimit)])
            .collect_vec()
        );
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 0));
        test.scheduler.recycle_work_object(work_b);
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 0));
    }

    #[test_case::test_case(false, true; "feasible_atomic_waits")]
    #[test_case::test_case(true, true; "impossible_atomic_releases_capacity")]
    #[test_case::test_case(true, false; "partial_batch_waits")]
    fn test_atomic_admission_preserves_priority_and_releases_failed_costs(
        impossible_head: bool,
        revert_on_error: bool,
    ) {
        let (mut test, bank) = admission_scheduler();
        let estimate = estimated_cost(&bank);
        let transfer = |outputs| {
            prioritized_tranfers(
                &Keypair::new(),
                (0..outputs).map(|_| Pubkey::new_unique()),
                1000,
                0,
            )
        };
        // The head fits the whole block limit, but settled work leaves only 3 estimates
        // available even after every earlier reservation is refunded.
        let settled = transfer(5);
        let settled_costs = QosService::compute_transaction_costs(
            &bank.feature_set,
            std::iter::once(&settled),
            std::iter::once(Ok(())),
        );
        let settled_cost = settled_costs[0].as_ref().unwrap();
        let baseline = settled_cost.sum();
        assert!(baseline > estimate);
        set_block_cost_limit(&bank, baseline + 3 * estimate);
        bank.write_cost_tracker()
            .unwrap()
            .try_add(settled_cost)
            .unwrap();
        let mut container = TransactionStateContainer::with_capacity(8);
        insert_admission_batch(&mut container, [transfer(1)], 0);
        container
            .insert_new_batch(
                [
                    transfer(1),
                    transfer(if impossible_head { 5 } else { 1 }),
                    transfer(1),
                ]
                .into_iter()
                .map(|tx| (tx, MaxAge::MAX))
                .collect(),
                u64::MAX - 1,
                revert_on_error,
                u64::MAX,
                1,
            )
            .unwrap();
        insert_admission_batch(&mut container, [transfer(1)], 2);
        test.receive_completed(
            &mut container,
            &BufferedPacketsDecision::Consume(bank.clone()),
        );

        let should_defer = !impossible_head || !revert_on_error;
        assert_eq!(
            test.schedule(&mut container),
            if should_defer { 1 } else { 5 }
        );
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        if should_defer {
            // A refund could admit the whole atomic head at equality, or part of a non-atomic
            // head. Independent C fits now, but must not take priority over that retry.
            assert_eq!(block_cost_and_in_flight(&bank), (baseline + estimate, 1));
            assert_eq!(test.scheduler.pending_admission.len(), 1);
            assert_eq!(test.schedule(&mut container), 0);
            assert!(test.consume_work_receivers[0].try_recv().is_err());
        } else {
            let work_b = test.consume_work_receivers[0].try_recv().unwrap();
            let work_c = test.consume_work_receivers[0].try_recv().unwrap();
            assert_eq!(
                test.scheduler.inflight_batch_info[&work_b.batch_id].batch_priority_ids[0].1,
                1
            );
            assert_eq!(
                test.scheduler.inflight_batch_info[&work_c.batch_id].batch_priority_ids[0].1,
                2
            );
            assert_eq!(
                work_b.admission.as_ref().unwrap().1.as_slice(),
                &[
                    Err(TransactionError::CommitCancelled),
                    Err(TransactionError::WouldExceedMaxBlockCostLimit),
                    Err(TransactionError::CommitCancelled),
                ]
            );
            assert_eq!(work_c.admission.as_ref().unwrap().1.as_slice(), &[Ok(())]);
            assert!(test.scheduler.pending_admission.is_empty());
            // B's accepted prefix AND suffix were released before C was admitted, while A
            // is still outstanding. Returning B must not release either A's or C's cost.
            test.scheduler.recycle_work_object(work_b);
            assert_eq!(
                block_cost_and_in_flight(&bank),
                (baseline + 2 * estimate, 2)
            );
            test.scheduler.recycle_work_object(work_c);
        }
        test.scheduler.recycle_work_object(work_a);
        assert_eq!(block_cost_and_in_flight(&bank), (baseline, 0));
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
        test.receive_completed(&mut container, &decision);

        test.schedule(&mut container);
        test.consume_work_receivers[0].try_recv().unwrap();
        assert!(!test.scheduler.pending_admission.is_empty());

        // Slot ends: the deferred batch and the batch it was blocking both go back to BAM.
        test.receive_completed(&mut container, &BufferedPacketsDecision::Forward);
        assert!(test.scheduler.pending_admission.is_empty());
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

    #[test_case::test_case(false; "repeated_replacement")]
    #[test_case::test_case(true; "slot_boundary")]
    fn test_returned_admissions_preserve_order_and_deferred_head(end_slot: bool) {
        let (mut test, mut container, bank_a, _) = setup_two_batches(1);
        let estimate = estimated_cost(&bank_a);
        set_block_cost_limit(&bank_a, estimate * 2);
        insert_admission_batch(
            &mut container,
            [prioritized_tranfers(
                &Keypair::new(),
                [Pubkey::new_unique()],
                1000,
                0,
            )],
            2,
        );
        assert_eq!(test.schedule(&mut container), 2);
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        let work_b = test.consume_work_receivers[0].try_recv().unwrap();
        let (&deferred_id, _) = test.scheduler.pending_admission.first_key_value().unwrap();
        let batch_ids = [work_a.batch_id.0, work_b.batch_id.0, deferred_id];
        assert!(test.consume_work_receivers[0].try_recv().is_err());

        let return_work = |test: &mut TestScheduler, work: ConsumeWork<Tx>| {
            let retryable_indexes = (0..work.transactions.len())
                .map(|index| RetryableIndex::new(index, true))
                .collect();
            test.finished_consume_work_sender
                .send(FinishedConsumeWork {
                    work,
                    retryable_indexes,
                    extra_info: None,
                })
                .unwrap();
        };
        // The later worker returns in the replacement gap; A still holds its old admission.
        test.scheduler.shared_leader_state.set_bank_replacement();
        return_work(&mut test, work_b);
        test.receive_completed(&mut container, &BufferedPacketsDecision::Hold);
        assert_eq!(block_cost_and_in_flight(&bank_a), (estimate, 1));
        assert_eq!(test.schedule(&mut container), 0);

        let bank_b = Arc::new(Bank::new_from_parent(
            bank_a.parent().unwrap(),
            SlotLeader::new_unique(),
            bank_a.slot(),
        ));
        set_block_cost_limit(&bank_b, estimate);
        set_leader_bank(
            &mut test.scheduler.shared_leader_state,
            Some(bank_b.clone()),
        );
        assert_eq!(test.schedule(&mut container), 0);
        return_work(&mut test, work_a);
        let decision = BufferedPacketsDecision::Consume(bank_b.clone());
        test.receive_completed(&mut container, &decision);
        assert_eq!(block_cost_and_in_flight(&bank_a), (0, 0));
        assert_eq!(test.scheduler.pending_admission.len(), 3);
        assert!(test.response_receiver.try_recv().is_err());

        if end_slot {
            test.receive_completed(&mut container, &BufferedPacketsDecision::Forward);
            assert!(test.scheduler.pending_admission.is_empty());
            assert!(test.scheduler.prio_graph.is_empty());
            assert_eq!(container.buffer_size(), 0);
            for expected_seq_id in 0..3 {
                let (seq_id, result) = next_result(&mut test.response_receiver);
                assert_eq!(seq_id, expected_seq_id);
                assert!(matches!(
                    result,
                    NotCommitted(not_committed) if not_committed.reason == Some(
                        Reason::SchedulingError(SchedulingError::OutsideLeaderSlot as i32)
                    )
                ));
            }
            assert!(test.response_receiver.try_recv().is_err());
            return;
        }

        assert_eq!(test.schedule(&mut container), 1);
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_a.batch_id.0, batch_ids[0]);
        assert!(test.consume_work_receivers[0].try_recv().is_err());
        assert_eq!(test.scheduler.pending_admission.len(), 2);

        // Replace again while A is dispatched, B is deferred, and C retains its original key.
        let bank_c = Arc::new(Bank::new_from_parent(
            bank_a.parent().unwrap(),
            SlotLeader::new_unique(),
            bank_a.slot(),
        ));
        set_block_cost_limit(&bank_c, estimate * 3);
        set_leader_bank(
            &mut test.scheduler.shared_leader_state,
            Some(bank_c.clone()),
        );
        return_work(&mut test, work_a);
        let decision = BufferedPacketsDecision::Consume(bank_c.clone());
        test.receive_completed(&mut container, &decision);
        assert_eq!(block_cost_and_in_flight(&bank_b), (0, 0));
        assert_eq!(test.schedule(&mut container), 3);
        for (expected_seq_id, batch_id) in batch_ids.into_iter().enumerate() {
            let mut work = test.consume_work_receivers[0].try_recv().unwrap();
            assert_eq!(work.batch_id.0, batch_id);
            let (owner, results) = work.admission.as_ref().unwrap();
            assert_eq!(owner.bank_id(), bank_c.bank_id());
            assert_eq!(results.as_slice(), &[Ok(())]);
            settle_committed(&bank_c, &mut work, 150);
            finish_committed(&mut test, &mut container, &decision, work, 150);
            let (seq_id, result) = next_result(&mut test.response_receiver);
            assert_eq!(seq_id as usize, expected_seq_id);
            assert!(matches!(result, Committed(_)));
        }
        assert!(test.scheduler.pending_admission.is_empty());
        assert!(!test.scheduler.has_in_flight_transactions());
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        assert_eq!(block_cost_and_in_flight(&bank_c).1, 0);
        assert_eq!(container.buffer_size(), 0);
        assert!(test.response_receiver.try_recv().is_err());
    }

    #[test]
    fn test_bank_replacement_within_slot_restarts_admission_on_new_bank() {
        let (mut test, mut container, bank_1, _) = setup_two_batches(1);
        let estimate = estimated_cost(&bank_1);
        // A failed preparation's deadline holds work without popping or reserving it.
        test.scheduler.tip_retry_at =
            Some((bank_1.bank_id(), Instant::now() + Duration::from_secs(60)));
        assert_eq!(test.schedule(&mut container), 0);
        assert_eq!(block_cost_and_in_flight(&bank_1), (0, 0));
        assert!(test.consume_work_receivers[0].try_recv().is_err());
        test.scheduler.tip_retry_at = Some((bank_1.bank_id(), Instant::now()));
        test.schedule(&mut container);
        assert!(test.scheduler.tip_retry_at.is_none());
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        let work_b = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(block_cost_and_in_flight(&bank_1), (estimate * 2, 2));

        // The bankless handover clears the graph. Work arriving in the gap must remain queued.
        test.receive_completed(&mut container, &BufferedPacketsDecision::Forward);
        assert_eq!(test.scheduler.slot, None);
        let transaction = prioritized_tranfers(&Keypair::new(), [Pubkey::new_unique()], 1000, 0);
        insert_admission_batch(&mut container, [transaction], 2);

        // ParentReady installs a replacement for the same slot. Do not adopt it or dispatch C
        // until both old-bank batches have returned.
        let bank_1b = Arc::new(Bank::new_from_parent(
            bank_1.parent().unwrap(),
            SlotLeader::new_unique(),
            bank_1.slot(),
        ));
        assert_ne!(bank_1b.bank_id(), bank_1.bank_id());
        set_leader_bank(
            &mut test.scheduler.shared_leader_state,
            Some(bank_1b.clone()),
        );
        let decision = BufferedPacketsDecision::Consume(bank_1b.clone());
        // Old work returned with its admission attached must drain before C can dispatch.
        for (work, remaining) in [(work_a, 1), (work_b, 0)] {
            test.receive_completed(&mut container, &decision);
            test.schedule(&mut container);
            assert!(test.consume_work_receivers[0].try_recv().is_err());
            assert_eq!(test.scheduler.slot, None);
            finish_committed(&mut test, &mut container, &decision, work, 150);
            assert_eq!(
                block_cost_and_in_flight(&bank_1),
                (estimate * remaining as u64, remaining)
            );
        }

        // With both old-bank batches drained, adopt the new bank and admit C.
        // The previous BankId's retry deadline must not delay replacement preparation.
        test.scheduler.tip_retry_at =
            Some((bank_1.bank_id(), Instant::now() + Duration::from_secs(60)));
        test.receive_completed(&mut container, &decision);
        test.schedule(&mut container);
        let work_c = test.consume_work_receivers[0].try_recv().unwrap();
        let admission = work_c.admission.as_ref().unwrap();
        assert_eq!(admission.0.bank_id(), bank_1b.bank_id());
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(block_cost_and_in_flight(&bank_1b), (estimate, 1));
        assert!(test.scheduler.tip_retry_at.is_none());
    }

    #[test_case::test_case(false; "returned_work")]
    #[test_case::test_case(true; "disconnected_worker")]
    fn test_unprocessed_work_releases_reservation_on_its_bank(disconnected: bool) {
        let (mut test, mut container, bank, decision) = setup_two_batches(1);
        let estimate = estimated_cost(&bank);
        set_block_cost_limit(&bank, estimate);

        if disconnected {
            drop(test.consume_work_receivers);
            assert!(matches!(
                test.scheduler.schedule(&mut container, 0),
                Err(super::SchedulerError::DisconnectedSendChannel(_))
            ));
            assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
            assert_eq!(test.scheduler.inflight_reserved_cost, 0);
            assert!(!test.scheduler.has_in_flight_transactions());
            return;
        }

        test.schedule(&mut container);
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));

        // The worker found the bank complete and returned A untouched, admission attached.
        test.finished_consume_work_sender
            .send(FinishedConsumeWork {
                work: work_a,
                retryable_indexes: vec![RetryableIndex::new(0, true)],
                extra_info: Some(FinishedConsumeWorkExtraInfo {
                    processed_results: vec![TransactionResult::NotCommitted(
                        NotCommittedReason::PohTimeout,
                    )],
                }),
            })
            .unwrap();
        test.receive_completed(&mut container, &decision);
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
