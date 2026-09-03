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
            qos_service::{CostAdmissionAttempt, QosService},
            scheduler_messages::{
                ConsumeWork, FinishedConsumeWork, NotCommittedReason, TransactionBatchId,
                TransactionId, TransactionResult,
            },
            transaction_scheduler::{
                bam_utils::convert_txn_error_to_proto, scheduler_common::SchedulingCommon,
            },
        },
    },
    crossbeam_channel::{Receiver, SendError, Sender},
    histogram::Histogram,
    jito_protos::proto::bam_types::{
        SchedulingError, atomic_txn_batch_result, not_committed::Reason,
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
    solana_svm_transaction::svm_message::SVMMessage,
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

#[inline(always)]
fn passthrough_priority(
    id: &TransactionPriorityId,
    _graph_node: &GraphNode<TransactionPriorityId>,
) -> TransactionPriorityId {
    *id
}

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

    // Cost admission runs here, in pop order, instead of in the eight workers (JSA-72).
    // See `try_dispatch`.
    /// Bank whose cost tracker holds the current reservations.
    admission_bank_id: Option<BankId>,
    /// Estimated cost reserved on `admission_bank_id` by dispatched work that has not settled.
    inflight_reserved_cost: u64,
    /// A popped batch held at the head of the line because its block-limit rejection could be
    /// covered by inflight work settling. Nothing is dispatched while it is set.
    pending_admission: Option<TransactionPriorityId>,
    /// A completion arrived since `pending_admission` was last attempted.
    retry_pending_admission: bool,
    admission_us: Histogram,
    num_deferred_admissions: u64,
    /// Reservations released here because the work came back with its admission unsettled.
    num_released_reservations: u64,
}

// A structure to hold information about inflight batches.
// A batch can either be one 'revert_on_error' batch or multiple
// 'non-revert_on_error' batches that are scheduled together.
struct InflightBatchInfo {
    pub schedule_time: Instant,
    // SmallVec 1: each scheduled work item typically corresponds to one batch id.
    pub batch_priority_ids: SmallVec<[(TransactionPriorityId, u32 /* seq_id */); 1]>,
    pub slot: Slot,
    /// Bank the scheduler reserved cost on, if it did. Held so the reservation can be released
    /// if the work comes back unsettled.
    pub bank: Option<Arc<Bank>>,
    /// Estimated cost reserved for this work; freed for deferred batches on completion.
    pub reserved_cost: u64,
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
            prio_graph: PrioGraph::new(passthrough_priority),
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
            admission_bank_id: None,
            inflight_reserved_cost: 0,
            pending_admission: None,
            retry_pending_admission: false,
            admission_us: Histogram::new(),
            num_deferred_admissions: 0,
            num_released_reservations: 0,
        }
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
    fn pull_into_prio_graph<S: StateContainer<Tx>>(&mut self, container: &mut S) {
        let Some(slot) = self.slot else {
            warn!("Slot is not set, cannot pull transactions into prio-graph");
            return;
        };

        let working_bank = self.bank_forks.read().unwrap().working_bank();
        let atomic_batches_enabled = self.shared_leader_state.load().atomic_batches_enabled();
        self.deferred_atomic_batches.clear();

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

        // Atomic batches must wait until ParentReady confirms or replaces the provisional bank.
        // Non-atomic batches can still be rescheduled after sad handover.
        container.push_ids_into_queue(self.deferred_atomic_batches.drain(..));
    }

    fn send_to_workers(
        &mut self,
        container: &mut impl StateContainer<Tx>,
        num_scheduled: &mut usize,
    ) -> Result<(), SchedulerError> {
        let Some(slot) = self.slot else {
            warn!("Slot is not set, cannot schedule transactions");
            return Ok(());
        };

        let now = Instant::now();
        let working_bank = self.bank_forks.read().unwrap().working_bank();
        let admission_bank = self.admission_bank(slot);

        // A deferred batch holds the head of the line until work on its bank settles or the
        // bank itself changes; either way it gets the next attempt before anything else.
        if let Some(id) = self.pending_admission {
            let bank_changed =
                admission_bank.as_ref().map(|bank| bank.bank_id()) != self.admission_bank_id;
            if !self.retry_pending_admission && !bank_changed {
                return Ok(());
            }
            self.pending_admission = None;
            if !self.try_dispatch(
                id,
                container,
                &working_bank,
                admission_bank.as_ref(),
                slot,
                now,
                num_scheduled,
            )? {
                return Ok(());
            }
        }

        while let Some(id) = self.prio_graph.pop() {
            if !self.try_dispatch(
                id,
                container,
                &working_bank,
                admission_bank.as_ref(),
                slot,
                now,
                num_scheduled,
            )? {
                return Ok(());
            }
        }
        Ok(())
    }

    /// The bank workers will execute `slot`'s work on, if the leader is active on it.
    fn admission_bank(&self, slot: Slot) -> Option<Arc<Bank>> {
        active_leader_state(&self.shared_leader_state)
            .and_then(|leader_state| leader_state.working_bank().cloned())
            .filter(|bank| bank.slot() == slot)
    }

    /// Filters, cost-admits, and dispatches one batch popped from the prio-graph.
    ///
    /// Returns `Ok(true)` when the batch left the head of the line, either dispatched or dropped
    /// with a result, and `Ok(false)` when it was deferred and now holds the head of the line.
    #[allow(clippy::too_many_arguments)]
    fn try_dispatch(
        &mut self,
        id: TransactionPriorityId,
        container: &mut impl StateContainer<Tx>,
        working_bank: &Arc<Bank>,
        admission_bank: Option<&Arc<Bank>>,
        slot: Slot,
        now: Instant,
        num_scheduled: &mut usize,
    ) -> Result<bool, SchedulerError> {
        let (batch_ids, revert_on_error, max_schedule_slot, seq_id) =
            container.get_batch(id.id).unwrap();
        let num_transactions = batch_ids.len();

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
            return Ok(true);
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
                return Ok(true);
            };
        }

        // Admit cost here, in pop order, so eight workers racing for the cost tracker cannot
        // reorder it. Without an active bank the worker takes its existing path and reports
        // the existing could-not-process result.
        let admission = match admission_bank {
            Some(bank) => {
                let (attempt, admission_us) =
                    measure_us!(self.try_admit(bank, batch_ids, container));
                let _ = self.admission_us.increment(admission_us);
                match attempt {
                    CostAdmissionAttempt::Admitted(admission) => Some(admission),
                    CostAdmissionAttempt::Deferred { shortfall } => {
                        debug!(
                            "slot {slot}, batch seq_id {seq_id}: block limit short by {shortfall} \
                             with {} reserved in flight; deferring admission",
                            self.inflight_reserved_cost
                        );
                        self.num_deferred_admissions += 1;
                        self.pending_admission = Some(id);
                        self.retry_pending_admission = false;
                        return Ok(false);
                    }
                }
            }
            None => None,
        };

        // Schedule it
        let mut work = self.get_or_create_work_object();
        let batch_id = self.get_next_schedule_id();
        *num_scheduled += num_transactions;
        Self::populate_consume_work(&mut work, batch_id, &[id], revert_on_error, container, slot);
        work.admission = admission;
        self.send_to_worker(SmallVec::from([(id, seq_id)]), work, slot, admission_bank)?;
        Ok(true)
    }

    /// Runs the cost model for one buffered batch on `bank` and reserves what fits, in order.
    fn try_admit(
        &mut self,
        bank: &Arc<Bank>,
        batch_ids: &[TransactionId],
        container: &impl StateContainer<Tx>,
    ) -> CostAdmissionAttempt {
        // Reservations live on one bank. When the bank changes, nothing inflight can free
        // budget on the new one, so the deferral accounting starts over.
        if self.admission_bank_id != Some(bank.bank_id()) {
            self.admission_bank_id = Some(bank.bank_id());
            self.inflight_reserved_cost = 0;
        }

        let mut txns: SmallVec<[&Tx; MAX_PACKETS_PER_BUNDLE]> = SmallVec::new();
        let mut pre_results: SmallVec<[Result<(), TransactionError>; MAX_PACKETS_PER_BUNDLE]> =
            SmallVec::new();
        for txn_id in batch_ids {
            let (Some(txn), Some(max_age)) = (
                container.get_transaction(*txn_id),
                container.get_transaction_max_age(*txn_id),
            ) else {
                continue;
            };
            // Same gate the worker's local admission applies.
            pre_results.push(bank.resanitize_transaction_minimally(
                txn,
                max_age.sanitized_epoch,
                max_age.alt_invalidation_slot,
            ));
            txns.push(txn);
        }

        QosService::try_admit_transactions(
            bank,
            txns.iter().copied(),
            pre_results.into_iter(),
            self.inflight_reserved_cost,
        )
    }

    fn send_to_worker(
        &mut self,
        // SmallVec 1: scheduler currently sends a single batch id per work item.
        priority_ids: SmallVec<[(TransactionPriorityId, u32); 1]>,
        work: ConsumeWork<Tx>,
        slot: Slot,
        admission_bank: Option<&Arc<Bank>>,
    ) -> Result<(), SchedulerError> {
        let batch_id = work.batch_id;
        let (admitted_on, reserved_cost) = match (work.admission.as_ref(), admission_bank) {
            (Some(admission), Some(bank)) => {
                debug_assert_eq!(admission.bank_id, bank.bank_id());
                (Some(bank.clone()), admission.reserved_cost)
            }
            _ => (None, 0),
        };

        if let Err(SendError(work)) = self.consume_work_sender.send(work) {
            // Every worker receiver is gone, which only happens at shutdown. The work was never
            // enqueued, so nothing downstream will settle its reservation; release it here.
            if let (Some(admission), Some(bank)) = (work.admission.as_ref(), admitted_on.as_ref()) {
                QosService::release_admitted_costs(bank, work.transactions.iter(), admission);
            }
            return Err(SchedulerError::DisconnectedSendChannel(
                "bam consume work sender",
            ));
        }

        if admitted_on.is_some() {
            self.inflight_reserved_cost = self.inflight_reserved_cost.saturating_add(reserved_cost);
        }
        self.inflight_batch_info.insert(
            batch_id,
            InflightBatchInfo {
                schedule_time: Instant::now(),
                batch_priority_ids: priority_ids,
                slot,
                bank: admitted_on,
                reserved_cost,
            },
        );
        Ok(())
    }

    /// Releases the reservation of work that came back with its admission still attached.
    ///
    /// A worker takes the admission out once it has settled the reservation on the admission
    /// bank. It leaves it attached when it returns the work without executing it (the bank
    /// completed or was replaced before the work was dequeued) or when it executed on a
    /// replacement bank through the local path. Either way the estimate and the inflight count
    /// are still charged to the original bank and nothing else will release them. That bank is
    /// no longer the leader bank, so this only keeps its cost-tracker statistics exact; it never
    /// affects admission.
    fn release_unsettled_admission(
        &mut self,
        work: &ConsumeWork<Tx>,
        inflight_batch_info: Option<&InflightBatchInfo>,
    ) {
        let Some(admission) = work.admission.as_ref() else {
            return;
        };
        match inflight_batch_info.and_then(|info| info.bank.as_ref()) {
            Some(bank) if bank.bank_id() == admission.bank_id => {
                QosService::release_admitted_costs(bank, work.transactions.iter(), admission);
                self.num_released_reservations += 1;
            }
            _ => warn!(
                "batch {} returned an unsettled admission on bank {} without a matching inflight \
                 record; reservation not released",
                work.batch_id, admission.bank_id
            ),
        }
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
                target_slot: 0,
                max_schedule_slot: None,
                admission: None,
            }
        })
    }

    fn recycle_work_object(&mut self, mut work: ConsumeWork<Tx>) {
        // Just in case, clear the work object
        work.ids.clear();
        work.transactions.clear();
        work.max_ages.clear();
        work.admission = None;
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
        output.target_slot = slot;
        output.max_schedule_slot = Some(slot);
        output.respond_with_extra_info = true;
        output.admission = None;
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
        let bank_slot = decision.bank().map(|bank| bank.slot());
        if bank_slot == self.slot {
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
        // unblocked so the drain below reaches its dependents. Reservations belong to the
        // slot that just ended; start the deferral accounting over.
        if let Some(pending_id) = self.pending_admission.take() {
            self.prio_graph.unblock(&pending_id);
            if let Some((_, _, _, seq_id)) = container.get_batch(pending_id.id) {
                self.send_no_leader_slot_bundle_result(seq_id);
            }
            container.remove_by_id(pending_id.id);
        }
        self.retry_pending_admission = false;
        self.admission_bank_id = None;
        self.inflight_reserved_cost = 0;

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

        datapoint_info!(
            "bam_scheduler_admission_metrics",
            (
                "admission_us_p50",
                self.admission_us.percentile(50.0).unwrap_or_default(),
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
            ("num_deferred_admissions", self.num_deferred_admissions, i64),
            (
                "num_released_reservations",
                self.num_released_reservations,
                i64
            ),
        );
        self.admission_us.clear();
        self.num_deferred_admissions = 0;
        self.num_released_reservations = 0;
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

        let mut num_scheduled = 0;

        self.pull_into_prio_graph(container);
        self.send_to_workers(container, &mut num_scheduled)?;

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
            let inflight_batch_info = self.inflight_batch_info.remove(&batch_id);
            self.release_unsettled_admission(&work, inflight_batch_info.as_ref());
            self.recycle_work_object(work);

            let Some(inflight_batch_info) = inflight_batch_info else {
                continue;
            };

            // Settled work may have freed budget for a batch deferred on this bank's block limit.
            let bank_id = inflight_batch_info.bank.as_ref().map(|bank| bank.bank_id());
            if bank_id.is_some() && bank_id == self.admission_bank_id {
                self.inflight_reserved_cost = self
                    .inflight_reserved_cost
                    .saturating_sub(inflight_batch_info.reserved_cost);
            }
            if self.pending_admission.is_some() {
                self.retry_pending_admission = true;
            }

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
                    ConsumeWork, CostAdmission, FinishedConsumeWork, FinishedConsumeWorkExtraInfo,
                    MaxAge, NotCommittedReason, TransactionResult,
                },
                tests::create_slow_genesis_config,
                transaction_scheduler::{
                    bam_scheduler::{BamScheduler, MAX_PACKETS_PER_BUNDLE},
                    scheduler::Scheduler,
                    scheduler_error::SchedulerError,
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
        solana_cost_model::cost_tracker::CostTrackerLimits,
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
        solana_system_interface::instruction::transfer_many,
        solana_transaction::{Transaction, sanitized::SanitizedTransaction},
        solana_transaction_error::TransactionError,
        std::{
            borrow::Borrow,
            sync::{Arc, RwLock},
        },
    };

    type Tx = RuntimeTransaction<SanitizedTransaction>;

    struct TestScheduler {
        scheduler: BamScheduler<Tx>,
        consume_work_receivers: Vec<crossbeam_channel::Receiver<ConsumeWork<Tx>>>,
        finished_consume_work_sender: crossbeam_channel::Sender<FinishedConsumeWork<Tx>>,
        response_receiver: tokio::sync::mpsc::Receiver<BamOutboundMessage>,
        shared_leader_state: SharedLeaderState,
    }

    fn create_test_scheduler(
        num_threads: usize,
        bank_forks: &Arc<RwLock<BankForks>>,
    ) -> TestScheduler {
        let (consume_work_sender, consume_work_receiver) = unbounded();
        let (finished_consume_work_sender, finished_consume_work_receiver) = unbounded();
        let (response_sender, response_receiver) = tokio::sync::mpsc::channel(100);
        let shared_leader_state = SharedLeaderState::new(0, None, None);
        let scheduler = BamScheduler::new(
            consume_work_sender,
            finished_consume_work_receiver,
            response_sender,
            bank_forks.clone(),
            shared_leader_state.clone(),
        );
        TestScheduler {
            scheduler,
            consume_work_receivers: (0..num_threads)
                .map(|_| consume_work_receiver.clone())
                .collect(),
            finished_consume_work_sender,
            response_receiver,
            shared_leader_state,
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
            .set_limits(CostTrackerLimits {
                account_cost: u64::MAX,
                block_cost,
                allocated_data_size: u64::MAX,
            });
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

    fn committed_work(work: ConsumeWork<Tx>, cus_consumed: u32) -> FinishedConsumeWork<Tx> {
        let processed_results = work
            .transactions
            .iter()
            .map(|_| {
                TransactionResult::Committed(TransactionCommittedResult {
                    cus_consumed,
                    feepayer_balance_lamports: 0,
                    loaded_accounts_data_size: 0,
                    execution_success: true,
                })
            })
            .collect();
        FinishedConsumeWork {
            work,
            retryable_indexes: vec![],
            extra_info: Some(FinishedConsumeWorkExtraInfo { processed_results }),
        }
    }

    /// What `ConsumeWorker::retry` sends back for work it could not execute: every transaction
    /// retryable and reported as PohTimeout, admission untouched.
    fn unprocessed_work(work: ConsumeWork<Tx>) -> FinishedConsumeWork<Tx> {
        let retryable_indexes = (0..work.transactions.len())
            .map(|index| RetryableIndex::new(index, true))
            .collect();
        let processed_results = work
            .transactions
            .iter()
            .map(|_| TransactionResult::NotCommitted(NotCommittedReason::PohTimeout))
            .collect();
        FinishedConsumeWork {
            work,
            retryable_indexes,
            extra_info: Some(FinishedConsumeWorkExtraInfo { processed_results }),
        }
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

    fn assert_outside_leader_slot(result: atomic_txn_batch_result::Result) {
        let NotCommitted(not_committed) = result else {
            panic!("expected NotCommitted, got {result:?}");
        };
        assert_eq!(
            not_committed.reason,
            Some(Reason::SchedulingError(
                SchedulingError::OutsideLeaderSlot as i32
            ))
        );
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
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(result.num_scheduled, 0);
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

        // Schedule the transactions
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();

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
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(result.num_scheduled, 1);
        // Check that the remaining transaction is sent to the worker
        let work_2 = consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_2.ids.len(), 1);

        // Try scheduling; nothing should be scheduled as the remaining transaction is blocked
        let result = scheduler.schedule(&mut container, 0, 0).unwrap();
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
    #[should_panic(expected = "node must exist")]
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
            if let Some((batch_ids, _, _, _)) = container.get_batch(txn_id.id) {
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
        scheduler.pull_into_prio_graph(&mut container);

        assert!(
            !scheduler.prio_graph.is_empty(),
            "bundle sharing a writable account should be inserted and schedulable"
        );
    }

    // ---- scheduler-side cost admission (JSA-72) ----

    /// Two independent batches plus a bank the scheduler can admit on, with `slot` set.
    fn setup_two_batches(
        block_cost_limit: Option<u64>,
    ) -> (
        TestScheduler,
        TransactionStateContainer<Tx>,
        Arc<Bank>,
        BufferedPacketsDecision,
    ) {
        let (bank_forks, _) = test_bank_forks();
        let mut test = create_test_scheduler(4, &bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let bank = bank_forks.read().unwrap().working_bank();
        set_leader_bank(&mut test.shared_leader_state, &bank);
        if let Some(limit) = block_cost_limit {
            set_block_cost_limit(&bank, limit);
        }

        let mut container = create_container(vec![
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                1000,
                0,
                u64::MAX,
            ),
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                1000,
                1,
                u64::MAX,
            ),
        ]);
        let decision = BufferedPacketsDecision::Consume(bank.clone());
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        (test, container, bank, decision)
    }

    #[test]
    fn test_admission_reserves_in_pop_order_before_dispatch() {
        let (mut test, mut container, bank, _decision) = setup_two_batches(None);
        let estimate = estimated_cost(&bank);

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 2);

        // Both reservations exist before any worker has seen the work.
        assert_eq!(block_cost_and_in_flight(&bank), (2 * estimate, 2));
        assert_eq!(test.scheduler.inflight_reserved_cost, 2 * estimate);
        assert_eq!(test.scheduler.admission_bank_id, Some(bank.bank_id()));

        for expected_seq_id in [0u32, 1] {
            let work = test.consume_work_receivers[0].try_recv().unwrap();
            let admission = work.admission.as_ref().expect("scheduler admitted");
            assert_eq!(
                admission,
                &CostAdmission {
                    bank_id: bank.bank_id(),
                    results: vec![Ok(())],
                    reserved_cost: estimate,
                }
            );
            let info = &test.scheduler.inflight_batch_info[&work.batch_id];
            assert_eq!(
                info.bank.as_ref().map(|bank| bank.bank_id()),
                Some(bank.bank_id())
            );
            assert_eq!(info.reserved_cost, estimate);
            assert_eq!(info.batch_priority_ids[0].1, expected_seq_id);
        }
        assert!(test.consume_work_receivers[0].try_recv().is_err());
    }

    #[test]
    fn test_coverable_block_limit_failure_defers_until_completion_frees_budget() {
        // Room for one and a half batches: the second fails by half an estimate, which the first
        // batch's reservation covers once it settles.
        let (bank_forks, _) = test_bank_forks();
        let estimate = estimated_cost(&bank_forks.read().unwrap().working_bank());
        drop(bank_forks);
        let (mut test, mut container, bank, decision) =
            setup_two_batches(Some(estimate + estimate / 2));

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        let mut work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(
            work_a.admission.as_ref().map(|a| a.results.clone()),
            Some(vec![Ok(())])
        );
        assert!(test.consume_work_receivers[0].try_recv().is_err());

        // Deferred: nothing reserved for it, nothing dispatched behind it.
        assert!(test.scheduler.pending_admission.is_some());
        assert!(!test.scheduler.retry_pending_admission);
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(test.scheduler.num_deferred_admissions, 1);

        // Without a completion there is nothing to retry.
        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 0);
        assert!(test.consume_work_receivers[0].try_recv().is_err());

        // The worker commits A well under its estimate and settles the reservation.
        settle_committed(&bank, &mut work_a, 150);
        let (settled_cost, _) = block_cost_and_in_flight(&bank);
        assert!(settled_cost < estimate);
        test.finished_consume_work_sender
            .send(committed_work(work_a, 150))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        assert!(test.scheduler.retry_pending_admission);
        let (seq_id, result) = next_result(&mut test.response_receiver);
        assert_eq!(seq_id, 0);
        assert!(matches!(result, Committed(_)));

        // Retry admits B into the freed budget.
        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        assert!(test.scheduler.pending_admission.is_none());
        let work_b = test.consume_work_receivers[0].try_recv().unwrap();
        let admission = work_b.admission.as_ref().unwrap();
        assert_eq!(admission.results, vec![Ok(())]);
        assert_eq!(admission.reserved_cost, estimate);
        assert_eq!(admission.bank_id, bank.bank_id());
        assert_eq!(
            block_cost_and_in_flight(&bank),
            (settled_cost + estimate, 1)
        );
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
    }

    #[test]
    fn test_deferred_batch_is_final_once_inflight_settles_without_freeing_budget() {
        let (bank_forks, _) = test_bank_forks();
        let estimate = estimated_cost(&bank_forks.read().unwrap().working_bank());
        drop(bank_forks);
        let (mut test, mut container, bank, decision) =
            setup_two_batches(Some(estimate + estimate / 2));

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let mut work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert!(test.scheduler.pending_admission.is_some());

        // A commits at exactly its estimate: nothing is freed. The worker settled it.
        bank.write_cost_tracker()
            .unwrap()
            .sub_transactions_in_flight(1);
        work_a.admission = None;
        test.finished_consume_work_sender
            .send(committed_work(work_a, estimate as u32))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);

        // Nothing inflight can cover the shortfall any more, so B is dispatched with the final
        // per-transaction error, exactly as the worker would have produced it.
        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        assert!(test.scheduler.pending_admission.is_none());
        let work_b = test.consume_work_receivers[0].try_recv().unwrap();
        let admission = work_b.admission.as_ref().unwrap();
        assert_eq!(
            admission.results,
            vec![Err(TransactionError::WouldExceedMaxBlockCostLimit)]
        );
        assert_eq!(admission.reserved_cost, 0);
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 0));
    }

    #[test]
    fn test_hopeless_block_limit_failure_is_dispatched_immediately() {
        let (bank_forks, _) = test_bank_forks();
        let estimate = estimated_cost(&bank_forks.read().unwrap().working_bank());
        drop(bank_forks);
        // Nothing fits and nothing is inflight: no deferral is possible.
        let (mut test, mut container, bank, _decision) = setup_two_batches(Some(estimate / 2));

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 2);
        assert!(test.scheduler.pending_admission.is_none());
        assert_eq!(test.scheduler.num_deferred_admissions, 0);
        for _ in 0..2 {
            let work = test.consume_work_receivers[0].try_recv().unwrap();
            let admission = work.admission.as_ref().unwrap();
            assert_eq!(
                admission.results,
                vec![Err(TransactionError::WouldExceedMaxBlockCostLimit)]
            );
            assert_eq!(admission.reserved_cost, 0);
        }
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
    }

    #[test]
    fn test_deferred_batch_and_its_dependents_are_drained_at_slot_boundary() {
        let (bank_forks, _) = test_bank_forks();
        let mut test = create_test_scheduler(4, &bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let bank = bank_forks.read().unwrap().working_bank();
        set_leader_bank(&mut test.shared_leader_state, &bank);
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

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        let _work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert!(test.scheduler.pending_admission.is_some());

        // Slot ends: the deferred batch and the batch it was blocking both go back to BAM.
        test.scheduler
            .receive_completed(&mut container, &BufferedPacketsDecision::Forward)
            .unwrap();
        assert!(test.scheduler.pending_admission.is_none());
        assert!(!test.scheduler.retry_pending_admission);
        assert_eq!(test.scheduler.admission_bank_id, None);
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        assert!(test.scheduler.prio_graph.is_empty());
        assert!(container.pop().is_none());

        let mut seq_ids = vec![];
        for _ in 0..2 {
            let (seq_id, result) = next_result(&mut test.response_receiver);
            assert_outside_leader_slot(result);
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
        let (bank_forks, _) = test_bank_forks();
        let mut test = create_test_scheduler(4, &bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let root = bank_forks.read().unwrap().working_bank();
        let bank_1 = Arc::new(Bank::new_from_parent(
            root.clone(),
            SlotLeader::new_unique(),
            1,
        ));
        set_leader_bank(&mut test.shared_leader_state, &bank_1);
        let estimate = estimated_cost(&bank_1);
        set_block_cost_limit(&bank_1, estimate + estimate / 2);

        let mut container = create_container(vec![
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                1000,
                0,
                u64::MAX,
            ),
            (
                &Keypair::new(),
                vec![Pubkey::new_unique()],
                1000,
                1,
                u64::MAX,
            ),
        ]);
        let decision = BufferedPacketsDecision::Consume(bank_1.clone());
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(work_a.admission.as_ref().unwrap().bank_id, bank_1.bank_id());
        assert!(test.scheduler.pending_admission.is_some());

        // ParentReady replaces the provisional bank for the same slot. The old reservations are
        // moot, so the deferred batch is retried on the new bank right away.
        let bank_1b = Arc::new(Bank::new_from_parent(root, SlotLeader::new_unique(), 1));
        assert_ne!(bank_1b.bank_id(), bank_1.bank_id());
        set_leader_bank(&mut test.shared_leader_state, &bank_1b);

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        assert!(test.scheduler.pending_admission.is_none());
        let mut work_b = test.consume_work_receivers[0].try_recv().unwrap();
        let admission = work_b.admission.as_ref().unwrap();
        assert_eq!(admission.bank_id, bank_1b.bank_id());
        assert_eq!(admission.results, vec![Ok(())]);
        assert_eq!(test.scheduler.admission_bank_id, Some(bank_1b.bank_id()));
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(block_cost_and_in_flight(&bank_1b), (estimate, 1));
        // The reservation on the abandoned bank stays until A's result comes back.
        assert_eq!(block_cost_and_in_flight(&bank_1), (estimate, 1));

        // A executed on the replacement bank through the worker's local path, so its admission
        // comes back untouched: not counted against the new bank's inflight budget, and the
        // stale reservation is released from the abandoned bank.
        test.finished_consume_work_sender
            .send(committed_work(work_a, 150))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(block_cost_and_in_flight(&bank_1), (0, 0));
        assert_eq!(test.scheduler.num_released_reservations, 1);

        // B was settled by the worker on the new bank: nothing left for the scheduler to release.
        settle_committed(&bank_1b, &mut work_b, 150);
        test.finished_consume_work_sender
            .send(committed_work(work_b, 150))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        assert_eq!(block_cost_and_in_flight(&bank_1b).1, 0);
        assert_eq!(test.scheduler.num_released_reservations, 1);
    }

    #[test]
    fn test_no_active_bank_dispatches_without_admission() {
        let (bank_forks, _) = test_bank_forks();
        let mut test = create_test_scheduler(4, &bank_forks);
        test.scheduler.extra_checks_enabled = false;
        let bank = bank_forks.read().unwrap().working_bank();
        let mut container = create_container(vec![(
            &Keypair::new(),
            vec![Pubkey::new_unique()],
            1000,
            0,
            u64::MAX,
        )]);
        test.scheduler
            .receive_completed(
                &mut container,
                &BufferedPacketsDecision::Consume(bank.clone()),
            )
            .unwrap();

        let summary = test.scheduler.schedule(&mut container, 0, 0).unwrap();
        assert_eq!(summary.num_scheduled, 1);
        let work = test.consume_work_receivers[0].try_recv().unwrap();
        assert!(work.admission.is_none());
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
        assert_eq!(test.scheduler.admission_bank_id, None);
        let info = &test.scheduler.inflight_batch_info[&work.batch_id];
        assert!(info.bank.is_none());
        assert_eq!(info.reserved_cost, 0);
    }

    #[test]
    fn test_unprocessed_completion_releases_reservation_on_its_bank() {
        let (mut test, mut container, bank, decision) = setup_two_batches(None);
        let estimate = estimated_cost(&bank);

        test.scheduler.schedule(&mut container, 0, 0).unwrap();
        let work_a = test.consume_work_receivers[0].try_recv().unwrap();
        let mut work_b = test.consume_work_receivers[0].try_recv().unwrap();
        assert_eq!(block_cost_and_in_flight(&bank), (2 * estimate, 2));

        // The worker found the bank complete and returned A untouched, admission attached.
        test.finished_consume_work_sender
            .send(unprocessed_work(work_a))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        assert_eq!(block_cost_and_in_flight(&bank), (estimate, 1));
        assert_eq!(test.scheduler.inflight_reserved_cost, estimate);
        assert_eq!(test.scheduler.num_released_reservations, 1);
        let (seq_id, result) = next_result(&mut test.response_receiver);
        assert_eq!(seq_id, 0);
        let NotCommitted(not_committed) = result else {
            panic!("expected NotCommitted, got {result:?}");
        };
        assert_eq!(
            not_committed.reason,
            Some(Reason::SchedulingError(SchedulingError::PohTimeout as i32))
        );

        // B was executed and settled by the worker, which took the admission: nothing to release.
        settle_committed(&bank, &mut work_b, 150);
        test.finished_consume_work_sender
            .send(committed_work(work_b, 150))
            .unwrap();
        test.scheduler
            .receive_completed(&mut container, &decision)
            .unwrap();
        let (settled_cost, in_flight) = block_cost_and_in_flight(&bank);
        assert!(settled_cost < estimate);
        assert_eq!(in_flight, 0);
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
        assert_eq!(test.scheduler.num_released_reservations, 1);
    }

    #[test]
    fn test_send_failure_releases_reservation_and_propagates() {
        let (mut test, mut container, bank, _decision) = setup_two_batches(None);
        drop(test.consume_work_receivers);

        let result = test.scheduler.schedule(&mut container, 0, 0);
        assert!(matches!(
            result,
            Err(SchedulerError::DisconnectedSendChannel(_))
        ));
        assert_eq!(block_cost_and_in_flight(&bank), (0, 0));
        assert!(test.scheduler.inflight_batch_info.is_empty());
        assert_eq!(test.scheduler.inflight_reserved_cost, 0);
    }
}
