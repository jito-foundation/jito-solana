//! Scheduling policy outside the validator, communicating exclusively through shared memory.
//!
//! The client owns TPU input allocations. Jito ingress allocations return to the validator with
//! their completion; they are never freed here after that transfer. In-flight allocations remain
//! borrowed by workers until their response. On session failure the entire session is abandoned,
//! without retrying unknown outcomes or freeing memory a worker might still be using.

#![cfg(unix)]

mod policy;

use {
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, LEADER_READY, MAX_TRANSACTIONS_PER_MESSAGE,
        PackToCheckWorkerMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
        check_message_flags, execution_message_flags, processed_codes, tpu_message_flags,
        worker_message_types::{
            CheckResponse, not_included_reasons as reason, parsing_and_sanitization_flags,
            resolve_flags, scheduling_details_flags, status_check_flags,
        },
    },
    agave_scheduling_utils::{
        handshake::ClientSession,
        pubkeys_ptr::PubkeysPtr,
        responses_region::CheckResponsesPtr,
        transaction_ptr::{TransactionPtr, TransactionPtrBatch},
    },
    agave_transaction_view::transaction_view::UnsanitizedTransactionView,
    jito_scheduler_bindings::{
        JitoExecutionRequest, JitoExecutionResponse, JitoIngressMessage, JitoProgressMessage,
        JitoResponseRegion, JitoTransactionResult, SOURCE_BAM, SOURCE_LEGACY_BUNDLE, SOURCE_TPU,
        SOURCE_VOTE, SharedBytes, allocate_results, free_results, read_results,
    },
    policy::{Access, Dispatch},
    rts_alloc::Allocator,
    std::{
        collections::HashMap,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub max_transactions: usize,
    pub max_bytes: usize,
    pub max_checks: usize,
    pub max_jobs_per_worker: usize,
    pub max_buffer_age: Duration,
    pub session_timeout: Duration,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            max_transactions: 32_768,
            max_bytes: 64 * 1024 * 1024,
            max_checks: 256,
            max_jobs_per_worker: 4,
            max_buffer_age: Duration::from_secs(2),
            session_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SchedulerStats {
    pub received_transactions: u64,
    pub dropped_transactions: u64,
    pub submitted_batches: u64,
    pub completed_batches: u64,
    pub check_requests: u64,
}

#[derive(Debug, Error)]
pub enum SchedulerError {
    #[error("Jito scheduler bindings were not negotiated")]
    MissingExtension,
    #[error("scheduler configuration must have nonzero bounds and 1..=64 workers")]
    InvalidConfig,
    #[error("shared allocator exhausted")]
    Allocation,
    #[error("invalid response from validator: {0}")]
    Protocol(&'static str),
    #[error("validator progress heartbeat stopped; in-flight outcomes are unknown")]
    SessionTimeout,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    PendingCheck,
    Checking,
    Ready,
    Executing(usize),
    Completing,
}

struct Job {
    ingress: JitoIngressMessage,
    order: u64,
    return_to_validator: bool,
    is_vote: bool,
    state: State,
    bytes: usize,
    arrived: Instant,
    check_after: Instant,
    access: Access,
    estimated_cost: u64,
    estimated_allocation: u64,
    priority: u128,
    checked_bank: u64,
    alt_expiry: u64,
    response: Option<JitoExecutionResponse>,
    cancelled: bool,
}

impl Job {
    fn ordered(&self) -> bool {
        self.return_to_validator
    }
    fn atomic(&self) -> bool {
        u16::from(self.ingress.flags) & execution_message_flags::ALL_OR_NOTHING != 0
    }
}

/// Execute the same loop used by the standalone scheduler binary.
pub fn run(
    session: ClientSession,
    exit: Arc<AtomicBool>,
    config: SchedulerConfig,
) -> Result<SchedulerStats, SchedulerError> {
    let mut scheduler = Scheduler::new(session, config)?;
    while !exit.load(Ordering::Relaxed) {
        if !scheduler.step()? {
            std::thread::sleep(Duration::from_micros(50));
        }
    }
    // Workers may still hold shared pointers: no per-job free or synthesized failure on shutdown.
    Ok(scheduler.stats)
}

/// Shared-memory capacities used by the binary and integration tests.
pub fn client_logon(
    workers: usize,
    check_workers: usize,
) -> agave_scheduling_utils::handshake::ClientLogon {
    agave_scheduling_utils::handshake::ClientLogon {
        worker_count: workers,
        check_worker_count: check_workers,
        allocator_size: 256 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 32_768,
        progress_tracker_capacity: 1024,
        pack_to_worker_capacity: 256,
        worker_to_pack_capacity: 256,
        flags: agave_scheduling_utils::handshake::logon_flags::JITO,
        pack_to_check_worker_capacity: 1024,
        check_worker_to_pack_capacity: 1024,
    }
}

struct Scheduler {
    session: ClientSession,
    config: SchedulerConfig,
    jobs: HashMap<usize, Job>,
    dispatch: Dispatch,
    progress: Option<JitoProgressMessage>,
    last_progress: Instant,
    last_heartbeat: Instant,
    order: u64,
    transactions: usize,
    bytes: usize,
    lane: usize,
    stats: SchedulerStats,
}

impl Scheduler {
    fn new(session: ClientSession, config: SchedulerConfig) -> Result<Self, SchedulerError> {
        if session.jito.is_none() || session.workers.iter().any(|worker| worker.jito.is_none()) {
            return Err(SchedulerError::MissingExtension);
        }
        if session.allocators.is_empty()
            || session.workers.is_empty()
            || session.workers.len() > 64
            || config.max_transactions == 0
            || config.max_bytes == 0
            || config.max_checks == 0
            || config.max_jobs_per_worker == 0
            || config.session_timeout.is_zero()
        {
            return Err(SchedulerError::InvalidConfig);
        }
        let dispatch = Dispatch::new(session.workers.len(), config.max_jobs_per_worker);
        Ok(Self {
            session,
            config,
            jobs: HashMap::new(),
            dispatch,
            progress: None,
            last_progress: Instant::now(),
            last_heartbeat: Instant::now(),
            order: 0,
            transactions: 0,
            bytes: 0,
            lane: 0,
            stats: SchedulerStats::default(),
        })
    }

    fn allocator(&self) -> &Allocator {
        &self.session.allocators[0]
    }

    fn step(&mut self) -> Result<bool, SchedulerError> {
        let mut worked = false;
        // The standard progress queue must also be consumed to avoid stopping its producer.
        while self.session.progress_tracker.try_read().is_some() {
            worked = true;
        }
        while let Some(progress) = self.session.jito.as_mut().unwrap().progress.try_read() {
            self.last_progress = Instant::now();
            let bank_changed = self
                .progress
                .as_ref()
                .is_none_or(|old| old.bank_id != progress.bank_id);
            if bank_changed {
                for job in self.jobs.values_mut() {
                    if job.state == State::Ready {
                        job.state = State::PendingCheck;
                    }
                }
            }
            // Remember invalidation even if checks return after a new connection is established.
            for job in self.jobs.values_mut() {
                if job.ingress.source == SOURCE_BAM
                    && (progress.bam_connected == 0
                        || job.ingress.bam_generation != progress.bam_generation)
                {
                    job.cancelled = true;
                }
            }
            self.progress = Some(progress);
            worked = true;
        }
        if self.last_progress.elapsed() > self.config.session_timeout {
            return Err(SchedulerError::SessionTimeout);
        }
        if self.last_heartbeat.elapsed() >= Duration::from_millis(500) {
            let heartbeat = JitoExecutionResponse {
                id: 0,
                batch: SharableTransactionBatchRegion {
                    num_transactions: 0,
                    transactions_offset: 0,
                },
                processed_code: processed_codes::PROCESSED,
                execution_slot: self
                    .progress
                    .as_ref()
                    .map_or(0, |progress| progress.progress.current_slot),
                bank_id: self
                    .progress
                    .as_ref()
                    .map_or(0, |progress| progress.bank_id),
                responses: JitoResponseRegion::default(),
            };
            if self
                .session
                .jito
                .as_mut()
                .unwrap()
                .completion
                .try_write(heartbeat)
                .is_ok()
            {
                self.last_heartbeat = Instant::now();
                worked = true;
            }
        }
        for worker in 0..self.session.workers.len() {
            for _ in 0..self.config.max_jobs_per_worker {
                let Some(response) = self.session.workers[worker]
                    .jito
                    .as_mut()
                    .unwrap()
                    .response
                    .try_read()
                else {
                    break;
                };
                self.execution_response(worker, response)?;
                worked = true;
            }
        }
        for _ in 0..self.config.max_checks {
            let Some(response) = self.session.check_worker_to_pack.try_read() else {
                break;
            };
            self.check_response(response)?;
            worked = true;
        }
        worked |= self.flush_completions();
        worked |= self.expire_jobs()?;
        // Backpressure is applied before taking ownership from the validator ingress queue.
        for _ in 0..64 {
            if self
                .transactions
                .saturating_add(MAX_TRANSACTIONS_PER_MESSAGE)
                > self.config.max_transactions
                || self
                    .bytes
                    .saturating_add(MAX_TRANSACTIONS_PER_MESSAGE * 4096)
                    > self.config.max_bytes
            {
                break;
            }
            let Some(message) = self.session.jito.as_mut().unwrap().ingress.try_read() else {
                break;
            };
            self.accept(message, true, false)?;
            worked = true;
        }
        for _ in 0..256 {
            let Some(message) = self.session.tpu_to_pack.try_read() else {
                break;
            };
            let is_vote = message.flags & tpu_message_flags::IS_SIMPLE_VOTE != 0;
            let bam = self
                .progress
                .as_ref()
                .is_some_and(|progress| progress.bam_connected != 0);
            if (!is_vote && bam)
                || self.transactions >= self.config.max_transactions
                || self
                    .bytes
                    .saturating_add(message.transaction.length as usize)
                    > self.config.max_bytes
            {
                unsafe {
                    TransactionPtr::from_sharable_transaction_region(
                        &message.transaction,
                        self.allocator(),
                    )
                    .free(self.allocator());
                }
                self.stats.dropped_transactions = self.stats.dropped_transactions.saturating_add(1);
            } else {
                let Some(batch) = allocate_batch(self.allocator(), &[message.transaction]) else {
                    unsafe {
                        TransactionPtr::from_sharable_transaction_region(
                            &message.transaction,
                            self.allocator(),
                        )
                        .free(self.allocator());
                    }
                    self.stats.dropped_transactions =
                        self.stats.dropped_transactions.saturating_add(1);
                    continue;
                };
                let ingress = JitoIngressMessage {
                    id: self.order,
                    source: if is_vote { SOURCE_VOTE } else { SOURCE_TPU },
                    flags: 0,
                    max_slot: u64::MAX,
                    bam_generation: 0,
                    batch,
                };
                self.accept(ingress, false, is_vote)?;
            }
            worked = true;
        }
        worked |= self.send_checks();
        worked |= self.schedule()?;
        self.allocator().clean_remote_frees();
        Ok(worked)
    }

    fn accept(
        &mut self,
        ingress: JitoIngressMessage,
        return_to_validator: bool,
        is_vote: bool,
    ) -> Result<(), SchedulerError> {
        let count = usize::from(ingress.batch.num_transactions);
        if count == 0 || count > MAX_TRANSACTIONS_PER_MESSAGE {
            return Err(SchedulerError::Protocol("invalid ingress batch length"));
        }
        let key = ingress.batch.transactions_offset;
        if self.jobs.contains_key(&key) {
            return Err(SchedulerError::Protocol("duplicate live batch allocation"));
        }
        let batch = unsafe {
            TransactionPtrBatch::<()>::from_sharable_transaction_batch_region(
                &ingress.batch,
                self.allocator(),
            )
        };
        let bytes = batch
            .iter()
            .map(|(transaction, ())| {
                use agave_transaction_view::transaction_data::TransactionData;
                transaction.data().len()
            })
            .sum::<usize>();
        self.transactions = self.transactions.checked_add(count).unwrap();
        self.bytes = self.bytes.checked_add(bytes).unwrap();
        self.stats.received_transactions = self
            .stats
            .received_transactions
            .saturating_add(count as u64);
        self.jobs.insert(
            key,
            Job {
                ingress,
                order: self.order,
                return_to_validator,
                is_vote,
                state: State::PendingCheck,
                bytes,
                arrived: Instant::now(),
                check_after: Instant::now(),
                access: Access::default(),
                estimated_cost: 0,
                estimated_allocation: 0,
                priority: 0,
                checked_bank: 0,
                alt_expiry: u64::MAX,
                response: None,
                cancelled: false,
            },
        );
        self.order = self.order.wrapping_add(1);
        Ok(())
    }

    fn send_checks(&mut self) -> bool {
        let active = self
            .jobs
            .values()
            .filter(|job| job.state == State::Checking)
            .count();
        let capacity = self.config.max_checks.saturating_sub(active);
        let mut pending: Vec<_> = self
            .jobs
            .iter()
            .filter(|(_, job)| {
                job.state == State::PendingCheck && job.check_after <= Instant::now()
            })
            .map(|(key, job)| (*key, job.order))
            .collect();
        pending.sort_unstable_by_key(|(_, order)| *order);
        let mut worked = false;
        for (key, _) in pending.into_iter().take(capacity) {
            let job = self.jobs.get_mut(&key).unwrap();
            let message = PackToCheckWorkerMessage {
                flags: check_message_flags::STATUS_CHECKS
                    | check_message_flags::LOAD_ADDRESS_LOOKUP_TABLES
                    | check_message_flags::CALCULATE_SCHEDULING_DETAILS,
                batch: job.ingress.batch,
            };
            if self
                .session
                .pack_to_check_worker
                .try_write(message)
                .is_err()
            {
                break;
            }
            job.state = State::Checking;
            job.checked_bank = self
                .progress
                .as_ref()
                .map_or(0, |progress| progress.bank_id);
            self.stats.check_requests = self.stats.check_requests.saturating_add(1);
            worked = true;
        }
        worked
    }

    fn check_response(&mut self, response: CheckWorkerToPackMessage) -> Result<(), SchedulerError> {
        let key = response.batch.transactions_offset;
        let Some(job) = self.jobs.get(&key) else {
            return Err(SchedulerError::Protocol("unknown check response"));
        };
        if job.state != State::Checking || response.batch != job.ingress.batch {
            return Err(SchedulerError::Protocol("unexpected check response"));
        }
        if response.processed_code != processed_codes::PROCESSED {
            return self.reject(key, reason::SANITIZE_FAILURE);
        }
        if response.responses.num_transaction_responses != response.batch.num_transactions {
            return Err(SchedulerError::Protocol("check response length mismatch"));
        }
        let responses = unsafe {
            CheckResponsesPtr::from_transaction_response_region(
                &response.responses,
                self.allocator(),
            )
        };
        let batch = unsafe {
            TransactionPtrBatch::<()>::from_sharable_transaction_batch_region(
                &response.batch,
                self.allocator(),
            )
        };
        let mut accesses = Vec::new();
        let mut errors = Vec::new();
        let mut cost = 0_u64;
        let mut allocation = 0_u64;
        let mut reward = 0_u64;
        let mut alt_expiry = u64::MAX;
        for ((transaction, ()), checked) in batch.iter().zip(responses.iter()) {
            let mut rejected = check_failure(checked);
            let resolved = if checked.resolve_flags & resolve_flags::PERFORMED != 0
                && checked.resolved_pubkeys.num_pubkeys != 0
            {
                // Check workers zero optional fields if not performed. Only access a returned allocation.
                Some(unsafe {
                    PubkeysPtr::from_sharable_pubkeys(&checked.resolved_pubkeys, self.allocator())
                })
            } else {
                None
            };
            if rejected == reason::NONE {
                match UnsanitizedTransactionView::try_new_unsanitized(transaction) {
                    Ok(view) => {
                        let signed = usize::from(view.num_required_signatures());
                        let write_signed = signed.saturating_sub(usize::from(
                            view.num_readonly_signed_static_accounts(),
                        ));
                        let write_unsigned_end = usize::from(view.num_static_account_keys())
                            .saturating_sub(usize::from(
                                view.num_readonly_unsigned_static_accounts(),
                            ));
                        accesses.extend(view.static_account_keys().iter().enumerate().map(
                            |(index, key)| {
                                (
                                    *key,
                                    index < write_signed
                                        || (index >= signed && index < write_unsigned_end),
                                )
                            },
                        ));
                        let loaded = resolved.as_ref().map_or(&[][..], PubkeysPtr::as_slice);
                        let writes = usize::from(view.total_writable_lookup_accounts());
                        if loaded.len()
                            != writes
                                .saturating_add(usize::from(view.total_readonly_lookup_accounts()))
                        {
                            rejected = reason::ADDRESS_LOOKUP_TABLE_NOT_FOUND;
                        } else {
                            accesses.extend(
                                loaded
                                    .iter()
                                    .enumerate()
                                    .map(|(index, key)| (*key, index < writes)),
                            );
                        }
                    }
                    Err(_) => rejected = reason::SANITIZE_FAILURE,
                }
            }
            if let Some(resolved) = resolved {
                unsafe {
                    resolved.free(self.allocator());
                }
            }
            errors.push(rejected);
            cost = cost.saturating_add(checked.estimated_cost_units);
            allocation = allocation.saturating_add(checked.allocated_accounts_data_size);
            reward = reward
                .saturating_add(checked.transaction_fee)
                .saturating_add(checked.prioritization_fee);
            alt_expiry = alt_expiry.min(checked.min_alt_deactivation_slot);
        }
        unsafe {
            responses.free(self.allocator());
        }
        if self.jobs[&key].cancelled {
            return self.reject(key, reason::BANK_NOT_AVAILABLE);
        }
        if errors.iter().any(|error| *error != reason::NONE) {
            // All client-created TPU jobs contain one transaction; Jito ingress is a logical group.
            for error in &mut errors {
                if *error == reason::NONE {
                    *error = reason::ALL_OR_NOTHING_BATCH_FAILURE;
                }
            }
            return self.reject_with_reasons(key, &errors);
        }
        let job = self.jobs.get_mut(&key).unwrap();
        job.access = Access::from_keys(accesses);
        job.estimated_cost = cost;
        job.estimated_allocation = allocation;
        job.priority = u128::from(reward)
            .saturating_mul(1_000_000)
            .checked_div(u128::from(cost.max(1)))
            .unwrap();
        job.alt_expiry = alt_expiry;
        job.state = if self
            .progress
            .as_ref()
            .is_some_and(|progress| progress.bank_id != job.checked_bank)
        {
            State::PendingCheck
        } else {
            State::Ready
        };
        Ok(())
    }

    fn reject(&mut self, key: usize, error: u8) -> Result<(), SchedulerError> {
        let count = usize::from(self.jobs[&key].ingress.batch.num_transactions);
        self.reject_with_reasons(key, &vec![error; count])
    }

    fn reject_with_reasons(&mut self, key: usize, errors: &[u8]) -> Result<(), SchedulerError> {
        let job = &self.jobs[&key];
        if !job.return_to_validator {
            self.remove_and_free(key);
            self.stats.dropped_transactions = self
                .stats
                .dropped_transactions
                .saturating_add(errors.len() as u64);
            return Ok(());
        }
        let result_values: Vec<_> = errors
            .iter()
            .map(|error| {
                (
                    JitoTransactionResult {
                        not_included_reason: *error,
                        executed_units: 0,
                        loaded_accounts_data_size: 0,
                        fee_payer_balance: 0,
                        execution_success: 0,
                        error: SharedBytes {
                            offset: 0,
                            length: 0,
                        },
                    },
                    &[][..],
                )
            })
            .collect();
        let responses =
            allocate_results(self.allocator(), &result_values).ok_or(SchedulerError::Allocation)?;
        let progress = self.progress.as_ref();
        let response = JitoExecutionResponse {
            id: job.ingress.id,
            batch: job.ingress.batch,
            processed_code: processed_codes::PROCESSED,
            execution_slot: progress.map_or(0, |value| value.progress.current_slot),
            bank_id: progress.map_or(0, |value| value.bank_id),
            responses,
        };
        let job = self.jobs.get_mut(&key).unwrap();
        job.state = State::Completing;
        job.response = Some(response);
        Ok(())
    }

    fn expire_jobs(&mut self) -> Result<bool, SchedulerError> {
        let progress = self.progress.as_ref();
        let slot = progress.map_or(0, |value| value.progress.current_slot);
        let bam = progress.is_some_and(|value| value.bam_connected != 0);
        let expired: Vec<_> = self
            .jobs
            .iter()
            .filter(|(_, job)| {
                matches!(job.state, State::Ready | State::PendingCheck)
                    && (job.cancelled
                        || (job.ingress.source == SOURCE_BAM && progress.is_some() && !bam)
                        || (job.ingress.source == SOURCE_BAM
                            && progress.is_some_and(|value| {
                                job.ingress.bam_generation != value.bam_generation
                            }))
                        || job.ingress.max_slot < slot
                        || job.arrived.elapsed() > self.config.max_buffer_age
                        || (bam && !job.is_vote && !job.return_to_validator))
            })
            .map(|(key, _)| *key)
            .collect();
        for key in &expired {
            self.reject(*key, reason::BANK_NOT_AVAILABLE)?;
        }
        Ok(!expired.is_empty())
    }

    fn schedule(&mut self) -> Result<bool, SchedulerError> {
        let Some(progress) = self.progress.as_ref() else {
            return Ok(false);
        };
        if progress.progress.leader_state != LEADER_READY {
            return Ok(false);
        }
        let slot = progress.progress.current_slot;
        let bank_id = progress.bank_id;
        let atomic_ready = progress.atomic_batches_enabled != 0;
        let bam = progress.bam_connected != 0;
        let executing_cost = self
            .jobs
            .values()
            .filter(|job| matches!(job.state, State::Executing(_)))
            .fold(0_u64, |sum, job| sum.saturating_add(job.estimated_cost));
        let executing_allocation = self
            .jobs
            .values()
            .filter(|job| matches!(job.state, State::Executing(_)))
            .fold(0_u64, |sum, job| {
                sum.saturating_add(job.estimated_allocation)
            });
        let mut budget = progress
            .progress
            .remaining_cost_units
            .saturating_sub(executing_cost);
        let mut allocation_budget = progress
            .progress
            .remaining_allocated_accounts_data_size
            .saturating_sub(executing_allocation);
        let mut worked = false;
        let mut lanes: [Vec<(usize, u64, u128)>; 3] = Default::default();
        for (key, job) in &self.jobs {
            let lane = if job.is_vote {
                0
            } else if job.ordered() {
                1
            } else {
                2
            };
            lanes[lane].push((*key, job.order, job.priority));
        }
        lanes[0].sort_unstable_by_key(|(_, order, _)| *order);
        lanes[1].sort_unstable_by_key(|(_, order, _)| *order);
        lanes[2]
            .sort_unstable_by_key(|(_, order, priority)| (std::cmp::Reverse(*priority), *order));
        let mut cursors = [0; 3];
        let mut barrier = Access::default();
        // Rotate the starting lane so continuous votes, bundles, or ordinary ingress cannot starve peers.
        for _ in 0..128 {
            let lane = self.lane % 3;
            self.lane = self.lane.wrapping_add(1);
            while let Some((key, _, _)) = lanes[lane].get(cursors[lane]).copied() {
                cursors[lane] = cursors[lane].saturating_add(1);
                let job = &self.jobs[&key];
                if matches!(job.state, State::Executing(_) | State::Completing) {
                    continue;
                }
                if job.state != State::Ready {
                    if lane == 1 {
                        cursors[lane] = lanes[lane].len();
                        break;
                    } // An unchecked predecessor has unknown account dependencies.
                    continue;
                }
                if job.ingress.max_slot < slot || job.checked_bank != bank_id {
                    continue;
                }
                if job.alt_expiry <= slot {
                    self.jobs.get_mut(&key).unwrap().state = State::PendingCheck;
                    if lane == 1 {
                        cursors[lane] = lanes[lane].len();
                        break;
                    }
                    continue;
                }
                if (job.atomic() && !atomic_ready)
                    || (job.ingress.source == SOURCE_BAM && !bam)
                    || (job.ingress.source == SOURCE_LEGACY_BUNDLE && bam)
                    || (!job.is_vote && !job.return_to_validator && bam)
                    || job.estimated_cost > budget
                    || job.estimated_allocation > allocation_budget
                    || (lane == 1 && barrier.conflicts(&job.access))
                {
                    if lane == 1 {
                        barrier.merge(&job.access);
                    }
                    continue;
                }
                let Some(worker) = self.dispatch.reserve(&job.access) else {
                    if lane == 1 {
                        barrier.merge(&job.access);
                    }
                    continue;
                };
                let request = JitoExecutionRequest {
                    id: job.ingress.id,
                    source: job.ingress.source,
                    flags: job.ingress.flags,
                    slot,
                    bank_id,
                    bam_generation: job.ingress.bam_generation,
                    batch: job.ingress.batch,
                };
                if self.session.workers[worker]
                    .jito
                    .as_mut()
                    .unwrap()
                    .request
                    .try_write(request)
                    .is_err()
                {
                    self.dispatch.release(&job.access, worker);
                    if lane == 1 {
                        barrier.merge(&job.access);
                    }
                    continue;
                }
                budget = budget.saturating_sub(job.estimated_cost);
                allocation_budget = allocation_budget.saturating_sub(job.estimated_allocation);
                self.jobs.get_mut(&key).unwrap().state = State::Executing(worker);
                self.stats.submitted_batches = self.stats.submitted_batches.saturating_add(1);
                worked = true;
                break;
            }
        }
        Ok(worked)
    }

    fn execution_response(
        &mut self,
        worker: usize,
        response: JitoExecutionResponse,
    ) -> Result<(), SchedulerError> {
        let key = response.batch.transactions_offset;
        let Some(job) = self.jobs.get(&key) else {
            return Err(SchedulerError::Protocol("unknown execution response"));
        };
        if job.state != State::Executing(worker)
            || response.batch != job.ingress.batch
            || response.id != job.ingress.id
        {
            return Err(SchedulerError::Protocol("mismatched execution response"));
        }
        self.dispatch.release(&job.access, worker);
        self.stats.completed_batches = self.stats.completed_batches.saturating_add(1);
        if job.return_to_validator {
            if job.ingress.source == SOURCE_LEGACY_BUNDLE
                && job.atomic()
                && job.arrived.elapsed() < self.config.max_buffer_age
                && response.processed_code == processed_codes::PROCESSED
            {
                let results = unsafe { read_results(self.allocator(), &response.responses) }
                    .ok_or(SchedulerError::Protocol("invalid execution results"))?;
                if results.len() != usize::from(response.batch.num_transactions) {
                    return Err(SchedulerError::Protocol("execution result length mismatch"));
                }
                if retryable_atomic_failure(&results) {
                    unsafe {
                        free_results(self.allocator(), response.responses);
                    }
                    let job = self.jobs.get_mut(&key).unwrap();
                    job.state = State::PendingCheck;
                    // Tip readiness and locks can recover within the same BankId. Recheck
                    // against the current bank without busy-looping or replaying any commit.
                    job.check_after = Instant::now()
                        .checked_add(Duration::from_millis(1))
                        .unwrap();
                    return Ok(());
                }
            }
            let job = self.jobs.get_mut(&key).unwrap();
            job.state = State::Completing;
            job.response = Some(response);
        } else {
            let mut retry = false;
            if response.processed_code == processed_codes::PROCESSED {
                let results = unsafe { read_results(self.allocator(), &response.responses) }
                    .ok_or(SchedulerError::Protocol("invalid execution results"))?;
                if results.len() != usize::from(response.batch.num_transactions) {
                    return Err(SchedulerError::Protocol("execution result length mismatch"));
                }
                retry = results
                    .iter()
                    .all(|(result, _)| retryable(result.not_included_reason));
                unsafe {
                    free_results(self.allocator(), response.responses);
                }
            } else if response.processed_code == processed_codes::MAX_WORKING_SLOT_EXCEEDED {
                retry = true;
            }
            if retry && self.jobs[&key].arrived.elapsed() < self.config.max_buffer_age {
                self.jobs.get_mut(&key).unwrap().state = State::PendingCheck;
            } else {
                self.remove_and_free(key);
            }
        }
        Ok(())
    }

    fn flush_completions(&mut self) -> bool {
        let mut keys: Vec<_> = self
            .jobs
            .iter()
            .filter(|(_, job)| job.state == State::Completing)
            .map(|(key, job)| (*key, job.order))
            .collect();
        keys.sort_unstable_by_key(|(_, order)| *order);
        let mut worked = false;
        for (key, _) in keys {
            let response = self.jobs[&key].response.unwrap();
            if self
                .session
                .jito
                .as_mut()
                .unwrap()
                .completion
                .try_write(response)
                .is_err()
            {
                break;
            }
            self.remove(key); // Queue now owns batch bytes, descriptor, and response allocation.
            worked = true;
        }
        worked
    }

    fn remove(&mut self, key: usize) -> Job {
        let job = self.jobs.remove(&key).unwrap();
        self.transactions = self
            .transactions
            .checked_sub(usize::from(job.ingress.batch.num_transactions))
            .unwrap();
        self.bytes = self.bytes.checked_sub(job.bytes).unwrap();
        job
    }

    fn remove_and_free(&mut self, key: usize) {
        let job = self.remove(key);
        unsafe {
            free_batch(self.allocator(), job.ingress.batch);
        }
    }
}

fn check_failure(response: &CheckResponse) -> u8 {
    if response.parsing_and_sanitization_flags & parsing_and_sanitization_flags::FAILED != 0 {
        return reason::SANITIZE_FAILURE;
    }
    if response.status_check_flags & status_check_flags::PERFORMED == 0 {
        return reason::BANK_NOT_AVAILABLE;
    }
    if response.status_check_flags & status_check_flags::ALREADY_PROCESSED != 0 {
        return reason::ALREADY_PROCESSED;
    }
    if response.status_check_flags & status_check_flags::TOO_OLD != 0 {
        return reason::BLOCKHASH_NOT_FOUND;
    }
    if response.status_check_flags & status_check_flags::UNSUPPORTED_VERSION != 0 {
        return reason::UNSUPPORTED_VERSION;
    }
    if response.status_check_flags & status_check_flags::INVALID_NONCE != 0 {
        return reason::BLOCKHASH_NOT_FOUND;
    }
    if response.resolve_flags & resolve_flags::PERFORMED == 0
        || response.resolve_flags & resolve_flags::FAILED != 0
    {
        return reason::ADDRESS_LOOKUP_TABLE_NOT_FOUND;
    }
    if response.scheduling_details_flags & scheduling_details_flags::PERFORMED == 0
        || response.scheduling_details_flags & scheduling_details_flags::FAILED != 0
    {
        return reason::SANITIZE_FAILURE;
    }
    reason::NONE
}

fn retryable(reason: u8) -> bool {
    matches!(
        reason,
        reason::BANK_NOT_AVAILABLE
            | reason::ACCOUNT_IN_USE
            | reason::WOULD_EXCEED_MAX_BLOCK_COST_LIMIT
            | reason::WOULD_EXCEED_MAX_ACCOUNT_COST_LIMIT
            | reason::WOULD_EXCEED_MAX_VOTE_COST_LIMIT
            | reason::WOULD_EXCEED_ACCOUNT_DATA_BLOCK_LIMIT
            | reason::WOULD_EXCEED_ACCOUNT_DATA_TOTAL_LIMIT
    )
}

fn retryable_atomic_failure(results: &[(JitoTransactionResult, Vec<u8>)]) -> bool {
    results
        .iter()
        .any(|(result, _)| retryable(result.not_included_reason))
        && results.iter().all(|(result, _)| {
            result.execution_success == 0
                && (retryable(result.not_included_reason)
                    || result.not_included_reason == reason::ALL_OR_NOTHING_BATCH_FAILURE)
        })
}

fn allocate_batch(
    allocator: &Allocator,
    transactions: &[SharableTransactionRegion],
) -> Option<SharableTransactionBatchRegion> {
    if transactions.is_empty() || transactions.len() > MAX_TRANSACTIONS_PER_MESSAGE {
        return None;
    }
    let pointer = allocator.allocate(TransactionPtrBatch::<()>::TRANSACTION_META_END as u32)?;
    for (index, transaction) in transactions.iter().enumerate() {
        unsafe {
            pointer
                .cast::<SharableTransactionRegion>()
                .add(index)
                .write(*transaction);
        }
    }
    Some(SharableTransactionBatchRegion {
        num_transactions: transactions.len() as u8,
        transactions_offset: unsafe { allocator.offset(pointer) },
    })
}

/// Only call when all check/execution workers have returned ownership of this batch.
unsafe fn free_batch(allocator: &Allocator, region: SharableTransactionBatchRegion) {
    let batch = unsafe {
        TransactionPtrBatch::<()>::from_sharable_transaction_batch_region(&region, allocator)
    };
    for (transaction, ()) in batch.iter() {
        unsafe {
            transaction.free(allocator);
        }
    }
    unsafe {
        batch.free();
    }
}

#[cfg(test)]
mod tests;
