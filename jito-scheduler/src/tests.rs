use {
    super::*,
    agave_scheduler_bindings::{ProgressMessage, SharablePubkeys, TpuToPackMessage},
    agave_scheduling_utils::{
        handshake::{AgaveSession, client, server::Server},
        responses_region::resolve_responses_from_iter,
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
};

struct Frame {
    scheduler: Scheduler,
    server: AgaveSession,
    generation: u64,
}

impl Frame {
    fn new(workers: usize) -> Self {
        let mut logon = client_logon(workers, 1);
        logon.allocator_size = 64 * 1024 * 1024;
        logon.tpu_to_pack_capacity = 16;
        logon.progress_tracker_capacity = 16;
        logon.pack_to_worker_capacity = 16;
        logon.worker_to_pack_capacity = 16;
        logon.pack_to_check_worker_capacity = 16;
        logon.check_worker_to_pack_capacity = 16;
        let (server, files) = Server::setup_session(logon).unwrap();
        let session = client::setup_session(&logon, files).unwrap();
        let scheduler = Scheduler::new(
            session,
            SchedulerConfig {
                max_transactions: 512,
                max_bytes: 4 * 1024 * 1024,
                ..Default::default()
            },
        )
        .unwrap();
        Self {
            scheduler,
            server,
            generation: 0,
        }
    }

    fn progress(&mut self, slot: u64, bank_id: u64, atomic: bool, bam: bool) {
        self.server
            .jito
            .as_mut()
            .unwrap()
            .progress
            .try_write(JitoProgressMessage {
                progress: ProgressMessage {
                    leader_state: LEADER_READY,
                    current_slot_progress: 1,
                    epoch: 0,
                    current_slot: slot,
                    next_leader_slot: slot.saturating_add(1),
                    leader_range_end: slot.saturating_add(3),
                    remaining_cost_units: 60_000_000,
                    remaining_allocated_accounts_data_size: 100_000_000,
                    latest_blockhash: [0; 32],
                    target_bank_time_ms: 400,
                },
                bank_id,
                atomic_batches_enabled: u8::from(atomic),
                bam_connected: u8::from(bam),
                bam_generation: self.generation,
            })
            .unwrap();
    }

    fn submit(
        &mut self,
        id: u64,
        source: u8,
        atomic: bool,
        max_slot: u64,
        bytes: &[Vec<u8>],
    ) -> SharableTransactionBatchRegion {
        let batch =
            jito_scheduler_bindings::allocate_batch(&self.server.tpu_to_pack.allocator, bytes)
                .unwrap();
        self.server
            .jito
            .as_mut()
            .unwrap()
            .ingress
            .try_write(JitoIngressMessage {
                id,
                source,
                flags: if atomic { 3 } else { 0 },
                max_slot,
                bam_generation: self.generation,
                batch,
            })
            .unwrap();
        batch
    }

    fn tpu(&mut self, bytes: &[u8], vote: bool) {
        let allocator = &self.server.tpu_to_pack.allocator;
        let pointer = allocator.allocate(bytes.len() as u32).unwrap();
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), pointer.as_ptr(), bytes.len());
        }
        self.server
            .tpu_to_pack
            .producer
            .try_write(TpuToPackMessage {
                transaction: SharableTransactionRegion {
                    offset: unsafe { allocator.offset(pointer) },
                    length: bytes.len() as u32,
                },
                flags: if vote {
                    tpu_message_flags::IS_SIMPLE_VOTE
                } else {
                    0
                },
                src_addr: [0; 16],
            })
            .unwrap();
    }

    fn check_requests(&mut self) -> Vec<PackToCheckWorkerMessage> {
        let mut requests = Vec::new();
        while let Some(request) = self.server.check_workers[0].pack_to_check_worker.try_read() {
            requests.push(request);
        }
        requests
    }

    fn respond_check(&self, request: PackToCheckWorkerMessage, failure_index: Option<usize>) {
        self.respond_check_with(request, |index| {
            let mut response = successful_check();
            if Some(index) == failure_index {
                response.status_check_flags |= status_check_flags::ALREADY_PROCESSED;
            }
            response
        });
    }

    fn respond_check_with(
        &self,
        request: PackToCheckWorkerMessage,
        response: impl FnMut(usize) -> CheckResponse,
    ) {
        let worker = &self.server.check_workers[0];
        let responses = resolve_responses_from_iter(
            &worker.allocator,
            (0..usize::from(request.batch.num_transactions)).map(response),
        )
        .unwrap();
        worker
            .check_worker_to_pack
            .try_write(CheckWorkerToPackMessage {
                batch: request.batch,
                processed_code: processed_codes::PROCESSED,
                responses,
            })
            .unwrap();
    }

    fn check_all(&mut self) {
        self.scheduler.step().unwrap();
        for request in self.check_requests() {
            self.respond_check(request, None);
        }
        self.scheduler.step().unwrap();
    }

    fn requests(&mut self) -> Vec<(usize, JitoExecutionRequest)> {
        let mut requests = Vec::new();
        for (index, worker) in self.server.workers.iter_mut().enumerate() {
            while let Some(request) = worker.jito.as_mut().unwrap().request.try_read() {
                requests.push((index, request));
            }
        }
        requests
    }

    fn finish(&mut self, worker: usize, request: JitoExecutionRequest, reason: u8) {
        let values = vec![
            (
                JitoTransactionResult {
                    not_included_reason: reason,
                    executed_units: 123,
                    loaded_accounts_data_size: 456,
                    fee_payer_balance: 789,
                    execution_success: u8::from(reason == reason::NONE),
                    error: SharedBytes::default(),
                },
                &b"preserved diagnostic"[..]
            );
            usize::from(request.batch.num_transactions)
        ];
        let allocator = &self.server.workers[worker].allocator;
        let responses = allocate_results(allocator, &values).unwrap();
        self.server.workers[worker]
            .jito
            .as_mut()
            .unwrap()
            .response
            .try_write(JitoExecutionResponse {
                id: request.id,
                batch: request.batch,
                processed_code: processed_codes::PROCESSED,
                execution_slot: request.slot,
                bank_id: request.bank_id,
                responses,
            })
            .unwrap();
    }

    fn completion(&mut self) -> JitoExecutionResponse {
        self.server
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .unwrap()
    }

    fn free_completion(&self, completion: JitoExecutionResponse) {
        let allocator = &self.server.tpu_to_pack.allocator;
        unsafe {
            free_results(allocator, completion.responses);
            jito_scheduler_bindings::free_batch(allocator, completion.batch);
        }
    }
}

fn successful_check() -> CheckResponse {
    CheckResponse {
        parsing_and_sanitization_flags: 0,
        status_check_flags: status_check_flags::REQUESTED | status_check_flags::PERFORMED,
        fee_payer_balance_flags: 0,
        resolve_flags: resolve_flags::REQUESTED | resolve_flags::PERFORMED,
        scheduling_details_flags: scheduling_details_flags::REQUESTED
            | scheduling_details_flags::PERFORMED,
        included_slot: 0,
        transaction_fee: 5000,
        prioritization_fee: 100,
        estimated_cost_units: 1000,
        allocated_accounts_data_size: 0,
        balance_slot: 10,
        fee_payer_balance: 1_000_000,
        resolution_slot: 10,
        min_alt_deactivation_slot: u64::MAX,
        resolved_pubkeys: SharablePubkeys {
            offset: 0,
            num_pubkeys: 0,
        },
    }
}

fn transfer(payer: &Keypair, recipient: &Pubkey) -> Vec<u8> {
    wincode::serialize(&solana_system_transaction::transfer(
        payer,
        recipient,
        1,
        Hash::new_from_array([9; 32]),
    ))
    .unwrap()
}

#[test]
fn real_session_parallel_workers_return_exact_bundle_details() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, true);
    let first = transfer(&Keypair::new(), &Pubkey::new_unique());
    let second = transfer(&Keypair::new(), &Pubkey::new_unique());
    frame.submit(1, SOURCE_BAM, true, 10, &[first]);
    frame.submit(2, SOURCE_BAM, true, 10, &[second]);
    frame.check_all();
    let requests = frame.requests();
    assert_eq!(requests.len(), 2);
    assert_ne!(requests[0].0, requests[1].0);
    for (worker, request) in requests {
        assert_eq!((request.slot, request.bank_id, request.flags), (10, 20, 3));
        frame.finish(worker, request, reason::NONE);
    }
    frame.scheduler.step().unwrap();
    for expected_id in [1, 2] {
        let completion = frame.completion();
        assert_eq!(completion.id, expected_id);
        let results =
            unsafe { read_results(&frame.server.tpu_to_pack.allocator, &completion.responses) }
                .unwrap();
        assert_eq!(results[0].0.executed_units, 123);
        assert_eq!(results[0].0.loaded_accounts_data_size, 456);
        assert_eq!(results[0].1, b"preserved diagnostic");
        frame.free_completion(completion);
    }
    assert!(frame.scheduler.jobs.is_empty());
    assert_eq!(
        (frame.scheduler.transactions, frame.scheduler.bytes),
        (0, 0)
    );
}

#[test]
fn atomic_waits_for_parent_and_uses_new_bank_identity() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, false, true);
    frame.submit(
        1,
        SOURCE_BAM,
        true,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.check_all();
    assert!(frame.requests().is_empty());
    frame.progress(10, 21, true, true);
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty()); // Recheck against the replacement bank before dispatch.
    for check in frame.check_requests() {
        frame.respond_check(check, None);
    }
    frame.scheduler.step().unwrap();
    let request = frame.requests().pop().unwrap().1;
    assert_eq!((request.slot, request.bank_id), (10, 21));
}

#[test]
fn out_of_order_checks_cannot_reverse_conflicting_bam_batches() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, true);
    let payer = Keypair::new();
    frame.submit(
        1,
        SOURCE_BAM,
        true,
        10,
        &[transfer(&payer, &Pubkey::new_unique())],
    );
    frame.submit(
        2,
        SOURCE_BAM,
        true,
        10,
        &[transfer(&payer, &Pubkey::new_unique())],
    );
    frame.scheduler.step().unwrap();
    let checks = frame.check_requests();
    frame.respond_check(checks[1], None);
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty());
    frame.respond_check(checks[0], None);
    frame.scheduler.step().unwrap();
    let requests = frame.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].0, requests[1].0);
    assert_eq!((requests[0].1.id, requests[1].1.id), (1, 2));
}

#[test]
fn precheck_failure_keeps_atomic_group_intact() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, false);
    let bytes = vec![
        transfer(&Keypair::new(), &Pubkey::new_unique()),
        transfer(&Keypair::new(), &Pubkey::new_unique()),
    ];
    frame.submit(7, SOURCE_LEGACY_BUNDLE, true, 10, &bytes);
    frame.scheduler.step().unwrap();
    let request = frame.check_requests().pop().unwrap();
    frame.respond_check(request, Some(1));
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty());
    let completion = frame.completion();
    let results =
        unsafe { read_results(&frame.server.tpu_to_pack.allocator, &completion.responses) }
            .unwrap();
    assert_eq!(
        results[0].0.not_included_reason,
        reason::ALL_OR_NOTHING_BATCH_FAILURE
    );
    assert_eq!(results[1].0.not_included_reason, reason::ALREADY_PROCESSED);
    assert_eq!(completion.batch.num_transactions, 2);
    frame.free_completion(completion);
}

#[test]
fn bam_drops_ordinary_tpu_but_preserves_votes() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, true);
    frame.tpu(&transfer(&Keypair::new(), &Pubkey::new_unique()), false);
    frame.tpu(&transfer(&Keypair::new(), &Pubkey::new_unique()), true);
    frame.check_all();
    let requests = frame.requests();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].1.source, SOURCE_VOTE);
    assert_eq!(frame.scheduler.stats.dropped_transactions, 1);
    frame.finish(requests[0].0, requests[0].1, reason::NONE);
    frame.scheduler.step().unwrap();
    assert!(frame.scheduler.jobs.is_empty());
    assert!(
        frame
            .server
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .is_none()
    );
}

#[test]
fn expired_batch_returns_without_execution() {
    let mut frame = Frame::new(2);
    frame.progress(11, 20, true, true);
    frame.submit(
        1,
        SOURCE_BAM,
        true,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.scheduler.step().unwrap();
    // Pending checks must return ownership before the client may expire their inputs.
    for check in frame.check_requests() {
        frame.respond_check(check, None);
    }
    frame.scheduler.step().unwrap();
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty());
    let completion = frame.completion();
    frame.free_completion(completion);
}

#[test]
fn stale_session_does_not_free_worker_borrowed_memory() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, true);
    let bytes = transfer(&Keypair::new(), &Pubkey::new_unique());
    frame.submit(1, SOURCE_BAM, true, 10, std::slice::from_ref(&bytes));
    frame.check_all();
    let request = frame.requests().pop().unwrap().1;
    frame.scheduler.last_progress = Instant::now() - Duration::from_secs(10);
    assert!(matches!(
        frame.scheduler.step(),
        Err(SchedulerError::SessionTimeout)
    ));
    let batch = unsafe {
        TransactionPtrBatch::<()>::from_sharable_transaction_batch_region(
            &request.batch,
            &frame.server.workers[0].allocator,
        )
    };
    use agave_transaction_view::transaction_data::TransactionData;
    assert_eq!(batch.iter().next().unwrap().0.data(), bytes);
    assert!(
        frame
            .server
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .is_none()
    );
}

#[test]
fn reconnect_generation_invalidates_checking_batch_without_disconnected_progress() {
    let mut frame = Frame::new(2);
    frame.generation = 5;
    frame.progress(10, 20, true, true);
    frame.submit(
        1,
        SOURCE_BAM,
        false,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.scheduler.step().unwrap();
    let old_check = frame.check_requests().pop().unwrap();
    frame.generation = 6;
    frame.progress(10, 20, true, true);
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty());
    assert_eq!(
        frame.scheduler.jobs[&old_check.batch.transactions_offset].state,
        State::Checking
    );
    // The checking worker still owns the input, so cancellation waits for its response.
    frame.respond_check(old_check, None);
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    assert_eq!(completion.id, 1);
    let results =
        unsafe { read_results(&frame.server.tpu_to_pack.allocator, &completion.responses) }
            .unwrap();
    assert_eq!(results[0].0.not_included_reason, reason::BANK_NOT_AVAILABLE);
    frame.free_completion(completion);
    assert!(frame.requests().is_empty());

    frame.submit(
        2,
        SOURCE_BAM,
        false,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.check_all();
    let (worker, request) = frame.requests().pop().unwrap();
    assert_eq!((request.id, request.bam_generation), (2, 6));
    frame.finish(worker, request, reason::NONE);
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    frame.free_completion(completion);
    assert!(frame.scheduler.jobs.is_empty());
}

#[test]
fn disconnect_cancellation_survives_reconnect_before_check_returns() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, true);
    frame.submit(
        1,
        SOURCE_BAM,
        false,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.scheduler.step().unwrap();
    let check = frame.check_requests().pop().unwrap();
    frame.progress(10, 20, true, false);
    frame.progress(10, 20, true, true);
    frame.respond_check(check, None);
    frame.scheduler.step().unwrap();
    assert!(frame.requests().is_empty());
    let completion = frame.completion();
    frame.free_completion(completion);
    assert!(frame.scheduler.jobs.is_empty());
}

#[test]
fn non_atomic_bam_runs_on_provisional_bank() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, false, true);
    frame.submit(
        1,
        SOURCE_BAM,
        false,
        10,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.check_all();
    let (worker, request) = frame.requests().pop().unwrap();
    assert_eq!(request.flags, 0);
    frame.finish(worker, request, reason::NONE);
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    frame.free_completion(completion);
}

#[test]
fn completion_backpressure_retains_ownership_and_sends_once() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, true);
    let bytes = transfer(&Keypair::new(), &Pubkey::new_unique());
    let batch = frame.submit(1, SOURCE_BAM, true, 10, std::slice::from_ref(&bytes));
    frame.check_all();
    let (worker, request) = frame.requests().pop().unwrap();
    let heartbeat = JitoExecutionResponse {
        id: 0,
        batch: SharableTransactionBatchRegion {
            num_transactions: 0,
            transactions_offset: 0,
        },
        processed_code: processed_codes::PROCESSED,
        execution_slot: 10,
        bank_id: 20,
        responses: JitoResponseRegion::default(),
    };
    let mut filled = 0;
    while frame
        .scheduler
        .session
        .jito
        .as_mut()
        .unwrap()
        .completion
        .try_write(heartbeat)
        .is_ok()
    {
        filled += 1;
    }
    assert!(filled > 0);
    frame.finish(worker, request, reason::NONE);
    frame.scheduler.step().unwrap();
    assert_eq!(
        frame.scheduler.jobs[&batch.transactions_offset].state,
        State::Completing
    );
    let borrowed = unsafe {
        TransactionPtrBatch::<()>::from_sharable_transaction_batch_region(
            &batch,
            &frame.server.tpu_to_pack.allocator,
        )
    };
    use agave_transaction_view::transaction_data::TransactionData;
    assert_eq!(borrowed.iter().next().unwrap().0.data(), bytes);
    for _ in 0..filled {
        assert_eq!(frame.completion().id, 0);
    }
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    assert_eq!((completion.id, completion.batch), (1, batch));
    frame.free_completion(completion);
    frame.scheduler.step().unwrap();
    assert!(
        frame
            .server
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .is_none()
    );
    assert!(frame.scheduler.jobs.is_empty());
    assert_eq!(frame.scheduler.stats.completed_batches, 1);
}

#[test]
fn resolved_alt_writes_keep_dependent_bam_batches_on_same_worker() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, true);
    let payer = Keypair::new();
    let recipient = Pubkey::new_unique();
    let original =
        solana_system_transaction::transfer(&payer, &recipient, 1, Hash::new_from_array([9; 32]));
    assert_eq!(original.message.account_keys[1], recipient);
    let message = solana_message::v0::Message {
        header: original.message.header,
        account_keys: vec![
            original.message.account_keys[0],
            original.message.account_keys[2],
        ],
        recent_blockhash: original.message.recent_blockhash,
        instructions: original
            .message
            .instructions
            .into_iter()
            .map(|mut instruction| {
                instruction.program_id_index = 1;
                instruction.accounts = vec![0, 2];
                instruction
            })
            .collect(),
        address_table_lookups: vec![solana_message::v0::MessageAddressTableLookup {
            account_key: Pubkey::new_unique(),
            writable_indexes: vec![0],
            readonly_indexes: vec![],
        }],
    };
    let transaction = solana_transaction::versioned::VersionedTransaction::try_new(
        solana_message::VersionedMessage::V0(message),
        &[&payer],
    )
    .unwrap();
    frame.submit(
        1,
        SOURCE_BAM,
        false,
        10,
        &[wincode::serialize(&transaction).unwrap()],
    );
    // The second batch uses the ALT-loaded destination as its ordinary static writable account.
    frame.submit(
        2,
        SOURCE_BAM,
        false,
        10,
        &[transfer(&Keypair::new(), &recipient)],
    );
    frame.scheduler.step().unwrap();
    for (index, request) in frame.check_requests().into_iter().enumerate() {
        let mut check = successful_check();
        if index == 0 {
            let allocator = &frame.server.check_workers[0].allocator;
            let pointer = allocator
                .allocate(std::mem::size_of::<Pubkey>() as u32)
                .unwrap();
            unsafe {
                pointer.cast::<Pubkey>().as_ptr().write(recipient);
            }
            check.resolved_pubkeys = SharablePubkeys {
                offset: unsafe { allocator.offset(pointer) },
                num_pubkeys: 1,
            };
        }
        frame.respond_check_with(request, |_| check);
    }
    frame.scheduler.step().unwrap();
    let requests = frame.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!((requests[0].1.id, requests[1].1.id), (1, 2));
    assert_eq!(requests[0].0, requests[1].0);
    for (worker, request) in requests {
        frame.finish(worker, request, reason::NONE);
    }
    frame.scheduler.step().unwrap();
    for _ in 0..2 {
        let completion = frame.completion();
        frame.free_completion(completion);
    }
}

#[test]
fn ordinary_transactions_use_fee_priority_with_fifo_execution() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, false);
    let payer = Keypair::new();
    for _ in 0..2 {
        frame.tpu(&transfer(&payer, &Pubkey::new_unique()), false);
    }
    frame.scheduler.step().unwrap();
    for (index, request) in frame.check_requests().into_iter().enumerate() {
        let mut check = successful_check();
        check.prioritization_fee = if index == 0 { 1 } else { 100_000 };
        frame.respond_check_with(request, |_| check);
    }
    frame.scheduler.step().unwrap();
    let requests = frame.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!((requests[0].1.id, requests[1].1.id), (1, 0));
    for (worker, request) in requests {
        frame.finish(worker, request, reason::NONE);
    }
    frame.scheduler.step().unwrap();
    assert!(frame.scheduler.jobs.is_empty());
}

#[test]
fn legacy_retries_proven_uncommitted_work_then_returns_committed_batch_once() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, false);
    let batch = frame.submit(
        1,
        SOURCE_LEGACY_BUNDLE,
        true,
        u64::MAX,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.check_all();
    let (worker, first) = frame.requests().pop().unwrap();
    frame.finish(worker, first, reason::BANK_NOT_AVAILABLE);
    frame.scheduler.step().unwrap();
    assert!(
        frame
            .server
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .is_none()
    );
    assert_eq!(
        frame.scheduler.jobs[&batch.transactions_offset].state,
        State::PendingCheck
    );
    frame
        .scheduler
        .jobs
        .get_mut(&batch.transactions_offset)
        .unwrap()
        .check_after = Instant::now();
    // Same-bank lock/tip recovery is allowed after a fresh check; no allocation is replaced.
    frame.check_all();
    let (worker, retried) = frame.requests().pop().unwrap();
    assert_eq!((retried.id, retried.batch, retried.bank_id), (1, batch, 20));
    frame.finish(worker, retried, reason::NONE);
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    assert_eq!(completion.batch, batch);
    frame.free_completion(completion);
    assert!(frame.scheduler.jobs.is_empty());
    assert_eq!(frame.scheduler.stats.submitted_batches, 2);
}

#[test]
fn legacy_permanent_failure_is_returned_without_retry() {
    let mut frame = Frame::new(1);
    frame.progress(10, 20, true, false);
    frame.submit(
        1,
        SOURCE_LEGACY_BUNDLE,
        true,
        u64::MAX,
        &[transfer(&Keypair::new(), &Pubkey::new_unique())],
    );
    frame.check_all();
    let (worker, request) = frame.requests().pop().unwrap();
    frame.finish(worker, request, reason::SANITIZE_FAILURE);
    frame.scheduler.step().unwrap();
    let completion = frame.completion();
    frame.free_completion(completion);
    assert!(frame.check_requests().is_empty());
    assert!(frame.scheduler.jobs.is_empty());
}

#[test]
fn atomic_retry_requires_no_commits_and_a_transient_root_failure() {
    let value = |not_included_reason| {
        (
            JitoTransactionResult {
                not_included_reason,
                ..Default::default()
            },
            Vec::new(),
        )
    };
    assert!(retryable_atomic_failure(&[
        value(reason::ACCOUNT_IN_USE),
        value(reason::ALL_OR_NOTHING_BATCH_FAILURE)
    ]));
    assert!(!retryable_atomic_failure(&[
        value(reason::ACCOUNT_IN_USE),
        value(reason::NONE)
    ]));
    assert!(!retryable_atomic_failure(&[
        value(reason::BANK_NOT_AVAILABLE),
        value(reason::SANITIZE_FAILURE)
    ]));
    assert!(!retryable_atomic_failure(&[value(
        reason::ALL_OR_NOTHING_BATCH_FAILURE
    )]));
}

#[test]
fn alt_revalidation_preserves_order_across_lane_rounds() {
    let mut frame = Frame::new(2);
    frame.progress(10, 20, true, true);
    let payer = Keypair::new();
    for id in 1..=2 {
        frame.submit(
            id,
            SOURCE_BAM,
            false,
            10,
            &[transfer(&payer, &Pubkey::new_unique())],
        );
    }
    frame.scheduler.step().unwrap();
    for (index, request) in frame.check_requests().into_iter().enumerate() {
        let mut check = successful_check();
        if index == 0 {
            check.min_alt_deactivation_slot = 10;
        }
        frame.respond_check_with(request, |_| check);
    }
    frame.scheduler.step().unwrap();
    assert!(
        frame.requests().is_empty(),
        "later dependent batch overtook expired ALT recheck"
    );
    frame.scheduler.step().unwrap();
    let checks = frame.check_requests();
    assert_eq!(checks.len(), 1);
    frame.respond_check(checks[0], None);
    frame.scheduler.step().unwrap();
    let requests = frame.requests();
    assert_eq!(requests.len(), 2);
    assert_eq!((requests[0].1.id, requests[1].1.id), (1, 2));
    for (worker, request) in requests {
        frame.finish(worker, request, reason::NONE);
    }
    frame.scheduler.step().unwrap();
    for _ in 0..2 {
        let completion = frame.completion();
        frame.free_completion(completion);
    }
}
