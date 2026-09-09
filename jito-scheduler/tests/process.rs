#![cfg(unix)]

use {
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, LEADER_READY, ProgressMessage, SharablePubkeys, processed_codes,
        worker_message_types::{CheckResponse, not_included_reasons},
    },
    agave_scheduling_utils::{
        handshake::server::Server, responses_region::resolve_responses_from_iter,
    },
    jito_scheduler_bindings::{
        JitoExecutionResponse, JitoIngressMessage, JitoProgressMessage, JitoTransactionResult,
        SOURCE_BAM, allocate_batch, allocate_results, free_batch, free_results, read_results,
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_pubkey::Pubkey,
    std::{
        process::{Child, Command},
        time::{Duration, Instant},
    },
};

struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn wait<T>(mut poll: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(value) = poll() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "child scheduler failed to make progress"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Exercise the built executable in a distinct process: real socket negotiation, transferred
/// file descriptors, shared input, check request/reply, execution request/reply, and completion.
#[test]
fn binary_round_trip_over_real_unix_socket() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("scheduler.ipc");
    let mut server = Server::new(&path).unwrap();
    let _child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_jito-scheduler"))
            .arg("--ipc-path")
            .arg(&path)
            .args(["--workers", "2", "--check-workers", "1"])
            .spawn()
            .unwrap(),
    );
    let mut session = server.accept().unwrap();
    let progress = JitoProgressMessage {
        bam_generation: 123,
        progress: ProgressMessage {
            leader_state: LEADER_READY,
            current_slot_progress: 1,
            epoch: 0,
            current_slot: 42,
            next_leader_slot: 43,
            leader_range_end: 45,
            remaining_cost_units: 60_000_000,
            remaining_allocated_accounts_data_size: 100_000_000,
            latest_blockhash: [0; 32],
            target_bank_time_ms: 400,
        },
        bank_id: 1234,
        atomic_batches_enabled: 1,
        bam_connected: 1,
    };
    session
        .jito
        .as_mut()
        .unwrap()
        .progress
        .try_write(progress)
        .unwrap();
    let transaction = solana_system_transaction::transfer(
        &Keypair::new(),
        &Pubkey::new_unique(),
        1,
        Hash::new_from_array([9; 32]),
    );
    let bytes = wincode::serialize(&transaction).unwrap();
    let batch = allocate_batch(&session.tpu_to_pack.allocator, &[bytes]).unwrap();
    session
        .jito
        .as_mut()
        .unwrap()
        .ingress
        .try_write(JitoIngressMessage {
            bam_generation: 123,
            id: 99,
            source: SOURCE_BAM,
            flags: 3,
            max_slot: 42,
            batch,
        })
        .unwrap();
    let check = wait(|| session.check_workers[0].pack_to_check_worker.try_read());
    assert_eq!(check.batch, batch);
    let check_result = CheckResponse {
        parsing_and_sanitization_flags: 0,
        status_check_flags: 3,
        fee_payer_balance_flags: 0,
        resolve_flags: 3,
        scheduling_details_flags: 3,
        included_slot: 0,
        transaction_fee: 5000,
        prioritization_fee: 0,
        estimated_cost_units: 1000,
        allocated_accounts_data_size: 0,
        balance_slot: 42,
        fee_payer_balance: 100_000,
        resolution_slot: 42,
        min_alt_deactivation_slot: u64::MAX,
        resolved_pubkeys: SharablePubkeys {
            offset: 0,
            num_pubkeys: 0,
        },
    };
    let responses = resolve_responses_from_iter(
        &session.check_workers[0].allocator,
        std::iter::once(check_result),
    )
    .unwrap();
    session.check_workers[0]
        .check_worker_to_pack
        .try_write(CheckWorkerToPackMessage {
            batch,
            processed_code: processed_codes::PROCESSED,
            responses,
        })
        .unwrap();
    let (worker, request) = wait(|| {
        session
            .workers
            .iter_mut()
            .enumerate()
            .find_map(|(worker, session)| {
                session
                    .jito
                    .as_mut()
                    .unwrap()
                    .request
                    .try_read()
                    .map(|request| (worker, request))
            })
    });
    assert_eq!(
        (request.id, request.slot, request.bank_id, request.flags),
        (99, 42, 1234, 3)
    );
    assert_eq!(request.bam_generation, 123);
    let results = allocate_results(
        &session.workers[worker].allocator,
        &[(
            JitoTransactionResult {
                not_included_reason: not_included_reasons::NONE,
                executed_units: 55,
                loaded_accounts_data_size: 66,
                fee_payer_balance: 77,
                execution_success: 1,
                ..Default::default()
            },
            &b"exact diagnostic"[..],
        )],
    )
    .unwrap();
    session.workers[worker]
        .jito
        .as_mut()
        .unwrap()
        .response
        .try_write(JitoExecutionResponse {
            id: 99,
            batch,
            processed_code: processed_codes::PROCESSED,
            execution_slot: 42,
            bank_id: 1234,
            responses: results,
        })
        .unwrap();
    let completion = wait(|| {
        session
            .jito
            .as_mut()
            .unwrap()
            .completion
            .try_read()
            .filter(|message| message.id != 0)
    });
    assert_eq!((completion.id, completion.batch), (99, batch));
    let results =
        unsafe { read_results(&session.tpu_to_pack.allocator, &completion.responses) }.unwrap();
    assert_eq!(results[0].0.executed_units, 55);
    assert_eq!(results[0].0.loaded_accounts_data_size, 66);
    assert_eq!(results[0].1, b"exact diagnostic");
    unsafe {
        free_results(&session.tpu_to_pack.allocator, completion.responses);
        free_batch(&session.tpu_to_pack.allocator, completion.batch);
    }
    // A second control message proves the idle child is still consuming the parent queue.
    session
        .jito
        .as_mut()
        .unwrap()
        .progress
        .try_write(progress)
        .unwrap();
    let heartbeat = wait(|| session.jito.as_mut().unwrap().completion.try_read());
    assert_eq!((heartbeat.id, heartbeat.batch.num_transactions), (0, 0));

    // Lose the validator heartbeat while a check still owns an input. The executable must
    // establish fresh mappings and must not replay this ambiguous old-session work.
    let stranded = allocate_batch(
        &session.tpu_to_pack.allocator,
        &[wincode::serialize(&transaction).unwrap()],
    )
    .unwrap();
    session
        .jito
        .as_mut()
        .unwrap()
        .ingress
        .try_write(JitoIngressMessage {
            id: 100,
            source: SOURCE_BAM,
            flags: 3,
            max_slot: 42,
            bam_generation: 123,
            batch: stranded,
        })
        .unwrap();
    let stranded_check = wait(|| session.check_workers[0].pack_to_check_worker.try_read());
    assert_eq!(stranded_check.batch, stranded);
    let (accepted, next_session) = std::sync::mpsc::sync_channel(1);
    let accept_thread = std::thread::spawn(move || accepted.send(server.accept()).unwrap());
    let mut fresh = next_session
        .recv_timeout(Duration::from_secs(8))
        .expect("child must reconnect after heartbeat loss")
        .unwrap();
    accept_thread.join().unwrap();
    fresh
        .jito
        .as_mut()
        .unwrap()
        .progress
        .try_write(JitoProgressMessage {
            bam_generation: 124,
            bank_id: 1235,
            ..progress
        })
        .unwrap();
    let heartbeat = wait(|| fresh.jito.as_mut().unwrap().completion.try_read());
    assert_eq!((heartbeat.id, heartbeat.bank_id), (0, 1235));
    assert!(
        fresh.check_workers[0]
            .pack_to_check_worker
            .try_read()
            .is_none()
    );
    assert!(
        fresh.workers.iter_mut().all(|worker| worker
            .jito
            .as_mut()
            .unwrap()
            .request
            .try_read()
            .is_none())
    );
    // This test has no actual old check worker; its retained server mapping still owns the batch.
    unsafe {
        free_batch(&session.tpu_to_pack.allocator, stranded);
    }
}
