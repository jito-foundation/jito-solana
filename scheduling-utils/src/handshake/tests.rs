use {
    crate::handshake::{
        AgaveHandshakeError, ClientHandshakeError, ClientLogon, client::connect, server::Server,
        shared::MAX_WORKERS,
    },
    agave_scheduler_bindings::{
        CheckResponseRegion, CheckWorkerToPackMessage, ExecutionResponseRegion,
        ExecutionWorkerToPackMessage, PackToCheckWorkerMessage, PackToExecutionWorkerMessage,
        ProgressMessage, SharableTransactionBatchRegion, SharableTransactionRegion,
        TpuToPackMessage,
    },
    std::time::Duration,
    tempfile::NamedTempFile,
};

#[test]
fn message_passing_on_all_queues() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    // Test messages.
    let tpu_to_pack = TpuToPackMessage {
        transaction: SharableTransactionRegion {
            offset: 10,
            length: 5,
        },
        flags: 21,
        src_addr: [4; 16],
    };
    let progress_tracker = ProgressMessage {
        leader_state: agave_scheduler_bindings::LEADER_READY,
        current_slot_progress: 32,
        epoch: 7,
        current_slot: 3,
        next_leader_slot: 12,
        leader_range_end: 16,
        remaining_cost_units: 12_000_000,
        remaining_allocated_accounts_data_size: 20_000_000,
        latest_blockhash: [42; 32],
        target_bank_time_ms: 0,
    };
    let batch = SharableTransactionBatchRegion {
        num_transactions: 5,
        transactions_offset: 100,
    };
    let pack_to_check_worker = PackToCheckWorkerMessage { flags: 123, batch };
    let pack_to_worker = PackToExecutionWorkerMessage {
        flags: 1,
        max_working_slot: 100,
        batch,
    };
    let check_worker_to_pack = CheckWorkerToPackMessage {
        batch,
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: CheckResponseRegion {
            num_transaction_responses: 2,
            transaction_responses_offset: 1,
        },
    };
    let worker_to_pack = ExecutionWorkerToPackMessage {
        batch,
        processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
        responses: ExecutionResponseRegion {
            num_transaction_responses: 2,
            transaction_responses_offset: 1,
        },
    };

    let server_handle = std::thread::spawn(move || {
        let mut session = server.accept().unwrap();

        // Send a tpu_to_pack message.
        session.tpu_to_pack.producer.try_write(tpu_to_pack).unwrap();

        // Send a progress_tracker message.
        session
            .progress_tracker
            .try_write(progress_tracker)
            .unwrap();

        assert_eq!(session.check_workers.len(), 2);

        // Receive pack_to_check_worker messages.
        let mut check_messages = Vec::new();
        while check_messages.len() < session.check_workers.len() {
            for worker in &session.check_workers {
                if let Some(msg) = worker.pack_to_check_worker.try_read() {
                    check_messages.push(msg);
                }
            }
        }
        assert_eq!(
            check_messages,
            vec![
                pack_to_check_worker,
                PackToCheckWorkerMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: pack_to_check_worker.batch.num_transactions + 1,
                        ..pack_to_check_worker.batch
                    },
                    ..pack_to_check_worker
                }
            ]
        );

        // Send check_worker_to_pack messages.
        for (i, worker) in session.check_workers.iter().enumerate() {
            worker
                .check_worker_to_pack
                .try_write(CheckWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: check_worker_to_pack.batch.num_transactions + i as u8,
                        ..check_worker_to_pack.batch
                    },
                    ..check_worker_to_pack
                })
                .unwrap();
        }

        // Receive pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                if let Some(msg) = worker.pack_to_worker.try_read() {
                    break msg;
                }
            };
            assert_eq!(
                PackToExecutionWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                },
                msg
            );
        }

        // Send worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .worker_to_pack
                .try_write(ExecutionWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                })
                .unwrap();
        }
    });
    let client_handle = std::thread::spawn(move || {
        let mut session = connect(
            ipc,
            ClientLogon {
                worker_count: 4,
                check_worker_count: 2,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        )
        .unwrap();

        // Receive tpu_to_pack message.
        let msg = loop {
            if let Some(msg) = session.tpu_to_pack.try_read() {
                break msg;
            };
        };
        assert_eq!(msg, tpu_to_pack);

        // Receive progress_tracker message.
        let msg = loop {
            if let Some(msg) = session.progress_tracker.try_read() {
                break msg;
            };
        };
        assert_eq!(msg, progress_tracker);

        // Send pack_to_check_worker messages.
        for i in 0..2 {
            session
                .pack_to_check_worker
                .try_write(PackToCheckWorkerMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: pack_to_check_worker.batch.num_transactions + i,
                        ..pack_to_check_worker.batch
                    },
                    ..pack_to_check_worker
                })
                .unwrap();
        }

        // Receive check_worker_to_pack messages.
        let mut check_messages = Vec::new();
        while check_messages.len() < 2 {
            if let Some(msg) = session.check_worker_to_pack.try_read() {
                check_messages.push(msg);
            }
        }
        assert_eq!(
            check_messages,
            vec![
                check_worker_to_pack,
                CheckWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: check_worker_to_pack.batch.num_transactions + 1,
                        ..check_worker_to_pack.batch
                    },
                    ..check_worker_to_pack
                }
            ]
        );

        // Send pack_to_worker messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            worker
                .pack_to_worker
                .try_write(PackToExecutionWorkerMessage {
                    max_working_slot: pack_to_worker.max_working_slot + i as u64,
                    ..pack_to_worker
                })
                .unwrap();
        }

        // Receive worker_to_pack messages.
        for (i, worker) in session.workers.iter_mut().enumerate() {
            let msg = loop {
                if let Some(msg) = worker.worker_to_pack.try_read() {
                    break msg;
                }
            };
            assert_eq!(
                ExecutionWorkerToPackMessage {
                    batch: SharableTransactionBatchRegion {
                        num_transactions: worker_to_pack.batch.num_transactions + i as u8,
                        ..worker_to_pack.batch
                    },
                    ..worker_to_pack
                },
                msg
            );
        }
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn check_worker_queues_use_dedicated_capacities() {
    const CHECK_REQUEST_CAPACITY: usize = 1 << 18;
    const CHECK_RESPONSE_CAPACITY: usize = 1 << 19;

    let logon = ClientLogon {
        worker_count: 1,
        check_worker_count: 1,
        allocator_size: 64 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 2,
        progress_tracker_capacity: 2,
        pack_to_worker_capacity: 2,
        worker_to_pack_capacity: 2,
        flags: 0,
        pack_to_check_worker_capacity: CHECK_REQUEST_CAPACITY,
        check_worker_to_pack_capacity: CHECK_RESPONSE_CAPACITY,
    };
    let (_agave, files) = Server::setup_session(logon).unwrap();

    assert!(
        files[3].metadata().unwrap().len()
            >= u64::try_from(shaq::mpmc::minimum_file_size::<PackToCheckWorkerMessage>(
                CHECK_REQUEST_CAPACITY
            ))
            .unwrap()
    );
    assert!(
        files[4].metadata().unwrap().len()
            >= u64::try_from(shaq::mpmc::minimum_file_size::<CheckWorkerToPackMessage>(
                CHECK_RESPONSE_CAPACITY
            ))
            .unwrap()
    );

    crate::handshake::client::setup_session(&logon, files).unwrap();
}

#[test]
fn accept_worker_count_max() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        assert!(res.is_ok());
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: MAX_WORKERS,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        assert!(res.is_ok());
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_low() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 0);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 0,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=0");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_worker_count_high() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::WorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 100);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 100,
                check_worker_count: 1,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Worker count; count=100");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_check_worker_count_low() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::CheckWorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 0);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 1,
                check_worker_count: 0,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Check worker count; count=0");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

#[test]
fn reject_check_worker_count_high() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();

    let server_handle = std::thread::spawn(move || {
        let res = server.accept();
        let Err(AgaveHandshakeError::CheckWorkerCount(count)) = res else {
            panic!();
        };
        assert_eq!(count, 100);
    });
    let client_handle = std::thread::spawn(move || {
        let res = connect(
            ipc,
            ClientLogon {
                worker_count: 1,
                check_worker_count: 100,
                allocator_size: 1024 * 1024 * 1024,
                allocator_handles: 3,
                tpu_to_pack_capacity: 65536,
                progress_tracker_capacity: 256,
                pack_to_worker_capacity: 1024,
                worker_to_pack_capacity: 1024,
                flags: 0,
                pack_to_check_worker_capacity: 1024,
                check_worker_to_pack_capacity: 1024,
            },
            Duration::from_secs(1),
        );
        let Err(ClientHandshakeError::Rejected(reason)) = res else {
            panic!();
        };
        assert_eq!(reason, "Check worker count; count=100");
    });

    client_handle.join().unwrap();
    server_handle.join().unwrap();
}

fn small_logon(flags: u16) -> ClientLogon {
    ClientLogon {
        worker_count: 2,
        check_worker_count: 1,
        allocator_size: 64 * 1024 * 1024,
        allocator_handles: 1,
        tpu_to_pack_capacity: 16,
        progress_tracker_capacity: 16,
        pack_to_worker_capacity: 16,
        worker_to_pack_capacity: 16,
        flags,
        pack_to_check_worker_capacity: 16,
        check_worker_to_pack_capacity: 16,
    }
}

#[test]
fn jito_handshake_all_queues_and_shared_results() {
    use {
        crate::handshake::logon_flags,
        jito_scheduler_bindings::{
            JitoExecutionRequest, JitoExecutionResponse, JitoIngressMessage, JitoProgressMessage,
            JitoTransactionResult, SOURCE_BAM, allocate_batch, allocate_results, free_batch,
            free_results, read_results,
        },
    };
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();
    server.require_jito();
    let server_handle = std::thread::spawn(move || server.accept().unwrap());
    let mut client = connect(ipc, small_logon(logon_flags::JITO), Duration::from_secs(1)).unwrap();
    let mut agave = server_handle.join().unwrap();
    let root = agave.jito.as_mut().unwrap();
    let sidecar = client.jito.as_mut().unwrap();
    let batch = allocate_batch(&root.allocator, &[b"transaction".as_slice()]).unwrap();
    root.ingress
        .try_write(JitoIngressMessage {
            bam_generation: 0,
            id: 71,
            source: SOURCE_BAM,
            flags: 0,
            max_slot: 9,
            batch,
        })
        .unwrap();
    let ingress = sidecar.ingress.try_read().unwrap();
    assert_eq!(ingress.id, 71);
    assert_eq!(ingress.batch, batch);
    let progress = ProgressMessage {
        leader_state: agave_scheduler_bindings::LEADER_READY,
        current_slot_progress: 0,
        epoch: 0,
        current_slot: 9,
        next_leader_slot: 9,
        leader_range_end: 12,
        remaining_cost_units: 100,
        remaining_allocated_accounts_data_size: 100,
        latest_blockhash: [0; 32],
        target_bank_time_ms: 350,
    };
    root.progress
        .try_write(JitoProgressMessage {
            bam_generation: 0,
            progress,
            bank_id: 42,
            atomic_batches_enabled: 1,
            bam_connected: 1,
        })
        .unwrap();
    assert_eq!(sidecar.progress.try_read().unwrap().bank_id, 42);

    for (client_worker, agave_worker) in client.workers.iter_mut().zip(&mut agave.workers) {
        let request = JitoExecutionRequest {
            bam_generation: 0,
            id: 71,
            source: SOURCE_BAM,
            flags: 0,
            slot: 9,
            bank_id: 42,
            batch,
        };
        client_worker
            .jito
            .as_mut()
            .unwrap()
            .request
            .try_write(request)
            .unwrap();
        let received = agave_worker
            .jito
            .as_mut()
            .unwrap()
            .request
            .try_read()
            .unwrap();
        assert_eq!(received.bank_id, 42);
        assert_eq!(received.batch, batch);
        let responses = allocate_results(
            &agave_worker.allocator,
            &[(
                JitoTransactionResult {
                    executed_units: 55,
                    execution_success: 1,
                    ..JitoTransactionResult::default()
                },
                b"diagnostic",
            )],
        )
        .unwrap();
        agave_worker
            .jito
            .as_mut()
            .unwrap()
            .response
            .try_write(JitoExecutionResponse {
                id: request.id,
                batch,
                processed_code: agave_scheduler_bindings::processed_codes::PROCESSED,
                execution_slot: 9,
                bank_id: 42,
                responses,
            })
            .unwrap();
        let response = client_worker
            .jito
            .as_mut()
            .unwrap()
            .response
            .try_read()
            .unwrap();
        // SAFETY: worker transferred a live result allocation to the sidecar.
        let decoded = unsafe { read_results(&client.allocators[0], &response.responses) }.unwrap();
        assert_eq!(decoded[0].0.executed_units, 55);
        assert_eq!(decoded[0].1, b"diagnostic");
        sidecar.completion.try_write(response).unwrap();
        let completion = root.completion.try_read().unwrap();
        assert_eq!(completion.id, ingress.id);
        // SAFETY: completion transfers exclusive result ownership back to the validator.
        unsafe { free_results(&root.allocator, completion.responses) };
    }
    // SAFETY: both simulated workers have finished; root still owns the input.
    unsafe { free_batch(&root.allocator, batch) };
}

#[test]
fn ordinary_handshake_has_no_jito_descriptors() {
    let ipc = NamedTempFile::new().unwrap();
    std::fs::remove_file(ipc.path()).unwrap();
    let mut server = Server::new(ipc.path()).unwrap();
    let server_handle = std::thread::spawn(move || server.accept().unwrap());
    let client = connect(ipc, small_logon(0), Duration::from_secs(1)).unwrap();
    let agave = server_handle.join().unwrap();
    assert!(client.jito.is_none());
    assert!(agave.jito.is_none());
    assert!(client.workers.iter().all(|worker| worker.jito.is_none()));
    assert!(agave.workers.iter().all(|worker| worker.jito.is_none()));
    let (_, files) = Server::setup_session(small_logon(0)).unwrap();
    assert_eq!(files.len(), 9);
}

#[test]
fn rejects_missing_unknown_or_wrong_version_jito_before_success() {
    use crate::handshake::logon_flags;
    for (flags, required, expected) in [
        (0, true, "Jito scheduler addon required"),
        (2, false, "Unsupported logon flags"),
        (logon_flags::JITO ^ 0x300, false, "Unsupported logon flags"),
    ] {
        let ipc = NamedTempFile::new().unwrap();
        std::fs::remove_file(ipc.path()).unwrap();
        let mut server = Server::new(ipc.path()).unwrap();
        if required {
            server.require_jito();
        }
        let server_handle = std::thread::spawn(move || assert!(server.accept().is_err()));
        let result = connect(ipc, small_logon(flags), Duration::from_secs(1));
        let Err(ClientHandshakeError::Rejected(reason)) = result else {
            panic!("expected explicit rejection before descriptor receipt");
        };
        assert!(reason.starts_with(expected), "{reason}");
        server_handle.join().unwrap();
    }
}

#[test]
fn rejects_jito_worker_count_above_descriptor_limit() {
    let mut logon = small_logon(crate::handshake::logon_flags::JITO);
    logon.worker_count = crate::handshake::MAX_JITO_WORKERS + 1;
    assert!(matches!(
        Server::setup_session(logon),
        Err(AgaveHandshakeError::WorkerCount(_))
    ));
}
