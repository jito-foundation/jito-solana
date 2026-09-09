#![cfg(unix)]

// Share the authenticated network fixture with the LocalCluster binding tests.
#[path = "../../local-cluster/tests/scheduler_bindings_tests/bam_server.rs"]
mod bam_server;

use {
    arc_swap::ArcSwap,
    bam_server::BamServer,
    jito_protos::proto::bam_types::{
        AtomicTxnBatch, Meta, Packet, PacketFlags, atomic_txn_batch_result,
    },
    solana_commitment_config::CommitmentConfig,
    solana_core::validator::ValidatorConfig,
    solana_local_cluster::{
        cluster::Cluster,
        integration_tests::{DEFAULT_NODE_STAKE, RUST_LOG_FILTER},
        local_cluster::{ClusterConfig, DEFAULT_MINT_LAMPORTS, LocalCluster},
        validator_configs::make_identical_validator_configs,
    },
    solana_net_utils::SocketAddrSpace,
    solana_pubkey::Pubkey,
    solana_rent::Rent,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{
        process::{Child, Command},
        sync::{Arc, atomic::Ordering},
        thread::sleep,
        time::{Duration, Instant},
    },
};

struct RunningBinary(Option<Child>);

impl RunningBinary {
    fn stop(&mut self) {
        if let Some(mut child) = self.0.take() {
            child.kill().unwrap();
            child.wait().unwrap();
        }
    }

    fn assert_running(&mut self) {
        assert_eq!(
            self.0.as_mut().unwrap().try_wait().unwrap(),
            None,
            "scheduler executable exited before completing its authenticated stream"
        );
    }
}

impl Drop for RunningBinary {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A real executable must commit authenticated BAM work in a real validator.
/// Each mode change requires a new authenticated BAM stream, so checking the stream
/// count through confirmation prevents internal fallback from satisfying the external phase.
#[test]
fn binary_commits_authenticated_bam_and_returns_to_internal_scheduler() {
    agave_logger::setup_with_default(RUST_LOG_FILTER);
    let validator_config = ValidatorConfig {
        jito_scheduler_bindings: true,
        ..ValidatorConfig::default_for_test()
    };
    let bam_url: Arc<ArcSwap<Option<String>>> = validator_config.bam_url.clone();
    let cluster = LocalCluster::new(
        &mut ClusterConfig {
            node_stakes: vec![DEFAULT_NODE_STAKE],
            mint_lamports: DEFAULT_MINT_LAMPORTS,
            validator_configs: make_identical_validator_configs(&validator_config, 1),
            ticks_per_slot: 16,
            rent: Rent::default(),
            ..ClusterConfig::default()
        },
        SocketAddrSpace::Unspecified,
    );
    let identity = *cluster.entry_point_info.pubkey();
    let server = BamServer::start(identity);
    let ledger = &cluster.validators[&identity].info.ledger_path;
    let rpc = cluster
        .build_rpc_client_with_commitment(&identity, CommitmentConfig::confirmed())
        .unwrap();

    let transfer_and_confirm = |stream: usize, seq_id: u32| {
        let deadline = Instant::now() + Duration::from_secs(20);
        while server.service.authenticated.load(Ordering::Acquire) < stream {
            assert!(
                Instant::now() < deadline,
                "BAM did not authenticate stream {stream}"
            );
            sleep(Duration::from_millis(10));
        }
        assert_eq!(
            server.service.authenticated.load(Ordering::Acquire),
            stream,
            "unexpected BAM reconnect before submitting work"
        );
        while server.leaders.try_recv().is_ok() {}
        let leader = server
            .leaders
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
        assert!(leader.slot_cu_budget_remaining > 0);
        let recipient = Pubkey::new_unique();
        let amount = 1_000_000;
        let payer_balance = rpc.get_balance(&cluster.funding_keypair.pubkey()).unwrap();
        let transaction = system_transaction::transfer(
            &cluster.funding_keypair,
            &recipient,
            amount,
            rpc.get_latest_blockhash().unwrap(),
        );
        let signature = transaction.signatures[0];
        let fee = rpc.get_fee_for_message(&transaction.message).unwrap();
        let data = wincode::serialize(&transaction).unwrap();
        *server.service.batch.lock().unwrap() = Some(AtomicTxnBatch {
            seq_id,
            max_schedule_slot: leader.slot + 128,
            packets: vec![Packet {
                meta: Some(Meta {
                    size: u64::try_from(data.len()).unwrap(),
                    flags: Some(PacketFlags {
                        simple_vote_tx: false,
                        revert_on_error: true,
                    }),
                }),
                data: data.into(),
            }],
        });
        let result = server
            .results
            .recv_timeout(Duration::from_secs(20))
            .unwrap();
        assert_eq!(result.seq_id, seq_id);
        let Some(atomic_txn_batch_result::Result::Committed(committed)) = result.result else {
            panic!("stream {stream} did not commit BAM batch: {result:?}");
        };
        assert_eq!(committed.transaction_results.len(), 1);
        let metadata = &committed.transaction_results[0];
        assert!(metadata.execution_success);
        assert!(metadata.cus_consumed > 0);
        assert_eq!(
            metadata.feepayer_balance_lamports,
            payer_balance - amount - fee
        );
        // This signature is never submitted through RPC or a TPU connection.
        let deadline = Instant::now() + Duration::from_secs(45);
        loop {
            assert_eq!(
                server.service.authenticated.load(Ordering::Acquire),
                stream,
                "BAM reconnected before confirming the expected scheduling mode"
            );
            if let Some(result) = rpc
                .get_signature_status_with_commitment(&signature, CommitmentConfig::confirmed())
                .unwrap()
            {
                result.unwrap();
                assert_eq!(rpc.get_balance(&recipient).unwrap(), amount);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "BAM transaction {signature} from stream {stream} was not confirmed"
            );
            sleep(Duration::from_millis(50));
        }
    };

    bam_url.store(Arc::new(Some(server.url.clone())));
    transfer_and_confirm(1, 101);
    let mut external = RunningBinary(Some(
        Command::new(env!("CARGO_BIN_EXE_jito-scheduler"))
            .arg("--ledger")
            .arg(ledger)
            .args(["--workers", "2", "--check-workers", "2"])
            .spawn()
            .unwrap(),
    ));
    transfer_and_confirm(2, 102);
    external.assert_running();

    external.stop();
    // Killing the executable leaves no client to execute this batch. A fresh
    // authenticated stream and committed reply require the internal scheduler.
    transfer_and_confirm(3, 103);
}
