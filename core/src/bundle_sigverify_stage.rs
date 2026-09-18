use {
    crate::packet_bundle::{PacketBundle, VerifiedPacketBundle},
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    rayon::ThreadPool,
    solana_perf::sigverify::ed25519_verify,
    solana_runtime::bank_forks::SharableBanks,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, JoinHandle, spawn},
        time::{Duration, Instant},
    },
};

pub struct BundleSigverifyStage {
    thread: JoinHandle<()>,
}

impl BundleSigverifyStage {
    pub fn new(
        thread_pool: Arc<ThreadPool>,
        receiver: Receiver<Vec<PacketBundle>>,
        sender: Sender<VerifiedPacketBundle>,
        exit: Arc<AtomicBool>,
        sharable_banks: SharableBanks,
    ) -> Self {
        let thread = spawn(move || {
            Self::sigverify_service(thread_pool, receiver, sender, exit, sharable_banks)
        });
        Self { thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }

    fn sigverify_service(
        thread_pool: Arc<ThreadPool>,
        receiver: Receiver<Vec<PacketBundle>>,
        sender: Sender<VerifiedPacketBundle>,
        exit: Arc<AtomicBool>,
        sharable_banks: SharableBanks,
    ) {
        let mut workspace = Vec::with_capacity(100);

        let mut num_packets_received: usize = 0;
        let mut num_bundles_received: usize = 0;
        let mut num_bundles_failed_sigverify: usize = 0;
        let mut num_packets_failed_sigverify: usize = 0;
        let mut num_bundles_failed_send: usize = 0;
        let mut num_packets_failed_send: usize = 0;
        let mut last_update = Instant::now();

        while !exit.load(Ordering::Relaxed) {
            let bundles = match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(bundles) => bundles,
                Err(RecvTimeoutError::Timeout) => {
                    if (num_bundles_received > 0 || num_packets_received > 0)
                        && last_update.elapsed().as_millis() > 20
                    {
                        datapoint_info!(
                            "bundle_sigverify_stage",
                            ("num_bundles_received", num_bundles_received, i64),
                            ("num_packets_received", num_packets_received, i64),
                            (
                                "num_bundles_failed_sigverify",
                                num_bundles_failed_sigverify,
                                i64
                            ),
                            (
                                "num_packets_failed_sigverify",
                                num_packets_failed_sigverify,
                                i64
                            ),
                            ("num_bundles_failed_send", num_bundles_failed_send, i64),
                            ("num_packets_failed_send", num_packets_failed_send, i64),
                        );
                        num_packets_received = 0;
                        num_bundles_received = 0;
                        num_bundles_failed_sigverify = 0;
                        num_packets_failed_sigverify = 0;
                        num_bundles_failed_send = 0;
                        num_packets_failed_send = 0;
                        last_update = Instant::now();
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            };

            workspace.extend(bundles.into_iter().map(|bundle| bundle.take()));

            let packet_count: usize = workspace.iter().map(|bundle| bundle.len()).sum();

            num_bundles_received += workspace.len();
            num_packets_received += packet_count;

            let enable_tx_v1 = sharable_banks.working().feature_set.snapshot().enable_tx_v1;
            ed25519_verify(
                &thread_pool,
                &mut workspace,
                false,
                packet_count,
                enable_tx_v1,
            );

            for bundle in workspace.drain(..) {
                let num_packets_failed_sigverify_in_bundle = bundle
                    .iter()
                    .filter(|packet| packet.meta().discard())
                    .count();

                // all the transactions in the bundle need to be verified to be valid
                let len = bundle.len();
                if num_packets_failed_sigverify_in_bundle == 0
                    && sender.send(VerifiedPacketBundle::new(bundle)).is_err()
                {
                    warn!("failed to send verified packet bundle");
                    num_bundles_failed_send += 1;
                    num_packets_failed_send += len;
                    break;
                } else if num_packets_failed_sigverify_in_bundle > 0 {
                    num_bundles_failed_sigverify += 1;
                    num_packets_failed_sigverify += num_packets_failed_sigverify_in_bundle;
                }
            }

            if (num_bundles_received > 0 || num_packets_received > 0)
                && last_update.elapsed().as_millis() > 20
            {
                datapoint_info!(
                    "bundle_sigverify_stage",
                    ("num_bundles_received", num_bundles_received, i64),
                    ("num_packets_received", num_packets_received, i64),
                    (
                        "num_bundles_failed_sigverify",
                        num_bundles_failed_sigverify,
                        i64
                    ),
                    (
                        "num_packets_failed_sigverify",
                        num_packets_failed_sigverify,
                        i64
                    ),
                    ("num_bundles_failed_send", num_bundles_failed_send, i64),
                    ("num_packets_failed_send", num_packets_failed_send, i64),
                );
                num_packets_received = 0;
                num_bundles_received = 0;
                num_bundles_failed_sigverify = 0;
                num_packets_failed_sigverify = 0;
                num_bundles_failed_send = 0;
                num_packets_failed_send = 0;
                last_update = Instant::now();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::bounded,
        solana_genesis_config::create_genesis_config,
        solana_hash::Hash,
        solana_instruction::Instruction,
        solana_keypair::{Keypair, Signature},
        solana_message::{VersionedMessage, v1},
        solana_packet::PACKET_DATA_SIZE,
        solana_perf::{
            packet::{BytesPacket, PacketBatch},
            test_tx::test_tx,
        },
        solana_runtime::bank::Bank,
        solana_signature::SIGNATURE_BYTES,
        solana_signer::Signer,
        solana_system_interface::program as system_program,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
        test_case::test_case,
    };

    fn test_sharable_banks() -> SharableBanks {
        let (genesis_config, _) = create_genesis_config(1);
        let (_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        bank_forks.read().unwrap().sharable_banks()
    }

    fn test_sharable_banks_with_tx_v1(enable_tx_v1: bool) -> SharableBanks {
        let (genesis_config, _) = create_genesis_config(1);
        let mut bank = Bank::new_for_tests(&genesis_config);
        if enable_tx_v1 {
            bank.activate_feature(&agave_feature_set::enable_tx_v1::id());
        } else {
            bank.deactivate_feature(&agave_feature_set::enable_tx_v1::id());
        }
        let (_bank, bank_forks) = bank.wrap_with_bank_forks_for_tests();
        bank_forks.read().unwrap().sharable_banks()
    }

    fn v1_transaction_with_wire_size(target_size: usize) -> VersionedTransaction {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();
        let base_instruction = Instruction::new_with_bytes(system_program::id(), &[], vec![]);
        let base_message =
            v1::Message::try_compile(&payer.pubkey(), &[base_instruction], blockhash)
                .expect("compile base v1 message");
        let signature_count = usize::from(base_message.header.num_required_signatures);
        let base_wire_size = core::mem::size_of_val(&v1::V1_PREFIX)
            + base_message.size()
            + signature_count * SIGNATURE_BYTES;
        let padding_len = target_size
            .checked_sub(base_wire_size)
            .expect("target size can fit v1 transaction padding");
        let padding = vec![0; padding_len];
        let instruction = Instruction::new_with_bytes(system_program::id(), &padding, vec![]);
        let message = v1::Message::try_compile(&payer.pubkey(), &[instruction], blockhash)
            .expect("compile padded v1 message");
        let transaction = VersionedTransaction::try_new(VersionedMessage::V1(message), &[&payer])
            .expect("sign v1 transaction");

        assert_eq!(
            wincode::serialize(&transaction)
                .expect("serialize v1 transaction")
                .len(),
            target_size
        );
        transaction
    }

    #[test]
    fn test_bundle_sigverify_stage_exit() {
        let (_unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, _verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));
        let thread_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let stage = BundleSigverifyStage::new(
            thread_pool,
            unverified_receiver,
            verified_sender,
            exit.clone(),
            test_sharable_banks(),
        );
        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test]
    fn test_bundle_sigverify_stage_many_packets_all_valid() {
        let (unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));

        let txs_1 = (0..3).map(|_| test_tx()).collect::<Vec<_>>();
        let packet_bundle_1 = PacketBundle::new(
            PacketBatch::from(
                txs_1
                    .iter()
                    .map(|tx| BytesPacket::from_data(tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        let txs_2 = (0..4).map(|_| test_tx()).collect::<Vec<_>>();
        let packet_bundle_2 = PacketBundle::new(
            PacketBatch::from(
                txs_2
                    .iter()
                    .map(|tx| BytesPacket::from_data(tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        unverified_sender
            .send(vec![packet_bundle_1, packet_bundle_2])
            .unwrap();

        let thread_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let stage = BundleSigverifyStage::new(
            thread_pool,
            unverified_receiver,
            verified_sender,
            exit.clone(),
            test_sharable_banks(),
        );

        let verified_bundle_1 = verified_receiver.recv().unwrap();
        assert_eq!(verified_bundle_1.batch().len(), 3);
        assert!(
            verified_bundle_1
                .batch()
                .iter()
                .all(|packet| !packet.meta().discard())
        );
        let txs_1_after: Vec<Transaction> = verified_bundle_1
            .batch()
            .iter()
            .map(|packet| bincode::deserialize(packet.data(..).unwrap()).unwrap())
            .collect();
        assert_eq!(txs_1, txs_1_after);

        let verified_bundle_2 = verified_receiver.recv().unwrap();
        assert_eq!(verified_bundle_2.batch().len(), 4);
        assert!(
            verified_bundle_2
                .batch()
                .iter()
                .all(|packet| !packet.meta().discard())
        );
        let txs_2_after: Vec<Transaction> = verified_bundle_2
            .batch()
            .iter()
            .map(|packet| bincode::deserialize(packet.data(..).unwrap()).unwrap())
            .collect();
        assert_eq!(txs_2, txs_2_after);

        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test]
    fn test_bundle_sigverify_stage_many_packets_some_invalid() {
        let (unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));

        let mut txs_1 = (0..3).map(|_| test_tx()).collect::<Vec<_>>();
        txs_1[0].signatures[0] = Signature::default();

        let packet_bundle_1 = PacketBundle::new(
            PacketBatch::from(
                txs_1
                    .iter()
                    .map(|tx| BytesPacket::from_data(tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        unverified_sender.send(vec![packet_bundle_1]).unwrap();

        let thread_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let stage = BundleSigverifyStage::new(
            thread_pool,
            unverified_receiver,
            verified_sender,
            exit.clone(),
            test_sharable_banks(),
        );

        assert_eq!(
            verified_receiver
                .recv_timeout(Duration::from_millis(10))
                .unwrap_err(),
            RecvTimeoutError::Timeout
        );

        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test_case(true, true; "tx_v1_enabled")]
    #[test_case(false, false; "tx_v1_disabled")]
    fn test_bundle_sigverify_stage_tx_v1_feature_gate(
        enable_tx_v1: bool,
        expected_verified_bundle: bool,
    ) {
        let (unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));
        let transaction = v1_transaction_with_wire_size(PACKET_DATA_SIZE + 1);
        let packet_bundle = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_bytes(
                None,
                wincode::serialize(&transaction).unwrap(),
            )]),
            "".to_string(),
        );

        unverified_sender.send(vec![packet_bundle]).unwrap();

        let thread_pool = Arc::new(rayon::ThreadPoolBuilder::new().build().unwrap());
        let stage = BundleSigverifyStage::new(
            thread_pool,
            unverified_receiver,
            verified_sender,
            exit.clone(),
            test_sharable_banks_with_tx_v1(enable_tx_v1),
        );

        let timeout = if expected_verified_bundle {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(100)
        };
        let verified_bundle = verified_receiver.recv_timeout(timeout);
        assert_eq!(verified_bundle.is_ok(), expected_verified_bundle);
        if let Ok(verified_bundle) = verified_bundle {
            assert_eq!(verified_bundle.batch().len(), 1);
            assert!(!verified_bundle.batch().get(0).unwrap().meta().discard());
        }

        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }
}
