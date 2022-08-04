///! Turns packets into SanitizedTransactions and ensure they pass sanity checks
use {
    crate::{
        bundle::PacketBundle,
        unprocessed_packet_batches::{deserialize_packets, ImmutableDeserializedPacket},
    },
    solana_perf::sigverify::verify_packet,
    solana_runtime::{bank::Bank, transaction_error_metrics::TransactionErrorMetrics},
    solana_sdk::{
        bundle::sanitized::SanitizedBundle,
        clock::MAX_PROCESSING_AGE,
        feature_set::FeatureSet,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{AddressLoader, SanitizedTransaction},
    },
    std::{
        collections::{hash_map::RandomState, HashSet},
        iter::repeat,
        sync::Arc,
    },
    thiserror::Error,
    uuid::Uuid,
};

pub const MAX_PACKETS_PER_BUNDLE: usize = 5;

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum BundleSchedulerError {
    #[error("Bundle locking error uuid: {0}")]
    LockingError(Uuid),
    #[error("Bundle contains a transaction that failed to serialize: {0}")]
    FailedToSerializeTransaction(Uuid),
    #[error("Bundle contains a duplicate transaction: {0}")]
    DuplicateTransaction(Uuid),
    #[error("Bundle failed check results: {0}")]
    FailedCheckResults(Uuid),
    #[error("Bundle packet batch failed pre-check: {0}")]
    FailedPacketBatchPreCheck(Uuid),
    #[error("Bank is in vote-only mode: {0}")]
    VoteOnlyMode(Uuid),
    #[error("Bundle mentions blacklisted account: {0}")]
    BlacklistedAccount(Uuid),
}

pub type Result<T> = std::result::Result<T, BundleSchedulerError>;

#[derive(Clone)]
pub struct BundleSanitizer {
    blacklisted_accounts: HashSet<Pubkey>,
}

impl BundleSanitizer {
    pub fn new(tip_program_id: &Pubkey) -> BundleSanitizer {
        BundleSanitizer {
            blacklisted_accounts: HashSet::from([
                // prevent bundles from changing the tip_receiver unexpectedly and stealing
                // all of the MEV profits from a validator and stakers.
                *tip_program_id,
            ]),
        }
    }

    /// An invalid bundle contains one of the following:
    ///  No packets.
    ///  Too many packets.
    ///  Packets marked for discard (not sure why someone would do this)
    ///  One of the packets fails signature verification.
    ///  Mentions an account in consensus or blacklisted accounts.
    ///  Contains a packet that failed to serialize to a transaction.
    ///  Contains duplicate transactions within the same bundle.
    ///  Contains a transaction that was already processed or one with an invalid blockhash.
    pub fn get_sanitized_bundle(
        bundle: &PacketBundle,
        bank: &Arc<Bank>,
        blacklisted_accounts: &HashSet<Pubkey>,
        consensus_accounts_cache: &HashSet<Pubkey>,
    ) -> Result<SanitizedBundle> {
        if bank.vote_only_bank() {
            return Err(BundleSchedulerError::VoteOnlyMode(bundle.uuid));
        }

        if bundle.batch.is_empty()
            || bundle.batch.len() > MAX_PACKETS_PER_BUNDLE
            || bundle.batch.iter().any(|p| p.meta.discard())
            || bundle
                .batch
                .iter()
                .any(|p| !verify_packet(&mut p.clone(), false))
        {
            return Err(BundleSchedulerError::FailedPacketBatchPreCheck(bundle.uuid));
        }

        let packet_indexes: Vec<usize> = (0..bundle.batch.len()).collect();
        let deserialized_packets = deserialize_packets(&bundle.batch, &packet_indexes);
        let transactions: Vec<SanitizedTransaction> = deserialized_packets
            .filter_map(|p| {
                let immutable_packet = p.immutable_section().clone();
                Self::transaction_from_deserialized_packet(
                    &immutable_packet,
                    &bank.feature_set,
                    bank.as_ref(),
                )
            })
            .collect();

        let unique_signatures: HashSet<&Signature, RandomState> =
            HashSet::from_iter(transactions.iter().map(|tx| tx.signature()));
        let contains_blacklisted_account = transactions.iter().any(|tx| {
            let accounts = tx.message.account_keys();
            accounts.iter().any(|acc| {
                blacklisted_accounts.contains(acc) || consensus_accounts_cache.contains(acc)
            })
        });

        if contains_blacklisted_account {
            return Err(BundleSchedulerError::BlacklistedAccount(bundle.uuid));
        }

        if transactions.is_empty() || bundle.batch.len() != transactions.len() {
            return Err(BundleSchedulerError::FailedToSerializeTransaction(
                bundle.uuid,
            ));
        }

        if unique_signatures.len() != transactions.len() {
            return Err(BundleSchedulerError::DuplicateTransaction(bundle.uuid));
        }

        // checks for already-processed transaction or expired/invalid blockhash
        let lock_results: Vec<_> = repeat(Ok(())).take(transactions.len()).collect();
        let mut metrics = TransactionErrorMetrics::default();
        let check_results = bank.check_transactions(
            &transactions,
            &lock_results,
            MAX_PROCESSING_AGE,
            &mut metrics,
        );
        if let Some(failure) = check_results.iter().find(|r| r.0.is_err()) {
            error!("failed: {:?}", failure);
            return Err(BundleSchedulerError::FailedCheckResults(bundle.uuid));
        }

        Ok(SanitizedBundle {
            transactions,
            uuid: bundle.uuid,
        })
    }

    // This function deserializes packets into transactions, computes the blake3 hash of transaction
    // messages, and verifies secp256k1 instructions. A list of sanitized transactions are returned
    // with their packet indexes.
    // NOTES on tx v2:
    // - tx v2 can only load addresses set in previous slots
    // - tx v2 can't reorg indices in a lookup table
    // - tx v2 transaction loading fails if it tries to access an invalid index (either doesn't exist
    //   or exists but was set in the current slot
    #[allow(clippy::needless_collect)]
    fn transaction_from_deserialized_packet(
        deserialized_packet: &ImmutableDeserializedPacket,
        feature_set: &Arc<FeatureSet>,
        address_loader: impl AddressLoader,
    ) -> Option<SanitizedTransaction> {
        let tx = SanitizedTransaction::try_new(
            deserialized_packet.transaction().clone(),
            *deserialized_packet.message_hash(),
            deserialized_packet.is_simple_vote(),
            address_loader,
        )
        .ok()?;
        tx.verify_precompiles(feature_set).ok()?;
        Some(tx)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            bundle::PacketBundle,
            bundle_sanitizer::{BundleSanitizer, MAX_PACKETS_PER_BUNDLE},
            tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
        },
        solana_address_lookup_table_program::instruction::create_lookup_table,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            hash::Hash,
            instruction::Instruction,
            packet::Packet,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_program,
            system_transaction::transfer,
            transaction::{SanitizedTransaction, Transaction, VersionedTransaction},
        },
        std::{collections::HashSet, sync::Arc},
        uuid::Uuid,
    };

    #[test]
    fn test_single_tx_bundle_push_pop() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        let locked_bundle = bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .unwrap();
        assert_eq!(locked_bundle.sanitized_bundle.transactions.len(), 1);
        assert_eq!(
            locked_bundle.sanitized_bundle.transactions[0].signature(),
            &tx.signatures[0]
        );

        assert_eq!(
            bundle_locker_sanitizer.read_locks(),
            HashSet::from([system_program::id()])
        );
        assert_eq!(
            bundle_locker_sanitizer.write_locks(),
            HashSet::from([mint_keypair.pubkey(), kp.pubkey()])
        );

        bundle_locker_sanitizer.unlock_bundle_accounts(&locked_bundle);
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_multi_tx_bundle_push_pop() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();

        let tx1 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp1.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let tx2 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp2.pubkey(),
            1,
            genesis_config.hash(),
        ));

        let packet1 = Packet::from_data(None, &tx1).unwrap();
        let packet2 = Packet::from_data(None, &tx2).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet1, packet2]),
            uuid: Uuid::new_v4(),
        };

        let locked_bundle = bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .unwrap();
        assert_eq!(locked_bundle.sanitized_bundle.transactions.len(), 2);
        assert_eq!(
            locked_bundle.sanitized_bundle.transactions[0].signature(),
            &tx1.signatures[0]
        );
        assert_eq!(
            locked_bundle.sanitized_bundle.transactions[1].signature(),
            &tx2.signatures[0]
        );

        assert_eq!(
            bundle_locker_sanitizer.read_locks(),
            HashSet::from([system_program::id()])
        );
        assert_eq!(
            bundle_locker_sanitizer.write_locks(),
            HashSet::from([mint_keypair.pubkey(), kp1.pubkey(), kp2.pubkey()])
        );

        bundle_locker_sanitizer.unlock_bundle_accounts(&locked_bundle);
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_multi_bundle_push_pop() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp1 = Keypair::new();
        let kp2 = Keypair::new();

        let tx1 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp1.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let tx2 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp2.pubkey(),
            1,
            genesis_config.hash(),
        ));

        let packet1 = Packet::from_data(None, &tx1).unwrap();
        let packet2 = Packet::from_data(None, &tx2).unwrap();

        let packet_bundle_1 = PacketBundle {
            batch: PacketBatch::new(vec![packet1]),
            uuid: Uuid::new_v4(),
        };

        let packet_bundle_2 = PacketBundle {
            batch: PacketBatch::new(vec![packet2]),
            uuid: Uuid::new_v4(),
        };

        let locked_bundle_1 = bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle_1, &bank, &HashSet::default())
            .unwrap();
        let locked_bundle_2 = bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle_2, &bank, &HashSet::default())
            .unwrap();
        assert_eq!(locked_bundle_1.sanitized_bundle.transactions.len(), 1);
        assert_eq!(
            locked_bundle_1.sanitized_bundle.transactions[0].signature(),
            &tx1.signatures[0]
        );

        assert_eq!(
            bundle_locker_sanitizer.read_locks(),
            HashSet::from([system_program::id()])
        );
        // pre-lock is being used, so kp2 in packet_bundle_2 is locked ahead of time
        assert_eq!(
            bundle_locker_sanitizer.write_locks(),
            HashSet::from([mint_keypair.pubkey(), kp1.pubkey(), kp2.pubkey()])
        );

        bundle_locker_sanitizer.unlock_bundle_accounts(&locked_bundle_1);

        // packet_bundle_1 is unlocked, so the lock should just contain contents for packet_bundle_2
        assert_eq!(
            bundle_locker_sanitizer.read_locks(),
            HashSet::from([system_program::id()])
        );
        assert_eq!(
            bundle_locker_sanitizer.write_locks(),
            HashSet::from([mint_keypair.pubkey(), kp2.pubkey()])
        );

        assert_eq!(locked_bundle_2.sanitized_bundle.transactions.len(), 1);
        assert_eq!(
            locked_bundle_2.sanitized_bundle.transactions[0].signature(),
            &tx2.signatures[0]
        );

        // locks shall just be for packet_bundle_2
        assert_eq!(
            bundle_locker_sanitizer.read_locks(),
            HashSet::from([system_program::id()])
        );
        assert_eq!(
            bundle_locker_sanitizer.write_locks(),
            HashSet::from([mint_keypair.pubkey(), kp2.pubkey()])
        );

        bundle_locker_sanitizer.unlock_bundle_accounts(&locked_bundle_2);
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_fails_to_pop_consensus_acc() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        // assert_eq!(bundle_locker_sanitizer.num_bundles(), 1);

        // fails to pop because bundle mentions consensus_accounts_cache
        let consensus_accounts_cache = HashSet::from([kp.pubkey()]);
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &consensus_accounts_cache)
            .is_err());

        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_duplicate_transactions_fails_to_lock() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();

        // bundle with a duplicate transaction
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone(), packet]),
            uuid: Uuid::new_v4(),
        };

        // fails to pop because bundle it locks the same transaction twice
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_bad_blockhash_fails_to_lock() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx =
            VersionedTransaction::from(transfer(&mint_keypair, &kp.pubkey(), 1, Hash::default()));
        let packet = Packet::from_data(None, &tx).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone(), packet]),
            uuid: Uuid::new_v4(),
        };

        // fails to pop because bundle has bad blockhash
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_transaction_already_processed_fails_to_lock() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone()]),
            uuid: Uuid::new_v4(),
        };

        let locked_bundle = bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .unwrap();

        let results = bank
            .process_entry_transactions(vec![
                locked_bundle.sanitized_bundle.transactions[0].to_versioned_transaction()
            ]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Ok(()));
        bundle_locker_sanitizer.unlock_bundle_accounts(&locked_bundle);

        // try to process the same one again shall fail
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_fails_to_pop_bundle_with_tip_program() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let tip_manager = TipManager::new(TipManagerConfig {
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig {
                payer: Arc::new(Keypair::new()),
                merkle_root_upload_authority: Pubkey::new_unique(),
                vote_account: Pubkey::new_unique(),
                commission_bps: 0,
            },
        });

        let mut bundle_locker_sanitizer =
            BundleSanitizer::new(&tip_manager.tip_payment_program_id());

        let kp = Keypair::new();
        let tx =
            SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
                &[Instruction::new_with_bytes(
                    tip_manager.tip_payment_program_id(),
                    &[0],
                    vec![],
                )],
                Some(&kp.pubkey()),
                &[&kp],
                genesis_config.hash(),
            ))
            .unwrap();

        let packet = Packet::from_data(None, &tx.to_versioned_transaction()).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        // fails to pop because bundle mentions tip program
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());

        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_fails_to_pop_bundle_with_txv2_program() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();
        let tx =
            SanitizedTransaction::try_from_legacy_transaction(Transaction::new_signed_with_payer(
                &[create_lookup_table(kp.pubkey(), kp.pubkey(), bank.slot()).0],
                Some(&kp.pubkey()),
                &[&kp],
                genesis_config.hash(),
            ))
            .unwrap();

        let packet = Packet::from_data(None, &tx.to_versioned_transaction()).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        // fails to pop because bundle mentions the txV2 program
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());

        assert!(bundle_locker_sanitizer.read_locks().is_empty());
        assert!(bundle_locker_sanitizer.write_locks().is_empty());
    }

    #[test]
    fn test_fails_to_pop_empty_bundle() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![]),
            uuid: Uuid::new_v4(),
        };
        // fails to pop because empty bundle
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
    }

    #[test]
    fn test_fails_to_pop_too_many_packets() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let packets = (0..MAX_PACKETS_PER_BUNDLE + 1).map(|i| {
            let tx = VersionedTransaction::from(transfer(
                &mint_keypair,
                &kp.pubkey(),
                i as u64,
                genesis_config.hash(),
            ));
            Packet::from_data(None, &tx).unwrap()
        });
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(packets.collect()),
            uuid: Uuid::new_v4(),
        };
        // fails to pop because too many packets in a bundle
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
    }

    #[test]
    fn test_fails_to_pop_packet_marked_as_discard() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let mut packet = Packet::from_data(None, &tx).unwrap();
        packet.meta.set_discard(true);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };

        // fails to pop because one of the packets is marked as discard
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
    }

    #[test]
    fn test_fails_to_pop_bad_sigverify() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let mut bundle_locker_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());
        let kp = Keypair::new();

        let mut tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));

        let _ = tx.signatures.pop();

        let bad_kp = Keypair::new();
        let serialized = tx.message.serialize();
        let bad_sig = bad_kp.sign_message(&serialized);
        tx.signatures.push(bad_sig);

        let packet = Packet::from_data(None, &tx).unwrap();

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            uuid: Uuid::new_v4(),
        };
        // assert_eq!(bundle_locker_sanitizer.num_bundles(), 1);
        // fails to pop because one of the packets is marked as discard
        assert!(bundle_locker_sanitizer
            .get_locked_bundle(packet_bundle, &bank, &HashSet::default())
            .is_err());
    }

    // TODO (LB): test txv2 bundle
}
