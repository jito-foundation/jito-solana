use crate::unprocessed_packet_batches::ImmutableDeserializedPacket;
///! Turns packets into SanitizedTransactions and ensure they pass sanity checks
use {
    crate::packet_bundle::PacketBundle,
    crate::unprocessed_packet_batches::deserialize_packets,
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
};

pub const MAX_PACKETS_PER_BUNDLE: usize = 5;

#[derive(Error, Debug, PartialEq, Eq, Clone)]
pub enum BundleSanitizerError {
    #[error("Bank is in vote-only mode")]
    VoteOnlyMode,
    #[error("Bundle packet batch failed pre-check")]
    FailedPacketBatchPreCheck,
    #[error("Bundle mentions blacklisted account")]
    BlacklistedAccount,
    #[error("Bundle contains a transaction that failed to serialize")]
    FailedToSerializeTransaction,
    #[error("Bundle contains a duplicate transaction")]
    DuplicateTransaction,
    #[error("Bundle failed check_transactions")]
    FailedCheckTransactions,
}

pub type BundleSanitizationResult<T> = Result<T, BundleSanitizerError>;

/// An invalid bundle contains one of the following:
///  No packets.
///  Too many packets.
///  Packets marked for discard (not sure why someone would do this)
///  One of the packets fails signature verification.
///  Mentions an account in consensus or blacklisted accounts.
///  Contains a packet that failed to serialize to a transaction.
///  Contains duplicate transactions within the same bundle.
///  Contains a transaction that was already processed or one with an invalid blockhash.
/// NOTE: bundles need to be sanitized for a given bank. For instance, a bundle sanitized
/// on bank n-1 will be valid for all of bank n-1, and may or may not be valid for bank n
pub fn get_sanitized_bundle(
    packet_bundle: &PacketBundle,
    bank: &Arc<Bank>,
    consensus_accounts_cache: &HashSet<Pubkey>,
    blacklisted_accounts: &HashSet<Pubkey>,
    transaction_error_metrics: &mut TransactionErrorMetrics,
) -> BundleSanitizationResult<SanitizedBundle> {
    if bank.vote_only_bank() {
        return Err(BundleSanitizerError::VoteOnlyMode);
    }

    if packet_bundle.batch.is_empty()
        || packet_bundle.batch.len() > MAX_PACKETS_PER_BUNDLE
        || packet_bundle.batch.iter().any(|p| p.meta.discard())
        || packet_bundle
            .batch
            .iter()
            .any(|p| !verify_packet(&mut p.clone(), false))
    {
        return Err(BundleSanitizerError::FailedPacketBatchPreCheck);
    }

    let packet_indexes = (0..packet_bundle.batch.len()).collect::<Vec<usize>>();
    let deserialized_packets = deserialize_packets(&packet_bundle.batch, &packet_indexes);
    let transactions: Vec<SanitizedTransaction> = deserialized_packets
        .filter_map(|p| {
            let immutable_packet = p.immutable_section().clone();
            transaction_from_deserialized_packet(
                &immutable_packet,
                &bank.feature_set,
                bank.as_ref(),
            )
        })
        .collect();

    let unique_signatures: HashSet<&Signature, RandomState> =
        HashSet::from_iter(transactions.iter().map(|tx| tx.signature()));
    let contains_blacklisted_account = transactions.iter().any(|tx| {
        let accounts = tx.message().account_keys();
        accounts
            .iter()
            .any(|acc| blacklisted_accounts.contains(acc) || consensus_accounts_cache.contains(acc))
    });

    if contains_blacklisted_account {
        return Err(BundleSanitizerError::BlacklistedAccount);
    }

    if transactions.is_empty() || packet_bundle.batch.len() != transactions.len() {
        return Err(BundleSanitizerError::FailedToSerializeTransaction);
    }

    if unique_signatures.len() != transactions.len() {
        return Err(BundleSanitizerError::DuplicateTransaction);
    }

    // assume everything locks okay to check for already-processed transaction or expired/invalid blockhash
    let lock_results: Vec<_> = repeat(Ok(())).take(transactions.len()).collect();
    let check_results = bank.check_transactions(
        &transactions,
        &lock_results,
        MAX_PROCESSING_AGE,
        transaction_error_metrics,
    );
    if check_results.iter().any(|r| r.0.is_err()) {
        return Err(BundleSanitizerError::FailedCheckTransactions);
    }

    Ok(SanitizedBundle {
        transactions,
        bundle_id: packet_bundle.bundle_id.clone(),
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

#[cfg(test)]
mod tests {
    use {
        crate::{
            bundle_sanitizer::{get_sanitized_bundle, MAX_PACKETS_PER_BUNDLE},
            packet_bundle::PacketBundle,
            tip_manager::{TipDistributionAccountConfig, TipManager, TipManagerConfig},
        },
        solana_address_lookup_table_program::instruction::create_lookup_table,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{
            bank::Bank, genesis_utils::GenesisConfigInfo,
            transaction_error_metrics::TransactionErrorMetrics,
        },
        solana_sdk::{
            bundle::sanitized::derive_bundle_id,
            hash::Hash,
            instruction::Instruction,
            packet::Packet,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::{Transaction, VersionedTransaction},
        },
        std::{collections::HashSet, sync::Arc},
    };

    #[test]
    fn test_simple_get_sanitized_bundle() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let tx_signature = tx.signatures[0];
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id,
        };

        let mut transaction_errors = TransactionErrorMetrics::default();
        let sanitized_bundle = get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors,
        )
        .unwrap();
        assert_eq!(sanitized_bundle.transactions.len(), 1);
        assert_eq!(sanitized_bundle.transactions[0].signature(), &tx_signature);
    }

    #[test]
    fn test_fail_to_sanitize_consensus_account() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id,
        };

        let consensus_accounts_cache = HashSet::from([kp.pubkey()]);
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &consensus_accounts_cache,
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fail_to_sanitize_duplicate_transaction() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        // bundle with a duplicate transaction
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone(), packet]),
            bundle_id,
        };

        // fails to pop because bundle it locks the same transaction twice
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_bad_blockhash() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let tx =
            VersionedTransaction::from(transfer(&mint_keypair, &kp.pubkey(), 1, Hash::default()));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone(), packet]),
            bundle_id,
        };

        // fails to pop because bundle has bad blockhash
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_already_processed() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let tx = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet.clone()]),
            bundle_id: bundle_id.clone(),
        };

        let mut transaction_errors = TransactionErrorMetrics::default();
        let sanitized_bundle = get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors,
        )
        .unwrap();

        let results = bank.process_entry_transactions(
            sanitized_bundle
                .transactions
                .into_iter()
                .map(|tx| tx.to_versioned_transaction())
                .collect(),
        );
        assert_eq!(results.len(), 1);
        assert_eq!(results[0], Ok(()));

        // try to process the same one again shall fail
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id,
        };

        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_bundle_tip_program() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let tip_manager = TipManager::new(TipManagerConfig {
            tip_payment_program_id: Pubkey::new_unique(),
            tip_distribution_program_id: Pubkey::new_unique(),
            tip_distribution_account_config: TipDistributionAccountConfig {
                merkle_root_upload_authority: Pubkey::new_unique(),
                vote_account: Pubkey::new_unique(),
                commission_bps: 0,
            },
        });

        let kp = Keypair::new();
        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[Instruction::new_with_bytes(
                tip_manager.tip_payment_program_id(),
                &[0],
                vec![],
            )],
            Some(&kp.pubkey()),
            &[&kp],
            genesis_config.hash(),
        ));
        tx.sanitize(false).unwrap();
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id,
        };

        // fails to pop because bundle mentions tip program
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::from_iter([tip_manager.tip_payment_program_id()]),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_txv2_sanitized_bundle_ok() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();
        let tx = VersionedTransaction::from(Transaction::new_signed_with_payer(
            &[create_lookup_table(kp.pubkey(), kp.pubkey(), bank.slot()).0],
            Some(&kp.pubkey()),
            &[&kp],
            genesis_config.hash(),
        ));
        tx.sanitize(false).unwrap();
        let packet = Packet::from_data(None, &tx).unwrap();
        let bundle_id = derive_bundle_id(&[tx]);

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![packet]),
            bundle_id,
        };

        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_ok());
    }

    #[test]
    fn test_fails_to_sanitize_empty_bundle() {
        solana_logger::setup();
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(vec![]),
            bundle_id: String::default(),
        };
        // fails to pop because empty bundle
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_too_many_packets() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let kp = Keypair::new();

        let txs = (0..MAX_PACKETS_PER_BUNDLE + 1)
            .map(|i| {
                VersionedTransaction::from(transfer(
                    &mint_keypair,
                    &kp.pubkey(),
                    i as u64,
                    genesis_config.hash(),
                ))
            })
            .collect::<Vec<_>>();
        let packets = txs.iter().map(|tx| Packet::from_data(None, tx).unwrap());
        let packet_bundle = PacketBundle {
            batch: PacketBatch::new(packets.collect()),
            bundle_id: derive_bundle_id(&txs),
        };
        // fails to pop because too many packets in a bundle
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_discarded() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

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
            bundle_id: derive_bundle_id(&[tx]),
        };

        // fails to pop because one of the packets is marked as discard
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }

    #[test]
    fn test_fails_to_sanitize_bad_sigverify() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

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
            bundle_id: derive_bundle_id(&[tx]),
        };
        let mut transaction_errors = TransactionErrorMetrics::default();
        assert!(get_sanitized_bundle(
            &packet_bundle,
            &bank,
            &HashSet::default(),
            &HashSet::default(),
            &mut transaction_errors
        )
        .is_err());
    }
}
