use {
    crate::{unprocessed_packet_batches, unprocessed_packet_batches::ImmutableDeserializedPacket},
    solana_mev::bundle::Bundle,
    solana_perf::{cuda_runtime::PinnedVec, packet::Packet},
    solana_runtime::{bank::Bank, transaction_batch::TransactionBatch},
    solana_sdk::{
        feature_set,
        pubkey::Pubkey,
        transaction::{AddressLoader, SanitizedTransaction, TransactionError},
    },
    std::sync::Arc,
};

/// Checks that preparing a bundle gives an acceptable batch back
pub fn check_bundle_batch_ok(batch: &TransactionBatch) -> BundleExecutionResult<()> {
    for r in batch.lock_results() {
        match r {
            Ok(())
            | Err(TransactionError::AccountInUse)
            | Err(TransactionError::BundleNotContinuous) => {}
            Err(e) => {
                return Err(e.clone().into());
            }
        }
    }
    Ok(())
}

pub fn get_bundle_txs(
    bundle: &Bundle,
    bank: &Arc<Bank>,
    tip_program_id: Option<&Pubkey>,
) -> Vec<SanitizedTransaction> {
    let packet_indexes = generate_packet_indexes(&bundle.batch.packets);
    let deserialized_packets =
        unprocessed_packet_batches::deserialize_packets(&bundle.batch, &packet_indexes, None);

    deserialized_packets
        .filter_map(|p| {
            let immutable_packet = p.immutable_section().clone();
            transaction_from_deserialized_packet(
                &immutable_packet,
                &bank.feature_set,
                bank.vote_only_bank(),
                bank.as_ref(),
                tip_program_id,
            )
        })
        .collect()
}

fn generate_packet_indexes(pkts: &PinnedVec<Packet>) -> Vec<usize> {
    pkts.iter()
        .enumerate()
        .filter(|(_, pkt)| !pkt.meta.discard())
        .map(|(index, _)| index)
        .collect()
}

// This function deserializes packets into transactions, computes the blake3 hash of transaction
// messages, and verifies secp256k1 instructions. A list of sanitized transactions are returned
// with their packet indexes.
#[allow(clippy::needless_collect)]
fn transaction_from_deserialized_packet(
    deserialized_packet: &ImmutableDeserializedPacket,
    feature_set: &Arc<feature_set::FeatureSet>,
    votes_only: bool,
    address_loader: impl AddressLoader,
    tip_program_id: Option<&Pubkey>,
) -> Option<SanitizedTransaction> {
    if votes_only && !deserialized_packet.is_simple_vote() {
        return None;
    }

    let tx = SanitizedTransaction::try_create(
        deserialized_packet.versioned_transaction().clone(),
        *deserialized_packet.message_hash(),
        Some(deserialized_packet.is_simple_vote()),
        address_loader,
        feature_set.is_active(&feature_set::require_static_program_ids_in_transaction::ID),
    )
    .ok()?;
    tx.verify_precompiles(feature_set).ok()?;

    if let Some(tip_program_id) = tip_program_id {
        if tx
            .message()
            .account_keys()
            .iter()
            .any(|a| a == tip_program_id)
        {
            warn!("someone attempted to change the tip program!! tx: {:?}", tx);
            return None;
        }
    }

    Some(tx)
}
