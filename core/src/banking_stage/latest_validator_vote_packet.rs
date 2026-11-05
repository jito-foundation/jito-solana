#[cfg(test)]
use {crate::banking_stage::packet_bytes, solana_perf::packet::PacketRef};
use {
    agave_transaction_view::transaction_view::SanitizedTransactionView,
    solana_bincode::limited_deserialize,
    solana_clock::{Slot, UnixTimestamp},
    solana_hash::Hash,
    solana_packet::PACKET_DATA_SIZE,
    solana_perf::packet::bytes::Bytes,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::sanitize_config::sanitize_config,
    solana_vote_program::vote_instruction::VoteInstruction,
    thiserror::Error,
};

#[derive(PartialEq, Eq, Debug, Copy, Clone)]
pub enum VoteSource {
    Gossip,
    Tpu,
}

/// Holds deserialized vote messages as well as their source, and slot
#[derive(Debug)]
pub struct LatestValidatorVote {
    vote_source: VoteSource,
    vote_pubkey: Pubkey,
    authorized_voter_pubkey: Pubkey,
    vote: Option<SanitizedTransactionView<Bytes>>,
    /// Successfully landed vote retained for a same-slot bank replacement.
    pub(super) retained_vote: Option<(Bytes, VoteSource, (Slot, Hash))>,
    /// Retained vote is a validated, one-shot fallback for the current bank.
    pub(super) restore_retained_on_failure: bool,
    slot: Slot,
    hash: Hash,
    timestamp: Option<UnixTimestamp>,
}

impl LatestValidatorVote {
    pub fn new_from_view(
        vote: SanitizedTransactionView<Bytes>,
        vote_source: VoteSource,
        deprecate_legacy_vote_ixs: bool,
    ) -> Result<Self, DeserializedPacketError> {
        let (_, instruction) = vote
            .program_instructions_iter()
            .next()
            .ok_or(DeserializedPacketError::VoteTransaction)?;

        let instruction_filter = |ix: &VoteInstruction| {
            if deprecate_legacy_vote_ixs {
                matches!(
                    ix,
                    VoteInstruction::TowerSync(_) | VoteInstruction::TowerSyncSwitch(_, _),
                )
            } else {
                ix.is_single_vote_state_update()
            }
        };

        match limited_deserialize::<VoteInstruction>(instruction.data, PACKET_DATA_SIZE as u64) {
            Ok(vote_state_update_instruction)
                if instruction_filter(&vote_state_update_instruction) =>
            {
                let ix_key = |offset| {
                    let index = instruction
                        .accounts
                        .get(offset)
                        .copied()
                        .ok_or(DeserializedPacketError::VoteTransaction)?;
                    let pubkey = vote
                        .static_account_keys()
                        .get(index as usize)
                        .copied()
                        .ok_or(DeserializedPacketError::VoteTransaction)?;
                    let signed = index < vote.num_required_signatures();

                    Ok((pubkey, signed))
                };

                let (vote_pubkey, _) = ix_key(0)?;
                let (authorized_voter_pubkey, authorized_voter_signed) = ix_key(1)?;
                if !authorized_voter_signed {
                    return Err(DeserializedPacketError::VoteTransaction);
                }

                let slot = vote_state_update_instruction.last_voted_slot().unwrap_or(0);
                let hash = vote_state_update_instruction.hash();
                let timestamp = vote_state_update_instruction.timestamp();

                Ok(Self {
                    vote: Some(vote),
                    retained_vote: None,
                    restore_retained_on_failure: false,
                    slot,
                    hash,
                    vote_pubkey,
                    authorized_voter_pubkey,
                    vote_source,
                    timestamp,
                })
            }
            _ => Err(DeserializedPacketError::VoteTransaction),
        }
    }

    #[cfg(test)]
    pub fn new(
        packet: PacketRef,
        vote_source: VoteSource,
        deprecate_legacy_vote_ixs: bool,
    ) -> Result<Self, DeserializedPacketError> {
        if !packet.meta().is_simple_vote_tx() {
            return Err(DeserializedPacketError::VoteTransaction);
        }

        let packet_data = packet.data(..).unwrap();
        let vote = SanitizedTransactionView::try_new_sanitized(
            packet_bytes(packet, packet_data),
            &solana_runtime_transaction::sanitize_config::sanitize_config(),
        )
        .unwrap();

        Self::new_from_view(vote, vote_source, deprecate_legacy_vote_ixs)
    }

    pub fn vote_pubkey(&self) -> Pubkey {
        self.vote_pubkey
    }

    pub fn authorized_voter_pubkey(&self) -> Pubkey {
        self.authorized_voter_pubkey
    }

    pub fn slot(&self) -> Slot {
        self.slot
    }

    pub fn source(&self) -> VoteSource {
        self.vote_source
    }

    pub(crate) fn hash(&self) -> Hash {
        self.hash
    }

    pub fn timestamp(&self) -> Option<UnixTimestamp> {
        self.timestamp
    }

    pub fn is_vote_taken(&self) -> bool {
        self.vote.is_none()
    }

    pub fn take_vote(&mut self) -> Option<SanitizedTransactionView<Bytes>> {
        self.vote.take()
    }

    pub fn restore_for_bank(
        &mut self,
        is_valid_for_fork: impl Fn((Slot, Hash)) -> bool,
        deprecate_legacy_vote_ixs: bool,
    ) -> Option<usize> {
        let current_vote_is_valid =
            self.vote.is_some() && is_valid_for_fork((self.slot, self.hash));
        let retained_vote_is_valid = self
            .retained_vote
            .as_ref()
            .is_some_and(|(_, _, slot_hash)| is_valid_for_fork(*slot_hash));
        self.restore_retained_on_failure = current_vote_is_valid && retained_vote_is_valid;

        if current_vote_is_valid || !retained_vote_is_valid {
            return None;
        }

        let newly_unprocessed = usize::from(self.is_vote_taken());
        self.restore_retained_vote(deprecate_legacy_vote_ixs)?;
        Some(newly_unprocessed)
    }

    pub(super) fn take_deferred_retained_vote(
        &mut self,
        deprecate_legacy_vote_ixs: bool,
    ) -> Option<SanitizedTransactionView<Bytes>> {
        if !std::mem::take(&mut self.restore_retained_on_failure) {
            return None;
        }

        self.restore_retained_vote(deprecate_legacy_vote_ixs)?;
        self.take_vote()
    }

    fn restore_retained_vote(&mut self, deprecate_legacy_vote_ixs: bool) -> Option<()> {
        let (bytes, source, _) = self.retained_vote.as_ref()?;
        let vote =
            SanitizedTransactionView::try_new_sanitized(bytes.clone(), &sanitize_config()).ok()?;
        let mut restored = Self::new_from_view(vote, *source, deprecate_legacy_vote_ixs).ok()?;
        restored.retained_vote = self.retained_vote.take();
        *self = restored;
        Some(())
    }
}

#[derive(Debug, Error)]
pub enum DeserializedPacketError {
    #[error("vote transaction failure")]
    VoteTransaction,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        itertools::Itertools,
        solana_packet::PacketFlags,
        solana_perf::packet::{BytesPacket, PacketBatch},
        solana_runtime::genesis_utils::ValidatorVoteKeypairs,
        solana_signer::Signer,
        solana_system_transaction::transfer,
        solana_vote::vote_transaction::new_tower_sync_transaction,
        solana_vote_program::vote_state::TowerSync,
    };

    fn deserialize_packets(
        packet_batch: &PacketBatch,
        vote_source: VoteSource,
    ) -> impl Iterator<Item = LatestValidatorVote> + '_ {
        packet_batch
            .iter()
            .filter_map(move |packet| LatestValidatorVote::new(packet, vote_source, true).ok())
    }

    #[test]
    fn test_deserialize_vote_packets() {
        let keypairs = ValidatorVoteKeypairs::new_rand();
        let blockhash = Hash::new_unique();
        let switch_proof = Hash::new_unique();
        let mut tower_sync = BytesPacket::from_data(new_tower_sync_transaction(
            TowerSync::from(vec![(0, 3), (1, 2), (2, 1)]),
            blockhash,
            &keypairs.node_keypair,
            &keypairs.vote_keypair,
            &keypairs.vote_keypair,
            None,
        ))
        .unwrap();
        tower_sync
            .meta_mut()
            .flags
            .set(PacketFlags::SIMPLE_VOTE_TX, true);
        let mut tower_sync_switch = BytesPacket::from_data(new_tower_sync_transaction(
            TowerSync::from(vec![(0, 3), (1, 2), (3, 1)]),
            blockhash,
            &keypairs.node_keypair,
            &keypairs.vote_keypair,
            &keypairs.vote_keypair,
            Some(switch_proof),
        ))
        .unwrap();
        tower_sync_switch
            .meta_mut()
            .flags
            .set(PacketFlags::SIMPLE_VOTE_TX, true);
        let random_transaction = BytesPacket::from_data(transfer(
            &keypairs.node_keypair,
            &Pubkey::new_unique(),
            1000,
            blockhash,
        ))
        .unwrap();
        let packet_batch =
            PacketBatch::from(vec![tower_sync, tower_sync_switch, random_transaction]);

        let deserialized_packets =
            deserialize_packets(&packet_batch, VoteSource::Gossip).collect_vec();

        assert_eq!(2, deserialized_packets.len());
        assert_eq!(VoteSource::Gossip, deserialized_packets[0].vote_source);
        assert_eq!(VoteSource::Gossip, deserialized_packets[1].vote_source);

        assert_eq!(
            keypairs.vote_keypair.pubkey(),
            deserialized_packets[0].vote_pubkey
        );
        assert_eq!(
            keypairs.vote_keypair.pubkey(),
            deserialized_packets[1].vote_pubkey
        );

        assert!(deserialized_packets[0].vote.is_some());
        assert!(deserialized_packets[1].vote.is_some());
    }
}
