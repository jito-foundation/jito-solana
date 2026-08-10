use {crate::candidate::CandidateIdentity, solana_clock::Slot, solana_hash::Hash};

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ConflictingRootedBankHashes {
    pub(super) slot: Slot,
    pub(super) existing_hash: Hash,
    pub(super) new_hash: Hash,
}

#[derive(Debug, Default)]
pub(super) struct RootedChain {
    banks: Vec<(Slot, Hash)>,
}

impl RootedChain {
    pub(super) fn record(
        &mut self,
        mut rooted_chain: Vec<(Slot, Hash)>,
    ) -> Result<(), ConflictingRootedBankHashes> {
        rooted_chain.sort_unstable_by_key(|(slot, _hash)| *slot);
        for (slot, new_hash) in rooted_chain {
            match self
                .banks
                .binary_search_by_key(&slot, |(rooted_slot, _hash)| *rooted_slot)
            {
                Ok(index) => {
                    let existing_hash = self.banks[index].1;
                    if existing_hash != new_hash {
                        return Err(ConflictingRootedBankHashes {
                            slot,
                            existing_hash,
                            new_hash,
                        });
                    }
                }
                Err(index) => self.banks.insert(index, (slot, new_hash)),
            }
        }
        Ok(())
    }

    pub(super) fn contains(&self, candidate: CandidateIdentity) -> bool {
        self.hash_at(candidate.slot) == Some(candidate.bank_hash)
    }

    pub(super) fn hash_at(&self, slot: Slot) -> Option<Hash> {
        self.banks
            .binary_search_by_key(&slot, |(rooted_slot, _hash)| *rooted_slot)
            .ok()
            .map(|index| self.banks[index].1)
    }

    pub(super) fn prune_before(&mut self, first_retained_slot: Slot) {
        let first_retained_index = self
            .banks
            .partition_point(|(slot, _hash)| *slot < first_retained_slot);
        self.banks.drain(..first_retained_index);
    }

    pub(super) fn clear(&mut self) {
        self.banks.clear();
    }
}
