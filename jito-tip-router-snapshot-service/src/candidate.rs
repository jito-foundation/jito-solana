use {
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CandidateIdentity {
    pub(crate) epoch: Epoch,
    pub(crate) slot: Slot,
    pub(crate) bank_hash: Hash,
}

impl CandidateIdentity {
    pub(crate) fn from_bank(bank: &Bank) -> Self {
        Self {
            epoch: bank.epoch(),
            slot: bank.slot(),
            bank_hash: bank.hash(),
        }
    }
}
