mod artifact_store;
pub mod config;
pub mod notification_filter;
pub mod service;
mod snapshot_worker;
mod stake_meta;

use {
    solana_clock::{BankId, Epoch, Slot},
    solana_runtime::bank::Bank,
    std::fmt,
};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct CandidateIdentity {
    pub(crate) epoch: Epoch,
    pub(crate) slot: Slot,
    pub(crate) bank_id: BankId,
}

impl CandidateIdentity {
    pub(crate) fn from_bank(bank: &Bank) -> Self {
        Self {
            epoch: bank.epoch(),
            slot: bank.slot(),
            bank_id: bank.bank_id(),
        }
    }
}

impl fmt::Display for CandidateIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "epoch={} slot={} bank_id={}",
            self.epoch, self.slot, self.bank_id
        )
    }
}
