//! Producer-side filter deciding which bank notifications reach the Tip Router snapshot service.
//!
//! The service is interested in a small fraction of the notifications the validator broadcasts.
//! Rejecting the rest here, on the producer thread, avoids cloning and queueing an `Arc<Bank>` that
//! the service would only drop.

use {
    crate::service::stake_meta_interval_slots,
    solana_rpc::optimistically_confirmed_bank_tracker::{BankNotification, NotificationFilter},
};

/// Accepts only the frozen banks the Tip Router snapshot service can act on.
///
/// This is a coarse, stateless classification. The service remains the owner of stateful policy and
/// still has to resolve the parent bank, reject an already-claimed epoch, and reject a candidate
/// while an artifact worker is running.
#[derive(Debug)]
pub struct TipRouterEpochBoundaryFilter {
    /// When set, the service snapshots on a fixed slot interval instead of at epoch boundaries.
    interval_slots: Option<u64>,
}

impl TipRouterEpochBoundaryFilter {
    pub fn new(interval_slots: Option<u64>) -> Self {
        Self { interval_slots }
    }

    /// Resolve the snapshot interval once, rather than re-reading the environment for every
    /// notification on a producer thread.
    pub fn from_env() -> Self {
        Self::new(stake_meta_interval_slots())
    }
}

impl NotificationFilter for TipRouterEpochBoundaryFilter {
    fn do_forward_notification(&self, notification: &BankNotification) -> bool {
        let BankNotification::Frozen(bank) = notification else {
            return false;
        };

        match self.interval_slots {
            Some(interval) => bank.slot().is_multiple_of(interval),
            // A boundary is a bank whose epoch is greater than its parent's. Deriving the parent's
            // epoch from `parent_slot` rather than `parent()` keeps this free of the lock and
            // `Arc` clone that `Bank::parent` costs, and still holds when the first slots of an
            // epoch are skipped.
            None => bank.epoch() > bank.epoch_schedule().get_epoch(bank.parent_slot()),
        }
    }
}
