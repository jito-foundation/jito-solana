//! Producer-side filter deciding which bank notifications reach the Tip Router snapshot service.
//!
//! The service is interested in a small fraction of the notifications the validator broadcasts.
//! Rejecting the rest here, on the producer thread, avoids cloning and queueing an `Arc<Bank>` that
//! the service would only drop.

use solana_rpc::optimistically_confirmed_bank_tracker::{BankNotification, NotificationFilter};

/// Accepts only the frozen banks the Tip Router snapshot service can act on.
///
/// This is a coarse, stateless classification. The service remains the owner of stateful policy and
/// still has to resolve the parent bank, reject an already-claimed epoch, and reject a candidate
/// while an artifact worker is running.
#[derive(Clone, Copy, Debug, Default)]
pub struct TipRouterEpochBoundaryFilter;

impl NotificationFilter for TipRouterEpochBoundaryFilter {
    fn do_forward_notification(&self, notification: &BankNotification) -> bool {
        let BankNotification::Frozen(bank) = notification else {
            return false;
        };

        // A boundary is a bank whose epoch is greater than its parent's. Deriving the parent's
        // epoch from `parent_slot` rather than `parent()` keeps this free of the lock and `Arc`
        // clone that `Bank::parent` costs, and still holds when the first slots of an epoch are
        // skipped.
        bank.epoch() > bank.epoch_schedule().get_epoch(bank.parent_slot())
    }
}
