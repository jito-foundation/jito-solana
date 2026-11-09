use {
    solana_clock::{Epoch, Slot},
    solana_epoch_schedule::EpochSchedule,
    solana_rpc::optimistically_confirmed_bank_tracker::{BankNotification, BankNotificationFilter},
};

fn is_epoch_boundary_child(
    child_epoch: Epoch,
    parent_slot: Slot,
    epoch_schedule: &EpochSchedule,
) -> bool {
    child_epoch > epoch_schedule.get_epoch(parent_slot)
}

/// Accepts epoch-boundary frozen banks and exact rooted chains the Tip Router service can act on.
#[derive(Clone, Copy, Debug, Default)]
pub struct TipRouterEpochBoundaryFilter;

impl BankNotificationFilter for TipRouterEpochBoundaryFilter {
    fn should_forward(&self, notification: &BankNotification) -> bool {
        match notification {
            BankNotification::Frozen(bank) => {
                // A boundary is a bank whose epoch is greater than its parent's. Deriving the
                // parent's epoch from `parent_slot` rather than `parent()` keeps this free of the
                // lock and `Arc` clone that `Bank::parent` costs, and still holds when the first
                // slots of an epoch are skipped.
                is_epoch_boundary_child(bank.epoch(), bank.parent_slot(), bank.epoch_schedule())
            }
            BankNotification::NewRootedChain(..) => true,
            BankNotification::OptimisticallyConfirmed(..) | BankNotification::NewRootBank(_) => {
                false
            }
        }
    }
}
