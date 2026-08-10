use solana_rpc::optimistically_confirmed_bank_tracker::{BankNotification, NotificationFilter};

/// Accepts epoch-boundary frozen banks and exact rooted chains the Tip Router service can act on.
#[derive(Clone, Copy, Debug, Default)]
pub struct TipRouterEpochBoundaryFilter;

impl NotificationFilter for TipRouterEpochBoundaryFilter {
    fn do_forward_notification(&self, notification: &BankNotification) -> bool {
        match notification {
            BankNotification::Frozen(bank) => {
                // A boundary is a bank whose epoch is greater than its parent's. Deriving the
                // parent's epoch from `parent_slot` rather than `parent()` keeps this free of the
                // lock and `Arc` clone that `Bank::parent` costs, and still holds when the first
                // slots of an epoch are skipped.
                bank.epoch() > bank.epoch_schedule().get_epoch(bank.parent_slot())
            }
            BankNotification::NewRootedChain(_) => true,
            BankNotification::OptimisticallyConfirmed(_) | BankNotification::NewRootBank(_) => {
                false
            }
        }
    }
}
