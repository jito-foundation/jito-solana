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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_boundary_predicate_matches_epoch_transitions_for_all_slot_pairs() {
        for epoch_schedule in [
            EpochSchedule::custom(32, 32, false),
            EpochSchedule::custom(32, 32, true),
        ] {
            let last_test_slot = epoch_schedule.get_last_slot_in_epoch(3);

            for parent_slot in 0..last_test_slot {
                for child_slot in parent_slot + 1..=last_test_slot {
                    let child_epoch = epoch_schedule.get_epoch(child_slot);
                    assert_eq!(
                        is_epoch_boundary_child(child_epoch, parent_slot, &epoch_schedule),
                        child_epoch > epoch_schedule.get_epoch(parent_slot),
                        "parent_slot={parent_slot}, child_slot={child_slot}, warmup={}",
                        epoch_schedule.warmup,
                    );
                }
            }
        }
    }
}
