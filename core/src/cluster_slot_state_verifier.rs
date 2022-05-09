use {
    crate::{
        ancestor_hashes_service::{AncestorHashesReplayUpdate, AncestorHashesReplayUpdateSender},
        fork_choice::ForkChoice,
        heaviest_subtree_fork_choice::HeaviestSubtreeForkChoice,
    },
    solana_ledger::blockstore::Blockstore,
    solana_sdk::{clock::Slot, hash::Hash},
    std::collections::{BTreeMap, BTreeSet, HashMap},
};

pub(crate) type DuplicateSlotsTracker = BTreeSet<Slot>;
pub(crate) type DuplicateSlotsToRepair = HashMap<Slot, Hash>;
pub(crate) type EpochSlotsFrozenSlots = BTreeMap<Slot, Hash>;
pub(crate) type GossipDuplicateConfirmedSlots = BTreeMap<Slot, Hash>;

#[derive(PartialEq, Clone, Debug)]
pub enum ClusterConfirmedHash {
    // Ordered from strongest confirmation to weakest. Stronger
    // confirmations take precedence over weaker ones.
    DuplicateConfirmed(Hash),
    EpochSlotsFrozen(Hash),
}

#[derive(PartialEq, Clone, Debug)]
pub enum BankStatus {
    Frozen(Hash),
    Dead,
    Unprocessed,
}

impl BankStatus {
    pub fn new(is_dead: impl Fn() -> bool, get_hash: impl Fn() -> Option<Hash>) -> Self {
        if is_dead() {
            Self::new_dead()
        } else {
            Self::new_from_hash(get_hash())
        }
    }

    fn new_dead() -> Self {
        BankStatus::Dead
    }

    fn new_from_hash(hash: Option<Hash>) -> Self {
        if let Some(hash) = hash {
            if hash == Hash::default() {
                BankStatus::Unprocessed
            } else {
                BankStatus::Frozen(hash)
            }
        } else {
            BankStatus::Unprocessed
        }
    }

    fn bank_hash(&self) -> Option<Hash> {
        match self {
            BankStatus::Frozen(hash) => Some(*hash),
            BankStatus::Dead => Some(Hash::default()),
            BankStatus::Unprocessed => None,
        }
    }

    fn is_dead(&self) -> bool {
        match self {
            BankStatus::Frozen(_) => false,
            BankStatus::Dead => true,
            BankStatus::Unprocessed => false,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct DeadState {
    // Keep fields private, forces construction
    // via constructor
    cluster_confirmed_hash: Option<ClusterConfirmedHash>,
    is_slot_duplicate: bool,
}

impl DeadState {
    pub fn new_from_state(
        slot: Slot,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
    ) -> Self {
        let cluster_confirmed_hash = get_cluster_confirmed_hash_from_state(
            slot,
            gossip_duplicate_confirmed_slots,
            epoch_slots_frozen_slots,
            fork_choice,
            Some(Hash::default()),
        );
        let is_slot_duplicate = duplicate_slots_tracker.contains(&slot);
        Self::new(cluster_confirmed_hash, is_slot_duplicate)
    }

    fn new(cluster_confirmed_hash: Option<ClusterConfirmedHash>, is_slot_duplicate: bool) -> Self {
        Self {
            cluster_confirmed_hash,
            is_slot_duplicate,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct BankFrozenState {
    // Keep fields private, forces construction
    // via constructor
    frozen_hash: Hash,
    cluster_confirmed_hash: Option<ClusterConfirmedHash>,
    is_slot_duplicate: bool,
}

impl BankFrozenState {
    pub fn new_from_state(
        slot: Slot,
        frozen_hash: Hash,
        duplicate_slots_tracker: &mut DuplicateSlotsTracker,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        epoch_slots_frozen_slots: &EpochSlotsFrozenSlots,
    ) -> Self {
        let cluster_confirmed_hash = get_cluster_confirmed_hash_from_state(
            slot,
            gossip_duplicate_confirmed_slots,
            epoch_slots_frozen_slots,
            fork_choice,
            Some(frozen_hash),
        );
        let is_slot_duplicate = duplicate_slots_tracker.contains(&slot);
        Self::new(frozen_hash, cluster_confirmed_hash, is_slot_duplicate)
    }

    fn new(
        frozen_hash: Hash,
        cluster_confirmed_hash: Option<ClusterConfirmedHash>,
        is_slot_duplicate: bool,
    ) -> Self {
        assert!(frozen_hash != Hash::default());
        Self {
            frozen_hash,
            cluster_confirmed_hash,
            is_slot_duplicate,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct DuplicateConfirmedState {
    // Keep fields private, forces construction
    // via constructor
    duplicate_confirmed_hash: Hash,
    bank_status: BankStatus,
}
impl DuplicateConfirmedState {
    pub fn new_from_state(
        duplicate_confirmed_hash: Hash,
        is_dead: impl Fn() -> bool,
        get_hash: impl Fn() -> Option<Hash>,
    ) -> Self {
        let bank_status = BankStatus::new(is_dead, get_hash);
        Self::new(duplicate_confirmed_hash, bank_status)
    }

    fn new(duplicate_confirmed_hash: Hash, bank_status: BankStatus) -> Self {
        Self {
            duplicate_confirmed_hash,
            bank_status,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct DuplicateState {
    // Keep fields private, forces construction
    // via constructor
    duplicate_confirmed_hash: Option<Hash>,
    bank_status: BankStatus,
}
impl DuplicateState {
    pub fn new_from_state(
        slot: Slot,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        is_dead: impl Fn() -> bool,
        get_hash: impl Fn() -> Option<Hash>,
    ) -> Self {
        let bank_status = BankStatus::new(is_dead, get_hash);

        // We can only skip marking duplicate if this slot has already been
        // duplicate confirmed, any weaker confirmation levels are not sufficient
        // to skip marking the slot as duplicate.
        let duplicate_confirmed_hash = get_duplicate_confirmed_hash_from_state(
            slot,
            gossip_duplicate_confirmed_slots,
            fork_choice,
            bank_status.bank_hash(),
        );
        Self::new(duplicate_confirmed_hash, bank_status)
    }

    fn new(duplicate_confirmed_hash: Option<Hash>, bank_status: BankStatus) -> Self {
        Self {
            duplicate_confirmed_hash,
            bank_status,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct EpochSlotsFrozenState {
    // Keep fields private, forces construction
    // via constructor
    epoch_slots_frozen_hash: Hash,
    duplicate_confirmed_hash: Option<Hash>,
    bank_status: BankStatus,
}
impl EpochSlotsFrozenState {
    pub fn new_from_state(
        slot: Slot,
        epoch_slots_frozen_hash: Hash,
        gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
        fork_choice: &mut HeaviestSubtreeForkChoice,
        is_dead: impl Fn() -> bool,
        get_hash: impl Fn() -> Option<Hash>,
    ) -> Self {
        let bank_status = BankStatus::new(is_dead, get_hash);
        let duplicate_confirmed_hash = get_duplicate_confirmed_hash_from_state(
            slot,
            gossip_duplicate_confirmed_slots,
            fork_choice,
            bank_status.bank_hash(),
        );
        Self::new(
            epoch_slots_frozen_hash,
            duplicate_confirmed_hash,
            bank_status,
        )
    }

    fn new(
        epoch_slots_frozen_hash: Hash,
        duplicate_confirmed_hash: Option<Hash>,
        bank_status: BankStatus,
    ) -> Self {
        Self {
            epoch_slots_frozen_hash,
            duplicate_confirmed_hash,
            bank_status,
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum SlotStateUpdate {
    BankFrozen(BankFrozenState),
    DuplicateConfirmed(DuplicateConfirmedState),
    Dead(DeadState),
    Duplicate(DuplicateState),
    EpochSlotsFrozen(EpochSlotsFrozenState),
}

impl SlotStateUpdate {
    fn bank_hash(&self) -> Option<Hash> {
        match self {
            SlotStateUpdate::BankFrozen(bank_frozen_state) => Some(bank_frozen_state.frozen_hash),
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state) => {
                duplicate_confirmed_state.bank_status.bank_hash()
            }
            SlotStateUpdate::Dead(_) => Some(Hash::default()),
            SlotStateUpdate::Duplicate(duplicate_state) => duplicate_state.bank_status.bank_hash(),
            SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state) => {
                epoch_slots_frozen_state.bank_status.bank_hash()
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub enum ResultingStateChange {
    // Bank was frozen
    BankFrozen(Hash),
    // Hash of our current frozen version of the slot
    MarkSlotDuplicate(Hash),
    // Hash of the either:
    // 1) Cluster duplicate confirmed slot
    // 2) Epoch Slots frozen sampled slot
    // that is not equivalent to our frozen version of the slot
    RepairDuplicateConfirmedVersion(Hash),
    // Hash of our current frozen version of the slot
    DuplicateConfirmedSlotMatchesCluster(Hash),
    SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate),
}

impl SlotStateUpdate {
    fn into_state_changes(self, slot: Slot) -> Vec<ResultingStateChange> {
        let bank_frozen_hash = self.bank_hash();
        if bank_frozen_hash == None {
            // If the bank hasn't been frozen yet, then there's nothing to do
            // since replay of the slot hasn't finished yet.
            return vec![];
        }

        match self {
            SlotStateUpdate::Dead(dead_state) => on_dead_slot(slot, dead_state),
            SlotStateUpdate::BankFrozen(bank_frozen_state) => {
                on_frozen_slot(slot, bank_frozen_state)
            }
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state) => {
                on_duplicate_confirmed(slot, duplicate_confirmed_state)
            }
            SlotStateUpdate::Duplicate(duplicate_state) => on_duplicate(duplicate_state),
            SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state) => {
                on_epoch_slots_frozen(slot, epoch_slots_frozen_state)
            }
        }
    }
}

fn check_duplicate_confirmed_hash_against_frozen_hash(
    state_changes: &mut Vec<ResultingStateChange>,
    slot: Slot,
    duplicate_confirmed_hash: Hash,
    bank_frozen_hash: Hash,
    is_dead: bool,
) {
    if duplicate_confirmed_hash != bank_frozen_hash {
        if is_dead {
            // If the cluster duplicate confirmed some version of this slot, then
            // there's another version of our dead slot
            warn!(
                "Cluster duplicate confirmed slot {} with hash {}, but we marked slot dead",
                slot, duplicate_confirmed_hash
            );
        } else {
            // The duplicate confirmed slot hash does not match our frozen hash.
            // Modify fork choice rule to exclude our version from being voted
            // on and also repair the correct version
            warn!(
                "Cluster duplicate confirmed slot {} with hash {}, but our version has hash {}",
                slot, duplicate_confirmed_hash, bank_frozen_hash
            );
        }
        state_changes.push(ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash));
        state_changes.push(ResultingStateChange::RepairDuplicateConfirmedVersion(
            duplicate_confirmed_hash,
        ));
    } else {
        // If the versions match, then add the slot to the candidate
        // set to account for the case where it was removed earlier
        // by the `on_duplicate_slot()` handler
        state_changes.push(ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
            bank_frozen_hash,
        ));
    }
}

fn check_epoch_slots_hash_against_frozen_hash(
    state_changes: &mut Vec<ResultingStateChange>,
    slot: Slot,
    epoch_slots_frozen_hash: Hash,
    bank_frozen_hash: Hash,
    is_dead: bool,
) {
    if epoch_slots_frozen_hash != bank_frozen_hash {
        if is_dead {
            // If the cluster duplicate confirmed some version of this slot, then
            // there's another version of our dead slot
            warn!(
                "EpochSlots sample returned slot {} with hash {}, but we marked slot dead",
                slot, epoch_slots_frozen_hash
            );
        } else {
            // The duplicate confirmed slot hash does not match our frozen hash.
            // Modify fork choice rule to exclude our version from being voted
            // on and also repair the correct version
            warn!(
                "EpochSlots sample returned slot {} with hash {}, but our version
                has hash {}",
                slot, epoch_slots_frozen_hash, bank_frozen_hash
            );
        }
        state_changes.push(ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash));
        state_changes.push(ResultingStateChange::RepairDuplicateConfirmedVersion(
            epoch_slots_frozen_hash,
        ));
    }
}

fn on_dead_slot(slot: Slot, dead_state: DeadState) -> Vec<ResultingStateChange> {
    let DeadState {
        cluster_confirmed_hash,
        is_slot_duplicate,
    } = dead_state;

    let mut state_changes = vec![];
    if let Some(cluster_confirmed_hash) = cluster_confirmed_hash {
        match cluster_confirmed_hash {
            ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash) => {
                // If the cluster duplicate_confirmed some version of this slot, then
                // check if our version agrees with the cluster,
                let bank_hash = Hash::default();
                let is_dead = true;
                state_changes.push(ResultingStateChange::SendAncestorHashesReplayUpdate(
                    AncestorHashesReplayUpdate::DeadDuplicateConfirmed(slot),
                ));
                check_duplicate_confirmed_hash_against_frozen_hash(
                    &mut state_changes,
                    slot,
                    duplicate_confirmed_hash,
                    bank_hash,
                    is_dead,
                );
            }
            ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash) => {
                // Lower priority than having seen an actual duplicate confirmed hash in the
                // match arm above.
                let bank_hash = Hash::default();
                let is_dead = true;
                check_epoch_slots_hash_against_frozen_hash(
                    &mut state_changes,
                    slot,
                    epoch_slots_frozen_hash,
                    bank_hash,
                    is_dead,
                );
            }
        }
    } else {
        state_changes.push(ResultingStateChange::SendAncestorHashesReplayUpdate(
            AncestorHashesReplayUpdate::Dead(slot),
        ));
        if is_slot_duplicate {
            state_changes.push(ResultingStateChange::MarkSlotDuplicate(Hash::default()));
        }
    }

    state_changes
}

fn on_frozen_slot(slot: Slot, bank_frozen_state: BankFrozenState) -> Vec<ResultingStateChange> {
    let BankFrozenState {
        frozen_hash,
        cluster_confirmed_hash,
        is_slot_duplicate,
    } = bank_frozen_state;
    let mut state_changes = vec![ResultingStateChange::BankFrozen(frozen_hash)];
    if let Some(cluster_confirmed_hash) = cluster_confirmed_hash {
        match cluster_confirmed_hash {
            ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash) => {
                // If the cluster duplicate_confirmed some version of this slot, then
                // check if our version agrees with the cluster,
                let is_dead = false;
                check_duplicate_confirmed_hash_against_frozen_hash(
                    &mut state_changes,
                    slot,
                    duplicate_confirmed_hash,
                    frozen_hash,
                    is_dead,
                );
            }
            ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash) => {
                // Lower priority than having seen an actual duplicate confirmed hash in the
                // match arm above.
                let is_dead = false;
                check_epoch_slots_hash_against_frozen_hash(
                    &mut state_changes,
                    slot,
                    epoch_slots_frozen_hash,
                    frozen_hash,
                    is_dead,
                );
            }
        }
    }
    // If `cluster_confirmed_hash` is Some above we should have already pushed a
    // `MarkSlotDuplicate` state change
    else if is_slot_duplicate {
        state_changes.push(ResultingStateChange::MarkSlotDuplicate(frozen_hash));
    }

    state_changes
}

fn on_duplicate_confirmed(
    slot: Slot,
    duplicate_confirmed_state: DuplicateConfirmedState,
) -> Vec<ResultingStateChange> {
    let DuplicateConfirmedState {
        bank_status,
        duplicate_confirmed_hash,
    } = duplicate_confirmed_state;

    match bank_status {
        BankStatus::Dead | BankStatus::Frozen(_) => (),
        // No action to be taken yet
        BankStatus::Unprocessed => {
            return vec![];
        }
    }

    let bank_hash = bank_status.bank_hash().expect("bank hash must exist");

    let mut state_changes = vec![];
    let is_dead = bank_status.is_dead();
    if is_dead {
        state_changes.push(ResultingStateChange::SendAncestorHashesReplayUpdate(
            AncestorHashesReplayUpdate::DeadDuplicateConfirmed(slot),
        ));
    }
    check_duplicate_confirmed_hash_against_frozen_hash(
        &mut state_changes,
        slot,
        duplicate_confirmed_hash,
        bank_hash,
        is_dead,
    );

    state_changes
}

fn on_duplicate(duplicate_state: DuplicateState) -> Vec<ResultingStateChange> {
    let DuplicateState {
        bank_status,
        duplicate_confirmed_hash,
    } = duplicate_state;

    match bank_status {
        BankStatus::Dead | BankStatus::Frozen(_) => (),
        // No action to be taken yet
        BankStatus::Unprocessed => {
            return vec![];
        }
    }

    let bank_hash = bank_status.bank_hash().expect("bank hash must exist");

    // If the cluster duplicate_confirmed some version of this slot
    // then either the `SlotStateUpdate::DuplicateConfirmed`, `SlotStateUpdate::BankFrozen`,
    // or `SlotStateUpdate::Dead` state transitions will take care of marking the fork as
    // duplicate if there's a mismatch with our local version.
    if duplicate_confirmed_hash.is_none() {
        // If we have not yet seen any version of the slot duplicate confirmed, then mark
        // the slot as duplicate
        return vec![ResultingStateChange::MarkSlotDuplicate(bank_hash)];
    }

    vec![]
}

fn on_epoch_slots_frozen(
    slot: Slot,
    epoch_slots_frozen_state: EpochSlotsFrozenState,
) -> Vec<ResultingStateChange> {
    let EpochSlotsFrozenState {
        bank_status,
        epoch_slots_frozen_hash,
        duplicate_confirmed_hash,
    } = epoch_slots_frozen_state;

    if let Some(duplicate_confirmed_hash) = duplicate_confirmed_hash {
        if epoch_slots_frozen_hash != duplicate_confirmed_hash {
            warn!(
                "EpochSlots sample returned slot {} with hash {}, but we already saw
                duplicate confirmation on hash: {:?}",
                slot, epoch_slots_frozen_hash, duplicate_confirmed_hash
            );
        }
        return vec![];
    }

    match bank_status {
        BankStatus::Dead | BankStatus::Frozen(_) => (),
        // No action to be taken yet
        BankStatus::Unprocessed => {
            return vec![];
        }
    }

    let frozen_hash = bank_status.bank_hash().expect("bank hash must exist");
    let is_dead = bank_status.is_dead();
    let mut state_changes = vec![];
    check_epoch_slots_hash_against_frozen_hash(
        &mut state_changes,
        slot,
        epoch_slots_frozen_hash,
        frozen_hash,
        is_dead,
    );

    state_changes
}

fn get_cluster_confirmed_hash_from_state(
    slot: Slot,
    gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
    epoch_slots_frozen_slots: &EpochSlotsFrozenSlots,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    bank_frozen_hash: Option<Hash>,
) -> Option<ClusterConfirmedHash> {
    let gossip_duplicate_confirmed_hash = gossip_duplicate_confirmed_slots.get(&slot).cloned();
    // If the bank hasn't been frozen yet, then we haven't duplicate confirmed a local version
    // this slot through replay yet.
    let is_local_replay_duplicate_confirmed = if let Some(bank_frozen_hash) = bank_frozen_hash {
        fork_choice
            .is_duplicate_confirmed(&(slot, bank_frozen_hash))
            .unwrap_or(false)
    } else {
        false
    };

    get_duplicate_confirmed_hash(
        slot,
        gossip_duplicate_confirmed_hash,
        bank_frozen_hash,
        is_local_replay_duplicate_confirmed,
    )
    .map(ClusterConfirmedHash::DuplicateConfirmed)
    .or_else(|| {
        epoch_slots_frozen_slots
            .get(&slot)
            .map(|hash| ClusterConfirmedHash::EpochSlotsFrozen(*hash))
    })
}

fn get_duplicate_confirmed_hash_from_state(
    slot: Slot,
    gossip_duplicate_confirmed_slots: &GossipDuplicateConfirmedSlots,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    bank_frozen_hash: Option<Hash>,
) -> Option<Hash> {
    let gossip_duplicate_confirmed_hash = gossip_duplicate_confirmed_slots.get(&slot).cloned();
    // If the bank hasn't been frozen yet, then we haven't duplicate confirmed a local version
    // this slot through replay yet.
    let is_local_replay_duplicate_confirmed = if let Some(bank_frozen_hash) = bank_frozen_hash {
        fork_choice
            .is_duplicate_confirmed(&(slot, bank_frozen_hash))
            .unwrap_or(false)
    } else {
        false
    };

    get_duplicate_confirmed_hash(
        slot,
        gossip_duplicate_confirmed_hash,
        bank_frozen_hash,
        is_local_replay_duplicate_confirmed,
    )
}

fn get_duplicate_confirmed_hash(
    slot: Slot,
    gossip_duplicate_confirmed_hash: Option<Hash>,
    bank_frozen_hash: Option<Hash>,
    is_local_replay_duplicate_confirmed: bool,
) -> Option<Hash> {
    let local_duplicate_confirmed_hash = if is_local_replay_duplicate_confirmed {
        // If local replay has duplicate_confirmed this slot, this slot must have
        // descendants with votes for this slot, hence this slot must be
        // frozen.
        let bank_frozen_hash = bank_frozen_hash.unwrap();
        assert!(bank_frozen_hash != Hash::default());
        Some(bank_frozen_hash)
    } else {
        None
    };

    match (
        local_duplicate_confirmed_hash,
        gossip_duplicate_confirmed_hash,
    ) {
        (Some(local_duplicate_confirmed_hash), Some(gossip_duplicate_confirmed_hash)) => {
            if local_duplicate_confirmed_hash != gossip_duplicate_confirmed_hash {
                error!(
                    "For slot {}, the gossip duplicate confirmed hash {}, is not equal
                to the confirmed hash we replayed: {}",
                    slot, gossip_duplicate_confirmed_hash, local_duplicate_confirmed_hash
                );
            }
            Some(local_duplicate_confirmed_hash)
        }
        (Some(bank_frozen_hash), None) => Some(bank_frozen_hash),
        _ => gossip_duplicate_confirmed_hash,
    }
}

fn apply_state_changes(
    slot: Slot,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    blockstore: &Blockstore,
    ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
    state_changes: Vec<ResultingStateChange>,
) {
    // Handle cases where the bank is frozen, but not duplicate confirmed
    // yet.
    let mut not_duplicate_confirmed_frozen_hash = None;
    for state_change in state_changes {
        match state_change {
            ResultingStateChange::BankFrozen(bank_frozen_hash) => {
                if !fork_choice
                    .is_duplicate_confirmed(&(slot, bank_frozen_hash))
                    .expect("frozen bank must exist in fork choice")
                {
                    not_duplicate_confirmed_frozen_hash = Some(bank_frozen_hash);
                }
            }
            ResultingStateChange::MarkSlotDuplicate(bank_frozen_hash) => {
                fork_choice.mark_fork_invalid_candidate(&(slot, bank_frozen_hash));
            }
            ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash) => {
                duplicate_slots_to_repair.insert(slot, duplicate_confirmed_hash);
            }
            ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(bank_frozen_hash) => {
                not_duplicate_confirmed_frozen_hash = None;
                // When we detect that our frozen slot matches the cluster version (note this
                // will catch both bank frozen first -> confirmation, or confirmation first ->
                // bank frozen), mark all the newly duplicate confirmed slots in blockstore
                let new_duplicate_confirmed_slot_hashes =
                    fork_choice.mark_fork_valid_candidate(&(slot, bank_frozen_hash));
                blockstore
                    .set_duplicate_confirmed_slots_and_hashes(
                        new_duplicate_confirmed_slot_hashes.into_iter(),
                    )
                    .unwrap();
                duplicate_slots_to_repair.remove(&slot);
            }
            ResultingStateChange::SendAncestorHashesReplayUpdate(ancestor_hashes_replay_update) => {
                let _ = ancestor_hashes_replay_update_sender.send(ancestor_hashes_replay_update);
            }
        }
    }

    if let Some(frozen_hash) = not_duplicate_confirmed_frozen_hash {
        blockstore.insert_bank_hash(slot, frozen_hash, false);
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn check_slot_agrees_with_cluster(
    slot: Slot,
    root: Slot,
    blockstore: &Blockstore,
    duplicate_slots_tracker: &mut DuplicateSlotsTracker,
    epoch_slots_frozen_slots: &mut EpochSlotsFrozenSlots,
    fork_choice: &mut HeaviestSubtreeForkChoice,
    duplicate_slots_to_repair: &mut DuplicateSlotsToRepair,
    ancestor_hashes_replay_update_sender: &AncestorHashesReplayUpdateSender,
    slot_state_update: SlotStateUpdate,
) {
    info!(
        "check_slot_agrees_with_cluster() slot: {}, root: {}, slot_state_update: {:?}",
        slot, root, slot_state_update
    );

    if slot <= root {
        return;
    }

    // Needs to happen before the bank_frozen_hash.is_none() check below to account for duplicate
    // signals arriving before the bank is constructed in replay.
    if matches!(slot_state_update, SlotStateUpdate::Duplicate(_)) {
        // If this slot has already been processed before, return
        if !duplicate_slots_tracker.insert(slot) {
            return;
        }
    }

    // Avoid duplicate work from multiple of the same DuplicateConfirmed signal. This can
    // happen if we get duplicate confirmed from gossip and from local replay.
    if let SlotStateUpdate::DuplicateConfirmed(state) = &slot_state_update {
        if let Some(bank_hash) = state.bank_status.bank_hash() {
            if let Some(true) = fork_choice.is_duplicate_confirmed(&(slot, bank_hash)) {
                return;
            }
        }
    }

    if let SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state) = &slot_state_update {
        if let Some(old_epoch_slots_frozen_hash) =
            epoch_slots_frozen_slots.insert(slot, epoch_slots_frozen_state.epoch_slots_frozen_hash)
        {
            if old_epoch_slots_frozen_hash == epoch_slots_frozen_state.epoch_slots_frozen_hash {
                // If EpochSlots has already told us this same hash was frozen, return
                return;
            }
        }
    }

    let state_changes = slot_state_update.into_state_changes(slot);
    apply_state_changes(
        slot,
        fork_choice,
        duplicate_slots_to_repair,
        blockstore,
        ancestor_hashes_replay_update_sender,
        state_changes,
    );
}

#[cfg(test)]
mod test {
    use {
        super::*,
        crate::{progress_map::ProgressMap, replay_stage::tests::setup_forks_from_tree},
        crossbeam_channel::unbounded,
        solana_runtime::bank_forks::BankForks,
        std::{
            collections::{HashMap, HashSet},
            sync::{Arc, RwLock},
        },
        trees::tr,
    };

    macro_rules! state_update_tests {
        ($($name:ident: $value:expr,)*) => {
        $(
            #[test]
            fn $name() {
                let slot = 10;
                let (state_update, expected) = $value;
                assert_eq!(expected, state_update.into_state_changes(slot));
            }
        )*
        }
    }

    state_update_tests! {
        bank_frozen_state_update_0: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = None;
            let is_slot_duplicate = false;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash)]
            )
        },
        bank_frozen_state_update_1: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = None;
            let is_slot_duplicate = true;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash), ResultingStateChange::MarkSlotDuplicate(frozen_hash)]
            )
        },
        bank_frozen_state_update_2: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(frozen_hash));
            let is_slot_duplicate = false;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(frozen_hash)]
            )
        },
        bank_frozen_state_update_3: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(frozen_hash));
            let is_slot_duplicate = false;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash)]
            )
        },
        bank_frozen_state_update_4: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(frozen_hash));
            let is_slot_duplicate = true;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(frozen_hash)]
            )
        },
        bank_frozen_state_update_5: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(frozen_hash));
            let is_slot_duplicate = true;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash)],
            )
        },
        bank_frozen_state_update_6: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash));
            let is_slot_duplicate = false;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        bank_frozen_state_update_7: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let epoch_slots_frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash));
            let is_slot_duplicate = false;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        bank_frozen_state_update_8: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash));
            let is_slot_duplicate = true;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        bank_frozen_state_update_9: {
            // frozen hash has to be non-default for frozen state transition
            let frozen_hash = Hash::new_unique();
            let epoch_slots_frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash));
            let is_slot_duplicate = true;
            let bank_frozen_state = BankFrozenState::new(
                frozen_hash,
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::BankFrozen(bank_frozen_state),
                vec![ResultingStateChange::BankFrozen(frozen_hash),
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        duplicate_confirmed_state_update_0: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let bank_status = BankStatus::Unprocessed;
            let duplicate_confirmed_state = DuplicateConfirmedState::new(
                duplicate_confirmed_hash,
                bank_status,
            );
            (
                SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        duplicate_confirmed_state_update_1: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let bank_status = BankStatus::Dead;
            let duplicate_confirmed_state = DuplicateConfirmedState::new(
                duplicate_confirmed_hash,
                bank_status,
            );
            (
                SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                vec![
                ResultingStateChange::SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(10)),
                ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        duplicate_confirmed_state_update_2: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let bank_status = BankStatus::Frozen(duplicate_confirmed_hash);
            let duplicate_confirmed_state = DuplicateConfirmedState::new(
                duplicate_confirmed_hash,
                bank_status,
            );
            (
                SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                vec![
                ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(duplicate_confirmed_hash)]
            )
        },
        duplicate_confirmed_state_update_3: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let frozen_hash = Hash::new_unique();
            let bank_status = BankStatus::Frozen(frozen_hash);
            let duplicate_confirmed_state = DuplicateConfirmedState::new(
                duplicate_confirmed_hash,
                bank_status,
            );
            (
                SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
                vec![
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        dead_state_update_0: {
            let cluster_confirmed_hash = None;
            let is_slot_duplicate = false;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![
                    ResultingStateChange::SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate::Dead(10))
                ],
            )
        },
        dead_state_update_1: {
            let cluster_confirmed_hash = None;
            let is_slot_duplicate = true;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![
                    ResultingStateChange::SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate::Dead(10)), ResultingStateChange::MarkSlotDuplicate(Hash::default())],
            )
        },
        dead_state_update_2: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash));
            let is_slot_duplicate = false;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![
                ResultingStateChange::SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(10)),
                ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        dead_state_update_3: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash));
            let is_slot_duplicate = false;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![
                ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        dead_state_update_4: {
            let duplicate_confirmed_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::DuplicateConfirmed(duplicate_confirmed_hash));
            let is_slot_duplicate = true;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![

                    ResultingStateChange::SendAncestorHashesReplayUpdate(AncestorHashesReplayUpdate::DeadDuplicateConfirmed(10)),
                ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                ResultingStateChange::RepairDuplicateConfirmedVersion(duplicate_confirmed_hash)],
            )
        },
        dead_state_update_5: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let cluster_confirmed_hash = Some(ClusterConfirmedHash::EpochSlotsFrozen(epoch_slots_frozen_hash));
            let is_slot_duplicate = true;
            let dead_state = DeadState::new(
                cluster_confirmed_hash,
                is_slot_duplicate,
            );
            (
                SlotStateUpdate::Dead(dead_state),
                vec![
                    ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                    ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        duplicate_state_update_0: {
            let duplicate_confirmed_hash = None;
            let bank_status = BankStatus::Unprocessed;
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        duplicate_state_update_1: {
            let duplicate_confirmed_hash = None;
            let bank_status = BankStatus::Dead;
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                vec![ResultingStateChange::MarkSlotDuplicate(Hash::default())],
            )
        },
        duplicate_state_update_2: {
            let duplicate_confirmed_hash = None;
            let bank_hash = Hash::new_unique();
            let bank_status = BankStatus::Frozen(bank_hash);
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                vec![ResultingStateChange::MarkSlotDuplicate(bank_hash)],
            )
        },
        duplicate_state_update_3: {
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Unprocessed;
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                Vec::<ResultingStateChange>::new(),
            )
        },
        duplicate_state_update_4: {
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Dead;
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        duplicate_state_update_5: {
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Frozen(duplicate_confirmed_hash.unwrap());
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        duplicate_state_update_6: {
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let frozen_hash = Hash::new_unique();
            let bank_status = BankStatus::Frozen(frozen_hash);
            let duplicate_state = DuplicateState::new(duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::Duplicate(duplicate_state),
                Vec::<ResultingStateChange>::new(),
            )
        },
        epoch_slots_frozen_state_update_0: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = None;
            let bank_status = BankStatus::Unprocessed;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_1: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Unprocessed;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_2: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(epoch_slots_frozen_hash);
            let bank_status = BankStatus::Unprocessed;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_3: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = None;
            let bank_status = BankStatus::Dead;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                vec![
                ResultingStateChange::MarkSlotDuplicate(Hash::default()),
                ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        epoch_slots_frozen_state_update_4: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Dead;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_5: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(epoch_slots_frozen_hash);
            let bank_status = BankStatus::Dead;
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_6: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = None;
            let frozen_hash = Hash::new_unique();
            let bank_status = BankStatus::Frozen(frozen_hash);
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                vec![
                ResultingStateChange::MarkSlotDuplicate(frozen_hash),
                ResultingStateChange::RepairDuplicateConfirmedVersion(epoch_slots_frozen_hash)],
            )
        },
        epoch_slots_frozen_state_update_7: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = None;
            let bank_status = BankStatus::Frozen(epoch_slots_frozen_hash);
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_8: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Frozen(Hash::new_unique());
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_9: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Frozen(epoch_slots_frozen_hash);
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
        epoch_slots_frozen_state_update_10: {
            let epoch_slots_frozen_hash = Hash::new_unique();
            let duplicate_confirmed_hash = Some(Hash::new_unique());
            let bank_status = BankStatus::Frozen(duplicate_confirmed_hash.unwrap());
            let epoch_slots_frozen_state = EpochSlotsFrozenState::new(epoch_slots_frozen_hash, duplicate_confirmed_hash, bank_status);
            (
                SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
                Vec::<ResultingStateChange>::new()
            )
        },
    }

    struct InitialState {
        heaviest_subtree_fork_choice: HeaviestSubtreeForkChoice,
        progress: ProgressMap,
        descendants: HashMap<Slot, HashSet<Slot>>,
        bank_forks: Arc<RwLock<BankForks>>,
        blockstore: Blockstore,
    }

    fn setup() -> InitialState {
        // Create simple fork 0 -> 1 -> 2 -> 3
        let forks = tr(0) / (tr(1) / (tr(2) / tr(3)));
        let (vote_simulator, blockstore) = setup_forks_from_tree(forks, 1, None);
        let descendants = vote_simulator.bank_forks.read().unwrap().descendants();
        InitialState {
            heaviest_subtree_fork_choice: vote_simulator.heaviest_subtree_fork_choice,
            progress: vote_simulator.progress,
            descendants,
            bank_forks: vote_simulator.bank_forks,
            blockstore,
        }
    }

    #[test]
    fn test_apply_state_changes() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            descendants,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // MarkSlotDuplicate should mark progress map and remove
        // the slot from fork choice
        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &ancestor_hashes_replay_update_sender,
            vec![ResultingStateChange::MarkSlotDuplicate(duplicate_slot_hash)],
        );
        assert!(!heaviest_subtree_fork_choice
            .is_candidate(&(duplicate_slot, duplicate_slot_hash))
            .unwrap());
        for child_slot in descendants
            .get(&duplicate_slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&duplicate_slot))
        {
            assert_eq!(
                heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(
                        *child_slot,
                        bank_forks.read().unwrap().get(*child_slot).unwrap().hash()
                    ))
                    .unwrap(),
                duplicate_slot
            );
        }
        assert!(duplicate_slots_to_repair.is_empty());

        // Simulate detecting another hash that is the correct version,
        // RepairDuplicateConfirmedVersion should add the slot to repair
        // to `duplicate_slots_to_repair`
        assert!(duplicate_slots_to_repair.is_empty());
        let correct_hash = Hash::new_unique();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &ancestor_hashes_replay_update_sender,
            vec![ResultingStateChange::RepairDuplicateConfirmedVersion(
                correct_hash,
            )],
        );
        assert_eq!(duplicate_slots_to_repair.len(), 1);
        assert_eq!(
            *duplicate_slots_to_repair.get(&duplicate_slot).unwrap(),
            correct_hash
        );
    }

    #[test]
    fn test_apply_state_changes_bank_frozen() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();

        // Simulate ReplayStage freezing a Bank with the given hash.
        // BankFrozen should mark it down in Blockstore.
        assert!(blockstore.get_bank_hash(duplicate_slot).is_none());
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &ancestor_hashes_replay_update_sender,
            vec![ResultingStateChange::BankFrozen(duplicate_slot_hash)],
        );
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            duplicate_slot_hash
        );
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));

        // If we freeze another version of the bank, it should overwrite the first
        // version in blockstore.
        let new_bank_hash = Hash::new_unique();
        let root_slot_hash = {
            let root_bank = bank_forks.read().unwrap().root_bank();
            (root_bank.slot(), root_bank.hash())
        };
        heaviest_subtree_fork_choice
            .add_new_leaf_slot((duplicate_slot, new_bank_hash), Some(root_slot_hash));
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &ancestor_hashes_replay_update_sender,
            vec![ResultingStateChange::BankFrozen(new_bank_hash)],
        );
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            new_bank_hash
        );
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));
    }

    fn run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
        modify_state_changes: impl Fn(Hash, &mut Vec<ResultingStateChange>),
    ) {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            descendants,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        let duplicate_slot = bank_forks.read().unwrap().root() + 1;
        let our_duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();

        // Setup and check the state that is about to change.
        duplicate_slots_to_repair.insert(duplicate_slot, Hash::new_unique());
        assert!(blockstore.get_bank_hash(duplicate_slot).is_none());
        assert!(!blockstore.is_duplicate_confirmed(duplicate_slot));

        // DuplicateConfirmedSlotMatchesCluster should:
        // 1) Re-enable fork choice
        // 2) Clear any pending repairs from `duplicate_slots_to_repair` since we have the
        //    right version now
        // 3) Set the status to duplicate confirmed in Blockstore
        let mut state_changes = vec![ResultingStateChange::DuplicateConfirmedSlotMatchesCluster(
            our_duplicate_slot_hash,
        )];
        modify_state_changes(our_duplicate_slot_hash, &mut state_changes);
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        apply_state_changes(
            duplicate_slot,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &blockstore,
            &ancestor_hashes_replay_update_sender,
            state_changes,
        );
        for child_slot in descendants
            .get(&duplicate_slot)
            .unwrap()
            .iter()
            .chain(std::iter::once(&duplicate_slot))
        {
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(
                    *child_slot,
                    bank_forks.read().unwrap().get(*child_slot).unwrap().hash()
                ))
                .is_none());
        }
        assert!(heaviest_subtree_fork_choice
            .is_candidate(&(duplicate_slot, our_duplicate_slot_hash))
            .unwrap());
        assert!(duplicate_slots_to_repair.is_empty());
        assert_eq!(
            blockstore.get_bank_hash(duplicate_slot).unwrap(),
            our_duplicate_slot_hash
        );
        assert!(blockstore.is_duplicate_confirmed(duplicate_slot));
    }

    #[test]
    fn test_apply_state_changes_duplicate_confirmed_matches_frozen() {
        run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
            |_our_duplicate_slot_hash, _state_changes: &mut Vec<ResultingStateChange>| {},
        );
    }

    #[test]
    fn test_apply_state_changes_bank_frozen_and_duplicate_confirmed_matches_frozen() {
        run_test_apply_state_changes_duplicate_confirmed_matches_frozen(
            |our_duplicate_slot_hash, state_changes: &mut Vec<ResultingStateChange>| {
                state_changes.push(ResultingStateChange::BankFrozen(our_duplicate_slot_hash));
            },
        );
    }

    fn run_test_state_duplicate_then_bank_frozen(initial_bank_hash: Option<Hash>) {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        // Setup a duplicate slot state transition with the initial bank state of the duplicate slot
        // determined by `initial_bank_hash`, which can be:
        // 1) A default hash (unfrozen bank),
        // 2) None (a slot that hasn't even started replay yet).
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();
        let duplicate_slot = 2;
        let duplicate_state = DuplicateState::new_from_state(
            duplicate_slot,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(duplicate_slot).unwrap_or(false),
            || initial_bank_hash,
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            duplicate_slot,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::Duplicate(duplicate_state),
        );
        assert!(duplicate_slots_tracker.contains(&duplicate_slot));
        // Nothing should be applied yet to fork choice, since bank was not yet frozen
        for slot in 2..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }

        // Now freeze the bank
        let frozen_duplicate_slot_hash = bank_forks
            .read()
            .unwrap()
            .get(duplicate_slot)
            .unwrap()
            .hash();
        let bank_frozen_state = BankFrozenState::new_from_state(
            duplicate_slot,
            frozen_duplicate_slot_hash,
            &mut duplicate_slots_tracker,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            &epoch_slots_frozen_slots,
        );
        check_slot_agrees_with_cluster(
            duplicate_slot,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::BankFrozen(bank_frozen_state),
        );

        // Progress map should have the correct updates, fork choice should mark duplicate
        // as unvotable
        assert!(heaviest_subtree_fork_choice
            .is_unconfirmed_duplicate(&(duplicate_slot, frozen_duplicate_slot_hash))
            .unwrap());

        // The ancestor of the duplicate slot should be the best slot now
        let (duplicate_ancestor, duplicate_parent_hash) = {
            let r_bank_forks = bank_forks.read().unwrap();
            let parent_bank = r_bank_forks.get(duplicate_slot).unwrap().parent().unwrap();
            (parent_bank.slot(), parent_bank.hash())
        };
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (duplicate_ancestor, duplicate_parent_hash)
        );
    }

    #[test]
    fn test_state_unfrozen_bank_duplicate_then_bank_frozen() {
        run_test_state_duplicate_then_bank_frozen(Some(Hash::default()));
    }

    #[test]
    fn test_state_unreplayed_bank_duplicate_then_bank_frozen() {
        run_test_state_duplicate_then_bank_frozen(None);
    }

    #[test]
    fn test_state_ancestor_confirmed_descendant_duplicate() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();

        // Mark slot 2 as duplicate confirmed
        let slot2_hash = bank_forks.read().unwrap().get(2).unwrap().hash();
        gossip_duplicate_confirmed_slots.insert(2, slot2_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            slot2_hash,
            || progress.is_dead(2).unwrap_or(false),
            || Some(slot2_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            2,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut EpochSlotsFrozenSlots::default(),
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        assert!(heaviest_subtree_fork_choice
            .is_duplicate_confirmed(&(2, slot2_hash))
            .unwrap());
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        for slot in 0..=2 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .is_duplicate_confirmed(&(slot, slot_hash))
                .unwrap());
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }

        // Mark 3 as duplicate, should not remove the duplicate confirmed slot 2 from
        // fork choice
        let duplicate_state = DuplicateState::new_from_state(
            3,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut EpochSlotsFrozenSlots::default(),
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::Duplicate(duplicate_state),
        );
        assert!(duplicate_slots_tracker.contains(&3));
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (2, slot2_hash)
        );
        for slot in 0..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            if slot <= 2 {
                assert!(heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap());
                assert!(heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(slot, slot_hash))
                    .is_none());
            } else {
                assert!(!heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap());
                assert_eq!(
                    heaviest_subtree_fork_choice
                        .latest_invalid_ancestor(&(slot, slot_hash))
                        .unwrap(),
                    3
                );
            }
        }
    }

    #[test]
    fn test_state_ancestor_duplicate_descendant_confirmed() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();

        // Mark 2 as duplicate
        let slot2_hash = bank_forks.read().unwrap().get(2).unwrap().hash();
        let duplicate_state = DuplicateState::new_from_state(
            2,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(2).unwrap_or(false),
            || Some(slot2_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            2,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut EpochSlotsFrozenSlots::default(),
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::Duplicate(duplicate_state),
        );
        assert!(duplicate_slots_tracker.contains(&2));
        for slot in 2..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert_eq!(
                heaviest_subtree_fork_choice
                    .latest_invalid_ancestor(&(slot, slot_hash))
                    .unwrap(),
                2
            );
        }

        let slot1_hash = bank_forks.read().unwrap().get(1).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (1, slot1_hash)
        );

        // Mark slot 3 as duplicate confirmed, should mark slot 2 as duplicate confirmed as well
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            slot3_hash,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut EpochSlotsFrozenSlots::default(),
            &mut heaviest_subtree_fork_choice,
            &mut DuplicateSlotsToRepair::default(),
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        for slot in 0..=3 {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            assert!(heaviest_subtree_fork_choice
                .is_duplicate_confirmed(&(slot, slot_hash))
                .unwrap());
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    fn verify_all_slots_duplicate_confirmed(
        bank_forks: &RwLock<BankForks>,
        heaviest_subtree_fork_choice: &HeaviestSubtreeForkChoice,
        upper_bound: Slot,
        expected_is_duplicate_confirmed: bool,
    ) {
        for slot in 0..upper_bound {
            let slot_hash = bank_forks.read().unwrap().get(slot).unwrap().hash();
            let expected_is_duplicate_confirmed = expected_is_duplicate_confirmed ||
            // root is always duplicate confirmed
            slot == 0;
            assert_eq!(
                heaviest_subtree_fork_choice
                    .is_duplicate_confirmed(&(slot, slot_hash))
                    .unwrap(),
                expected_is_duplicate_confirmed
            );
            assert!(heaviest_subtree_fork_choice
                .latest_invalid_ancestor(&(slot, slot_hash))
                .is_none());
        }
    }

    #[test]
    fn test_state_descendant_confirmed_ancestor_duplicate() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as duplicate confirmed
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            slot3_hash,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        verify_all_slots_duplicate_confirmed(&bank_forks, &heaviest_subtree_fork_choice, 3, true);
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );

        // Mark ancestor 1 as duplicate, fork choice should be unaffected since
        // slot 1 was duplicate confirmed by the confirmation on its
        // descendant, 3.
        let slot1_hash = bank_forks.read().unwrap().get(1).unwrap().hash();
        let duplicate_state = DuplicateState::new_from_state(
            1,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(1).unwrap_or(false),
            || Some(slot1_hash),
        );
        check_slot_agrees_with_cluster(
            1,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::Duplicate(duplicate_state),
        );
        assert!(duplicate_slots_tracker.contains(&1));
        verify_all_slots_duplicate_confirmed(&bank_forks, &heaviest_subtree_fork_choice, 3, true);
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    #[test]
    fn test_duplicate_confirmed_and_epoch_slots_frozen() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as only epoch slots frozen, matching our `slot3_hash`, should not duplicate
        // confirm the slot
        let mut expected_is_duplicate_confirmed = false;
        let epoch_slots_frozen_state = EpochSlotsFrozenState::new_from_state(
            3,
            slot3_hash,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
        );
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );

        // Mark 3 as duplicate confirmed and epoch slots frozen with the same hash. Should
        // duplicate confirm all descendants of 3
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        expected_is_duplicate_confirmed = true;
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            slot3_hash,
            || progress.is_dead(2).unwrap_or(false),
            || Some(slot3_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        assert_eq!(*epoch_slots_frozen_slots.get(&3).unwrap(), slot3_hash);
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }

    #[test]
    fn test_duplicate_confirmed_and_epoch_slots_frozen_mismatched() {
        // Common state
        let InitialState {
            mut heaviest_subtree_fork_choice,
            progress,
            bank_forks,
            blockstore,
            ..
        } = setup();

        let slot3_hash = bank_forks.read().unwrap().get(3).unwrap().hash();
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
        let root = 0;
        let mut duplicate_slots_tracker = DuplicateSlotsTracker::default();
        let mut gossip_duplicate_confirmed_slots = GossipDuplicateConfirmedSlots::default();
        let mut epoch_slots_frozen_slots = EpochSlotsFrozenSlots::default();
        let mut duplicate_slots_to_repair = DuplicateSlotsToRepair::default();

        // Mark 3 as only epoch slots frozen with different hash than the our
        // locally replayed `slot3_hash`. This should not duplicate confirm the slot,
        // but should add the epoch slots frozen hash to the repair set
        let mismatched_hash = Hash::new_unique();
        let mut expected_is_duplicate_confirmed = false;
        let epoch_slots_frozen_state = EpochSlotsFrozenState::new_from_state(
            3,
            mismatched_hash,
            &gossip_duplicate_confirmed_slots,
            &mut heaviest_subtree_fork_choice,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        let (ancestor_hashes_replay_update_sender, _ancestor_hashes_replay_update_receiver) =
            unbounded();
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::EpochSlotsFrozen(epoch_slots_frozen_state),
        );
        assert_eq!(*duplicate_slots_to_repair.get(&3).unwrap(), mismatched_hash);
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );

        // Mark our version of slot 3 as duplicate confirmed with a hash different than
        // the epoch slots frozen hash above. Should duplicate confirm all descendants of
        // 3 and remove the mismatched hash from `duplicate_slots_to_repair`, since we
        // have the right version now, no need to repair
        gossip_duplicate_confirmed_slots.insert(3, slot3_hash);
        expected_is_duplicate_confirmed = true;
        let duplicate_confirmed_state = DuplicateConfirmedState::new_from_state(
            slot3_hash,
            || progress.is_dead(3).unwrap_or(false),
            || Some(slot3_hash),
        );
        check_slot_agrees_with_cluster(
            3,
            root,
            &blockstore,
            &mut duplicate_slots_tracker,
            &mut epoch_slots_frozen_slots,
            &mut heaviest_subtree_fork_choice,
            &mut duplicate_slots_to_repair,
            &ancestor_hashes_replay_update_sender,
            SlotStateUpdate::DuplicateConfirmed(duplicate_confirmed_state),
        );
        assert!(duplicate_slots_to_repair.is_empty());
        assert_eq!(*epoch_slots_frozen_slots.get(&3).unwrap(), mismatched_hash);
        verify_all_slots_duplicate_confirmed(
            &bank_forks,
            &heaviest_subtree_fork_choice,
            3,
            expected_is_duplicate_confirmed,
        );
        assert_eq!(
            heaviest_subtree_fork_choice.best_overall_slot(),
            (3, slot3_hash)
        );
    }
}
