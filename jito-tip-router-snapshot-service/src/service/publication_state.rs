use {
    crate::candidate::CandidateIdentity,
    log::{debug, error, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
    std::{collections::HashSet, sync::Arc},
};

// At any given time the service is either:
// 1. (Idle) - waiting for end of epoch
// 2. (Collecting) - handling one or more unrooted epoch-boundary candidates
// 3. (Publishing) - taking the rooted candidate, publishing it, and cleaning up dead forks
#[derive(Debug, Default)]
enum StakeSnapshotState {
    #[default]
    Idle,
    Collecting {
        epoch: Epoch,
        candidates: HashSet<CandidateIdentity>,
    },
    Publishing {
        winner: CandidateIdentity,
    },
}

pub(super) struct SnapshotPublicationState {
    state: StakeSnapshotState,
    latest_published_epoch: Option<Epoch>,
}

impl SnapshotPublicationState {
    pub(super) fn new(latest_published_epoch: Option<Epoch>) -> Self {
        Self {
            state: StakeSnapshotState::Idle,
            latest_published_epoch,
        }
    }

    pub(super) fn candidate_from_boundary_bank(
        &self,
        boundary_child_bank: Arc<Bank>,
    ) -> Option<(CandidateIdentity, Arc<Bank>)> {
        let Some(parent_bank) = boundary_child_bank.parent() else {
            error!("frozen epoch-boundary bank has no parent");
            return None;
        };

        if boundary_child_bank.epoch() <= parent_bank.epoch() {
            warn!("non boundary-candidate passed through boundary-candidate filter");
            return None;
        }

        let candidate = CandidateIdentity::from_bank(&parent_bank);

        // This should never happen
        if self
            .latest_published_epoch
            .is_some_and(|published_epoch| candidate.epoch <= published_epoch)
        {
            warn!(
                "skipping frozen bank at epoch={} because it is already published: {}",
                candidate.epoch,
                self.latest_published_epoch.unwrap(),
            );
            return None;
        }

        Some((candidate, parent_bank))
    }

    pub(super) fn allows_candidate(&self, candidate: CandidateIdentity) -> bool {
        match &self.state {
            StakeSnapshotState::Idle => true,
            StakeSnapshotState::Publishing { winner } => {
                debug!(
                    "discarding frozen epoch-boundary candidate {candidate:?} while publishing \
                     {winner:?}"
                );
                false
            }
            StakeSnapshotState::Collecting { epoch, .. } if candidate.epoch < *epoch => {
                warn!(
                    "received out-of-order frozen epoch-boundary candidate {candidate:?} while \
                     collecting candidates for newer epoch {epoch}"
                );
                false
            }
            StakeSnapshotState::Collecting { candidates, .. }
                if candidates.contains(&candidate) =>
            {
                warn!(
                    "received duplicate frozen bank: {candidate:?} has already been seen and \
                     handled"
                );
                false
            }
            StakeSnapshotState::Collecting { .. } => true,
        }
    }

    /// State Machine Transition Function
    /// Keeps candidates for one epoch. Advancing to a newer epoch abandons the old
    /// candidates in memory and deliberately leaves their durable files untouched.
    pub(super) fn record_candidate(&mut self, candidate: CandidateIdentity) {
        //TODO: This contains a lot of duplicate error cases from "allows_candidate" but we
        //want to explicitly run the "write" to the fork state after we launch the writer thread.
        //But we want to run the checks before it
        let state = &mut self.state;
        match state {
            StakeSnapshotState::Idle => {
                *state = StakeSnapshotState::Collecting {
                    epoch: candidate.epoch,
                    candidates: HashSet::from([candidate]),
                };
            }
            StakeSnapshotState::Publishing { winner } => error!(
                "could not record spawned tip-router snapshot candidate {candidate:?}: \
                 publication of {winner:?} began after it was admitted"
            ),
            StakeSnapshotState::Collecting { epoch, .. } if candidate.epoch < *epoch => {
                error!(
                    "could not record spawned tip-router snapshot candidate {candidate:?}: \
                     collection advanced to newer epoch {epoch} after it was admitted"
                );
            }
            StakeSnapshotState::Collecting { candidates, .. }
                if candidates.contains(&candidate) =>
            {
                error!(
                    "could not record spawned tip-router snapshot candidate {candidate:?}: it was \
                     recorded after admission"
                );
            }
            StakeSnapshotState::Collecting { epoch, candidates } if candidate.epoch == *epoch => {
                candidates.insert(candidate);
            }
            StakeSnapshotState::Collecting { .. } => {
                *state = StakeSnapshotState::Collecting {
                    epoch: candidate.epoch,
                    candidates: HashSet::from([candidate]),
                };
            }
        }
    }

    pub(super) fn active_candidates(&self) -> Option<&HashSet<CandidateIdentity>> {
        if let StakeSnapshotState::Collecting { candidates, .. } = &self.state {
            Some(candidates)
        } else {
            None
        }
    }

    // Checks to see if any candidates we've collected have been rooted yet
    pub(super) fn select_rooted_winner(
        &mut self,
        rooted_chain: &[(Slot, Hash)],
    ) -> Option<CandidateIdentity> {
        if let Some(candidates) = self.active_candidates().cloned() {
            for c in candidates {
                if rooted_chain.contains(&(c.slot, c.bank_hash)) {
                    self.state = StakeSnapshotState::Publishing { winner: c };
                    return Some(c);
                }
            }
        }
        None
    }

    /// Returns true when the failed worker owned the winner currently being published.
    pub(super) fn discard_failed_candidate(&mut self, candidate: CandidateIdentity) -> bool {
        match &mut self.state {
            StakeSnapshotState::Collecting { candidates, .. } => {
                candidates.remove(&candidate);
                false
            }
            StakeSnapshotState::Publishing { winner } => *winner == candidate,
            StakeSnapshotState::Idle => false,
        }
    }

    pub(super) fn publishing_winner(&self) -> Option<CandidateIdentity> {
        match &self.state {
            StakeSnapshotState::Publishing { winner } => Some(*winner),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(super) fn latest_published_epoch(&self) -> Option<Epoch> {
        self.latest_published_epoch
    }

    pub(super) fn finish_publication(&mut self, winner: CandidateIdentity) {
        self.latest_published_epoch = Some(
            self.latest_published_epoch
                .map_or(winner.epoch, |epoch| epoch.max(winner.epoch)),
        );
        self.state = StakeSnapshotState::Idle;
    }
}
