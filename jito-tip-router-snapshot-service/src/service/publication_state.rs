use {
    crate::candidate::CandidateIdentity,
    log::{debug, error, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
    std::{collections::HashSet, sync::Arc},
};

// At any given time the service is either:
// 1. (AwaitingCandidate) - waiting for end of epoch
// 2. (TrackingCandidates) - handling one or more unrooted epoch-boundary candidates
// 3. (WinnerPendingPublication) - taking the rooted candidate, publishing it, and cleaning up
// dead forks
#[derive(Debug, Default)]
enum SnapshotPublicationPhase {
    #[default]
    AwaitingCandidate,
    TrackingCandidates {
        candidate_epoch: Epoch,
        tracked_candidates: HashSet<CandidateIdentity>,
    },
    WinnerPendingPublication {
        pending_winner: CandidateIdentity,
    },
}

pub(super) struct SnapshotPublicationTracker {
    phase: SnapshotPublicationPhase,
    latest_published_epoch: Option<Epoch>,
}

impl SnapshotPublicationTracker {
    pub(super) fn new(latest_published_epoch: Option<Epoch>) -> Self {
        Self {
            phase: SnapshotPublicationPhase::AwaitingCandidate,
            latest_published_epoch,
        }
    }

    pub(super) fn eligible_candidate_from_boundary_child(
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

    pub(super) fn can_spawn_candidate(&self, candidate: CandidateIdentity) -> bool {
        match &self.phase {
            SnapshotPublicationPhase::AwaitingCandidate => true,
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner } => {
                debug!(
                    "discarding frozen epoch-boundary candidate {candidate:?} while publishing \
                     {pending_winner:?}"
                );
                false
            }
            SnapshotPublicationPhase::TrackingCandidates {
                candidate_epoch, ..
            } if candidate.epoch < *candidate_epoch => {
                warn!(
                    "received out-of-order frozen epoch-boundary candidate {candidate:?} while \
                     tracking candidates for newer epoch {candidate_epoch}"
                );
                false
            }
            SnapshotPublicationPhase::TrackingCandidates {
                tracked_candidates, ..
            } if tracked_candidates.contains(&candidate) => {
                warn!(
                    "received duplicate frozen bank: {candidate:?} has already been seen and \
                     handled"
                );
                false
            }
            SnapshotPublicationPhase::TrackingCandidates { .. } => true,
        }
    }

    /// State Machine Transition Function
    /// Keeps candidates for one epoch. Advancing to a newer epoch abandons the old
    /// candidates in memory and deliberately leaves their durable files untouched.
    pub(super) fn record_spawned_candidate(&mut self, candidate: CandidateIdentity) {
        //TODO: This contains a lot of duplicate error cases from "can_spawn_candidate" but we
        //want to explicitly run the "write" to the fork state after we launch the writer thread.
        //But we want to run the checks before it
        let phase = &mut self.phase;
        match phase {
            SnapshotPublicationPhase::AwaitingCandidate => {
                *phase = SnapshotPublicationPhase::TrackingCandidates {
                    candidate_epoch: candidate.epoch,
                    tracked_candidates: HashSet::from([candidate]),
                };
            }
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner } => error!(
                "could not record spawned tip-router snapshot candidate {candidate:?}: \
                 publication of {pending_winner:?} began after it was admitted"
            ),
            SnapshotPublicationPhase::TrackingCandidates {
                candidate_epoch, ..
            } if candidate.epoch < *candidate_epoch => {
                error!(
                    "could not record spawned tip-router snapshot candidate {candidate:?}: \
                     candidate tracking advanced to newer epoch {candidate_epoch} after it was \
                     admitted"
                );
            }
            SnapshotPublicationPhase::TrackingCandidates {
                tracked_candidates, ..
            } if tracked_candidates.contains(&candidate) => {
                error!(
                    "could not record spawned tip-router snapshot candidate {candidate:?}: it was \
                     recorded after admission"
                );
            }
            SnapshotPublicationPhase::TrackingCandidates {
                candidate_epoch,
                tracked_candidates,
            } if candidate.epoch == *candidate_epoch => {
                tracked_candidates.insert(candidate);
            }
            SnapshotPublicationPhase::TrackingCandidates { .. } => {
                *phase = SnapshotPublicationPhase::TrackingCandidates {
                    candidate_epoch: candidate.epoch,
                    tracked_candidates: HashSet::from([candidate]),
                };
            }
        }
    }

    pub(super) fn tracked_candidates(&self) -> Option<&HashSet<CandidateIdentity>> {
        if let SnapshotPublicationPhase::TrackingCandidates {
            tracked_candidates, ..
        } = &self.phase
        {
            Some(tracked_candidates)
        } else {
            None
        }
    }

    // Checks to see if any candidates we've collected have been rooted yet
    pub(super) fn select_winner_for_publication(
        &mut self,
        rooted_chain: &[(Slot, Hash)],
    ) -> Option<CandidateIdentity> {
        if let Some(candidates) = self.tracked_candidates().cloned() {
            for candidate in candidates {
                if rooted_chain.contains(&(candidate.slot, candidate.bank_hash)) {
                    self.phase = SnapshotPublicationPhase::WinnerPendingPublication {
                        pending_winner: candidate,
                    };
                    return Some(candidate);
                }
            }
        }
        None
    }

    pub(super) fn record_winner_publication_failure(&mut self, winner: CandidateIdentity) {
        if !matches!(
            self.phase,
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner }
                if pending_winner == winner
        ) {
            error!(
                "could not record failed publication for {winner:?}: it is not the pending winner"
            );
            return;
        }

        self.phase = SnapshotPublicationPhase::AwaitingCandidate;
    }

    /// Returns true when the failed worker owned the winner currently being published.
    pub(super) fn record_candidate_failure(&mut self, candidate: CandidateIdentity) -> bool {
        match &mut self.phase {
            SnapshotPublicationPhase::TrackingCandidates {
                tracked_candidates, ..
            } => {
                tracked_candidates.remove(&candidate);
                false
            }
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner } => {
                *pending_winner == candidate
            }
            SnapshotPublicationPhase::AwaitingCandidate => false,
        }
    }

    pub(super) fn record_winner_published(&mut self, winner: CandidateIdentity) {
        self.latest_published_epoch = Some(
            self.latest_published_epoch
                .map_or(winner.epoch, |epoch| epoch.max(winner.epoch)),
        );
        self.phase = SnapshotPublicationPhase::AwaitingCandidate;
    }
}
