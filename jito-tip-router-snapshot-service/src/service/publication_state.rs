use {
    crate::CandidateIdentity,
    log::{debug, error, info, warn},
    solana_clock::{BankId, Epoch, Slot},
    solana_runtime::bank::Bank,
    std::{collections::HashMap, sync::Arc},
};

/// Whether a candidate's snapshot worker has finished writing its artifact file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactState {
    InFlight,
    Written,
}

#[derive(Debug)]
struct TrackedCandidate {
    artifact_state: ArtifactState,
    boundary_children: Vec<(Slot, BankId)>,
}

impl TrackedCandidate {
    // Checks to see if the candidate's child bank (the bank kicking off the new
    // epoch) has been rooted. If so, the candidate is eligible for publication.
    fn has_rooted_boundary_child(&self, rooted_chain: &[(Slot, BankId)]) -> bool {
        self.boundary_children
            .iter()
            .any(|child| rooted_chain.contains(child))
    }
}

// At any given time the service is either:
// 1. (AwaitingCandidate) - waiting for end of epoch
// 2. (TrackingCandidates) - handling parents of unrooted epoch-boundary children
// 3. (WinnerPendingPublication) - a boundary child has rooted and its parent's artifact is
// either being published or waiting for its worker to finish writing
#[derive(Debug, Default)]
enum SnapshotPublicationPhase {
    #[default]
    AwaitingCandidate,
    TrackingCandidates {
        candidate_epoch: Epoch,
        tracked_candidates: HashMap<CandidateIdentity, TrackedCandidate>,
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
    pub(super) fn new() -> Self {
        Self {
            phase: SnapshotPublicationPhase::AwaitingCandidate,
            latest_published_epoch: None,
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
                warn!(
                    "discarding frozen epoch-boundary candidate {candidate} while publishing \
                     {pending_winner}"
                );
                false
            }
            // This should never happen
            SnapshotPublicationPhase::TrackingCandidates {
                candidate_epoch, ..
            } if candidate.epoch < *candidate_epoch => {
                warn!(
                    "received out-of-order frozen epoch-boundary candidate {candidate} while \
                     tracking candidates for newer epoch {candidate_epoch}"
                );
                false
            }
            SnapshotPublicationPhase::TrackingCandidates { .. } => true,
        }
    }

    pub(super) fn record_boundary_child_for_existing_candidate(
        &mut self,
        candidate: CandidateIdentity,
        boundary_child: (Slot, BankId),
    ) -> bool {
        let SnapshotPublicationPhase::TrackingCandidates {
            tracked_candidates, ..
        } = &mut self.phase
        else {
            return false;
        };
        let Some(tracked) = tracked_candidates.get_mut(&candidate) else {
            return false;
        };

        if !tracked.boundary_children.contains(&boundary_child) {
            tracked.boundary_children.push(boundary_child);
        }
        true
    }

    /// State Machine Transition Function
    /// Keeps candidates for one epoch. Advancing to a newer epoch abandons the old
    /// candidates in memory and deliberately leaves their durable files untouched.
    pub(super) fn record_spawned_candidate(
        &mut self,
        candidate: CandidateIdentity,
        boundary_child: (Slot, BankId),
    ) {
        let tracked = TrackedCandidate {
            artifact_state: ArtifactState::InFlight,
            boundary_children: vec![boundary_child],
        };
        let phase = &mut self.phase;
        match phase {
            SnapshotPublicationPhase::AwaitingCandidate => {
                *phase = SnapshotPublicationPhase::TrackingCandidates {
                    candidate_epoch: candidate.epoch,
                    tracked_candidates: HashMap::from([(candidate, tracked)]),
                };
            }
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner } => error!(
                "could not record spawned tip-router snapshot candidate {candidate}: publication \
                 of {pending_winner} began after it was admitted"
            ),
            SnapshotPublicationPhase::TrackingCandidates {
                candidate_epoch,
                tracked_candidates,
            } if candidate.epoch == *candidate_epoch => {
                tracked_candidates.insert(candidate, tracked);
            }
            SnapshotPublicationPhase::TrackingCandidates { .. } => {
                *phase = SnapshotPublicationPhase::TrackingCandidates {
                    candidate_epoch: candidate.epoch,
                    tracked_candidates: HashMap::from([(candidate, tracked)]),
                };
            }
        }
    }

    fn tracked_candidates(&self) -> Option<&HashMap<CandidateIdentity, TrackedCandidate>> {
        if let SnapshotPublicationPhase::TrackingCandidates {
            tracked_candidates, ..
        } = &self.phase
        {
            Some(tracked_candidates)
        } else {
            None
        }
    }

    /// Selects a parent only when one of its epoch-boundary children is rooted.
    /// A parent can be an ancestor of a rooted fork without its boundary child surviving,
    /// so rooting the parent alone cannot validate publication of its snapshot.
    /// Returns the winner only when its artifact has already been written. Otherwise,
    /// `record_candidate_written` returns it when the worker finishes.
    pub(super) fn select_winner_for_publication(
        &mut self,
        rooted_chain: &[(Slot, BankId)],
    ) -> Option<CandidateIdentity> {
        // Match both slot and bank ID: a rooted chain contains one bank per slot, and the
        // additional identity prevents a competing fork at the same slot from winning.
        let (winner, tracked) = self
            .tracked_candidates()?
            .iter()
            .find(|(_, candidate)| candidate.has_rooted_boundary_child(rooted_chain))?;
        let (winner, written) = (*winner, tracked.artifact_state == ArtifactState::Written);

        debug!(
            "picked tip-router snapshot parent {winner} through a rooted boundary child; rooted \
             chain slots: {:?}",
            rooted_chain
                .iter()
                .map(|(slot, _bank_id)| *slot)
                .collect::<Vec<_>>(),
        );
        self.phase = SnapshotPublicationPhase::WinnerPendingPublication {
            pending_winner: winner,
        };
        if written {
            Some(winner)
        } else {
            info!(
                "tip-router snapshot parent {winner} selected by a rooted boundary child before \
                 its artifact finished writing; deferring publication until the worker completes"
            );
            None
        }
    }

    /// Records that `candidate`'s worker finished writing its artifact. Returns the winner
    /// when this write completes a rooted winner awaiting publication, meaning the caller
    /// should publish now.
    pub(super) fn record_candidate_written(
        &mut self,
        candidate: CandidateIdentity,
    ) -> Option<CandidateIdentity> {
        match &mut self.phase {
            SnapshotPublicationPhase::TrackingCandidates {
                tracked_candidates, ..
            } => {
                // A completion for an untracked candidate belongs to an abandoned epoch;
                // its durable file is deliberately left untouched.
                if let Some(tracked) = tracked_candidates.get_mut(&candidate) {
                    tracked.artifact_state = ArtifactState::Written;
                }
                None
            }
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner }
                if *pending_winner == candidate =>
            {
                Some(*pending_winner)
            }
            _ => None,
        }
    }

    pub(super) fn record_winner_publication_failure(&mut self, winner: CandidateIdentity) {
        if !matches!(
            self.phase,
            SnapshotPublicationPhase::WinnerPendingPublication { pending_winner }
                if pending_winner == winner
        ) {
            error!(
                "could not record failed publication for {winner}: it is not the pending winner"
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

#[cfg(test)]
mod tests {
    use {super::SnapshotPublicationTracker, crate::CandidateIdentity, solana_clock::BankId};

    fn parent(slot: u64, bank_id: BankId) -> CandidateIdentity {
        CandidateIdentity {
            epoch: 3,
            slot,
            bank_id,
        }
    }

    #[test]
    fn rooted_parents_do_not_select_or_block_their_boundary_children() {
        let mut tracker = SnapshotPublicationTracker::new();
        // Forks: 98 -> 100 and 98 -> 99 -> 104. Only child 104 survives.
        let ancestor = parent(98, 198);
        let winner = parent(99, 199);
        tracker.record_spawned_candidate(ancestor, (100, 200));
        tracker.record_spawned_candidate(winner, (104, 204));
        assert_eq!(tracker.record_candidate_written(ancestor), None);
        assert_eq!(tracker.record_candidate_written(winner), None);

        assert_eq!(tracker.select_winner_for_publication(&[(98, 198)]), None);
        assert!(tracker.can_spawn_candidate(parent(97, 197)));
        assert_eq!(
            tracker.select_winner_for_publication(&[(99, 199), (98, 198)]),
            None
        );
        assert!(tracker.can_spawn_candidate(parent(97, 197)));

        assert_eq!(
            tracker.select_winner_for_publication(&[(104, 204), (99, 199), (98, 198)]),
            Some(winner)
        );
    }

    #[test]
    fn descendant_root_selects_parent_through_boundary_child() {
        let mut tracker = SnapshotPublicationTracker::new();
        let winner = parent(99, 199);
        tracker.record_spawned_candidate(winner, (104, 204));
        assert_eq!(tracker.record_candidate_written(winner), None);

        assert_eq!(
            tracker.select_winner_for_publication(&[(110, 210), (104, 204), (99, 199)]),
            Some(winner)
        );
    }

    #[test]
    fn completing_parent_worker_without_rooted_child_does_not_publish() {
        let mut tracker = SnapshotPublicationTracker::new();
        let candidate = parent(99, 199);
        tracker.record_spawned_candidate(candidate, (104, 204));

        assert_eq!(tracker.select_winner_for_publication(&[(99, 199)]), None);
        assert_eq!(tracker.record_candidate_written(candidate), None);
        assert!(tracker.can_spawn_candidate(parent(97, 197)));
        assert_eq!(
            tracker.select_winner_for_publication(&[(104, 204), (99, 199)]),
            Some(candidate)
        );
    }

    #[test]
    fn shared_parent_children_are_idempotent_and_preserve_written_artifact() {
        let candidate = parent(99, 199);
        let first_child = (104, 204);
        let second_child = (105, 205);
        let mut tracker = SnapshotPublicationTracker::new();
        tracker.record_spawned_candidate(candidate, first_child);
        assert!(tracker.record_boundary_child_for_existing_candidate(candidate, first_child));
        assert_eq!(tracker.record_candidate_written(candidate), None);
        assert!(tracker.record_boundary_child_for_existing_candidate(candidate, second_child));
        assert!(tracker.record_boundary_child_for_existing_candidate(candidate, second_child));
        assert_eq!(
            tracker.tracked_candidates().unwrap()[&candidate].boundary_children,
            vec![first_child, second_child]
        );
        assert_eq!(
            tracker.select_winner_for_publication(&[second_child]),
            Some(candidate)
        );

        let mut tracker = SnapshotPublicationTracker::new();
        tracker.record_spawned_candidate(candidate, first_child);
        assert!(tracker.record_boundary_child_for_existing_candidate(candidate, second_child));
        assert_eq!(tracker.record_candidate_written(candidate), None);
        assert_eq!(
            tracker.select_winner_for_publication(&[first_child]),
            Some(candidate)
        );
    }

    #[test]
    fn boundary_child_must_match_bank_id() {
        let mut tracker = SnapshotPublicationTracker::new();
        let candidate = parent(99, 199);
        tracker.record_spawned_candidate(candidate, (104, 204));
        assert_eq!(tracker.record_candidate_written(candidate), None);

        assert_eq!(tracker.select_winner_for_publication(&[(104, 999)]), None);
        assert!(tracker.can_spawn_candidate(parent(97, 197)));
        assert_eq!(
            tracker.select_winner_for_publication(&[(104, 204)]),
            Some(candidate)
        );
    }

    #[test]
    fn rooted_child_locks_pending_winner_until_its_worker_completes() {
        let mut tracker = SnapshotPublicationTracker::new();
        let loser = parent(98, 198);
        let winner = parent(99, 199);
        tracker.record_spawned_candidate(loser, (100, 200));
        tracker.record_spawned_candidate(winner, (104, 204));

        assert_eq!(tracker.select_winner_for_publication(&[(104, 204)]), None);
        assert!(!tracker.can_spawn_candidate(parent(97, 197)));
        assert_eq!(tracker.select_winner_for_publication(&[(100, 200)]), None);
        assert_eq!(tracker.record_candidate_written(loser), None);
        assert_eq!(tracker.record_candidate_written(winner), Some(winner));
    }
}
