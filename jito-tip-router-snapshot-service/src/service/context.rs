use {
    super::{
        TipRouterSnapshotServiceError, TipRouterSnapshotServiceResult,
        publication_state::SnapshotPublicationTracker,
        worker_pool::{SnapshotWorkerPool, WorkerShutdownTimeout},
    },
    crate::{
        candidate::CandidateIdentity,
        candidate_store::{CandidateStore, CandidateStoreError, PublicationOutcome},
        config::TipRouterSnapshotConfig,
        snapshot_worker::{SnapshotWorkerError, WorkerCompletion, WorkerOutcome},
    },
    crossbeam_channel::{Receiver, Sender},
    log::{error, info, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationWithDependencyWork,
    },
    solana_runtime::bank::Bank,
    std::{
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

const ARTIFACT_WORKERS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Main state for the top-level service
pub(super) struct TipRouterSnapshotServiceContext {
    publication_state: SnapshotPublicationTracker,
    workers: SnapshotWorkerPool,
}

impl TipRouterSnapshotServiceContext {
    pub(super) fn new(
        completion_sender: Sender<WorkerCompletion>,
        latest_published_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            publication_state: SnapshotPublicationTracker::new(latest_published_epoch),
            workers: SnapshotWorkerPool::new(completion_sender),
        }
    }

    pub(super) fn handle_bank_notification(
        &mut self,
        config: &TipRouterSnapshotConfig,
        candidate_store: &CandidateStore,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
    ) -> TipRouterSnapshotServiceResult {
        match notification {
            BankNotification::Frozen(bank) => {
                self.handle_frozen_bank(config, candidate_store, bank)
            }
            BankNotification::NewRootedChain(rooted_chain) => {
                self.handle_new_rooted_chain(rooted_chain, candidate_store)
            }
            _ => Ok(()),
        }
    }

    fn handle_new_rooted_chain(
        &mut self,
        rooted_chain: Vec<(Slot, Hash)>,
        candidate_store: &CandidateStore,
    ) -> TipRouterSnapshotServiceResult {
        if let Some(winner) = self
            .publication_state
            .select_winner_for_publication(&rooted_chain)
        {
            info!("selected rooted tip-router snapshot candidate {winner:?}");
        }
        self.try_publish_winner(candidate_store);
        Ok(())
    }

    fn handle_frozen_bank(
        &mut self,
        config: &TipRouterSnapshotConfig,
        candidate_store: &CandidateStore,
        boundary_child_bank: Arc<Bank>,
    ) -> TipRouterSnapshotServiceResult {
        let Some((candidate, parent_bank)) = self
            .publication_state
            .eligible_candidate_from_boundary_child(boundary_child_bank)
        else {
            // 99.999% of slots are not epoch-boundary slots
            // (though our notification filter should technically not forward them on the sender
            // side)
            return Ok(());
        };

        if !self.publication_state.can_spawn_candidate(candidate) {
            return Ok(());
        }

        info!(
            "generating tip-router snapshot candidate at slot={}, bank_hash={}, epoch={}",
            candidate.slot, candidate.bank_hash, candidate.epoch,
        );

        if let Err(err) = self.workers.spawn(
            config.clone(),
            candidate_store.clone(),
            candidate,
            parent_bank,
        ) {
            error!("failed to spawn tip-router snapshot worker for {candidate:?}: {err}");
            return Ok(());
        }

        self.publication_state.record_spawned_candidate(candidate);
        Ok(())
    }

    pub(super) fn handle_worker_completion(
        &mut self,
        worker_completion: WorkerCompletion,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let Some(completion) = self.workers.complete_worker(worker_completion) else {
            // Weird case where the worker was already removed and this has been duplicated
            return Ok(());
        };
        self.record_worker_completion(completion, exit)
    }

    fn record_worker_completion(
        &mut self,
        completion: WorkerCompletion,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let WorkerCompletion { candidate, outcome } = completion;
        match outcome {
            WorkerOutcome::Written(path) => {
                info!(
                    "wrote tip-router snapshot candidate {candidate:?} to {}",
                    path.display()
                );
            }
            WorkerOutcome::Failed(SnapshotWorkerError::CandidateStore(
                CandidateStoreError::DirectoryUnavailable { path, source },
            )) => {
                error!(
                    "candidate {candidate:?} failed because {} is unavailable",
                    path.display()
                );
                self.publication_state.record_candidate_failure(candidate);
                exit.store(true, Ordering::Relaxed);
                return Err(TipRouterSnapshotServiceError::CandidateStoreUnavailable {
                    path,
                    source,
                });
            }
            WorkerOutcome::Failed(err) => {
                error!("tip-router snapshot candidate {candidate:?} failed: {err}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
            WorkerOutcome::Panicked => {
                error!("tip-router snapshot worker panicked for {candidate:?}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
            WorkerOutcome::MissingResult => {
                error!("tip-router snapshot worker returned no result for {candidate:?}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
        }
        Ok(())
    }

    fn rooted_candidate_failed(
        &self,
        candidate: CandidateIdentity,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        exit.store(true, Ordering::Relaxed);
        Err(TipRouterSnapshotServiceError::RootedCandidateFailed {
            epoch: candidate.epoch,
            slot: candidate.slot,
            bank_hash: candidate.bank_hash,
        })
    }

    fn try_publish_winner(&mut self, candidate_store: &CandidateStore) {
        if let Some(winner) = self.publication_state.winner_pending_publication() {
            match candidate_store.finalize_publication(winner) {
                Ok(PublicationOutcome::Published { path }) => {
                    info!(
                        "published tip-router snapshot winner {winner:?} to {}",
                        path.display()
                    );
                    self.publication_state.record_winner_published(winner);
                }
                Ok(PublicationOutcome::AlreadyPublished { path }) => {
                    warn!(
                        "tip-router snapshot epoch {} was already published at {}",
                        winner.epoch,
                        path.display()
                    );
                    self.publication_state.record_winner_published(winner);
                }
                Err(err) => {
                    warn!(
                        "failed to finalize tip-router snapshot winner {winner:?}; will retry on \
                         the next rooted-chain notification: {err}"
                    )
                }
            }
        };
    }

    pub(super) fn shutdown_workers(
        &mut self,
        completion_receiver: &Receiver<WorkerCompletion>,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        self.shutdown_workers_with_timeout(
            completion_receiver,
            exit,
            ARTIFACT_WORKERS_SHUTDOWN_TIMEOUT,
        )
    }

    fn shutdown_workers_with_timeout(
        &mut self,
        completion_receiver: &Receiver<WorkerCompletion>,
        exit: &Arc<AtomicBool>,
        timeout: Duration,
    ) -> TipRouterSnapshotServiceResult {
        let mut first_error = None;

        let (completions, shutdown_timeout) = match self
            .workers
            .shutdown_with_timeout(completion_receiver, timeout)
        {
            Ok(completions) => (completions, None),
            Err(WorkerShutdownTimeout {
                worker_count,
                timeout,
                completions,
            }) => (
                completions,
                Some(
                    TipRouterSnapshotServiceError::ArtifactWorkersShutdownTimeout {
                        worker_count,
                        timeout,
                    },
                ),
            ),
        };
        for completion in completions {
            if let Err(err) = self.record_worker_completion(completion, exit) {
                first_error.get_or_insert(err);
            }
        }

        if let Some(shutdown_timeout) = shutdown_timeout {
            exit.store(true, Ordering::Relaxed);
            return Err(shutdown_timeout);
        }

        first_error.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crossbeam_channel::unbounded, std::fs, tempfile::tempdir};

    fn candidate(epoch: Epoch, slot: Slot, hash: Hash) -> CandidateIdentity {
        CandidateIdentity {
            epoch,
            slot,
            bank_hash: hash,
        }
    }

    fn context() -> TipRouterSnapshotServiceContext {
        let (sender, _receiver) = unbounded();
        TipRouterSnapshotServiceContext::new(sender, None)
    }

    fn candidate_path(
        output_dir: &std::path::Path,
        candidate: CandidateIdentity,
    ) -> std::path::PathBuf {
        output_dir.join(format!(
            "tmp_{}_{}_{}_stake_meta_collection.json",
            candidate.slot, candidate.bank_hash, candidate.epoch
        ))
    }

    fn collect(context: &mut TipRouterSnapshotServiceContext, candidates: &[CandidateIdentity]) {
        let epoch = candidates.first().unwrap().epoch;
        assert!(candidates.iter().all(|candidate| candidate.epoch == epoch));
        for candidate in candidates {
            context
                .publication_state
                .record_spawned_candidate(*candidate);
        }
    }

    #[test]
    fn exact_root_publishes_immediately_and_purges_durable_candidates() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        let loser = candidate(7, 41, Hash::new_unique());
        let stale = candidate(6, 40, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        fs::write(candidate_path(output_dir.path(), winner), b"winner").unwrap();
        fs::write(candidate_path(output_dir.path(), loser), b"loser").unwrap();
        fs::write(candidate_path(output_dir.path(), stale), b"stale").unwrap();
        collect(&mut context, &[winner, loser]);

        context
            .handle_new_rooted_chain(vec![(winner.slot, winner.bank_hash)], &store)
            .unwrap();

        assert_eq!(
            fs::read(output_dir.path().join("7_stake_meta_collection.json")).unwrap(),
            b"winner"
        );
        assert_eq!(context.publication_state.winner_pending_publication(), None);
        assert_eq!(context.publication_state.latest_published_epoch(), Some(7));
        assert!(!candidate_path(output_dir.path(), loser).exists());
        assert!(!candidate_path(output_dir.path(), stale).exists());
    }

    #[test]
    fn same_slot_candidates_publish_only_the_rooted_hash() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        let loser = candidate(7, 42, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        fs::write(candidate_path(output_dir.path(), winner), b"winner").unwrap();
        fs::write(candidate_path(output_dir.path(), loser), b"loser").unwrap();
        collect(&mut context, &[winner, loser]);

        context
            .handle_new_rooted_chain(vec![(winner.slot, winner.bank_hash)], &store)
            .unwrap();

        assert_eq!(
            fs::read(output_dir.path().join("7_stake_meta_collection.json")).unwrap(),
            b"winner"
        );
        assert!(!candidate_path(output_dir.path(), loser).exists());
    }

    #[test]
    fn roots_are_not_retained_for_future_candidates() {
        let mut context = context();
        let candidate = candidate(7, 42, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        fs::write(candidate_path(output_dir.path(), candidate), b"candidate").unwrap();

        context
            .handle_new_rooted_chain(vec![(candidate.slot, candidate.bank_hash)], &store)
            .unwrap();
        collect(&mut context, &[candidate]);

        assert!(context.publication_state.tracked_candidates().is_some());
        assert!(candidate_path(output_dir.path(), candidate).exists());
        assert!(
            !output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );
    }

    #[test]
    fn newer_candidate_epoch_forgets_prior_candidates_but_leaves_files() {
        let mut context = context();
        let stale = candidate(6, 40, Hash::new_unique());
        let newer = candidate(7, 41, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let stale_path = candidate_path(output_dir.path(), stale);
        fs::write(&stale_path, b"stale").unwrap();
        collect(&mut context, &[stale]);

        context.publication_state.record_spawned_candidate(newer);

        assert_eq!(
            context.publication_state.tracked_candidates(),
            Some(&std::collections::HashSet::from([newer]))
        );
        assert!(stale_path.exists());
        context.publication_state.record_spawned_candidate(stale);
        assert_eq!(
            context.publication_state.tracked_candidates(),
            Some(&std::collections::HashSet::from([newer]))
        );
    }

    #[test]
    fn failed_publication_retries_on_the_next_rooted_chain() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        let blocking_loser = candidate(6, 40, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        fs::write(candidate_path(output_dir.path(), winner), b"winner").unwrap();
        fs::create_dir(candidate_path(output_dir.path(), blocking_loser)).unwrap();
        collect(&mut context, &[winner]);

        context
            .handle_new_rooted_chain(vec![(winner.slot, winner.bank_hash)], &store)
            .unwrap();

        assert_eq!(
            context.publication_state.winner_pending_publication(),
            Some(winner)
        );
        assert!(
            !output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );

        fs::remove_dir(candidate_path(output_dir.path(), blocking_loser)).unwrap();
        context.handle_new_rooted_chain(Vec::new(), &store).unwrap();

        assert_eq!(context.publication_state.winner_pending_publication(), None);
        assert!(
            output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );
    }

    #[test]
    fn worker_completion_does_not_publish_a_pending_winner() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let canonical_path = output_dir.path().join("7_stake_meta_collection.json");
        let winner_path = candidate_path(output_dir.path(), winner);
        let exit = Arc::new(AtomicBool::new(false));
        collect(&mut context, &[winner]);

        context
            .handle_new_rooted_chain(vec![(winner.slot, winner.bank_hash)], &store)
            .unwrap();
        assert_eq!(
            context.publication_state.winner_pending_publication(),
            Some(winner)
        );

        fs::write(&winner_path, b"winner").unwrap();
        context
            .record_worker_completion(
                WorkerCompletion {
                    candidate: winner,
                    outcome: WorkerOutcome::Written(winner_path),
                },
                &exit,
            )
            .unwrap();
        assert!(!canonical_path.exists());

        context.handle_new_rooted_chain(Vec::new(), &store).unwrap();
        assert!(canonical_path.exists());
    }

    #[test]
    fn rooted_candidate_worker_failure_stops_the_service() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        context.publication_state.record_spawned_candidate(winner);
        context
            .publication_state
            .select_winner_for_publication(&[(winner.slot, winner.bank_hash)]);
        let exit = Arc::new(AtomicBool::new(false));

        let result = context.record_worker_completion(
            WorkerCompletion {
                candidate: winner,
                outcome: WorkerOutcome::MissingResult,
            },
            &exit,
        );

        assert!(matches!(
            result,
            Err(TipRouterSnapshotServiceError::RootedCandidateFailed {
                epoch: 7,
                slot: 42,
                bank_hash,
            }) if bank_hash == winner.bank_hash
        ));
        assert!(exit.load(Ordering::Relaxed));
    }

    #[test]
    fn shutdown_uses_one_deadline_for_all_workers() {
        let (sender, receiver) = unbounded();
        let mut context = TipRouterSnapshotServiceContext::new(sender.clone(), None);
        for slot in 40..50 {
            let identity = candidate(7, slot, Hash::new_unique());
            context
                .workers
                .spawn_test_worker(identity, Duration::from_millis(200));
            context.publication_state.record_spawned_candidate(identity);
        }
        let exit = Arc::new(AtomicBool::new(false));
        let started = std::time::Instant::now();

        let result =
            context.shutdown_workers_with_timeout(&receiver, &exit, Duration::from_millis(20));

        assert!(matches!(
            result,
            Err(TipRouterSnapshotServiceError::ArtifactWorkersShutdownTimeout {
                worker_count: 10,
                timeout
            }) if timeout == Duration::from_millis(20)
        ));
        assert!(started.elapsed() < Duration::from_millis(150));
        assert!(exit.load(Ordering::Relaxed));
    }
}
