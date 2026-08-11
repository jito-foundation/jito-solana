use {
    super::{
        TipRouterSnapshotServiceError, TipRouterSnapshotServiceResult,
        publication_state::SnapshotPublicationState,
        worker_pool::{SnapshotWorkerPool, WorkerShutdownTimeout},
    },
    crate::{
        candidate::CandidateIdentity,
        candidate_store::{CandidateStore, CandidateStoreError},
        config::TipRouterSnapshotConfig,
        snapshot_worker::{SnapshotWorkerError, WorkerCompletion, WorkerReport},
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
    publication_state: SnapshotPublicationState,
    workers: SnapshotWorkerPool,
}

impl TipRouterSnapshotServiceContext {
    pub(super) fn new(
        completion_sender: Sender<WorkerReport>,
        latest_published_epoch: Option<Epoch>,
    ) -> Self {
        Self {
            publication_state: SnapshotPublicationState::new(latest_published_epoch),
            workers: SnapshotWorkerPool::new(completion_sender),
        }
    }
}

impl TipRouterSnapshotServiceContext {
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
        if let Some(winner) = self.publication_state.select_rooted_winner(&rooted_chain) {
            info!("selected rooted tip-router snapshot candidate {winner:?}");
            self.try_publish_winner(candidate_store);
        }
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
            .candidate_from_boundary_bank(boundary_child_bank)
        else {
            // 99.999% of slots are not epoch-boundary slots
            return Ok(());
        };

        if !self.publication_state.allows_candidate(candidate) {
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

        self.publication_state.record_candidate(candidate);
        Ok(())
    }

    pub(super) fn handle_worker_report(
        &mut self,
        report: WorkerReport,
        candidate_store: &CandidateStore,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let Some(completion) = self.workers.complete_report(report) else {
            return Ok(());
        };
        self.record_worker_completion(completion, candidate_store, exit)
    }

    fn record_worker_completion(
        &mut self,
        completion: WorkerCompletion,
        candidate_store: &CandidateStore,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        match completion {
            WorkerCompletion::Written { candidate, path } => {
                info!(
                    "wrote tip-router snapshot candidate {candidate:?} to {}",
                    path.display()
                );
            }
            WorkerCompletion::Failed {
                candidate,
                err:
                    SnapshotWorkerError::CandidateStore(CandidateStoreError::DirectoryUnavailable {
                        path,
                        source,
                    }),
            } => {
                error!(
                    "candidate {candidate:?} failed because {} is unavailable",
                    path.display()
                );
                self.publication_state.discard_failed_candidate(candidate);
                exit.store(true, Ordering::Relaxed);
                return Err(TipRouterSnapshotServiceError::CandidateStoreUnavailable {
                    path,
                    source,
                });
            }
            WorkerCompletion::Failed { candidate, err } => {
                error!("tip-router snapshot candidate {candidate:?} failed: {err}");
                if self.publication_state.discard_failed_candidate(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
            WorkerCompletion::Panicked { candidate } => {
                error!("tip-router snapshot worker panicked for {candidate:?}");
                if self.publication_state.discard_failed_candidate(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
            WorkerCompletion::MissingResult { candidate } => {
                error!("tip-router snapshot worker returned no result for {candidate:?}");
                if self.publication_state.discard_failed_candidate(candidate) {
                    return self.rooted_candidate_failed(candidate, exit);
                }
            }
        }
        self.maintenance(candidate_store)
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

    pub(super) fn maintenance(
        &mut self,
        candidate_store: &CandidateStore,
    ) -> TipRouterSnapshotServiceResult {
        self.try_publish_winner(candidate_store);
        Ok(())
    }

    fn try_publish_winner(&mut self, candidate_store: &CandidateStore) {
        if let Some(winner) = self.publication_state.publishing_winner() {
            match candidate_store.publish_winner(winner) {
                Ok(path) => {
                    info!(
                        "published tip-router snapshot winner {winner:?} to {}",
                        path.display()
                    );
                    self.publication_state.finish_publication(winner);
                }
                Err(CandidateStoreError::AlreadyPublished { path, .. }) => {
                    warn!(
                        "tip-router snapshot epoch {} was already published at {}",
                        winner.epoch,
                        path.display()
                    );
                    let _ = candidate_store.delete_candidate(winner);
                    self.publication_state.finish_publication(winner);
                }
                Err(err) => {
                    warn!(
                        "failed to clean candidates or publish winner {winner:?}; will retry: \
                         {err}"
                    )
                }
            }
        };
    }

    pub(super) fn shutdown_workers(
        &mut self,
        completion_receiver: &Receiver<WorkerReport>,
        candidate_store: &CandidateStore,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        self.shutdown_workers_with_timeout(
            completion_receiver,
            candidate_store,
            exit,
            ARTIFACT_WORKERS_SHUTDOWN_TIMEOUT,
        )
    }

    fn shutdown_workers_with_timeout(
        &mut self,
        completion_receiver: &Receiver<WorkerReport>,
        candidate_store: &CandidateStore,
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
            if let Err(err) = self.record_worker_completion(completion, candidate_store, exit) {
                first_error.get_or_insert(err);
            }
        }

        if let Some(shutdown_timeout) = shutdown_timeout {
            exit.store(true, Ordering::Relaxed);
            return Err(shutdown_timeout);
        }

        self.maintenance(candidate_store)?;
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
            context.publication_state.record_candidate(*candidate);
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
        assert_eq!(context.publication_state.publishing_winner(), None);
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
        context.maintenance(&store).unwrap();

        assert!(context.publication_state.active_candidates().is_some());
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

        context.publication_state.record_candidate(newer);

        assert_eq!(
            context.publication_state.active_candidates(),
            Some(&std::collections::HashSet::from([newer]))
        );
        assert!(stale_path.exists());
        context.publication_state.record_candidate(stale);
        assert_eq!(
            context.publication_state.active_candidates(),
            Some(&std::collections::HashSet::from([newer]))
        );
    }

    #[test]
    fn failed_publication_retains_only_the_winner_for_retry() {
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

        assert_eq!(context.publication_state.publishing_winner(), Some(winner));
        assert!(
            !output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );

        fs::remove_dir(candidate_path(output_dir.path(), blocking_loser)).unwrap();
        context.maintenance(&store).unwrap();

        assert_eq!(context.publication_state.publishing_winner(), None);
        assert!(
            output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );
    }

    #[test]
    fn rooted_candidate_worker_failure_stops_the_service() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        context.publication_state.record_candidate(winner);
        context
            .publication_state
            .select_rooted_winner(&[(winner.slot, winner.bank_hash)]);
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let exit = Arc::new(AtomicBool::new(false));

        let result = context.record_worker_completion(
            WorkerCompletion::MissingResult { candidate: winner },
            &store,
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
            context.publication_state.record_candidate(identity);
        }
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let exit = Arc::new(AtomicBool::new(false));
        let started = std::time::Instant::now();

        let result = context.shutdown_workers_with_timeout(
            &receiver,
            &store,
            &exit,
            Duration::from_millis(20),
        );

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
