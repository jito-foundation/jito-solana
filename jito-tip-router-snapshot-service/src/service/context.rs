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
        let Some(winner) = self
            .publication_state
            .select_winner_for_publication(&rooted_chain)
        else {
            return Ok(());
        };

        info!("selected rooted tip-router snapshot candidate {winner:?}");

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
                warn!("failed to finalize tip-router snapshot winner {winner:?}: {err}");
                self.publication_state
                    .record_winner_publication_failure(winner);
            }
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
