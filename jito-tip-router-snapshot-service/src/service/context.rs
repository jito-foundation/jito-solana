use {
    super::{
        TipRouterSnapshotServiceError, TipRouterSnapshotServiceResult,
        metrics::{report_publication_failure, report_spawn_failure, report_worker_outcome},
        publication_state::SnapshotPublicationTracker,
        worker_pool::{SnapshotWorkerPool, WorkerShutdownTimeout},
    },
    crate::{
        CandidateIdentity,
        artifact_store::{ArtifactStore, ArtifactStoreError, PublishError},
        config::TipRouterSnapshotConfig,
        snapshot_worker::{SnapshotWorkerError, WorkerCompletion, WorkerOutcome},
    },
    crossbeam_channel::{Receiver, Sender},
    log::{debug, error, info, warn},
    solana_clock::Slot,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationWithDependencyWork,
    },
    solana_runtime::bank::{Bank, BankId},
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
    /// State machine
    publication_state: SnapshotPublicationTracker,
    workers: SnapshotWorkerPool,
}

impl TipRouterSnapshotServiceContext {
    pub(super) fn new(completion_sender: Sender<WorkerCompletion>) -> Self {
        Self {
            publication_state: SnapshotPublicationTracker::new(),
            workers: SnapshotWorkerPool::new(completion_sender),
        }
    }

    pub(super) fn handle_bank_notification(
        &mut self,
        config: &TipRouterSnapshotConfig,
        artifact_store: &ArtifactStore,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
    ) -> TipRouterSnapshotServiceResult {
        match notification {
            BankNotification::Frozen(bank) => self.handle_frozen_bank(config, artifact_store, bank),
            BankNotification::NewRootedChain(rooted_chain, _) => {
                self.handle_new_rooted_chain(rooted_chain, artifact_store)
            }
            _ => Ok(()),
        }
    }

    fn handle_new_rooted_chain(
        &mut self,
        rooted_chain: Vec<(Slot, BankId)>,
        artifact_store: &ArtifactStore,
    ) -> TipRouterSnapshotServiceResult {
        // Returns a winner only once its artifact is written; a parent selected through a
        // rooted boundary child is published when its worker finishes if still unwritten.
        let Some(winner) = self
            .publication_state
            .select_winner_for_publication(&rooted_chain)
        else {
            return Ok(());
        };

        debug!("selected tip-router snapshot parent {winner} through a rooted boundary child");

        self.publish_winner(winner, artifact_store)
    }

    fn publish_winner(
        &mut self,
        winner: CandidateIdentity,
        artifact_store: &ArtifactStore,
    ) -> TipRouterSnapshotServiceResult {
        match artifact_store.publish_candidate(winner) {
            Ok(()) => {
                info!("published tip-router snapshot winner {winner}");
                self.publication_state.record_winner_published(winner);
            }
            Err(ArtifactStoreError::PublishError(PublishError::AlreadyPublished { path })) => {
                warn!(
                    "tip-router snapshot epoch {} was already published at {}",
                    winner.epoch,
                    path.display()
                );
                self.publication_state.record_winner_published(winner);
            }
            Err(err) => {
                warn!("failed to finalize tip-router snapshot winner {winner}: {err}");
                report_publication_failure(&err, winner);
                self.publication_state
                    .record_winner_publication_failure(winner);
            }
        }

        Ok(())
    }

    fn handle_frozen_bank(
        &mut self,
        config: &TipRouterSnapshotConfig,
        artifact_store: &ArtifactStore,
        boundary_child_bank: Arc<Bank>,
    ) -> TipRouterSnapshotServiceResult {
        let boundary_child = (boundary_child_bank.slot(), boundary_child_bank.bank_id());
        let Some((candidate, parent_bank)) = self
            .publication_state
            .eligible_candidate_from_boundary_child(boundary_child_bank)
        else {
            // 99.999% of slots are not epoch-boundary slots
            // (though our notification filter should technically not forward them on the sender
            // side)
            return Ok(());
        };

        if self
            .publication_state
            .record_boundary_child_for_existing_candidate(candidate, boundary_child)
        {
            return Ok(());
        }

        if !self.publication_state.can_spawn_candidate(candidate) {
            return Ok(());
        }

        info!(
            "generating tip-router snapshot candidate at slot={}, bank_id={}, epoch={}",
            candidate.slot,
            u64::from(candidate.bank_id),
            candidate.epoch,
        );

        if let Err(err) = self.workers.spawn(
            config.clone(),
            artifact_store.clone(),
            candidate,
            parent_bank,
        ) {
            error!("failed to spawn tip-router snapshot worker for {candidate}: {err}");
            report_spawn_failure(&err, candidate);
            return Ok(());
        }

        self.publication_state
            .record_spawned_candidate(candidate, boundary_child);
        Ok(())
    }

    pub(super) fn handle_worker_completion(
        &mut self,
        artifact_store: &ArtifactStore,
        worker_completion: WorkerCompletion,
    ) -> TipRouterSnapshotServiceResult {
        let Some(completion) = self.workers.complete_worker(worker_completion) else {
            // Weird case where the worker was already removed and this has been duplicated
            return Ok(());
        };
        self.record_worker_completion(artifact_store, completion)
    }

    fn record_worker_completion(
        &mut self,
        artifact_store: &ArtifactStore,
        completion: WorkerCompletion,
    ) -> TipRouterSnapshotServiceResult {
        let WorkerCompletion { candidate, outcome } = completion;

        report_worker_outcome(&outcome, candidate);

        match outcome {
            WorkerOutcome::Written(path) => {
                debug!(
                    "wrote tip-router snapshot candidate {candidate} to {}",
                    path.display()
                );
                // If one of this parent's boundary children already rooted, publication was
                // deferred until the artifact write finished; complete it now.
                if let Some(winner) = self.publication_state.record_candidate_written(candidate) {
                    return self.publish_winner(winner, artifact_store);
                }
            }
            WorkerOutcome::Failed(SnapshotWorkerError::ArtifactStore(
                ArtifactStoreError::DirectoryUnavailable { path, source },
            )) => {
                error!(
                    "candidate {candidate} failed because {} is unavailable",
                    path.display()
                );
                self.publication_state.record_candidate_failure(candidate);
                return Err(TipRouterSnapshotServiceError::ArtifactStoreUnavailable {
                    path,
                    source,
                });
            }
            WorkerOutcome::Failed(err) => {
                error!("tip-router snapshot candidate {candidate} failed: {err}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate);
                }
            }
            WorkerOutcome::Panicked => {
                error!("tip-router snapshot worker panicked for {candidate}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate);
                }
            }
            WorkerOutcome::MissingResult => {
                error!("tip-router snapshot worker returned no result for {candidate}");
                if self.publication_state.record_candidate_failure(candidate) {
                    return self.rooted_candidate_failed(candidate);
                }
            }
        }
        Ok(())
    }

    fn rooted_candidate_failed(
        &self,
        candidate: CandidateIdentity,
    ) -> TipRouterSnapshotServiceResult {
        Err(TipRouterSnapshotServiceError::RootedCandidateFailed {
            epoch: candidate.epoch,
            slot: candidate.slot,
            bank_id: candidate.bank_id,
        })
    }

    pub(super) fn shutdown_workers(
        &mut self,
        artifact_store: &ArtifactStore,
        completion_receiver: &Receiver<WorkerCompletion>,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        self.shutdown_workers_with_timeout(
            artifact_store,
            completion_receiver,
            exit,
            ARTIFACT_WORKERS_SHUTDOWN_TIMEOUT,
        )
    }

    fn shutdown_workers_with_timeout(
        &mut self,
        artifact_store: &ArtifactStore,
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
            if let Err(err) = self.record_worker_completion(artifact_store, completion) {
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
    use {
        super::TipRouterSnapshotServiceContext,
        crate::{artifact_store::ArtifactStore, config::TipRouterSnapshotConfig},
        crossbeam_channel::{RecvTimeoutError, unbounded},
        solana_epoch_schedule::EpochSchedule,
        solana_pubkey::Pubkey,
        solana_rpc::optimistically_confirmed_bank_tracker::BankNotification,
        solana_runtime::{
            bank::{Bank, SlotLeader},
            genesis_utils::create_genesis_config,
        },
        std::{sync::Arc, time::Duration},
    };

    #[test]
    fn sibling_boundary_child_does_not_spawn_second_worker() {
        let output_dir = tempfile::tempdir().unwrap();
        let config = TipRouterSnapshotConfig {
            output_dir: output_dir.path().to_path_buf(),
            tip_distribution_program_id: Pubkey::new_from_array([1; 32]),
            priority_fee_distribution_program_id: Pubkey::new_from_array([2; 32]),
            tip_payment_program_id: Pubkey::new_from_array([3; 32]),
        };
        let artifact_store = ArtifactStore::new(config.output_dir.clone()).unwrap();
        let (completion_sender, completion_receiver) = unbounded();
        let mut context = TipRouterSnapshotServiceContext::new(completion_sender);

        let mut genesis_config = create_genesis_config(1_000_000).genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::custom(32, 32, false);
        let (genesis_bank, _bank_forks) =
            Bank::new_for_tests(&genesis_config).wrap_with_bank_forks_for_tests();
        let parent_bank = Arc::new(Bank::new_from_parent(
            genesis_bank,
            SlotLeader::default(),
            31,
        ));
        for child_slot in [32, 33] {
            let child_bank = Arc::new(Bank::new_from_parent(
                parent_bank.clone(),
                SlotLeader::default(),
                child_slot,
            ));
            context
                .handle_bank_notification(
                    &config,
                    &artifact_store,
                    (BankNotification::Frozen(child_bank), None),
                )
                .unwrap();
        }

        let completion = completion_receiver
            .recv_timeout(Duration::from_secs(30))
            .unwrap();
        context
            .handle_worker_completion(&artifact_store, completion)
            .unwrap();
        assert!(matches!(
            completion_receiver.recv_timeout(Duration::from_secs(2)),
            Err(RecvTimeoutError::Timeout)
        ));
    }
}
