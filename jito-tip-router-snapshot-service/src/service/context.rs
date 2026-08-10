use {
    super::{
        TipRouterSnapshotServiceError, TipRouterSnapshotServiceResult,
        rooted_chain::{ConflictingRootedBankHashes, RootedChain},
        worker_pool::{SnapshotWorkerPool, WorkerShutdownTimeout},
    },
    crate::{
        candidate::CandidateIdentity,
        candidate_store::{CandidateStore, CandidateStoreError},
        config::TipRouterSnapshotConfig,
        snapshot_worker::{SnapshotWorkerError, WorkerCompletion, WorkerReport},
    },
    crossbeam_channel::{Receiver, Sender},
    log::{debug, error, info, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationWithDependencyWork,
    },
    solana_runtime::bank::Bank,
    std::{
        collections::{HashMap, HashSet},
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    },
};

const ARTIFACT_WORKERS_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug)]
enum CandidateProgress {
    Generating,
    Written(PathBuf),
    Terminal,
}

#[derive(Debug)]
struct CandidateRecord {
    progress: CandidateProgress,
    losing: bool,
}

pub(super) struct TipRouterSnapshotServiceContext {
    candidates: HashMap<CandidateIdentity, CandidateRecord>,
    workers: SnapshotWorkerPool,
    rooted_chain: RootedChain,
    winner: Option<CandidateIdentity>,
    //TODO: Single one
    published_epochs: HashSet<Epoch>,
}

impl TipRouterSnapshotServiceContext {
    pub(super) fn new(
        completion_sender: Sender<WorkerReport>,
        published_epochs: HashSet<Epoch>,
    ) -> Self {
        Self {
            candidates: HashMap::new(),
            workers: SnapshotWorkerPool::new(completion_sender),
            rooted_chain: RootedChain::default(),
            winner: None,
            published_epochs,
        }
    }
}

impl TipRouterSnapshotServiceContext {
    pub(super) fn handle_bank_notification(
        &mut self,
        config: &TipRouterSnapshotConfig,
        candidate_store: &CandidateStore,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        match notification {
            BankNotification::Frozen(bank) => {
                self.handle_frozen_bank(config, candidate_store, bank)?;
                Ok(())
            }
            BankNotification::NewRootedChain(rooted_chain) => {
                self.handle_new_rooted_chain(rooted_chain, candidate_store, exit)
            }
            BankNotification::OptimisticallyConfirmed(_) | BankNotification::NewRootBank(_) => {
                Ok(())
            }
        }
    }

    fn handle_new_rooted_chain(
        &mut self,
        rooted_chain: Vec<(Slot, Hash)>,
        candidate_store: &CandidateStore,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        if let Err(ConflictingRootedBankHashes {
            slot,
            existing_hash,
            new_hash,
        }) = self.rooted_chain.record(rooted_chain)
        {
            exit.store(true, Ordering::Relaxed);
            return Err(TipRouterSnapshotServiceError::ConflictingRootedBankHashes {
                slot,
                existing_hash,
                new_hash,
            });
        }
        self.reconcile_rooted_candidates(candidate_store)
    }

    fn reconcile_rooted_candidates(
        &mut self,
        candidate_store: &CandidateStore,
    ) -> TipRouterSnapshotServiceResult {
        if self.winner.is_some() {
            return self.maintenance(candidate_store);
        }

        let rooted_winner = self
            .candidates
            .keys()
            .copied()
            .filter(|candidate| self.rooted_chain.contains(*candidate))
            .max_by_key(|candidate| (candidate.epoch, candidate.slot));
        if let Some(winner) = rooted_winner {
            self.select_winner(winner);
            self.maintenance(candidate_store)
        } else {
            Ok(())
        }
    }

    fn handle_frozen_bank(
        &mut self,
        config: &TipRouterSnapshotConfig,
        candidate_store: &CandidateStore,
        boundary_child_bank: Arc<Bank>,
    ) -> TipRouterSnapshotServiceResult {
        //TODO: Add a error variant here
        let Some(parent_bank) = boundary_child_bank.parent() else {
            error!("frozen epoch-boundary bank has no parent");
            return Ok(());
        };

        //TODO: This is technically redundant since the notif filter handles it at the Sender level
        if boundary_child_bank.epoch() <= parent_bank.epoch() {
            return Ok(());
        }

        let candidate = CandidateIdentity::from_bank(&parent_bank);
        if self.winner.is_some()
            || self.candidates.contains_key(&candidate)
            || self
                .published_epochs
                .iter()
                .any(|published_epoch| candidate.epoch <= *published_epoch)
        {
            return Ok(());
        }
        if self
            .rooted_chain
            .hash_at(candidate.slot)
            .is_some_and(|rooted_hash| rooted_hash != candidate.bank_hash)
        {
            debug!("rejecting non-rooted tip-router snapshot candidate {candidate:?}");
            return Ok(());
        }

        let oldest_candidate_epoch = self
            .candidates
            .keys()
            .map(|candidate| candidate.epoch)
            .min()
            .map_or(candidate.epoch, |epoch| epoch.min(candidate.epoch));
        let first_retained_slot = parent_bank
            .epoch_schedule()
            .get_first_slot_in_epoch(oldest_candidate_epoch);
        self.rooted_chain.prune_before(first_retained_slot);

        debug!(
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
        self.candidates.insert(
            candidate,
            CandidateRecord {
                progress: CandidateProgress::Generating,
                losing: false,
            },
        );
        self.reconcile_rooted_candidates(candidate_store)
    }

    fn select_winner(&mut self, winner: CandidateIdentity) {
        info!("selected rooted tip-router snapshot candidate {winner:?}");
        self.winner = Some(winner);
        for (candidate, record) in &mut self.candidates {
            record.losing = *candidate != winner;
        }
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
                if let Some(record) = self.candidates.get_mut(&candidate) {
                    record.progress = CandidateProgress::Written(path);
                } else if let Err(err) = candidate_store.delete_candidate(candidate) {
                    warn!("failed to delete untracked candidate {candidate:?}: {err}");
                }
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
                if let Some(record) = self.candidates.get_mut(&candidate) {
                    record.progress = CandidateProgress::Terminal;
                }
                exit.store(true, Ordering::Relaxed);
                return Err(TipRouterSnapshotServiceError::CandidateStoreUnavailable {
                    path,
                    source,
                });
            }
            WorkerCompletion::Failed { candidate, err } => {
                error!("tip-router snapshot candidate {candidate:?} failed: {err}");
                if let Some(record) = self.candidates.get_mut(&candidate) {
                    record.progress = CandidateProgress::Terminal;
                }
            }
            WorkerCompletion::Panicked { candidate } => {
                error!("tip-router snapshot worker panicked for {candidate:?}");
                if let Some(record) = self.candidates.get_mut(&candidate) {
                    record.progress = CandidateProgress::Terminal;
                }
            }
            WorkerCompletion::MissingResult { candidate } => {
                error!("tip-router snapshot worker returned no result for {candidate:?}");
                if let Some(record) = self.candidates.get_mut(&candidate) {
                    record.progress = CandidateProgress::Terminal;
                }
            }
        }
        self.maintenance(candidate_store)
    }

    pub(super) fn maintenance(
        &mut self,
        candidate_store: &CandidateStore,
    ) -> TipRouterSnapshotServiceResult {
        self.cleanup_written_losers(candidate_store);
        self.try_publish_winner(candidate_store);
        Ok(())
    }

    fn cleanup_written_losers(&mut self, candidate_store: &CandidateStore) {
        let written_losers = self
            .candidates
            .iter()
            .filter_map(|(candidate, record)| {
                if record.losing
                    && let CandidateProgress::Written(path) = &record.progress
                {
                    Some((*candidate, path.clone()))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for (candidate, path) in written_losers {
            match candidate_store.delete_candidate(candidate) {
                Ok(()) => {
                    if let Some(record) = self.candidates.get_mut(&candidate) {
                        record.progress = CandidateProgress::Terminal;
                    }
                }
                Err(err) => warn!(
                    "failed to delete losing candidate {candidate:?} at {}: {err}",
                    path.display()
                ),
            }
        }
    }

    fn try_publish_winner(&mut self, candidate_store: &CandidateStore) {
        let Some(winner) = self.winner else {
            return;
        };
        let winner_is_written = self.candidates.get(&winner).is_some_and(|record| {
            !record.losing && matches!(record.progress, CandidateProgress::Written(_))
        });
        let losers_are_terminal = self.candidates.iter().all(|(candidate, record)| {
            *candidate == winner || matches!(record.progress, CandidateProgress::Terminal)
        });
        if !winner_is_written || !losers_are_terminal {
            return;
        }

        match candidate_store.publish_winner(winner) {
            Ok(path) => {
                info!(
                    "published tip-router snapshot winner {winner:?} to {}",
                    path.display()
                );
                self.finish_publication(winner);
            }
            Err(CandidateStoreError::AlreadyPublished { path, .. }) => {
                warn!(
                    "tip-router snapshot epoch {} was already published at {}",
                    winner.epoch,
                    path.display()
                );
                let _ = candidate_store.delete_candidate(winner);
                self.finish_publication(winner);
            }
            Err(err) => {
                warn!("failed to clean candidates or publish winner {winner:?}; will retry: {err}")
            }
        }
    }

    fn finish_publication(&mut self, winner: CandidateIdentity) {
        debug_assert!(self.workers.is_empty());
        self.published_epochs.insert(winner.epoch);
        self.candidates.clear();
        self.winner = None;
        self.rooted_chain.clear();
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
        TipRouterSnapshotServiceContext::new(sender, HashSet::new())
    }

    fn candidate_path(output_dir: &std::path::Path, candidate: CandidateIdentity) -> PathBuf {
        output_dir.join(format!(
            "tmp_{}_{}_{}_stake_meta_collection.json",
            candidate.slot, candidate.bank_hash, candidate.epoch
        ))
    }

    #[test]
    fn root_before_candidate_selects_exact_identity() {
        let mut context = context();
        let rooted_hash = Hash::new_unique();
        let winner = candidate(1, 10, rooted_hash);
        context
            .rooted_chain
            .record(vec![(10, rooted_hash)])
            .unwrap();
        context.candidates.insert(
            winner,
            CandidateRecord {
                progress: CandidateProgress::Generating,
                losing: false,
            },
        );
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        context.reconcile_rooted_candidates(&store).unwrap();

        assert_eq!(context.winner, Some(winner));
    }

    #[test]
    fn winner_waits_for_running_loser_then_purges_every_candidate() {
        let mut context = context();
        let winner = candidate(7, 42, Hash::new_unique());
        let loser = candidate(7, 41, Hash::new_unique());
        let stale = candidate(6, 40, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        fs::write(candidate_path(output_dir.path(), winner), b"winner").unwrap();
        fs::write(candidate_path(output_dir.path(), loser), b"loser").unwrap();
        fs::write(candidate_path(output_dir.path(), stale), b"stale").unwrap();
        context.candidates.insert(
            winner,
            CandidateRecord {
                progress: CandidateProgress::Written(candidate_path(output_dir.path(), winner)),
                losing: false,
            },
        );
        context.candidates.insert(
            loser,
            CandidateRecord {
                progress: CandidateProgress::Generating,
                losing: false,
            },
        );
        context
            .rooted_chain
            .record(vec![(winner.slot, winner.bank_hash)])
            .unwrap();
        context.reconcile_rooted_candidates(&store).unwrap();
        assert!(
            !output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );

        context.candidates.get_mut(&loser).unwrap().progress =
            CandidateProgress::Written(candidate_path(output_dir.path(), loser));
        context.maintenance(&store).unwrap();

        assert!(
            output_dir
                .path()
                .join("7_stake_meta_collection.json")
                .exists()
        );
        assert!(context.candidates.is_empty());
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
        for identity in [winner, loser] {
            context.candidates.insert(
                identity,
                CandidateRecord {
                    progress: CandidateProgress::Written(candidate_path(
                        output_dir.path(),
                        identity,
                    )),
                    losing: false,
                },
            );
        }
        context
            .rooted_chain
            .record(vec![(winner.slot, winner.bank_hash)])
            .unwrap();
        context.reconcile_rooted_candidates(&store).unwrap();

        assert_eq!(
            fs::read(output_dir.path().join("7_stake_meta_collection.json")).unwrap(),
            b"winner"
        );
        assert!(!candidate_path(output_dir.path(), loser).exists());
    }

    #[test]
    fn newest_exact_root_wins_when_stale_epochs_remain() {
        let mut context = context();
        let stale = candidate(6, 40, Hash::new_unique());
        let winner = candidate(7, 42, Hash::new_unique());
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        for identity in [stale, winner] {
            let path = candidate_path(output_dir.path(), identity);
            fs::write(&path, identity.epoch.to_string()).unwrap();
            context.candidates.insert(
                identity,
                CandidateRecord {
                    progress: CandidateProgress::Written(path),
                    losing: false,
                },
            );
        }
        context
            .rooted_chain
            .record(vec![
                (stale.slot, stale.bank_hash),
                (winner.slot, winner.bank_hash),
            ])
            .unwrap();

        context.reconcile_rooted_candidates(&store).unwrap();

        assert_eq!(
            fs::read_to_string(output_dir.path().join("7_stake_meta_collection.json")).unwrap(),
            "7"
        );
        assert!(
            !output_dir
                .path()
                .join("6_stake_meta_collection.json")
                .exists()
        );
    }

    #[test]
    fn shutdown_uses_one_deadline_for_all_workers() {
        let (sender, receiver) = unbounded();
        let mut context = TipRouterSnapshotServiceContext::new(sender.clone(), HashSet::new());
        for slot in 40..50 {
            let identity = candidate(7, slot, Hash::new_unique());
            context.candidates.insert(
                identity,
                CandidateRecord {
                    progress: CandidateProgress::Generating,
                    losing: false,
                },
            );
            context
                .workers
                .spawn_test_worker(identity, Duration::from_millis(200));
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
