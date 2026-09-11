//! Asynchronous stake-meta snapshot candidate generation.

use {
    crate::{
        CandidateIdentity,
        artifact_store::{ArtifactStore, ArtifactStoreError},
        config::TipRouterSnapshotConfig,
        stake_meta::{self, StakeMetaCapture, StakeMetaError},
    },
    crossbeam_channel::Sender,
    solana_runtime::bank::Bank,
    std::{
        io,
        panic::{AssertUnwindSafe, catch_unwind},
        path::PathBuf,
        sync::Arc,
        thread::{Builder, JoinHandle},
    },
};

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotWorkerError {
    #[error("stake metadata collection failed: {0}")]
    StakeMeta(#[from] StakeMetaError),
    #[error("artifact store failed: {0}")]
    ArtifactStore(#[from] ArtifactStoreError),
}

pub(crate) type SnapshotWorkerResult = Result<PathBuf, SnapshotWorkerError>;

pub(crate) struct WorkerCompletion {
    pub(crate) candidate: CandidateIdentity,
    pub(crate) outcome: WorkerOutcome,
}

pub(crate) enum WorkerOutcome {
    Written(PathBuf),
    Failed(SnapshotWorkerError),
    Panicked,
    MissingResult,
}

pub(crate) struct SnapshotWorkerHandle {
    candidate: CandidateIdentity,
    handle: JoinHandle<()>,
}

impl SnapshotWorkerHandle {
    pub(crate) fn spawn(
        config: TipRouterSnapshotConfig,
        artifact_store: ArtifactStore,
        candidate: CandidateIdentity,
        parent_bank: Arc<Bank>,
        completion_sender: Sender<WorkerCompletion>,
    ) -> io::Result<Self> {
        let handle = Builder::new()
            .name(format!(
                "tipRtSnapshot-{}-{}",
                candidate.epoch, candidate.slot
            ))
            .spawn(move || {
                let outcome = match catch_unwind(AssertUnwindSafe(|| {
                    generate_and_write_snapshot(&config, &artifact_store, candidate, parent_bank)
                })) {
                    Ok(Ok(path)) => WorkerOutcome::Written(path),
                    Ok(Err(err)) => WorkerOutcome::Failed(err),
                    Err(_) => WorkerOutcome::Panicked,
                };
                let _ = completion_sender.send(WorkerCompletion { candidate, outcome });
            })?;

        Ok(Self { candidate, handle })
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub(crate) fn join_after_completion(self, completion: WorkerCompletion) -> WorkerCompletion {
        let candidate = self.candidate;
        if self.handle.join().is_err() {
            return WorkerCompletion {
                candidate,
                outcome: WorkerOutcome::Panicked,
            };
        }
        completion
    }

    pub(crate) fn join_without_report(self) -> WorkerCompletion {
        let candidate = self.candidate;
        let outcome = match self.handle.join() {
            Ok(()) => WorkerOutcome::MissingResult,
            Err(_) => WorkerOutcome::Panicked,
        };
        WorkerCompletion { candidate, outcome }
    }
}

fn generate_and_write_snapshot(
    config: &TipRouterSnapshotConfig,
    artifact_store: &ArtifactStore,
    candidate: CandidateIdentity,
    parent_bank: Arc<Bank>,
) -> SnapshotWorkerResult {
    // StakeMetaCapture pins the AccountsDB state needed by direct indexed reads before the worker
    // releases its Arc<Bank>.
    let stake_meta_capture = StakeMetaCapture::new(parent_bank)?;

    let stake_meta = stake_meta::collect_stake_meta(config, stake_meta_capture)?;

    Ok(artifact_store.write_candidate(candidate, &stake_meta)?)
}
