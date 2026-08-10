//! Asynchronous stake-meta snapshot candidate generation.

use {
    crate::{
        candidate::CandidateIdentity,
        candidate_store::{CandidateStore, CandidateStoreError},
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

#[derive(Debug)]
pub(crate) enum SnapshotWorkerError {
    StakeMeta(StakeMetaError),
    CandidateStore(CandidateStoreError),
}

impl std::fmt::Display for SnapshotWorkerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StakeMeta(err) => write!(formatter, "failed to collect stake metadata: {err}"),
            Self::CandidateStore(err) => write!(formatter, "failed to persist candidate: {err}"),
        }
    }
}

impl std::error::Error for SnapshotWorkerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StakeMeta(err) => Some(err),
            Self::CandidateStore(err) => Some(err),
        }
    }
}

pub(crate) type SnapshotWorkerResult = Result<PathBuf, SnapshotWorkerError>;

pub(crate) enum WorkerReport {
    Completed {
        candidate: CandidateIdentity,
        result: SnapshotWorkerResult,
    },
    Panicked {
        candidate: CandidateIdentity,
    },
}

pub(crate) enum WorkerCompletion {
    Written {
        candidate: CandidateIdentity,
        path: PathBuf,
    },
    Failed {
        candidate: CandidateIdentity,
        err: SnapshotWorkerError,
    },
    Panicked {
        candidate: CandidateIdentity,
    },
    MissingResult {
        candidate: CandidateIdentity,
    },
}

pub(crate) struct SnapshotWorkerHandle {
    candidate: CandidateIdentity,
    handle: JoinHandle<()>,
}

impl SnapshotWorkerHandle {
    pub(crate) fn spawn(
        config: TipRouterSnapshotConfig,
        candidate_store: CandidateStore,
        candidate: CandidateIdentity,
        parent_bank: Arc<Bank>,
        completion_sender: Sender<WorkerReport>,
    ) -> io::Result<Self> {
        let handle = Builder::new()
            .name(format!(
                "tipRtSnapshot-{}-{}",
                candidate.epoch, candidate.slot
            ))
            .spawn(move || {
                let report = match catch_unwind(AssertUnwindSafe(|| {
                    generate_and_write_snapshot(&config, &candidate_store, candidate, parent_bank)
                })) {
                    Ok(result) => WorkerReport::Completed { candidate, result },
                    Err(_) => WorkerReport::Panicked { candidate },
                };
                let _ = completion_sender.send(report);
            })?;

        Ok(Self { candidate, handle })
    }

    #[cfg(test)]
    pub(crate) fn spawn_test_worker(
        candidate: CandidateIdentity,
        duration: std::time::Duration,
        completion_sender: Sender<WorkerReport>,
    ) -> Self {
        let handle = Builder::new()
            .spawn(move || {
                std::thread::sleep(duration);
                let _ = completion_sender.send(WorkerReport::Completed {
                    candidate,
                    result: Ok(PathBuf::new()),
                });
            })
            .unwrap();
        Self { candidate, handle }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.handle.is_finished()
    }

    pub(crate) fn join_after_report(self, report: WorkerReport) -> WorkerCompletion {
        let candidate = self.candidate;
        if self.handle.join().is_err() {
            return WorkerCompletion::Panicked { candidate };
        }
        match report {
            WorkerReport::Completed {
                candidate: reported_candidate,
                result: Ok(path),
            } if reported_candidate == candidate => WorkerCompletion::Written { candidate, path },
            WorkerReport::Completed {
                candidate: reported_candidate,
                result: Err(err),
            } if reported_candidate == candidate => WorkerCompletion::Failed { candidate, err },
            WorkerReport::Panicked {
                candidate: reported_candidate,
            } if reported_candidate == candidate => WorkerCompletion::Panicked { candidate },
            _ => WorkerCompletion::MissingResult { candidate },
        }
    }

    pub(crate) fn join_without_report(self) -> WorkerCompletion {
        let candidate = self.candidate;
        match self.handle.join() {
            Ok(()) => WorkerCompletion::MissingResult { candidate },
            Err(_) => WorkerCompletion::Panicked { candidate },
        }
    }
}

fn generate_and_write_snapshot(
    config: &TipRouterSnapshotConfig,
    candidate_store: &CandidateStore,
    candidate: CandidateIdentity,
    parent_bank: Arc<Bank>,
) -> SnapshotWorkerResult {
    // StakeMetaCapture pins the AccountsDB state needed by direct indexed reads before the worker
    // releases its Arc<Bank>.
    let stake_meta_capture =
        StakeMetaCapture::new(parent_bank).map_err(SnapshotWorkerError::StakeMeta)?;
    let stake_meta = stake_meta::collect_stake_meta(config, stake_meta_capture)
        .map_err(SnapshotWorkerError::StakeMeta)?;

    candidate_store
        .write_candidate(candidate, &stake_meta)
        .map_err(SnapshotWorkerError::CandidateStore)
}
