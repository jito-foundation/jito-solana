//! Tip-router snapshot artifact generation and disk persistence.

mod writer;
pub(crate) use writer::{ArtifactDirectoryError, SnapshotArtifactWriter};
use {
    crate::{
        candidate::CandidateIdentity,
        config::TipRouterSnapshotConfig,
        stake_meta::{self, StakeMetaCapture, StakeMetaError},
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, bounded},
    solana_runtime::bank::Bank,
    std::{
        io,
        path::PathBuf,
        sync::Arc,
        thread::{Builder, JoinHandle},
        time::Duration,
    },
};

#[derive(Debug)]
pub(crate) enum SnapshotArtifactError {
    StakeMeta(StakeMetaError),
    DirectoryUnavailable { path: PathBuf, source: io::Error },
    Io(io::Error),
}

impl std::fmt::Display for SnapshotArtifactError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StakeMeta(err) => write!(formatter, "failed to collect stake metadata: {err}"),
            Self::DirectoryUnavailable { path, source } => write!(
                formatter,
                "artifact directory {} is unavailable: {source}",
                path.display()
            ),
            Self::Io(err) => write!(formatter, "artifact I/O failed: {err}"),
        }
    }
}

impl std::error::Error for SnapshotArtifactError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::StakeMeta(err) => Some(err),
            Self::DirectoryUnavailable { source, .. } | Self::Io(source) => Some(source),
        }
    }
}

pub(crate) type ArtifactResult = Result<PathBuf, SnapshotArtifactError>;
pub(crate) type WorkerResult = Result<ArtifactResult, crossbeam_channel::RecvError>;

pub(crate) enum WorkerCompletion {
    Written {
        candidate: CandidateIdentity,
        temp_path: PathBuf,
    },
    Failed {
        candidate: CandidateIdentity,
        err: SnapshotArtifactError,
    },
    Panicked {
        candidate: CandidateIdentity,
    },
    MissingResult {
        candidate: CandidateIdentity,
    },
    TimedOut {
        candidate: CandidateIdentity,
        timeout: Duration,
    },
}

pub(crate) struct SnapshotArtifactWorkerHandle {
    candidate: CandidateIdentity,
    result_receiver: Receiver<ArtifactResult>,
    handle: JoinHandle<()>,
}

impl SnapshotArtifactWorkerHandle {
    /// Spawns a joinable artifact worker.
    ///
    /// An error means the worker thread was not created. Collection and write
    /// failures are reported asynchronously through the completion receiver.
    pub(crate) fn spawn(
        config: TipRouterSnapshotConfig,
        writer: SnapshotArtifactWriter,
        candidate: CandidateIdentity,
        parent_bank: Arc<Bank>,
    ) -> io::Result<Self> {
        let (result_sender, result_receiver) = bounded(1);
        let handle = Builder::new()
            .name(format!("tipRtSnapshot-{}", candidate.epoch))
            .spawn(move || {
                let outcome = generate_and_write_snapshot(&config, &writer, candidate, parent_bank);
                let _ = result_sender.send(outcome);
            })?;

        Ok(Self {
            candidate,
            result_receiver,
            handle,
        })
    }

    pub(crate) fn candidate(&self) -> CandidateIdentity {
        self.candidate
    }

    pub(crate) fn join_and_classify(self, received_result: WorkerResult) -> WorkerCompletion {
        let candidate = self.candidate;
        match (received_result, self.handle.join()) {
            (Ok(Ok(temp_path)), Ok(())) => WorkerCompletion::Written {
                candidate,
                temp_path,
            },
            (Ok(Err(err)), Ok(())) => WorkerCompletion::Failed { candidate, err },
            (_, Err(_)) => WorkerCompletion::Panicked { candidate },
            (Err(_), Ok(())) => WorkerCompletion::MissingResult { candidate },
        }
    }

    /// Waits up to `timeout` from this call for the artifact result.
    ///
    /// Consumes and detaches the worker handle if the timeout elapses.
    pub(crate) fn wait_for_completion_or_timeout(self, timeout: Duration) -> WorkerCompletion {
        match self.result_receiver.recv_timeout(timeout) {
            Ok(artifact_result) => self.join_and_classify(Ok(artifact_result)),
            Err(RecvTimeoutError::Disconnected) => {
                self.join_and_classify(Err(crossbeam_channel::RecvError))
            }
            Err(RecvTimeoutError::Timeout) => WorkerCompletion::TimedOut {
                candidate: self.candidate,
                timeout,
            },
        }
    }

    pub(crate) fn artifact_result_receiver(&self) -> Receiver<ArtifactResult> {
        self.result_receiver.clone()
    }
}

fn generate_and_write_snapshot(
    config: &TipRouterSnapshotConfig,
    writer: &SnapshotArtifactWriter,
    candidate: CandidateIdentity,
    parent_bank: Arc<Bank>,
) -> ArtifactResult {
    // Phase 5 must establish cleanup protection for any direct AccountsDB reads
    // performed by stake-meta extraction. Retaining this Arc<Bank> alone is not that pin.
    let stake_meta_capture =
        StakeMetaCapture::new(parent_bank).map_err(SnapshotArtifactError::StakeMeta)?;
    let stake_meta = stake_meta::collect_stake_meta(config, stake_meta_capture)
        .map_err(SnapshotArtifactError::StakeMeta)?;

    writer.write_temp(
        candidate.epoch,
        candidate.slot,
        candidate.bank_hash,
        &stake_meta,
    )
}
