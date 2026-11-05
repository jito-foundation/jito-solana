//! Wrapper around the artifact path
//! Responsible for creating and removing candidates and publishing the winner to the artifact path

use {
    crate::CandidateIdentity,
    jito_stake_meta_types::StakeMetaCollection,
    solana_clock::{BankId, Epoch},
    std::{
        ffi::OsStr,
        fs::{self, OpenOptions},
        io::{self, BufWriter, Write},
        path::{Path, PathBuf},
    },
};

const ARTIFACT_SUFFIX: &str = "_stake_meta_collection.json";
const CANDIDATES_DIRECTORY_NAME: &str = "candidates";

#[derive(Clone, Debug)]
pub(crate) struct ArtifactStore {
    /// Top-level artifact path
    /// Candidates are stored in artifact_path/candidates
    artifact_path: PathBuf,
}

impl ArtifactStore {
    pub(crate) fn new(output_dir: PathBuf) -> Result<Self, ArtifactStoreError> {
        let candidates_dir = candidate_directory(&output_dir);
        ensure_output_directory(&candidates_dir).map_err(|source| {
            ArtifactStoreError::DirectoryUnavailable {
                path: candidates_dir.clone(),
                source,
            }
        })?;
        Ok(Self {
            artifact_path: output_dir,
        })
    }

    pub(crate) fn write_candidate(
        &self,
        candidate: CandidateIdentity,
        stake_meta: &StakeMetaCollection,
    ) -> Result<PathBuf, ArtifactStoreError> {
        let candidate_path = self.candidate_path(candidate);

        // If file already exists, it errors
        // If parent dir does not exist, it errors
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate_path)?;

        let mut writer = BufWriter::new(&file);
        serde_json::to_writer_pretty(&mut writer, stake_meta)?;

        writer.flush()?;

        Ok(candidate_path)
    }

    /// Takes a tmp file, removes the forks from the epoch, and moves the winner to the toplevel
    /// artifact_dir
    pub(crate) fn publish_candidate(
        &self,
        winner: CandidateIdentity,
    ) -> Result<(), ArtifactStoreError> {
        self.move_winner(winner)?;
        self.remove_candidates_for_epoch(winner.epoch)
    }

    /// Publishes `winner` without allowing an existing canonical artifact to be replaced.
    ///
    /// A hard link makes the canonical name visible atomically while retaining the candidate
    /// file for later cleanup.
    fn move_winner(&self, winner: CandidateIdentity) -> Result<(), PublishError> {
        let winner_path = self.candidate_path(winner);
        let publish_path = self.canonical_path(winner.epoch);

        // The canonical name is the publication signal for downstream watchers.
        match fs::hard_link(&winner_path, &publish_path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => {
                Err(PublishError::CandidateNotFound { path: winner_path })
            }
            Err(source)
                if source.kind() == io::ErrorKind::AlreadyExists && publish_path.is_file() =>
            {
                Err(PublishError::AlreadyPublished { path: publish_path })
            }
            Err(source) => Err(PublishError::PublishFailed {
                candidate_path: winner_path,
                publication_path: publish_path,
                source,
            }),
        }
    }

    fn candidate_path(&self, candidate: CandidateIdentity) -> PathBuf {
        self.candidates_directory().join(format!(
            "{}_{}_{}{ARTIFACT_SUFFIX}",
            candidate.slot, candidate.bank_id, candidate.epoch,
        ))
    }

    fn candidates_directory(&self) -> PathBuf {
        candidate_directory(&self.artifact_path)
    }

    fn canonical_path(&self, epoch: Epoch) -> PathBuf {
        self.artifact_path.join(format!("{epoch}{ARTIFACT_SUFFIX}"))
    }

    fn remove_candidates_for_epoch(&self, epoch: Epoch) -> Result<(), ArtifactStoreError> {
        let candidates_directory = self.candidates_directory();
        let candidates_to_remove = fs::read_dir(&candidates_directory)
            .map_err(|source| ArtifactStoreError::CleanupFailed { epoch, source })?
            .map(|entry| {
                entry.map_err(|source| ArtifactStoreError::CleanupFailed { epoch, source })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .filter(|entry| candidate_epoch_from_name(&entry.file_name()) == Some(epoch))
            .map(|entry| entry.path())
            .collect::<Vec<_>>();

        for candidate_path in candidates_to_remove {
            fs::remove_file(candidate_path)
                .map_err(|source| ArtifactStoreError::CleanupFailed { epoch, source })?;
        }

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum ArtifactStoreError {
    #[error("unavailable directory: {}", path.display())]
    DirectoryUnavailable { path: PathBuf, source: io::Error },

    #[error("artifact store io error")]
    Io(#[from] io::Error),

    #[error("stake_meta serialization error")]
    Serialiazation(#[from] serde_json::Error),

    #[error("error publish artiifact")]
    PublishError(#[from] PublishError),

    #[error("failed to clean up candidate artifacts for epoch {epoch}: {source}")]
    CleanupFailed {
        epoch: Epoch,
        #[source]
        source: io::Error,
    },
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishError {
    #[error("winning candidate artifact does not exist: {}", path.display())]
    CandidateNotFound { path: PathBuf },

    #[error(
        "failed to publish winning candidate from {} to {}: {source}",
        candidate_path.display(),
        publication_path.display()
    )]
    PublishFailed {
        candidate_path: PathBuf,
        publication_path: PathBuf,
        #[source]
        source: io::Error,
    },

    #[error("artifact was already published at {}", path.display())]
    AlreadyPublished { path: PathBuf },
}

fn candidate_directory(output_dir: &Path) -> PathBuf {
    output_dir.join(CANDIDATES_DIRECTORY_NAME)
}

/// Checks path_dir is directory, if not, creates it
fn ensure_output_directory(output_dir: &Path) -> io::Result<()> {
    match fs::metadata(output_dir) {
        Ok(metadata) if metadata.is_dir() => Ok(()),
        Ok(_) => Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{} is not a directory", output_dir.display()),
        )),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(output_dir)?;
            if fs::metadata(output_dir)?.is_dir() {
                Ok(())
            } else {
                Err(io::Error::new(
                    io::ErrorKind::NotADirectory,
                    format!("{} is not a directory", output_dir.display()),
                ))
            }
        }
        Err(err) => Err(err),
    }
}

/// Parses + Validates file name
fn candidate_epoch_from_name(file_name: &OsStr) -> Option<Epoch> {
    let identity = file_name.to_str()?.strip_suffix(ARTIFACT_SUFFIX)?;
    let (slot_and_bank_id, epoch) = identity.rsplit_once('_')?;
    let (slot, bank_id) = slot_and_bank_id.split_once('_')?;

    slot.parse::<u64>().ok()?;
    bank_id.parse::<BankId>().ok()?;
    epoch.parse().ok()
}
