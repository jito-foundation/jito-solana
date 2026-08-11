use {
    crate::candidate::CandidateIdentity,
    jito_stake_meta_types::StakeMetaCollection,
    log::warn,
    solana_clock::Epoch,
    solana_hash::Hash,
    std::{
        ffi::OsStr,
        fs::{self, File, OpenOptions},
        io::{self, BufWriter, Write},
        path::{Path, PathBuf},
        str::FromStr,
    },
};

const ARTIFACT_SUFFIX: &str = "_stake_meta_collection.json";
const TEMP_ARTIFACT_PREFIX: &str = "tmp_";
const CANDIDATES_DIRECTORY_NAME: &str = "candidates";

#[derive(Debug)]
pub(crate) struct CandidateStoreInitializationError {
    pub(crate) path: PathBuf,
    pub(crate) source: io::Error,
}

#[derive(Debug)]
pub(crate) enum CandidateStoreError {
    DirectoryUnavailable { path: PathBuf, source: io::Error },
    Io(io::Error),
}

impl std::fmt::Display for CandidateStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DirectoryUnavailable { path, source } => write!(
                formatter,
                "candidate directory {} is unavailable: {source}",
                path.display()
            ),
            Self::Io(err) => write!(formatter, "candidate store I/O failed: {err}"),
        }
    }
}

impl std::error::Error for CandidateStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::DirectoryUnavailable { source, .. } | Self::Io(source) => Some(source),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PublicationOutcome {
    Published { path: PathBuf },
    AlreadyPublished { path: PathBuf },
}

#[derive(Clone, Debug)]
pub(crate) struct CandidateStore {
    output_dir: PathBuf,
}

impl CandidateStore {
    pub(crate) fn new(output_dir: PathBuf) -> Result<Self, CandidateStoreInitializationError> {
        ensure_output_directory(&output_dir).map_err(|source| {
            CandidateStoreInitializationError {
                path: output_dir.clone(),
                source,
            }
        })?;
        let candidates_dir = candidate_directory(&output_dir);
        ensure_output_directory(&candidates_dir).map_err(|source| {
            CandidateStoreInitializationError {
                path: candidates_dir,
                source,
            }
        })?;
        Ok(Self { output_dir })
    }

    pub(crate) fn latest_published_epoch(&self) -> io::Result<Option<Epoch>> {
        ensure_output_directory(&self.output_dir)?;
        ensure_output_directory(&candidate_directory(&self.output_dir))?;
        let mut latest_epoch = None;
        for entry in fs::read_dir(&self.output_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_file()
                && let Some(epoch) = canonical_artifact_epoch(&entry.file_name())
            {
                latest_epoch = Some(latest_epoch.map_or(epoch, |latest: Epoch| latest.max(epoch)));
            }
        }
        Ok(latest_epoch)
    }

    pub(crate) fn write_candidate(
        &self,
        candidate: CandidateIdentity,
        stake_meta: &StakeMetaCollection,
    ) -> Result<PathBuf, CandidateStoreError> {
        self.ensure_available()?;
        let candidate_path = self.candidate_path(candidate);
        let mut cleanup_guard = CandidateCleanupGuard::new(&candidate_path);

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate_path)
            .map_err(CandidateStoreError::Io)?;
        {
            let mut writer = BufWriter::new(&file);
            serde_json::to_writer_pretty(&mut writer, stake_meta)
                .map_err(io::Error::other)
                .map_err(CandidateStoreError::Io)?;
            writer.write_all(b"\n").map_err(CandidateStoreError::Io)?;
            writer.flush().map_err(CandidateStoreError::Io)?;
        }
        file.sync_all().map_err(CandidateStoreError::Io)?;
        cleanup_guard.disarm();
        drop(cleanup_guard);
        Ok(candidate_path)
    }

    /// Reconciles durable candidates into the final published state for `winner`.
    ///
    /// On success, the canonical artifact exists and all temporary candidates for the winner's
    /// epoch are gone.
    pub(crate) fn finalize_publication(
        &self,
        winner: CandidateIdentity,
    ) -> Result<PublicationOutcome, CandidateStoreError> {
        self.ensure_available()?;

        let winner_path = self.candidate_path(winner);
        let artifact_path = self.canonical_path(winner.epoch);
        // The canonical name is the publication signal for downstream watchers.
        let outcome = match fs::hard_link(&winner_path, &artifact_path) {
            Ok(()) => PublicationOutcome::Published {
                path: artifact_path.clone(),
            },
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists && artifact_path.is_file() => {
                PublicationOutcome::AlreadyPublished {
                    path: artifact_path.clone(),
                }
            }
            Err(err) => return Err(CandidateStoreError::Io(err)),
        };

        File::open(&self.output_dir)
            .and_then(|directory| directory.sync_all())
            .map_err(CandidateStoreError::Io)?;
        self.remove_candidates_for_epoch(winner.epoch)?;
        File::open(self.candidates_directory())
            .and_then(|directory| directory.sync_all())
            .map_err(CandidateStoreError::Io)?;
        Ok(outcome)
    }

    /// Deletes temporary candidates only after their epoch has been published.
    fn remove_candidates_for_epoch(&self, epoch: Epoch) -> Result<(), CandidateStoreError> {
        for entry in fs::read_dir(self.candidates_directory()).map_err(CandidateStoreError::Io)? {
            let entry = entry.map_err(CandidateStoreError::Io)?;
            let Some(candidate) = candidate_identity_from_name(&entry.file_name()) else {
                continue;
            };
            if candidate.epoch == epoch {
                remove_file_idempotently(&entry.path()).map_err(CandidateStoreError::Io)?;
            }
        }
        Ok(())
    }

    fn ensure_available(&self) -> Result<(), CandidateStoreError> {
        ensure_output_directory(&self.output_dir).map_err(|source| {
            CandidateStoreError::DirectoryUnavailable {
                path: self.output_dir.clone(),
                source,
            }
        })?;
        let candidates_dir = candidate_directory(&self.output_dir);
        ensure_output_directory(&candidates_dir).map_err(|source| {
            CandidateStoreError::DirectoryUnavailable {
                path: candidates_dir,
                source,
            }
        })
    }

    fn candidate_path(&self, candidate: CandidateIdentity) -> PathBuf {
        self.candidates_directory().join(format!(
            "{TEMP_ARTIFACT_PREFIX}{}_{}_{}{ARTIFACT_SUFFIX}",
            candidate.slot, candidate.bank_hash, candidate.epoch,
        ))
    }

    fn candidates_directory(&self) -> PathBuf {
        candidate_directory(&self.output_dir)
    }

    fn canonical_path(&self, epoch: Epoch) -> PathBuf {
        self.output_dir.join(format!("{epoch}{ARTIFACT_SUFFIX}"))
    }
}

fn candidate_directory(output_dir: &Path) -> PathBuf {
    output_dir.join(CANDIDATES_DIRECTORY_NAME)
}

fn ensure_output_directory(output_dir: &Path) -> io::Result<()> {
    match fs::metadata(output_dir) {
        Ok(metadata) if metadata.is_dir() => return Ok(()),
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::NotADirectory,
                format!("{} is not a directory", output_dir.display()),
            ));
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

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

fn candidate_identity_from_name(file_name: &OsStr) -> Option<CandidateIdentity> {
    let identity = file_name
        .to_str()?
        .strip_prefix(TEMP_ARTIFACT_PREFIX)?
        .strip_suffix(ARTIFACT_SUFFIX)?;
    let mut parts = identity.split('_');
    let slot = parts.next()?.parse().ok()?;
    let bank_hash = Hash::from_str(parts.next()?).ok()?;
    let epoch = parts.next()?.parse().ok()?;
    parts.next().is_none().then_some(CandidateIdentity {
        epoch,
        slot,
        bank_hash,
    })
}

fn canonical_artifact_epoch(file_name: &OsStr) -> Option<Epoch> {
    let epoch = file_name.to_str()?.strip_suffix(ARTIFACT_SUFFIX)?;
    (!epoch.starts_with(TEMP_ARTIFACT_PREFIX))
        .then(|| epoch.parse().ok())
        .flatten()
}

fn remove_file_idempotently(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err),
    }
}

struct CandidateCleanupGuard<'a> {
    path: &'a Path,
    armed: bool,
}

impl<'a> CandidateCleanupGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for CandidateCleanupGuard<'_> {
    fn drop(&mut self) {
        if self.armed
            && let Err(err) = remove_file_idempotently(self.path)
        {
            warn!(
                "IMPORTANT: failed to clean up tip-router snapshot candidate {}: {}",
                self.path.display(),
                err
            );
        }
    }
}
