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

#[cfg(test)]
mod tests {
    use {super::*, tempfile::tempdir};

    fn candidate(epoch: Epoch, slot: u64) -> CandidateIdentity {
        CandidateIdentity {
            epoch,
            slot,
            bank_hash: Hash::new_unique(),
        }
    }

    #[test]
    fn startup_creates_candidates_directory() {
        let workspace = tempdir().unwrap();
        let output_dir = workspace.path().join("tip-router-artifacts");

        CandidateStore::new(output_dir.clone()).unwrap();

        assert!(output_dir.is_dir());
        assert!(candidate_directory(&output_dir).is_dir());
    }

    #[test]
    fn startup_rejects_a_file_at_candidates_directory_path() {
        let output_dir = tempdir().unwrap();
        let candidates_dir = candidate_directory(output_dir.path());
        fs::write(&candidates_dir, b"not a directory").unwrap();

        let err = CandidateStore::new(output_dir.path().to_path_buf()).unwrap_err();

        assert_eq!(err.path, candidates_dir);
        assert_eq!(err.source.kind(), io::ErrorKind::NotADirectory);
    }

    #[test]
    fn latest_published_epoch_recreates_missing_candidates_directory() {
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let candidates_dir = candidate_directory(output_dir.path());
        fs::remove_dir(&candidates_dir).unwrap();

        assert_eq!(store.latest_published_epoch().unwrap(), None);
        assert!(candidates_dir.is_dir());
    }

    #[test]
    fn candidate_name_uses_slot_hash_epoch_order() {
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let candidate = candidate(7, 42);

        assert_eq!(
            store
                .candidate_path(candidate)
                .file_name()
                .unwrap()
                .to_str()
                .unwrap(),
            format!(
                "tmp_{}_{}_{}_stake_meta_collection.json",
                candidate.slot, candidate.bank_hash, candidate.epoch
            )
        );
        assert_eq!(
            store.candidate_path(candidate).parent(),
            Some(candidate_directory(output_dir.path()).as_path())
        );
    }

    #[test]
    fn startup_preserves_candidates_and_discovers_canonical_epochs() {
        let output_dir = tempdir().unwrap();
        let candidate = candidate(7, 42);
        let candidate_path = candidate_directory(output_dir.path()).join(format!(
            "tmp_{}_{}_{}_stake_meta_collection.json",
            candidate.slot, candidate.bank_hash, candidate.epoch
        ));
        fs::create_dir(candidate_directory(output_dir.path())).unwrap();
        fs::write(&candidate_path, b"candidate").unwrap();
        fs::write(
            output_dir.path().join("8_stake_meta_collection.json"),
            b"published",
        )
        .unwrap();

        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();

        assert!(candidate_path.exists());
        assert_eq!(store.latest_published_epoch().unwrap(), Some(8));
    }

    #[test]
    fn publication_cleans_only_winning_epoch_and_never_replaces_canonical() {
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let winner = candidate(7, 42);
        let loser_same_epoch = candidate(7, 41);
        let loser_other_epoch = candidate(6, 40);
        fs::write(store.candidate_path(winner), b"winner").unwrap();
        fs::write(store.candidate_path(loser_same_epoch), b"loser").unwrap();
        fs::write(store.candidate_path(loser_other_epoch), b"stale").unwrap();

        let PublicationOutcome::Published {
            path: canonical_path,
        } = store.finalize_publication(winner).unwrap()
        else {
            panic!("expected a newly published artifact");
        };

        assert_eq!(fs::read(&canonical_path).unwrap(), b"winner");
        assert!(!store.candidate_path(loser_same_epoch).exists());
        assert!(store.candidate_path(loser_other_epoch).exists());

        fs::write(store.candidate_path(winner), b"replacement").unwrap();
        assert!(matches!(
            store.finalize_publication(winner),
            Ok(PublicationOutcome::AlreadyPublished { .. })
        ));
        assert_eq!(fs::read(canonical_path).unwrap(), b"winner");
        assert!(!store.candidate_path(winner).exists());
    }

    #[test]
    fn failed_publication_preserves_same_epoch_candidates() {
        let output_dir = tempdir().unwrap();
        let store = CandidateStore::new(output_dir.path().to_path_buf()).unwrap();
        let winner = candidate(7, 42);
        let loser = candidate(7, 40);
        fs::write(store.candidate_path(winner), b"winner").unwrap();
        fs::write(store.candidate_path(loser), b"loser").unwrap();
        fs::create_dir(store.canonical_path(winner.epoch)).unwrap();

        assert!(store.finalize_publication(winner).is_err());
        assert!(store.candidate_path(winner).exists());
        assert!(store.candidate_path(loser).exists());

        fs::remove_dir(store.canonical_path(winner.epoch)).unwrap();
        assert!(matches!(
            store.finalize_publication(winner),
            Ok(PublicationOutcome::Published { .. })
        ));
        assert!(!store.candidate_path(winner).exists());
        assert!(!store.candidate_path(loser).exists());
    }
}
