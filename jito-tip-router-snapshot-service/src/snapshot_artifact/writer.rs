use {
    super::SnapshotArtifactError,
    jito_stake_meta_types::StakeMetaCollection,
    log::warn,
    solana_clock::Epoch,
    std::{
        ffi::OsStr,
        fs::{self, File, OpenOptions},
        io::{self, BufWriter, Write},
        path::{Path, PathBuf},
    },
};

const ARTIFACT_SUFFIX: &str = "-stake_meta_collection.json";
const TEMP_ARTIFACT_PREFIX: &str = "tmp-";

#[derive(Debug)]
pub(crate) struct ArtifactDirectoryError {
    pub(crate) path: PathBuf,
    pub(crate) source: io::Error,
}

#[derive(Clone, Debug)]
pub(crate) struct SnapshotArtifactWriter {
    output_dir: PathBuf,
}

impl SnapshotArtifactWriter {
    pub(crate) fn new(output_dir: PathBuf) -> Result<Self, ArtifactDirectoryError> {
        initialize_output_directory(&output_dir).map_err(|source| ArtifactDirectoryError {
            path: output_dir.clone(),
            source,
        })?;
        Ok(Self { output_dir })
    }

    // First creates a tmp file (since other processes are watching specifically for new stake-meta
    // files), then writes it, then mv
    pub(super) fn write(
        &self,
        epoch: Epoch,
        stake_meta: &StakeMetaCollection,
    ) -> Result<PathBuf, SnapshotArtifactError> {
        ensure_output_directory(&self.output_dir).map_err(|source| {
            SnapshotArtifactError::DirectoryUnavailable {
                path: self.output_dir.clone(),
                source,
            }
        })?;

        let artifact_id = if std::env::var(crate::service::STAKE_META_INTERVAL_SLOTS_ENV)
            .is_ok_and(|value| !value.is_empty())
        {
            stake_meta.bank_hash.clone()
        } else {
            epoch.to_string()
        };
        let temp_path = self.output_dir.join(format!(
            "{TEMP_ARTIFACT_PREFIX}{artifact_id}{ARTIFACT_SUFFIX}"
        ));
        let artifact_path = self
            .output_dir
            .join(format!("{artifact_id}{ARTIFACT_SUFFIX}"));

        let mut cleanup_guard = TempArtifactCleanupGuard::new(&temp_path);

        // Overwrite the file if it already exists. Perhaps bc tmp was created by another
        // TODO: This will  be an issue if we end up with multiple fork candidates and they all
        // write the same file name
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp_path)
            .map_err(SnapshotArtifactError::Io)?;
        {
            let mut writer = BufWriter::new(&file);
            serde_json::to_writer_pretty(&mut writer, stake_meta)
                .map_err(io::Error::other)
                .map_err(SnapshotArtifactError::Io)?;
            // Done solely bc it was done in old tip router. Not sure of use
            writer.write_all(b"\n").map_err(SnapshotArtifactError::Io)?;
            writer.flush().map_err(SnapshotArtifactError::Io)?;
        }
        file.sync_all().map_err(SnapshotArtifactError::Io)?;

        if artifact_path
            .try_exists()
            .map_err(SnapshotArtifactError::Io)?
        {
            warn!(
                "replacing tip-router snapshot artifact for epoch {} at {}",
                epoch,
                artifact_path.display()
            );
        }

        // Rename tmp file to canonical path. This will trigger other processes watching for this
        // file name to start processing it
        fs::rename(&temp_path, &artifact_path).map_err(SnapshotArtifactError::Io)?;

        // TODO: Whats this
        cleanup_guard.disarm();
        if let Err(err) = File::open(&self.output_dir).and_then(|directory| directory.sync_all()) {
            warn!(
                "tip-router snapshot artifact for epoch {} was published to {}, but syncing the \
                 artifact directory failed; crash durability is not guaranteed: {}",
                epoch,
                artifact_path.display(),
                err
            );
        }

        Ok(artifact_path)
    }
}

fn initialize_output_directory(output_dir: &Path) -> io::Result<()> {
    // Make sure dir exists
    ensure_output_directory(output_dir)?;

    // Remove all leftover tmp-* snapshot files
    for entry in fs::read_dir(output_dir)? {
        let entry = entry?;
        if is_temp_artifact_name(&entry.file_name()) {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Make sure full dir exists or create it
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
    let metadata = fs::metadata(output_dir)?;
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotADirectory,
            format!("{} is not a directory", output_dir.display()),
        ))
    }
}

fn is_temp_artifact_name(file_name: &OsStr) -> bool {
    let Some(file_name) = file_name.to_str() else {
        return false;
    };
    let Some(artifact_id) = file_name
        .strip_prefix(TEMP_ARTIFACT_PREFIX)
        .and_then(|name| name.strip_suffix(ARTIFACT_SUFFIX))
    else {
        return false;
    };

    !artifact_id.is_empty()
}

struct TempArtifactCleanupGuard<'a> {
    path: &'a Path,
    armed: bool,
}

impl<'a> TempArtifactCleanupGuard<'a> {
    fn new(path: &'a Path) -> Self {
        Self { path, armed: true }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TempArtifactCleanupGuard<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }

        if let Err(err) = fs::remove_file(self.path)
            && err.kind() != io::ErrorKind::NotFound
        {
            warn!(
                "IMPORTANT: failed to clean up temporary tip-router snapshot artifact {}: {}",
                self.path.display(),
                err
            );
        }
    }
}
