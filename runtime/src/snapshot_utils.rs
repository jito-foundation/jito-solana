use {
    crate::{
        accounts_db::{
            AccountShrinkThreshold, AccountsDbConfig, SnapshotStorage, SnapshotStorages,
        },
        accounts_index::AccountSecondaryIndexes,
        accounts_update_notifier_interface::AccountsUpdateNotifier,
        bank::{Bank, BankSlotDelta},
        builtins::Builtins,
        hardened_unpack::{unpack_snapshot, ParallelSelector, UnpackError, UnpackedAppendVecMap},
        serde_snapshot::{bank_from_streams, bank_to_stream, SerdeStyle, SnapshotStreams},
        shared_buffer_reader::{SharedBuffer, SharedBufferReader},
        snapshot_archive_info::{
            FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
        },
        snapshot_package::{
            AccountsPackage, PendingAccountsPackage, SnapshotPackage, SnapshotType,
        },
    },
    bincode::{config::Options, serialize_into},
    bzip2::bufread::BzDecoder,
    flate2::read::GzDecoder,
    lazy_static::lazy_static,
    log::*,
    rayon::prelude::*,
    regex::Regex,
    solana_measure::measure::Measure,
    solana_sdk::{clock::Slot, genesis_config::GenesisConfig, hash::Hash, pubkey::Pubkey},
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet},
        fmt,
        fs::{self, File},
        io::{BufReader, BufWriter, Error as IoError, ErrorKind, Read, Seek, Write},
        path::{Path, PathBuf},
        process::ExitStatus,
        str::FromStr,
        sync::Arc,
    },
    tar::{self, Archive},
    tempfile::TempDir,
    thiserror::Error,
};

mod archive_format;
pub use archive_format::*;

pub const SNAPSHOT_STATUS_CACHE_FILENAME: &str = "status_cache";
pub const SNAPSHOT_ARCHIVE_DOWNLOAD_DIR: &str = "remote";
pub const DEFAULT_FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot = 25_000;
pub const DEFAULT_INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot = 100;
const MAX_SNAPSHOT_DATA_FILE_SIZE: u64 = 32 * 1024 * 1024 * 1024; // 32 GiB
const MAX_SNAPSHOT_VERSION_FILE_SIZE: u64 = 8; // byte
const VERSION_STRING_V1_2_0: &str = "1.2.0";
pub(crate) const TMP_BANK_SNAPSHOT_PREFIX: &str = "tmp-bank-snapshot-";
pub const TMP_SNAPSHOT_ARCHIVE_PREFIX: &str = "tmp-snapshot-archive-";
pub const BANK_SNAPSHOT_PRE_FILENAME_EXTENSION: &str = "pre";
pub const MAX_BANK_SNAPSHOTS_TO_RETAIN: usize = 8; // Save some bank snapshots but not too many
pub const DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN: usize = 2;
pub const DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN: usize = 4;
pub const FULL_SNAPSHOT_ARCHIVE_FILENAME_REGEX: &str = r"^snapshot-(?P<slot>[[:digit:]]+)-(?P<hash>[[:alnum:]]+)\.(?P<ext>tar|tar\.bz2|tar\.zst|tar\.gz)$";
pub const INCREMENTAL_SNAPSHOT_ARCHIVE_FILENAME_REGEX: &str = r"^incremental-snapshot-(?P<base>[[:digit:]]+)-(?P<slot>[[:digit:]]+)-(?P<hash>[[:alnum:]]+)\.(?P<ext>tar|tar\.bz2|tar\.zst|tar\.gz)$";

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub enum SnapshotVersion {
    V1_2_0,
}

impl Default for SnapshotVersion {
    fn default() -> Self {
        SnapshotVersion::V1_2_0
    }
}

impl fmt::Display for SnapshotVersion {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(From::from(*self))
    }
}

impl From<SnapshotVersion> for &'static str {
    fn from(snapshot_version: SnapshotVersion) -> &'static str {
        match snapshot_version {
            SnapshotVersion::V1_2_0 => VERSION_STRING_V1_2_0,
        }
    }
}

impl FromStr for SnapshotVersion {
    type Err = &'static str;

    fn from_str(version_string: &str) -> std::result::Result<Self, Self::Err> {
        // Remove leading 'v' or 'V' from slice
        let version_string = if version_string
            .get(..1)
            .map_or(false, |s| s.eq_ignore_ascii_case("v"))
        {
            &version_string[1..]
        } else {
            version_string
        };
        match version_string {
            VERSION_STRING_V1_2_0 => Ok(SnapshotVersion::V1_2_0),
            _ => Err("unsupported snapshot version"),
        }
    }
}

impl SnapshotVersion {
    pub fn as_str(self) -> &'static str {
        <&str as From<Self>>::from(self)
    }

    fn maybe_from_string(version_string: &str) -> Option<SnapshotVersion> {
        version_string.parse::<Self>().ok()
    }
}

/// Information about a bank snapshot. Namely the slot of the bank, the path to the snapshot, and
/// the type of the snapshot.
#[derive(PartialEq, Eq, Debug)]
pub struct BankSnapshotInfo {
    /// Slot of the bank
    pub slot: Slot,
    /// Path to the snapshot
    pub snapshot_path: PathBuf,
    /// Type of the snapshot
    pub snapshot_type: BankSnapshotType,
}

impl PartialOrd for BankSnapshotInfo {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

// Order BankSnapshotInfo by slot (ascending), which practically is sorting chronologically
impl Ord for BankSnapshotInfo {
    fn cmp(&self, other: &Self) -> Ordering {
        self.slot.cmp(&other.slot)
    }
}

/// Bank snapshots traditionally had their accounts hash calculated prior to serialization.  Since
/// the hash calculation takes a long time, an optimization has been put in to offload the accounts
/// hash calculation.  The bank serialization format has not changed, so we need another way to
/// identify if a bank snapshot contains the calculated accounts hash or not.
///
/// When a bank snapshot is first taken, it does not have the calculated accounts hash.  It is said
/// that this bank snapshot is "pre" accounts hash.  Later, when the accounts hash is calculated,
/// the bank snapshot is re-serialized, and is now "post" accounts hash.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum BankSnapshotType {
    /// This bank snapshot has *not* yet had its accounts hash calculated
    Pre,
    /// This bank snapshot *has* had its accounts hash calculated
    Post,
}

/// Helper type when rebuilding from snapshots.  Designed to handle when rebuilding from just a
/// full snapshot, or from both a full snapshot and an incremental snapshot.
#[derive(Debug)]
struct SnapshotRootPaths {
    full_snapshot_root_file_path: PathBuf,
    incremental_snapshot_root_file_path: Option<PathBuf>,
}

/// Helper type to bundle up the results from `unarchive_snapshot()`
#[derive(Debug)]
struct UnarchivedSnapshot {
    #[allow(dead_code)]
    unpack_dir: TempDir,
    unpacked_append_vec_map: UnpackedAppendVecMap,
    unpacked_snapshots_dir_and_version: UnpackedSnapshotsDirAndVersion,
    measure_untar: Measure,
}

/// Helper type for passing around the unpacked snapshots dir and the snapshot version together
#[derive(Debug)]
struct UnpackedSnapshotsDirAndVersion {
    unpacked_snapshots_dir: PathBuf,
    snapshot_version: String,
}

#[derive(Error, Debug)]
#[allow(clippy::large_enum_variant)]
pub enum SnapshotError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serialize(#[from] bincode::Error),

    #[error("archive generation failure {0}")]
    ArchiveGenerationFailure(ExitStatus),

    #[error("storage path symlink is invalid")]
    StoragePathSymlinkInvalid,

    #[error("Unpack error: {0}")]
    UnpackError(#[from] UnpackError),

    #[error("source({1}) - I/O error: {0}")]
    IoWithSource(std::io::Error, &'static str),

    #[error("could not get file name from path: {}", .0.display())]
    PathToFileNameError(PathBuf),

    #[error("could not get str from file name: {}", .0.display())]
    FileNameToStrError(PathBuf),

    #[error("could not parse snapshot archive's file name: {0}")]
    ParseSnapshotArchiveFileNameError(String),

    #[error("snapshots are incompatible: full snapshot slot ({0}) and incremental snapshot base slot ({1}) do not match")]
    MismatchedBaseSlot(Slot, Slot),

    #[error("no snapshot archives to load from")]
    NoSnapshotArchives,

    #[error("snapshot has mismatch: deserialized bank: {:?}, snapshot archive info: {:?}", .0, .1)]
    MismatchedSlotHash((Slot, Hash), (Slot, Hash)),
}
pub type Result<T> = std::result::Result<T, SnapshotError>;

/// If the validator halts in the middle of `archive_snapshot_package()`, the temporary staging
/// directory won't be cleaned up.  Call this function to clean them up.
pub fn remove_tmp_snapshot_archives(snapshot_archives_dir: impl AsRef<Path>) {
    if let Ok(entries) = fs::read_dir(snapshot_archives_dir) {
        for entry in entries.filter_map(|entry| entry.ok()) {
            let file_name = entry
                .file_name()
                .into_string()
                .unwrap_or_else(|_| String::new());
            if file_name.starts_with(TMP_SNAPSHOT_ARCHIVE_PREFIX) {
                if entry.path().is_file() {
                    fs::remove_file(entry.path())
                } else {
                    fs::remove_dir_all(entry.path())
                }
                .unwrap_or_else(|err| {
                    warn!("Failed to remove {}: {}", entry.path().display(), err)
                });
            }
        }
    }
}

/// Make a snapshot archive out of the snapshot package
pub fn archive_snapshot_package(
    snapshot_package: &SnapshotPackage,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) -> Result<()> {
    info!(
        "Generating snapshot archive for slot {}",
        snapshot_package.slot()
    );

    serialize_status_cache(
        snapshot_package.slot(),
        &snapshot_package.slot_deltas,
        &snapshot_package
            .snapshot_links
            .path()
            .join(SNAPSHOT_STATUS_CACHE_FILENAME),
    )?;

    let mut timer = Measure::start("snapshot_package-package_snapshots");
    let tar_dir = snapshot_package
        .path()
        .parent()
        .expect("Tar output path is invalid");

    fs::create_dir_all(tar_dir)
        .map_err(|e| SnapshotError::IoWithSource(e, "create archive path"))?;

    // Create the staging directories
    let staging_dir_prefix = TMP_SNAPSHOT_ARCHIVE_PREFIX;
    let staging_dir = tempfile::Builder::new()
        .prefix(&format!(
            "{}{}-",
            staging_dir_prefix,
            snapshot_package.slot()
        ))
        .tempdir_in(tar_dir)
        .map_err(|e| SnapshotError::IoWithSource(e, "create archive tempdir"))?;

    let staging_accounts_dir = staging_dir.path().join("accounts");
    let staging_snapshots_dir = staging_dir.path().join("snapshots");
    let staging_version_file = staging_dir.path().join("version");
    fs::create_dir_all(&staging_accounts_dir)
        .map_err(|e| SnapshotError::IoWithSource(e, "create staging path"))?;

    // Add the snapshots to the staging directory
    symlink::symlink_dir(
        snapshot_package.snapshot_links.path(),
        &staging_snapshots_dir,
    )
    .map_err(|e| SnapshotError::IoWithSource(e, "create staging symlinks"))?;

    // Add the AppendVecs into the compressible list
    for storage in snapshot_package.snapshot_storages.iter().flatten() {
        storage.flush()?;
        let storage_path = storage.get_path();
        let output_path = staging_accounts_dir.join(crate::append_vec::AppendVec::file_name(
            storage.slot(),
            storage.append_vec_id(),
        ));

        // `storage_path` - The file path where the AppendVec itself is located
        // `output_path` - The file path where the AppendVec will be placed in the staging directory.
        let storage_path =
            fs::canonicalize(storage_path).expect("Could not get absolute path for accounts");
        symlink::symlink_file(storage_path, &output_path)
            .map_err(|e| SnapshotError::IoWithSource(e, "create storage symlink"))?;
        if !output_path.is_file() {
            return Err(SnapshotError::StoragePathSymlinkInvalid);
        }
    }

    // Write version file
    {
        let mut f = fs::File::create(staging_version_file)
            .map_err(|e| SnapshotError::IoWithSource(e, "create version file"))?;
        f.write_all(snapshot_package.snapshot_version.as_str().as_bytes())
            .map_err(|e| SnapshotError::IoWithSource(e, "write version file"))?;
    }

    // Tar the staging directory into the archive at `archive_path`
    let archive_path = tar_dir.join(format!(
        "{}{}.{}",
        staging_dir_prefix,
        snapshot_package.slot(),
        snapshot_package.archive_format().extension(),
    ));

    {
        let mut archive_file = fs::File::create(&archive_path)?;

        let do_archive_files = |encoder: &mut dyn Write| -> Result<()> {
            let mut archive = tar::Builder::new(encoder);
            for dir in ["accounts", "snapshots"] {
                archive.append_dir_all(dir, staging_dir.as_ref().join(dir))?;
            }
            archive.append_path_with_name(staging_dir.as_ref().join("version"), "version")?;
            archive.into_inner()?;
            Ok(())
        };

        match snapshot_package.archive_format() {
            ArchiveFormat::TarBzip2 => {
                let mut encoder =
                    bzip2::write::BzEncoder::new(archive_file, bzip2::Compression::best());
                do_archive_files(&mut encoder)?;
                encoder.finish()?;
            }
            ArchiveFormat::TarGzip => {
                let mut encoder =
                    flate2::write::GzEncoder::new(archive_file, flate2::Compression::default());
                do_archive_files(&mut encoder)?;
                encoder.finish()?;
            }
            ArchiveFormat::TarZstd => {
                let mut encoder = zstd::stream::Encoder::new(archive_file, 0)?;
                do_archive_files(&mut encoder)?;
                encoder.finish()?;
            }
            ArchiveFormat::Tar => {
                do_archive_files(&mut archive_file)?;
            }
        };
    }

    // Atomically move the archive into position for other validators to find
    let metadata = fs::metadata(&archive_path)
        .map_err(|e| SnapshotError::IoWithSource(e, "archive path stat"))?;
    fs::rename(&archive_path, &snapshot_package.path())
        .map_err(|e| SnapshotError::IoWithSource(e, "archive path rename"))?;

    purge_old_snapshot_archives(
        tar_dir,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    );

    timer.stop();
    info!(
        "Successfully created {:?}. slot: {}, elapsed ms: {}, size={}",
        snapshot_package.path(),
        snapshot_package.slot(),
        timer.as_ms(),
        metadata.len()
    );

    datapoint_info!(
        "archive-snapshot-package",
        ("slot", snapshot_package.slot(), i64),
        ("duration_ms", timer.as_ms(), i64),
        (
            if snapshot_package.snapshot_type.is_full_snapshot() {
                "full-snapshot-archive-size"
            } else {
                "incremental-snapshot-archive-size"
            },
            metadata.len(),
            i64
        ),
    );
    Ok(())
}

/// Get the bank snapshots in a directory
pub fn get_bank_snapshots(bank_snapshots_dir: impl AsRef<Path>) -> Vec<BankSnapshotInfo> {
    let mut bank_snapshots = Vec::default();
    match fs::read_dir(&bank_snapshots_dir) {
        Err(err) => {
            info!(
                "Unable to read bank snapshots directory {}: {}",
                bank_snapshots_dir.as_ref().display(),
                err
            );
        }
        Ok(paths) => paths
            .filter_map(|entry| {
                // check if this entry is a directory and only a Slot
                // bank snapshots are bank_snapshots_dir/slot/slot(BANK_SNAPSHOT_PRE_FILENAME_EXTENSION)
                entry
                    .ok()
                    .filter(|entry| entry.path().is_dir())
                    .and_then(|entry| {
                        entry
                            .path()
                            .file_name()
                            .and_then(|file_name| file_name.to_str())
                            .and_then(|file_name| file_name.parse::<Slot>().ok())
                    })
            })
            .for_each(|slot| {
                // check this directory to see if there is a BankSnapshotPre and/or
                // BankSnapshotPost file
                let bank_snapshot_outer_dir = get_bank_snapshots_dir(&bank_snapshots_dir, slot);
                let bank_snapshot_post_path =
                    bank_snapshot_outer_dir.join(get_snapshot_file_name(slot));
                let mut bank_snapshot_pre_path = bank_snapshot_post_path.clone();
                bank_snapshot_pre_path.set_extension(BANK_SNAPSHOT_PRE_FILENAME_EXTENSION);

                if bank_snapshot_pre_path.is_file() {
                    bank_snapshots.push(BankSnapshotInfo {
                        slot,
                        snapshot_path: bank_snapshot_pre_path,
                        snapshot_type: BankSnapshotType::Pre,
                    });
                }

                if bank_snapshot_post_path.is_file() {
                    bank_snapshots.push(BankSnapshotInfo {
                        slot,
                        snapshot_path: bank_snapshot_post_path,
                        snapshot_type: BankSnapshotType::Post,
                    });
                }
            }),
    }
    bank_snapshots
}

/// Get the bank snapshots in a directory
///
/// This function retains only the bank snapshots of type BankSnapshotType::Pre
pub fn get_bank_snapshots_pre(bank_snapshots_dir: impl AsRef<Path>) -> Vec<BankSnapshotInfo> {
    let mut bank_snapshots = get_bank_snapshots(bank_snapshots_dir);
    bank_snapshots.retain(|bank_snapshot| bank_snapshot.snapshot_type == BankSnapshotType::Pre);
    bank_snapshots
}

/// Get the bank snapshots in a directory
///
/// This function retains only the bank snapshots of type BankSnapshotType::Post
pub fn get_bank_snapshots_post(bank_snapshots_dir: impl AsRef<Path>) -> Vec<BankSnapshotInfo> {
    let mut bank_snapshots = get_bank_snapshots(bank_snapshots_dir);
    bank_snapshots.retain(|bank_snapshot| bank_snapshot.snapshot_type == BankSnapshotType::Post);
    bank_snapshots
}

/// Get the bank snapshot with the highest slot in a directory
///
/// This function gets the highest bank snapshot of type BankSnapshotType::Pre
pub fn get_highest_bank_snapshot_pre(
    bank_snapshots_dir: impl AsRef<Path>,
) -> Option<BankSnapshotInfo> {
    do_get_highest_bank_snapshot(get_bank_snapshots_pre(bank_snapshots_dir))
}

/// Get the bank snapshot with the highest slot in a directory
///
/// This function gets the highest bank snapshot of type BankSnapshotType::Post
pub fn get_highest_bank_snapshot_post(
    bank_snapshots_dir: impl AsRef<Path>,
) -> Option<BankSnapshotInfo> {
    do_get_highest_bank_snapshot(get_bank_snapshots_post(bank_snapshots_dir))
}

fn do_get_highest_bank_snapshot(
    mut bank_snapshots: Vec<BankSnapshotInfo>,
) -> Option<BankSnapshotInfo> {
    bank_snapshots.sort_unstable();
    bank_snapshots.into_iter().rev().next()
}

pub fn serialize_snapshot_data_file<F>(data_file_path: &Path, serializer: F) -> Result<u64>
where
    F: FnOnce(&mut BufWriter<File>) -> Result<()>,
{
    serialize_snapshot_data_file_capped::<F>(
        data_file_path,
        MAX_SNAPSHOT_DATA_FILE_SIZE,
        serializer,
    )
}

pub fn deserialize_snapshot_data_file<T: Sized>(
    data_file_path: &Path,
    deserializer: impl FnOnce(&mut BufReader<File>) -> Result<T>,
) -> Result<T> {
    let wrapped_deserializer = move |streams: &mut SnapshotStreams<File>| -> Result<T> {
        deserializer(streams.full_snapshot_stream)
    };

    let wrapped_data_file_path = SnapshotRootPaths {
        full_snapshot_root_file_path: data_file_path.to_path_buf(),
        incremental_snapshot_root_file_path: None,
    };

    deserialize_snapshot_data_files_capped(
        &wrapped_data_file_path,
        MAX_SNAPSHOT_DATA_FILE_SIZE,
        wrapped_deserializer,
    )
}

fn deserialize_snapshot_data_files<T: Sized>(
    snapshot_root_paths: &SnapshotRootPaths,
    deserializer: impl FnOnce(&mut SnapshotStreams<File>) -> Result<T>,
) -> Result<T> {
    deserialize_snapshot_data_files_capped(
        snapshot_root_paths,
        MAX_SNAPSHOT_DATA_FILE_SIZE,
        deserializer,
    )
}

fn serialize_snapshot_data_file_capped<F>(
    data_file_path: &Path,
    maximum_file_size: u64,
    serializer: F,
) -> Result<u64>
where
    F: FnOnce(&mut BufWriter<File>) -> Result<()>,
{
    let data_file = File::create(data_file_path)?;
    let mut data_file_stream = BufWriter::new(data_file);
    serializer(&mut data_file_stream)?;
    data_file_stream.flush()?;

    let consumed_size = data_file_stream.stream_position()?;
    if consumed_size > maximum_file_size {
        let error_message = format!(
            "too large snapshot data file to serialize: {:?} has {} bytes",
            data_file_path, consumed_size
        );
        return Err(get_io_error(&error_message));
    }
    Ok(consumed_size)
}

fn deserialize_snapshot_data_files_capped<T: Sized>(
    snapshot_root_paths: &SnapshotRootPaths,
    maximum_file_size: u64,
    deserializer: impl FnOnce(&mut SnapshotStreams<File>) -> Result<T>,
) -> Result<T> {
    let (full_snapshot_file_size, mut full_snapshot_data_file_stream) =
        create_snapshot_data_file_stream(
            &snapshot_root_paths.full_snapshot_root_file_path,
            maximum_file_size,
        )?;

    let (incremental_snapshot_file_size, mut incremental_snapshot_data_file_stream) =
        if let Some(ref incremental_snapshot_root_file_path) =
            snapshot_root_paths.incremental_snapshot_root_file_path
        {
            let (incremental_snapshot_file_size, incremental_snapshot_data_file_stream) =
                create_snapshot_data_file_stream(
                    incremental_snapshot_root_file_path,
                    maximum_file_size,
                )?;
            (
                Some(incremental_snapshot_file_size),
                Some(incremental_snapshot_data_file_stream),
            )
        } else {
            (None, None)
        };

    let mut snapshot_streams = SnapshotStreams {
        full_snapshot_stream: &mut full_snapshot_data_file_stream,
        incremental_snapshot_stream: incremental_snapshot_data_file_stream.as_mut(),
    };
    let ret = deserializer(&mut snapshot_streams)?;

    check_deserialize_file_consumed(
        full_snapshot_file_size,
        &snapshot_root_paths.full_snapshot_root_file_path,
        &mut full_snapshot_data_file_stream,
    )?;

    if let Some(ref incremental_snapshot_root_file_path) =
        snapshot_root_paths.incremental_snapshot_root_file_path
    {
        check_deserialize_file_consumed(
            incremental_snapshot_file_size.unwrap(),
            incremental_snapshot_root_file_path,
            incremental_snapshot_data_file_stream.as_mut().unwrap(),
        )?;
    }

    Ok(ret)
}

/// Before running the deserializer function, perform common operations on the snapshot archive
/// files, such as checking the file size and opening the file into a stream.
fn create_snapshot_data_file_stream<P>(
    snapshot_root_file_path: P,
    maximum_file_size: u64,
) -> Result<(u64, BufReader<File>)>
where
    P: AsRef<Path>,
{
    let snapshot_file_size = fs::metadata(&snapshot_root_file_path)?.len();

    if snapshot_file_size > maximum_file_size {
        let error_message =
            format!(
            "too large snapshot data file to deserialize: {} has {} bytes (max size is {} bytes)",
            snapshot_root_file_path.as_ref().display(), snapshot_file_size, maximum_file_size
        );
        return Err(get_io_error(&error_message));
    }

    let snapshot_data_file = File::open(&snapshot_root_file_path)?;
    let snapshot_data_file_stream = BufReader::new(snapshot_data_file);

    Ok((snapshot_file_size, snapshot_data_file_stream))
}

/// After running the deserializer function, perform common checks to ensure the snapshot archive
/// files were consumed correctly.
fn check_deserialize_file_consumed<P>(
    file_size: u64,
    file_path: P,
    file_stream: &mut BufReader<File>,
) -> Result<()>
where
    P: AsRef<Path>,
{
    let consumed_size = file_stream.stream_position()?;

    if consumed_size != file_size {
        let error_message =
            format!(
            "invalid snapshot data file: {} has {} bytes, however consumed {} bytes to deserialize",
            file_path.as_ref().display(), file_size, consumed_size
        );
        return Err(get_io_error(&error_message));
    }

    Ok(())
}

/// Serialize a bank to a snapshot
pub fn add_bank_snapshot<P: AsRef<Path>>(
    bank_snapshots_dir: P,
    bank: &Bank,
    snapshot_storages: &[SnapshotStorage],
    snapshot_version: SnapshotVersion,
) -> Result<BankSnapshotInfo> {
    let slot = bank.slot();
    // bank_snapshots_dir/slot
    let bank_snapshots_dir = get_bank_snapshots_dir(bank_snapshots_dir, slot);
    fs::create_dir_all(&bank_snapshots_dir)?;

    // the bank snapshot is stored as bank_snapshots_dir/slot/slot.BANK_SNAPSHOT_PRE_FILENAME_EXTENSION
    let mut bank_snapshot_path = bank_snapshots_dir.join(get_snapshot_file_name(slot));
    bank_snapshot_path.set_extension(BANK_SNAPSHOT_PRE_FILENAME_EXTENSION);

    info!(
        "Creating bank snapshot for slot {}, path: {}",
        slot,
        bank_snapshot_path.display(),
    );

    let mut bank_serialize = Measure::start("bank-serialize-ms");
    let bank_snapshot_serializer = move |stream: &mut BufWriter<File>| -> Result<()> {
        let serde_style = match snapshot_version {
            SnapshotVersion::V1_2_0 => SerdeStyle::Newer,
        };
        bank_to_stream(serde_style, stream.by_ref(), bank, snapshot_storages)?;
        Ok(())
    };
    let consumed_size =
        serialize_snapshot_data_file(&bank_snapshot_path, bank_snapshot_serializer)?;
    bank_serialize.stop();

    // Monitor sizes because they're capped to MAX_SNAPSHOT_DATA_FILE_SIZE
    datapoint_info!(
        "snapshot-bank-file",
        ("slot", slot, i64),
        ("size", consumed_size, i64)
    );

    inc_new_counter_info!("bank-serialize-ms", bank_serialize.as_ms() as usize);

    info!(
        "{} for slot {} at {}",
        bank_serialize,
        slot,
        bank_snapshot_path.display(),
    );

    Ok(BankSnapshotInfo {
        slot,
        snapshot_path: bank_snapshot_path,
        snapshot_type: BankSnapshotType::Pre,
    })
}

fn serialize_status_cache(
    slot: Slot,
    slot_deltas: &[BankSlotDelta],
    status_cache_path: &Path,
) -> Result<()> {
    let mut status_cache_serialize = Measure::start("status_cache_serialize-ms");
    let consumed_size = serialize_snapshot_data_file(status_cache_path, |stream| {
        serialize_into(stream, slot_deltas)?;
        Ok(())
    })?;
    status_cache_serialize.stop();

    // Monitor sizes because they're capped to MAX_SNAPSHOT_DATA_FILE_SIZE
    datapoint_info!(
        "snapshot-status-cache-file",
        ("slot", slot, i64),
        ("size", consumed_size, i64)
    );

    inc_new_counter_info!(
        "serialize-status-cache-ms",
        status_cache_serialize.as_ms() as usize
    );
    Ok(())
}

/// Remove the snapshot directory for this slot
pub fn remove_bank_snapshot<P>(slot: Slot, bank_snapshots_dir: P) -> Result<()>
where
    P: AsRef<Path>,
{
    let bank_snapshot_dir = get_bank_snapshots_dir(&bank_snapshots_dir, slot);
    fs::remove_dir_all(bank_snapshot_dir)?;
    Ok(())
}

#[derive(Debug, Default)]
pub struct BankFromArchiveTimings {
    pub rebuild_bank_from_snapshots_us: u64,
    pub full_snapshot_untar_us: u64,
    pub incremental_snapshot_untar_us: u64,
    pub verify_snapshot_bank_us: u64,
}

// From testing, 4 seems to be a sweet spot for ranges of 60M-360M accounts and 16-64 cores. This may need to be tuned later.
const PARALLEL_UNTAR_READERS_DEFAULT: usize = 4;

/// Rebuild bank from snapshot archives.  Handles either just a full snapshot, or both a full
/// snapshot and an incremental snapshot.
#[allow(clippy::too_many_arguments)]
pub fn bank_from_snapshot_archives(
    account_paths: &[PathBuf],
    bank_snapshots_dir: impl AsRef<Path>,
    full_snapshot_archive_info: &FullSnapshotArchiveInfo,
    incremental_snapshot_archive_info: Option<&IncrementalSnapshotArchiveInfo>,
    genesis_config: &GenesisConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&Builtins>,
    account_secondary_indexes: AccountSecondaryIndexes,
    accounts_db_caching_enabled: bool,
    limit_load_slot_count_from_snapshot: Option<usize>,
    shrink_ratio: AccountShrinkThreshold,
    test_hash_calculation: bool,
    accounts_db_skip_shrink: bool,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<(Bank, BankFromArchiveTimings)> {
    check_are_snapshots_compatible(
        full_snapshot_archive_info,
        incremental_snapshot_archive_info,
    )?;

    let parallel_divisions = std::cmp::min(
        PARALLEL_UNTAR_READERS_DEFAULT,
        std::cmp::max(1, num_cpus::get() / 4),
    );

    let unarchived_full_snapshot = unarchive_snapshot(
        &bank_snapshots_dir,
        TMP_SNAPSHOT_ARCHIVE_PREFIX,
        full_snapshot_archive_info.path(),
        "snapshot untar",
        account_paths,
        full_snapshot_archive_info.archive_format(),
        parallel_divisions,
    )?;

    let mut unarchived_incremental_snapshot =
        if let Some(incremental_snapshot_archive_info) = incremental_snapshot_archive_info {
            let unarchived_incremental_snapshot = unarchive_snapshot(
                &bank_snapshots_dir,
                TMP_SNAPSHOT_ARCHIVE_PREFIX,
                incremental_snapshot_archive_info.path(),
                "incremental snapshot untar",
                account_paths,
                incremental_snapshot_archive_info.archive_format(),
                parallel_divisions,
            )?;
            Some(unarchived_incremental_snapshot)
        } else {
            None
        };

    let mut unpacked_append_vec_map = unarchived_full_snapshot.unpacked_append_vec_map;
    if let Some(ref mut unarchive_preparation_result) = unarchived_incremental_snapshot {
        let incremental_snapshot_unpacked_append_vec_map =
            std::mem::take(&mut unarchive_preparation_result.unpacked_append_vec_map);
        unpacked_append_vec_map.extend(incremental_snapshot_unpacked_append_vec_map.into_iter());
    }

    let mut measure_rebuild = Measure::start("rebuild bank from snapshots");
    let bank = rebuild_bank_from_snapshots(
        &unarchived_full_snapshot.unpacked_snapshots_dir_and_version,
        unarchived_incremental_snapshot
            .as_ref()
            .map(|unarchive_preparation_result| {
                &unarchive_preparation_result.unpacked_snapshots_dir_and_version
            }),
        account_paths,
        unpacked_append_vec_map,
        genesis_config,
        debug_keys,
        additional_builtins,
        account_secondary_indexes,
        accounts_db_caching_enabled,
        limit_load_slot_count_from_snapshot,
        shrink_ratio,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
    )?;
    measure_rebuild.stop();
    info!("{}", measure_rebuild);

    let mut measure_verify = Measure::start("verify");
    if !bank.verify_snapshot_bank(
        test_hash_calculation,
        accounts_db_skip_shrink || !full_snapshot_archive_info.is_remote(),
        Some(full_snapshot_archive_info.slot()),
    ) && limit_load_slot_count_from_snapshot.is_none()
    {
        panic!("Snapshot bank for slot {} failed to verify", bank.slot());
    }
    measure_verify.stop();

    let timings = BankFromArchiveTimings {
        rebuild_bank_from_snapshots_us: measure_rebuild.as_us(),
        full_snapshot_untar_us: unarchived_full_snapshot.measure_untar.as_us(),
        incremental_snapshot_untar_us: unarchived_incremental_snapshot
            .map_or(0, |unarchive_preparation_result| {
                unarchive_preparation_result.measure_untar.as_us()
            }),
        verify_snapshot_bank_us: measure_verify.as_us(),
    };
    Ok((bank, timings))
}

/// Rebuild bank from snapshot archives.  This function searches `snapshot_archives_dir` for the
/// highest full snapshot and highest corresponding incremental snapshot, then rebuilds the bank.
#[allow(clippy::too_many_arguments)]
pub fn bank_from_latest_snapshot_archives(
    bank_snapshots_dir: impl AsRef<Path>,
    snapshot_archives_dir: impl AsRef<Path>,
    account_paths: &[PathBuf],
    genesis_config: &GenesisConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&Builtins>,
    account_secondary_indexes: AccountSecondaryIndexes,
    accounts_db_caching_enabled: bool,
    limit_load_slot_count_from_snapshot: Option<usize>,
    shrink_ratio: AccountShrinkThreshold,
    test_hash_calculation: bool,
    accounts_db_skip_shrink: bool,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<(
    Bank,
    FullSnapshotArchiveInfo,
    Option<IncrementalSnapshotArchiveInfo>,
)> {
    let full_snapshot_archive_info = get_highest_full_snapshot_archive_info(&snapshot_archives_dir)
        .ok_or(SnapshotError::NoSnapshotArchives)?;

    let incremental_snapshot_archive_info = get_highest_incremental_snapshot_archive_info(
        &snapshot_archives_dir,
        full_snapshot_archive_info.slot(),
    );

    info!(
        "Loading bank from full snapshot: {}, and incremental snapshot: {:?}",
        full_snapshot_archive_info.path().display(),
        incremental_snapshot_archive_info
            .as_ref()
            .map(
                |incremental_snapshot_archive_info| incremental_snapshot_archive_info
                    .path()
                    .display()
            )
    );

    let (bank, timings) = bank_from_snapshot_archives(
        account_paths,
        bank_snapshots_dir.as_ref(),
        &full_snapshot_archive_info,
        incremental_snapshot_archive_info.as_ref(),
        genesis_config,
        debug_keys,
        additional_builtins,
        account_secondary_indexes,
        accounts_db_caching_enabled,
        limit_load_slot_count_from_snapshot,
        shrink_ratio,
        test_hash_calculation,
        accounts_db_skip_shrink,
        verify_index,
        accounts_db_config,
        accounts_update_notifier,
    )?;

    datapoint_info!(
        "bank_from_snapshot_archives",
        (
            "full_snapshot_untar_us",
            timings.full_snapshot_untar_us,
            i64
        ),
        (
            "incremental_snapshot_untar_us",
            timings.incremental_snapshot_untar_us,
            i64
        ),
        (
            "rebuild_bank_from_snapshots_us",
            timings.rebuild_bank_from_snapshots_us,
            i64
        ),
        (
            "verify_snapshot_bank_us",
            timings.verify_snapshot_bank_us,
            i64
        ),
    );

    verify_bank_against_expected_slot_hash(
        &bank,
        incremental_snapshot_archive_info.as_ref().map_or(
            full_snapshot_archive_info.slot(),
            |incremental_snapshot_archive_info| incremental_snapshot_archive_info.slot(),
        ),
        incremental_snapshot_archive_info.as_ref().map_or(
            *full_snapshot_archive_info.hash(),
            |incremental_snapshot_archive_info| *incremental_snapshot_archive_info.hash(),
        ),
    )?;

    Ok((
        bank,
        full_snapshot_archive_info,
        incremental_snapshot_archive_info,
    ))
}

/// Check to make sure the deserialized bank's slot and hash matches the snapshot archive's slot
/// and hash
fn verify_bank_against_expected_slot_hash(
    bank: &Bank,
    expected_slot: Slot,
    expected_hash: Hash,
) -> Result<()> {
    let bank_slot = bank.slot();
    let bank_hash = bank.get_accounts_hash();

    if bank_slot != expected_slot || bank_hash != expected_hash {
        return Err(SnapshotError::MismatchedSlotHash(
            (bank_slot, bank_hash),
            (expected_slot, expected_hash),
        ));
    }

    Ok(())
}

/// Perform the common tasks when unarchiving a snapshot.  Handles creating the temporary
/// directories, untaring, reading the version file, and then returning those fields plus the
/// unpacked append vec map.
fn unarchive_snapshot<P, Q>(
    bank_snapshots_dir: P,
    unpacked_snapshots_dir_prefix: &'static str,
    snapshot_archive_path: Q,
    measure_name: &'static str,
    account_paths: &[PathBuf],
    archive_format: ArchiveFormat,
    parallel_divisions: usize,
) -> Result<UnarchivedSnapshot>
where
    P: AsRef<Path>,
    Q: AsRef<Path>,
{
    let unpack_dir = tempfile::Builder::new()
        .prefix(unpacked_snapshots_dir_prefix)
        .tempdir_in(bank_snapshots_dir)?;
    let unpacked_snapshots_dir = unpack_dir.path().join("snapshots");

    let mut measure_untar = Measure::start(measure_name);
    let unpacked_append_vec_map = untar_snapshot_in(
        snapshot_archive_path,
        unpack_dir.path(),
        account_paths,
        archive_format,
        parallel_divisions,
    )?;
    measure_untar.stop();
    info!("{}", measure_untar);

    let unpacked_version_file = unpack_dir.path().join("version");
    let snapshot_version = snapshot_version_from_file(&unpacked_version_file)?;

    Ok(UnarchivedSnapshot {
        unpack_dir,
        unpacked_append_vec_map,
        unpacked_snapshots_dir_and_version: UnpackedSnapshotsDirAndVersion {
            unpacked_snapshots_dir,
            snapshot_version,
        },
        measure_untar,
    })
}

/// Reads the `snapshot_version` from a file. Before opening the file, its size
/// is compared to `MAX_SNAPSHOT_VERSION_FILE_SIZE`. If the size exceeds this
/// threshold, it is not opened and an error is returned.
fn snapshot_version_from_file(path: impl AsRef<Path>) -> Result<String> {
    // Check file size.
    let file_size = fs::metadata(&path)?.len();
    if file_size > MAX_SNAPSHOT_VERSION_FILE_SIZE {
        let error_message = format!(
            "snapshot version file too large: {} has {} bytes (max size is {} bytes)",
            path.as_ref().display(),
            file_size,
            MAX_SNAPSHOT_VERSION_FILE_SIZE,
        );
        return Err(get_io_error(&error_message));
    }

    // Read snapshot_version from file.
    let mut snapshot_version = String::new();
    File::open(path).and_then(|mut f| f.read_to_string(&mut snapshot_version))?;
    Ok(snapshot_version.trim().to_string())
}

/// Check if an incremental snapshot is compatible with a full snapshot.  This is done by checking
/// if the incremental snapshot's base slot is the same as the full snapshot's slot.
fn check_are_snapshots_compatible(
    full_snapshot_archive_info: &FullSnapshotArchiveInfo,
    incremental_snapshot_archive_info: Option<&IncrementalSnapshotArchiveInfo>,
) -> Result<()> {
    if incremental_snapshot_archive_info.is_none() {
        return Ok(());
    }

    let incremental_snapshot_archive_info = incremental_snapshot_archive_info.unwrap();

    (full_snapshot_archive_info.slot() == incremental_snapshot_archive_info.base_slot())
        .then(|| ())
        .ok_or_else(|| {
            SnapshotError::MismatchedBaseSlot(
                full_snapshot_archive_info.slot(),
                incremental_snapshot_archive_info.base_slot(),
            )
        })
}

/// Get the `&str` from a `&Path`
pub fn path_to_file_name_str(path: &Path) -> Result<&str> {
    path.file_name()
        .ok_or_else(|| SnapshotError::PathToFileNameError(path.to_path_buf()))?
        .to_str()
        .ok_or_else(|| SnapshotError::FileNameToStrError(path.to_path_buf()))
}

pub fn build_snapshot_archives_remote_dir(snapshot_archives_dir: impl AsRef<Path>) -> PathBuf {
    snapshot_archives_dir
        .as_ref()
        .join(SNAPSHOT_ARCHIVE_DOWNLOAD_DIR)
}

/// Build the full snapshot archive path from its components: the snapshot archives directory, the
/// snapshot slot, the accounts hash, and the archive format.
pub fn build_full_snapshot_archive_path(
    snapshot_archives_dir: impl AsRef<Path>,
    slot: Slot,
    hash: &Hash,
    archive_format: ArchiveFormat,
) -> PathBuf {
    snapshot_archives_dir.as_ref().join(format!(
        "snapshot-{}-{}.{}",
        slot,
        hash,
        archive_format.extension(),
    ))
}

/// Build the incremental snapshot archive path from its components: the snapshot archives
/// directory, the snapshot base slot, the snapshot slot, the accounts hash, and the archive
/// format.
pub fn build_incremental_snapshot_archive_path(
    snapshot_archives_dir: impl AsRef<Path>,
    base_slot: Slot,
    slot: Slot,
    hash: &Hash,
    archive_format: ArchiveFormat,
) -> PathBuf {
    snapshot_archives_dir.as_ref().join(format!(
        "incremental-snapshot-{}-{}-{}.{}",
        base_slot,
        slot,
        hash,
        archive_format.extension(),
    ))
}

/// Parse a full snapshot archive filename into its Slot, Hash, and Archive Format
pub(crate) fn parse_full_snapshot_archive_filename(
    archive_filename: &str,
) -> Result<(Slot, Hash, ArchiveFormat)> {
    lazy_static! {
        static ref RE: Regex = Regex::new(FULL_SNAPSHOT_ARCHIVE_FILENAME_REGEX).unwrap();
    }

    let do_parse = || {
        RE.captures(archive_filename).and_then(|captures| {
            let slot = captures
                .name("slot")
                .map(|x| x.as_str().parse::<Slot>())?
                .ok()?;
            let hash = captures
                .name("hash")
                .map(|x| x.as_str().parse::<Hash>())?
                .ok()?;
            let archive_format = captures
                .name("ext")
                .map(|x| x.as_str().parse::<ArchiveFormat>())?
                .ok()?;

            Some((slot, hash, archive_format))
        })
    };

    do_parse().ok_or_else(|| {
        SnapshotError::ParseSnapshotArchiveFileNameError(archive_filename.to_string())
    })
}

/// Parse an incremental snapshot archive filename into its base Slot, actual Slot, Hash, and Archive Format
pub(crate) fn parse_incremental_snapshot_archive_filename(
    archive_filename: &str,
) -> Result<(Slot, Slot, Hash, ArchiveFormat)> {
    lazy_static! {
        static ref RE: Regex = Regex::new(INCREMENTAL_SNAPSHOT_ARCHIVE_FILENAME_REGEX).unwrap();
    }

    let do_parse = || {
        RE.captures(archive_filename).and_then(|captures| {
            let base_slot = captures
                .name("base")
                .map(|x| x.as_str().parse::<Slot>())?
                .ok()?;
            let slot = captures
                .name("slot")
                .map(|x| x.as_str().parse::<Slot>())?
                .ok()?;
            let hash = captures
                .name("hash")
                .map(|x| x.as_str().parse::<Hash>())?
                .ok()?;
            let archive_format = captures
                .name("ext")
                .map(|x| x.as_str().parse::<ArchiveFormat>())?
                .ok()?;

            Some((base_slot, slot, hash, archive_format))
        })
    };

    do_parse().ok_or_else(|| {
        SnapshotError::ParseSnapshotArchiveFileNameError(archive_filename.to_string())
    })
}

/// Walk down the snapshot archive to collect snapshot archive file info
fn get_snapshot_archives<T, F>(snapshot_archives_dir: &Path, cb: F) -> Vec<T>
where
    F: Fn(PathBuf) -> Result<T>,
{
    let walk_dir = |dir: &Path| -> Vec<T> {
        let entry_iter = fs::read_dir(dir);
        match entry_iter {
            Err(err) => {
                info!(
                    "Unable to read snapshot archives directory: err: {}, path: {}",
                    err,
                    dir.display()
                );
                vec![]
            }
            Ok(entries) => entries
                .filter_map(|entry| entry.map_or(None, |entry| cb(entry.path()).ok()))
                .collect(),
        }
    };

    let mut ret = walk_dir(snapshot_archives_dir);
    let remote_dir = build_snapshot_archives_remote_dir(snapshot_archives_dir);
    if remote_dir.exists() {
        ret.append(&mut walk_dir(remote_dir.as_ref()));
    }
    ret
}

/// Get a list of the full snapshot archives from a directory
pub fn get_full_snapshot_archives<P>(snapshot_archives_dir: P) -> Vec<FullSnapshotArchiveInfo>
where
    P: AsRef<Path>,
{
    get_snapshot_archives(
        snapshot_archives_dir.as_ref(),
        FullSnapshotArchiveInfo::new_from_path,
    )
}

/// Get a list of the incremental snapshot archives from a directory
pub fn get_incremental_snapshot_archives<P>(
    snapshot_archives_dir: P,
) -> Vec<IncrementalSnapshotArchiveInfo>
where
    P: AsRef<Path>,
{
    get_snapshot_archives(
        snapshot_archives_dir.as_ref(),
        IncrementalSnapshotArchiveInfo::new_from_path,
    )
}

/// Get the highest slot of the full snapshot archives in a directory
pub fn get_highest_full_snapshot_archive_slot<P>(snapshot_archives_dir: P) -> Option<Slot>
where
    P: AsRef<Path>,
{
    get_highest_full_snapshot_archive_info(snapshot_archives_dir)
        .map(|full_snapshot_archive_info| full_snapshot_archive_info.slot())
}

/// Get the highest slot of the incremental snapshot archives in a directory, for a given full
/// snapshot slot
pub fn get_highest_incremental_snapshot_archive_slot<P: AsRef<Path>>(
    snapshot_archives_dir: P,
    full_snapshot_slot: Slot,
) -> Option<Slot> {
    get_highest_incremental_snapshot_archive_info(snapshot_archives_dir, full_snapshot_slot)
        .map(|incremental_snapshot_archive_info| incremental_snapshot_archive_info.slot())
}

/// Get the path (and metadata) for the full snapshot archive with the highest slot in a directory
pub fn get_highest_full_snapshot_archive_info<P>(
    snapshot_archives_dir: P,
) -> Option<FullSnapshotArchiveInfo>
where
    P: AsRef<Path>,
{
    let mut full_snapshot_archives = get_full_snapshot_archives(snapshot_archives_dir);
    full_snapshot_archives.sort_unstable();
    full_snapshot_archives.into_iter().rev().next()
}

/// Get the path for the incremental snapshot archive with the highest slot, for a given full
/// snapshot slot, in a directory
pub fn get_highest_incremental_snapshot_archive_info<P>(
    snapshot_archives_dir: P,
    full_snapshot_slot: Slot,
) -> Option<IncrementalSnapshotArchiveInfo>
where
    P: AsRef<Path>,
{
    // Since we want to filter down to only the incremental snapshot archives that have the same
    // full snapshot slot as the value passed in, perform the filtering before sorting to avoid
    // doing unnecessary work.
    let mut incremental_snapshot_archives =
        get_incremental_snapshot_archives(snapshot_archives_dir)
            .into_iter()
            .filter(|incremental_snapshot_archive_info| {
                incremental_snapshot_archive_info.base_slot() == full_snapshot_slot
            })
            .collect::<Vec<_>>();
    incremental_snapshot_archives.sort_unstable();
    incremental_snapshot_archives.into_iter().rev().next()
}

pub fn purge_old_snapshot_archives<P>(
    snapshot_archives_dir: P,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) where
    P: AsRef<Path>,
{
    info!(
        "Purging old snapshot archives in {}, retaining up to {} full snapshots and up to {} incremental snapshots",
        snapshot_archives_dir.as_ref().display(),
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain
    );

    let mut full_snapshot_archives = get_full_snapshot_archives(&snapshot_archives_dir);
    full_snapshot_archives.sort_unstable();
    full_snapshot_archives.reverse();

    let num_to_retain = full_snapshot_archives.len().min(
        maximum_full_snapshot_archives_to_retain
            .max(1 /* Always keep at least one full snapshot */),
    );
    trace!(
        "There are {} full snapshot archives, retaining {}",
        full_snapshot_archives.len(),
        num_to_retain,
    );

    let (full_snapshot_archives_to_retain, full_snapshot_archives_to_remove) =
        if full_snapshot_archives.is_empty() {
            None
        } else {
            Some(full_snapshot_archives.split_at(num_to_retain))
        }
        .unwrap_or_default();

    let retained_full_snapshot_slots = full_snapshot_archives_to_retain
        .iter()
        .map(|ai| ai.slot())
        .collect::<HashSet<_>>();

    fn remove_archives<T: SnapshotArchiveInfoGetter>(archives: &[T]) {
        for path in archives.iter().map(|a| a.path()) {
            trace!("Removing snapshot archive: {}", path.display());
            fs::remove_file(path)
                .unwrap_or_else(|err| info!("Failed to remove {}: {}", path.display(), err));
        }
    }
    remove_archives(full_snapshot_archives_to_remove);

    let mut incremental_snapshot_archives_by_base_slot = HashMap::<Slot, Vec<_>>::new();
    for incremental_snapshot_archive in get_incremental_snapshot_archives(&snapshot_archives_dir) {
        incremental_snapshot_archives_by_base_slot
            .entry(incremental_snapshot_archive.base_slot())
            .or_default()
            .push(incremental_snapshot_archive)
    }

    let highest_full_snapshot_slot = retained_full_snapshot_slots.iter().max().copied();
    for (base_slot, mut incremental_snapshot_archives) in incremental_snapshot_archives_by_base_slot
    {
        incremental_snapshot_archives.sort_unstable();
        let num_to_retain = if Some(base_slot) == highest_full_snapshot_slot {
            maximum_incremental_snapshot_archives_to_retain
        } else if retained_full_snapshot_slots.contains(&base_slot) {
            1
        } else {
            0
        };
        trace!(
            "There are {} incremental snapshot archives for base slot {}, removing {} of them",
            incremental_snapshot_archives.len(),
            base_slot,
            incremental_snapshot_archives
                .len()
                .saturating_sub(num_to_retain),
        );

        incremental_snapshot_archives.truncate(
            incremental_snapshot_archives
                .len()
                .saturating_sub(num_to_retain),
        );
        remove_archives(&incremental_snapshot_archives);
    }
}

fn unpack_snapshot_local<T: 'static + Read + std::marker::Send, F: Fn() -> T>(
    reader: F,
    ledger_dir: &Path,
    account_paths: &[PathBuf],
    parallel_archivers: usize,
) -> Result<UnpackedAppendVecMap> {
    assert!(parallel_archivers > 0);
    // a shared 'reader' that reads the decompressed stream once, keeps some history, and acts as a reader for multiple parallel archive readers
    let shared_buffer = SharedBuffer::new(reader());

    // allocate all readers before any readers start reading
    let readers = (0..parallel_archivers)
        .into_iter()
        .map(|_| SharedBufferReader::new(&shared_buffer))
        .collect::<Vec<_>>();

    // create 'parallel_archivers' # of parallel workers, each responsible for 1/parallel_archivers of all the files to extract.
    let all_unpacked_append_vec_map = readers
        .into_par_iter()
        .enumerate()
        .map(|(index, reader)| {
            let parallel_selector = Some(ParallelSelector {
                index,
                divisions: parallel_archivers,
            });
            let mut archive = Archive::new(reader);
            unpack_snapshot(&mut archive, ledger_dir, account_paths, parallel_selector)
        })
        .collect::<Vec<_>>();
    let mut unpacked_append_vec_map = UnpackedAppendVecMap::new();
    for h in all_unpacked_append_vec_map {
        unpacked_append_vec_map.extend(h?);
    }

    Ok(unpacked_append_vec_map)
}

fn untar_snapshot_in<P: AsRef<Path>>(
    snapshot_tar: P,
    unpack_dir: &Path,
    account_paths: &[PathBuf],
    archive_format: ArchiveFormat,
    parallel_divisions: usize,
) -> Result<UnpackedAppendVecMap> {
    let open_file = || File::open(&snapshot_tar).unwrap();
    let account_paths_map = match archive_format {
        ArchiveFormat::TarBzip2 => unpack_snapshot_local(
            || BzDecoder::new(BufReader::new(open_file())),
            unpack_dir,
            account_paths,
            parallel_divisions,
        )?,
        ArchiveFormat::TarGzip => unpack_snapshot_local(
            || GzDecoder::new(BufReader::new(open_file())),
            unpack_dir,
            account_paths,
            parallel_divisions,
        )?,
        ArchiveFormat::TarZstd => unpack_snapshot_local(
            || zstd::stream::read::Decoder::new(BufReader::new(open_file())).unwrap(),
            unpack_dir,
            account_paths,
            parallel_divisions,
        )?,
        ArchiveFormat::Tar => unpack_snapshot_local(
            || BufReader::new(open_file()),
            unpack_dir,
            account_paths,
            parallel_divisions,
        )?,
    };
    Ok(account_paths_map)
}

fn verify_unpacked_snapshots_dir_and_version(
    unpacked_snapshots_dir_and_version: &UnpackedSnapshotsDirAndVersion,
) -> Result<(SnapshotVersion, BankSnapshotInfo)> {
    info!(
        "snapshot version: {}",
        &unpacked_snapshots_dir_and_version.snapshot_version
    );

    let snapshot_version =
        SnapshotVersion::maybe_from_string(&unpacked_snapshots_dir_and_version.snapshot_version)
            .ok_or_else(|| {
                get_io_error(&format!(
                    "unsupported snapshot version: {}",
                    &unpacked_snapshots_dir_and_version.snapshot_version,
                ))
            })?;
    let mut bank_snapshots =
        get_bank_snapshots_post(&unpacked_snapshots_dir_and_version.unpacked_snapshots_dir);
    if bank_snapshots.len() > 1 {
        return Err(get_io_error("invalid snapshot format"));
    }
    bank_snapshots.sort_unstable();
    let root_paths = bank_snapshots
        .pop()
        .ok_or_else(|| get_io_error("No snapshots found in snapshots directory"))?;
    Ok((snapshot_version, root_paths))
}

#[allow(clippy::too_many_arguments)]
fn rebuild_bank_from_snapshots(
    full_snapshot_unpacked_snapshots_dir_and_version: &UnpackedSnapshotsDirAndVersion,
    incremental_snapshot_unpacked_snapshots_dir_and_version: Option<
        &UnpackedSnapshotsDirAndVersion,
    >,
    account_paths: &[PathBuf],
    unpacked_append_vec_map: UnpackedAppendVecMap,
    genesis_config: &GenesisConfig,
    debug_keys: Option<Arc<HashSet<Pubkey>>>,
    additional_builtins: Option<&Builtins>,
    account_secondary_indexes: AccountSecondaryIndexes,
    accounts_db_caching_enabled: bool,
    limit_load_slot_count_from_snapshot: Option<usize>,
    shrink_ratio: AccountShrinkThreshold,
    verify_index: bool,
    accounts_db_config: Option<AccountsDbConfig>,
    accounts_update_notifier: Option<AccountsUpdateNotifier>,
) -> Result<Bank> {
    let (full_snapshot_version, full_snapshot_root_paths) =
        verify_unpacked_snapshots_dir_and_version(
            full_snapshot_unpacked_snapshots_dir_and_version,
        )?;
    let (incremental_snapshot_version, incremental_snapshot_root_paths) =
        if let Some(snapshot_unpacked_snapshots_dir_and_version) =
            incremental_snapshot_unpacked_snapshots_dir_and_version
        {
            let (snapshot_version, bank_snapshot_info) = verify_unpacked_snapshots_dir_and_version(
                snapshot_unpacked_snapshots_dir_and_version,
            )?;
            (Some(snapshot_version), Some(bank_snapshot_info))
        } else {
            (None, None)
        };
    info!(
        "Loading bank from full snapshot {} and incremental snapshot {:?}",
        full_snapshot_root_paths.snapshot_path.display(),
        incremental_snapshot_root_paths
            .as_ref()
            .map(|paths| paths.snapshot_path.display()),
    );

    let snapshot_root_paths = SnapshotRootPaths {
        full_snapshot_root_file_path: full_snapshot_root_paths.snapshot_path,
        incremental_snapshot_root_file_path: incremental_snapshot_root_paths
            .map(|root_paths| root_paths.snapshot_path),
    };

    let bank = deserialize_snapshot_data_files(&snapshot_root_paths, |snapshot_streams| {
        Ok(
            match incremental_snapshot_version.unwrap_or(full_snapshot_version) {
                SnapshotVersion::V1_2_0 => bank_from_streams(
                    SerdeStyle::Newer,
                    snapshot_streams,
                    account_paths,
                    unpacked_append_vec_map,
                    genesis_config,
                    debug_keys,
                    additional_builtins,
                    account_secondary_indexes,
                    accounts_db_caching_enabled,
                    limit_load_slot_count_from_snapshot,
                    shrink_ratio,
                    verify_index,
                    accounts_db_config,
                    accounts_update_notifier,
                ),
            }?,
        )
    })?;

    // The status cache is rebuilt from the latest snapshot.  So, if there's an incremental
    // snapshot, use that.  Otherwise use the full snapshot.
    let status_cache_path = incremental_snapshot_unpacked_snapshots_dir_and_version
        .map_or_else(
            || {
                full_snapshot_unpacked_snapshots_dir_and_version
                    .unpacked_snapshots_dir
                    .as_path()
            },
            |unpacked_snapshots_dir_and_version| {
                unpacked_snapshots_dir_and_version
                    .unpacked_snapshots_dir
                    .as_path()
            },
        )
        .join(SNAPSHOT_STATUS_CACHE_FILENAME);
    let slot_deltas = deserialize_snapshot_data_file(&status_cache_path, |stream| {
        info!(
            "Rebuilding status cache from {}",
            status_cache_path.display()
        );
        let slot_deltas: Vec<BankSlotDelta> = bincode::options()
            .with_limit(MAX_SNAPSHOT_DATA_FILE_SIZE)
            .with_fixint_encoding()
            .allow_trailing_bytes()
            .deserialize_from(stream)?;
        Ok(slot_deltas)
    })?;

    bank.src.append(&slot_deltas);

    bank.prepare_rewrites_for_hash();

    info!("Loaded bank for slot: {}", bank.slot());
    Ok(bank)
}

pub(crate) fn get_snapshot_file_name(slot: Slot) -> String {
    slot.to_string()
}

pub(crate) fn get_bank_snapshots_dir<P: AsRef<Path>>(path: P, slot: Slot) -> PathBuf {
    path.as_ref().join(slot.to_string())
}

fn get_io_error(error: &str) -> SnapshotError {
    warn!("Snapshot Error: {:?}", error);
    SnapshotError::Io(IoError::new(ErrorKind::Other, error))
}

#[derive(Debug, Copy, Clone)]
/// allow tests to specify what happened to the serialized format
pub enum VerifyBank {
    /// the bank's serialized format is expected to be identical to what we are comparing against
    Deterministic,
    /// the serialized bank was 'reserialized' into a non-deterministic format at the specified slot
    /// so, deserialize both files and compare deserialized results
    NonDeterministic(Slot),
}

pub fn verify_snapshot_archive<P, Q, R>(
    snapshot_archive: P,
    snapshots_to_verify: Q,
    storages_to_verify: R,
    archive_format: ArchiveFormat,
    verify_bank: VerifyBank,
) where
    P: AsRef<Path>,
    Q: AsRef<Path>,
    R: AsRef<Path>,
{
    let temp_dir = tempfile::TempDir::new().unwrap();
    let unpack_dir = temp_dir.path();
    untar_snapshot_in(
        snapshot_archive,
        unpack_dir,
        &[unpack_dir.to_path_buf()],
        archive_format,
        1,
    )
    .unwrap();

    // Check snapshots are the same
    let unpacked_snapshots = unpack_dir.join("snapshots");
    if let VerifyBank::NonDeterministic(slot) = verify_bank {
        // file contents may be different, but deserialized structs should be equal
        let slot = slot.to_string();
        let p1 = snapshots_to_verify.as_ref().join(&slot).join(&slot);
        let p2 = unpacked_snapshots.join(&slot).join(&slot);

        assert!(crate::serde_snapshot::compare_two_serialized_banks(&p1, &p2).unwrap());
        std::fs::remove_file(p1).unwrap();
        std::fs::remove_file(p2).unwrap();
    }

    assert!(!dir_diff::is_different(&snapshots_to_verify, unpacked_snapshots).unwrap());

    // Check the account entries are the same
    let unpacked_accounts = unpack_dir.join("accounts");
    assert!(!dir_diff::is_different(&storages_to_verify, unpacked_accounts).unwrap());
}

/// Remove outdated bank snapshots
pub fn purge_old_bank_snapshots(bank_snapshots_dir: impl AsRef<Path>) {
    let do_purge = |mut bank_snapshots: Vec<BankSnapshotInfo>| {
        bank_snapshots.sort_unstable();
        bank_snapshots
            .into_iter()
            .rev()
            .skip(MAX_BANK_SNAPSHOTS_TO_RETAIN)
            .for_each(|bank_snapshot| {
                let r = remove_bank_snapshot(bank_snapshot.slot, &bank_snapshots_dir);
                if r.is_err() {
                    warn!(
                        "Couldn't remove bank snapshot at: {}",
                        bank_snapshot.snapshot_path.display()
                    );
                }
            })
    };

    do_purge(get_bank_snapshots_pre(&bank_snapshots_dir));
    do_purge(get_bank_snapshots_post(&bank_snapshots_dir));
}

/// Gather the necessary elements for a snapshot of the given `root_bank`.
///
/// **DEVELOPER NOTE** Any error that is returned from this function may bring down the node!  This
/// function is called from AccountsBackgroundService to handle snapshot requests.  Since taking a
/// snapshot is not permitted to fail, any errors returned here will trigger the node to shutdown.
/// So, be careful whenever adding new code that may return errors.
pub fn snapshot_bank(
    root_bank: &Bank,
    status_cache_slot_deltas: Vec<BankSlotDelta>,
    pending_accounts_package: &PendingAccountsPackage,
    bank_snapshots_dir: impl AsRef<Path>,
    snapshot_archives_dir: impl AsRef<Path>,
    snapshot_version: SnapshotVersion,
    archive_format: ArchiveFormat,
    hash_for_testing: Option<Hash>,
    snapshot_type: Option<SnapshotType>,
) -> Result<()> {
    let snapshot_storages = get_snapshot_storages(root_bank);

    let mut add_snapshot_time = Measure::start("add-snapshot-ms");
    let bank_snapshot_info = add_bank_snapshot(
        &bank_snapshots_dir,
        root_bank,
        &snapshot_storages,
        snapshot_version,
    )?;
    add_snapshot_time.stop();
    inc_new_counter_info!("add-snapshot-ms", add_snapshot_time.as_ms() as usize);

    let accounts_package = AccountsPackage::new(
        root_bank,
        &bank_snapshot_info,
        bank_snapshots_dir,
        status_cache_slot_deltas,
        snapshot_archives_dir,
        snapshot_storages,
        archive_format,
        snapshot_version,
        hash_for_testing,
        snapshot_type,
    )
    .expect("failed to hard link bank snapshot into a tmpdir");

    if can_submit_accounts_package(&accounts_package, pending_accounts_package) {
        let old_accounts_package = pending_accounts_package
            .lock()
            .unwrap()
            .replace(accounts_package);
        if let Some(old_accounts_package) = old_accounts_package {
            debug!(
                "The pending AccountsPackage has been overwritten: \
                \nNew AccountsPackage slot: {}, snapshot type: {:?} \
                \nOld AccountsPackage slot: {}, snapshot type: {:?}",
                root_bank.slot(),
                snapshot_type,
                old_accounts_package.slot,
                old_accounts_package.snapshot_type,
            );
        }
    }

    Ok(())
}

/// Get the snapshot storages for this bank
fn get_snapshot_storages(bank: &Bank) -> SnapshotStorages {
    let mut measure_snapshot_storages = Measure::start("snapshot-storages");
    let snapshot_storages = bank.get_snapshot_storages(None);
    measure_snapshot_storages.stop();
    let snapshot_storages_count = snapshot_storages.iter().map(Vec::len).sum::<usize>();
    datapoint_info!(
        "get_snapshot_storages",
        ("snapshot-storages-count", snapshot_storages_count, i64),
        (
            "snapshot-storages-time-ms",
            measure_snapshot_storages.as_ms(),
            i64
        ),
    );

    snapshot_storages
}

/// Convenience function to create a full snapshot archive out of any Bank, regardless of state.
/// The Bank will be frozen during the process.
///
/// Requires:
///     - `bank` is complete
pub fn bank_to_full_snapshot_archive(
    bank_snapshots_dir: impl AsRef<Path>,
    bank: &Bank,
    snapshot_version: Option<SnapshotVersion>,
    snapshot_archives_dir: impl AsRef<Path>,
    archive_format: ArchiveFormat,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) -> Result<FullSnapshotArchiveInfo> {
    let snapshot_version = snapshot_version.unwrap_or_default();

    assert!(bank.is_complete());
    bank.squash(); // Bank may not be a root
    bank.force_flush_accounts_cache();
    bank.clean_accounts(true, false, Some(bank.slot()));
    bank.update_accounts_hash();
    bank.rehash(); // Bank accounts may have been manually modified by the caller

    let temp_dir = tempfile::tempdir_in(bank_snapshots_dir)?;
    let snapshot_storages = bank.get_snapshot_storages(None);
    let bank_snapshot_info =
        add_bank_snapshot(&temp_dir, bank, &snapshot_storages, snapshot_version)?;

    package_and_archive_full_snapshot(
        bank,
        &bank_snapshot_info,
        &temp_dir,
        snapshot_archives_dir,
        snapshot_storages,
        archive_format,
        snapshot_version,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    )
}

/// Convenience function to create an incremental snapshot archive out of any Bank, regardless of
/// state.  The Bank will be frozen during the process.
///
/// Requires:
///     - `bank` is complete
///     - `bank`'s slot is greater than `full_snapshot_slot`
pub fn bank_to_incremental_snapshot_archive(
    bank_snapshots_dir: impl AsRef<Path>,
    bank: &Bank,
    full_snapshot_slot: Slot,
    snapshot_version: Option<SnapshotVersion>,
    snapshot_archives_dir: impl AsRef<Path>,
    archive_format: ArchiveFormat,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) -> Result<IncrementalSnapshotArchiveInfo> {
    let snapshot_version = snapshot_version.unwrap_or_default();

    assert!(bank.is_complete());
    assert!(bank.slot() > full_snapshot_slot);
    bank.squash(); // Bank may not be a root
    bank.force_flush_accounts_cache();
    bank.clean_accounts(true, false, Some(full_snapshot_slot));
    bank.update_accounts_hash();
    bank.rehash(); // Bank accounts may have been manually modified by the caller

    let temp_dir = tempfile::tempdir_in(bank_snapshots_dir)?;
    let snapshot_storages = bank.get_snapshot_storages(Some(full_snapshot_slot));
    let bank_snapshot_info =
        add_bank_snapshot(&temp_dir, bank, &snapshot_storages, snapshot_version)?;

    package_and_archive_incremental_snapshot(
        bank,
        full_snapshot_slot,
        &bank_snapshot_info,
        &temp_dir,
        snapshot_archives_dir,
        snapshot_storages,
        archive_format,
        snapshot_version,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    )
}

/// Helper function to hold shared code to package, process, and archive full snapshots
pub fn package_and_archive_full_snapshot(
    bank: &Bank,
    bank_snapshot_info: &BankSnapshotInfo,
    bank_snapshots_dir: impl AsRef<Path>,
    snapshot_archives_dir: impl AsRef<Path>,
    snapshot_storages: SnapshotStorages,
    archive_format: ArchiveFormat,
    snapshot_version: SnapshotVersion,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) -> Result<FullSnapshotArchiveInfo> {
    let accounts_package = AccountsPackage::new(
        bank,
        bank_snapshot_info,
        bank_snapshots_dir,
        bank.src.slot_deltas(&bank.src.roots()),
        snapshot_archives_dir,
        snapshot_storages,
        archive_format,
        snapshot_version,
        None,
        Some(SnapshotType::FullSnapshot),
    )?;

    crate::serde_snapshot::reserialize_bank_with_new_accounts_hash(
        accounts_package.snapshot_links.path(),
        accounts_package.slot,
        &bank.get_accounts_hash(),
    );

    let snapshot_package = SnapshotPackage::new(accounts_package, bank.get_accounts_hash());
    archive_snapshot_package(
        &snapshot_package,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    )?;

    Ok(FullSnapshotArchiveInfo::new(
        snapshot_package.snapshot_archive_info,
    ))
}

/// Helper function to hold shared code to package, process, and archive incremental snapshots
#[allow(clippy::too_many_arguments)]
pub fn package_and_archive_incremental_snapshot(
    bank: &Bank,
    incremental_snapshot_base_slot: Slot,
    bank_snapshot_info: &BankSnapshotInfo,
    bank_snapshots_dir: impl AsRef<Path>,
    snapshot_archives_dir: impl AsRef<Path>,
    snapshot_storages: SnapshotStorages,
    archive_format: ArchiveFormat,
    snapshot_version: SnapshotVersion,
    maximum_full_snapshot_archives_to_retain: usize,
    maximum_incremental_snapshot_archives_to_retain: usize,
) -> Result<IncrementalSnapshotArchiveInfo> {
    let accounts_package = AccountsPackage::new(
        bank,
        bank_snapshot_info,
        bank_snapshots_dir,
        bank.src.slot_deltas(&bank.src.roots()),
        snapshot_archives_dir,
        snapshot_storages,
        archive_format,
        snapshot_version,
        None,
        Some(SnapshotType::IncrementalSnapshot(
            incremental_snapshot_base_slot,
        )),
    )?;

    crate::serde_snapshot::reserialize_bank_with_new_accounts_hash(
        accounts_package.snapshot_links.path(),
        accounts_package.slot,
        &bank.get_accounts_hash(),
    );

    let snapshot_package = SnapshotPackage::new(accounts_package, bank.get_accounts_hash());
    archive_snapshot_package(
        &snapshot_package,
        maximum_full_snapshot_archives_to_retain,
        maximum_incremental_snapshot_archives_to_retain,
    )?;

    Ok(IncrementalSnapshotArchiveInfo::new(
        incremental_snapshot_base_slot,
        snapshot_package.snapshot_archive_info,
    ))
}

pub fn should_take_full_snapshot(
    block_height: Slot,
    full_snapshot_archive_interval_slots: Slot,
) -> bool {
    block_height % full_snapshot_archive_interval_slots == 0
}

pub fn should_take_incremental_snapshot(
    block_height: Slot,
    incremental_snapshot_archive_interval_slots: Slot,
    last_full_snapshot_slot: Option<Slot>,
) -> bool {
    block_height % incremental_snapshot_archive_interval_slots == 0
        && last_full_snapshot_slot.is_some()
}

/// Decide if an accounts package can be submitted to the PendingAccountsPackage
///
/// This is based on the values for `snapshot_type` in both the `accounts_package` and the
/// `pending_accounts_package`:
/// - if the new AccountsPackage is for a full snapshot, always submit
/// - if the new AccountsPackage is for an incremental snapshot, submit as long as there isn't a
///   pending full snapshot
/// - otherwise, only submit the new AccountsPackage as long as there's not a pending package
///   destined for a snapshot archive
fn can_submit_accounts_package(
    accounts_package: &AccountsPackage,
    pending_accounts_package: &PendingAccountsPackage,
) -> bool {
    match accounts_package.snapshot_type {
        Some(SnapshotType::FullSnapshot) => true,
        Some(SnapshotType::IncrementalSnapshot(_)) => pending_accounts_package
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|old_accounts_package| old_accounts_package.snapshot_type)
            .map(|old_snapshot_type| !old_snapshot_type.is_full_snapshot())
            .unwrap_or(true),
        None => pending_accounts_package
            .lock()
            .unwrap()
            .as_ref()
            .map(|old_accounts_package| old_accounts_package.snapshot_type.is_none())
            .unwrap_or(true),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::accounts_db::ACCOUNTS_DB_CONFIG_FOR_TESTING,
        assert_matches::assert_matches,
        bincode::{deserialize_from, serialize_into},
        solana_sdk::{
            genesis_config::create_genesis_config,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::SanitizedTransaction,
        },
        std::{convert::TryFrom, mem::size_of},
        tempfile::NamedTempFile,
    };

    #[test]
    fn test_serialize_snapshot_data_file_under_limit() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let expected_consumed_size = size_of::<u32>() as u64;
        let consumed_size = serialize_snapshot_data_file_capped(
            &temp_dir.path().join("data-file"),
            expected_consumed_size,
            |stream| {
                serialize_into(stream, &2323_u32)?;
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(consumed_size, expected_consumed_size);
    }

    #[test]
    fn test_serialize_snapshot_data_file_over_limit() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let expected_consumed_size = size_of::<u32>() as u64;
        let result = serialize_snapshot_data_file_capped(
            &temp_dir.path().join("data-file"),
            expected_consumed_size - 1,
            |stream| {
                serialize_into(stream, &2323_u32)?;
                Ok(())
            },
        );
        assert_matches!(result, Err(SnapshotError::Io(ref message)) if message.to_string().starts_with("too large snapshot data file to serialize"));
    }

    #[test]
    fn test_deserialize_snapshot_data_file_under_limit() {
        let expected_data = 2323_u32;
        let expected_consumed_size = size_of::<u32>() as u64;

        let temp_dir = tempfile::TempDir::new().unwrap();
        serialize_snapshot_data_file_capped(
            &temp_dir.path().join("data-file"),
            expected_consumed_size,
            |stream| {
                serialize_into(stream, &expected_data)?;
                Ok(())
            },
        )
        .unwrap();

        let snapshot_root_paths = SnapshotRootPaths {
            full_snapshot_root_file_path: temp_dir.path().join("data-file"),
            incremental_snapshot_root_file_path: None,
        };

        let actual_data = deserialize_snapshot_data_files_capped(
            &snapshot_root_paths,
            expected_consumed_size,
            |stream| {
                Ok(deserialize_from::<_, u32>(
                    &mut stream.full_snapshot_stream,
                )?)
            },
        )
        .unwrap();
        assert_eq!(actual_data, expected_data);
    }

    #[test]
    fn test_deserialize_snapshot_data_file_over_limit() {
        let expected_data = 2323_u32;
        let expected_consumed_size = size_of::<u32>() as u64;

        let temp_dir = tempfile::TempDir::new().unwrap();
        serialize_snapshot_data_file_capped(
            &temp_dir.path().join("data-file"),
            expected_consumed_size,
            |stream| {
                serialize_into(stream, &expected_data)?;
                Ok(())
            },
        )
        .unwrap();

        let snapshot_root_paths = SnapshotRootPaths {
            full_snapshot_root_file_path: temp_dir.path().join("data-file"),
            incremental_snapshot_root_file_path: None,
        };

        let result = deserialize_snapshot_data_files_capped(
            &snapshot_root_paths,
            expected_consumed_size - 1,
            |stream| {
                Ok(deserialize_from::<_, u32>(
                    &mut stream.full_snapshot_stream,
                )?)
            },
        );
        assert_matches!(result, Err(SnapshotError::Io(ref message)) if message.to_string().starts_with("too large snapshot data file to deserialize"));
    }

    #[test]
    fn test_deserialize_snapshot_data_file_extra_data() {
        let expected_data = 2323_u32;
        let expected_consumed_size = size_of::<u32>() as u64;

        let temp_dir = tempfile::TempDir::new().unwrap();
        serialize_snapshot_data_file_capped(
            &temp_dir.path().join("data-file"),
            expected_consumed_size * 2,
            |stream| {
                serialize_into(stream.by_ref(), &expected_data)?;
                serialize_into(stream.by_ref(), &expected_data)?;
                Ok(())
            },
        )
        .unwrap();

        let snapshot_root_paths = SnapshotRootPaths {
            full_snapshot_root_file_path: temp_dir.path().join("data-file"),
            incremental_snapshot_root_file_path: None,
        };

        let result = deserialize_snapshot_data_files_capped(
            &snapshot_root_paths,
            expected_consumed_size * 2,
            |stream| {
                Ok(deserialize_from::<_, u32>(
                    &mut stream.full_snapshot_stream,
                )?)
            },
        );
        assert_matches!(result, Err(SnapshotError::Io(ref message)) if message.to_string().starts_with("invalid snapshot data file"));
    }

    #[test]
    fn test_snapshot_version_from_file_under_limit() {
        let file_content = SnapshotVersion::default().as_str();
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(file_content.as_bytes()).unwrap();
        let version_from_file = snapshot_version_from_file(file.path()).unwrap();
        assert_eq!(version_from_file, file_content);
    }

    #[test]
    fn test_snapshot_version_from_file_over_limit() {
        let over_limit_size = usize::try_from(MAX_SNAPSHOT_VERSION_FILE_SIZE + 1).unwrap();
        let file_content = vec![7u8; over_limit_size];
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(&file_content).unwrap();
        assert_matches!(
            snapshot_version_from_file(file.path()),
            Err(SnapshotError::Io(ref message)) if message.to_string().starts_with("snapshot version file too large")
        );
    }

    #[test]
    fn test_parse_full_snapshot_archive_filename() {
        assert_eq!(
            parse_full_snapshot_archive_filename(&format!(
                "snapshot-42-{}.tar.bz2",
                Hash::default()
            ))
            .unwrap(),
            (42, Hash::default(), ArchiveFormat::TarBzip2)
        );
        assert_eq!(
            parse_full_snapshot_archive_filename(&format!(
                "snapshot-43-{}.tar.zst",
                Hash::default()
            ))
            .unwrap(),
            (43, Hash::default(), ArchiveFormat::TarZstd)
        );
        assert_eq!(
            parse_full_snapshot_archive_filename(&format!("snapshot-44-{}.tar", Hash::default()))
                .unwrap(),
            (44, Hash::default(), ArchiveFormat::Tar)
        );

        assert!(parse_full_snapshot_archive_filename("invalid").is_err());
        assert!(
            parse_full_snapshot_archive_filename("snapshot-bad!slot-bad!hash.bad!ext").is_err()
        );

        assert!(
            parse_full_snapshot_archive_filename("snapshot-12345678-bad!hash.bad!ext").is_err()
        );
        assert!(parse_full_snapshot_archive_filename(&format!(
            "snapshot-12345678-{}.bad!ext",
            Hash::new_unique()
        ))
        .is_err());
        assert!(parse_full_snapshot_archive_filename("snapshot-12345678-bad!hash.tar").is_err());

        assert!(parse_full_snapshot_archive_filename(&format!(
            "snapshot-bad!slot-{}.bad!ext",
            Hash::new_unique()
        ))
        .is_err());
        assert!(parse_full_snapshot_archive_filename(&format!(
            "snapshot-12345678-{}.bad!ext",
            Hash::new_unique()
        ))
        .is_err());
        assert!(parse_full_snapshot_archive_filename(&format!(
            "snapshot-bad!slot-{}.tar",
            Hash::new_unique()
        ))
        .is_err());

        assert!(parse_full_snapshot_archive_filename("snapshot-bad!slot-bad!hash.tar").is_err());
        assert!(parse_full_snapshot_archive_filename("snapshot-12345678-bad!hash.tar").is_err());
        assert!(parse_full_snapshot_archive_filename(&format!(
            "snapshot-bad!slot-{}.tar",
            Hash::new_unique()
        ))
        .is_err());
    }

    #[test]
    fn test_parse_incremental_snapshot_archive_filename() {
        solana_logger::setup();
        assert_eq!(
            parse_incremental_snapshot_archive_filename(&format!(
                "incremental-snapshot-42-123-{}.tar.bz2",
                Hash::default()
            ))
            .unwrap(),
            (42, 123, Hash::default(), ArchiveFormat::TarBzip2)
        );
        assert_eq!(
            parse_incremental_snapshot_archive_filename(&format!(
                "incremental-snapshot-43-234-{}.tar.zst",
                Hash::default()
            ))
            .unwrap(),
            (43, 234, Hash::default(), ArchiveFormat::TarZstd)
        );
        assert_eq!(
            parse_incremental_snapshot_archive_filename(&format!(
                "incremental-snapshot-44-345-{}.tar",
                Hash::default()
            ))
            .unwrap(),
            (44, 345, Hash::default(), ArchiveFormat::Tar)
        );

        assert!(parse_incremental_snapshot_archive_filename("invalid").is_err());
        assert!(parse_incremental_snapshot_archive_filename(&format!(
            "snapshot-42-{}.tar",
            Hash::new_unique()
        ))
        .is_err());
        assert!(parse_incremental_snapshot_archive_filename(
            "incremental-snapshot-bad!slot-bad!slot-bad!hash.bad!ext"
        )
        .is_err());

        assert!(parse_incremental_snapshot_archive_filename(&format!(
            "incremental-snapshot-bad!slot-56785678-{}.tar",
            Hash::new_unique()
        ))
        .is_err());

        assert!(parse_incremental_snapshot_archive_filename(&format!(
            "incremental-snapshot-12345678-bad!slot-{}.tar",
            Hash::new_unique()
        ))
        .is_err());

        assert!(parse_incremental_snapshot_archive_filename(
            "incremental-snapshot-12341234-56785678-bad!HASH.tar"
        )
        .is_err());

        assert!(parse_incremental_snapshot_archive_filename(&format!(
            "incremental-snapshot-12341234-56785678-{}.bad!ext",
            Hash::new_unique()
        ))
        .is_err());
    }

    #[test]
    fn test_check_are_snapshots_compatible() {
        solana_logger::setup();
        let slot1: Slot = 1234;
        let slot2: Slot = 5678;
        let slot3: Slot = 999_999;

        let full_snapshot_archive_info = FullSnapshotArchiveInfo::new_from_path(PathBuf::from(
            format!("/dir/snapshot-{}-{}.tar", slot1, Hash::new_unique()),
        ))
        .unwrap();

        assert!(check_are_snapshots_compatible(&full_snapshot_archive_info, None,).is_ok());

        let incremental_snapshot_archive_info =
            IncrementalSnapshotArchiveInfo::new_from_path(PathBuf::from(format!(
                "/dir/incremental-snapshot-{}-{}-{}.tar",
                slot1,
                slot2,
                Hash::new_unique()
            )))
            .unwrap();

        assert!(check_are_snapshots_compatible(
            &full_snapshot_archive_info,
            Some(&incremental_snapshot_archive_info)
        )
        .is_ok());

        let incremental_snapshot_archive_info =
            IncrementalSnapshotArchiveInfo::new_from_path(PathBuf::from(format!(
                "/dir/incremental-snapshot-{}-{}-{}.tar",
                slot2,
                slot3,
                Hash::new_unique()
            )))
            .unwrap();

        assert!(check_are_snapshots_compatible(
            &full_snapshot_archive_info,
            Some(&incremental_snapshot_archive_info)
        )
        .is_err());
    }

    /// A test heler function that creates bank snapshot files
    fn common_create_bank_snapshot_files(
        bank_snapshots_dir: &Path,
        min_slot: Slot,
        max_slot: Slot,
    ) {
        for slot in min_slot..max_slot {
            let snapshot_dir = get_bank_snapshots_dir(bank_snapshots_dir, slot);
            fs::create_dir_all(&snapshot_dir).unwrap();

            let snapshot_filename = get_snapshot_file_name(slot);
            let snapshot_path = snapshot_dir.join(snapshot_filename);
            File::create(snapshot_path).unwrap();
        }
    }

    #[test]
    fn test_get_bank_snapshots() {
        solana_logger::setup();
        let temp_snapshots_dir = tempfile::TempDir::new().unwrap();
        let min_slot = 10;
        let max_slot = 20;
        common_create_bank_snapshot_files(temp_snapshots_dir.path(), min_slot, max_slot);

        let bank_snapshots = get_bank_snapshots(temp_snapshots_dir.path());
        assert_eq!(bank_snapshots.len() as Slot, max_slot - min_slot);
    }

    #[test]
    fn test_get_highest_bank_snapshot_post() {
        solana_logger::setup();
        let temp_snapshots_dir = tempfile::TempDir::new().unwrap();
        let min_slot = 99;
        let max_slot = 123;
        common_create_bank_snapshot_files(temp_snapshots_dir.path(), min_slot, max_slot);

        let highest_bank_snapshot = get_highest_bank_snapshot_post(temp_snapshots_dir.path());
        assert!(highest_bank_snapshot.is_some());
        assert_eq!(highest_bank_snapshot.unwrap().slot, max_slot - 1);
    }

    /// A test helper function that creates full and incremental snapshot archive files.  Creates
    /// full snapshot files in the range (`min_full_snapshot_slot`, `max_full_snapshot_slot`], and
    /// incremental snapshot files in the range (`min_incremental_snapshot_slot`,
    /// `max_incremental_snapshot_slot`].  Additionally, "bad" files are created for both full and
    /// incremental snapshots to ensure the tests properly filter them out.
    fn common_create_snapshot_archive_files(
        snapshot_archives_dir: &Path,
        min_full_snapshot_slot: Slot,
        max_full_snapshot_slot: Slot,
        min_incremental_snapshot_slot: Slot,
        max_incremental_snapshot_slot: Slot,
    ) {
        fs::create_dir_all(snapshot_archives_dir).unwrap();
        for full_snapshot_slot in min_full_snapshot_slot..max_full_snapshot_slot {
            for incremental_snapshot_slot in
                min_incremental_snapshot_slot..max_incremental_snapshot_slot
            {
                let snapshot_filename = format!(
                    "incremental-snapshot-{}-{}-{}.tar",
                    full_snapshot_slot,
                    incremental_snapshot_slot,
                    Hash::default()
                );
                let snapshot_filepath = snapshot_archives_dir.join(snapshot_filename);
                File::create(snapshot_filepath).unwrap();
            }

            let snapshot_filename =
                format!("snapshot-{}-{}.tar", full_snapshot_slot, Hash::default());
            let snapshot_filepath = snapshot_archives_dir.join(snapshot_filename);
            File::create(snapshot_filepath).unwrap();

            // Add in an incremental snapshot with a bad filename and high slot to ensure filename are filtered and sorted correctly
            let bad_filename = format!(
                "incremental-snapshot-{}-{}-bad!hash.tar",
                full_snapshot_slot,
                max_incremental_snapshot_slot + 1,
            );
            let bad_filepath = snapshot_archives_dir.join(bad_filename);
            File::create(bad_filepath).unwrap();
        }

        // Add in a snapshot with a bad filename and high slot to ensure filename are filtered and
        // sorted correctly
        let bad_filename = format!("snapshot-{}-bad!hash.tar", max_full_snapshot_slot + 1);
        let bad_filepath = snapshot_archives_dir.join(bad_filename);
        File::create(bad_filepath).unwrap();
    }

    #[test]
    fn test_get_full_snapshot_archives() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_slot = 123;
        let max_slot = 456;
        common_create_snapshot_archive_files(
            temp_snapshot_archives_dir.path(),
            min_slot,
            max_slot,
            0,
            0,
        );

        let snapshot_archives = get_full_snapshot_archives(temp_snapshot_archives_dir);
        assert_eq!(snapshot_archives.len() as Slot, max_slot - min_slot);
    }

    #[test]
    fn test_get_full_snapshot_archives_remote() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_slot = 123;
        let max_slot = 456;
        common_create_snapshot_archive_files(
            &temp_snapshot_archives_dir.path().join("remote"),
            min_slot,
            max_slot,
            0,
            0,
        );

        let snapshot_archives = get_full_snapshot_archives(temp_snapshot_archives_dir);
        assert_eq!(snapshot_archives.len() as Slot, max_slot - min_slot);
        assert!(snapshot_archives.iter().all(|info| info.is_remote()));
    }

    #[test]
    fn test_get_incremental_snapshot_archives() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_full_snapshot_slot = 12;
        let max_full_snapshot_slot = 23;
        let min_incremental_snapshot_slot = 34;
        let max_incremental_snapshot_slot = 45;
        common_create_snapshot_archive_files(
            temp_snapshot_archives_dir.path(),
            min_full_snapshot_slot,
            max_full_snapshot_slot,
            min_incremental_snapshot_slot,
            max_incremental_snapshot_slot,
        );

        let incremental_snapshot_archives =
            get_incremental_snapshot_archives(temp_snapshot_archives_dir);
        assert_eq!(
            incremental_snapshot_archives.len() as Slot,
            (max_full_snapshot_slot - min_full_snapshot_slot)
                * (max_incremental_snapshot_slot - min_incremental_snapshot_slot)
        );
    }

    #[test]
    fn test_get_incremental_snapshot_archives_remote() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_full_snapshot_slot = 12;
        let max_full_snapshot_slot = 23;
        let min_incremental_snapshot_slot = 34;
        let max_incremental_snapshot_slot = 45;
        common_create_snapshot_archive_files(
            &temp_snapshot_archives_dir.path().join("remote"),
            min_full_snapshot_slot,
            max_full_snapshot_slot,
            min_incremental_snapshot_slot,
            max_incremental_snapshot_slot,
        );

        let incremental_snapshot_archives =
            get_incremental_snapshot_archives(temp_snapshot_archives_dir);
        assert_eq!(
            incremental_snapshot_archives.len() as Slot,
            (max_full_snapshot_slot - min_full_snapshot_slot)
                * (max_incremental_snapshot_slot - min_incremental_snapshot_slot)
        );
        assert!(incremental_snapshot_archives
            .iter()
            .all(|info| info.is_remote()));
    }

    #[test]
    fn test_get_highest_full_snapshot_archive_slot() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_slot = 123;
        let max_slot = 456;
        common_create_snapshot_archive_files(
            temp_snapshot_archives_dir.path(),
            min_slot,
            max_slot,
            0,
            0,
        );

        assert_eq!(
            get_highest_full_snapshot_archive_slot(temp_snapshot_archives_dir.path()),
            Some(max_slot - 1)
        );
    }

    #[test]
    fn test_get_highest_incremental_snapshot_slot() {
        solana_logger::setup();
        let temp_snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let min_full_snapshot_slot = 12;
        let max_full_snapshot_slot = 23;
        let min_incremental_snapshot_slot = 34;
        let max_incremental_snapshot_slot = 45;
        common_create_snapshot_archive_files(
            temp_snapshot_archives_dir.path(),
            min_full_snapshot_slot,
            max_full_snapshot_slot,
            min_incremental_snapshot_slot,
            max_incremental_snapshot_slot,
        );

        for full_snapshot_slot in min_full_snapshot_slot..max_full_snapshot_slot {
            assert_eq!(
                get_highest_incremental_snapshot_archive_slot(
                    temp_snapshot_archives_dir.path(),
                    full_snapshot_slot
                ),
                Some(max_incremental_snapshot_slot - 1)
            );
        }

        assert_eq!(
            get_highest_incremental_snapshot_archive_slot(
                temp_snapshot_archives_dir.path(),
                max_full_snapshot_slot
            ),
            None
        );
    }

    fn common_test_purge_old_snapshot_archives(
        snapshot_names: &[&String],
        maximum_full_snapshot_archives_to_retain: usize,
        maximum_incremental_snapshot_archives_to_retain: usize,
        expected_snapshots: &[&String],
    ) {
        let temp_snap_dir = tempfile::TempDir::new().unwrap();

        for snap_name in snapshot_names {
            let snap_path = temp_snap_dir.path().join(&snap_name);
            let mut _snap_file = File::create(snap_path);
        }
        purge_old_snapshot_archives(
            temp_snap_dir.path(),
            maximum_full_snapshot_archives_to_retain,
            maximum_incremental_snapshot_archives_to_retain,
        );

        let mut retained_snaps = HashSet::new();
        for entry in fs::read_dir(temp_snap_dir.path()).unwrap() {
            let entry_path_buf = entry.unwrap().path();
            let entry_path = entry_path_buf.as_path();
            let snapshot_name = entry_path
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            retained_snaps.insert(snapshot_name);
        }

        for snap_name in expected_snapshots {
            assert!(
                retained_snaps.contains(snap_name.as_str()),
                "{} not found",
                snap_name
            );
        }
        assert_eq!(retained_snaps.len(), expected_snapshots.len());
    }

    #[test]
    fn test_purge_old_full_snapshot_archives() {
        let snap1_name = format!("snapshot-1-{}.tar.zst", Hash::default());
        let snap2_name = format!("snapshot-3-{}.tar.zst", Hash::default());
        let snap3_name = format!("snapshot-50-{}.tar.zst", Hash::default());
        let snapshot_names = vec![&snap1_name, &snap2_name, &snap3_name];

        // expecting only the newest to be retained
        let expected_snapshots = vec![&snap3_name];
        common_test_purge_old_snapshot_archives(
            &snapshot_names,
            1,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            &expected_snapshots,
        );

        // retaining 0, but minimum to retain is 1
        common_test_purge_old_snapshot_archives(
            &snapshot_names,
            0,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            &expected_snapshots,
        );

        // retaining 2, expecting the 2 newest to be retained
        let expected_snapshots = vec![&snap2_name, &snap3_name];
        common_test_purge_old_snapshot_archives(
            &snapshot_names,
            2,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            &expected_snapshots,
        );

        // retaining 3, all three should be retained
        let expected_snapshots = vec![&snap1_name, &snap2_name, &snap3_name];
        common_test_purge_old_snapshot_archives(
            &snapshot_names,
            3,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            &expected_snapshots,
        );
    }

    /// Mimic a running node's behavior w.r.t. purging old snapshot archives.  Take snapshots in a
    /// loop, and periodically purge old snapshot archives.  After purging, check to make sure the
    /// snapshot archives on disk are correct.
    #[test]
    fn test_purge_old_full_snapshot_archives_in_the_loop() {
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let maximum_snapshots_to_retain = 5;
        let starting_slot: Slot = 42;

        for slot in (starting_slot..).take(100) {
            let full_snapshot_archive_file_name =
                format!("snapshot-{}-{}.tar", slot, Hash::default());
            let full_snapshot_archive_path = snapshot_archives_dir
                .as_ref()
                .join(full_snapshot_archive_file_name);
            File::create(full_snapshot_archive_path).unwrap();

            // don't purge-and-check until enough snapshot archives have been created
            if slot < starting_slot + maximum_snapshots_to_retain as Slot {
                continue;
            }

            // purge infrequently, so there will always be snapshot archives to purge
            if slot % (maximum_snapshots_to_retain as Slot * 2) != 0 {
                continue;
            }

            purge_old_snapshot_archives(
                &snapshot_archives_dir,
                maximum_snapshots_to_retain,
                usize::MAX,
            );
            let mut full_snapshot_archives = get_full_snapshot_archives(&snapshot_archives_dir);
            full_snapshot_archives.sort_unstable();
            assert_eq!(full_snapshot_archives.len(), maximum_snapshots_to_retain);
            assert_eq!(full_snapshot_archives.last().unwrap().slot(), slot);
            for (i, full_snapshot_archive) in full_snapshot_archives.iter().rev().enumerate() {
                assert_eq!(full_snapshot_archive.slot(), slot - i as Slot);
            }
        }
    }

    #[test]
    fn test_purge_old_incremental_snapshot_archives() {
        solana_logger::setup();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let starting_slot = 100_000;

        let maximum_incremental_snapshot_archives_to_retain =
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN;
        let maximum_full_snapshot_archives_to_retain = DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN;

        let incremental_snapshot_interval = 100;
        let num_incremental_snapshots_per_full_snapshot =
            maximum_incremental_snapshot_archives_to_retain * 2;
        let full_snapshot_interval =
            incremental_snapshot_interval * num_incremental_snapshots_per_full_snapshot;

        let mut snapshot_filenames = vec![];
        (starting_slot..)
            .step_by(full_snapshot_interval)
            .take(maximum_full_snapshot_archives_to_retain * 2)
            .for_each(|full_snapshot_slot| {
                let snapshot_filename =
                    format!("snapshot-{}-{}.tar", full_snapshot_slot, Hash::default());
                let snapshot_path = snapshot_archives_dir.path().join(&snapshot_filename);
                File::create(snapshot_path).unwrap();
                snapshot_filenames.push(snapshot_filename);

                (full_snapshot_slot..)
                    .step_by(incremental_snapshot_interval)
                    .take(num_incremental_snapshots_per_full_snapshot)
                    .skip(1)
                    .for_each(|incremental_snapshot_slot| {
                        let snapshot_filename = format!(
                            "incremental-snapshot-{}-{}-{}.tar",
                            full_snapshot_slot,
                            incremental_snapshot_slot,
                            Hash::default()
                        );
                        let snapshot_path = snapshot_archives_dir.path().join(&snapshot_filename);
                        File::create(snapshot_path).unwrap();
                        snapshot_filenames.push(snapshot_filename);
                    });
            });

        purge_old_snapshot_archives(
            snapshot_archives_dir.path(),
            maximum_full_snapshot_archives_to_retain,
            maximum_incremental_snapshot_archives_to_retain,
        );

        // Ensure correct number of full snapshot archives are purged/retained
        // NOTE: One extra full snapshot is always kept (the oldest), hence the `+1`
        let mut remaining_full_snapshot_archives =
            get_full_snapshot_archives(snapshot_archives_dir.path());
        assert_eq!(
            remaining_full_snapshot_archives.len(),
            maximum_full_snapshot_archives_to_retain,
        );
        remaining_full_snapshot_archives.sort_unstable();
        let latest_full_snapshot_archive_slot =
            remaining_full_snapshot_archives.last().unwrap().slot();

        // Ensure correct number of incremental snapshot archives are purged/retained
        let mut remaining_incremental_snapshot_archives =
            get_incremental_snapshot_archives(snapshot_archives_dir.path());
        assert_eq!(
            remaining_incremental_snapshot_archives.len(),
            maximum_incremental_snapshot_archives_to_retain
                + maximum_full_snapshot_archives_to_retain.saturating_sub(1)
        );
        remaining_incremental_snapshot_archives.sort_unstable();
        remaining_incremental_snapshot_archives.reverse();

        // Ensure there exists one incremental snapshot all but the latest full snapshot
        for i in (1..maximum_full_snapshot_archives_to_retain).rev() {
            let incremental_snapshot_archive =
                remaining_incremental_snapshot_archives.pop().unwrap();

            let expected_base_slot =
                latest_full_snapshot_archive_slot - (i * full_snapshot_interval) as u64;
            assert_eq!(incremental_snapshot_archive.base_slot(), expected_base_slot);
            let expected_slot = expected_base_slot
                + (full_snapshot_interval - incremental_snapshot_interval) as u64;
            assert_eq!(incremental_snapshot_archive.slot(), expected_slot);
        }

        // Ensure all remaining incremental snapshots are only for the latest full snapshot
        for incremental_snapshot_archive in &remaining_incremental_snapshot_archives {
            assert_eq!(
                incremental_snapshot_archive.base_slot(),
                latest_full_snapshot_archive_slot
            );
        }

        // Ensure the remaining incremental snapshots are at the right slot
        let expected_remaing_incremental_snapshot_archive_slots =
            (latest_full_snapshot_archive_slot..)
                .step_by(incremental_snapshot_interval)
                .take(num_incremental_snapshots_per_full_snapshot)
                .skip(
                    num_incremental_snapshots_per_full_snapshot
                        - maximum_incremental_snapshot_archives_to_retain,
                )
                .collect::<HashSet<_>>();

        let actual_remaining_incremental_snapshot_archive_slots =
            remaining_incremental_snapshot_archives
                .iter()
                .map(|snapshot| snapshot.slot())
                .collect::<HashSet<_>>();
        assert_eq!(
            actual_remaining_incremental_snapshot_archive_slots,
            expected_remaing_incremental_snapshot_archive_slots
        );
    }

    #[test]
    fn test_purge_all_incremental_snapshot_archives_when_no_full_snapshot_archives() {
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();

        for snapshot_filenames in [
            format!("incremental-snapshot-100-120-{}.tar", Hash::default()),
            format!("incremental-snapshot-100-140-{}.tar", Hash::default()),
            format!("incremental-snapshot-100-160-{}.tar", Hash::default()),
            format!("incremental-snapshot-100-180-{}.tar", Hash::default()),
            format!("incremental-snapshot-200-220-{}.tar", Hash::default()),
            format!("incremental-snapshot-200-240-{}.tar", Hash::default()),
            format!("incremental-snapshot-200-260-{}.tar", Hash::default()),
            format!("incremental-snapshot-200-280-{}.tar", Hash::default()),
        ] {
            let snapshot_path = snapshot_archives_dir.path().join(&snapshot_filenames);
            File::create(snapshot_path).unwrap();
        }

        purge_old_snapshot_archives(snapshot_archives_dir.path(), usize::MAX, usize::MAX);

        let remaining_incremental_snapshot_archives =
            get_incremental_snapshot_archives(snapshot_archives_dir.path());
        assert!(remaining_incremental_snapshot_archives.is_empty());
    }

    /// Test roundtrip of bank to a full snapshot, then back again.  This test creates the simplest
    /// bank possible, so the contents of the snapshot archive will be quite minimal.
    #[test]
    fn test_roundtrip_bank_to_and_from_full_snapshot_simple() {
        solana_logger::setup();
        let genesis_config = GenesisConfig::default();
        let original_bank = Bank::new_for_tests(&genesis_config);

        while !original_bank.is_complete() {
            original_bank.register_tick(&Hash::new_unique());
        }

        let accounts_dir = tempfile::TempDir::new().unwrap();
        let bank_snapshots_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archive_format = ArchiveFormat::Tar;

        let snapshot_archive_info = bank_to_full_snapshot_archive(
            &bank_snapshots_dir,
            &original_bank,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let (roundtrip_bank, _) = bank_from_snapshot_archives(
            &[PathBuf::from(accounts_dir.path())],
            bank_snapshots_dir.path(),
            &snapshot_archive_info,
            None,
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        assert_eq!(original_bank, roundtrip_bank);
    }

    /// Test roundtrip of bank to a full snapshot, then back again.  This test is more involved
    /// than the simple version above; creating multiple banks over multiple slots and doing
    /// multiple transfers.  So this full snapshot should contain more data.
    #[test]
    fn test_roundtrip_bank_to_and_from_snapshot_complex() {
        solana_logger::setup();
        let collector = Pubkey::new_unique();
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let key3 = Keypair::new();
        let key4 = Keypair::new();
        let key5 = Keypair::new();

        let (genesis_config, mint_keypair) = create_genesis_config(1_000_000);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        bank0.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        bank0.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        bank0.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        while !bank0.is_complete() {
            bank0.register_tick(&Hash::new_unique());
        }

        let slot = 1;
        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &collector, slot));
        bank1.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        bank1.transfer(4, &mint_keypair, &key4.pubkey()).unwrap();
        bank1.transfer(5, &mint_keypair, &key5.pubkey()).unwrap();
        while !bank1.is_complete() {
            bank1.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &collector, slot));
        bank2.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank2.is_complete() {
            bank2.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank3 = Arc::new(Bank::new_from_parent(&bank2, &collector, slot));
        bank3.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank3.is_complete() {
            bank3.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank4 = Arc::new(Bank::new_from_parent(&bank3, &collector, slot));
        bank4.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank4.is_complete() {
            bank4.register_tick(&Hash::new_unique());
        }

        let accounts_dir = tempfile::TempDir::new().unwrap();
        let bank_snapshots_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archive_format = ArchiveFormat::TarGzip;

        let full_snapshot_archive_info = bank_to_full_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank4,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let (roundtrip_bank, _) = bank_from_snapshot_archives(
            &[PathBuf::from(accounts_dir.path())],
            bank_snapshots_dir.path(),
            &full_snapshot_archive_info,
            None,
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        assert_eq!(*bank4, roundtrip_bank);
    }

    /// Test roundtrip of bank to snapshots, then back again, with incremental snapshots.  In this
    /// version, build up a few slots and take a full snapshot.  Continue on a few more slots and
    /// take an incremental snapshot.  Rebuild the bank from both the incremental snapshot and full
    /// snapshot.
    ///
    /// For the full snapshot, touch all the accounts, but only one for the incremental snapshot.
    /// This is intended to mimic the real behavior of transactions, where only a small number of
    /// accounts are modified often, which are captured by the incremental snapshot.  The majority
    /// of the accounts are not modified often, and are captured by the full snapshot.
    #[test]
    fn test_roundtrip_bank_to_and_from_incremental_snapshot() {
        solana_logger::setup();
        let collector = Pubkey::new_unique();
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let key3 = Keypair::new();
        let key4 = Keypair::new();
        let key5 = Keypair::new();

        let (genesis_config, mint_keypair) = create_genesis_config(1_000_000);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        bank0.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        bank0.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        bank0.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        while !bank0.is_complete() {
            bank0.register_tick(&Hash::new_unique());
        }

        let slot = 1;
        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &collector, slot));
        bank1.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        bank1.transfer(4, &mint_keypair, &key4.pubkey()).unwrap();
        bank1.transfer(5, &mint_keypair, &key5.pubkey()).unwrap();
        while !bank1.is_complete() {
            bank1.register_tick(&Hash::new_unique());
        }

        let accounts_dir = tempfile::TempDir::new().unwrap();
        let bank_snapshots_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archive_format = ArchiveFormat::TarZstd;

        let full_snapshot_slot = slot;
        let full_snapshot_archive_info = bank_to_full_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank1,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let slot = slot + 1;
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &collector, slot));
        bank2.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank2.is_complete() {
            bank2.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank3 = Arc::new(Bank::new_from_parent(&bank2, &collector, slot));
        bank3.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank3.is_complete() {
            bank3.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank4 = Arc::new(Bank::new_from_parent(&bank3, &collector, slot));
        bank4.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank4.is_complete() {
            bank4.register_tick(&Hash::new_unique());
        }

        let incremental_snapshot_archive_info = bank_to_incremental_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank4,
            full_snapshot_slot,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let (roundtrip_bank, _) = bank_from_snapshot_archives(
            &[PathBuf::from(accounts_dir.path())],
            bank_snapshots_dir.path(),
            &full_snapshot_archive_info,
            Some(&incremental_snapshot_archive_info),
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        assert_eq!(*bank4, roundtrip_bank);
    }

    /// Test rebuilding bank from the latest snapshot archives
    #[test]
    fn test_bank_from_latest_snapshot_archives() {
        solana_logger::setup();
        let collector = Pubkey::new_unique();
        let key1 = Keypair::new();
        let key2 = Keypair::new();
        let key3 = Keypair::new();

        let (genesis_config, mint_keypair) = create_genesis_config(1_000_000);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        bank0.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        bank0.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        bank0.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        while !bank0.is_complete() {
            bank0.register_tick(&Hash::new_unique());
        }

        let slot = 1;
        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &collector, slot));
        bank1.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        bank1.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        bank1.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        while !bank1.is_complete() {
            bank1.register_tick(&Hash::new_unique());
        }

        let accounts_dir = tempfile::TempDir::new().unwrap();
        let bank_snapshots_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archive_format = ArchiveFormat::Tar;

        let full_snapshot_slot = slot;
        bank_to_full_snapshot_archive(
            &bank_snapshots_dir,
            &bank1,
            None,
            &snapshot_archives_dir,
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let slot = slot + 1;
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &collector, slot));
        bank2.transfer(1, &mint_keypair, &key1.pubkey()).unwrap();
        while !bank2.is_complete() {
            bank2.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank3 = Arc::new(Bank::new_from_parent(&bank2, &collector, slot));
        bank3.transfer(2, &mint_keypair, &key2.pubkey()).unwrap();
        while !bank3.is_complete() {
            bank3.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank4 = Arc::new(Bank::new_from_parent(&bank3, &collector, slot));
        bank4.transfer(3, &mint_keypair, &key3.pubkey()).unwrap();
        while !bank4.is_complete() {
            bank4.register_tick(&Hash::new_unique());
        }

        bank_to_incremental_snapshot_archive(
            &bank_snapshots_dir,
            &bank4,
            full_snapshot_slot,
            None,
            &snapshot_archives_dir,
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let (deserialized_bank, ..) = bank_from_latest_snapshot_archives(
            &bank_snapshots_dir,
            &snapshot_archives_dir,
            &[accounts_dir.as_ref().to_path_buf()],
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        assert_eq!(deserialized_bank, *bank4);
    }

    /// Test that cleaning works well in the edge cases of zero-lamport accounts and snapshots.
    /// Here's the scenario:
    ///
    /// slot 1:
    ///     - send some lamports to Account1 (from Account2) to bring it to life
    ///     - take a full snapshot
    /// slot 2:
    ///     - make Account1 have zero lamports (send back to Account2)
    ///     - take an incremental snapshot
    ///     - ensure deserializing from this snapshot is equal to this bank
    /// slot 3:
    ///     - remove Account2's reference back to slot 2 by transfering from the mint to Account2
    /// slot 4:
    ///     - ensure `clean_accounts()` has run and that Account1 is gone
    ///     - take another incremental snapshot
    ///     - ensure deserializing from this snapshots is equal to this bank
    ///     - ensure Account1 hasn't come back from the dead
    ///
    /// The check at slot 4 will fail with the pre-incremental-snapshot cleaning logic.  Because
    /// of the cleaning/purging at slot 4, the incremental snapshot at slot 4 will no longer have
    /// information about Account1, but the full snapshost _does_ have info for Account1, which is
    /// no longer correct!
    #[test]
    fn test_incremental_snapshots_handle_zero_lamport_accounts() {
        solana_logger::setup();

        let collector = Pubkey::new_unique();
        let key1 = Keypair::new();
        let key2 = Keypair::new();

        let accounts_dir = tempfile::TempDir::new().unwrap();
        let bank_snapshots_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archives_dir = tempfile::TempDir::new().unwrap();
        let snapshot_archive_format = ArchiveFormat::Tar;

        let (genesis_config, mint_keypair) = create_genesis_config(1_000_000);

        let lamports_to_transfer = 123_456;
        let bank0 = Arc::new(Bank::new_with_paths_for_tests(
            &genesis_config,
            vec![accounts_dir.path().to_path_buf()],
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            AccountShrinkThreshold::default(),
            false,
        ));
        bank0
            .transfer(lamports_to_transfer, &mint_keypair, &key2.pubkey())
            .unwrap();
        while !bank0.is_complete() {
            bank0.register_tick(&Hash::new_unique());
        }

        let slot = 1;
        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &collector, slot));
        bank1
            .transfer(lamports_to_transfer, &key2, &key1.pubkey())
            .unwrap();
        while !bank1.is_complete() {
            bank1.register_tick(&Hash::new_unique());
        }

        let full_snapshot_slot = slot;
        let full_snapshot_archive_info = bank_to_full_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank1,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let slot = slot + 1;
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &collector, slot));
        let blockhash = bank2.last_blockhash();
        let tx = SanitizedTransaction::from_transaction_for_tests(system_transaction::transfer(
            &key1,
            &key2.pubkey(),
            lamports_to_transfer,
            blockhash,
        ));
        let fee = bank2.get_fee_for_message(tx.message()).unwrap();
        let tx = system_transaction::transfer(
            &key1,
            &key2.pubkey(),
            lamports_to_transfer - fee,
            blockhash,
        );
        bank2.process_transaction(&tx).unwrap();
        assert_eq!(
            bank2.get_balance(&key1.pubkey()),
            0,
            "Ensure Account1's balance is zero"
        );
        while !bank2.is_complete() {
            bank2.register_tick(&Hash::new_unique());
        }

        // Take an incremental snapshot and then do a roundtrip on the bank and ensure it
        // deserializes correctly.
        let incremental_snapshot_archive_info = bank_to_incremental_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank2,
            full_snapshot_slot,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();
        let (deserialized_bank, _) = bank_from_snapshot_archives(
            &[accounts_dir.path().to_path_buf()],
            bank_snapshots_dir.path(),
            &full_snapshot_archive_info,
            Some(&incremental_snapshot_archive_info),
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();
        assert_eq!(
            deserialized_bank, *bank2,
            "Ensure rebuilding from an incremental snapshot works"
        );

        let slot = slot + 1;
        let bank3 = Arc::new(Bank::new_from_parent(&bank2, &collector, slot));
        // Update Account2 so that it no longer holds a reference to slot2
        bank3
            .transfer(lamports_to_transfer, &mint_keypair, &key2.pubkey())
            .unwrap();
        while !bank3.is_complete() {
            bank3.register_tick(&Hash::new_unique());
        }

        let slot = slot + 1;
        let bank4 = Arc::new(Bank::new_from_parent(&bank3, &collector, slot));
        while !bank4.is_complete() {
            bank4.register_tick(&Hash::new_unique());
        }

        // Ensure account1 has been cleaned/purged from everywhere
        bank4.squash();
        bank4.clean_accounts(true, false, Some(full_snapshot_slot));
        assert!(
            bank4.get_account_modified_slot(&key1.pubkey()).is_none(),
            "Ensure Account1 has been cleaned and purged from AccountsDb"
        );

        // Take an incremental snapshot and then do a roundtrip on the bank and ensure it
        // deserializes correctly
        let incremental_snapshot_archive_info = bank_to_incremental_snapshot_archive(
            bank_snapshots_dir.path(),
            &bank4,
            full_snapshot_slot,
            None,
            snapshot_archives_dir.path(),
            snapshot_archive_format,
            DEFAULT_MAX_FULL_SNAPSHOT_ARCHIVES_TO_RETAIN,
            DEFAULT_MAX_INCREMENTAL_SNAPSHOT_ARCHIVES_TO_RETAIN,
        )
        .unwrap();

        let (deserialized_bank, _) = bank_from_snapshot_archives(
            &[accounts_dir.path().to_path_buf()],
            bank_snapshots_dir.path(),
            &full_snapshot_archive_info,
            Some(&incremental_snapshot_archive_info),
            &genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();
        assert_eq!(
            deserialized_bank, *bank4,
            "Ensure rebuilding from an incremental snapshot works",
        );
        assert!(
            deserialized_bank
                .get_account_modified_slot(&key1.pubkey())
                .is_none(),
            "Ensure Account1 has not been brought back from the dead"
        );
    }

    /// All the permutations of `snapshot_type` for the new-and-old accounts packages:
    ///
    ///  new      | old      |
    ///  snapshot | snapshot |
    ///  type     | type     | result
    /// ----------+----------+--------
    ///   FSS     |  FSS     |  true
    ///   FSS     |  ISS     |  true
    ///   FSS     |  None    |  true
    ///   ISS     |  FSS     |  false
    ///   ISS     |  ISS     |  true
    ///   ISS     |  None    |  true
    ///   None    |  FSS     |  false
    ///   None    |  ISS     |  false
    ///   None    |  None    |  true
    #[test]
    fn test_can_submit_accounts_package() {
        /// helper function to create an AccountsPackage that's good enough for this test
        fn new_accounts_package_with(snapshot_type: Option<SnapshotType>) -> AccountsPackage {
            AccountsPackage {
                slot: Slot::default(),
                block_height: Slot::default(),
                slot_deltas: Vec::default(),
                snapshot_links: TempDir::new().unwrap(),
                snapshot_storages: SnapshotStorages::default(),
                archive_format: ArchiveFormat::Tar,
                snapshot_version: SnapshotVersion::default(),
                snapshot_archives_dir: PathBuf::default(),
                expected_capitalization: u64::default(),
                accounts_hash_for_testing: None,
                cluster_type: solana_sdk::genesis_config::ClusterType::Development,
                snapshot_type,
                accounts: Arc::new(crate::accounts::Accounts::default_for_tests()),
                epoch_schedule: solana_sdk::epoch_schedule::EpochSchedule::default(),
                rent_collector: crate::rent_collector::RentCollector::default(),
            }
        }

        let pending_accounts_package = PendingAccountsPackage::default();
        for (new_snapshot_type, old_snapshot_type, expected_result) in [
            (
                Some(SnapshotType::FullSnapshot),
                Some(SnapshotType::FullSnapshot),
                true,
            ),
            (
                Some(SnapshotType::FullSnapshot),
                Some(SnapshotType::IncrementalSnapshot(0)),
                true,
            ),
            (Some(SnapshotType::FullSnapshot), None, true),
            (
                Some(SnapshotType::IncrementalSnapshot(0)),
                Some(SnapshotType::FullSnapshot),
                false,
            ),
            (
                Some(SnapshotType::IncrementalSnapshot(0)),
                Some(SnapshotType::IncrementalSnapshot(0)),
                true,
            ),
            (Some(SnapshotType::IncrementalSnapshot(0)), None, true),
            (None, Some(SnapshotType::FullSnapshot), false),
            (None, Some(SnapshotType::IncrementalSnapshot(0)), false),
            (None, None, true),
        ] {
            let new_accounts_package = new_accounts_package_with(new_snapshot_type);
            let old_accounts_package = new_accounts_package_with(old_snapshot_type);
            pending_accounts_package
                .lock()
                .unwrap()
                .replace(old_accounts_package);

            let actual_result =
                can_submit_accounts_package(&new_accounts_package, &pending_accounts_package);
            assert_eq!(expected_result, actual_result);
        }
    }
}
