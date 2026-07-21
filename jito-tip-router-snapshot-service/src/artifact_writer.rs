//! Tip-router snapshot artifact serialization and disk writes.

use {
    crate::bank_collector::TipRouterSnapshotArtifacts,
    std::{
        fs::{self, File, OpenOptions},
        io::{self, Write},
        path::{Path, PathBuf},
        time::{SystemTime, UNIX_EPOCH},
    },
};

pub fn write_tip_router_snapshot_artifacts(
    output_dir: &Path,
    artifacts: &TipRouterSnapshotArtifacts,
) -> io::Result<PathBuf> {
    fs::create_dir_all(output_dir)?;

    let artifact_path = output_dir.join(format!("tip-router-snapshot-{}.json", artifacts.slot));
    let temp_path = output_dir.join(format!(
        ".tip-router-snapshot-{}-{}-{}.json.tmp",
        artifacts.slot,
        std::process::id(),
        unique_file_suffix()
    ));

    let write_result = write_artifact_file(&temp_path, &artifact_path, artifacts);
    if write_result.is_err() {
        let _ = fs::remove_file(&temp_path);
    }
    write_result
}

fn write_artifact_file(
    temp_path: &Path,
    artifact_path: &Path,
    artifacts: &TipRouterSnapshotArtifacts,
) -> io::Result<PathBuf> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(temp_path)?;
    serde_json::to_writer_pretty(&mut file, &artifacts.contents).map_err(io::Error::other)?;
    file.write_all(b"\n")?;
    file.sync_all()?;

    fs::rename(temp_path, artifact_path)?;
    if let Some(output_dir) = artifact_path.parent() {
        sync_directory(output_dir);
    }

    Ok(artifact_path.to_path_buf())
}

fn sync_directory(path: &Path) {
    if let Ok(directory) = File::open(path) {
        let _ = directory.sync_all();
    }
}

fn unique_file_suffix() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
}
