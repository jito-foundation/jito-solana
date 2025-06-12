use {
    crate::local_cluster::LocalCluster,
    log::*,
    solana_runtime::{
        snapshot_archive_info::{
            FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
        },
        snapshot_utils,
    },
    std::{
        path::Path,
        thread::sleep,
        time::{Duration, Instant},
    },
};

impl LocalCluster {
    /// Return the next produced full snapshot archive info
    pub fn wait_for_next_full_snapshot<T>(
        &self,
        full_snapshot_archives_dir: T,
        max_wait_duration: Option<Duration>,
    ) -> FullSnapshotArchiveInfo
    where
        T: AsRef<Path>,
    {
        match self.wait_for_next_snapshot(
            full_snapshot_archives_dir,
            None::<T>,
            NextSnapshotType::FullSnapshot,
            max_wait_duration,
        ) {
            NextSnapshotResult::FullSnapshot(full_snapshot_archive_info) => {
                full_snapshot_archive_info
            }
            _ => unreachable!(),
        }
    }

    /// Return the next produced incremental snapshot archive info (and associated full snapshot archive info)
    pub fn wait_for_next_incremental_snapshot(
        &self,
        full_snapshot_archives_dir: impl AsRef<Path>,
        incremental_snapshot_archives_dir: impl AsRef<Path>,
        max_wait_duration: Option<Duration>,
    ) -> (IncrementalSnapshotArchiveInfo, FullSnapshotArchiveInfo) {
        match self.wait_for_next_snapshot(
            full_snapshot_archives_dir,
            Some(incremental_snapshot_archives_dir),
            NextSnapshotType::IncrementalAndFullSnapshot,
            max_wait_duration,
        ) {
            NextSnapshotResult::IncrementalAndFullSnapshot(
                incremental_snapshot_archive_info,
                full_snapshot_archive_info,
            ) => (
                incremental_snapshot_archive_info,
                full_snapshot_archive_info,
            ),
            _ => unreachable!(),
        }
    }

    /// Return the next produced snapshot archive infos
    fn wait_for_next_snapshot(
        &self,
        full_snapshot_archives_dir: impl AsRef<Path>,
        incremental_snapshot_archives_dir: Option<impl AsRef<Path>>,
        next_snapshot_type: NextSnapshotType,
        max_wait_duration: Option<Duration>,
    ) -> NextSnapshotResult {
        let full_snapshot_slot = snapshot_utils::get_highest_full_snapshot_archive_slot(
            &full_snapshot_archives_dir,
            None,
        );
        let last_slot = match next_snapshot_type {
            NextSnapshotType::FullSnapshot => full_snapshot_slot,
            NextSnapshotType::IncrementalAndFullSnapshot => {
                full_snapshot_slot.and_then(|full_snapshot_slot| {
                    snapshot_utils::get_highest_incremental_snapshot_archive_slot(
                        incremental_snapshot_archives_dir.as_ref().unwrap(),
                        full_snapshot_slot,
                        None,
                    )
                })
            }
        }
        .unwrap_or(0);

        // Wait for a snapshot for a bank > last_slot to be made
        trace!(
            "Waiting for {:?} snapshot archive to be generated with slot > {}, max wait duration: {:?}",
            next_snapshot_type,
            last_slot,
            max_wait_duration,
        );
        let timer = Instant::now();
        let next_snapshot = loop {
            if let Some(full_snapshot_archive_info) =
                snapshot_utils::get_highest_full_snapshot_archive_info(
                    &full_snapshot_archives_dir,
                    None,
                )
            {
                match next_snapshot_type {
                    NextSnapshotType::FullSnapshot => {
                        if full_snapshot_archive_info.slot() > last_slot {
                            break NextSnapshotResult::FullSnapshot(full_snapshot_archive_info);
                        }
                    }
                    NextSnapshotType::IncrementalAndFullSnapshot => {
                        if let Some(incremental_snapshot_archive_info) =
                            snapshot_utils::get_highest_incremental_snapshot_archive_info(
                                incremental_snapshot_archives_dir.as_ref().unwrap(),
                                full_snapshot_archive_info.slot(),
                                None,
                            )
                        {
                            if incremental_snapshot_archive_info.slot() > last_slot {
                                break NextSnapshotResult::IncrementalAndFullSnapshot(
                                    incremental_snapshot_archive_info,
                                    full_snapshot_archive_info,
                                );
                            }
                        }
                    }
                }
            }
            if let Some(max_wait_duration) = max_wait_duration {
                assert!(
                    timer.elapsed() < max_wait_duration,
                    "Waiting for next {next_snapshot_type:?} snapshot exceeded the {max_wait_duration:?} maximum wait duration!",
                );
            }
            sleep(Duration::from_secs(1));
        };
        trace!(
            "Waited {:?} for next snapshot archive: {:?}",
            timer.elapsed(),
            next_snapshot,
        );

        next_snapshot
    }
}

#[derive(Debug)]
pub enum NextSnapshotType {
    FullSnapshot,
    IncrementalAndFullSnapshot,
}

#[derive(Debug)]
pub enum NextSnapshotResult {
    FullSnapshot(FullSnapshotArchiveInfo),
    IncrementalAndFullSnapshot(IncrementalSnapshotArchiveInfo, FullSnapshotArchiveInfo),
}
