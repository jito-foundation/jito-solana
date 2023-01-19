use {
    crate::{cluster::Cluster, local_cluster::LocalCluster},
    log::*,
    solana_runtime::{
        snapshot_archive_info::{
            FullSnapshotArchiveInfo, IncrementalSnapshotArchiveInfo, SnapshotArchiveInfoGetter,
        },
        snapshot_utils,
    },
    solana_sdk::{client::SyncClient, commitment_config::CommitmentConfig},
    std::{path::Path, thread::sleep, time::Duration},
};

impl LocalCluster {
    /// Return the next full snapshot archive info after the cluster's last processed slot
    pub fn wait_for_next_full_snapshot(
        &self,
        snapshot_archives_dir: impl AsRef<Path>,
    ) -> FullSnapshotArchiveInfo {
        match self.wait_for_next_snapshot(snapshot_archives_dir, NextSnapshotType::FullSnapshot) {
            NextSnapshotResult::FullSnapshot(full_snapshot_archive_info) => {
                full_snapshot_archive_info
            }
            _ => unreachable!(),
        }
    }

    /// Return the next incremental snapshot archive info (and associated full snapshot archive info)
    /// after the cluster's last processed slot
    pub fn wait_for_next_incremental_snapshot(
        &self,
        snapshot_archives_dir: impl AsRef<Path>,
    ) -> (IncrementalSnapshotArchiveInfo, FullSnapshotArchiveInfo) {
        match self.wait_for_next_snapshot(
            snapshot_archives_dir,
            NextSnapshotType::IncrementalAndFullSnapshot,
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

    /// Return the next snapshot archive infos after the cluster's last processed slot
    pub fn wait_for_next_snapshot(
        &self,
        snapshot_archives_dir: impl AsRef<Path>,
        next_snapshot_type: NextSnapshotType,
    ) -> NextSnapshotResult {
        // Get slot after which this was generated
        let client = self
            .get_validator_client(&self.entry_point_info.id)
            .unwrap();
        let last_slot = client
            .get_slot_with_commitment(CommitmentConfig::processed())
            .expect("Couldn't get slot");

        // Wait for a snapshot for a bank >= last_slot to be made so we know that the snapshot
        // must include the transactions just pushed
        trace!(
            "Waiting for {:?} snapshot archive to be generated with slot >= {}",
            next_snapshot_type,
            last_slot
        );
        loop {
            if let Some(full_snapshot_archive_info) =
                snapshot_utils::get_highest_full_snapshot_archive_info(&snapshot_archives_dir, None)
            {
                match next_snapshot_type {
                    NextSnapshotType::FullSnapshot => {
                        if full_snapshot_archive_info.slot() >= last_slot {
                            return NextSnapshotResult::FullSnapshot(full_snapshot_archive_info);
                        }
                    }
                    NextSnapshotType::IncrementalAndFullSnapshot => {
                        if let Some(incremental_snapshot_archive_info) =
                            snapshot_utils::get_highest_incremental_snapshot_archive_info(
                                &snapshot_archives_dir,
                                full_snapshot_archive_info.slot(),
                                None,
                            )
                        {
                            if incremental_snapshot_archive_info.slot() >= last_slot {
                                return NextSnapshotResult::IncrementalAndFullSnapshot(
                                    incremental_snapshot_archive_info,
                                    full_snapshot_archive_info,
                                );
                            }
                        }
                    }
                }
            }
            sleep(Duration::from_secs(5));
        }
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
