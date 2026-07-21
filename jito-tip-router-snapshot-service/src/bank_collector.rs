//! Bank state collection for tip-router snapshots.

use {
    crate::{config::TipRouterSnapshotConfig, distribution_meta, merkle, stake_meta},
    serde_json::{Value, json},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
};

pub struct TipRouterSnapshotArtifacts {
    pub slot: Slot,
    pub bank_hash: Hash,
    pub epoch: Epoch,
    pub boundary_child_slot: Slot,
    pub boundary_child_hash: Hash,
    pub boundary_child_epoch: Epoch,
    pub contents: Value,
}

pub fn collect_tip_router_snapshot_artifacts(
    config: &TipRouterSnapshotConfig,
    bank: &Bank,
    boundary_child_slot: Slot,
    boundary_child_hash: Hash,
    boundary_child_epoch: Epoch,
) -> TipRouterSnapshotArtifacts {
    let stake_meta = stake_meta::collect_stake_meta(config, bank);
    let distribution_meta = distribution_meta::collect_distribution_meta(config, bank);
    let merkle_trees = merkle::build_merkle_trees(&stake_meta, &distribution_meta);

    TipRouterSnapshotArtifacts {
        slot: bank.slot(),
        bank_hash: bank.hash(),
        epoch: bank.epoch(),
        boundary_child_slot,
        boundary_child_hash,
        boundary_child_epoch,
        contents: json!({
            "slot": bank.slot(),
            "epoch": bank.epoch(),
            "bank_hash": bank.hash().to_string(),
            "boundary_child_slot": boundary_child_slot,
            "boundary_child_epoch": boundary_child_epoch,
            "boundary_child_hash": boundary_child_hash.to_string(),
            "parent_slot": bank.parent_slot(),
            "parent_hash": bank.parent_hash().to_string(),
            "ncn": config.ncn.as_ref().map(ToString::to_string),
            "program_ids": {
                "tip_router": config.tip_router_program_id.as_ref().map(ToString::to_string),
                "tip_distribution": config
                    .tip_distribution_program_id
                    .as_ref()
                    .map(ToString::to_string),
                "priority_fee_distribution": config
                    .priority_fee_distribution_program_id
                    .as_ref()
                    .map(ToString::to_string),
                "tip_payment": config.tip_payment_program_id.as_ref().map(ToString::to_string),
            },
            "stake_meta": stake_meta,
            "distribution_meta": distribution_meta,
            "merkle_trees": merkle_trees,
        }),
    }
}
