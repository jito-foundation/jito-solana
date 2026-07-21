//! Stake metadata extraction for tip-router snapshots.

use {
    crate::config::TipRouterSnapshotConfig,
    serde_json::{Value, json},
    solana_runtime::bank::Bank,
};

pub fn collect_stake_meta(config: &TipRouterSnapshotConfig, bank: &Bank) -> Value {
    json!({
        "bank_slot": bank.slot(),
        "bank_epoch": bank.epoch(),
        "ncn": config.ncn.as_ref().map(ToString::to_string),
        "accounts": [],
    })
}
