//! Tip distribution metadata extraction for tip-router snapshots.

use {
    crate::config::TipRouterSnapshotConfig,
    serde_json::{Value, json},
    solana_runtime::bank::Bank,
};

pub fn collect_distribution_meta(config: &TipRouterSnapshotConfig, bank: &Bank) -> Value {
    json!({
        "bank_slot": bank.slot(),
        "bank_epoch": bank.epoch(),
        "program_ids": {
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
        "accounts": [],
    })
}
