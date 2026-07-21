//! Stake metadata extraction for tip-router snapshots.

use {
    crate::config::TipRouterSnapshotConfig,
    serde_json::{json, Value},
    solana_runtime::bank::Bank,
    std::sync::Arc,
};

pub fn collect_stake_meta(config: &TipRouterSnapshotConfig, bank: &Arc<Bank>) -> Value {
    #[cfg(feature = "stake-meta-gen")]
    {
        if let Some(value) = generate_real_stake_meta(config, bank) {
            return value;
        }
    }

    json!({
        "bank_slot": bank.slot(),
        "bank_epoch": bank.epoch(),
        "ncn": config.ncn.as_ref().map(ToString::to_string),
        "accounts": [],
    })
}

/// Run the ported tip-router generator when all required program IDs are
/// configured. Returns `None` (and falls back to the placeholder) on missing
/// config or generation/serialization failure.
#[cfg(feature = "stake-meta-gen")]
fn generate_real_stake_meta(config: &TipRouterSnapshotConfig, bank: &Arc<Bank>) -> Option<Value> {
    let (
        Some(tip_distribution_program_id),
        Some(priority_fee_distribution_program_id),
        Some(tip_payment_program_id),
    ) = (
        config.tip_distribution_program_id,
        config.priority_fee_distribution_program_id,
        config.tip_payment_program_id,
    )
    else {
        log::warn!(
            "stake-meta-gen enabled but program IDs are missing in config; using placeholder"
        );
        return None;
    };

    match crate::stake_meta_generator::generate_stake_meta_collection(
        bank,
        &tip_distribution_program_id,
        &priority_fee_distribution_program_id,
        &tip_payment_program_id,
    ) {
        Ok(collection) => match serde_json::to_value(&collection) {
            Ok(value) => Some(value),
            Err(err) => {
                log::error!("failed to serialize stake meta collection: {err}");
                None
            }
        },
        Err(err) => {
            log::error!("failed to generate stake meta collection: {err}");
            None
        }
    }
}
