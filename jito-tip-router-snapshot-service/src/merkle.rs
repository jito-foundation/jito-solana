//! Merkle tree construction for tip-router snapshot artifacts.

use serde_json::{Value, json};

pub fn build_merkle_trees(stake_meta: &Value, distribution_meta: &Value) -> Value {
    json!({
        "stake_meta_root": null,
        "distribution_meta_root": null,
        "stake_meta_leaf_count": stake_meta
            .get("accounts")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        "distribution_meta_leaf_count": distribution_meta
            .get("accounts")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
    })
}
