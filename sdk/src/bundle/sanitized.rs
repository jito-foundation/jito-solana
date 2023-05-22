#![cfg(feature = "full")]

use solana_sdk::transaction::SanitizedTransaction;

#[derive(Clone, Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<SanitizedTransaction>,
    pub bundle_id: String,
}
