#![cfg(feature = "full")]

use solana_sdk::transaction::SanitizedTransaction;

pub struct SanitizedBundle {
    pub sanitized_transactions: Vec<SanitizedTransaction>,
}
