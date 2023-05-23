#![cfg(feature = "full")]

use {
    crate::transaction::VersionedTransaction,
    itertools::Itertools,
    sha2::{Digest, Sha256},
    solana_sdk::transaction::SanitizedTransaction,
};

#[derive(Clone, Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<SanitizedTransaction>,
    pub bundle_id: String,
}

pub fn derive_bundle_id(transactions: &[VersionedTransaction]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transactions.iter().map(|tx| tx.signatures[0]).join(","));
    format!("{:x}", hasher.finalize())
}
