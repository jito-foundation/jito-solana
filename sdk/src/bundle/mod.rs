#![cfg(feature = "full")]

use {
    crate::transaction::{SanitizedTransaction, VersionedTransaction},
    digest::Digest,
    itertools::Itertools,
    sha2::Sha256,
};

#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize)]
pub struct VersionedBundle {
    pub transactions: Vec<VersionedTransaction>,
}

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

pub fn derive_bundle_id_from_sanitized_transactions(
    transactions: &[SanitizedTransaction],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transactions.iter().map(|tx| tx.signature()).join(","));
    format!("{:x}", hasher.finalize())
}
