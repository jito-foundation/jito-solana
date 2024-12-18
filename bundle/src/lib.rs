use {
    itertools::Itertools,
    sha2::{Digest, Sha256},
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
};

pub fn derive_bundle_id(transactions: &[impl TransactionWithMeta]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transactions.iter().map(|tx| tx.signature()).join(","));
    format!("{:x}", hasher.finalize())
}
