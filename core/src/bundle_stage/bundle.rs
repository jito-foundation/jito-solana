use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_transaction::sanitized::SanitizedTransaction;

#[derive(Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<RuntimeTransaction<SanitizedTransaction>>,
    pub bundle_id: String,
}

pub fn derive_bundle_id_from_sanitized_transactions(
    transactions: &[RuntimeTransaction<SanitizedTransaction>],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(transactions.iter().map(|tx| tx.signature()).join(","));
    format!("{:x}", hasher.finalize())
}
