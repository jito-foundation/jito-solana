use solana_runtime_transaction::runtime_transaction::RuntimeTransaction;
use solana_transaction::sanitized::SanitizedTransaction;

#[derive(Debug)]
pub struct SanitizedBundle {
    pub transactions: Vec<RuntimeTransaction<SanitizedTransaction>>,
    pub bundle_id: String,
}
