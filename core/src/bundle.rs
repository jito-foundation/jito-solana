use {
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction::sanitized::SanitizedTransaction,
};

#[derive(Debug)]
pub struct SanitizedBundle {
    transactions: Vec<RuntimeTransaction<SanitizedTransaction>>,
    bundle_id: String,
}

impl SanitizedBundle {
    pub fn new(
        transactions: Vec<RuntimeTransaction<SanitizedTransaction>>,
        bundle_id: String,
    ) -> Self {
        Self {
            transactions,
            bundle_id,
        }
    }

    pub fn transactions(&self) -> &[RuntimeTransaction<SanitizedTransaction>] {
        &self.transactions
    }

    pub fn bundle_id(&self) -> &String {
        &self.bundle_id
    }
}
