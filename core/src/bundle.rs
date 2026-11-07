use {
    itertools::Itertools, solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
};

pub fn derive_bundle_id(transactions: &[impl TransactionWithMeta]) -> String {
    let signatures = transactions.iter().map(|tx| tx.signature()).join(",");
    solana_sha256_hasher::hash(signatures.as_bytes())
        .to_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug)]
pub struct SanitizedBundle<Tx: TransactionWithMeta> {
    transactions: Vec<Tx>,
    bundle_id: String,
}

impl<Tx: TransactionWithMeta> SanitizedBundle<Tx> {
    pub fn new(transactions: Vec<Tx>) -> Self {
        let bundle_id = derive_bundle_id(&transactions);
        Self {
            transactions,
            bundle_id,
        }
    }

    pub fn transactions(&self) -> &[Tx] {
        &self.transactions
    }

    pub fn bundle_id(&self) -> &String {
        &self.bundle_id
    }
}
