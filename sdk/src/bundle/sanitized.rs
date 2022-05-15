use solana_sdk::transaction::SanitizedTransaction;

// TODO(seg)
pub struct SanitizedBundleBatch {
    pub sanitized_bundles: Vec<SanitizedBundle>,
}

pub struct SanitizedBundle {
    pub seq_id: usize,
    pub sanitized_transactions: Vec<SanitizedTransaction>,
}
