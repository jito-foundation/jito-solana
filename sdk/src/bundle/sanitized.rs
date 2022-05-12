use solana_sdk::transaction::SanitizedTransaction;
// TODO(seg)
pub struct SanitizedBundleBatch {
    pub bundles: Vec<Bundle>,
}

pub struct SanitizedBundle {
    pub transactions: Vec<SanitizedTransaction>,
}
