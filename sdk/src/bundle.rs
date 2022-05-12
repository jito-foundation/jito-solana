use solana_sdk::transaction::SanitizedTransaction;

// TODO(seg)
pub struct BundleBatch {
    pub bundles: Vec<Bundle>,
}

pub struct Bundle {
    pub transactions: Vec<SanitizedTransaction>,
}
