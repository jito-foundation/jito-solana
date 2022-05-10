use crate::transaction::Transaction;

// TODO(seg)
pub struct BundleBatch {
    pub bundles: Vec<Bundle>,
}

pub struct Bundle {
    pub transactions: Vec<Transaction>,
}
