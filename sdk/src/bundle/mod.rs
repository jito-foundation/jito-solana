use crate::transaction::Transaction;

pub mod error;
pub mod sanitized;
pub mod utils;

#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize)]
pub struct BundleBatch {
    pub bundles: Vec<Bundle>,
}

impl BundleBatch {
    pub fn new(mut bundles: Vec<Bundle>) -> Self {
        for (i, mut b) in bundles.iter_mut().enumerate() {
            b.seq_id = i;
        }

        Self { bundles }
    }
}

// TODO(seg): maybe derive `AbiExample`
#[derive(Debug, PartialEq, Default, Eq, Clone, Serialize, Deserialize)]
pub struct Bundle {
    pub seq_id: usize,
    pub transactions: Vec<Transaction>,
}
