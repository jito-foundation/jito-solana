use {crate::bundle::error::BundleExecutionError, solana_sdk::transaction::TransactionError};

type LockResult = Result<(), TransactionError>;

/// Checks that preparing a bundle gives an acceptable batch back
pub fn check_bundle_lock_results(lock_results: &[LockResult]) -> Option<(TransactionError, usize)> {
    for (i, res) in lock_results.iter().enumerate() {
        match res {
            Ok(())
            | Err(TransactionError::AccountInUse)
            | Err(TransactionError::BundleNotContinuous) => {}
            Err(e) => {
                return Some((e.clone(), i));
            }
        }
    }
    None
}

pub type BundleExecutionResult<T> = Result<T, BundleExecutionError>;
