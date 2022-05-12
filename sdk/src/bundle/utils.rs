use {
    solana_sdk::{bundle::error::BundleExecutionError, transaction::TransactionError},
    std::result,
};

type LockResult = result::Result<(), TransactionError>;

/// Checks that preparing a bundle gives an acceptable batch back
pub fn check_bundle_lock_results(
    lock_results: &[LockResult],
) -> Option<(BundleExecutionError, usize)> {
    for (i, res) in lock_results.iter().enumerate() {
        match res {
            Ok(())
            | Err(TransactionError::AccountInUse)
            | Err(TransactionError::BundleNotContinuous) => {}
            Err(e) => {
                return Some((e.clone().into(), i));
            }
        }
    }
    None
}
