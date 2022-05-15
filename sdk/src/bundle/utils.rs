use {solana_sdk::transaction::TransactionError, std::result};

type LockResult = result::Result<(), TransactionError>;

/// Checks that preparing a bundle gives an acceptable batch back
pub fn check_bundle_lock_results<E>(lock_results: &[LockResult]) -> Option<(E, usize)>
where
    E: From<TransactionError>,
{
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
