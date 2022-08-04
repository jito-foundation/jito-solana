///! Handles pre-locking bundles accounts so that accounts bundles touch can be reserved ahead
/// of time for execution. Also, ensures that ALL accounts mentioned across a bundle are locked
/// to avoid race conditions between BundleStage and banking stage.
///
/// For instance, imagine a bundle with three transactions and the set of accounts for each transaction
/// is: {{A, B}, {B, C}, {C, D}}. We need to lock A, B, and C even though only one is executed at a time.
/// Imagine BundleStage is in the middle of processing {C, D} and we didn't have a lock on accounts {A, B, C}.
/// In this situation, there's a chance that BankingStage can process a transaction containing A or B
/// and commit the results before the bundle completes. By the time the bundle commits the new account
/// state for {A, B, C}, A and B would be incorrect and the entries containing the bundle would be
/// replayed improperly and that leader would have produced an invalid block.
use {
    solana_sdk::{
        bundle::sanitized::SanitizedBundle, pubkey::Pubkey, transaction::TransactionAccountLocks,
    },
    std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        result,
    },
    uuid::Uuid,
};

#[derive(Debug)]
pub enum BundleAccountLockerError {
    LockingError(Uuid),
}

pub type Result<T> = result::Result<T, BundleAccountLockerError>;

#[derive(Clone, Default)]
pub struct BundleAccountLocker {
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
}

impl BundleAccountLocker {
    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleLockerSanitizer
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.read_locks.keys().cloned().collect()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleLockerSanitizer
    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.write_locks.keys().cloned().collect()
    }

    pub fn unlock_bundle_accounts(&mut self, sanitized_bundle: &SanitizedBundle) -> Result<()> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle)?;
        for (acc, count) in read_locks {
            if let Entry::Occupied(mut entry) = self.read_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            }
        }
        for (acc, count) in write_locks {
            if let Entry::Occupied(mut entry) = self.write_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            }
        }

        debug!("unlock read locks: {:?}", self.read_locks);
        debug!("unlock write locks: {:?}", self.write_locks);

        Ok(())
    }

    pub fn lock_bundle_accounts(&mut self, sanitized_bundle: &SanitizedBundle) -> Result<()> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle)?;
        for (acc, count) in read_locks {
            *self.read_locks.entry(acc).or_insert(0) += count;
        }
        for (acc, count) in write_locks {
            *self.write_locks.entry(acc).or_insert(0) += count;
        }

        debug!("lock read locks: {:?}", self.read_locks);
        debug!("lock write locks: {:?}", self.write_locks);

        Ok(())
    }

    /// Returns the read and write locks for this bundle
    /// Each lock type contains a HashMap which maps Pubkey to number of locks held
    fn get_read_write_locks(
        bundle: &SanitizedBundle,
    ) -> Result<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>)> {
        let transaction_locks: Vec<TransactionAccountLocks> = bundle
            .transactions
            .iter()
            .filter_map(|tx| tx.get_account_locks().ok())
            .collect();

        if transaction_locks.len() != bundle.transactions.len() {
            return Err(BundleAccountLockerError::LockingError(bundle.uuid));
        }

        let bundle_read_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.readonly.iter().map(|a| **a));
        let bundle_read_locks =
            bundle_read_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        let bundle_write_locks = transaction_locks
            .iter()
            .flat_map(|tx| tx.writable.iter().map(|a| **a));
        let bundle_write_locks =
            bundle_write_locks
                .into_iter()
                .fold(HashMap::new(), |mut map, acc| {
                    *map.entry(acc).or_insert(0) += 1;
                    map
                });

        Ok((bundle_read_locks, bundle_write_locks))
    }
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_foo() {}
}
