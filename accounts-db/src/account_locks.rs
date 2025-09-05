#[cfg(feature = "dev-context-only-utils")]
use qualifier_attr::qualifiers;
use {
    ahash::{AHashMap, AHashSet},
    solana_message::AccountKeys,
    solana_pubkey::Pubkey,
    solana_transaction::sanitized::MAX_TX_ACCOUNT_LOCKS,
    solana_transaction_error::{TransactionError, TransactionResult},
    std::{cell::RefCell, collections::hash_map},
};

#[derive(Debug, Default)]
pub struct AccountLocks {
    write_locks: AHashMap<Pubkey, u64>,
    readonly_locks: AHashMap<Pubkey, u64>,
}

impl AccountLocks {
    /// Lock the account keys in `keys` for a transaction.
    /// The bool in the tuple indicates if the account is writable.
    /// Returns an error if any of the accounts are already locked in a way
    /// that conflicts with the requested lock.
    /// NOTE this is the pre-SIMD83 logic and can be removed once SIMD83 is active.
    pub fn try_lock_accounts<'a>(
        &mut self,
        keys: impl Iterator<Item = (&'a Pubkey, bool)> + Clone,
        is_read_prelocked_callback: &impl Fn(&Pubkey) -> bool,
        is_write_prelocked_callback: &impl Fn(&Pubkey) -> bool,
    ) -> TransactionResult<()> {
        self.can_lock_accounts(
            keys.clone(),
            is_read_prelocked_callback,
            is_write_prelocked_callback,
        )?;
        self.lock_accounts(keys);

        Ok(())
    }

    /// Lock accounts for all transactions in a batch which don't conflict
    /// with existing locks. Returns a vector of `TransactionResult` indicating
    /// success or failure for each transaction in the batch.
    /// NOTE this is the SIMD83 logic; after the feature is active, it becomes
    /// the only logic, and this note can be removed with the feature gate.
    pub fn try_lock_transaction_batch<'a>(
        &mut self,
        mut validated_batch_keys: Vec<
            TransactionResult<impl Iterator<Item = (&'a Pubkey, bool)> + Clone>,
        >,
        is_read_locked_callback: &impl Fn(&Pubkey) -> bool,
        is_write_locked_callback: &impl Fn(&Pubkey) -> bool,
    ) -> Vec<TransactionResult<()>> {
        validated_batch_keys.iter_mut().for_each(|validated_keys| {
            if let Ok(ref keys) = validated_keys {
                if let Err(e) = self.can_lock_accounts(
                    keys.clone(),
                    is_read_locked_callback,
                    is_write_locked_callback,
                ) {
                    *validated_keys = Err(e);
                }
            }
        });

        validated_batch_keys
            .into_iter()
            .map(|available_keys| available_keys.map(|keys| self.lock_accounts(keys)))
            .collect()
    }

    /// Unlock the account keys in `keys` after a transaction.
    /// The bool in the tuple indicates if the account is writable.
    /// In debug-mode this function will panic if an attempt is made to unlock
    /// an account that wasn't locked in the way requested.
    pub fn unlock_accounts<'a>(&mut self, keys: impl Iterator<Item = (&'a Pubkey, bool)>) {
        for (k, writable) in keys {
            if writable {
                self.unlock_write(k);
            } else {
                self.unlock_readonly(k);
            }
        }
    }

    fn can_lock_accounts<'a>(
        &self,
        keys: impl Iterator<Item = (&'a Pubkey, bool)>,
        is_read_prelocked_callback: &impl Fn(&Pubkey) -> bool,
        is_write_prelocked_callback: &impl Fn(&Pubkey) -> bool,
    ) -> TransactionResult<()> {
        for (key, writable) in keys {
            if writable {
                if !self.can_write_lock(key)
                    || is_read_prelocked_callback(key)
                    || is_write_prelocked_callback(key)
                {
                    return Err(TransactionError::AccountInUse);
                }
            } else if !self.can_read_lock(key) || is_write_prelocked_callback(key) {
                return Err(TransactionError::AccountInUse);
            }
        }

        Ok(())
    }

    fn lock_accounts<'a>(&mut self, keys: impl Iterator<Item = (&'a Pubkey, bool)>) {
        for (key, writable) in keys {
            if writable {
                self.lock_write(key);
            } else {
                self.lock_readonly(key);
            }
        }
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn is_locked_readonly(&self, key: &Pubkey) -> bool {
        self.readonly_locks.get(key).is_some_and(|count| *count > 0)
    }

    #[cfg_attr(feature = "dev-context-only-utils", qualifiers(pub))]
    fn is_locked_write(&self, key: &Pubkey) -> bool {
        self.write_locks.get(key).is_some_and(|count| *count > 0)
    }

    fn can_read_lock(&self, key: &Pubkey) -> bool {
        // If the key is not write-locked, it can be read-locked
        !self.is_locked_write(key)
    }

    fn can_write_lock(&self, key: &Pubkey) -> bool {
        // If the key is not read-locked or write-locked, it can be write-locked
        !self.is_locked_readonly(key) && !self.is_locked_write(key)
    }

    fn lock_readonly(&mut self, key: &Pubkey) {
        *self.readonly_locks.entry(*key).or_default() += 1;
    }

    fn lock_write(&mut self, key: &Pubkey) {
        *self.write_locks.entry(*key).or_default() += 1;
    }

    fn unlock_readonly(&mut self, key: &Pubkey) {
        if let hash_map::Entry::Occupied(mut occupied_entry) = self.readonly_locks.entry(*key) {
            let count = occupied_entry.get_mut();
            *count -= 1;
            if *count == 0 {
                occupied_entry.remove_entry();
            }
        } else {
            debug_assert!(
                false,
                "Attempted to remove a read-lock for a key that wasn't read-locked"
            );
        }
    }

    fn unlock_write(&mut self, key: &Pubkey) {
        if let hash_map::Entry::Occupied(mut occupied_entry) = self.write_locks.entry(*key) {
            let count = occupied_entry.get_mut();
            *count -= 1;
            if *count == 0 {
                occupied_entry.remove_entry();
            }
        } else {
            debug_assert!(
                false,
                "Attempted to remove a write-lock for a key that wasn't write-locked"
            );
        }
    }
}

/// Validate account locks before locking.
pub fn validate_account_locks(
    account_keys: AccountKeys,
    tx_account_lock_limit: usize,
) -> TransactionResult<()> {
    if account_keys.len() > tx_account_lock_limit {
        Err(TransactionError::TooManyAccountLocks)
    } else if has_duplicates(account_keys) {
        Err(TransactionError::AccountLoadedTwice)
    } else {
        Ok(())
    }
}

thread_local! {
    static HAS_DUPLICATES_SET: RefCell<AHashSet<Pubkey>> = RefCell::new(AHashSet::with_capacity(MAX_TX_ACCOUNT_LOCKS));
}

/// Check for duplicate account keys.
fn has_duplicates(account_keys: AccountKeys) -> bool {
    // Benchmarking has shown that for sets of 32 or more keys, it is faster to
    // use a HashSet to check for duplicates.
    // For smaller sets a brute-force O(n^2) check seems to be faster.
    const USE_ACCOUNT_LOCK_SET_SIZE: usize = 32;
    if account_keys.len() >= USE_ACCOUNT_LOCK_SET_SIZE {
        HAS_DUPLICATES_SET.with_borrow_mut(|set| {
            let has_duplicates = account_keys.iter().any(|key| !set.insert(*key));
            set.clear();
            has_duplicates
        })
    } else {
        for (idx, key) in account_keys.iter().enumerate() {
            for jdx in idx + 1..account_keys.len() {
                if key == &account_keys[jdx] {
                    return true;
                }
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use {super::*, solana_message::v0::LoadedAddresses};

    #[test]
    fn test_account_locks() {
        let mut account_locks = AccountLocks::default();

        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();

        // Add write and read-lock.
        let result = account_locks.try_lock_accounts(
            [(&key1, true), (&key2, false)].into_iter(),
            &|_| false,
            &|_| false,
        );
        assert!(result.is_ok());

        // Try to add duplicate write-lock.
        let result =
            account_locks.try_lock_accounts([(&key1, true)].into_iter(), &|_| false, &|_| false);
        assert_eq!(result, Err(TransactionError::AccountInUse));

        // Try to add write lock on read-locked account.
        let result =
            account_locks.try_lock_accounts([(&key2, true)].into_iter(), &|_| false, &|_| false);
        assert_eq!(result, Err(TransactionError::AccountInUse));

        // Try to add read lock on write-locked account.
        let result =
            account_locks.try_lock_accounts([(&key1, false)].into_iter(), &|_| false, &|_| false);
        assert_eq!(result, Err(TransactionError::AccountInUse));

        // Add read lock on read-locked account.
        let result =
            account_locks.try_lock_accounts([(&key2, false)].into_iter(), &|_| false, &|_| false);
        assert!(result.is_ok());

        // Unlock write and read locks.
        account_locks.unlock_accounts([(&key1, true), (&key2, false)].into_iter());

        // No more remaining write-locks. Read-lock remains.
        assert!(!account_locks.is_locked_write(&key1));
        assert!(account_locks.is_locked_readonly(&key2));

        // Unlock read lock.
        account_locks.unlock_accounts([(&key2, false)].into_iter());
        assert!(!account_locks.is_locked_readonly(&key2));
    }

    #[test]
    fn test_validate_account_locks_valid_no_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let account_keys = AccountKeys::new(static_keys, None);
        assert!(validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS).is_ok());
    }

    #[test]
    fn test_validate_account_locks_too_many_no_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let account_keys = AccountKeys::new(static_keys, None);
        assert_eq!(
            validate_account_locks(account_keys, 1),
            Err(TransactionError::TooManyAccountLocks)
        );
    }

    #[test]
    fn test_validate_account_locks_duplicate_no_dynamic() {
        let duplicate_key = Pubkey::new_unique();
        let static_keys = &[duplicate_key, Pubkey::new_unique(), duplicate_key];
        let account_keys = AccountKeys::new(static_keys, None);
        assert_eq!(
            validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS),
            Err(TransactionError::AccountLoadedTwice)
        );
    }

    #[test]
    fn test_validate_account_locks_valid_dynamic() {
        let static_keys = &[Pubkey::new_unique(), Pubkey::new_unique()];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![Pubkey::new_unique()],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert!(validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS).is_ok());
    }

    #[test]
    fn test_validate_account_locks_too_many_dynamic() {
        let static_keys = &[Pubkey::new_unique()];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![Pubkey::new_unique()],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert_eq!(
            validate_account_locks(account_keys, 2),
            Err(TransactionError::TooManyAccountLocks)
        );
    }

    #[test]
    fn test_validate_account_locks_duplicate_dynamic() {
        let duplicate_key = Pubkey::new_unique();
        let static_keys = &[duplicate_key];
        let dynamic_keys = LoadedAddresses {
            writable: vec![Pubkey::new_unique()],
            readonly: vec![duplicate_key],
        };
        let account_keys = AccountKeys::new(static_keys, Some(&dynamic_keys));
        assert_eq!(
            validate_account_locks(account_keys, MAX_TX_ACCOUNT_LOCKS),
            Err(TransactionError::AccountLoadedTwice)
        );
    }

    #[test]
    fn test_has_duplicates_small() {
        let mut keys = (0..16).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let account_keys = AccountKeys::new(&keys, None);
        assert!(!has_duplicates(account_keys));

        keys[14] = keys[3]; // Duplicate key
        let account_keys = AccountKeys::new(&keys, None);
        assert!(has_duplicates(account_keys));
    }

    #[test]
    fn test_has_duplicates_large() {
        let mut keys = (0..64).map(|_| Pubkey::new_unique()).collect::<Vec<_>>();
        let account_keys = AccountKeys::new(&keys, None);
        assert!(!has_duplicates(account_keys));

        keys[47] = keys[3]; // Duplicate key
        let account_keys = AccountKeys::new(&keys, None);
        assert!(has_duplicates(account_keys));
    }

    #[test]
    fn test_additional_read_locks() {
        let mut account_locks = AccountLocks::default();

        let key1 = Pubkey::new_unique();

        // write lock key1 while key1 in additional read locks will fail
        let result = account_locks.try_lock_accounts(
            [(&key1, true)].into_iter(),
            &|key| *key == key1,
            &|_| false,
        );
        assert_eq!(result, Err(TransactionError::AccountInUse));

        // read lock key1 while key1 in additional read locks will be ok
        let result = account_locks.try_lock_accounts(
            [(&key1, false)].into_iter(),
            &|key| *key == key1,
            &|_| false,
        );
        assert_eq!(result, Ok(()));
    }

    #[test]
    fn test_additional_write_locks() {
        let mut account_locks = AccountLocks::default();

        let key1 = Pubkey::new_unique();

        // write lock key1 while key1 in addition write locks will fail
        let result =
            account_locks
                .try_lock_accounts([(&key1, true)].into_iter(), &|_| false, &|key| *key == key1);
        assert_eq!(result, Err(TransactionError::AccountInUse));

        // read lock key1 while key1 in addition write locks will fail
        let result =
            account_locks.try_lock_accounts([(&key1, false)].into_iter(), &|_| false, &|key| {
                *key == key1
            });
        assert_eq!(result, Err(TransactionError::AccountInUse));
    }
}
