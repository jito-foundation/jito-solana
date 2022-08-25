//! Handles pre-locking bundle accounts so that accounts bundles touch can be reserved ahead
// of time for execution. Also, ensures that ALL accounts mentioned across a bundle are locked
// to avoid race conditions between BundleStage and BankingStage.
//
// For instance, imagine a bundle with three transactions and the set of accounts for each transaction
// is: {{A, B}, {B, C}, {C, D}}. We need to lock A, B, and C even though only one is executed at a time.
// Imagine BundleStage is in the middle of processing {C, D} and we didn't have a lock on accounts {A, B, C}.
// In this situation, there's a chance that BankingStage can process a transaction containing A or B
// and commit the results before the bundle completes. By the time the bundle commits the new account
// state for {A, B, C}, A and B would be incorrect and the entries containing the bundle would be
// replayed improperly and that leader would have produced an invalid block.
use {
    solana_runtime::bank::Bank,
    solana_sdk::{bundle::SanitizedBundle, pubkey::Pubkey, transaction::TransactionAccountLocks},
    std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        sync::{Arc, Mutex, MutexGuard},
    },
    thiserror::Error,
};

#[derive(Clone, Error, Debug)]
pub enum BundleAccountLockerError {
    #[error("locking error")]
    LockingError,
}

pub type BundleAccountLockerResult<T> = Result<T, BundleAccountLockerError>;

pub struct LockedBundle<'a, 'b> {
    bundle_account_locker: &'a BundleAccountLocker,
    sanitized_bundle: &'b SanitizedBundle,
    bank: Arc<Bank>,
}

impl<'a, 'b> LockedBundle<'a, 'b> {
    pub fn new(
        bundle_account_locker: &'a BundleAccountLocker,
        sanitized_bundle: &'b SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> Self {
        Self {
            bundle_account_locker,
            sanitized_bundle,
            bank: bank.clone(),
        }
    }

    pub fn sanitized_bundle(&self) -> &SanitizedBundle {
        self.sanitized_bundle
    }
}

// Automatically unlock bundle accounts when destructed
impl<'a, 'b> Drop for LockedBundle<'a, 'b> {
    fn drop(&mut self) {
        let _ = self
            .bundle_account_locker
            .unlock_bundle_accounts(self.sanitized_bundle, &self.bank);
    }
}

#[derive(Default, Clone)]
pub struct BundleAccountLocks {
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
}

impl BundleAccountLocks {
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.read_locks.keys().cloned().collect()
    }

    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.write_locks.keys().cloned().collect()
    }

    pub fn lock_accounts(
        &mut self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        for (acc, count) in read_locks {
            *self.read_locks.entry(acc).or_insert(0) += count;
        }
        for (acc, count) in write_locks {
            *self.write_locks.entry(acc).or_insert(0) += count;
        }
    }

    pub fn unlock_accounts(
        &mut self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        for (acc, count) in read_locks {
            if let Entry::Occupied(mut entry) = self.read_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            } else {
                warn!("error unlocking read-locked account, account: {:?}", acc);
            }
        }
        for (acc, count) in write_locks {
            if let Entry::Occupied(mut entry) = self.write_locks.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            } else {
                warn!("error unlocking write-locked account, account: {:?}", acc);
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct BundleAccountLocker {
    account_locks: Arc<Mutex<BundleAccountLocks>>,
}

impl BundleAccountLocker {
    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.lock().unwrap().read_locks()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.lock().unwrap().write_locks()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn account_locks(&self) -> MutexGuard<BundleAccountLocks> {
        self.account_locks.lock().unwrap()
    }

    /// Prepares a locked bundle and returns a LockedBundle containing locked accounts.
    /// When a LockedBundle is dropped, the accounts are automatically unlocked
    pub fn prepare_locked_bundle<'a, 'b>(
        &'a self,
        sanitized_bundle: &'b SanitizedBundle,
        bank: &Arc<Bank>,
    ) -> BundleAccountLockerResult<LockedBundle<'a, 'b>> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle, bank)?;

        self.account_locks
            .lock()
            .unwrap()
            .lock_accounts(read_locks, write_locks);
        Ok(LockedBundle::new(self, sanitized_bundle, bank))
    }

    /// Unlocks bundle accounts. Note that LockedBundle::drop will auto-drop the bundle account locks
    fn unlock_bundle_accounts(
        &self,
        sanitized_bundle: &SanitizedBundle,
        bank: &Bank,
    ) -> BundleAccountLockerResult<()> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle, bank)?;

        self.account_locks
            .lock()
            .unwrap()
            .unlock_accounts(read_locks, write_locks);
        Ok(())
    }

    /// Returns the read and write locks for this bundle
    /// Each lock type contains a HashMap which maps Pubkey to number of locks held
    fn get_read_write_locks(
        bundle: &SanitizedBundle,
        bank: &Bank,
    ) -> BundleAccountLockerResult<(HashMap<Pubkey, u64>, HashMap<Pubkey, u64>)> {
        let transaction_locks: Vec<TransactionAccountLocks> = bundle
            .transactions
            .iter()
            .filter_map(|tx| {
                tx.get_account_locks(bank.get_transaction_account_lock_limit())
                    .ok()
            })
            .collect();

        if transaction_locks.len() != bundle.transactions.len() {
            return Err(BundleAccountLockerError::LockingError);
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
    use {
        crate::{
            bundle_stage::bundle_account_locker::BundleAccountLocker,
            immutable_deserialized_bundle::ImmutableDeserializedBundle,
            packet_bundle::PacketBundle,
        },
        solana_accounts_db::transaction_error_metrics::TransactionErrorMetrics,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            packet::Packet, signature::Signer, signer::keypair::Keypair, system_program,
            system_transaction::transfer, transaction::VersionedTransaction,
        },
        std::collections::HashSet,
    };

    #[test]
    fn test_simple_lock_bundles() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let (bank, _) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let bundle_account_locker = BundleAccountLocker::default();

        let kp0 = Keypair::new();
        let kp1 = Keypair::new();

        let tx0 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp0.pubkey(),
            1,
            genesis_config.hash(),
        ));
        let tx1 = VersionedTransaction::from(transfer(
            &mint_keypair,
            &kp1.pubkey(),
            1,
            genesis_config.hash(),
        ));

        let mut packet_bundle0 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx0).unwrap()]),
            bundle_id: tx0.signatures[0].to_string(),
        };
        let mut packet_bundle1 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx1).unwrap()]),
            bundle_id: tx1.signatures[0].to_string(),
        };

        let mut transaction_errors = TransactionErrorMetrics::default();

        let sanitized_bundle0 = ImmutableDeserializedBundle::new(&mut packet_bundle0, None)
            .unwrap()
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors)
            .expect("sanitize bundle 0");
        let sanitized_bundle1 = ImmutableDeserializedBundle::new(&mut packet_bundle1, None)
            .unwrap()
            .build_sanitized_bundle(&bank, &HashSet::default(), &mut transaction_errors)
            .expect("sanitize bundle 1");

        let locked_bundle0 = bundle_account_locker
            .prepare_locked_bundle(&sanitized_bundle0, &bank)
            .unwrap();

        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        let locked_bundle1 = bundle_account_locker
            .prepare_locked_bundle(&sanitized_bundle1, &bank)
            .unwrap();
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        drop(locked_bundle0);
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        drop(locked_bundle1);
        assert!(bundle_account_locker.write_locks().is_empty());
        assert!(bundle_account_locker.read_locks().is_empty());
    }
}
