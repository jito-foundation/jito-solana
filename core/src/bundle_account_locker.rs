use {
    crate::packet_bundle::PacketBundle,
    solana_sdk::loader_instruction::write,
    std::{
        borrow::BorrowMut,
        sync::{Arc, Mutex},
    },
};
///! Handles pre-locking bundle accounts so that accounts bundles touch can be reserved ahead
/// of time for execution. Also, ensures that ALL accounts mentioned across a bundle are locked
/// to avoid race conditions between BundleStage and BankingStage.
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
    LockingError,
}

pub type Result<T> = result::Result<T, BundleAccountLockerError>;

pub struct LockedBundle<'a, 'b> {
    bundle_account_locker: &'a BundleAccountLocker,
    sanitized_bundle: &'b SanitizedBundle,
}

impl<'a, 'b> LockedBundle<'a, 'b> {
    pub fn new(
        bundle_account_locker: &'a BundleAccountLocker,
        sanitized_bundle: &'b SanitizedBundle,
    ) -> Self {
        Self {
            bundle_account_locker,
            sanitized_bundle,
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
            .unlock_bundle_accounts(self.sanitized_bundle);
    }
}

#[derive(Default, Clone)]
struct BundleAccountLocks {
    read_locks: Arc<Mutex<HashMap<Pubkey, u64>>>,
    write_locks: Arc<Mutex<HashMap<Pubkey, u64>>>,
}

impl BundleAccountLocks {
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.read_locks.lock().unwrap().keys().cloned().collect()
    }

    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.write_locks.lock().unwrap().keys().cloned().collect()
    }

    pub fn lock_accounts(
        &self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        let mut read_locks_l = self.read_locks.lock().unwrap();
        let mut write_locks_l = self.write_locks.lock().unwrap();
        for (acc, count) in read_locks {
            *read_locks_l.entry(acc).or_insert(0) += count;
        }
        for (acc, count) in write_locks {
            *write_locks_l.entry(acc).or_insert(0) += count;
        }
    }

    pub fn unlock_accounts(
        &self,
        read_locks: HashMap<Pubkey, u64>,
        write_locks: HashMap<Pubkey, u64>,
    ) {
        let mut read_locks_l = self.read_locks.lock().unwrap();
        let mut write_locks_l = self.write_locks.lock().unwrap();

        for (acc, count) in read_locks {
            if let Entry::Occupied(mut entry) = read_locks_l.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            }
        }
        for (acc, count) in write_locks {
            if let Entry::Occupied(mut entry) = write_locks_l.entry(acc) {
                let val = entry.get_mut();
                *val = val.saturating_sub(count);
                if entry.get() == &0 {
                    let _ = entry.remove();
                }
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct BundleAccountLocker {
    account_locks: BundleAccountLocks,
}

impl BundleAccountLocker {
    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn read_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.read_locks()
    }

    /// used in BankingStage during TransactionBatch construction to ensure that BankingStage
    /// doesn't lock anything currently locked in the BundleAccountLocker
    pub fn write_locks(&self) -> HashSet<Pubkey> {
        self.account_locks.write_locks()
    }

    pub fn unlock_bundle_accounts(&self, sanitized_bundle: &SanitizedBundle) -> Result<()> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle)?;

        self.account_locks.unlock_accounts(read_locks, write_locks);
        Ok(())
    }

    pub fn lock_bundle_accounts<'a, 'b>(
        &'a self,
        sanitized_bundle: &'b SanitizedBundle,
    ) -> Result<LockedBundle<'a, 'b>> {
        let (read_locks, write_locks) = Self::get_read_write_locks(sanitized_bundle)?;

        self.account_locks.lock_accounts(read_locks, write_locks);
        Ok(LockedBundle::new(self, sanitized_bundle))
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
            bundle_account_locker::BundleAccountLocker, bundle_sanitizer::BundleSanitizer,
            packet_bundle::PacketBundle,
        },
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            packet::Packet, pubkey::Pubkey, signature::Signer, signer::keypair::Keypair,
            system_program, system_transaction::transfer, transaction::VersionedTransaction,
        },
        std::{collections::HashSet, sync::Arc},
        uuid::Uuid,
    };

    #[test]
    fn test_simple_lock_bundles() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_no_wallclock_throttle_for_tests(&genesis_config));

        let bundle_sanitizer = BundleSanitizer::new(&Pubkey::new_unique());

        let mut bundle_account_locker = BundleAccountLocker::default();

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

        let packet_bundle0 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx0).unwrap()]),
            uuid: Uuid::new_v4(),
        };
        let packet_bundle1 = PacketBundle {
            batch: PacketBatch::new(vec![Packet::from_data(None, &tx1).unwrap()]),
            uuid: Uuid::new_v4(),
        };

        let sanitized_bundle0 = bundle_sanitizer
            .get_sanitized_bundle(&packet_bundle0, &bank, &HashSet::default())
            .unwrap();
        let sanitized_bundle1 = bundle_sanitizer
            .get_sanitized_bundle(&packet_bundle1, &bank, &HashSet::default())
            .unwrap();

        bundle_account_locker
            .lock_bundle_accounts(&sanitized_bundle0)
            .unwrap();
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        bundle_account_locker
            .lock_bundle_accounts(&sanitized_bundle1)
            .unwrap();
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        bundle_account_locker
            .unlock_bundle_accounts(&sanitized_bundle0)
            .unwrap();
        assert_eq!(
            bundle_account_locker.write_locks(),
            HashSet::from_iter([mint_keypair.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker.read_locks(),
            HashSet::from_iter([system_program::id()])
        );

        bundle_account_locker
            .unlock_bundle_accounts(&sanitized_bundle1)
            .unwrap();
        assert!(bundle_account_locker.write_locks().is_empty());
        assert!(bundle_account_locker.read_locks().is_empty());
    }
}
