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
    ahash::HashMap,
    solana_accounts_db::{
        account_locks::validate_account_locks, accounts::TransactionAccountLocksIterator,
    },
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    std::{
        collections::hash_map::Entry,
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

#[derive(Default, Clone)]
pub struct BundleAccountLocks {
    read_locks: HashMap<Pubkey, u64>,
    write_locks: HashMap<Pubkey, u64>,
}

impl BundleAccountLocks {
    pub fn read_locks(&self) -> &HashMap<Pubkey, u64> {
        &self.read_locks
    }

    pub fn write_locks(&self) -> &HashMap<Pubkey, u64> {
        &self.write_locks
    }

    pub fn lock_accounts<'a>(
        &mut self,
        transaction_locks: Vec<impl Iterator<Item = (&'a Pubkey, bool)>>,
    ) {
        for transaction_lock in transaction_locks {
            for (acc, writable) in transaction_lock {
                if writable {
                    *self.write_locks.entry(*acc).or_insert(0) += 1;
                } else {
                    *self.read_locks.entry(*acc).or_insert(0) += 1;
                }
            }
        }
    }

    pub fn unlock_accounts<'a>(
        &mut self,
        transaction_locks: Vec<impl Iterator<Item = (&'a Pubkey, bool)>>,
    ) {
        for transaction_lock in transaction_locks {
            for (acc, writable) in transaction_lock {
                if writable {
                    let entry = self.write_locks.entry(*acc);
                    if let Entry::Occupied(mut entry) = entry {
                        *entry.get_mut() = entry.get().saturating_sub(1);
                        if *entry.get() == 0 {
                            entry.remove();
                        }
                    }
                } else {
                    let entry = self.read_locks.entry(*acc);
                    if let Entry::Occupied(mut entry) = entry {
                        *entry.get_mut() = entry.get().saturating_sub(1);
                        if *entry.get() == 0 {
                            entry.remove();
                        }
                    }
                }
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
    pub fn account_locks(&self) -> MutexGuard<'_, BundleAccountLocks> {
        self.account_locks.lock().unwrap()
    }

    /// Prepares a locked bundle and returns a LockedBundle containing locked accounts.
    /// When a LockedBundle is dropped, the accounts are automatically unlocked
    pub fn lock_bundle<Tx: TransactionWithMeta>(
        &self,
        transactions: &[Tx],
        bank: &Bank,
    ) -> BundleAccountLockerResult<()> {
        let transaction_locks = Self::get_transaction_locks(transactions, bank)?;

        self.account_locks
            .lock()
            .unwrap()
            .lock_accounts(transaction_locks);
        Ok(())
    }

    /// Unlocks bundle accounts. Note that LockedBundle::drop will auto-drop the bundle account locks
    pub fn unlock_bundle<Tx: TransactionWithMeta>(
        &self,
        transactions: &[Tx],
        bank: &Bank,
    ) -> BundleAccountLockerResult<()> {
        let transaction_locks = Self::get_transaction_locks(transactions, bank)?;

        self.account_locks
            .lock()
            .unwrap()
            .unlock_accounts(transaction_locks);
        Ok(())
    }

    /// Returns the read and write locks for this bundle
    /// Each lock type contains a HashMap which maps Pubkey to number of locks held
    fn get_transaction_locks<'a, Tx: TransactionWithMeta>(
        transactions: &'a [Tx],
        bank: &Bank,
    ) -> BundleAccountLockerResult<Vec<impl Iterator<Item = (&'a Pubkey, bool)> + use<'a, Tx>>>
    {
        let transaction_locks = transactions
            .iter()
            .filter_map(|tx| {
                if validate_account_locks(
                    tx.account_keys(),
                    bank.get_transaction_account_lock_limit(),
                )
                .is_ok()
                {
                    Some(TransactionAccountLocksIterator::new(tx).accounts_with_is_writable())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        if transaction_locks.len() != transactions.len() {
            return Err(BundleAccountLockerError::LockingError);
        }

        Ok(transaction_locks)
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::bundle_stage::{
            bundle_account_locker::BundleAccountLocker,
            bundle_packet_deserializer::BundlePacketDeserializer,
        },
        ahash::HashMap,
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::BytesPacket,
        solana_pubkey::Pubkey,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_signer::Signer,
        solana_system_transaction::transfer,
        solana_transaction::versioned::VersionedTransaction,
        std::{collections::HashSet, sync::Arc},
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

        let tx0_data = Arc::new(
            BytesPacket::from_data(None, &tx0)
                .unwrap()
                .data(..)
                .unwrap()
                .to_vec(),
        );

        let tx1_data = Arc::new(
            BytesPacket::from_data(None, &tx1)
                .unwrap()
                .data(..)
                .unwrap()
                .to_vec(),
        );

        let mut tx0 = BundlePacketDeserializer::try_handle_packet(
            tx0_data,
            &bank,
            &bank,
            false,
            bank.get_transaction_account_lock_limit(),
            &HashSet::default(),
        )
        .unwrap();
        let tx0 = vec![tx0.take_transaction_for_scheduling().0];

        let mut tx1 = BundlePacketDeserializer::try_handle_packet(
            tx1_data,
            &bank,
            &bank,
            false,
            bank.get_transaction_account_lock_limit(),
            &HashSet::default(),
        )
        .unwrap();
        let tx1 = vec![tx1.take_transaction_for_scheduling().0];

        bundle_account_locker.lock_bundle(&tx0, &bank).unwrap();

        assert_eq!(
            bundle_account_locker
                .account_locks()
                .write_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey()])
        );
        assert_eq!(
            bundle_account_locker
                .account_locks()
                .read_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([solana_system_interface::program::id()])
        );

        bundle_account_locker.lock_bundle(&tx1, &bank).unwrap();
        assert_eq!(
            bundle_account_locker
                .account_locks()
                .write_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([mint_keypair.pubkey(), kp0.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker
                .account_locks()
                .read_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([solana_system_interface::program::id()])
        );

        bundle_account_locker.unlock_bundle(&tx0, &bank).unwrap();

        assert_eq!(
            bundle_account_locker
                .account_locks()
                .write_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([mint_keypair.pubkey(), kp1.pubkey()])
        );
        assert_eq!(
            bundle_account_locker
                .account_locks()
                .read_locks()
                .keys()
                .cloned()
                .collect::<HashSet<Pubkey>>(),
            HashSet::from_iter([solana_system_interface::program::id()])
        );

        bundle_account_locker.unlock_bundle(&tx1, &bank).unwrap();

        assert!(bundle_account_locker
            .account_locks()
            .write_locks()
            .is_empty());
        assert!(bundle_account_locker
            .account_locks()
            .read_locks()
            .is_empty());
    }

    #[test]
    fn test_lock_bundles_duplicate_accounts() {
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
        let tx0_data = Arc::new(
            BytesPacket::from_data(None, &tx0)
                .unwrap()
                .data(..)
                .unwrap()
                .to_vec(),
        );
        let tx1_data = Arc::new(
            BytesPacket::from_data(None, &tx1)
                .unwrap()
                .data(..)
                .unwrap()
                .to_vec(),
        );

        let mut tx0 = BundlePacketDeserializer::try_handle_packet(
            tx0_data,
            &bank,
            &bank,
            false,
            bank.get_transaction_account_lock_limit(),
            &HashSet::default(),
        )
        .unwrap();
        let tx0 = vec![tx0.take_transaction_for_scheduling().0];
        let mut tx1 = BundlePacketDeserializer::try_handle_packet(
            tx1_data,
            &bank,
            &bank,
            false,
            bank.get_transaction_account_lock_limit(),
            &HashSet::default(),
        )
        .unwrap();
        let tx1 = vec![tx1.take_transaction_for_scheduling().0];

        bundle_account_locker.lock_bundle(&tx0, &bank).unwrap();
        bundle_account_locker.lock_bundle(&tx1, &bank).unwrap();
        assert_eq!(
            bundle_account_locker.account_locks().write_locks(),
            &HashMap::from_iter([
                (mint_keypair.pubkey(), 2),
                (kp0.pubkey(), 1),
                (kp1.pubkey(), 1)
            ])
        );
        assert_eq!(
            bundle_account_locker.account_locks().read_locks(),
            &HashMap::from_iter([(solana_system_interface::program::id(), 2)])
        );
        bundle_account_locker.unlock_bundle(&tx0, &bank).unwrap();
        bundle_account_locker.unlock_bundle(&tx1, &bank).unwrap();

        assert_eq!(
            bundle_account_locker.account_locks().write_locks(),
            &HashMap::default()
        );
        assert_eq!(
            bundle_account_locker.account_locks().read_locks(),
            &HashMap::default()
        );
    }
}
