use {
    // Importing necessary components from the current crate and Solana libraries
    crate::accounts_db::AccountsDb, 
    solana_account::AccountSharedData, 
    solana_clock::Slot,
    solana_pubkey::Pubkey, 
    solana_transaction::sanitized::SanitizedTransaction,
};

// Implementation block for methods attached to the AccountsDb struct
impl AccountsDb {
    /// Notifies the registered AccountsUpdateNotifier (Geyser Plugin) about an account update
    /// triggered by transaction activity at a specific slot.
    pub fn notify_account_at_accounts_update(
        &self,
        slot: Slot,
        account: &AccountSharedData,
        // Optional transaction context. Using &Option<&SanitizedTransaction> avoids cloning the txn.
        txn: &Option<&SanitizedTransaction>, 
        pubkey: &Pubkey,
        write_version: u64,
    ) {
        // Only notify if a Geyser plugin has been initialized and set up
        if let Some(accounts_update_notifier) = &self.accounts_update_notifier {
            accounts_update_notifier.notify_account_update(
                slot,
                account,
                txn,
                pubkey,
                write_version,
            );
        }
    }
}

// --- Test Module ---

#[cfg(test)]
pub mod tests {
    // Import all external dependencies required for testing.
    // Using explicit use statements for clarity in a test module.
    use {
        super::*,
        crate::{
            accounts_db::{AccountsDbConfig, MarkObsoleteAccounts, ACCOUNTS_DB_CONFIG_FOR_TESTING},
            accounts_update_notifier_interface::{
                AccountForGeyser, AccountsUpdateNotifier, AccountsUpdateNotifierInterface,
            },
            utils::create_account_shared_data,
        },
        dashmap::DashMap,
        solana_account::ReadableAccount as _,
        std::sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        test_case::test_case,
    };

    // Helper function for tests to set the geyser plugin
    impl AccountsDb {
        pub fn set_geyser_plugin_notifier(&mut self, notifier: Option<AccountsUpdateNotifier>) {
            self.accounts_update_notifier = notifier;
        }
    }

    /// A mock implementation of the AccountsUpdateNotifierInterface for testing purposes.
    /// Uses a DashMap to safely collect concurrent account notifications.
    #[derive(Debug, Default)]
    struct GeyserTestPlugin {
        // Map: Pubkey -> Vector of (Slot, WriteVersion, AccountData)
        pub accounts_notified: DashMap<Pubkey, Vec<(Slot, u64, AccountSharedData)>>,
        // Flag to track the end of snapshot restoration
        pub is_startup_done: AtomicBool,
    }

    impl AccountsUpdateNotifierInterface for GeyserTestPlugin {
        fn snapshot_notifications_enabled(&self) -> bool {
            true
        }

        /// Handles account updates due to runtime transaction execution.
        fn notify_account_update(
            &self,
            slot: Slot,
            account: &AccountSharedData,
            _txn: &Option<&SanitizedTransaction>,
            pubkey: &Pubkey,
            write_version: u64,
        ) {
            // Use entry().or_default() to safely insert/update the vector for the Pubkey
            self.accounts_notified.entry(*pubkey).or_default().push((
                slot,
                write_version,
                // Must clone the AccountSharedData as the reference is temporary
                account.clone(),
            ));
        }

        /// Handles account restores during startup from a snapshot.
        fn notify_account_restore_from_snapshot(
            &self,
            slot: Slot,
            write_version: u64,
            account: &AccountForGeyser<'_>,
        ) {
            self.accounts_notified
                .entry(*account.pubkey)
                .or_default()
                .push((slot, write_version, create_account_shared_data(account)));
        }

        /// Signals that the snapshot restoration process is complete.
        fn notify_end_of_restore_from_snapshot(&self) {
            self.is_startup_done.store(true, Ordering::Relaxed);
        }
    }

    /// Tests account notification during the initial database restoration phase.
    /// Covers both MarkObsoleteAccounts configurations.
    #[test_case(MarkObsoleteAccounts::Enabled)]
    #[test_case(MarkObsoleteAccounts::Disabled)]
    fn test_notify_account_restore_from_snapshot(mark_obsolete_accounts: MarkObsoleteAccounts) {
        let mut accounts_db = AccountsDb::new_with_config(
            Vec::new(),
            AccountsDbConfig {
                mark_obsolete_accounts,
                ..ACCOUNTS_DB_CONFIG_FOR_TESTING
            },
            None,
            Arc::default(),
        );
        let key1 = Pubkey::new_unique();
        let key2 = Pubkey::new_unique();
        let account = AccountSharedData::new(1, 0, &Pubkey::default());

        // --- Setup Data (Creating stores simulates writing accounts to disk) ---

        // key1 update 1 (slot 0)
        let slot0 = 0;
        let storage0 = accounts_db.create_and_insert_store(slot0, 4_096, "");
        storage0
            .accounts
            .write_accounts(&(slot0, [(&key1, &account)].as_slice()), 0);

        // key1 update 2 (slot 1)
        let slot1 = 1;
        let storage1 = accounts_db.create_and_insert_store(slot1, 4_096, "");
        storage1
            .accounts
            .write_accounts(&(slot1, [(&key1, &account)].as_slice()), 0);

        // key2 update 1 (slot 2)
        let slot2 = 2;
        let storage2 = accounts_db.create_and_insert_store(slot2, 4_096, "");
        storage2
            .accounts
            .write_accounts(&(slot2, [(&key2, &account)].as_slice()), 0);

        // --- Execution ---

        let notifier = Arc::new(GeyserTestPlugin::default());
        accounts_db.set_geyser_plugin_notifier(Some(notifier.clone()));
        
        // This triggers the iteration over all accounts for notification
        accounts_db.generate_index(None, false); 

        // --- Assertions ---

        // Assert key1 was notified twice (once per slot it was written to)
        {
            let notified_key1 = notifier.accounts_notified.get(&key1).expect("key1 should be notified");
            assert_eq!(notified_key1.len(), 2, "key1 should have 2 notifications");

            // Sort by slot due to non-deterministic parallel processing order
            let mut notified_key1_values = notified_key1.value().clone();
            notified_key1_values.sort_unstable_by_key(|k| k.0); // k.0 is the slot

            // Check slot 0 notification
            let (slot, write_version, _account) = &notified_key1_values[0];
            assert_eq!(*slot, slot0, "First notification slot should be 0");
            assert_eq!(*write_version, 0); 
            
            // Check slot 1 notification
            let (slot, write_version, _account) = &notified_key1_values[1];
            assert_eq!(*slot, slot1, "Second notification slot should be 1");
            assert_eq!(*write_version, 0);
        }

        // Assert key2 was notified once
        {
            let notified_key2 = notifier.accounts_notified.get(&key2).expect("key2 should be notified");
            assert_eq!(notified_key2.len(), 1, "key2 should have 1 notification");
            
            let (slot, write_version, _account) = &notified_key2[0];
            assert_eq!(*slot, slot2, "key2 notification slot should be 2");
            assert_eq!(*write_version, 0);
        }

        // Assert startup done flag was set
        assert!(notifier.is_startup_done.load(Ordering::Relaxed), "Startup done flag should be true");
    }

    /// Tests notification during runtime account updates via store_for_tests.
    #[test]
    fn test_notify_account_at_accounts_update() {
        let mut accounts = AccountsDb::new_single_for_tests();

        let notifier = Arc::new(GeyserTestPlugin::default());
        accounts.set_geyser_plugin_notifier(Some(notifier.clone()));

        // Create keys and initial accounts
        let key1 = solana_pubkey::new_rand();
        let account1_lamports1: u64 = 1;
        let account1 = AccountSharedData::new(account1_lamports1, 1, AccountSharedData::default().owner());
        
        let key2 = solana_pubkey::new_rand();
        let account2_lamports: u64 = 200;
        let account2 = AccountSharedData::new(account2_lamports, 1, AccountSharedData::default().owner());
        
        let key3 = solana_pubkey::new_rand();
        let account3_lamports: u64 = 300;
        let account3 = AccountSharedData::new(account3_lamports, 1, AccountSharedData::default().owner());
        
        // --- Store Updates ---

        let slot0 = 0;
        // key1 first update
        accounts.store_for_tests((slot0, &[(&key1, &account1)][..]));
        // key2 update
        accounts.store_for_tests((slot0, &[(&key2, &account2)][..]));

        // key1 second update (new lamports)
        let account1_lamports2 = 2;
        let slot1 = 1;
        let account1_v2 = AccountSharedData::new(account1_lamports2, 1, account1.owner());
        accounts.store_for_tests((slot1, &[(&key1, &account1_v2)][..]));

        // key3 update
        accounts.store_for_tests((slot1, &[(&key3, &account3)][..]));

        // --- Assertions ---

        // key1 updated twice (slot 0 and slot 1)
        let notified_key1 = notifier.accounts_notified.get(&key1).expect("key1 notified");
        assert_eq!(notified_key1.len(), 2);
        assert_eq!(notified_key1[0].2.lamports(), account1_lamports1);
        assert_eq!(notified_key1[0].0, slot0);
        assert_eq!(notified_key1[1].2.lamports(), account1_lamports2);
        assert_eq!(notified_key1[1].0, slot1);

        // key2 updated once (slot 0)
        let notified_key2 = notifier.accounts_notified.get(&key2).expect("key2 notified");
        assert_eq!(notified_key2.len(), 1);
        assert_eq!(notified_key2[0].2.lamports(), account2_lamports);
        assert_eq!(notified_key2[0].0, slot0);
        
        // key3 updated once (slot 1)
        let notified_key3 = notifier.accounts_notified.get(&key3).expect("key3 notified");
        assert_eq!(notified_key3.len(), 1);
        assert_eq!(notified_key3[0].2.lamports(), account3_lamports);
        assert_eq!(notified_key3[0].0, slot1);
    }
}
