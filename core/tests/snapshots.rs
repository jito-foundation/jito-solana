// Long-running bank_forks tests
#![allow(clippy::integer_arithmetic)]

macro_rules! DEFINE_SNAPSHOT_VERSION_PARAMETERIZED_TEST_FUNCTIONS {
    ($x:ident, $y:ident, $z:ident) => {
        #[allow(non_snake_case)]
        mod $z {
            use super::*;

            const SNAPSHOT_VERSION: SnapshotVersion = SnapshotVersion::$x;
            const CLUSTER_TYPE: ClusterType = ClusterType::$y;

            #[test]
            fn test_bank_forks_status_cache_snapshot_n() {
                run_test_bank_forks_status_cache_snapshot_n(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }

            #[test]
            fn test_bank_forks_snapshot_n() {
                run_test_bank_forks_snapshot_n(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }

            #[test]
            fn test_concurrent_snapshot_packaging() {
                run_test_concurrent_snapshot_packaging(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }

            #[test]
            fn test_slots_to_snapshot() {
                run_test_slots_to_snapshot(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }

            #[test]
            fn test_bank_forks_incremental_snapshot_n() {
                run_test_bank_forks_incremental_snapshot_n(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }

            #[test]
            fn test_snapshots_with_background_services() {
                run_test_snapshots_with_background_services(SNAPSHOT_VERSION, CLUSTER_TYPE)
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use {
        bincode::serialize_into,
        crossbeam_channel::unbounded,
        fs_extra::dir::CopyOptions,
        itertools::Itertools,
        log::{info, trace},
        solana_core::{
            accounts_hash_verifier::AccountsHashVerifier,
            snapshot_packager_service::SnapshotPackagerService,
        },
        solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
        solana_runtime::{
            accounts_background_service::{
                AbsRequestHandler, AbsRequestSender, AccountsBackgroundService,
                SnapshotRequestHandler,
            },
            accounts_db::{self, ACCOUNTS_DB_CONFIG_FOR_TESTING},
            accounts_index::AccountSecondaryIndexes,
            bank::{Bank, BankSlotDelta},
            bank_forks::BankForks,
            genesis_utils::{create_genesis_config_with_leader, GenesisConfigInfo},
            snapshot_archive_info::FullSnapshotArchiveInfo,
            snapshot_config::SnapshotConfig,
            snapshot_package::{
                AccountsPackage, PendingAccountsPackage, PendingSnapshotPackage, SnapshotPackage,
                SnapshotType,
            },
            snapshot_utils::{self, ArchiveFormat, SnapshotVersion},
            status_cache::MAX_CACHE_ENTRIES,
        },
        solana_sdk::{
            clock::Slot,
            genesis_config::{ClusterType, GenesisConfig},
            hash::{hashv, Hash},
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction,
            timing::timestamp,
        },
        solana_streamer::socket::SocketAddrSpace,
        std::{
            collections::HashSet,
            fs,
            io::{Error, ErrorKind},
            path::PathBuf,
            sync::{
                atomic::{AtomicBool, Ordering},
                Arc, RwLock,
            },
            time::Duration,
        },
        tempfile::TempDir,
    };

    DEFINE_SNAPSHOT_VERSION_PARAMETERIZED_TEST_FUNCTIONS!(V1_2_0, Development, V1_2_0_Development);
    DEFINE_SNAPSHOT_VERSION_PARAMETERIZED_TEST_FUNCTIONS!(V1_2_0, Devnet, V1_2_0_Devnet);
    DEFINE_SNAPSHOT_VERSION_PARAMETERIZED_TEST_FUNCTIONS!(V1_2_0, Testnet, V1_2_0_Testnet);
    DEFINE_SNAPSHOT_VERSION_PARAMETERIZED_TEST_FUNCTIONS!(V1_2_0, MainnetBeta, V1_2_0_MainnetBeta);

    struct SnapshotTestConfig {
        accounts_dir: TempDir,
        bank_snapshots_dir: TempDir,
        snapshot_archives_dir: TempDir,
        snapshot_config: SnapshotConfig,
        bank_forks: BankForks,
        genesis_config_info: GenesisConfigInfo,
    }

    impl SnapshotTestConfig {
        fn new(
            snapshot_version: SnapshotVersion,
            cluster_type: ClusterType,
            accounts_hash_interval_slots: Slot,
            full_snapshot_archive_interval_slots: Slot,
            incremental_snapshot_archive_interval_slots: Slot,
        ) -> SnapshotTestConfig {
            let accounts_dir = TempDir::new().unwrap();
            let bank_snapshots_dir = TempDir::new().unwrap();
            let snapshot_archives_dir = TempDir::new().unwrap();
            // validator_stake_lamports should be non-zero otherwise stake
            // account will not be stored in accounts-db but still cached in
            // bank stakes which results in mismatch when banks are loaded from
            // snapshots.
            let mut genesis_config_info = create_genesis_config_with_leader(
                10_000,                          // mint_lamports
                &solana_sdk::pubkey::new_rand(), // validator_pubkey
                1,                               // validator_stake_lamports
            );
            genesis_config_info.genesis_config.cluster_type = cluster_type;
            let bank0 = Bank::new_with_paths_for_tests(
                &genesis_config_info.genesis_config,
                vec![accounts_dir.path().to_path_buf()],
                None,
                None,
                AccountSecondaryIndexes::default(),
                false,
                accounts_db::AccountShrinkThreshold::default(),
                false,
            );
            bank0.freeze();
            let mut bank_forks = BankForks::new(bank0);
            bank_forks.accounts_hash_interval_slots = accounts_hash_interval_slots;

            let snapshot_config = SnapshotConfig {
                full_snapshot_archive_interval_slots,
                incremental_snapshot_archive_interval_slots,
                snapshot_archives_dir: snapshot_archives_dir.path().to_path_buf(),
                bank_snapshots_dir: bank_snapshots_dir.path().to_path_buf(),
                snapshot_version,
                ..SnapshotConfig::default()
            };
            bank_forks.set_snapshot_config(Some(snapshot_config.clone()));
            SnapshotTestConfig {
                accounts_dir,
                bank_snapshots_dir,
                snapshot_archives_dir,
                snapshot_config,
                bank_forks,
                genesis_config_info,
            }
        }
    }

    fn restore_from_snapshot(
        old_bank_forks: &BankForks,
        old_last_slot: Slot,
        old_genesis_config: &GenesisConfig,
        account_paths: &[PathBuf],
    ) {
        let snapshot_archives_dir = old_bank_forks
            .snapshot_config
            .as_ref()
            .map(|c| &c.snapshot_archives_dir)
            .unwrap();

        let old_last_bank = old_bank_forks.get(old_last_slot).unwrap();

        let check_hash_calculation = false;
        let full_snapshot_archive_path = snapshot_utils::build_full_snapshot_archive_path(
            snapshot_archives_dir,
            old_last_bank.slot(),
            &old_last_bank.get_accounts_hash(),
            ArchiveFormat::TarBzip2,
        );
        let full_snapshot_archive_info =
            FullSnapshotArchiveInfo::new_from_path(full_snapshot_archive_path).unwrap();

        let (deserialized_bank, _timing) = snapshot_utils::bank_from_snapshot_archives(
            account_paths,
            &old_bank_forks
                .snapshot_config
                .as_ref()
                .unwrap()
                .bank_snapshots_dir,
            &full_snapshot_archive_info,
            None,
            old_genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            accounts_db::AccountShrinkThreshold::default(),
            check_hash_calculation,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        let bank = old_bank_forks.get(deserialized_bank.slot()).unwrap();
        assert_eq!(bank.as_ref(), &deserialized_bank);
    }

    // creates banks up to "last_slot" and runs the input function `f` on each bank created
    // also marks each bank as root and generates snapshots
    // finally tries to restore from the last bank's snapshot and compares the restored bank to the
    // `last_slot` bank
    fn run_bank_forks_snapshot_n<F>(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
        last_slot: Slot,
        f: F,
        set_root_interval: u64,
    ) where
        F: Fn(&mut Bank, &Keypair),
    {
        solana_logger::setup();
        // Set up snapshotting config
        let mut snapshot_test_config = SnapshotTestConfig::new(
            snapshot_version,
            cluster_type,
            set_root_interval,
            set_root_interval,
            Slot::MAX,
        );

        let bank_forks = &mut snapshot_test_config.bank_forks;
        let mint_keypair = &snapshot_test_config.genesis_config_info.mint_keypair;

        let (snapshot_request_sender, snapshot_request_receiver) = unbounded();
        let request_sender = AbsRequestSender::new(snapshot_request_sender);
        let snapshot_request_handler = SnapshotRequestHandler {
            snapshot_config: snapshot_test_config.snapshot_config.clone(),
            snapshot_request_receiver,
            pending_accounts_package: PendingAccountsPackage::default(),
        };
        for slot in 1..=last_slot {
            let mut bank = Bank::new_from_parent(&bank_forks[slot - 1], &Pubkey::default(), slot);
            f(&mut bank, mint_keypair);
            let bank = bank_forks.insert(bank);
            // Set root to make sure we don't end up with too many account storage entries
            // and to allow snapshotting of bank and the purging logic on status_cache to
            // kick in
            if slot % set_root_interval == 0 || slot == last_slot {
                // set_root should send a snapshot request
                bank_forks.set_root(bank.slot(), &request_sender, None);
                bank.update_accounts_hash();
                snapshot_request_handler.handle_snapshot_requests(false, false, 0, &mut None);
            }
        }

        // Generate a snapshot package for last bank
        let last_bank = bank_forks.get(last_slot).unwrap();
        let snapshot_config = &snapshot_test_config.snapshot_config;
        let bank_snapshots_dir = &snapshot_config.bank_snapshots_dir;
        let last_bank_snapshot_info =
            snapshot_utils::get_highest_bank_snapshot_pre(bank_snapshots_dir)
                .expect("no bank snapshots found in path");
        let accounts_package = AccountsPackage::new(
            &last_bank,
            &last_bank_snapshot_info,
            bank_snapshots_dir,
            last_bank.src.slot_deltas(&last_bank.src.roots()),
            &snapshot_config.snapshot_archives_dir,
            last_bank.get_snapshot_storages(None),
            ArchiveFormat::TarBzip2,
            snapshot_version,
            None,
            Some(SnapshotType::FullSnapshot),
        )
        .unwrap();
        solana_runtime::serde_snapshot::reserialize_bank_with_new_accounts_hash(
            accounts_package.snapshot_links.path(),
            accounts_package.slot,
            &last_bank.get_accounts_hash(),
        );
        let snapshot_package =
            SnapshotPackage::new(accounts_package, last_bank.get_accounts_hash());
        snapshot_utils::archive_snapshot_package(
            &snapshot_package,
            snapshot_config.maximum_full_snapshot_archives_to_retain,
            snapshot_config.maximum_incremental_snapshot_archives_to_retain,
        )
        .unwrap();

        // Restore bank from snapshot
        let account_paths = &[snapshot_test_config.accounts_dir.path().to_path_buf()];
        let genesis_config = &snapshot_test_config.genesis_config_info.genesis_config;
        restore_from_snapshot(bank_forks, last_slot, genesis_config, account_paths);
    }

    fn run_test_bank_forks_snapshot_n(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
    ) {
        // create banks up to slot 4 and create 1 new account in each bank. test that bank 4 snapshots
        // and restores correctly
        run_bank_forks_snapshot_n(
            snapshot_version,
            cluster_type,
            4,
            |bank, mint_keypair| {
                let key1 = Keypair::new().pubkey();
                let tx =
                    system_transaction::transfer(mint_keypair, &key1, 1, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                let key2 = Keypair::new().pubkey();
                let tx =
                    system_transaction::transfer(mint_keypair, &key2, 0, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                bank.freeze();
            },
            1,
        );
    }

    fn goto_end_of_slot(bank: &mut Bank) {
        let mut tick_hash = bank.last_blockhash();
        loop {
            tick_hash = hashv(&[tick_hash.as_ref(), &[42]]);
            bank.register_tick(&tick_hash);
            if tick_hash == bank.last_blockhash() {
                bank.freeze();
                return;
            }
        }
    }

    fn run_test_concurrent_snapshot_packaging(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
    ) {
        solana_logger::setup();

        // Set up snapshotting config
        let mut snapshot_test_config =
            SnapshotTestConfig::new(snapshot_version, cluster_type, 1, 1, Slot::MAX);

        let bank_forks = &mut snapshot_test_config.bank_forks;
        let snapshot_config = &snapshot_test_config.snapshot_config;
        let bank_snapshots_dir = &snapshot_config.bank_snapshots_dir;
        let snapshot_archives_dir = &snapshot_config.snapshot_archives_dir;
        let mint_keypair = &snapshot_test_config.genesis_config_info.mint_keypair;
        let genesis_config = &snapshot_test_config.genesis_config_info.genesis_config;

        // Take snapshot of zeroth bank
        let bank0 = bank_forks.get(0).unwrap();
        let storages = bank0.get_snapshot_storages(None);
        snapshot_utils::add_bank_snapshot(bank_snapshots_dir, &bank0, &storages, snapshot_version)
            .unwrap();

        // Set up snapshotting channels
        let real_pending_accounts_package = PendingAccountsPackage::default();
        let fake_pending_accounts_package = PendingAccountsPackage::default();

        // Create next MAX_CACHE_ENTRIES + 2 banks and snapshots. Every bank will get snapshotted
        // and the snapshot purging logic will run on every snapshot taken. This means the three
        // (including snapshot for bank0 created above) earliest snapshots will get purged by the
        // time this loop is done.

        // Also, make a saved copy of the state of the snapshot for a bank with
        // bank.slot == saved_slot, so we can use it for a correctness check later.
        let saved_snapshots_dir = TempDir::new().unwrap();
        let saved_accounts_dir = TempDir::new().unwrap();
        let saved_slot = 4;
        let mut saved_archive_path = None;

        for forks in 0..snapshot_utils::MAX_BANK_SNAPSHOTS_TO_RETAIN + 2 {
            let bank = Bank::new_from_parent(
                &bank_forks[forks as u64],
                &Pubkey::default(),
                (forks + 1) as u64,
            );
            let slot = bank.slot();
            let key1 = Keypair::new().pubkey();
            let tx = system_transaction::transfer(mint_keypair, &key1, 1, genesis_config.hash());
            assert_eq!(bank.process_transaction(&tx), Ok(()));
            bank.squash();

            let pending_accounts_package = {
                if slot == saved_slot as u64 {
                    // Only send one package on the real pending_accounts_package so that the
                    // packaging service doesn't take forever to run the packaging logic on all
                    // MAX_CACHE_ENTRIES later
                    &real_pending_accounts_package
                } else {
                    &fake_pending_accounts_package
                }
            };

            snapshot_utils::snapshot_bank(
                &bank,
                vec![],
                pending_accounts_package,
                bank_snapshots_dir,
                snapshot_archives_dir,
                snapshot_config.snapshot_version,
                snapshot_config.archive_format,
                None,
                Some(SnapshotType::FullSnapshot),
            )
            .unwrap();

            bank_forks.insert(bank);
            if slot == saved_slot as u64 {
                // Find the relevant snapshot storages
                let snapshot_storage_files: HashSet<_> = bank_forks[slot]
                    .get_snapshot_storages(None)
                    .into_iter()
                    .flatten()
                    .map(|s| s.get_path())
                    .collect();

                // Only save off the files returned by `get_snapshot_storages`. This is because
                // some of the storage entries in the accounts directory may be filtered out by
                // `get_snapshot_storages()` and will not be included in the snapshot. Ultimately,
                // this means copying natively everything in `accounts_dir` to the `saved_accounts_dir`
                // will lead to test failure by mismatch when `saved_accounts_dir` is compared to
                // the unpacked snapshot later in this test's call to `verify_snapshot_archive()`.
                for file in snapshot_storage_files {
                    fs::copy(
                        &file,
                        &saved_accounts_dir.path().join(file.file_name().unwrap()),
                    )
                    .unwrap();
                }
                let last_snapshot_path = fs::read_dir(bank_snapshots_dir)
                    .unwrap()
                    .filter_map(|entry| {
                        let e = entry.unwrap();
                        let file_path = e.path();
                        let file_name = file_path.file_name().unwrap();
                        file_name
                            .to_str()
                            .map(|s| s.parse::<u64>().ok().map(|_| file_path.clone()))
                            .unwrap_or(None)
                    })
                    .sorted()
                    .last()
                    .unwrap();
                // only save off the snapshot of this slot, we don't need the others.
                let options = CopyOptions::new();
                fs_extra::dir::copy(&last_snapshot_path, &saved_snapshots_dir, &options).unwrap();

                saved_archive_path = Some(snapshot_utils::build_full_snapshot_archive_path(
                    snapshot_archives_dir,
                    slot,
                    // this needs to match the hash value that we reserialize with later. It is complicated, so just use default.
                    // This hash value is just used to build the file name. Since this is mocked up test code, it is sufficient to pass default here.
                    &Hash::default(),
                    ArchiveFormat::TarBzip2,
                ));
            }
        }

        // Purge all the outdated snapshots, including the ones needed to generate the package
        // currently sitting in the channel
        snapshot_utils::purge_old_bank_snapshots(bank_snapshots_dir);

        let mut bank_snapshots = snapshot_utils::get_bank_snapshots_pre(&bank_snapshots_dir);
        bank_snapshots.sort_unstable();
        assert!(bank_snapshots
            .into_iter()
            .map(|path| path.slot)
            .eq(3..=snapshot_utils::MAX_BANK_SNAPSHOTS_TO_RETAIN as u64 + 2));

        // Create a SnapshotPackagerService to create tarballs from all the pending
        // SnapshotPackage's on the channel. By the time this service starts, we have already
        // purged the first two snapshots, which are needed by every snapshot other than
        // the last two snapshots. However, the packaging service should still be able to
        // correctly construct the earlier snapshots because the SnapshotPackage's on the
        // channel hold hard links to these deleted snapshots. We verify this is the case below.
        let exit = Arc::new(AtomicBool::new(false));

        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::default(),
            Arc::new(Keypair::new()),
            SocketAddrSpace::Unspecified,
        ));

        let pending_snapshot_package = PendingSnapshotPackage::default();
        let snapshot_packager_service = SnapshotPackagerService::new(
            pending_snapshot_package.clone(),
            None,
            &exit,
            &cluster_info,
            snapshot_config.clone(),
            true,
        );

        let _package_receiver = std::thread::Builder::new()
            .name("package-receiver".to_string())
            .spawn(move || {
                let accounts_package = real_pending_accounts_package
                    .lock()
                    .unwrap()
                    .take()
                    .unwrap();
                solana_runtime::serde_snapshot::reserialize_bank_with_new_accounts_hash(
                    accounts_package.snapshot_links.path(),
                    accounts_package.slot,
                    &Hash::default(),
                );
                let snapshot_package = SnapshotPackage::new(accounts_package, Hash::default());
                pending_snapshot_package
                    .lock()
                    .unwrap()
                    .replace(snapshot_package);

                // Wait until the package is consumed by SnapshotPackagerService
                while pending_snapshot_package.lock().unwrap().is_some() {
                    std::thread::sleep(Duration::from_millis(100));
                }

                // Shutdown SnapshotPackagerService
                exit.store(true, Ordering::Relaxed);
            })
            .unwrap();

        // Wait for service to finish
        snapshot_packager_service
            .join()
            .expect("SnapshotPackagerService exited with error");

        // Check the archive we cached the state for earlier was generated correctly

        // before we compare, stick an empty status_cache in this dir so that the package comparison works
        // This is needed since the status_cache is added by the packager and is not collected from
        // the source dir for snapshots
        snapshot_utils::serialize_snapshot_data_file(
            &saved_snapshots_dir
                .path()
                .join(snapshot_utils::SNAPSHOT_STATUS_CACHE_FILENAME),
            |stream| {
                serialize_into(stream, &[] as &[BankSlotDelta])?;
                Ok(())
            },
        )
        .unwrap();

        // files were saved off before we reserialized the bank in the hacked up accounts_hash_verifier stand-in.
        solana_runtime::serde_snapshot::reserialize_bank_with_new_accounts_hash(
            saved_snapshots_dir.path(),
            saved_slot,
            &Hash::default(),
        );

        snapshot_utils::verify_snapshot_archive(
            saved_archive_path.unwrap(),
            saved_snapshots_dir.path(),
            saved_accounts_dir.path(),
            ArchiveFormat::TarBzip2,
            snapshot_utils::VerifyBank::NonDeterministic(saved_slot),
        );
    }

    fn run_test_slots_to_snapshot(snapshot_version: SnapshotVersion, cluster_type: ClusterType) {
        solana_logger::setup();
        let num_set_roots = MAX_CACHE_ENTRIES * 2;

        for add_root_interval in &[1, 3, 9] {
            let (snapshot_sender, _snapshot_receiver) = unbounded();
            // Make sure this test never clears bank.slots_since_snapshot
            let mut snapshot_test_config = SnapshotTestConfig::new(
                snapshot_version,
                cluster_type,
                (*add_root_interval * num_set_roots * 2) as Slot,
                (*add_root_interval * num_set_roots * 2) as Slot,
                Slot::MAX,
            );
            let mut current_bank = snapshot_test_config.bank_forks[0].clone();
            let request_sender = AbsRequestSender::new(snapshot_sender);
            for _ in 0..num_set_roots {
                for _ in 0..*add_root_interval {
                    let new_slot = current_bank.slot() + 1;
                    let new_bank =
                        Bank::new_from_parent(&current_bank, &Pubkey::default(), new_slot);
                    snapshot_test_config.bank_forks.insert(new_bank);
                    current_bank = snapshot_test_config.bank_forks[new_slot].clone();
                }
                snapshot_test_config.bank_forks.set_root(
                    current_bank.slot(),
                    &request_sender,
                    None,
                );
            }

            let num_old_slots = num_set_roots * *add_root_interval - MAX_CACHE_ENTRIES + 1;
            let expected_slots_to_snapshot =
                num_old_slots as u64..=num_set_roots as u64 * *add_root_interval as u64;

            let slots_to_snapshot = snapshot_test_config
                .bank_forks
                .get(snapshot_test_config.bank_forks.root())
                .unwrap()
                .src
                .roots();
            assert!(slots_to_snapshot.into_iter().eq(expected_slots_to_snapshot));
        }
    }

    fn run_test_bank_forks_status_cache_snapshot_n(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
    ) {
        // create banks up to slot (MAX_CACHE_ENTRIES * 2) + 1 while transferring 1 lamport into 2 different accounts each time
        // this is done to ensure the AccountStorageEntries keep getting cleaned up as the root moves
        // ahead. Also tests the status_cache purge and status cache snapshotting.
        // Makes sure that the last bank is restored correctly
        let key1 = Keypair::new().pubkey();
        let key2 = Keypair::new().pubkey();
        for set_root_interval in &[1, 4] {
            run_bank_forks_snapshot_n(
                snapshot_version,
                cluster_type,
                (MAX_CACHE_ENTRIES * 2) as u64,
                |bank, mint_keypair| {
                    let tx = system_transaction::transfer(
                        mint_keypair,
                        &key1,
                        1,
                        bank.parent().unwrap().last_blockhash(),
                    );
                    assert_eq!(bank.process_transaction(&tx), Ok(()));
                    let tx = system_transaction::transfer(
                        mint_keypair,
                        &key2,
                        1,
                        bank.parent().unwrap().last_blockhash(),
                    );
                    assert_eq!(bank.process_transaction(&tx), Ok(()));
                    goto_end_of_slot(bank);
                },
                *set_root_interval,
            );
        }
    }

    fn run_test_bank_forks_incremental_snapshot_n(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
    ) {
        solana_logger::setup();

        const SET_ROOT_INTERVAL: Slot = 2;
        const INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot = SET_ROOT_INTERVAL * 2;
        const FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot =
            INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS * 5;
        const LAST_SLOT: Slot = FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS * 2 - 1;

        info!("Running bank forks incremental snapshot test, full snapshot interval: {} slots, incremental snapshot interval: {} slots, last slot: {}, set root interval: {} slots",
              FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS, INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS, LAST_SLOT, SET_ROOT_INTERVAL);

        let mut snapshot_test_config = SnapshotTestConfig::new(
            snapshot_version,
            cluster_type,
            SET_ROOT_INTERVAL,
            FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
        );
        trace!("SnapshotTestConfig:\naccounts_dir: {}\nbank_snapshots_dir: {}\nsnapshot_archives_dir: {}", snapshot_test_config.accounts_dir.path().display(), snapshot_test_config.bank_snapshots_dir.path().display(), snapshot_test_config.snapshot_archives_dir.path().display());

        let bank_forks = &mut snapshot_test_config.bank_forks;
        let mint_keypair = &snapshot_test_config.genesis_config_info.mint_keypair;

        let (snapshot_request_sender, snapshot_request_receiver) = unbounded();
        let request_sender = AbsRequestSender::new(snapshot_request_sender);
        let snapshot_request_handler = SnapshotRequestHandler {
            snapshot_config: snapshot_test_config.snapshot_config.clone(),
            snapshot_request_receiver,
            pending_accounts_package: PendingAccountsPackage::default(),
        };

        let mut last_full_snapshot_slot = None;
        for slot in 1..=LAST_SLOT {
            // Make a new bank and perform some transactions
            let bank = {
                let bank = Bank::new_from_parent(&bank_forks[slot - 1], &Pubkey::default(), slot);

                let key = Keypair::new().pubkey();
                let tx = system_transaction::transfer(mint_keypair, &key, 1, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                let key = Keypair::new().pubkey();
                let tx = system_transaction::transfer(mint_keypair, &key, 0, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                while !bank.is_complete() {
                    bank.register_tick(&Hash::new_unique());
                }

                bank_forks.insert(bank)
            };

            // Set root to make sure we don't end up with too many account storage entries
            // and to allow snapshotting of bank and the purging logic on status_cache to
            // kick in
            if slot % SET_ROOT_INTERVAL == 0 {
                // set_root sends a snapshot request
                bank_forks.set_root(bank.slot(), &request_sender, None);
                bank.update_accounts_hash();
                snapshot_request_handler.handle_snapshot_requests(
                    false,
                    false,
                    0,
                    &mut last_full_snapshot_slot,
                );
            }

            // Since AccountsBackgroundService isn't running, manually make a full snapshot archive
            // at the right interval
            if snapshot_utils::should_take_full_snapshot(slot, FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS)
            {
                make_full_snapshot_archive(&bank, &snapshot_test_config.snapshot_config).unwrap();
            }
            // Similarly, make an incremental snapshot archive at the right interval, but only if
            // there's been at least one full snapshot first, and a full snapshot wasn't already
            // taken at this slot.
            //
            // Then, after making an incremental snapshot, restore the bank and verify it is correct
            else if snapshot_utils::should_take_incremental_snapshot(
                slot,
                INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
                last_full_snapshot_slot,
            ) && slot != last_full_snapshot_slot.unwrap()
            {
                make_incremental_snapshot_archive(
                    &bank,
                    last_full_snapshot_slot.unwrap(),
                    &snapshot_test_config.snapshot_config,
                )
                .unwrap();

                restore_from_snapshots_and_check_banks_are_equal(
                    &bank,
                    &snapshot_test_config.snapshot_config,
                    snapshot_test_config.accounts_dir.path().to_path_buf(),
                    &snapshot_test_config.genesis_config_info.genesis_config,
                )
                .unwrap();
            }
        }
    }

    fn make_full_snapshot_archive(
        bank: &Bank,
        snapshot_config: &SnapshotConfig,
    ) -> snapshot_utils::Result<()> {
        let slot = bank.slot();
        info!("Making full snapshot archive from bank at slot: {}", slot);
        let bank_snapshot_info =
            snapshot_utils::get_bank_snapshots_pre(&snapshot_config.bank_snapshots_dir)
                .into_iter()
                .find(|elem| elem.slot == slot)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Other,
                        "did not find bank snapshot with this path",
                    )
                })?;
        snapshot_utils::package_and_archive_full_snapshot(
            bank,
            &bank_snapshot_info,
            &snapshot_config.bank_snapshots_dir,
            &snapshot_config.snapshot_archives_dir,
            bank.get_snapshot_storages(None),
            snapshot_config.archive_format,
            snapshot_config.snapshot_version,
            snapshot_config.maximum_full_snapshot_archives_to_retain,
            snapshot_config.maximum_incremental_snapshot_archives_to_retain,
        )?;

        Ok(())
    }

    fn make_incremental_snapshot_archive(
        bank: &Bank,
        incremental_snapshot_base_slot: Slot,
        snapshot_config: &SnapshotConfig,
    ) -> snapshot_utils::Result<()> {
        let slot = bank.slot();
        info!(
            "Making incremental snapshot archive from bank at slot: {}, and base slot: {}",
            slot, incremental_snapshot_base_slot,
        );
        let bank_snapshot_info =
            snapshot_utils::get_bank_snapshots_pre(&snapshot_config.bank_snapshots_dir)
                .into_iter()
                .find(|elem| elem.slot == slot)
                .ok_or_else(|| {
                    Error::new(
                        ErrorKind::Other,
                        "did not find bank snapshot with this path",
                    )
                })?;
        let storages = bank.get_snapshot_storages(Some(incremental_snapshot_base_slot));
        snapshot_utils::package_and_archive_incremental_snapshot(
            bank,
            incremental_snapshot_base_slot,
            &bank_snapshot_info,
            &snapshot_config.bank_snapshots_dir,
            &snapshot_config.snapshot_archives_dir,
            storages,
            snapshot_config.archive_format,
            snapshot_config.snapshot_version,
            snapshot_config.maximum_full_snapshot_archives_to_retain,
            snapshot_config.maximum_incremental_snapshot_archives_to_retain,
        )?;

        Ok(())
    }

    fn restore_from_snapshots_and_check_banks_are_equal(
        bank: &Bank,
        snapshot_config: &SnapshotConfig,
        accounts_dir: PathBuf,
        genesis_config: &GenesisConfig,
    ) -> snapshot_utils::Result<()> {
        let (deserialized_bank, ..) = snapshot_utils::bank_from_latest_snapshot_archives(
            &snapshot_config.bank_snapshots_dir,
            &snapshot_config.snapshot_archives_dir,
            &[accounts_dir],
            genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            accounts_db::AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )?;

        assert_eq!(bank, &deserialized_bank);

        Ok(())
    }

    /// Spin up the background services fully and test taking snapshots
    fn run_test_snapshots_with_background_services(
        snapshot_version: SnapshotVersion,
        cluster_type: ClusterType,
    ) {
        solana_logger::setup();

        const SET_ROOT_INTERVAL_SLOTS: Slot = 2;
        const BANK_SNAPSHOT_INTERVAL_SLOTS: Slot = SET_ROOT_INTERVAL_SLOTS * 2;
        const INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot = BANK_SNAPSHOT_INTERVAL_SLOTS * 3;
        const FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS: Slot =
            INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS * 5;
        const LAST_SLOT: Slot = FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS * 3
            + INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS * 2;

        info!("Running snapshots with background services test...");
        trace!(
            "Test configuration parameters:\
        \n\tfull snapshot archive interval: {} slots\
        \n\tincremental snapshot archive interval: {} slots\
        \n\tbank snapshot interval: {} slots\
        \n\tset root interval: {} slots\
        \n\tlast slot: {}",
            FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            BANK_SNAPSHOT_INTERVAL_SLOTS,
            SET_ROOT_INTERVAL_SLOTS,
            LAST_SLOT
        );

        let snapshot_test_config = SnapshotTestConfig::new(
            snapshot_version,
            cluster_type,
            BANK_SNAPSHOT_INTERVAL_SLOTS,
            FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
            INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS,
        );

        let node_keypair = Arc::new(Keypair::new());
        let cluster_info = Arc::new(ClusterInfo::new(
            ContactInfo::new_localhost(&node_keypair.pubkey(), timestamp()),
            node_keypair,
            SocketAddrSpace::Unspecified,
        ));

        let (pruned_banks_sender, pruned_banks_receiver) = unbounded();
        let (snapshot_request_sender, snapshot_request_receiver) = unbounded();
        let pending_accounts_package = PendingAccountsPackage::default();
        let pending_snapshot_package = PendingSnapshotPackage::default();

        let bank_forks = Arc::new(RwLock::new(snapshot_test_config.bank_forks));
        let callback = bank_forks
            .read()
            .unwrap()
            .root_bank()
            .rc
            .accounts
            .accounts_db
            .create_drop_bank_callback(pruned_banks_sender);
        for bank in bank_forks.read().unwrap().banks().values() {
            bank.set_callback(Some(Box::new(callback.clone())));
        }

        let abs_request_sender = AbsRequestSender::new(snapshot_request_sender);
        let snapshot_request_handler = Some(SnapshotRequestHandler {
            snapshot_config: snapshot_test_config.snapshot_config.clone(),
            snapshot_request_receiver,
            pending_accounts_package: Arc::clone(&pending_accounts_package),
        });
        let abs_request_handler = AbsRequestHandler {
            snapshot_request_handler,
            pruned_banks_receiver,
        };

        let exit = Arc::new(AtomicBool::new(false));
        let snapshot_packager_service = SnapshotPackagerService::new(
            pending_snapshot_package.clone(),
            None,
            &exit,
            &cluster_info,
            snapshot_test_config.snapshot_config.clone(),
            true,
        );

        let accounts_hash_verifier = AccountsHashVerifier::new(
            pending_accounts_package,
            Some(pending_snapshot_package),
            &exit,
            &cluster_info,
            None,
            false,
            0,
            Some(snapshot_test_config.snapshot_config.clone()),
        );

        let accounts_background_service = AccountsBackgroundService::new(
            bank_forks.clone(),
            &exit,
            abs_request_handler,
            false,
            true,
            None,
        );

        let mint_keypair = &snapshot_test_config.genesis_config_info.mint_keypair;
        for slot in 1..=LAST_SLOT {
            // Make a new bank and perform some transactions
            let bank = {
                let bank = Bank::new_from_parent(
                    &bank_forks.read().unwrap().get(slot - 1).unwrap(),
                    &Pubkey::default(),
                    slot,
                );

                let key = Keypair::new().pubkey();
                let tx = system_transaction::transfer(mint_keypair, &key, 1, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                let key = Keypair::new().pubkey();
                let tx = system_transaction::transfer(mint_keypair, &key, 0, bank.last_blockhash());
                assert_eq!(bank.process_transaction(&tx), Ok(()));

                while !bank.is_complete() {
                    bank.register_tick(&Hash::new_unique());
                }

                bank_forks.write().unwrap().insert(bank)
            };

            // Call `BankForks::set_root()` to cause bank snapshots to be taken
            if slot % SET_ROOT_INTERVAL_SLOTS == 0 {
                bank_forks
                    .write()
                    .unwrap()
                    .set_root(slot, &abs_request_sender, None);
                bank.update_accounts_hash();
            }

            // Sleep for a second when making a snapshot archive so the background services get a
            // chance to run (and since FULL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS is a multiple of
            // INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS, we only need to check the one here).
            if slot % INCREMENTAL_SNAPSHOT_ARCHIVE_INTERVAL_SLOTS == 0 {
                std::thread::sleep(Duration::from_secs(1));
            }
        }

        // NOTE: The 5 seconds of sleeping is arbitrary.  This should be plenty of time since the
        // snapshots should be quite small.  If this test fails at `unwrap()` or because the bank
        // slots do not match, increase this sleep duration.
        info!("Sleeping for 5 seconds to give background services time to process snapshot archives...");
        std::thread::sleep(Duration::from_secs(5));
        info!("Awake! Rebuilding bank from latest snapshot archives...");

        let (deserialized_bank, ..) = snapshot_utils::bank_from_latest_snapshot_archives(
            &snapshot_test_config.snapshot_config.bank_snapshots_dir,
            &snapshot_test_config.snapshot_config.snapshot_archives_dir,
            &[snapshot_test_config.accounts_dir.as_ref().to_path_buf()],
            &snapshot_test_config.genesis_config_info.genesis_config,
            None,
            None,
            AccountSecondaryIndexes::default(),
            false,
            None,
            accounts_db::AccountShrinkThreshold::default(),
            false,
            false,
            false,
            Some(ACCOUNTS_DB_CONFIG_FOR_TESTING),
            None,
        )
        .unwrap();

        assert_eq!(deserialized_bank.slot(), LAST_SLOT);
        assert_eq!(
            &deserialized_bank,
            bank_forks
                .read()
                .unwrap()
                .get(deserialized_bank.slot())
                .unwrap()
                .as_ref()
        );

        // Stop the background services
        info!("Shutting down background services...");
        exit.store(true, Ordering::Relaxed);
        accounts_background_service.join().unwrap();
        accounts_hash_verifier.join().unwrap();
        snapshot_packager_service.join().unwrap();
    }
}
