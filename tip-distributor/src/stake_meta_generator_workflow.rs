use {
    crate::{
        fetch_and_deserialize_tip_distribution_account, AccountFetcher, BankAccountFetcher,
        RpcAccountFetcher, StakeMeta, StakeMetaCollection, TipDistributionAccountWrapper,
        TipDistributionMeta,
    },
    itertools::Itertools,
    log::*,
    solana_client::{client_error::ClientError, rpc_client::RpcClient},
    solana_ledger::{
        bank_forks_utils,
        blockstore::{Blockstore, BlockstoreError},
        blockstore_options::{AccessType, BlockstoreOptions, BlockstoreRecoveryMode},
        blockstore_processor::{BlockstoreProcessorError, ProcessOptions},
    },
    solana_runtime::{
        bank::Bank,
        bank_forks::BankForks,
        hardened_unpack::{open_genesis_config, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE},
        snapshot_config::SnapshotConfig,
        snapshot_utils,
        stakes::StakeAccount,
        vote_account::VoteAccount,
    },
    solana_sdk::{
        account::ReadableAccount,
        bs58,
        clock::{Epoch, Slot},
        genesis_config::GenesisConfig,
        pubkey::Pubkey,
    },
    std::{
        collections::HashMap,
        fmt::{Debug, Display, Formatter},
        fs::{self, File},
        io::{BufWriter, Write},
        path::Path,
        sync::{Arc, RwLock},
    },
    thiserror::Error as ThisError,
};

#[derive(ThisError, Debug)]
pub enum Error {
    #[error(transparent)]
    AnchorError(#[from] anchor_lang::error::Error),

    #[error(transparent)]
    BlockstoreError(#[from] BlockstoreError),

    #[error(transparent)]
    BlockstoreProcessorError(#[from] BlockstoreProcessorError),

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    CheckedMathError,

    #[error(transparent)]
    RpcError(#[from] ClientError),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    SnapshotSlotNotFound,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// Runs the entire workflow of creating a bank from a snapshot to writing stake meta-data
/// to a JSON file.
pub fn run_workflow(
    ledger_path: &Path,
    snapshot_bank_hash: String,
    snapshot_slot: Slot,
    tip_distribution_program_id: Pubkey,
    out_path: String,
    rpc_client: RpcClient,
) -> Result<(), Error> {
    info!("Creating bank from ledger path...");
    let bank = create_bank_from_snapshot(ledger_path, snapshot_bank_hash, snapshot_slot)?;

    info!("Generating stake_meta_collection object...");
    let stake_meta_coll =
        generate_stake_meta_collection(&bank, tip_distribution_program_id, Some(rpc_client))?;

    info!("Writing stake_meta_collection to JSON {}...", out_path);
    write_to_json_file(&stake_meta_coll, out_path)?;

    Ok(())
}

fn create_bank_from_snapshot(
    ledger_path: &Path,
    expected_bank_hash: String,
    snapshot_slot: Slot,
) -> Result<Arc<Bank>, Error> {
    let genesis_config = open_genesis_config(ledger_path, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE);
    let blockstore = open_blockstore(ledger_path, AccessType::Secondary, None)?;

    let bank_forks = load_bank_forks(&blockstore, &genesis_config, snapshot_slot)?;

    let working_bank = bank_forks.read().unwrap().working_bank();
    assert_eq!(
        working_bank.hash().to_string(),
        expected_bank_hash,
        "expected working bank hash {}, found {} at slot {}",
        expected_bank_hash,
        working_bank.hash(),
        snapshot_slot
    );
    assert_eq!(
        working_bank.slot(),
        snapshot_slot,
        "expected working bank slot {}, found {}",
        snapshot_slot,
        working_bank.slot()
    );

    Ok(working_bank)
}

fn load_bank_forks(
    blockstore: &Blockstore,
    genesis_config: &GenesisConfig,
    snapshot_slot: Slot,
) -> Result<Arc<RwLock<BankForks>>, Error> {
    let bank_snapshots_dir = blockstore
        .ledger_path()
        .join(if blockstore.is_primary_access() {
            "snapshot"
        } else {
            "snapshot.ledger-tool"
        });

    let full_snapshot_archives_dir = blockstore.ledger_path().to_path_buf();
    let full_snapshot_slot =
        snapshot_utils::get_highest_full_snapshot_archive_slot(&full_snapshot_archives_dir)
            .ok_or(Error::SnapshotSlotNotFound)?;
    assert_eq!(full_snapshot_slot, snapshot_slot);
    if full_snapshot_slot != snapshot_slot {
        assert_eq!(full_snapshot_slot, snapshot_slot, "The expected snapshot was not found, try moving your snapshot to a different directory than the ledger directory if you haven't already. [actual_highest_snapshot={}, expected_highest_snapshot={}]", full_snapshot_slot, snapshot_slot);
    }

    let incremental_snapshot_archives_dir = blockstore.ledger_path().to_path_buf();

    let snapshot_config = SnapshotConfig {
        full_snapshot_archive_interval_slots: Slot::MAX,
        incremental_snapshot_archive_interval_slots: Slot::MAX,
        full_snapshot_archives_dir,
        incremental_snapshot_archives_dir,
        bank_snapshots_dir,
        ..SnapshotConfig::default()
    };

    let account_paths = if blockstore.is_primary_access() {
        vec![blockstore.ledger_path().join("accounts")]
    } else {
        let non_primary_accounts_path = blockstore.ledger_path().join("accounts.ledger-tool");
        info!(
            "Default accounts path is switched aligning with Blockstore's secondary access: {:?}",
            non_primary_accounts_path
        );

        if non_primary_accounts_path.exists() {
            info!("Clearing {:?}", non_primary_accounts_path);
            if let Err(err) = fs::remove_dir_all(&non_primary_accounts_path) {
                error!(
                    "error deleting accounts path {:?}: {}",
                    non_primary_accounts_path, err
                );
                return Err(err.into());
            }
        }

        vec![non_primary_accounts_path]
    };

    Ok(bank_forks_utils::load(
        genesis_config,
        blockstore,
        account_paths,
        None,
        Some(&snapshot_config),
        ProcessOptions {
            new_hard_forks: None,
            halt_at_slot: Some(snapshot_slot),
            poh_verify: false,
            ..ProcessOptions::default()
        },
        None,
        None,
        None,
    )
    .map(|(bank_forks, ..)| bank_forks)?)
}

fn open_blockstore(
    ledger_path: &Path,
    access_type: AccessType,
    wal_recovery_mode: Option<BlockstoreRecoveryMode>,
) -> Result<Blockstore, Error> {
    match Blockstore::open_with_options(
        ledger_path,
        BlockstoreOptions {
            access_type,
            recovery_mode: wal_recovery_mode,
            enforce_ulimit_nofile: true,
            ..BlockstoreOptions::default()
        },
    ) {
        Ok(blockstore) => Ok(blockstore),
        Err(e) => {
            error!("Failed to open ledger at {:?}: {:?}", ledger_path, e);
            Err(e.into())
        }
    }
}

fn write_to_json_file(
    stake_meta_coll: &StakeMetaCollection,
    out_path: String,
) -> Result<(), Error> {
    let file = File::create(out_path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, stake_meta_coll)?;
    writer.flush()?;

    Ok(())
}

/// Creates a collection of [StakeMeta]'s from the given bank.
pub fn generate_stake_meta_collection(
    bank: &Arc<Bank>,
    // Used to derive the PDA and fetch the account data from the Bank.
    tip_distribution_program_id: Pubkey,
    // Optionally used to fetch the tip distribution accounts from an RPC node.
    maybe_rpc_client: Option<RpcClient>,
) -> Result<StakeMetaCollection, Error> {
    assert!(bank.is_frozen());

    let epoch_vote_accounts = bank.epoch_vote_accounts(bank.epoch()).expect(&*format!(
        "No epoch_vote_accounts found for slot {} at epoch {}",
        bank.slot(),
        bank.epoch()
    ));

    let l_stakes = bank.stakes_cache.stakes();
    let delegations = l_stakes.stake_delegations();

    let account_fetcher = if let Some(rpc_client) = maybe_rpc_client {
        Box::new(RpcAccountFetcher { rpc_client }) as Box<dyn AccountFetcher>
    } else {
        Box::new(BankAccountFetcher { bank: bank.clone() }) as Box<dyn AccountFetcher>
    };

    let vote_pk_and_maybe_tdas: Vec<(
        (Pubkey, &VoteAccount),
        Option<TipDistributionAccountWrapper>,
    )> = epoch_vote_accounts
        .iter()
        .map(|(&vote_pubkey, (_total_stake, vote_account))| {
            let tda = fetch_and_deserialize_tip_distribution_account(
                &account_fetcher,
                &vote_pubkey,
                &tip_distribution_program_id,
                bank.epoch(),
            )
            .map_err(Error::from)?;

            Ok(((vote_pubkey, vote_account), tda))
        })
        .collect::<Result<_, Error>>()?;

    let voter_pubkey_to_delegations = group_delegations_by_voter_pubkey(delegations, bank.epoch());

    let mut stake_metas = vec![];
    for ((vote_pubkey, vote_account), maybe_tda) in vote_pk_and_maybe_tdas {
        if let Some(delegations) = voter_pubkey_to_delegations.get(&vote_pubkey).cloned() {
            let total_delegated = delegations.iter().fold(0u64, |sum, delegation| {
                sum.checked_add(delegation.amount_delegated).unwrap()
            });

            let maybe_tip_distribution_meta = if let Some(tda) = maybe_tda {
                let rent_exempt_amount =
                    bank.get_minimum_balance_for_rent_exemption(tda.account_data.data().len());

                Some(TipDistributionMeta::from_tda_wrapper(
                    tda,
                    rent_exempt_amount,
                )?)
            } else {
                None
            };

            stake_metas.push(StakeMeta {
                maybe_tip_distribution_meta,
                validator_vote_account: bs58::encode(vote_pubkey).into_string(),
                delegations: delegations.clone(),
                total_delegated,
                commission: vote_account.vote_state().as_ref().unwrap().commission,
            });
        } else {
            warn!(
                    "voter_pubkey not found in voter_pubkey_to_delegations map [validator_vote_pubkey={}]",
                    vote_pubkey
                );
        }
    }

    Ok(StakeMetaCollection {
        stake_metas,
        tip_distribution_program_id: bs58::encode(tip_distribution_program_id.as_ref())
            .into_string(),
        bank_hash: bank.hash().to_string(),
        epoch: bank.epoch(),
        slot: bank.slot(),
    })
}

/// Given an [EpochStakes] object, return delegations grouped by voter_pubkey (validator delegated to).
fn group_delegations_by_voter_pubkey(
    delegations: &im::HashMap<Pubkey, StakeAccount>,
    epoch: Epoch,
) -> HashMap<Pubkey, Vec<crate::Delegation>> {
    delegations
        .into_iter()
        .filter(|(_stake_pubkey, stake_account)| stake_account.delegation().stake(epoch, None) > 0)
        .into_group_map_by(|(_stake_pubkey, stake_account)| stake_account.delegation().voter_pubkey)
        .into_iter()
        .map(|(voter_pubkey, group)| {
            (
                voter_pubkey,
                group
                    .into_iter()
                    .map(|(stake_pubkey, stake_account)| crate::Delegation {
                        stake_account: bs58::encode(stake_pubkey).into_string(),
                        amount_delegated: stake_account.delegation().stake,
                    })
                    .collect::<Vec<crate::Delegation>>(),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::derive_tip_distribution_account_address,
        anchor_lang::AccountSerialize,
        solana_runtime::genesis_utils::{
            create_genesis_config_with_vote_accounts, GenesisConfigInfo, ValidatorVoteKeypairs,
        },
        solana_sdk::{
            self,
            account::{from_account, AccountSharedData},
            message::Message,
            signature::{Keypair, Signer},
            stake::{
                self,
                state::{Authorized, Lockup},
            },
            stake_history::StakeHistory,
            sysvar,
            transaction::Transaction,
        },
        solana_stake_program::stake_state,
        std::str::FromStr,
        tip_distribution::state::TipDistributionAccount,
    };

    #[test]
    fn test_generate_stake_meta_collection_happy_path() {
        /* 1. Create a Bank seeded with some validator stake accounts */
        let validator_keypairs_0 = ValidatorVoteKeypairs::new_rand();
        let validator_keypairs_1 = ValidatorVoteKeypairs::new_rand();
        let validator_keypairs_2 = ValidatorVoteKeypairs::new_rand();
        let validator_keypairs = vec![
            &validator_keypairs_0,
            &validator_keypairs_1,
            &validator_keypairs_2,
        ];
        const INITIAL_VALIDATOR_STAKES: u64 = 10_000;
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config_with_vote_accounts(
            1_000_000_000,
            &validator_keypairs,
            vec![INITIAL_VALIDATOR_STAKES; 3],
        );

        let bank = Bank::new_for_tests(&genesis_config);

        /* 2. Seed the Bank with [TipDistributionAccount]'s */
        let merkle_root_upload_authority = Pubkey::new_unique();
        let tip_distribution_program_id = Pubkey::new_unique();

        let delegator_0 = Keypair::new();
        let delegator_1 = Keypair::new();
        let delegator_2 = Keypair::new();
        let delegator_3 = Keypair::new();
        let delegator_4 = Keypair::new();

        let delegator_0_pk = delegator_0.pubkey();
        let delegator_1_pk = delegator_1.pubkey();
        let delegator_2_pk = delegator_2.pubkey();
        let delegator_3_pk = delegator_3.pubkey();
        let delegator_4_pk = delegator_4.pubkey();

        let d_0_data = AccountSharedData::new(
            300_000_000_000_000 * 10,
            0,
            &solana_sdk::system_program::id(),
        );
        let d_1_data = AccountSharedData::new(
            100_000_203_000_000 * 10,
            0,
            &solana_sdk::system_program::id(),
        );
        let d_2_data = AccountSharedData::new(
            100_000_235_899_000 * 10,
            0,
            &solana_sdk::system_program::id(),
        );
        let d_3_data = AccountSharedData::new(
            200_000_000_000_000 * 10,
            0,
            &solana_sdk::system_program::id(),
        );
        let d_4_data = AccountSharedData::new(
            100_000_000_777_000 * 10,
            0,
            &solana_sdk::system_program::id(),
        );

        let accounts = vec![
            (&delegator_0_pk, &d_0_data),
            (&delegator_1_pk, &d_1_data),
            (&delegator_2_pk, &d_2_data),
            (&delegator_3_pk, &d_3_data),
            (&delegator_4_pk, &d_4_data),
        ];

        bank.store_accounts((bank.slot(), &accounts[..]));

        /* 3. Delegate some stake to the initial set of validators */
        let mut validator_0_delegations = vec![crate::Delegation {
            stake_account: bs58::encode(validator_keypairs_0.stake_keypair.pubkey().as_ref())
                .into_string(),
            amount_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_0,
            &validator_keypairs_0.vote_keypair.pubkey(),
            30_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 30_000_000_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_1,
            &validator_keypairs_0.vote_keypair.pubkey(),
            3_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 3_000_000_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_2,
            &validator_keypairs_0.vote_keypair.pubkey(),
            33_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 33_000_000_000,
        });

        let mut validator_1_delegations = vec![crate::Delegation {
            stake_account: bs58::encode(validator_keypairs_1.stake_keypair.pubkey().as_ref())
                .into_string(),
            amount_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_3,
            &validator_keypairs_1.vote_keypair.pubkey(),
            4_222_364_000,
        );
        validator_1_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 4_222_364_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_4,
            &validator_keypairs_1.vote_keypair.pubkey(),
            6_000_000_527,
        );
        validator_1_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 6_000_000_527,
        });

        let mut validator_2_delegations = vec![crate::Delegation {
            stake_account: bs58::encode(validator_keypairs_2.stake_keypair.pubkey().as_ref())
                .into_string(),
            amount_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_0,
            &validator_keypairs_2.vote_keypair.pubkey(),
            1_300_123_156,
        );
        validator_2_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 1_300_123_156,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_4,
            &validator_keypairs_2.vote_keypair.pubkey(),
            1_610_565_420,
        );
        validator_2_delegations.push(crate::Delegation {
            stake_account: bs58::encode(stake_account.as_ref()).into_string(),
            amount_delegated: 1_610_565_420,
        });

        /* 4. Run assertions */
        fn warmed_up(bank: &Bank, stake_pubkeys: &[Pubkey]) -> bool {
            for stake_pubkey in stake_pubkeys {
                let stake =
                    stake_state::stake_from(&bank.get_account(stake_pubkey).unwrap()).unwrap();

                if stake.delegation.stake
                    != stake.stake(
                        bank.epoch(),
                        Some(
                            &from_account::<StakeHistory, _>(
                                &bank.get_account(&sysvar::stake_history::id()).unwrap(),
                            )
                            .unwrap(),
                        ),
                    )
                {
                    return false;
                }
            }

            true
        }
        fn next_epoch(bank: &Arc<Bank>) -> Arc<Bank> {
            bank.squash();

            Arc::new(Bank::new_from_parent(
                bank,
                &Pubkey::default(),
                bank.get_slots_in_epoch(bank.epoch()) + bank.slot(),
            ))
        }

        let mut bank = Arc::new(bank);
        let mut stake_pubkeys = validator_0_delegations
            .iter()
            .map(|v| Pubkey::from_str(&*v.stake_account).unwrap())
            .collect::<Vec<Pubkey>>();
        stake_pubkeys.extend(
            validator_1_delegations
                .iter()
                .map(|v| Pubkey::from_str(&*v.stake_account).unwrap()),
        );
        stake_pubkeys.extend(
            validator_2_delegations
                .iter()
                .map(|v| Pubkey::from_str(&*v.stake_account).unwrap()),
        );
        loop {
            if warmed_up(&bank, &stake_pubkeys[..]) {
                break;
            }

            // Cycle thru banks until we're fully warmed up
            bank = next_epoch(&bank);
        }

        let tip_distribution_account_0 = derive_tip_distribution_account_address(
            &tip_distribution_program_id,
            &validator_keypairs_0.vote_keypair.pubkey(),
            bank.epoch(),
        );
        let tip_distribution_account_1 = derive_tip_distribution_account_address(
            &tip_distribution_program_id,
            &validator_keypairs_1.vote_keypair.pubkey(),
            bank.epoch(),
        );
        let tip_distribution_account_2 = derive_tip_distribution_account_address(
            &tip_distribution_program_id,
            &validator_keypairs_2.vote_keypair.pubkey(),
            bank.epoch(),
        );

        let tda_0 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_0.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 50,
            bump: tip_distribution_account_0.1,
        };
        let tda_1 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_1.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 500,
            bump: tip_distribution_account_1.1,
        };
        let tda_2 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_2.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 75,
            bump: tip_distribution_account_2.1,
        };

        let tip_distro_0_tips = 1_000_000 * 10;
        let tip_distro_1_tips = 69_000_420 * 10;
        let tip_distro_2_tips = 789_000_111 * 10;

        let tda_0_fields = (tip_distribution_account_0.0, tda_0.validator_commission_bps);
        let data_0 =
            tda_to_account_shared_data(&tip_distribution_program_id, tip_distro_0_tips, tda_0);
        let tda_1_fields = (tip_distribution_account_1.0, tda_1.validator_commission_bps);
        let data_1 =
            tda_to_account_shared_data(&tip_distribution_program_id, tip_distro_1_tips, tda_1);
        let tda_2_fields = (tip_distribution_account_2.0, tda_2.validator_commission_bps);
        let data_2 =
            tda_to_account_shared_data(&tip_distribution_program_id, tip_distro_2_tips, tda_2);

        let accounts = vec![
            (&tip_distribution_account_0.0, &data_0),
            (&tip_distribution_account_1.0, &data_1),
            (&tip_distribution_account_2.0, &data_2),
        ];
        bank.store_accounts((bank.slot(), &accounts[..]));

        bank.freeze();
        let stake_meta_collection =
            generate_stake_meta_collection(&bank, tip_distribution_program_id, None).unwrap();
        assert_eq!(
            stake_meta_collection.tip_distribution_program_id,
            bs58::encode(tip_distribution_program_id.as_ref()).into_string()
        );
        assert_eq!(stake_meta_collection.slot, bank.slot());
        assert_eq!(stake_meta_collection.epoch, bank.epoch());

        let mut expected_stake_metas = HashMap::new();
        expected_stake_metas.insert(
            bs58::encode(validator_keypairs_0.vote_keypair.pubkey()).into_string(),
            StakeMeta {
                validator_vote_account: bs58::encode(
                    validator_keypairs_0.vote_keypair.pubkey().as_ref(),
                )
                .into_string(),
                delegations: validator_0_delegations.clone(),
                total_delegated: validator_0_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.amount_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority: bs58::encode(
                        merkle_root_upload_authority.as_ref(),
                    )
                    .into_string(),
                    tip_distribution_account: bs58::encode(tda_0_fields.0.as_ref()).into_string(),
                    total_tips: tip_distro_0_tips
                        .checked_sub(
                            bank.get_minimum_balance_for_rent_exemption(
                                TipDistributionAccount::SIZE,
                            ),
                        )
                        .unwrap(),
                    validator_fee_bps: tda_0_fields.1,
                }),
                commission: 0,
            },
        );
        expected_stake_metas.insert(
            bs58::encode(validator_keypairs_1.vote_keypair.pubkey().as_ref()).into_string(),
            StakeMeta {
                validator_vote_account: bs58::encode(
                    validator_keypairs_1.vote_keypair.pubkey().as_ref(),
                )
                .into_string(),
                delegations: validator_1_delegations.clone(),
                total_delegated: validator_1_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.amount_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority: bs58::encode(
                        merkle_root_upload_authority.as_ref(),
                    )
                    .into_string(),
                    tip_distribution_account: bs58::encode(tda_1_fields.0.as_ref()).into_string(),
                    total_tips: tip_distro_1_tips
                        .checked_sub(
                            bank.get_minimum_balance_for_rent_exemption(
                                TipDistributionAccount::SIZE,
                            ),
                        )
                        .unwrap(),
                    validator_fee_bps: tda_1_fields.1,
                }),
                commission: 0,
            },
        );
        expected_stake_metas.insert(
            bs58::encode(validator_keypairs_2.vote_keypair.pubkey().as_ref()).into_string(),
            StakeMeta {
                validator_vote_account: bs58::encode(
                    validator_keypairs_2.vote_keypair.pubkey().as_ref(),
                )
                .into_string(),
                delegations: validator_2_delegations.clone(),
                total_delegated: validator_2_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.amount_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority: bs58::encode(
                        merkle_root_upload_authority.as_ref(),
                    )
                    .into_string(),
                    tip_distribution_account: bs58::encode(tda_2_fields.0.as_ref()).into_string(),
                    total_tips: tip_distro_2_tips
                        .checked_sub(
                            bank.get_minimum_balance_for_rent_exemption(
                                TipDistributionAccount::SIZE,
                            ),
                        )
                        .unwrap(),
                    validator_fee_bps: tda_2_fields.1,
                }),
                commission: 0,
            },
        );

        println!(
            "validator_0 [vote_account={}, stake_account={}]",
            validator_keypairs_0.vote_keypair.pubkey(),
            validator_keypairs_0.stake_keypair.pubkey()
        );
        println!(
            "validator_1 [vote_account={}, stake_account={}]",
            validator_keypairs_1.vote_keypair.pubkey(),
            validator_keypairs_1.stake_keypair.pubkey()
        );
        println!(
            "validator_2 [vote_account={}, stake_account={}]",
            validator_keypairs_2.vote_keypair.pubkey(),
            validator_keypairs_2.stake_keypair.pubkey(),
        );

        assert_eq!(
            expected_stake_metas.len(),
            stake_meta_collection.stake_metas.len()
        );

        for actual_stake_meta in stake_meta_collection.stake_metas {
            let expected_stake_meta = expected_stake_metas
                .get(&actual_stake_meta.validator_vote_account)
                .unwrap();
            assert_eq!(
                expected_stake_meta.maybe_tip_distribution_meta,
                actual_stake_meta.maybe_tip_distribution_meta
            );
            assert_eq!(
                expected_stake_meta.total_delegated,
                actual_stake_meta.total_delegated
            );
            assert_eq!(expected_stake_meta.commission, actual_stake_meta.commission);
            assert_eq!(
                expected_stake_meta.validator_vote_account,
                actual_stake_meta.validator_vote_account
            );

            assert_eq!(
                expected_stake_meta.delegations.len(),
                actual_stake_meta.delegations.len()
            );

            for expected_delegation in &expected_stake_meta.delegations {
                let actual_delegation = actual_stake_meta
                    .delegations
                    .iter()
                    .find(|d| d.stake_account == expected_delegation.stake_account)
                    .unwrap();

                assert_eq!(expected_delegation, actual_delegation);
            }
        }
    }

    /// Helper function that sends a delegate stake instruction to the bank.
    /// Returns the created stake account pubkey.
    fn delegate_stake_helper(
        bank: &Bank,
        from_keypair: &Keypair,
        vote_account: &Pubkey,
        delegation_amount: u64,
    ) -> Pubkey {
        let minimum_delegation = solana_stake_program::get_minimum_delegation(&*bank.feature_set);
        assert!(
            delegation_amount >= minimum_delegation,
            "{}",
            format!(
                "received delegation_amount {}, must be at least {}",
                delegation_amount, minimum_delegation
            )
        );
        if let Some(from_account) = bank.get_account(&from_keypair.pubkey()) {
            assert_eq!(from_account.owner(), &solana_sdk::system_program::id());
        } else {
            panic!("from_account DNE");
        }
        assert!(bank.get_account(vote_account).is_some());

        let stake_keypair = Keypair::new();
        let instructions = stake::instruction::create_account_and_delegate_stake(
            &from_keypair.pubkey(),
            &stake_keypair.pubkey(),
            vote_account,
            &Authorized::auto(&stake_keypair.pubkey()),
            &Lockup::default(),
            delegation_amount,
        );

        let message = Message::new(&instructions[..], Some(&from_keypair.pubkey()));
        let transaction = Transaction::new(
            &[from_keypair, &stake_keypair],
            message,
            bank.last_blockhash(),
        );

        bank.process_transaction(&transaction)
            .map_err(|e| {
                eprintln!("Error delegating stake [error={}]", e);
                e
            })
            .unwrap();

        stake_keypair.pubkey()
    }

    fn tda_to_account_shared_data(
        tip_distribution_program_id: &Pubkey,
        lamports: u64,
        tda: TipDistributionAccount,
    ) -> AccountSharedData {
        let mut account_data = AccountSharedData::new(
            lamports,
            TipDistributionAccount::SIZE,
            tip_distribution_program_id,
        );

        let mut data: [u8; TipDistributionAccount::SIZE] = [0u8; TipDistributionAccount::SIZE];
        let mut cursor = std::io::Cursor::new(&mut data[..]);
        tda.try_serialize(&mut cursor).unwrap();

        account_data.set_data(data.to_vec());
        account_data
    }
}
