use {
    crate::{
        derive_tip_distribution_account_address, derive_tip_payment_pubkeys, Config, StakeMeta,
        StakeMetaCollection, TipDistributionAccount, TipDistributionAccountWrapper,
        TipDistributionMeta,
    },
    anchor_lang::AccountDeserialize,
    itertools::Itertools,
    log::*,
    solana_accounts_db::hardened_unpack::{open_genesis_config, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE},
    solana_client::client_error::ClientError,
    solana_ledger::{
        bank_forks_utils,
        blockstore::{Blockstore, BlockstoreError},
        blockstore_options::{AccessType, BlockstoreOptions, LedgerColumnOptions},
        blockstore_processor::{BlockstoreProcessorError, ProcessOptions},
    },
    solana_runtime::{bank::Bank, snapshot_config::SnapshotConfig, stakes::StakeAccount},
    solana_sdk::{
        account::{ReadableAccount, WritableAccount},
        clock::Slot,
        pubkey::Pubkey,
    },
    solana_vote::vote_account::VoteAccount,
    std::{
        collections::HashMap,
        fmt::{Debug, Display, Formatter},
        fs::File,
        io::{BufWriter, Write},
        mem::size_of,
        path::{Path, PathBuf},
        sync::{atomic::AtomicBool, Arc},
    },
    thiserror::Error,
};

#[derive(Error, Debug)]
pub enum StakeMetaGeneratorError {
    #[error(transparent)]
    AnchorError(#[from] Box<anchor_lang::error::Error>),

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

impl Display for StakeMetaGeneratorError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

/// Runs the entire workflow of creating a bank from a snapshot to writing stake meta-data
/// to a JSON file.
pub fn generate_stake_meta(
    ledger_path: &Path,
    snapshot_slot: &Slot,
    tip_distribution_program_id: &Pubkey,
    out_path: &str,
    tip_payment_program_id: &Pubkey,
) -> Result<(), StakeMetaGeneratorError> {
    info!("Creating bank from ledger path...");
    let bank = create_bank_from_snapshot(ledger_path, snapshot_slot)?;

    info!("Generating stake_meta_collection object...");
    let stake_meta_coll =
        generate_stake_meta_collection(&bank, tip_distribution_program_id, tip_payment_program_id)?;

    info!("Writing stake_meta_collection to JSON {}...", out_path);
    write_to_json_file(&stake_meta_coll, out_path)?;

    Ok(())
}

fn create_bank_from_snapshot(
    ledger_path: &Path,
    snapshot_slot: &Slot,
) -> Result<Arc<Bank>, StakeMetaGeneratorError> {
    let genesis_config = open_genesis_config(ledger_path, MAX_GENESIS_ARCHIVE_UNPACKED_SIZE);
    let snapshot_config = SnapshotConfig {
        full_snapshot_archive_interval_slots: Slot::MAX,
        incremental_snapshot_archive_interval_slots: Slot::MAX,
        full_snapshot_archives_dir: PathBuf::from(ledger_path),
        incremental_snapshot_archives_dir: PathBuf::from(ledger_path),
        bank_snapshots_dir: PathBuf::from(ledger_path),
        ..SnapshotConfig::default()
    };
    let blockstore = Blockstore::open_with_options(
        ledger_path,
        BlockstoreOptions {
            access_type: AccessType::PrimaryForMaintenance,
            recovery_mode: None,
            enforce_ulimit_nofile: false,
            column_options: LedgerColumnOptions::default(),
        },
    )?;
    let (bank_forks, _, _) = bank_forks_utils::load_bank_forks(
        &genesis_config,
        &blockstore,
        vec![PathBuf::from(ledger_path).join(Path::new("stake-meta.accounts"))],
        None,
        Some(&snapshot_config),
        &ProcessOptions::default(),
        None,
        None,
        None,
        Arc::new(AtomicBool::new(false)),
        false,
    );

    let working_bank = bank_forks.read().unwrap().working_bank();
    assert_eq!(
        working_bank.slot(),
        *snapshot_slot,
        "expected working bank slot {}, found {}",
        snapshot_slot,
        working_bank.slot()
    );

    Ok(working_bank)
}

fn write_to_json_file(
    stake_meta_coll: &StakeMetaCollection,
    out_path: &str,
) -> Result<(), StakeMetaGeneratorError> {
    let file = File::create(out_path)?;
    let mut writer = BufWriter::new(file);
    let json = serde_json::to_string_pretty(&stake_meta_coll).unwrap();
    writer.write_all(json.as_bytes())?;
    writer.flush()?;

    Ok(())
}

/// Creates a collection of [StakeMeta]'s from the given bank.
pub fn generate_stake_meta_collection(
    bank: &Arc<Bank>,
    tip_distribution_program_id: &Pubkey,
    tip_payment_program_id: &Pubkey,
) -> Result<StakeMetaCollection, StakeMetaGeneratorError> {
    assert!(bank.is_frozen());

    let epoch_vote_accounts = bank.epoch_vote_accounts(bank.epoch()).unwrap_or_else(|| {
        panic!(
            "No epoch_vote_accounts found for slot {} at epoch {}",
            bank.slot(),
            bank.epoch()
        )
    });

    let l_stakes = bank.stakes_cache.stakes();
    let delegations = l_stakes.stake_delegations();

    let voter_pubkey_to_delegations = group_delegations_by_voter_pubkey(delegations, bank);

    // the last leader in an epoch may not crank the tip program before the epoch is over, which
    // would result in MEV rewards for epoch N not being cranked until epoch N + 1. This means that
    // the account balance in the snapshot could be incorrect.
    // We assume that the rewards sitting in the tip program PDAs are cranked out by the time all of
    // the rewards are claimed.
    let tip_accounts = derive_tip_payment_pubkeys(tip_payment_program_id);
    let account = bank
        .get_account(&tip_accounts.config_pda)
        .expect("config pda exists");

    let config = Config::try_deserialize(&mut account.data()).expect("deserializes configuration");

    let bb_commission_pct: u64 = config.block_builder_commission_pct;
    let tip_receiver: Pubkey = config.tip_receiver;

    // includes the block builder fee
    let excess_tip_balances: u64 = tip_accounts
        .tip_pdas
        .iter()
        .map(|pubkey| {
            let tip_account = bank.get_account(pubkey).expect("tip account exists");
            tip_account
                .lamports()
                .checked_sub(bank.get_minimum_balance_for_rent_exemption(tip_account.data().len()))
                .expect("tip balance underflow")
        })
        .sum();
    // matches math in tip payment program
    let block_builder_tips = excess_tip_balances
        .checked_mul(bb_commission_pct)
        .expect("block_builder_tips overflow")
        .checked_div(100)
        .expect("block_builder_tips division error");
    let tip_receiver_fee = excess_tip_balances
        .checked_sub(block_builder_tips)
        .expect("tip_receiver_fee doesnt underflow");

    let vote_pk_and_maybe_tdas: Vec<(
        (Pubkey, &VoteAccount),
        Option<TipDistributionAccountWrapper>,
    )> = epoch_vote_accounts
        .iter()
        .map(|(vote_pubkey, (_total_stake, vote_account))| {
            let tip_distribution_pubkey = derive_tip_distribution_account_address(
                tip_distribution_program_id,
                vote_pubkey,
                bank.epoch(),
            )
            .0;
            let tda = if let Some(mut account_data) = bank.get_account(&tip_distribution_pubkey) {
                // TDAs may be funded with lamports and therefore exist in the bank, but would fail the deserialization step
                // if the buffer is yet to be allocated thru the init call to the program.
                if let Ok(tip_distribution_account) =
                    TipDistributionAccount::try_deserialize(&mut account_data.data())
                {
                    // this snapshot might have tips that weren't claimed by the time the epoch is over
                    // assume that it will eventually be cranked and credit the excess to this account
                    if tip_distribution_pubkey == tip_receiver {
                        account_data.set_lamports(
                            account_data
                                .lamports()
                                .checked_add(tip_receiver_fee)
                                .expect("tip overflow"),
                        );
                    }
                    Some(TipDistributionAccountWrapper {
                        tip_distribution_account,
                        account_data,
                        tip_distribution_pubkey,
                    })
                } else {
                    None
                }
            } else {
                None
            };
            Ok(((*vote_pubkey, vote_account), tda))
        })
        .collect::<Result<_, StakeMetaGeneratorError>>()?;

    let mut stake_metas = vec![];
    for ((vote_pubkey, vote_account), maybe_tda) in vote_pk_and_maybe_tdas {
        if let Some(mut delegations) = voter_pubkey_to_delegations.get(&vote_pubkey).cloned() {
            let total_delegated = delegations.iter().fold(0u64, |sum, delegation| {
                sum.checked_add(delegation.lamports_delegated).unwrap()
            });

            let maybe_tip_distribution_meta = if let Some(tda) = maybe_tda {
                let actual_len = tda.account_data.data().len();
                let expected_len = 8_usize.saturating_add(size_of::<TipDistributionAccount>());
                if actual_len != expected_len {
                    warn!("len mismatch actual={actual_len}, expected={expected_len}");
                }
                let rent_exempt_amount =
                    bank.get_minimum_balance_for_rent_exemption(tda.account_data.data().len());

                Some(TipDistributionMeta::from_tda_wrapper(
                    tda,
                    rent_exempt_amount,
                )?)
            } else {
                None
            };

            let vote_state = vote_account.vote_state().unwrap();
            delegations.sort();
            stake_metas.push(StakeMeta {
                maybe_tip_distribution_meta,
                validator_node_pubkey: vote_state.node_pubkey,
                validator_vote_account: vote_pubkey,
                delegations,
                total_delegated,
                commission: vote_state.commission,
            });
        } else {
            warn!(
                    "voter_pubkey not found in voter_pubkey_to_delegations map [validator_vote_pubkey={}]",
                    vote_pubkey
                );
        }
    }
    stake_metas.sort();

    Ok(StakeMetaCollection {
        stake_metas,
        tip_distribution_program_id: *tip_distribution_program_id,
        bank_hash: bank.hash().to_string(),
        epoch: bank.epoch(),
        slot: bank.slot(),
    })
}

/// Given an [EpochStakes] object, return delegations grouped by voter_pubkey (validator delegated to).
fn group_delegations_by_voter_pubkey(
    delegations: &im::HashMap<Pubkey, StakeAccount>,
    bank: &Bank,
) -> HashMap<Pubkey, Vec<crate::Delegation>> {
    delegations
        .into_iter()
        .filter(|(_stake_pubkey, stake_account)| {
            stake_account.delegation().stake(
                bank.epoch(),
                None,
                bank.new_warmup_cooldown_rate_epoch(),
            ) > 0
        })
        .into_group_map_by(|(_stake_pubkey, stake_account)| stake_account.delegation().voter_pubkey)
        .into_iter()
        .map(|(voter_pubkey, group)| {
            (
                voter_pubkey,
                group
                    .into_iter()
                    .map(|(stake_pubkey, stake_account)| crate::Delegation {
                        stake_account_pubkey: *stake_pubkey,
                        staker_pubkey: stake_account
                            .stake_state()
                            .authorized()
                            .map(|a| a.staker)
                            .unwrap_or_default(),
                        withdrawer_pubkey: stake_account
                            .stake_state()
                            .authorized()
                            .map(|a| a.withdrawer)
                            .unwrap_or_default(),
                        lamports_delegated: stake_account.delegation().stake,
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
        jito_tip_distribution::state::TipDistributionAccount,
        jito_tip_payment::{
            InitBumps, TipPaymentAccount, CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0,
            TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2, TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4,
            TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6, TIP_ACCOUNT_SEED_7,
        },
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
        let tip_payment_program_id = Pubkey::new_unique();

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

        bank.store_account(&delegator_0_pk, &d_0_data);
        bank.store_account(&delegator_1_pk, &d_1_data);
        bank.store_account(&delegator_2_pk, &d_2_data);
        bank.store_account(&delegator_3_pk, &d_3_data);
        bank.store_account(&delegator_4_pk, &d_4_data);

        /* 3. Delegate some stake to the initial set of validators */
        let mut validator_0_delegations = vec![crate::Delegation {
            stake_account_pubkey: validator_keypairs_0.stake_keypair.pubkey(),
            staker_pubkey: validator_keypairs_0.stake_keypair.pubkey(),
            withdrawer_pubkey: validator_keypairs_0.stake_keypair.pubkey(),
            lamports_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_0,
            &validator_keypairs_0.vote_keypair.pubkey(),
            30_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_0.pubkey(),
            withdrawer_pubkey: delegator_0.pubkey(),
            lamports_delegated: 30_000_000_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_1,
            &validator_keypairs_0.vote_keypair.pubkey(),
            3_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_1.pubkey(),
            withdrawer_pubkey: delegator_1.pubkey(),
            lamports_delegated: 3_000_000_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_2,
            &validator_keypairs_0.vote_keypair.pubkey(),
            33_000_000_000,
        );
        validator_0_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_2.pubkey(),
            withdrawer_pubkey: delegator_2.pubkey(),
            lamports_delegated: 33_000_000_000,
        });

        let mut validator_1_delegations = vec![crate::Delegation {
            stake_account_pubkey: validator_keypairs_1.stake_keypair.pubkey(),
            staker_pubkey: validator_keypairs_1.stake_keypair.pubkey(),
            withdrawer_pubkey: validator_keypairs_1.stake_keypair.pubkey(),
            lamports_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_3,
            &validator_keypairs_1.vote_keypair.pubkey(),
            4_222_364_000,
        );
        validator_1_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_3.pubkey(),
            withdrawer_pubkey: delegator_3.pubkey(),
            lamports_delegated: 4_222_364_000,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_4,
            &validator_keypairs_1.vote_keypair.pubkey(),
            6_000_000_527,
        );
        validator_1_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_4.pubkey(),
            withdrawer_pubkey: delegator_4.pubkey(),
            lamports_delegated: 6_000_000_527,
        });

        let mut validator_2_delegations = vec![crate::Delegation {
            stake_account_pubkey: validator_keypairs_2.stake_keypair.pubkey(),
            staker_pubkey: validator_keypairs_2.stake_keypair.pubkey(),
            withdrawer_pubkey: validator_keypairs_2.stake_keypair.pubkey(),
            lamports_delegated: INITIAL_VALIDATOR_STAKES,
        }];
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_0,
            &validator_keypairs_2.vote_keypair.pubkey(),
            1_300_123_156,
        );
        validator_2_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_0.pubkey(),
            withdrawer_pubkey: delegator_0.pubkey(),
            lamports_delegated: 1_300_123_156,
        });
        let stake_account = delegate_stake_helper(
            &bank,
            &delegator_4,
            &validator_keypairs_2.vote_keypair.pubkey(),
            1_610_565_420,
        );
        validator_2_delegations.push(crate::Delegation {
            stake_account_pubkey: stake_account,
            staker_pubkey: delegator_4.pubkey(),
            withdrawer_pubkey: delegator_4.pubkey(),
            lamports_delegated: 1_610_565_420,
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
                        bank.new_warmup_cooldown_rate_epoch(),
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
                bank.clone(),
                &Pubkey::default(),
                bank.get_slots_in_epoch(bank.epoch()) + bank.slot(),
            ))
        }

        let mut bank = Arc::new(bank);
        let mut stake_pubkeys = validator_0_delegations
            .iter()
            .map(|v| v.stake_account_pubkey)
            .collect::<Vec<Pubkey>>();
        stake_pubkeys.extend(
            validator_1_delegations
                .iter()
                .map(|v| v.stake_account_pubkey),
        );
        stake_pubkeys.extend(
            validator_2_delegations
                .iter()
                .map(|v| v.stake_account_pubkey),
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

        let expires_at = bank.epoch() + 3;

        let tda_0 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_0.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 50,
            expires_at,
            bump: tip_distribution_account_0.1,
        };
        let tda_1 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_1.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 500,
            expires_at: 0,
            bump: tip_distribution_account_1.1,
        };
        let tda_2 = TipDistributionAccount {
            validator_vote_account: validator_keypairs_2.vote_keypair.pubkey(),
            merkle_root_upload_authority,
            merkle_root: None,
            epoch_created_at: bank.epoch(),
            validator_commission_bps: 75,
            expires_at: 0,
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

        let accounts_data = create_config_account_data(&tip_payment_program_id, &bank);
        for (pubkey, data) in accounts_data {
            bank.store_account(&pubkey, &data);
        }

        bank.store_account(&tip_distribution_account_0.0, &data_0);
        bank.store_account(&tip_distribution_account_1.0, &data_1);
        bank.store_account(&tip_distribution_account_2.0, &data_2);

        bank.freeze();
        let stake_meta_collection = generate_stake_meta_collection(
            &bank,
            &tip_distribution_program_id,
            &tip_payment_program_id,
        )
        .unwrap();
        assert_eq!(
            stake_meta_collection.tip_distribution_program_id,
            tip_distribution_program_id
        );
        assert_eq!(stake_meta_collection.slot, bank.slot());
        assert_eq!(stake_meta_collection.epoch, bank.epoch());

        let mut expected_stake_metas = HashMap::new();
        expected_stake_metas.insert(
            validator_keypairs_0.vote_keypair.pubkey(),
            StakeMeta {
                validator_vote_account: validator_keypairs_0.vote_keypair.pubkey(),
                delegations: validator_0_delegations.clone(),
                total_delegated: validator_0_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.lamports_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority,
                    tip_distribution_pubkey: tda_0_fields.0,
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
                validator_node_pubkey: validator_keypairs_0.node_keypair.pubkey(),
            },
        );
        expected_stake_metas.insert(
            validator_keypairs_1.vote_keypair.pubkey(),
            StakeMeta {
                validator_vote_account: validator_keypairs_1.vote_keypair.pubkey(),
                delegations: validator_1_delegations.clone(),
                total_delegated: validator_1_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.lamports_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority,
                    tip_distribution_pubkey: tda_1_fields.0,
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
                validator_node_pubkey: validator_keypairs_1.node_keypair.pubkey(),
            },
        );
        expected_stake_metas.insert(
            validator_keypairs_2.vote_keypair.pubkey(),
            StakeMeta {
                validator_vote_account: validator_keypairs_2.vote_keypair.pubkey(),
                delegations: validator_2_delegations.clone(),
                total_delegated: validator_2_delegations
                    .iter()
                    .fold(0u64, |sum, delegation| {
                        sum.checked_add(delegation.lamports_delegated).unwrap()
                    }),
                maybe_tip_distribution_meta: Some(TipDistributionMeta {
                    merkle_root_upload_authority,
                    tip_distribution_pubkey: tda_2_fields.0,
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
                validator_node_pubkey: validator_keypairs_2.node_keypair.pubkey(),
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
                    .find(|d| d.stake_account_pubkey == expected_delegation.stake_account_pubkey)
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
        let minimum_delegation = solana_stake_program::get_minimum_delegation(&bank.feature_set);
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
            &Authorized::auto(&from_keypair.pubkey()),
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

    fn create_config_account_data(
        tip_payment_program_id: &Pubkey,
        bank: &Bank,
    ) -> Vec<(Pubkey, AccountSharedData)> {
        let mut account_datas = vec![];

        let config_pda =
            Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], tip_payment_program_id);

        let tip_accounts = [
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], tip_payment_program_id),
            Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], tip_payment_program_id),
        ];

        let config = Config {
            tip_receiver: Pubkey::new_unique(),
            block_builder: Pubkey::new_unique(),
            block_builder_commission_pct: 10,
            bumps: InitBumps {
                config: config_pda.1,
                tip_payment_account_0: tip_accounts[0].1,
                tip_payment_account_1: tip_accounts[1].1,
                tip_payment_account_2: tip_accounts[2].1,
                tip_payment_account_3: tip_accounts[3].1,
                tip_payment_account_4: tip_accounts[4].1,
                tip_payment_account_5: tip_accounts[5].1,
                tip_payment_account_6: tip_accounts[6].1,
                tip_payment_account_7: tip_accounts[7].1,
            },
        };

        let mut config_account_data = AccountSharedData::new(
            bank.get_minimum_balance_for_rent_exemption(Config::SIZE),
            Config::SIZE,
            tip_payment_program_id,
        );

        let mut config_data: [u8; Config::SIZE] = [0u8; Config::SIZE];
        let mut config_cursor = std::io::Cursor::new(&mut config_data[..]);
        config.try_serialize(&mut config_cursor).unwrap();
        config_account_data.set_data(config_data.to_vec());
        account_datas.push((config_pda.0, config_account_data));

        account_datas.extend(tip_accounts.into_iter().map(|(pubkey, _)| {
            let mut tip_account_data = AccountSharedData::new(
                bank.get_minimum_balance_for_rent_exemption(TipPaymentAccount::SIZE),
                TipPaymentAccount::SIZE,
                tip_payment_program_id,
            );

            let mut data: [u8; TipPaymentAccount::SIZE] = [0u8; TipPaymentAccount::SIZE];
            let mut cursor = std::io::Cursor::new(&mut data[..]);
            TipPaymentAccount::default()
                .try_serialize(&mut cursor)
                .unwrap();
            tip_account_data.set_data(data.to_vec());

            (pubkey, tip_account_data)
        }));

        account_datas
    }
}
