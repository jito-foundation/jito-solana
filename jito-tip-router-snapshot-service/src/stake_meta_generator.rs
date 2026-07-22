//! Stake-meta generation over a frozen `Bank`.
//!
//! Ported from `jito-tip-router` (`tip-router-operator-cli/src/stake_meta_generator.rs`
//! and `distribution_meta.rs`), dropping the `_with_stats` instrumentation twin.
//! Produces a fully-owned [`StakeMetaCollection`] from the epoch/stake caches and a
//! small set of deterministic account reads on a frozen bank.
//!
//! Gated behind the `stake-meta-gen` feature so the (optional) Jito SDK git
//! dependencies never touch default validator builds.

use {
    borsh::de::BorshDeserialize,
    itertools::Itertools,
    jito_priority_fee_distribution_sdk::{
        PriorityFeeDistributionAccount, derive_priority_fee_distribution_account_address,
    },
    jito_stake_meta_types::{
        Delegation, PriorityFeeDistributionMeta, StakeMeta, StakeMetaCollection,
        TipDistributionMeta,
    },
    jito_tip_distribution_sdk::{TipDistributionAccount, derive_tip_distribution_account_address},
    jito_tip_payment_sdk::{
        CONFIG_ACCOUNT_SEED, Config, TIP_ACCOUNT_SEED_0, TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2,
        TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4, TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6,
        TIP_ACCOUNT_SEED_7,
    },
    log::warn,
    solana_account::{AccountSharedData, ReadableAccount, WritableAccount, from_account},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::{bank::Bank, stakes::StakeAccount},
    solana_stake_interface::{stake_history::StakeHistory, sysvar::stake_history},
    std::{collections::HashMap, mem::size_of, sync::Arc},
};

/// Errors surfaced while generating a [`StakeMetaCollection`].
///
/// Trimmed to the variants the generation path actually produces; the upstream
/// enum carried additional variants for the snapshot/ledger loading paths that
/// are not part of this port.
#[derive(Debug)]
pub enum StakeMetaGeneratorError {
    AnchorError(String),
    CheckedMathError,
    NoVoteAccounts(u64, u64),
}

impl std::fmt::Display for StakeMetaGeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(self, f)
    }
}

impl std::error::Error for StakeMetaGeneratorError {}

/// Maps an on-chain distribution account to its owned `*DistributionMeta` output
/// type and knows how to derive its PDA.
pub trait DistributionMeta {
    type DistributionAccountType;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaGeneratorError>
    where
        Self: Sized;

    fn derive_distribution_account_address(
        program_id: &Pubkey,
        vote_pubkey: &Pubkey,
        epoch: Epoch,
    ) -> Pubkey;
}

pub struct WrappedTipDistributionMeta(pub TipDistributionMeta);
impl DistributionMeta for WrappedTipDistributionMeta {
    type DistributionAccountType = TipDistributionAccount;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaGeneratorError> {
        Ok(Self(TipDistributionMeta {
            tip_distribution_pubkey: pubkey,
            total_tips: account_data
                .lamports()
                .checked_sub(rent_exempt_amount)
                .ok_or(StakeMetaGeneratorError::CheckedMathError)?,
            validator_fee_bps: distribution_account.validator_commission_bps,
            merkle_root_upload_authority: distribution_account.merkle_root_upload_authority,
        }))
    }

    fn derive_distribution_account_address(
        program_id: &Pubkey,
        vote_pubkey: &Pubkey,
        epoch: Epoch,
    ) -> Pubkey {
        derive_tip_distribution_account_address(program_id, vote_pubkey, epoch).0
    }
}

pub struct WrappedPriorityFeeDistributionMeta(pub PriorityFeeDistributionMeta);
impl DistributionMeta for WrappedPriorityFeeDistributionMeta {
    type DistributionAccountType = PriorityFeeDistributionAccount;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaGeneratorError> {
        Ok(Self(PriorityFeeDistributionMeta {
            priority_fee_distribution_pubkey: pubkey,
            total_tips: account_data
                .lamports()
                .checked_sub(rent_exempt_amount)
                .ok_or(StakeMetaGeneratorError::CheckedMathError)?,
            validator_fee_bps: distribution_account.validator_commission_bps,
            merkle_root_upload_authority: distribution_account.merkle_root_upload_authority,
        }))
    }

    fn derive_distribution_account_address(
        program_id: &Pubkey,
        vote_pubkey: &Pubkey,
        epoch: Epoch,
    ) -> Pubkey {
        derive_priority_fee_distribution_account_address(program_id, vote_pubkey, epoch).0
    }
}

pub struct TipReceiverInfo {
    pub tip_receiver: Pubkey,
    pub tip_receiver_fee: u64,
}

/// Read and deserialize a validator's distribution account (tip or priority-fee)
/// from the bank, returning its owned meta. Missing accounts yield `None`.
pub fn get_distribution_meta<DistributionAccount, DistMeta>(
    bank: &Arc<Bank>,
    program_id: &Pubkey,
    vote_pubkey: &Pubkey,
    tip_receiver_info: Option<TipReceiverInfo>,
) -> Option<DistMeta>
where
    DistributionAccount: BorshDeserialize,
    DistMeta: DistributionMeta<DistributionAccountType = DistributionAccount>,
{
    let distribution_account_pubkey =
        DistMeta::derive_distribution_account_address(program_id, vote_pubkey, bank.epoch());
    bank.get_account(&distribution_account_pubkey).map_or_else(
        || None,
        |mut account_data| {
            if account_data.owner() != program_id {
                return None;
            }
            // DAs may be funded with lamports and therefore exist in the bank, but would fail the
            // deserialization step if the buffer is yet to be allocated thru the init call to the
            // program.
            let distribution_account_data = account_data.data().get(8..)?;
            DistributionAccount::deserialize(&mut &distribution_account_data[..]).map_or_else(
                |_| None,
                |distribution_account| {
                    // [TIp Distribution ONLY] this snapshot might have tips that weren't claimed
                    // by the time the epoch is over assume that it will eventually be cranked and
                    // credit the excess to this account
                    if let Some(tip_receiver_info) = tip_receiver_info {
                        if distribution_account_pubkey == tip_receiver_info.tip_receiver {
                            account_data.set_lamports(
                                account_data
                                    .lamports()
                                    .checked_add(tip_receiver_info.tip_receiver_fee)
                                    .expect("tip overflow"),
                            );
                        }
                    }

                    let actual_len = account_data.data().len();
                    let expected_len = 8_usize.saturating_add(size_of::<DistributionAccount>());
                    if actual_len != expected_len {
                        warn!("len mismatch actual={actual_len}, expected={expected_len}");
                    }
                    let rent_exempt_amount =
                        bank.get_minimum_balance_for_rent_exemption(account_data.data().len());

                    DistMeta::new_from_account(
                        distribution_account,
                        account_data,
                        distribution_account_pubkey,
                        rent_exempt_amount,
                    )
                    .ok()
                },
            )
        },
    )
}

struct TipPaymentPubkeys {
    _config_pda: Pubkey,
    tip_pdas: Vec<Pubkey>,
}

fn derive_tip_payment_pubkeys(program_id: &Pubkey) -> TipPaymentPubkeys {
    let config_pda = Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], program_id).0;
    let tip_pda_0 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_0], program_id).0;
    let tip_pda_1 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_1], program_id).0;
    let tip_pda_2 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_2], program_id).0;
    let tip_pda_3 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_3], program_id).0;
    let tip_pda_4 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_4], program_id).0;
    let tip_pda_5 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_5], program_id).0;
    let tip_pda_6 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_6], program_id).0;
    let tip_pda_7 = Pubkey::find_program_address(&[TIP_ACCOUNT_SEED_7], program_id).0;

    TipPaymentPubkeys {
        _config_pda: config_pda,
        tip_pdas: vec![
            tip_pda_0, tip_pda_1, tip_pda_2, tip_pda_3, tip_pda_4, tip_pda_5, tip_pda_6, tip_pda_7,
        ],
    }
}

/// Generate the full [`StakeMetaCollection`] for a frozen bank.
///
/// The stake/vote universe comes from the bank's epoch/stake caches; the direct
/// account reads (stake-history sysvar, tip-payment config + holding PDAs, and
/// per-validator distribution PDAs) are deterministic given the vote pubkeys and
/// epoch.
pub fn generate_stake_meta_collection(
    bank: &Arc<Bank>,
    tip_distribution_program_id: &Pubkey,
    priority_fee_distribution_program_id: &Pubkey,
    tip_payment_program_id: &Pubkey,
) -> Result<StakeMetaCollection, StakeMetaGeneratorError> {
    assert!(bank.is_frozen());

    let epoch_vote_accounts =
        bank.epoch_vote_accounts(bank.epoch())
            .ok_or(StakeMetaGeneratorError::NoVoteAccounts(
                bank.slot(),
                bank.epoch(),
            ))?;

    let top_epoch_stakes = bank.get_top_epoch_stakes();
    let delegations = top_epoch_stakes.stake_delegations();
    let voter_pubkey_to_delegations = group_delegations_by_voter_pubkey(delegations, bank);

    // Get config PDA
    let (config_pda, _) =
        Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], tip_payment_program_id);
    let config = get_config(bank, &config_pda)?;

    let bb_commission_pct: u64 = config.block_builder_commission_pct;
    let tip_receiver: Pubkey = config.tip_receiver;

    // the last leader in an epoch may not crank the tip program before the epoch is over, which
    // would result in MEV rewards for epoch N not being cranked until epoch N + 1. This means that
    // the account balance in the snapshot could be incorrect.
    // We assume that the rewards sitting in the tip program PDAs are cranked out by the time all of
    // the rewards are claimed.
    let tip_accounts = derive_tip_payment_pubkeys(tip_payment_program_id);

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

    let mut stake_metas: Vec<StakeMeta> = epoch_vote_accounts
        .iter()
        .filter_map(|(vote_pubkey, (_, vote_account))| {
            voter_pubkey_to_delegations
                .get(vote_pubkey)
                .cloned()
                .map_or_else(
                    || {
                        warn!(
                            "voter_pubkey not found in voter_pubkey_to_delegations map \
                             [validator_vote_pubkey={}]",
                            vote_pubkey
                        );
                        None
                    },
                    |mut delegations| {
                        let total_delegated = delegations.iter().fold(0u64, |sum, delegation| {
                            sum.checked_add(delegation.lamports_delegated)
                                .expect("total delegated lamports should not overflow u64")
                        });

                        let maybe_tip_distribution_meta = get_distribution_meta::<
                            TipDistributionAccount,
                            WrappedTipDistributionMeta,
                        >(
                            bank,
                            tip_distribution_program_id,
                            vote_pubkey,
                            Some(TipReceiverInfo {
                                tip_receiver,
                                tip_receiver_fee,
                            }),
                        );

                        let maybe_priority_fee_distribution_meta = get_distribution_meta::<
                            PriorityFeeDistributionAccount,
                            WrappedPriorityFeeDistributionMeta,
                        >(
                            bank,
                            priority_fee_distribution_program_id,
                            vote_pubkey,
                            None,
                        );

                        let vote_state = vote_account.vote_state_view();
                        delegations.sort();
                        Some(StakeMeta {
                            maybe_tip_distribution_meta: maybe_tip_distribution_meta.map(|x| x.0),
                            maybe_priority_fee_distribution_meta:
                                maybe_priority_fee_distribution_meta.map(|x| x.0),
                            validator_node_pubkey: *vote_state.node_pubkey(),
                            validator_vote_account: *vote_pubkey,
                            delegations,
                            total_delegated,
                            commission: vote_state.commission(),
                        })
                    },
                )
        })
        .collect();

    stake_metas.sort();

    Ok(StakeMetaCollection {
        stake_metas,
        tip_distribution_program_id: tip_distribution_program_id.to_owned(),
        priority_fee_distribution_program_id: priority_fee_distribution_program_id.to_owned(),
        bank_hash: bank.hash().to_string(),
        epoch: bank.epoch(),
        slot: bank.slot(),
    })
}

/// Load and deserialize config from Bank. If it does not exist, propagate error.
fn get_config(bank: &Arc<Bank>, config_pubkey: &Pubkey) -> Result<Config, StakeMetaGeneratorError> {
    bank.get_account(config_pubkey)
        .ok_or_else(|| {
            StakeMetaGeneratorError::AnchorError(String::from("Config account not found in bank"))
        })
        .and_then(|config_account| {
            Config::deserialize(config_account.data()).map_err(|_| {
                StakeMetaGeneratorError::AnchorError(String::from("Failed to deserialize config"))
            })
        })
}

/// Given the bank's stake delegations, return delegations grouped by voter_pubkey
/// (validator delegated to), filtered to those with non-zero active stake.
fn group_delegations_by_voter_pubkey(
    delegations: &imbl::HashMap<Pubkey, StakeAccount>,
    bank: &Bank,
) -> HashMap<Pubkey, Vec<Delegation>> {
    delegations
        .into_iter()
        .filter(|(_stake_pubkey, stake_account)| {
            stake_account.delegation().stake(
                bank.epoch(),
                &from_account::<StakeHistory, _>(
                    &bank.get_account(&stake_history::id()).expect(
                        "stake history sysvar account should be present in the loaded bank",
                    ),
                )
                .expect("stake history sysvar account should deserialize"),
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
                    .map(|(stake_pubkey, stake_account)| Delegation {
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
                    .collect::<Vec<Delegation>>(),
            )
        })
        .collect()
}
