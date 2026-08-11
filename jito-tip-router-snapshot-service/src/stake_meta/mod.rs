//! Stake metadata extraction over a frozen `Bank`.
//!
//! Ported from `jito-tip-router` (`tip-router-operator-cli/src/stake_meta_generator.rs`
//! and `distribution_meta.rs`), dropping the `_with_stats` instrumentation twin.
//! Produces a fully-owned [`StakeMetaCollection`] from the bank's live stakes
//! cache and a small set of deterministic account reads on a frozen bank.
//!
//! The output must stay byte-identical to the operator CLI's: NCN consensus
//! rides on every tip-router operator deriving the same merkle roots from the
//! same slot. Delegations therefore come from the runtime's stakes cache (the
//! same source the original in-tree `tip-distributor` used), with the operator
//! CLI's account scan retained only as a fallback; a parity test pins the two
//! paths to identical results.

mod capture;

pub(crate) use capture::StakeMetaCapture;
use {
    crate::config::TipRouterSnapshotConfig,
    borsh::de::BorshDeserialize,
    capture::{CapturedStakeAccounts, CapturedStakeMetaInputs, CapturedVoter},
    jito_priority_fee_distribution_sdk::{
        PriorityFeeDistributionAccount, derive_priority_fee_distribution_account_address,
    },
    jito_stake_meta_types::{
        Delegation, PriorityFeeDistributionMeta, StakeMeta, StakeMetaCollection,
        TipDistributionMeta,
    },
    jito_tip_distribution_sdk::{TipDistributionAccount, derive_tip_distribution_account_address},
    log::{info, warn},
    solana_account::{AccountSharedData, ReadableAccount, WritableAccount},
    solana_accounts_db::accounts_scan::ScanError,
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::stakes::StakeAccount,
    solana_stake_interface::stake_history::StakeHistory,
    std::{collections::HashMap, mem::size_of, time::Instant},
};

pub(crate) fn collect_stake_meta(
    config: &TipRouterSnapshotConfig,
    bank: StakeMetaCapture,
) -> Result<StakeMetaCollection, StakeMetaError> {
    generate_stake_meta_collection(
        bank,
        &config.tip_distribution_program_id,
        &config.priority_fee_distribution_program_id,
        &config.tip_payment_program_id,
    )
}

/// Errors surfaced while generating a [`StakeMetaCollection`].
///
/// Trimmed to the variants the generation path actually produces; the upstream
/// enum carried additional variants for the snapshot/ledger loading paths that
/// are not part of this port.
#[derive(Debug)]
pub(crate) enum StakeMetaError {
    NotFrozen(u64),
    AnchorError(String),
    CheckedMathError,
    NoVoteAccounts(u64, u64),
    ScanError(ScanError),
}

impl std::fmt::Display for StakeMetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFrozen(slot) => {
                write!(f, "stake metadata requires a frozen bank at slot {slot}")
            }
            Self::AnchorError(error) => {
                write!(f, "failed to read tip payment configuration: {error}")
            }
            Self::CheckedMathError => f.write_str("overflow while calculating stake metadata"),
            Self::NoVoteAccounts(slot, epoch) => {
                write!(f, "no vote accounts found at slot {slot} in epoch {epoch}")
            }
            Self::ScanError(error) => write!(f, "failed to scan stake accounts: {error}"),
        }
    }
}

impl std::error::Error for StakeMetaError {}

/// Maps an on-chain distribution account to its owned `*DistributionMeta` output
/// type and knows how to derive its PDA.
pub(super) trait DistributionMeta {
    type DistributionAccountType;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaError>
    where
        Self: Sized;

    fn derive_distribution_account_address(
        program_id: &Pubkey,
        vote_pubkey: &Pubkey,
        epoch: Epoch,
    ) -> Pubkey;
}

pub(super) struct WrappedTipDistributionMeta(pub TipDistributionMeta);
impl DistributionMeta for WrappedTipDistributionMeta {
    type DistributionAccountType = TipDistributionAccount;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaError> {
        Ok(Self(TipDistributionMeta {
            tip_distribution_pubkey: pubkey,
            total_tips: account_data
                .lamports()
                .checked_sub(rent_exempt_amount)
                .ok_or(StakeMetaError::CheckedMathError)?,
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

pub(super) struct WrappedPriorityFeeDistributionMeta(pub PriorityFeeDistributionMeta);
impl DistributionMeta for WrappedPriorityFeeDistributionMeta {
    type DistributionAccountType = PriorityFeeDistributionAccount;

    fn new_from_account(
        distribution_account: Self::DistributionAccountType,
        account_data: AccountSharedData,
        pubkey: Pubkey,
        rent_exempt_amount: u64,
    ) -> Result<Self, StakeMetaError> {
        Ok(Self(PriorityFeeDistributionMeta {
            priority_fee_distribution_pubkey: pubkey,
            total_tips: account_data
                .lamports()
                .checked_sub(rent_exempt_amount)
                .ok_or(StakeMetaError::CheckedMathError)?,
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

struct TipReceiverInfo {
    tip_receiver: Pubkey,
    tip_receiver_fee: u64,
}

/// Validate and deserialize a captured distribution account without accessing the bank.
fn build_distribution_meta<DistributionAccount, DistMeta>(
    captured_account: capture::CapturedDistributionAccount,
    program_id: &Pubkey,
    tip_receiver_info: Option<TipReceiverInfo>,
) -> Option<DistMeta>
where
    DistributionAccount: BorshDeserialize,
    DistMeta: DistributionMeta<DistributionAccountType = DistributionAccount>,
{
    let capture::CapturedDistributionAccount::Loaded {
        address: distribution_account_address,
        mut account,
        rent_exempt_amount,
    } = captured_account
    else {
        return None;
    };

    if account.owner() != program_id {
        return None;
    }

    // Funded-but-uninitialized accounts exist in the bank but have no payload to deserialize.
    let serialized_account = account.data().get(8..)?;
    let distribution_account =
        DistributionAccount::deserialize(&mut &serialized_account[..]).ok()?;

    // Tip distributions may contain tips unclaimed at epoch end; credit them to the receiver.
    if let Some(tip_receiver_info) = tip_receiver_info
        && distribution_account_address == tip_receiver_info.tip_receiver
    {
        account.set_lamports(
            account
                .lamports()
                .checked_add(tip_receiver_info.tip_receiver_fee)
                .expect("tip receiver balance overflow"),
        );
    }

    let actual_len = account.data().len();
    let expected_len = 8 + size_of::<DistributionAccount>();
    if actual_len != expected_len {
        // This would likely suggest that we have an old version of
        // jito-{tip|priority-fee}-distribution-sdk pinned
        warn!("distribution account length mismatch: actual={actual_len}, expected={expected_len}");
    }

    DistMeta::new_from_account(
        distribution_account,
        account,
        distribution_account_address,
        rent_exempt_amount,
    )
    .ok()
}

/// Build stake metadata using only owned inputs captured from a frozen bank.
fn build_stake_meta_collection(captured: CapturedStakeMetaInputs) -> StakeMetaCollection {
    let CapturedStakeMetaInputs {
        bank_epoch,
        bank_slot,
        bank_hash,
        tip_distribution_program_id,
        priority_fee_distribution_program_id,
        tip_receiver,
        tip_receiver_fee,
        stake_accounts,
        stake_history,
        warmup_cooldown_rate_epoch,
        voters,
    } = captured;

    let mut voter_delegations = Vec::with_capacity(voters.len());
    let mut voter_indexes = HashMap::with_capacity(voters.len());
    for voter in voters {
        voter_indexes.insert(voter.vote_pubkey, voter_delegations.len());
        voter_delegations.push(VoterDelegations {
            voter,
            delegations: Vec::new(),
        });
    }

    match &stake_accounts {
        CapturedStakeAccounts::Cached(cached_stakes) => collect_delegations_for_epoch_voters(
            cached_stakes.stake_delegations().iter(),
            &stake_history,
            bank_epoch,
            warmup_cooldown_rate_epoch,
            &voter_indexes,
            &mut voter_delegations,
        ),
        CapturedStakeAccounts::Scanned(scanned_stake_accounts) => {
            collect_delegations_for_epoch_voters(
                scanned_stake_accounts
                    .iter()
                    .map(|(stake_pubkey, stake_account)| (stake_pubkey, stake_account)),
                &stake_history,
                bank_epoch,
                warmup_cooldown_rate_epoch,
                &voter_indexes,
                &mut voter_delegations,
            )
        }
    }
    drop(stake_accounts);
    drop(voter_indexes);

    let mut stake_metas = Vec::with_capacity(voter_delegations.len());
    for VoterDelegations { voter, delegations } in voter_delegations {
        // Ignore vote accounts with 0 delegations.
        if delegations.is_empty() {
            continue;
        }

        stake_metas.push(build_voter_meta(
            voter,
            &tip_distribution_program_id,
            &priority_fee_distribution_program_id,
            tip_receiver,
            tip_receiver_fee,
            delegations,
        ));
    }

    StakeMetaCollection {
        stake_metas,
        tip_distribution_program_id,
        priority_fee_distribution_program_id,
        bank_hash,
        epoch: bank_epoch,
        slot: bank_slot,
    }
}

/// Generate the full [`StakeMetaCollection`] for a frozen bank.
///
/// The bank is retained only while capturing frozen runtime state and direct
/// account reads. Delegation traversal, aggregation, and sorting operate on
/// fully-owned inputs after the worker releases its `Arc<Bank>`.
pub fn generate_stake_meta_collection(
    bank: StakeMetaCapture,
    tip_distribution_program_id: &Pubkey,
    priority_fee_distribution_program_id: &Pubkey,
    tip_payment_program_id: &Pubkey,
) -> Result<StakeMetaCollection, StakeMetaError> {
    let stake_meta_started_at = Instant::now();
    let captured = capture::capture_stake_meta_inputs(
        &bank,
        tip_distribution_program_id,
        priority_fee_distribution_program_id,
        tip_payment_program_id,
    )?;
    drop(bank);

    let mut stake_meta_collection = build_stake_meta_collection(captured);

    for stake_meta in &mut stake_meta_collection.stake_metas {
        stake_meta.delegations.sort_unstable();
    }
    stake_meta_collection.stake_metas.sort();

    info!(
        "calculated tip-router stake metadata for epoch {} at slot {} in {:?}",
        stake_meta_collection.epoch,
        stake_meta_collection.slot,
        stake_meta_started_at.elapsed(),
    );

    Ok(stake_meta_collection)
}

/// Inputs needed to build metadata for a single validator vote account.
struct VoterDelegations {
    voter: CapturedVoter,
    delegations: Vec<Delegation>,
}

fn build_voter_meta(
    voter: CapturedVoter,
    tip_distribution_program_id: &Pubkey,
    priority_fee_distribution_program_id: &Pubkey,
    tip_receiver: Pubkey,
    tip_receiver_fee: u64,
    delegations: Vec<Delegation>,
) -> StakeMeta {
    let CapturedVoter {
        vote_pubkey,
        validator_node_pubkey,
        commission,
        tip_distribution_account,
        priority_fee_distribution_account,
    } = voter;

    let total_delegated = delegations.iter().fold(0u64, |sum, delegation| {
        sum.checked_add(delegation.lamports_delegated)
            .expect("total delegated lamports should not overflow u64")
    });

    let maybe_tip_distribution_meta =
        build_distribution_meta::<TipDistributionAccount, WrappedTipDistributionMeta>(
            tip_distribution_account,
            tip_distribution_program_id,
            Some(TipReceiverInfo {
                tip_receiver,
                tip_receiver_fee,
            }),
        )
        .map(|meta| meta.0);

    let maybe_priority_fee_distribution_meta = build_distribution_meta::<
        PriorityFeeDistributionAccount,
        WrappedPriorityFeeDistributionMeta,
    >(
        priority_fee_distribution_account,
        priority_fee_distribution_program_id,
        None,
    )
    .map(|meta| meta.0);

    StakeMeta {
        maybe_tip_distribution_meta,
        maybe_priority_fee_distribution_meta,
        validator_node_pubkey,
        validator_vote_account: vote_pubkey,
        delegations,
        total_delegated,
        commission,
    }
}

/// Traverse borrowed stake accounts once, filter active delegations, and build
/// the final delegation vectors for the bank's epoch-vote-account universe.
///
/// Keeping the persistent stakes snapshot alive makes every `StakeAccount`
/// borrow valid without cloning the roughly 300-byte cached value. Building
/// final `Delegation`s directly also avoids intermediate per-voter vectors of
/// `StakeAccount`s.
fn collect_delegations_for_epoch_voters<'a>(
    stake_accounts: impl IntoIterator<Item = (&'a Pubkey, &'a StakeAccount)>,
    stake_history: &StakeHistory,
    epoch: Epoch,
    warmup_cooldown_rate: Option<Epoch>,
    voter_indexes: &HashMap<Pubkey, usize>,
    voter_delegations: &mut [VoterDelegations],
) {
    for (stake_pubkey, stake_account) in stake_accounts {
        let stake_delegation = stake_account.delegation();
        // `stake_v2` uses integer, eBPF-compatible warmup/cooldown math. The deprecated
        // `stake` implementation uses floating-point math and can differ at rounding edges.
        if stake_delegation.stake_v2(epoch, stake_history, warmup_cooldown_rate) == 0 {
            continue;
        }

        let Some(voter_index) = voter_indexes.get(&stake_delegation.voter_pubkey) else {
            continue;
        };

        let authorized = stake_account.stake_state().authorized().unwrap_or_default();
        voter_delegations[*voter_index]
            .delegations
            .push(Delegation {
                stake_account_pubkey: *stake_pubkey,
                staker_pubkey: authorized.staker,
                withdrawer_pubkey: authorized.withdrawer,
                lamports_delegated: stake_delegation.stake,
            });
    }
}

