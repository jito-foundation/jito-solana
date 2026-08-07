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

use {
    crate::config::TipRouterSnapshotConfig,
    borsh::de::BorshDeserialize,
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
    log::{info, warn},
    solana_account::{AccountSharedData, ReadableAccount, WritableAccount},
    solana_accounts_db::{accounts_index::IndexKey, accounts_scan::ScanError},
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        stakes::{StakeAccount, Stakes},
    },
    solana_stake_interface::{self as stake, stake_history::StakeHistory, sysvar::stake_history},
    solana_vote::vote_account::VoteAccount,
    std::{collections::HashMap, mem::size_of, sync::Arc, time::Instant},
};

const TIP_ACCOUNT_SEEDS: [&[u8]; 8] = [
    TIP_ACCOUNT_SEED_0,
    TIP_ACCOUNT_SEED_1,
    TIP_ACCOUNT_SEED_2,
    TIP_ACCOUNT_SEED_3,
    TIP_ACCOUNT_SEED_4,
    TIP_ACCOUNT_SEED_5,
    TIP_ACCOUNT_SEED_6,
    TIP_ACCOUNT_SEED_7,
];

struct TipPaymentPubkeys {
    tip_pdas: [Pubkey; TIP_ACCOUNT_SEEDS.len()],
}

fn derive_tip_payment_pubkeys(program_id: &Pubkey) -> TipPaymentPubkeys {
    TipPaymentPubkeys {
        tip_pdas: TIP_ACCOUNT_SEEDS.map(|seed| Pubkey::find_program_address(&[seed], program_id).0),
    }
}

// ===========================================================================
// TEMPORARY BENCHMARK INSTRUMENTATION - DO NOT MERGE
//
// Attributes the ~1.6s reported by the aggregate `info!` in
// `generate_stake_meta_collection` to individual phases. Removed once the
// numbers are collected.
//
// Two rules make the numbers trustworthy:
//
// 1. No `Instant::now()` inside the ~1.5M-iteration delegation loop - the
//    measurement itself would cost 60-120ms and distort the bucket. The fused
//    traversal is timed once by `collect_delegations_for_epoch_voters`. Only
//    the ~1500-iteration validator loop gets per-iteration timers.
// 2. Accumulators hold NANOSECONDS, not microseconds. A per-iteration cost
//    below 1us truncates to 0 with `as_micros()`, which would silently
//    report a real 150us bucket as zero. Conversion happens at report time.
// ===========================================================================

/// Elapsed nanoseconds since `started_at`.
fn elapsed_ns(started_at: Instant) -> u64 {
    started_at.elapsed().as_nanos() as u64
}

/// Per-iteration accumulators for one kind of distribution account, gathered
/// across the validator loop.
// `pub` only to match the visibility of `get_distribution_meta`, which takes it.
#[derive(Default)]
pub struct DistributionTimings {
    derive_ns: u64,
    load_ns: u64,
    rent_ns: u64,
    /// Returned a fully-deserialized meta.
    n_found: u64,
    /// `bank.get_account` found nothing.
    n_absent: u64,
    /// Present in the bank but rejected (wrong owner, or undeserializable).
    n_rejected: u64,
}

/// Timings for the fused stake-account traversal.
#[derive(Default)]
struct DelegationCollectionTimings {
    collect_ns: u64,
    n_active: u64,
    n_epoch_voter_delegations: u64,
}

#[derive(Default)]
struct BenchTimings {
    // Sequential phases, each measured once.
    vote_accounts_ns: u64,
    stakes_lock_ns: u64,
    /// Retained in the temporary log for direct baseline comparison. This is
    /// zero now that cached `StakeAccount`s are traversed by reference.
    stakes_materialize_ns: u64,
    /// Non-zero only if the stakes cache came back empty and the accounts-db
    /// fallback scan ran.
    stakes_scan_ns: u64,
    stake_history_ns: u64,
    delegations_collect_ns: u64,
    tip_config_ns: u64,
    tip_pdas_ns: u64,
    voter_loop_ns: u64,
    /// The pre-existing aggregate `info!`, kept for comparability. Bucketed
    /// because it is a logger write inside the measured span, and on a
    /// contended logger it is the one unbucketed item that could plausibly
    /// show up as a confusing `unaccounted_us`.
    legacy_log_ns: u64,
    bank_drop_ns: u64,
    metas_sort_ns: u64,

    // Accumulated across the validator loop; already counted inside
    // `voter_loop_ns`, so excluded from the accounted-for sum.
    total_delegated_ns: u64,
    deleg_sort_ns: u64,
    tda: DistributionTimings,
    pfda: DistributionTimings,

    // Denominators. A bucket without one isn't actionable.
    n_cached: u64,
    n_active: u64,
    n_epoch_voter_delegations: u64,
    n_voters: u64,
    n_metas: u64,
    max_delegations_per_voter: u64,
    cached_bytes: u64,
}

impl BenchTimings {
    /// Sum of the top-level sequential phases. The validator-loop accumulators
    /// are deliberately omitted: they are subdivisions of `voter_loop_ns`.
    fn accounted_ns(&self) -> u64 {
        self.vote_accounts_ns
            + self.stakes_lock_ns
            + self.stakes_materialize_ns
            + self.stakes_scan_ns
            + self.stake_history_ns
            + self.delegations_collect_ns
            + self.tip_config_ns
            + self.tip_pdas_ns
            + self.voter_loop_ns
            + self.legacy_log_ns
            + self.bank_drop_ns
            + self.metas_sort_ns
    }

    /// One greppable line: `grep STAKEMETA_BENCH validator.log`.
    fn log(&self, slot: u64, epoch: Epoch, total_ns: u64) {
        // Truncating division is fine here; these are whole-phase totals in
        // the millisecond range, not per-iteration samples.
        let us = |ns: u64| ns / 1_000;
        info!(
            "STAKEMETA_BENCH slot={} epoch={} total_us={} \
             vote_accounts_us={} stakes_lock_us={} stakes_materialize_us={} stakes_scan_us={} \
             stake_history_us={} delegations_collect_us={} tip_config_us={} tip_pdas_us={} \
             voter_loop_us={} \
             legacy_log_us={} bank_drop_us={} metas_sort_us={} unaccounted_us={} \
             total_delegated_us={} deleg_sort_us={} \
             tda_derive_us={} tda_load_us={} tda_rent_us={} \
             pfda_derive_us={} pfda_load_us={} pfda_rent_us={} \
             n_cached={} n_active={} n_epoch_voter_delegations={} n_voters={} n_metas={} \
             n_tda_found={} n_tda_absent={} n_tda_rejected={} \
             n_pfda_found={} n_pfda_absent={} n_pfda_rejected={} \
             max_delegations_per_voter={} cached_bytes={}",
            slot,
            epoch,
            us(total_ns),
            us(self.vote_accounts_ns),
            us(self.stakes_lock_ns),
            us(self.stakes_materialize_ns),
            us(self.stakes_scan_ns),
            us(self.stake_history_ns),
            us(self.delegations_collect_ns),
            us(self.tip_config_ns),
            us(self.tip_pdas_ns),
            us(self.voter_loop_ns),
            us(self.legacy_log_ns),
            us(self.bank_drop_ns),
            us(self.metas_sort_ns),
            us(total_ns.saturating_sub(self.accounted_ns())),
            us(self.total_delegated_ns),
            us(self.deleg_sort_ns),
            us(self.tda.derive_ns),
            us(self.tda.load_ns),
            us(self.tda.rent_ns),
            us(self.pfda.derive_ns),
            us(self.pfda.load_ns),
            us(self.pfda.rent_ns),
            self.n_cached,
            self.n_active,
            self.n_epoch_voter_delegations,
            self.n_voters,
            self.n_metas,
            self.tda.n_found,
            self.tda.n_absent,
            self.tda.n_rejected,
            self.pfda.n_found,
            self.pfda.n_absent,
            self.pfda.n_rejected,
            self.max_delegations_per_voter,
            self.cached_bytes,
        );
    }
}

pub(crate) fn collect_stake_meta(
    config: &TipRouterSnapshotConfig,
    bank: Arc<Bank>,
) -> Result<StakeMetaCollection, StakeMetaError> {
    let (
        Some(tip_distribution_program_id),
        Some(priority_fee_distribution_program_id),
        Some(tip_payment_program_id),
    ) = (
        config.tip_distribution_program_id,
        config.priority_fee_distribution_program_id,
        config.tip_payment_program_id,
    )
    else {
        return Err(StakeMetaError::MissingProgramIds);
    };

    generate_stake_meta_collection(
        bank,
        &tip_distribution_program_id,
        &priority_fee_distribution_program_id,
        &tip_payment_program_id,
    )
}

/// Errors surfaced while generating a [`StakeMetaCollection`].
///
/// Trimmed to the variants the generation path actually produces; the upstream
/// enum carried additional variants for the snapshot/ledger loading paths that
/// are not part of this port.
#[derive(Debug)]
pub(crate) enum StakeMetaError {
    MissingProgramIds,
    AnchorError(String),
    CheckedMathError,
    NoVoteAccounts(u64, u64),
    ScanError(ScanError),
}

impl std::fmt::Display for StakeMetaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingProgramIds => f.write_str(
                "tip distribution, priority-fee distribution, and tip payment program IDs are \
                 required",
            ),
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
pub trait DistributionMeta {
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

pub struct WrappedTipDistributionMeta(pub TipDistributionMeta);
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

pub struct WrappedPriorityFeeDistributionMeta(pub PriorityFeeDistributionMeta);
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
    timings: &mut DistributionTimings,
) -> Option<DistMeta>
where
    DistributionAccount: BorshDeserialize,
    DistMeta: DistributionMeta<DistributionAccountType = DistributionAccount>,
{
    let derive_started_at = Instant::now();
    let distribution_account_address =
        DistMeta::derive_distribution_account_address(program_id, vote_pubkey, bank.epoch());
    timings.derive_ns += elapsed_ns(derive_started_at);

    // BENCH RULE 2: capture the elapsed time BEFORE the `?`. Binding straight
    // to `bank.get_account(..)?` would drop the load time of every validator
    // that has no such account - the common case for priority-fee accounts,
    // and exactly the population that makes this loop look cheap.
    let load_started_at = Instant::now();
    let loaded_account = bank.get_account(&distribution_account_address);
    timings.load_ns += elapsed_ns(load_started_at);
    let Some(mut account) = loaded_account else {
        timings.n_absent += 1;
        return None;
    };

    if account.owner() != program_id {
        timings.n_rejected += 1;
        return None;
    }

    // Funded-but-uninitialized accounts exist in the bank but have no payload to deserialize.
    let Some(serialized_account) = account.data().get(8..) else {
        timings.n_rejected += 1;
        return None;
    };
    let Ok(distribution_account) = DistributionAccount::deserialize(&mut &serialized_account[..])
    else {
        timings.n_rejected += 1;
        return None;
    };

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

    let rent_started_at = Instant::now();
    let rent_exempt_amount = bank.get_minimum_balance_for_rent_exemption(actual_len);
    timings.rent_ns += elapsed_ns(rent_started_at);

    let meta = DistMeta::new_from_account(
        distribution_account,
        account,
        distribution_account_address,
        rent_exempt_amount,
    )
    .ok();
    if meta.is_some() {
        timings.n_found += 1;
    } else {
        timings.n_rejected += 1;
    }
    meta
}

/// Generate the full [`StakeMetaCollection`] for a frozen bank.
///
/// The validator universe comes from the bank's epoch vote accounts.
/// Delegations come from the bank's live stakes cache (see
/// [`cached_stake_accounts`]), never from the VAT-filtered epoch-stakes
/// accessors, whose delegation maps are intentionally empty. The remaining
/// direct account reads are deterministic given the vote pubkeys and epoch.
pub fn generate_stake_meta_collection(
    bank: Arc<Bank>,
    tip_distribution_program_id: &Pubkey,
    priority_fee_distribution_program_id: &Pubkey,
    tip_payment_program_id: &Pubkey,
) -> Result<StakeMetaCollection, StakeMetaError> {
    assert!(bank.is_frozen());
    let stake_meta_started_at = Instant::now();
    let mut bench = BenchTimings::default();

    let bank_epoch = bank.epoch();
    let bank_slot = bank.slot();
    let bank_hash = bank.hash().to_string();

    let vote_accounts_started_at = Instant::now();
    let epoch_vote_accounts = bank
        .epoch_vote_accounts(bank_epoch)
        .ok_or(StakeMetaError::NoVoteAccounts(bank_slot, bank_epoch))?;
    bench.vote_accounts_ns = elapsed_ns(vote_accounts_started_at);
    bench.n_voters = epoch_vote_accounts.len() as u64;

    let mut voter_delegations = Vec::with_capacity(epoch_vote_accounts.len());
    let mut voter_indexes = HashMap::with_capacity(epoch_vote_accounts.len());
    for (vote_pubkey, (_, vote_account)) in epoch_vote_accounts {
        voter_indexes.insert(*vote_pubkey, voter_delegations.len());
        voter_delegations.push(VoterDelegations {
            vote_pubkey,
            vote_account,
            delegations: Vec::new(),
        });
    }

    let cached_stakes = cached_stakes(&bank, &mut bench);

    let stake_history_started_at = Instant::now();
    let stake_history = bincode::deserialize::<StakeHistory>(
        bank.get_account(&stake_history::id())
            .expect("stake history sysvar account should be present in the loaded bank")
            .data(),
    )
    .expect("stake history sysvar account should deserialize");
    bench.stake_history_ns = elapsed_ns(stake_history_started_at);

    let collection_timings = if cached_stakes.stake_delegations().is_empty() {
        warn!("stakes cache returned no delegations; falling back to a stake-program account scan");
        let scan_started_at = Instant::now();
        let scanned_stake_accounts = scan_stake_accounts(&bank)?;
        bench.stakes_scan_ns = elapsed_ns(scan_started_at);
        bench.n_cached = scanned_stake_accounts.len() as u64;
        bench.cached_bytes = bench.n_cached * size_of::<(Pubkey, StakeAccount)>() as u64;
        collect_delegations_for_epoch_voters(
            scanned_stake_accounts
                .iter()
                .map(|(stake_pubkey, stake_account)| (stake_pubkey, stake_account)),
            &stake_history,
            bank_epoch,
            bank.new_warmup_cooldown_rate_epoch(),
            &voter_indexes,
            &mut voter_delegations,
        )
    } else {
        bench.n_cached = cached_stakes.stake_delegations().len() as u64;
        // Retain the old materialized size as an avoided-allocation metric so
        // benchmark logs remain directly comparable with the cloning path.
        bench.cached_bytes = bench.n_cached * size_of::<(Pubkey, StakeAccount)>() as u64;
        collect_delegations_for_epoch_voters(
            cached_stakes.stake_delegations().iter(),
            &stake_history,
            bank_epoch,
            bank.new_warmup_cooldown_rate_epoch(),
            &voter_indexes,
            &mut voter_delegations,
        )
    };
    bench.delegations_collect_ns = collection_timings.collect_ns;
    bench.n_active = collection_timings.n_active;
    bench.n_epoch_voter_delegations = collection_timings.n_epoch_voter_delegations;
    // Release the persistent snapshot while `bank` still owns the underlying
    // HAMT nodes, so this function does not retain the stakes cache through
    // account lookups, sorting, and serialization preparation.
    drop(cached_stakes);
    drop(voter_indexes);

    // Get config PDA
    let tip_config_started_at = Instant::now();
    let (config_pda, _) =
        Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], tip_payment_program_id);
    let config = get_config(&bank, &config_pda)?;
    bench.tip_config_ns = elapsed_ns(tip_config_started_at);

    let bb_commission_pct: u64 = config.block_builder_commission_pct;
    let tip_receiver: Pubkey = config.tip_receiver;

    // the last leader in an epoch may not crank the tip program before the epoch is over, which
    // would result in MEV rewards for epoch N not being cranked until epoch N + 1. This means that
    // the account balance in the snapshot could be incorrect.
    // We assume that the rewards sitting in the tip program PDAs are cranked out by the time all of
    // the rewards are claimed.
    let tip_pdas_started_at = Instant::now();
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
    bench.tip_pdas_ns = elapsed_ns(tip_pdas_started_at);

    // matches math in tip payment program
    let block_builder_tips = excess_tip_balances
        .checked_mul(bb_commission_pct)
        .expect("block_builder_tips overflow")
        .checked_div(100)
        .expect("block_builder_tips division error");

    let tip_receiver_fee = excess_tip_balances
        .checked_sub(block_builder_tips)
        .expect("tip_receiver_fee doesnt underflow");

    let voter_loop_started_at = Instant::now();
    let mut stake_metas = Vec::with_capacity(epoch_vote_accounts.len());
    for VoterDelegations {
        vote_pubkey,
        vote_account,
        delegations,
    } in voter_delegations
    {
        // Ignore vote accounts with 0 delegations
        if !delegations.is_empty() {
            bench.max_delegations_per_voter = bench
                .max_delegations_per_voter
                .max(delegations.len() as u64);
            let stake_meta = build_voter_meta(
                &bank,
                VoterInfo {
                    vote_account,
                    vote_pubkey,
                    tip_distribution_program_id,
                    priority_fee_distribution_program_id,
                    tip_receiver,
                    tip_receiver_fee,
                },
                delegations,
                &mut bench,
            );
            stake_metas.push(stake_meta);
        }
    }
    bench.voter_loop_ns = elapsed_ns(voter_loop_started_at);
    bench.n_metas = stake_metas.len() as u64;

    let legacy_log_started_at = Instant::now();
    info!(
        "calculated tip-router stake metadata for epoch {} at slot {} in {:?}",
        bank_epoch,
        bank_slot,
        stake_meta_started_at.elapsed(),
    );
    bench.legacy_log_ns = elapsed_ns(legacy_log_started_at);

    let bank_drop_started_at = Instant::now();
    drop(bank);
    bench.bank_drop_ns = elapsed_ns(bank_drop_started_at);

    let metas_sort_started_at = Instant::now();
    stake_metas.sort();
    bench.metas_sort_ns = elapsed_ns(metas_sort_started_at);

    // TEMPORARY BENCHMARK INSTRUMENTATION - DO NOT MERGE.
    // Deliberately after the sort so `bank_drop_ns` and `metas_sort_ns` (both
    // outside the pre-existing aggregate `info!` above) are included in
    // `total_us`. Still excluded: the JSON serialize + fsync in
    // `snapshot_artifact::writer`.
    bench.log(bank_slot, bank_epoch, elapsed_ns(stake_meta_started_at));

    Ok(StakeMetaCollection {
        stake_metas,
        tip_distribution_program_id: tip_distribution_program_id.to_owned(),
        priority_fee_distribution_program_id: priority_fee_distribution_program_id.to_owned(),
        bank_hash,
        epoch: bank_epoch,
        slot: bank_slot,
    })
}

/// Inputs needed to build metadata for a single validator vote account.
struct VoterDelegations<'a> {
    vote_pubkey: &'a Pubkey,
    vote_account: &'a VoteAccount,
    delegations: Vec<Delegation>,
}

struct VoterInfo<'a> {
    vote_account: &'a VoteAccount,
    vote_pubkey: &'a Pubkey,
    tip_distribution_program_id: &'a Pubkey,
    priority_fee_distribution_program_id: &'a Pubkey,
    tip_receiver: Pubkey,
    tip_receiver_fee: u64,
}

fn build_voter_meta(
    bank: &Arc<Bank>,
    voter_info: VoterInfo<'_>,
    mut delegations: Vec<Delegation>,
    bench: &mut BenchTimings,
) -> StakeMeta {
    let VoterInfo {
        vote_account,
        vote_pubkey,
        tip_distribution_program_id,
        priority_fee_distribution_program_id,
        tip_receiver,
        tip_receiver_fee,
    } = voter_info;

    let total_delegated_started_at = Instant::now();
    let total_delegated = delegations.iter().fold(0u64, |sum, delegation| {
        sum.checked_add(delegation.lamports_delegated)
            .expect("total delegated lamports should not overflow u64")
    });
    bench.total_delegated_ns += elapsed_ns(total_delegated_started_at);

    let maybe_tip_distribution_meta =
        get_distribution_meta::<TipDistributionAccount, WrappedTipDistributionMeta>(
            bank,
            tip_distribution_program_id,
            vote_pubkey,
            Some(TipReceiverInfo {
                tip_receiver,
                tip_receiver_fee,
            }),
            &mut bench.tda,
        )
        .map(|x| x.0);

    let maybe_priority_fee_distribution_meta = get_distribution_meta::<
        PriorityFeeDistributionAccount,
        WrappedPriorityFeeDistributionMeta,
    >(
        bank,
        priority_fee_distribution_program_id,
        vote_pubkey,
        None,
        &mut bench.pfda,
    )
    .map(|x| x.0);

    let vote_state = vote_account.vote_state_view();
    let deleg_sort_started_at = Instant::now();
    delegations.sort_unstable();
    bench.deleg_sort_ns += elapsed_ns(deleg_sort_started_at);
    StakeMeta {
        maybe_tip_distribution_meta,
        maybe_priority_fee_distribution_meta,
        validator_node_pubkey: *vote_state.node_pubkey(),
        validator_vote_account: *vote_pubkey,
        delegations,
        total_delegated,
        commission: vote_state.commission(),
    }
}

/// Load and deserialize config from Bank. If it does not exist, propagate error.
fn get_config(bank: &Arc<Bank>, config_pubkey: &Pubkey) -> Result<Config, StakeMetaError> {
    bank.get_account(config_pubkey)
        .ok_or_else(|| {
            StakeMetaError::AnchorError(String::from("Config account not found in bank"))
        })
        .and_then(|config_account| {
            Config::deserialize(config_account.data()).map_err(|_| {
                StakeMetaError::AnchorError(String::from("Failed to deserialize config"))
            })
        })
}

/// Take an O(1) persistent snapshot of the bank's live stakes cache.
///
/// The runtime maintains this map synchronously on every stake-account write
/// (epoch rewards and next-epoch stakes are computed from it) and rehydrates
/// it with per-account consistency checks when the validator boots from a
/// snapshot, so it is exactly the delegated-stake state at this bank's slot.
/// Reading it avoids the accounts-db scan (and the ProgramId secondary index)
/// that the out-of-tree operator CLI must use, and is how the original
/// in-tree `tip-distributor` sourced delegations.
///
/// `Bank::unfiltered_stakes` is used rather than `Bank::get_top_epoch_stakes`
/// because VAT filtering (SIMD-0357) rebuilds `Stakes` from vote accounts
/// alone, leaving its delegation map unconditionally empty.
fn cached_stakes(bank: &Bank, bench: &mut BenchTimings) -> Stakes<StakeAccount> {
    let lock_started_at = Instant::now();
    let stakes = bank.unfiltered_stakes();
    bench.stakes_lock_ns = elapsed_ns(lock_started_at);
    stakes
}

/// Scan delegated stake accounts out of accounts-db, identically to the
/// operator CLI's `stake_meta_generator`.
///
/// Retained as a fallback so that if a future rebase ever changes the stakes
/// cache semantics (as VAT did for the epoch-stakes accessors), the service
/// degrades to the operator-CLI-identical scan instead of silently producing
/// an empty, consensus-divergent stake meta. Prefer the ProgramId secondary
/// index when present, and fall back to a full program scan otherwise (an
/// absent index is reported as an empty result).
fn scan_stake_accounts(bank: &Bank) -> Result<Vec<(Pubkey, StakeAccount)>, StakeMetaError> {
    let stake_program_id = stake::program::id();
    let mut stake_accounts = bank
        .get_filtered_indexed_accounts(&IndexKey::ProgramId(stake_program_id), |_| true, None)
        .map_err(StakeMetaError::ScanError)?;

    if stake_accounts.is_empty() {
        warn!("ProgramId index returned no stake accounts; falling back to full program scan");
        stake_accounts = bank
            .get_program_accounts(&stake_program_id)
            .map_err(StakeMetaError::ScanError)?;
    }

    Ok(stake_accounts
        .into_iter()
        .filter_map(|(stake_pubkey, account)| {
            StakeAccount::try_from(account)
                .ok()
                .map(|stake_account| (stake_pubkey, stake_account))
        })
        .collect())
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
    voter_delegations: &mut [VoterDelegations<'_>],
) -> DelegationCollectionTimings {
    let collect_started_at = Instant::now();
    let mut timings = DelegationCollectionTimings::default();

    for (stake_pubkey, stake_account) in stake_accounts {
        let stake_delegation = stake_account.delegation();
        // `stake_v2` uses integer, eBPF-compatible warmup/cooldown math. The deprecated
        // `stake` implementation uses floating-point math and can differ at rounding edges.
        if stake_delegation.stake_v2(epoch, stake_history, warmup_cooldown_rate) == 0 {
            continue;
        }
        timings.n_active += 1;

        let Some(voter_index) = voter_indexes.get(&stake_delegation.voter_pubkey) else {
            continue;
        };
        timings.n_epoch_voter_delegations += 1;

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

    timings.collect_ns = elapsed_ns(collect_started_at);
    timings
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        borsh::BorshSerialize,
        jito_tip_payment_sdk::{CONFIG_SIZE, InitBumps},
        solana_accounts_db::{
            accounts_db::{ACCOUNTS_DB_CONFIG_FOR_TESTING, AccountsDbConfig},
            accounts_index::{
                AccountIndex, AccountSecondaryIndexes, AccountSecondaryIndexesIncludeExclude,
            },
        },
        solana_runtime::{
            bank::BankTestConfig,
            genesis_utils::{
                GenesisConfigInfo, ValidatorVoteKeypairs, create_genesis_config_with_vote_accounts,
            },
        },
        solana_signer::Signer,
        std::collections::HashSet,
    };

    /// Build a bank whose accounts-db carries a stake-program `ProgramId`
    /// index, with `validator`'s vote and stake accounts baked into genesis.
    fn new_indexed_bank_with_validator(validator: &ValidatorVoteKeypairs) -> Arc<Bank> {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config_with_vote_accounts(
            100_000_000_000,
            &[validator],
            vec![1_000_000_000],
        );
        let account_indexes = AccountSecondaryIndexes {
            keys: Some(AccountSecondaryIndexesIncludeExclude {
                exclude: false,
                keys: HashSet::from([stake::program::id()]),
            }),
            indexes: HashSet::from([AccountIndex::ProgramId]),
        };
        Arc::new(Bank::new_with_paths_for_tests(
            &genesis_config,
            Some(BankTestConfig {
                accounts_db_config: AccountsDbConfig {
                    account_indexes: Some(account_indexes),
                    ..ACCOUNTS_DB_CONFIG_FOR_TESTING
                },
            }),
            vec![],
            None,
        ))
    }

    /// The stakes cache must yield exactly the delegated stake accounts the
    /// operator CLI's accounts-db scan produces: the scan is what every other
    /// tip-router operator runs, and merkle-root consensus requires identical
    /// stake metas. If a rebase changes stakes-cache semantics, this is the
    /// tripwire.
    #[test]
    fn test_cached_stake_accounts_match_account_scan() {
        let validator = ValidatorVoteKeypairs::new_rand();
        let bank = new_indexed_bank_with_validator(&validator);
        bank.freeze();

        let cached_stakes = cached_stakes(&bank, &mut BenchTimings::default());
        let mut cached = cached_stakes
            .stake_delegations()
            .iter()
            .map(|(stake_pubkey, stake_account)| (*stake_pubkey, stake_account.clone()))
            .collect::<Vec<_>>();
        let mut scanned = scan_stake_accounts(&bank).unwrap();
        cached.sort_by_key(|(stake_pubkey, _)| *stake_pubkey);
        scanned.sort_by_key(|(stake_pubkey, _)| *stake_pubkey);

        assert!(!cached.is_empty());
        assert_eq!(cached.len(), scanned.len());
        for ((cached_pubkey, cached_account), (scanned_pubkey, scanned_account)) in
            cached.iter().zip(scanned.iter())
        {
            assert_eq!(cached_pubkey, scanned_pubkey);
            assert_eq!(cached_account, scanned_account);
        }
    }

    #[test]
    fn test_indexed_vat_stake_meta_generation() {
        let validator = ValidatorVoteKeypairs::new_rand();
        let bank = new_indexed_bank_with_validator(&validator);

        let tip_payment_program_id = Pubkey::new_unique();
        store_tip_payment_accounts(&bank, &tip_payment_program_id);
        bank.freeze();

        let indexed_stake_accounts = bank
            .get_filtered_indexed_accounts(
                &IndexKey::ProgramId(stake::program::id()),
                |_| true,
                None,
            )
            .unwrap();
        assert!(
            indexed_stake_accounts
                .iter()
                .any(|(pubkey, _)| { pubkey == &validator.stake_keypair.pubkey() })
        );

        let stake_meta = generate_stake_meta_collection(
            bank,
            &Pubkey::new_unique(),
            &Pubkey::new_unique(),
            &tip_payment_program_id,
        )
        .unwrap();
        assert!(!stake_meta.stake_metas.is_empty());
        assert_eq!(
            stake_meta.stake_metas[0].validator_vote_account,
            validator.vote_keypair.pubkey()
        );
        assert!(!stake_meta.stake_metas[0].delegations.is_empty());
    }

    fn store_tip_payment_accounts(bank: &Bank, program_id: &Pubkey) {
        let tip_receiver = Pubkey::new_unique();
        let config = Config {
            tip_receiver,
            block_builder: Pubkey::new_unique(),
            block_builder_commission_pct: 0,
            bumps: InitBumps {
                config: 0,
                tip_payment_account_0: 0,
                tip_payment_account_1: 0,
                tip_payment_account_2: 0,
                tip_payment_account_3: 0,
                tip_payment_account_4: 0,
                tip_payment_account_5: 0,
                tip_payment_account_6: 0,
                tip_payment_account_7: 0,
            },
        };
        let mut config_data = Config::DISCRIMINATOR.to_vec();
        config.serialize(&mut config_data).unwrap();
        assert_eq!(config_data.len(), CONFIG_SIZE);
        let mut config_account = AccountSharedData::new(1, config_data.len(), program_id);
        config_account.set_data_from_slice(&config_data);
        let config_pda = Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], program_id).0;
        bank.store_account(&config_pda, &config_account);

        for tip_pda in derive_tip_payment_pubkeys(program_id).tip_pdas {
            bank.store_account(&tip_pda, &AccountSharedData::new(1, 0, program_id));
        }
    }
}
