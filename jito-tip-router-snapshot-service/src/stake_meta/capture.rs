//! Frozen-bank capture for stake-metadata generation.
//!
//! This is the only place that should call `Bank` APIs used by stake-meta
//! capture. When the runtime renames or moves those methods, update
//! [`StakeMetaCapture`] only.

use {
    super::{
        DistributionMeta, StakeMetaError, WrappedPriorityFeeDistributionMeta,
        WrappedTipDistributionMeta,
    },
    jito_tip_payment_sdk::{
        CONFIG_ACCOUNT_SEED, TIP_ACCOUNT_SEED_0, TIP_ACCOUNT_SEED_1, TIP_ACCOUNT_SEED_2,
        TIP_ACCOUNT_SEED_3, TIP_ACCOUNT_SEED_4, TIP_ACCOUNT_SEED_5, TIP_ACCOUNT_SEED_6,
        TIP_ACCOUNT_SEED_7, Config,
    },
    log::warn,
    solana_account::{AccountSharedData, ReadableAccount},
    solana_accounts_db::accounts_index::IndexKey,
    solana_clock::Epoch,
    solana_pubkey::Pubkey,
    solana_runtime::{
        bank::Bank,
        stakes::{StakeAccount, Stakes},
    },
    solana_stake_interface::{self as stake, stake_history::StakeHistory, sysvar::stake_history},
    solana_vote::vote_account::VoteAccountsHashMap,
    std::sync::Arc,
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

/// Frozen-bank view with exactly the runtime surface stake-meta capture needs.
pub(crate) struct StakeMetaCapture {
    bank: Arc<Bank>,
}

/// Bank-independent inputs retained while the expensive delegation traversal runs.
///
/// `Cached` owns a persistent, structurally-shared snapshot of the runtime's stake
/// map. It keeps those map nodes alive without retaining the `Bank` or materializing
/// every `StakeAccount` into a second allocation.
pub(super) enum CapturedStakeAccounts {
    Cached(Stakes<StakeAccount>),
    Scanned(Vec<(Pubkey, StakeAccount)>),
}

/// Result of one deterministic distribution-account read from the frozen bank.
pub(super) enum CapturedDistributionAccount {
    Missing,
    Loaded {
        address: Pubkey,
        account: AccountSharedData,
        rent_exempt_amount: u64,
    },
}

/// Fully-owned per-validator inputs captured from a frozen bank.
pub(super) struct CapturedVoter {
    pub(super) vote_pubkey: Pubkey,
    pub(super) validator_node_pubkey: Pubkey,
    pub(super) commission: u8,
    pub(super) tip_distribution_account: CapturedDistributionAccount,
    pub(super) priority_fee_distribution_account: CapturedDistributionAccount,
}

/// The crossing shape between frozen-bank reads and detached metadata computation.
pub(super) struct CapturedStakeMetaInputs {
    pub(super) bank_epoch: Epoch,
    pub(super) bank_slot: u64,
    pub(super) bank_hash: String,
    pub(super) tip_distribution_program_id: Pubkey,
    pub(super) priority_fee_distribution_program_id: Pubkey,
    pub(super) tip_receiver: Pubkey,
    pub(super) tip_receiver_fee: u64,
    pub(super) stake_accounts: CapturedStakeAccounts,
    pub(super) stake_history: StakeHistory,
    pub(super) warmup_cooldown_rate_epoch: Option<Epoch>,
    pub(super) voters: Vec<CapturedVoter>,
}

impl StakeMetaCapture {
    pub(crate) fn new(bank: Arc<Bank>) -> Result<Self, StakeMetaError> {
        if !bank.is_frozen() {
            return Err(StakeMetaError::NotFrozen(bank.slot()));
        }
        Ok(Self { bank })
    }

    pub(super) fn epoch(&self) -> Epoch {
        self.bank.epoch()
    }

    pub(super) fn slot(&self) -> u64 {
        self.bank.slot()
    }

    fn bank_hash(&self) -> String {
        self.bank.hash().to_string()
    }

    fn warmup_cooldown_rate_epoch(&self) -> Option<Epoch> {
        self.bank.new_warmup_cooldown_rate_epoch()
    }

    fn epoch_vote_accounts(&self) -> Result<&VoteAccountsHashMap, StakeMetaError> {
        self.bank
            .epoch_vote_accounts(self.epoch())
            .ok_or(StakeMetaError::NoVoteAccounts(self.slot(), self.epoch()))
    }

    /// Persistent snapshot of delegated stake at this bank's slot.
    ///
    /// Uses `unfiltered_stakes` today because VAT-filtered epoch stakes can be
    /// empty. If a rebase changes semantics, adapt here — not in delegation
    /// traversal.
    pub(super) fn delegated_stakes_snapshot(&self) -> Stakes<StakeAccount> {
        self.bank.unfiltered_stakes()
    }

    fn stake_history(&self) -> StakeHistory {
        let account = self
            .get_account(&stake_history::id())
            .expect("stake history sysvar account should be present in the loaded bank");
        bincode::deserialize(account.data())
            .expect("stake history sysvar account should deserialize")
    }

    fn tip_payment_config(&self, config_pubkey: &Pubkey) -> Result<Config, StakeMetaError> {
        self.get_account(config_pubkey)
            .ok_or_else(|| {
                StakeMetaError::AnchorError(String::from("Config account not found in bank"))
            })
            .and_then(|config_account| {
                Config::deserialize(config_account.data()).map_err(|_| {
                    StakeMetaError::AnchorError(String::from("Failed to deserialize config"))
                })
            })
    }

    fn get_account(&self, pubkey: &Pubkey) -> Option<AccountSharedData> {
        self.bank.get_account(pubkey)
    }

    fn rent_exempt_minimum(&self, data_len: usize) -> u64 {
        self.bank.get_minimum_balance_for_rent_exemption(data_len)
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
    pub(super) fn scan_stake_accounts(
        &self,
    ) -> Result<Vec<(Pubkey, StakeAccount)>, StakeMetaError> {
        let stake_program_id = stake::program::id();
        let mut stake_accounts = self
            .bank
            .get_filtered_indexed_accounts(&IndexKey::ProgramId(stake_program_id), |_| true, None)
            .map_err(StakeMetaError::ScanError)?;

        if stake_accounts.is_empty() {
            warn!("ProgramId index returned no stake accounts; falling back to full program scan");
            stake_accounts = self
                .bank
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
}

fn derive_tip_payment_pubkeys(program_id: &Pubkey) -> TipPaymentPubkeys {
    TipPaymentPubkeys {
        tip_pdas: TIP_ACCOUNT_SEEDS.map(|seed| Pubkey::find_program_address(&[seed], program_id).0),
    }
}

/// Capture a validator's distribution account while the frozen bank is available.
fn capture_distribution_account<DistMeta>(
    bank: &StakeMetaCapture,
    program_id: &Pubkey,
    vote_pubkey: &Pubkey,
) -> CapturedDistributionAccount
where
    DistMeta: DistributionMeta,
{
    let distribution_account_address =
        DistMeta::derive_distribution_account_address(program_id, vote_pubkey, bank.epoch());

    let Some(account) = bank.get_account(&distribution_account_address) else {
        return CapturedDistributionAccount::Missing;
    };

    let rent_exempt_amount = bank.rent_exempt_minimum(account.data().len());

    CapturedDistributionAccount::Loaded {
        address: distribution_account_address,
        account,
        rent_exempt_amount,
    }
}

/// Capture every bank-dependent input needed to generate stake metadata.
///
/// The returned value owns its data and does not retain `bank`. This keeps the
/// direct account reads grouped at the infrastructure boundary and lets the
/// expensive delegation traversal run after the caller releases its `Arc<Bank>`.
pub(super) fn capture_stake_meta_inputs(
    bank: &StakeMetaCapture,
    tip_distribution_program_id: &Pubkey,
    priority_fee_distribution_program_id: &Pubkey,
    tip_payment_program_id: &Pubkey,
) -> Result<CapturedStakeMetaInputs, StakeMetaError> {
    let bank_epoch = bank.epoch();
    let bank_slot = bank.slot();
    let bank_hash = bank.bank_hash();

    let epoch_vote_accounts = bank.epoch_vote_accounts()?;

    let cached_stakes = bank.delegated_stakes_snapshot();

    let stake_history = bank.stake_history();

    let stake_accounts = if cached_stakes.stake_delegations().is_empty() {
        warn!("stakes cache returned no delegations; falling back to a stake-program account scan");
        CapturedStakeAccounts::Scanned(bank.scan_stake_accounts()?)
    } else {
        CapturedStakeAccounts::Cached(cached_stakes)
    };

    // Get config PDA
    let (config_pda, _) =
        Pubkey::find_program_address(&[CONFIG_ACCOUNT_SEED], tip_payment_program_id);
    let config = bank.tip_payment_config(&config_pda)?;

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
                .checked_sub(bank.rent_exempt_minimum(tip_account.data().len()))
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

    // Capture the account-derived portion of every voter before releasing the
    // bank. The epoch-voter population is tiny relative to the stake map, and
    // the final zero-delegation filter still runs in the detached phase.
    let voters = epoch_vote_accounts
        .iter()
        .map(|(vote_pubkey, (_, vote_account))| {
            let tip_distribution_account =
                capture_distribution_account::<WrappedTipDistributionMeta>(
                    bank,
                    tip_distribution_program_id,
                    vote_pubkey,
                );
            let priority_fee_distribution_account =
                capture_distribution_account::<WrappedPriorityFeeDistributionMeta>(
                    bank,
                    priority_fee_distribution_program_id,
                    vote_pubkey,
                );
            let vote_state = vote_account.vote_state_view();

            CapturedVoter {
                vote_pubkey: *vote_pubkey,
                validator_node_pubkey: *vote_state.node_pubkey(),
                commission: vote_state.commission(),
                tip_distribution_account,
                priority_fee_distribution_account,
            }
        })
        .collect();

    Ok(CapturedStakeMetaInputs {
        bank_epoch,
        bank_slot,
        bank_hash,
        tip_distribution_program_id: *tip_distribution_program_id,
        priority_fee_distribution_program_id: *priority_fee_distribution_program_id,
        tip_receiver,
        tip_receiver_fee,
        stake_accounts,
        stake_history,
        warmup_cooldown_rate_epoch: bank.warmup_cooldown_rate_epoch(),
        voters,
    })
}

#[cfg(test)]
pub(super) fn derive_tip_payment_pubkeys_for_tests(program_id: &Pubkey) -> [Pubkey; 8] {
    derive_tip_payment_pubkeys(program_id).tip_pdas
}
