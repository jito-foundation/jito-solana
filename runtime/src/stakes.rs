//! Stakes serve as a cache of stake and vote accounts to derive
//! node stakes
use {
    crate::{stake_account, stake_history::StakeHistory},
    dashmap::DashMap,
    im::HashMap as ImHashMap,
    log::error,
    num_derive::ToPrimitive,
    num_traits::ToPrimitive,
    rayon::{prelude::*, ThreadPool},
    solana_accounts_db::stake_rewards::StakeReward,
    solana_sdk::{
        account::{AccountSharedData, ReadableAccount},
        clock::{Epoch, Slot},
        pubkey::Pubkey,
        stake::state::{Delegation, StakeActivationStatus},
        vote::state::VoteStateVersions,
    },
    solana_stake_program::stake_state::Stake,
    solana_vote::vote_account::{VoteAccount, VoteAccounts},
    std::{
        collections::HashMap,
        ops::Add,
        sync::{Arc, RwLock, RwLockReadGuard},
    },
    thiserror::Error,
};

mod serde_stakes;
pub(crate) use serde_stakes::serde_stakes_to_delegation_format;
pub use serde_stakes::SerdeStakesToStakeFormat;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid delegation: {0}")]
    InvalidDelegation(Pubkey),
    #[error(transparent)]
    InvalidStakeAccount(#[from] stake_account::Error),
    #[error("Stake account not found: {0}")]
    StakeAccountNotFound(Pubkey),
    #[error("Vote account mismatch: {0}")]
    VoteAccountMismatch(Pubkey),
    #[error("Vote account not cached: {0}")]
    VoteAccountNotCached(Pubkey),
    #[error("Vote account not found: {0}")]
    VoteAccountNotFound(Pubkey),
}

#[derive(Debug, Clone, PartialEq, Eq, ToPrimitive)]
pub enum InvalidCacheEntryReason {
    Missing,
    BadState,
    WrongOwner,
}

pub type StakeAccount = stake_account::StakeAccount<Delegation>;

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Default, Debug)]
pub struct StakesCache(RwLock<Stakes<StakeAccount>>);

impl StakesCache {
    pub(crate) fn new(stakes: Stakes<StakeAccount>) -> Self {
        Self(RwLock::new(stakes))
    }

    pub fn stakes(&self) -> RwLockReadGuard<Stakes<StakeAccount>> {
        self.0.read().unwrap()
    }

    pub(crate) fn check_and_store(
        &self,
        pubkey: &Pubkey,
        account: &impl ReadableAccount,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        // TODO: If the account is already cached as a vote or stake account
        // but the owner changes, then this needs to evict the account from
        // the cache. see:
        // https://github.com/solana-labs/solana/pull/24200#discussion_r849935444
        let owner = account.owner();
        // Zero lamport accounts are not stored in accounts-db
        // and so should be removed from cache as well.
        if account.lamports() == 0 {
            if solana_vote_program::check_id(owner) {
                let _old_vote_account = {
                    let mut stakes = self.0.write().unwrap();
                    stakes.remove_vote_account(pubkey)
                };
            } else if solana_stake_program::check_id(owner) {
                let mut stakes = self.0.write().unwrap();
                stakes.remove_stake_delegation(pubkey, new_rate_activation_epoch);
            }
            return;
        }
        debug_assert_ne!(account.lamports(), 0u64);
        if solana_vote_program::check_id(owner) {
            if VoteStateVersions::is_correct_size_and_initialized(account.data()) {
                match VoteAccount::try_from(account.to_account_shared_data()) {
                    Ok(vote_account) => {
                        // drop the old account after releasing the lock
                        let _old_vote_account = {
                            let mut stakes = self.0.write().unwrap();
                            stakes.upsert_vote_account(
                                pubkey,
                                vote_account,
                                new_rate_activation_epoch,
                            )
                        };
                    }
                    Err(_) => {
                        // drop the old account after releasing the lock
                        let _old_vote_account = {
                            let mut stakes = self.0.write().unwrap();
                            stakes.remove_vote_account(pubkey)
                        };
                    }
                }
            } else {
                // drop the old account after releasing the lock
                let _old_vote_account = {
                    let mut stakes = self.0.write().unwrap();
                    stakes.remove_vote_account(pubkey)
                };
            };
        } else if solana_stake_program::check_id(owner) {
            match StakeAccount::try_from(account.to_account_shared_data()) {
                Ok(stake_account) => {
                    let mut stakes = self.0.write().unwrap();
                    stakes.upsert_stake_delegation(
                        *pubkey,
                        stake_account,
                        new_rate_activation_epoch,
                    );
                }
                Err(_) => {
                    let mut stakes = self.0.write().unwrap();
                    stakes.remove_stake_delegation(pubkey, new_rate_activation_epoch);
                }
            }
        }
    }

    pub(crate) fn activate_epoch(
        &self,
        next_epoch: Epoch,
        thread_pool: &ThreadPool,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        let mut stakes = self.0.write().unwrap();
        stakes.activate_epoch(next_epoch, thread_pool, new_rate_activation_epoch)
    }

    pub(crate) fn update_stake_accounts(
        &self,
        thread_pool: &ThreadPool,
        stake_rewards: &[StakeReward],
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        self.0.write().unwrap().update_stake_accounts(
            thread_pool,
            stake_rewards,
            new_rate_activation_epoch,
        )
    }

    pub(crate) fn handle_invalid_keys(
        &self,
        invalid_vote_keys: DashMap<Pubkey, InvalidCacheEntryReason>,
        current_slot: Slot,
    ) {
        if invalid_vote_keys.is_empty() {
            return;
        }

        // Prune invalid stake delegations and vote accounts that were
        // not properly evicted in normal operation.
        let mut stakes = self.0.write().unwrap();

        for (vote_pubkey, reason) in invalid_vote_keys {
            stakes.remove_vote_account(&vote_pubkey);
            datapoint_warn!(
                "bank-stake_delegation_accounts-invalid-account",
                ("slot", current_slot as i64, i64),
                ("vote-address", format!("{vote_pubkey:?}"), String),
                ("reason", reason.to_i64().unwrap_or_default(), i64),
            );
        }
    }
}

/// The generic type T is either Delegation or StakeAccount.
/// [`Stakes<Delegation>`] is equivalent to the old code and is used for backward
/// compatibility in [`crate::bank::BankFieldsToDeserialize`].
/// But banks cache [`Stakes<StakeAccount>`] which includes the entire stake
/// account and StakeStateV2 deserialized from the account. Doing so, will remove
/// the need to load the stake account from accounts-db when working with
/// stake-delegations.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Default, Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct Stakes<T: Clone> {
    /// vote accounts
    vote_accounts: VoteAccounts,

    /// stake_delegations
    pub stake_delegations: ImHashMap<Pubkey, T>,

    /// unused
    unused: u64,

    /// current epoch, used to calculate current stake
    epoch: Epoch,

    /// history of staking levels
    stake_history: StakeHistory,
}

// For backward compatibility, we can only serialize and deserialize
// Stakes<Delegation> in the old `epoch_stakes` bank snapshot field. However,
// Stakes<StakeAccount> entries are added to the bank's epoch stakes hashmap
// when crossing epoch boundaries and Stakes<Stake> entries are added when
// starting up from bank snapshots that have the new epoch stakes field. By
// using this enum, the cost of converting all entries to Stakes<Delegation> is
// put off until serializing new snapshots. This helps avoid bogging down epoch
// boundaries and startup with the conversion overhead.
#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug, Clone)]
pub enum StakesEnum {
    Accounts(Stakes<StakeAccount>),
    Delegations(Stakes<Delegation>),
    Stakes(Stakes<Stake>),
}

impl<T: Clone> Stakes<T> {
    pub fn vote_accounts(&self) -> &VoteAccounts {
        &self.vote_accounts
    }

    pub(crate) fn staked_nodes(&self) -> Arc<HashMap<Pubkey, u64>> {
        self.vote_accounts.staked_nodes()
    }
}

impl Stakes<StakeAccount> {
    /// Creates a Stake<StakeAccount> from Stake<Delegation> by loading the
    /// full account state for respective stake pubkeys. get_account function
    /// should return the account at the respective slot where stakes where
    /// cached.
    pub fn new<F>(stakes: &Stakes<Delegation>, get_account: F) -> Result<Self, Error>
    where
        F: Fn(&Pubkey) -> Option<AccountSharedData> + Sync,
    {
        let stake_delegations = stakes
            .stake_delegations
            .iter()
            // im::HashMap doesn't support rayon so we manually build a temporary vector. Note this is
            // what std HashMap::par_iter() does internally too.
            .collect::<Vec<_>>()
            .into_par_iter()
            // We use fold/reduce to aggregate the results, which does a bit more work than calling
            // collect()/collect_vec_list() and then im::HashMap::from_iter(collected.into_iter()),
            // but it does it in background threads, so effectively it's faster.
            .try_fold(ImHashMap::new, |mut map, (pubkey, delegation)| {
                let Some(stake_account) = get_account(pubkey) else {
                    return Err(Error::StakeAccountNotFound(*pubkey));
                };

                // Assert that all valid vote-accounts referenced in stake delegations are already
                // contained in `stakes.vote_account`.
                let voter_pubkey = &delegation.voter_pubkey;
                if stakes.vote_accounts.get(voter_pubkey).is_none() {
                    if let Some(account) = get_account(voter_pubkey) {
                        if VoteStateVersions::is_correct_size_and_initialized(account.data())
                            && VoteAccount::try_from(account.clone()).is_ok()
                        {
                            error!("vote account not cached: {voter_pubkey}, {account:?}");
                            return Err(Error::VoteAccountNotCached(*voter_pubkey));
                        }
                    }
                }

                let stake_account = StakeAccount::try_from(stake_account)?;
                // Sanity check that the delegation is consistent with what is
                // stored in the account.
                if stake_account.delegation() == delegation {
                    map.insert(*pubkey, stake_account);
                    Ok(map)
                } else {
                    Err(Error::InvalidDelegation(*pubkey))
                }
            })
            .try_reduce(ImHashMap::new, |a, b| Ok(a.union(b)))?;

        // Assert that cached vote accounts are consistent with accounts-db.
        //
        // This currently includes ~5500 accounts, parallelizing brings minor
        // (sub 2s) improvements.
        for (pubkey, vote_account) in stakes.vote_accounts.iter() {
            let Some(account) = get_account(pubkey) else {
                return Err(Error::VoteAccountNotFound(*pubkey));
            };
            let vote_account = vote_account.account();
            if vote_account != &account {
                error!("vote account mismatch: {pubkey}, {vote_account:?}, {account:?}");
                return Err(Error::VoteAccountMismatch(*pubkey));
            }
        }

        Ok(Self {
            vote_accounts: stakes.vote_accounts.clone(),
            stake_delegations,
            unused: stakes.unused,
            epoch: stakes.epoch,
            stake_history: stakes.stake_history.clone(),
        })
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn new_for_tests(
        epoch: Epoch,
        vote_accounts: VoteAccounts,
        stake_delegations: ImHashMap<Pubkey, StakeAccount>,
    ) -> Self {
        Self {
            vote_accounts,
            stake_delegations,
            unused: 0,
            epoch,
            stake_history: StakeHistory::default(),
        }
    }

    pub(crate) fn history(&self) -> &StakeHistory {
        &self.stake_history
    }

    fn activate_epoch(
        &mut self,
        next_epoch: Epoch,
        thread_pool: &ThreadPool,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        let stake_delegations: Vec<_> = self.stake_delegations.values().collect();
        // Wrap up the prev epoch by adding new stake history entry for the
        // prev epoch.
        let stake_history_entry = thread_pool.install(|| {
            stake_delegations
                .par_iter()
                .fold(StakeActivationStatus::default, |acc, stake_account| {
                    let delegation = stake_account.delegation();
                    acc + delegation.stake_activating_and_deactivating(
                        self.epoch,
                        &self.stake_history,
                        new_rate_activation_epoch,
                    )
                })
                .reduce(StakeActivationStatus::default, Add::add)
        });
        self.stake_history.add(self.epoch, stake_history_entry);
        self.epoch = next_epoch;
        // Refresh the stake distribution of vote accounts for the next epoch,
        // using new stake history.
        self.vote_accounts = refresh_vote_accounts(
            thread_pool,
            self.epoch,
            &self.vote_accounts,
            &stake_delegations,
            &self.stake_history,
            new_rate_activation_epoch,
        );
    }

    /// Sum the stakes that point to the given voter_pubkey
    fn calculate_stake(
        stake_delegations: &ImHashMap<Pubkey, StakeAccount>,
        voter_pubkey: &Pubkey,
        epoch: Epoch,
        stake_history: &StakeHistory,
        new_rate_activation_epoch: Option<Epoch>,
    ) -> u64 {
        stake_delegations
            .values()
            .map(StakeAccount::delegation)
            .filter(|delegation| &delegation.voter_pubkey == voter_pubkey)
            .map(|delegation| delegation.stake(epoch, stake_history, new_rate_activation_epoch))
            .sum()
    }

    /// Sum the lamports of the vote accounts and the delegated stake
    pub(crate) fn vote_balance_and_staked(&self) -> u64 {
        let get_stake = |stake_account: &StakeAccount| stake_account.delegation().stake;
        let get_lamports = |(_, vote_account): (_, &VoteAccount)| vote_account.lamports();

        self.stake_delegations.values().map(get_stake).sum::<u64>()
            + self.vote_accounts.iter().map(get_lamports).sum::<u64>()
    }

    fn remove_vote_account(&mut self, vote_pubkey: &Pubkey) -> Option<VoteAccount> {
        self.vote_accounts.remove(vote_pubkey).map(|(_, a)| a)
    }

    fn remove_stake_delegation(
        &mut self,
        stake_pubkey: &Pubkey,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        if let Some(stake_account) = self.stake_delegations.remove(stake_pubkey) {
            let removed_delegation = stake_account.delegation();
            let removed_stake = removed_delegation.stake(
                self.epoch,
                &self.stake_history,
                new_rate_activation_epoch,
            );
            self.vote_accounts
                .sub_stake(&removed_delegation.voter_pubkey, removed_stake);
        }
    }

    fn upsert_vote_account(
        &mut self,
        vote_pubkey: &Pubkey,
        vote_account: VoteAccount,
        new_rate_activation_epoch: Option<Epoch>,
    ) -> Option<VoteAccount> {
        debug_assert_ne!(vote_account.lamports(), 0u64);

        let stake_delegations = &self.stake_delegations;
        self.vote_accounts.insert(*vote_pubkey, vote_account, || {
            Self::calculate_stake(
                stake_delegations,
                vote_pubkey,
                self.epoch,
                &self.stake_history,
                new_rate_activation_epoch,
            )
        })
    }

    fn upsert_stake_delegation(
        &mut self,
        stake_pubkey: Pubkey,
        stake_account: StakeAccount,
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        debug_assert_ne!(stake_account.lamports(), 0u64);
        let delegation = stake_account.delegation();
        let voter_pubkey = delegation.voter_pubkey;
        let stake = delegation.stake(self.epoch, &self.stake_history, new_rate_activation_epoch);
        match self.stake_delegations.insert(stake_pubkey, stake_account) {
            None => self.vote_accounts.add_stake(&voter_pubkey, stake),
            Some(old_stake_account) => {
                let old_delegation = old_stake_account.delegation();
                let old_voter_pubkey = old_delegation.voter_pubkey;
                let old_stake = old_delegation.stake(
                    self.epoch,
                    &self.stake_history,
                    new_rate_activation_epoch,
                );
                if voter_pubkey != old_voter_pubkey || stake != old_stake {
                    self.vote_accounts.sub_stake(&old_voter_pubkey, old_stake);
                    self.vote_accounts.add_stake(&voter_pubkey, stake);
                }
            }
        }
    }

    fn update_stake_accounts(
        &mut self,
        thread_pool: &ThreadPool,
        stake_rewards: &[StakeReward],
        new_rate_activation_epoch: Option<Epoch>,
    ) {
        let stake_delegations: Vec<_> = thread_pool.install(|| {
            stake_rewards
                .into_par_iter()
                .filter_map(|stake_reward| {
                    let stake_account = StakeAccount::try_from(stake_reward.stake_account.clone());
                    Some((stake_reward.stake_pubkey, stake_account.ok()?))
                })
                .collect()
        });
        self.stake_delegations = std::mem::take(&mut self.stake_delegations)
            .into_iter()
            .chain(stake_delegations)
            .collect::<HashMap<Pubkey, StakeAccount>>()
            .into_iter()
            .filter(|(_, account)| account.lamports() != 0u64)
            .collect();
        let stake_delegations: Vec<_> = self.stake_delegations.values().collect();
        self.vote_accounts = refresh_vote_accounts(
            thread_pool,
            self.epoch,
            &self.vote_accounts,
            &stake_delegations,
            &self.stake_history,
            new_rate_activation_epoch,
        );
    }

    pub fn stake_delegations(&self) -> &ImHashMap<Pubkey, StakeAccount> {
        &self.stake_delegations
    }

    pub(crate) fn highest_staked_node(&self) -> Option<&Pubkey> {
        let vote_account = self.vote_accounts.find_max_by_delegated_stake()?;
        Some(vote_account.node_pubkey())
    }
}

impl StakesEnum {
    pub fn vote_accounts(&self) -> &VoteAccounts {
        match self {
            StakesEnum::Accounts(stakes) => stakes.vote_accounts(),
            StakesEnum::Delegations(stakes) => stakes.vote_accounts(),
            StakesEnum::Stakes(stakes) => stakes.vote_accounts(),
        }
    }

    pub(crate) fn staked_nodes(&self) -> Arc<HashMap<Pubkey, u64>> {
        match self {
            StakesEnum::Accounts(stakes) => stakes.staked_nodes(),
            StakesEnum::Delegations(stakes) => stakes.staked_nodes(),
            StakesEnum::Stakes(stakes) => stakes.staked_nodes(),
        }
    }
}

/// This conversion is very memory intensive so should only be used in
/// development contexts.
#[cfg(feature = "dev-context-only-utils")]
impl From<Stakes<StakeAccount>> for Stakes<Delegation> {
    fn from(stakes: Stakes<StakeAccount>) -> Self {
        let stake_delegations = stakes
            .stake_delegations
            .into_iter()
            .map(|(pubkey, stake_account)| (pubkey, *stake_account.delegation()))
            .collect();
        Self {
            vote_accounts: stakes.vote_accounts,
            stake_delegations,
            unused: stakes.unused,
            epoch: stakes.epoch,
            stake_history: stakes.stake_history,
        }
    }
}

/// This conversion is very memory intensive so should only be used in
/// development contexts.
#[cfg(feature = "dev-context-only-utils")]
impl From<Stakes<StakeAccount>> for Stakes<Stake> {
    fn from(stakes: Stakes<StakeAccount>) -> Self {
        let stake_delegations = stakes
            .stake_delegations
            .into_iter()
            .map(|(pubkey, stake_account)| (pubkey, *stake_account.stake()))
            .collect();
        Self {
            vote_accounts: stakes.vote_accounts,
            stake_delegations,
            unused: stakes.unused,
            epoch: stakes.epoch,
            stake_history: stakes.stake_history,
        }
    }
}

/// This conversion is memory intensive so should only be used in development
/// contexts.
#[cfg(feature = "dev-context-only-utils")]
impl From<Stakes<Stake>> for Stakes<Delegation> {
    fn from(stakes: Stakes<Stake>) -> Self {
        let stake_delegations = stakes
            .stake_delegations
            .into_iter()
            .map(|(pubkey, stake)| (pubkey, stake.delegation))
            .collect();
        Self {
            vote_accounts: stakes.vote_accounts,
            stake_delegations,
            unused: stakes.unused,
            epoch: stakes.epoch,
            stake_history: stakes.stake_history,
        }
    }
}

/// This conversion is memory intensive so should only be used in development
/// contexts.
#[cfg(feature = "dev-context-only-utils")]
impl From<StakesEnum> for Stakes<Delegation> {
    fn from(stakes: StakesEnum) -> Self {
        match stakes {
            StakesEnum::Accounts(stakes) => stakes.into(),
            StakesEnum::Delegations(stakes) => stakes,
            StakesEnum::Stakes(stakes) => stakes.into(),
        }
    }
}

impl From<Stakes<StakeAccount>> for StakesEnum {
    fn from(stakes: Stakes<StakeAccount>) -> Self {
        Self::Accounts(stakes)
    }
}

impl From<Stakes<Delegation>> for StakesEnum {
    fn from(stakes: Stakes<Delegation>) -> Self {
        Self::Delegations(stakes)
    }
}

// Two StakesEnums are equal as long as they represent the same delegations;
// whether these delegations are stored as StakeAccounts or Delegations.
// Therefore, if one side is Stakes<StakeAccount> and the other is a
// Stakes<Delegation> we convert the former one to Stakes<Delegation> before
// comparing for equality.
#[cfg(feature = "dev-context-only-utils")]
impl PartialEq<StakesEnum> for StakesEnum {
    fn eq(&self, other: &StakesEnum) -> bool {
        match (self, other) {
            (Self::Accounts(stakes), Self::Accounts(other)) => stakes == other,
            (Self::Delegations(stakes), Self::Delegations(other)) => stakes == other,
            (Self::Stakes(stakes), Self::Stakes(other)) => stakes == other,
            (stakes, other) => {
                let stakes = Stakes::<Delegation>::from(stakes.clone());
                let other = Stakes::<Delegation>::from(other.clone());
                stakes == other
            }
        }
    }
}

fn refresh_vote_accounts(
    thread_pool: &ThreadPool,
    epoch: Epoch,
    vote_accounts: &VoteAccounts,
    stake_delegations: &[&StakeAccount],
    stake_history: &StakeHistory,
    new_rate_activation_epoch: Option<Epoch>,
) -> VoteAccounts {
    type StakesHashMap = HashMap</*voter:*/ Pubkey, /*stake:*/ u64>;
    fn merge(mut stakes: StakesHashMap, other: StakesHashMap) -> StakesHashMap {
        if stakes.len() < other.len() {
            return merge(other, stakes);
        }
        for (pubkey, stake) in other {
            *stakes.entry(pubkey).or_default() += stake;
        }
        stakes
    }
    let delegated_stakes = thread_pool.install(|| {
        stake_delegations
            .par_iter()
            .fold(HashMap::default, |mut delegated_stakes, stake_account| {
                let delegation = stake_account.delegation();
                let entry = delegated_stakes.entry(delegation.voter_pubkey).or_default();
                *entry += delegation.stake(epoch, stake_history, new_rate_activation_epoch);
                delegated_stakes
            })
            .reduce(HashMap::default, merge)
    });
    vote_accounts
        .iter()
        .map(|(&vote_pubkey, vote_account)| {
            let delegated_stake = delegated_stakes
                .get(&vote_pubkey)
                .copied()
                .unwrap_or_default();
            (vote_pubkey, (delegated_stake, vote_account.clone()))
        })
        .collect()
}

#[cfg(test)]
pub(crate) mod tests {
    use {
        super::*,
        rayon::ThreadPoolBuilder,
        solana_sdk::{account::WritableAccount, pubkey::Pubkey, rent::Rent, stake},
        solana_stake_program::stake_state,
        solana_vote_program::vote_state::{self, VoteState, VoteStateVersions},
    };

    //  set up some dummies for a staked node     ((     vote      )  (     stake     ))
    pub(crate) fn create_staked_node_accounts(
        stake: u64,
    ) -> ((Pubkey, AccountSharedData), (Pubkey, AccountSharedData)) {
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        let vote_account =
            vote_state::create_account(&vote_pubkey, &solana_sdk::pubkey::new_rand(), 0, 1);
        let stake_pubkey = solana_sdk::pubkey::new_rand();
        (
            (vote_pubkey, vote_account),
            (
                stake_pubkey,
                create_stake_account(stake, &vote_pubkey, &stake_pubkey),
            ),
        )
    }

    //   add stake to a vote_pubkey                               (   stake    )
    pub(crate) fn create_stake_account(
        stake: u64,
        vote_pubkey: &Pubkey,
        stake_pubkey: &Pubkey,
    ) -> AccountSharedData {
        stake_state::create_account(
            stake_pubkey,
            vote_pubkey,
            &vote_state::create_account(vote_pubkey, &solana_sdk::pubkey::new_rand(), 0, 1),
            &Rent::free(),
            stake,
        )
    }

    fn create_warming_staked_node_accounts(
        stake: u64,
        epoch: Epoch,
    ) -> ((Pubkey, AccountSharedData), (Pubkey, AccountSharedData)) {
        let vote_pubkey = solana_sdk::pubkey::new_rand();
        let vote_account =
            vote_state::create_account(&vote_pubkey, &solana_sdk::pubkey::new_rand(), 0, 1);
        (
            (vote_pubkey, vote_account),
            create_warming_stake_account(stake, epoch, &vote_pubkey),
        )
    }

    // add stake to a vote_pubkey                               (   stake    )
    fn create_warming_stake_account(
        stake: u64,
        epoch: Epoch,
        vote_pubkey: &Pubkey,
    ) -> (Pubkey, AccountSharedData) {
        let stake_pubkey = solana_sdk::pubkey::new_rand();
        (
            stake_pubkey,
            stake_state::create_account_with_activation_epoch(
                &stake_pubkey,
                vote_pubkey,
                &vote_state::create_account(vote_pubkey, &solana_sdk::pubkey::new_rand(), 0, 1),
                &Rent::free(),
                stake,
                epoch,
            ),
        )
    }

    #[test]
    fn test_stakes_basic() {
        for i in 0..4 {
            let stakes_cache = StakesCache::new(Stakes {
                epoch: i,
                ..Stakes::default()
            });

            let ((vote_pubkey, vote_account), (stake_pubkey, mut stake_account)) =
                create_staked_node_accounts(10);

            stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
            stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
            let stake = stake_state::stake_from(&stake_account).unwrap();
            {
                let stakes = stakes_cache.stakes();
                let vote_accounts = stakes.vote_accounts();
                assert!(vote_accounts.get(&vote_pubkey).is_some());
                assert_eq!(
                    vote_accounts.get_delegated_stake(&vote_pubkey),
                    stake.stake(i, &StakeHistory::default(), None)
                );
            }

            stake_account.set_lamports(42);
            stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
            {
                let stakes = stakes_cache.stakes();
                let vote_accounts = stakes.vote_accounts();
                assert!(vote_accounts.get(&vote_pubkey).is_some());
                assert_eq!(
                    vote_accounts.get_delegated_stake(&vote_pubkey),
                    stake.stake(i, &StakeHistory::default(), None)
                ); // stays old stake, because only 10 is activated
            }

            // activate more
            let mut stake_account =
                create_stake_account(42, &vote_pubkey, &solana_sdk::pubkey::new_rand());
            stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
            let stake = stake_state::stake_from(&stake_account).unwrap();
            {
                let stakes = stakes_cache.stakes();
                let vote_accounts = stakes.vote_accounts();
                assert!(vote_accounts.get(&vote_pubkey).is_some());
                assert_eq!(
                    vote_accounts.get_delegated_stake(&vote_pubkey),
                    stake.stake(i, &StakeHistory::default(), None)
                ); // now stake of 42 is activated
            }

            stake_account.set_lamports(0);
            stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
            {
                let stakes = stakes_cache.stakes();
                let vote_accounts = stakes.vote_accounts();
                assert!(vote_accounts.get(&vote_pubkey).is_some());
                assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
            }
        }
    }

    #[test]
    fn test_stakes_highest() {
        let stakes_cache = StakesCache::default();

        assert_eq!(stakes_cache.stakes().highest_staked_node(), None);

        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        let ((vote11_pubkey, vote11_account), (stake11_pubkey, stake11_account)) =
            create_staked_node_accounts(20);

        stakes_cache.check_and_store(&vote11_pubkey, &vote11_account, None);
        stakes_cache.check_and_store(&stake11_pubkey, &stake11_account, None);

        let vote11_node_pubkey = vote_state::from(&vote11_account).unwrap().node_pubkey;

        let highest_staked_node = stakes_cache.stakes().highest_staked_node().copied();
        assert_eq!(highest_staked_node, Some(vote11_node_pubkey));
    }

    #[test]
    fn test_stakes_vote_account_disappear_reappear() {
        let stakes_cache = StakesCache::new(Stakes {
            epoch: 4,
            ..Stakes::default()
        });

        let ((vote_pubkey, mut vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 10);
        }

        vote_account.set_lamports(0);
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_none());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
        }

        vote_account.set_lamports(1);
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 10);
        }

        // Vote account too big
        let cache_data = vote_account.data().to_vec();
        let mut pushed = vote_account.data().to_vec();
        pushed.push(0);
        vote_account.set_data(pushed);
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_none());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
        }

        // Vote account uninitialized
        let default_vote_state = VoteState::default();
        let versioned = VoteStateVersions::new_current(default_vote_state);
        vote_state::to(&versioned, &mut vote_account).unwrap();
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_none());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
        }

        vote_account.set_data(cache_data);
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 10);
        }
    }

    #[test]
    fn test_stakes_change_delegate() {
        let stakes_cache = StakesCache::new(Stakes {
            epoch: 4,
            ..Stakes::default()
        });

        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        let ((vote_pubkey2, vote_account2), (_stake_pubkey2, stake_account2)) =
            create_staked_node_accounts(10);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&vote_pubkey2, &vote_account2, None);

        // delegates to vote_pubkey
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        let stake = stake_state::stake_from(&stake_account).unwrap();

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(
                vote_accounts.get_delegated_stake(&vote_pubkey),
                stake.stake(stakes.epoch, &stakes.stake_history, None)
            );
            assert!(vote_accounts.get(&vote_pubkey2).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey2), 0);
        }

        // delegates to vote_pubkey2
        stakes_cache.check_and_store(&stake_pubkey, &stake_account2, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
            assert!(vote_accounts.get(&vote_pubkey2).is_some());
            assert_eq!(
                vote_accounts.get_delegated_stake(&vote_pubkey2),
                stake.stake(stakes.epoch, &stakes.stake_history, None)
            );
        }
    }
    #[test]
    fn test_stakes_multiple_stakers() {
        let stakes_cache = StakesCache::new(Stakes {
            epoch: 4,
            ..Stakes::default()
        });

        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        let stake_pubkey2 = solana_sdk::pubkey::new_rand();
        let stake_account2 = create_stake_account(10, &vote_pubkey, &stake_pubkey2);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);

        // delegates to vote_pubkey
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
        stakes_cache.check_and_store(&stake_pubkey2, &stake_account2, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 20);
        }
    }

    #[test]
    fn test_activate_epoch() {
        let stakes_cache = StakesCache::default();

        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);
        let stake = stake_state::stake_from(&stake_account).unwrap();

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert_eq!(
                vote_accounts.get_delegated_stake(&vote_pubkey),
                stake.stake(stakes.epoch, &stakes.stake_history, None)
            );
        }
        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        stakes_cache.activate_epoch(3, &thread_pool, None);
        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert_eq!(
                vote_accounts.get_delegated_stake(&vote_pubkey),
                stake.stake(stakes.epoch, &stakes.stake_history, None)
            );
        }
    }

    #[test]
    fn test_stakes_not_delegate() {
        let stakes_cache = StakesCache::new(Stakes {
            epoch: 4,
            ..Stakes::default()
        });

        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_staked_node_accounts(10);

        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 10);
        }

        // not a stake account, and whacks above entry
        stakes_cache.check_and_store(
            &stake_pubkey,
            &AccountSharedData::new(1, 0, &stake::program::id()),
            None,
        );
        {
            let stakes = stakes_cache.stakes();
            let vote_accounts = stakes.vote_accounts();
            assert!(vote_accounts.get(&vote_pubkey).is_some());
            assert_eq!(vote_accounts.get_delegated_stake(&vote_pubkey), 0);
        }
    }

    #[test]
    fn test_vote_balance_and_staked_empty() {
        let stakes = Stakes::<StakeAccount>::default();
        assert_eq!(stakes.vote_balance_and_staked(), 0);
    }

    #[test]
    fn test_vote_balance_and_staked_normal() {
        let stakes_cache = StakesCache::default();
        #[allow(non_local_definitions)]
        impl Stakes<StakeAccount> {
            fn vote_balance_and_warmed_staked(&self) -> u64 {
                let vote_balance: u64 = self
                    .vote_accounts
                    .iter()
                    .map(|(_pubkey, account)| account.lamports())
                    .sum();
                let warmed_stake: u64 = self
                    .vote_accounts
                    .delegated_stakes()
                    .map(|(_pubkey, stake)| stake)
                    .sum();
                vote_balance + warmed_stake
            }
        }

        let genesis_epoch = 0;
        let ((vote_pubkey, vote_account), (stake_pubkey, stake_account)) =
            create_warming_staked_node_accounts(10, genesis_epoch);
        stakes_cache.check_and_store(&vote_pubkey, &vote_account, None);
        stakes_cache.check_and_store(&stake_pubkey, &stake_account, None);

        {
            let stakes = stakes_cache.stakes();
            assert_eq!(stakes.vote_balance_and_staked(), 11);
            assert_eq!(stakes.vote_balance_and_warmed_staked(), 1);
        }

        let thread_pool = ThreadPoolBuilder::new().num_threads(1).build().unwrap();
        for (epoch, expected_warmed_stake) in ((genesis_epoch + 1)..=3).zip(&[2, 3, 4]) {
            stakes_cache.activate_epoch(epoch, &thread_pool, None);
            // vote_balance_and_staked() always remain to return same lamports
            // while vote_balance_and_warmed_staked() gradually increases
            let stakes = stakes_cache.stakes();
            assert_eq!(stakes.vote_balance_and_staked(), 11);
            assert_eq!(
                stakes.vote_balance_and_warmed_staked(),
                *expected_warmed_stake
            );
        }
    }
}
