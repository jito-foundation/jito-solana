//! `cost_tracker` keeps tracking transaction cost per chained accounts as well as for entire block
//! The main functions are:
//! - would_fit(&tx_cost), immutable function to test if tx with tx_cost would fit into current block
//! - add_transaction_cost(&tx_cost), mutable function to accumulate tx_cost to tracker.
//!
use {
    crate::{block_cost_limits::*, transaction_cost::TransactionCost},
    solana_metrics::datapoint_info,
    solana_pubkey::Pubkey,
    solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
    solana_transaction_error::TransactionError,
    std::{cmp::Ordering, collections::HashMap, num::Saturating},
};

const WRITABLE_ACCOUNTS_PER_BLOCK: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostTrackerError {
    /// would exceed block max limit
    WouldExceedBlockMaxLimit,

    /// would exceed vote max limit
    WouldExceedVoteMaxLimit,

    /// would exceed account max limit
    WouldExceedAccountMaxLimit,

    /// would exceed account data block limit
    WouldExceedAccountDataBlockLimit,

    /// would exceed account data total limit
    WouldExceedAccountDataTotalLimit,
}

impl From<CostTrackerError> for TransactionError {
    fn from(err: CostTrackerError) -> Self {
        match err {
            CostTrackerError::WouldExceedBlockMaxLimit => Self::WouldExceedMaxBlockCostLimit,
            CostTrackerError::WouldExceedVoteMaxLimit => Self::WouldExceedMaxVoteCostLimit,
            CostTrackerError::WouldExceedAccountMaxLimit => Self::WouldExceedMaxAccountCostLimit,
            CostTrackerError::WouldExceedAccountDataBlockLimit => {
                Self::WouldExceedAccountDataBlockLimit
            }
            CostTrackerError::WouldExceedAccountDataTotalLimit => {
                Self::WouldExceedAccountDataTotalLimit
            }
        }
    }
}

/// Relevant block costs that were updated after successful `try_add()`
#[derive(Debug, Default)]
pub struct UpdatedCosts {
    pub updated_block_cost: u64,
    // for all write-locked accounts `try_add()` successfully updated, the highest account cost
    // can be useful info.
    pub updated_costliest_account_cost: u64,
}

#[cfg_attr(feature = "frozen-abi", derive(AbiExample))]
#[derive(Debug)]
pub struct CostTracker {
    account_cost_limit: u64,
    block_cost_limit: u64,
    vote_cost_limit: u64,
    cost_by_writable_accounts: HashMap<Pubkey, u64, ahash::RandomState>,
    block_cost: u64,
    vote_cost: u64,
    transaction_count: Saturating<u64>,
    allocated_accounts_data_size: Saturating<u64>,
    transaction_signature_count: Saturating<u64>,
    secp256k1_instruction_signature_count: Saturating<u64>,
    ed25519_instruction_signature_count: Saturating<u64>,
    /// The number of transactions that have had their estimated cost added to
    /// the tracker, but are still waiting for an update with actual usage or
    /// removal if the transaction does not end up getting committed.
    in_flight_transaction_count: Saturating<usize>,
    secp256r1_instruction_signature_count: Saturating<u64>,
}

impl Default for CostTracker {
    fn default() -> Self {
        // Clippy doesn't like asserts in const contexts, so need to explicitly allow them.  For
        // more info, see this issue: https://github.com/rust-lang/rust-clippy/issues/8159
        #![allow(clippy::assertions_on_constants)]
        const _: () = assert!(MAX_WRITABLE_ACCOUNT_UNITS <= MAX_BLOCK_UNITS);
        const _: () = assert!(MAX_VOTE_UNITS <= MAX_BLOCK_UNITS);

        Self {
            account_cost_limit: MAX_WRITABLE_ACCOUNT_UNITS,
            block_cost_limit: MAX_BLOCK_UNITS,
            vote_cost_limit: MAX_VOTE_UNITS,
            cost_by_writable_accounts: HashMap::with_capacity_and_hasher(
                WRITABLE_ACCOUNTS_PER_BLOCK,
                ahash::RandomState::new(),
            ),
            block_cost: 0,
            vote_cost: 0,
            transaction_count: Saturating(0),
            allocated_accounts_data_size: Saturating(0),
            transaction_signature_count: Saturating(0),
            secp256k1_instruction_signature_count: Saturating(0),
            ed25519_instruction_signature_count: Saturating(0),
            in_flight_transaction_count: Saturating(0),
            secp256r1_instruction_signature_count: Saturating(0),
        }
    }
}

impl CostTracker {
    pub fn new_from_parent_limits(&self) -> Self {
        let mut new = Self::default();
        new.set_limits(
            self.account_cost_limit,
            self.block_cost_limit,
            self.vote_cost_limit,
        );
        new
    }

    pub fn reset(&mut self) {
        self.cost_by_writable_accounts.clear();
        self.block_cost = 0;
        self.vote_cost = 0;
        self.transaction_count = Saturating(0);
        self.allocated_accounts_data_size = Saturating(0);
        self.transaction_signature_count = Saturating(0);
        self.secp256k1_instruction_signature_count = Saturating(0);
        self.ed25519_instruction_signature_count = Saturating(0);
        self.in_flight_transaction_count = Saturating(0);
    }

    /// Get the overall block limit.
    pub fn get_block_limit(&self) -> u64 {
        self.block_cost_limit
    }

    /// allows to adjust limits initiated during construction
    pub fn set_limits(
        &mut self,
        account_cost_limit: u64,
        block_cost_limit: u64,
        vote_cost_limit: u64,
    ) {
        self.account_cost_limit = account_cost_limit;
        self.block_cost_limit = block_cost_limit;
        self.vote_cost_limit = vote_cost_limit;
    }

    pub fn set_block_cost_limit(&mut self, block_cost_limit: u64) {
        self.block_cost_limit = block_cost_limit;
    }

    pub fn in_flight_transaction_count(&self) -> usize {
        self.in_flight_transaction_count.0
    }

    pub fn add_transactions_in_flight(&mut self, in_flight_transaction_count: usize) {
        self.in_flight_transaction_count += in_flight_transaction_count;
    }

    pub fn sub_transactions_in_flight(&mut self, in_flight_transaction_count: usize) {
        self.in_flight_transaction_count -= in_flight_transaction_count
    }

    pub fn try_add(
        &mut self,
        tx_cost: &TransactionCost<impl TransactionWithMeta>,
        block_cost_limit_reservation: u64,
    ) -> Result<UpdatedCosts, CostTrackerError> {
        self.would_fit(tx_cost, block_cost_limit_reservation)?;
        let updated_costliest_account_cost = self.add_transaction_cost(tx_cost);
        Ok(UpdatedCosts {
            updated_block_cost: self.block_cost,
            updated_costliest_account_cost,
        })
    }

    pub fn update_execution_cost(
        &mut self,
        estimated_tx_cost: &TransactionCost<impl TransactionWithMeta>,
        actual_execution_units: u64,
        actual_loaded_accounts_data_size_cost: u64,
    ) {
        let actual_load_and_execution_units =
            actual_execution_units.saturating_add(actual_loaded_accounts_data_size_cost);
        let estimated_load_and_execution_units = estimated_tx_cost
            .programs_execution_cost()
            .saturating_add(estimated_tx_cost.loaded_accounts_data_size_cost());
        match actual_load_and_execution_units.cmp(&estimated_load_and_execution_units) {
            Ordering::Equal => (),
            Ordering::Greater => {
                self.add_transaction_execution_cost(
                    estimated_tx_cost,
                    actual_load_and_execution_units - estimated_load_and_execution_units,
                );
            }
            Ordering::Less => {
                self.sub_transaction_execution_cost(
                    estimated_tx_cost,
                    estimated_load_and_execution_units - actual_load_and_execution_units,
                );
            }
        }
    }

    pub fn remove(&mut self, tx_cost: &TransactionCost<impl TransactionWithMeta>) {
        self.remove_transaction_cost(tx_cost);
    }

    pub fn block_cost(&self) -> u64 {
        self.block_cost
    }

    pub fn vote_cost(&self) -> u64 {
        self.vote_cost
    }

    pub fn block_cost_limit(&self) -> u64 {
        self.block_cost_limit
    }

    pub fn transaction_count(&self) -> u64 {
        self.transaction_count.0
    }

    pub fn report_stats(&self, bank_slot: solana_clock::Slot) {
        // skip reporting if block is empty
        if self.transaction_count.0 == 0 {
            return;
        }

        let (costliest_account, costliest_account_cost) = self.find_costliest_account();

        datapoint_info!(
            "cost_tracker_stats",
            ("bank_slot", bank_slot as i64, i64),
            ("block_cost", self.block_cost as i64, i64),
            ("vote_cost", self.vote_cost as i64, i64),
            ("transaction_count", self.transaction_count.0 as i64, i64),
            ("number_of_accounts", self.number_of_accounts() as i64, i64),
            ("costliest_account", costliest_account.to_string(), String),
            ("costliest_account_cost", costliest_account_cost as i64, i64),
            (
                "allocated_accounts_data_size",
                self.allocated_accounts_data_size.0,
                i64
            ),
            (
                "transaction_signature_count",
                self.transaction_signature_count.0,
                i64
            ),
            (
                "secp256k1_instruction_signature_count",
                self.secp256k1_instruction_signature_count.0,
                i64
            ),
            (
                "ed25519_instruction_signature_count",
                self.ed25519_instruction_signature_count.0,
                i64
            ),
            (
                "inflight_transaction_count",
                self.in_flight_transaction_count.0,
                i64
            ),
            (
                "secp256r1_instruction_signature_count",
                self.secp256r1_instruction_signature_count.0,
                i64
            )
        );
    }

    fn find_costliest_account(&self) -> (Pubkey, u64) {
        self.cost_by_writable_accounts
            .iter()
            .max_by_key(|(_, &cost)| cost)
            .map(|(&pubkey, &cost)| (pubkey, cost))
            .unwrap_or_default()
    }

    fn would_fit(
        &self,
        tx_cost: &TransactionCost<impl TransactionWithMeta>,
        block_cost_limit_reservation: u64,
    ) -> Result<(), CostTrackerError> {
        let cost: u64 = tx_cost.sum();

        if tx_cost.is_simple_vote() {
            // if vote transaction, check if it exceeds vote_transaction_limit
            if self.vote_cost.saturating_add(cost) > self.vote_cost_limit {
                return Err(CostTrackerError::WouldExceedVoteMaxLimit);
            }
        }

        if self.block_cost.saturating_add(cost)
            > self
                .block_cost_limit
                .saturating_sub(block_cost_limit_reservation)
        {
            // check against the total package cost
            return Err(CostTrackerError::WouldExceedBlockMaxLimit);
        }

        // check if the transaction itself is more costly than the account_cost_limit
        if cost > self.account_cost_limit {
            return Err(CostTrackerError::WouldExceedAccountMaxLimit);
        }

        let allocated_accounts_data_size =
            self.allocated_accounts_data_size + Saturating(tx_cost.allocated_accounts_data_size());

        if allocated_accounts_data_size.0 > MAX_BLOCK_ACCOUNTS_DATA_SIZE_DELTA {
            return Err(CostTrackerError::WouldExceedAccountDataBlockLimit);
        }

        // check each account against account_cost_limit,
        for account_key in tx_cost.writable_accounts() {
            match self.cost_by_writable_accounts.get(account_key) {
                Some(chained_cost) => {
                    if chained_cost.saturating_add(cost) > self.account_cost_limit {
                        return Err(CostTrackerError::WouldExceedAccountMaxLimit);
                    } else {
                        continue;
                    }
                }
                None => continue,
            }
        }

        Ok(())
    }

    // Returns the highest account cost for all write-lock accounts `TransactionCost` updated
    fn add_transaction_cost(&mut self, tx_cost: &TransactionCost<impl TransactionWithMeta>) -> u64 {
        self.allocated_accounts_data_size += tx_cost.allocated_accounts_data_size();
        self.transaction_count += 1;
        self.transaction_signature_count += tx_cost.num_transaction_signatures();
        self.secp256k1_instruction_signature_count +=
            tx_cost.num_secp256k1_instruction_signatures();
        self.ed25519_instruction_signature_count += tx_cost.num_ed25519_instruction_signatures();
        self.secp256r1_instruction_signature_count +=
            tx_cost.num_secp256r1_instruction_signatures();
        self.add_transaction_execution_cost(tx_cost, tx_cost.sum())
    }

    fn remove_transaction_cost(&mut self, tx_cost: &TransactionCost<impl TransactionWithMeta>) {
        let cost = tx_cost.sum();
        self.sub_transaction_execution_cost(tx_cost, cost);
        self.allocated_accounts_data_size -= tx_cost.allocated_accounts_data_size();
        self.transaction_count -= 1;
        self.transaction_signature_count -= tx_cost.num_transaction_signatures();
        self.secp256k1_instruction_signature_count -=
            tx_cost.num_secp256k1_instruction_signatures();
        self.ed25519_instruction_signature_count -= tx_cost.num_ed25519_instruction_signatures();
        self.secp256r1_instruction_signature_count -=
            tx_cost.num_secp256r1_instruction_signatures();
    }

    /// Apply additional actual execution units to cost_tracker
    /// Return the costliest account cost that were updated by `TransactionCost`
    fn add_transaction_execution_cost(
        &mut self,
        tx_cost: &TransactionCost<impl TransactionWithMeta>,
        adjustment: u64,
    ) -> u64 {
        let mut costliest_account_cost = 0;
        for account_key in tx_cost.writable_accounts() {
            let account_cost = self
                .cost_by_writable_accounts
                .entry(*account_key)
                .or_insert(0);
            *account_cost = account_cost.saturating_add(adjustment);
            costliest_account_cost = costliest_account_cost.max(*account_cost);
        }
        self.block_cost = self.block_cost.saturating_add(adjustment);
        if tx_cost.is_simple_vote() {
            self.vote_cost = self.vote_cost.saturating_add(adjustment);
        }

        costliest_account_cost
    }

    /// Subtract extra execution units from cost_tracker
    fn sub_transaction_execution_cost(
        &mut self,
        tx_cost: &TransactionCost<impl TransactionWithMeta>,
        adjustment: u64,
    ) {
        for account_key in tx_cost.writable_accounts() {
            let account_cost = self
                .cost_by_writable_accounts
                .entry(*account_key)
                .or_insert(0);
            *account_cost = account_cost.saturating_sub(adjustment);
        }
        self.block_cost = self.block_cost.saturating_sub(adjustment);
        if tx_cost.is_simple_vote() {
            self.vote_cost = self.vote_cost.saturating_sub(adjustment);
        }
    }

    /// count number of none-zero CU accounts
    fn number_of_accounts(&self) -> usize {
        self.cost_by_writable_accounts
            .values()
            .filter(|units| **units > 0)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::transaction_cost::{WritableKeysTransaction, *},
        solana_keypair::Keypair,
        solana_signer::Signer,
        std::cmp,
    };

    impl CostTracker {
        fn new(account_cost_limit: u64, block_cost_limit: u64, vote_cost_limit: u64) -> Self {
            assert!(account_cost_limit <= block_cost_limit);
            assert!(vote_cost_limit <= block_cost_limit);
            Self {
                account_cost_limit,
                block_cost_limit,
                vote_cost_limit,
                ..Self::default()
            }
        }
    }

    fn test_setup() -> Keypair {
        solana_logger::setup();
        Keypair::new()
    }

    fn build_simple_transaction(mint_keypair: &Keypair) -> WritableKeysTransaction {
        WritableKeysTransaction(vec![mint_keypair.pubkey()])
    }

    fn simple_usage_cost_details(
        transaction: &WritableKeysTransaction,
        programs_execution_cost: u64,
    ) -> UsageCostDetails<WritableKeysTransaction> {
        UsageCostDetails {
            transaction,
            signature_cost: 0,
            write_lock_cost: 0,
            data_bytes_cost: 0,
            programs_execution_cost,
            loaded_accounts_data_size_cost: 0,
            allocated_accounts_data_size: 0,
        }
    }

    fn simple_transaction_cost(
        transaction: &WritableKeysTransaction,
        programs_execution_cost: u64,
    ) -> TransactionCost<WritableKeysTransaction> {
        TransactionCost::Transaction(simple_usage_cost_details(
            transaction,
            programs_execution_cost,
        ))
    }

    fn simple_vote_transaction_cost(
        transaction: &WritableKeysTransaction,
    ) -> TransactionCost<WritableKeysTransaction> {
        TransactionCost::SimpleVote { transaction }
    }

    #[test]
    fn test_cost_tracker_initialization() {
        let testee = CostTracker::new(10, 11, 8);
        assert_eq!(10, testee.account_cost_limit);
        assert_eq!(11, testee.block_cost_limit);
        assert_eq!(8, testee.vote_cost_limit);
        assert_eq!(0, testee.cost_by_writable_accounts.len());
        assert_eq!(0, testee.block_cost);
    }

    #[test]
    fn test_cost_tracker_ok_add_one() {
        let mint_keypair = test_setup();
        let tx = build_simple_transaction(&mint_keypair);
        let tx_cost = simple_transaction_cost(&tx, 5);
        let cost = tx_cost.sum();

        // build testee to have capacity for one simple transaction
        let mut testee = CostTracker::new(cost, cost, cost);
        assert!(testee.would_fit(&tx_cost, 0).is_ok());
        testee.add_transaction_cost(&tx_cost);
        assert_eq!(cost, testee.block_cost);
        assert_eq!(0, testee.vote_cost);
        let (_costliest_account, costliest_account_cost) = testee.find_costliest_account();
        assert_eq!(cost, costliest_account_cost);
    }

    #[test]
    fn test_cost_tracker_ok_add_one_vote() {
        let mint_keypair = test_setup();
        let tx = build_simple_transaction(&mint_keypair);
        let tx_cost = simple_vote_transaction_cost(&tx);
        let cost = tx_cost.sum();

        // build testee to have capacity for one simple transaction
        let mut testee = CostTracker::new(cost, cost, cost);
        assert!(testee.would_fit(&tx_cost, 0).is_ok());
        testee.add_transaction_cost(&tx_cost);
        assert_eq!(cost, testee.block_cost);
        assert_eq!(cost, testee.vote_cost);
        let (_costliest_account, costliest_account_cost) = testee.find_costliest_account();
        assert_eq!(cost, costliest_account_cost);
    }

    #[test]
    fn test_cost_tracker_add_data() {
        let mint_keypair = test_setup();
        let tx = build_simple_transaction(&mint_keypair);
        let mut tx_cost = simple_transaction_cost(&tx, 5);
        if let TransactionCost::Transaction(ref mut usage_cost) = tx_cost {
            usage_cost.allocated_accounts_data_size = 1;
        } else {
            unreachable!();
        }
        let cost = tx_cost.sum();

        // build testee to have capacity for one simple transaction
        let mut testee = CostTracker::new(cost, cost, cost);
        assert!(testee.would_fit(&tx_cost, 0).is_ok());
        let old = testee.allocated_accounts_data_size;
        testee.add_transaction_cost(&tx_cost);
        assert_eq!(old.0 + 1, testee.allocated_accounts_data_size.0);
    }

    #[test]
    fn test_cost_tracker_ok_add_two_same_accounts() {
        let mint_keypair = test_setup();
        // build two transactions with same signed account
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_transaction_cost(&tx1, 5);
        let cost1 = tx_cost1.sum();
        let tx2 = build_simple_transaction(&mint_keypair);
        let tx_cost2 = simple_transaction_cost(&tx2, 5);
        let cost2 = tx_cost2.sum();

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostTracker::new(cost1 + cost2, cost1 + cost2, cost1 + cost2);
        {
            assert!(testee.would_fit(&tx_cost1, 0).is_ok());
            testee.add_transaction_cost(&tx_cost1);
        }
        {
            assert!(testee.would_fit(&tx_cost2, 0).is_ok());
            testee.add_transaction_cost(&tx_cost2);
        }
        assert_eq!(cost1 + cost2, testee.block_cost);
        assert_eq!(1, testee.cost_by_writable_accounts.len());
        let (_ccostliest_account, costliest_account_cost) = testee.find_costliest_account();
        assert_eq!(cost1 + cost2, costliest_account_cost);
    }

    #[test]
    fn test_cost_tracker_ok_add_two_diff_accounts() {
        let mint_keypair = test_setup();
        // build two transactions with diff accounts
        let second_account = Keypair::new();
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_transaction_cost(&tx1, 5);
        let cost1 = tx_cost1.sum();

        let tx2 = build_simple_transaction(&second_account);
        let tx_cost2 = simple_transaction_cost(&tx2, 5);
        let cost2 = tx_cost2.sum();

        // build testee to have capacity for two simple transactions, with same accounts
        let mut testee = CostTracker::new(cmp::max(cost1, cost2), cost1 + cost2, cost1 + cost2);
        {
            assert!(testee.would_fit(&tx_cost1, 0).is_ok());
            testee.add_transaction_cost(&tx_cost1);
        }
        {
            assert!(testee.would_fit(&tx_cost2, 0).is_ok());
            testee.add_transaction_cost(&tx_cost2);
        }
        assert_eq!(cost1 + cost2, testee.block_cost);
        assert_eq!(2, testee.cost_by_writable_accounts.len());
        let (_ccostliest_account, costliest_account_cost) = testee.find_costliest_account();
        assert_eq!(std::cmp::max(cost1, cost2), costliest_account_cost);
    }

    #[test]
    fn test_cost_tracker_chain_reach_limit() {
        let mint_keypair = test_setup();
        // build two transactions with same signed account
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_transaction_cost(&tx1, 5);
        let cost1 = tx_cost1.sum();
        let tx2 = build_simple_transaction(&mint_keypair);
        let tx_cost2 = simple_transaction_cost(&tx2, 5);
        let cost2 = tx_cost2.sum();

        // build testee to have capacity for two simple transactions, but not for same accounts
        let mut testee = CostTracker::new(cmp::min(cost1, cost2), cost1 + cost2, cost1 + cost2);
        // should have room for first transaction
        {
            assert!(testee.would_fit(&tx_cost1, 0).is_ok());
            testee.add_transaction_cost(&tx_cost1);
        }
        // but no more sapce on the same chain (same signer account)
        {
            assert!(testee.would_fit(&tx_cost2, 0).is_err());
        }
    }

    #[test]
    fn test_cost_tracker_reach_limit() {
        let mint_keypair = test_setup();
        // build two transactions with diff accounts
        let second_account = Keypair::new();
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_transaction_cost(&tx1, 5);
        let cost1 = tx_cost1.sum();
        let tx2 = build_simple_transaction(&second_account);
        let tx_cost2 = simple_transaction_cost(&tx2, 5);
        let cost2 = tx_cost2.sum();

        // build testee to have capacity for each chain, but not enough room for both transactions
        let mut testee =
            CostTracker::new(cmp::max(cost1, cost2), cost1 + cost2 - 1, cost1 + cost2 - 1);
        // should have room for first transaction
        {
            assert!(testee.would_fit(&tx_cost1, 0).is_ok());
            testee.add_transaction_cost(&tx_cost1);
        }
        // but no more room for package as whole
        {
            assert!(testee.would_fit(&tx_cost2, 0).is_err());
        }
    }

    #[test]
    fn test_cost_tracker_reach_vote_limit() {
        let mint_keypair = test_setup();
        // build two mocking vote transactions with diff accounts
        let second_account = Keypair::new();
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_vote_transaction_cost(&tx1);
        let cost1 = tx_cost1.sum();
        let tx2 = build_simple_transaction(&second_account);
        let tx_cost2 = simple_vote_transaction_cost(&tx2);
        let cost2 = tx_cost2.sum();

        // build testee to have capacity for each chain, but not enough room for both votes
        let mut testee = CostTracker::new(cmp::max(cost1, cost2), cost1 + cost2, cost1 + cost2 - 1);
        // should have room for first vote
        {
            assert!(testee.would_fit(&tx_cost1, 0).is_ok());
            testee.add_transaction_cost(&tx_cost1);
        }
        // but no more room for package as whole
        {
            assert!(testee.would_fit(&tx_cost2, 0).is_err());
        }
        // however there is room for none-vote tx3
        {
            let third_account = Keypair::new();
            let tx3 = build_simple_transaction(&third_account);
            let tx_cost3 = simple_transaction_cost(&tx3, 5);
            assert!(testee.would_fit(&tx_cost3, 0).is_ok());
        }
    }

    #[test]
    fn test_cost_tracker_reach_data_block_limit() {
        let mint_keypair = test_setup();
        // build two transactions with diff accounts
        let second_account = Keypair::new();
        let tx1 = build_simple_transaction(&mint_keypair);
        let mut tx_cost1 = simple_transaction_cost(&tx1, 5);
        let tx2 = build_simple_transaction(&second_account);
        let mut tx_cost2 = simple_transaction_cost(&tx2, 5);
        if let TransactionCost::Transaction(ref mut usage_cost) = tx_cost1 {
            usage_cost.allocated_accounts_data_size = MAX_BLOCK_ACCOUNTS_DATA_SIZE_DELTA;
        } else {
            unreachable!();
        }
        if let TransactionCost::Transaction(ref mut usage_cost) = tx_cost2 {
            usage_cost.allocated_accounts_data_size = MAX_BLOCK_ACCOUNTS_DATA_SIZE_DELTA + 1;
        } else {
            unreachable!();
        }
        let cost1 = tx_cost1.sum();
        let cost2 = tx_cost2.sum();

        // build testee that passes
        let testee = CostTracker::new(cmp::max(cost1, cost2), cost1 + cost2 - 1, cost1 + cost2 - 1);
        assert!(testee.would_fit(&tx_cost1, 0).is_ok());
        // data is too big
        assert_eq!(
            testee.would_fit(&tx_cost2, 0),
            Err(CostTrackerError::WouldExceedAccountDataBlockLimit),
        );
    }

    #[test]
    fn test_cost_tracker_remove() {
        let mint_keypair = test_setup();
        // build two transactions with diff accounts
        let second_account = Keypair::new();
        let tx1 = build_simple_transaction(&mint_keypair);
        let tx_cost1 = simple_transaction_cost(&tx1, 5);
        let tx2 = build_simple_transaction(&second_account);
        let tx_cost2 = simple_transaction_cost(&tx2, 5);
        let cost1 = tx_cost1.sum();
        let cost2 = tx_cost2.sum();

        // build testee
        let mut testee = CostTracker::new(cost1 + cost2, cost1 + cost2, cost1 + cost2);

        assert!(testee.try_add(&tx_cost1, 0).is_ok());
        assert!(testee.try_add(&tx_cost2, 0).is_ok());
        assert_eq!(cost1 + cost2, testee.block_cost);

        // removing a tx_cost affects block_cost
        testee.remove(&tx_cost1);
        assert_eq!(cost2, testee.block_cost);

        // add back tx1
        assert!(testee.try_add(&tx_cost1, 0).is_ok());
        assert_eq!(cost1 + cost2, testee.block_cost);

        // cannot add tx1 again, cost limit would be exceeded
        assert!(testee.try_add(&tx_cost1, 0).is_err());
    }

    #[test]
    fn test_cost_tracker_try_add_is_atomic() {
        let acct1 = Pubkey::new_unique();
        let acct2 = Pubkey::new_unique();
        let acct3 = Pubkey::new_unique();
        let cost = 100;
        let account_max = cost * 2;
        let block_max = account_max * 3; // for three accts

        let mut testee = CostTracker::new(account_max, block_max, block_max);

        // case 1: a tx writes to 3 accounts, should success, we will have:
        // | acct1 | $cost |
        // | acct2 | $cost |
        // | acct3 | $cost |
        // and block_cost = $cost
        {
            let transaction = WritableKeysTransaction(vec![acct1, acct2, acct3]);
            let tx_cost = simple_transaction_cost(&transaction, cost);
            assert!(testee.try_add(&tx_cost, 0).is_ok());
            let (_costliest_account, costliest_account_cost) = testee.find_costliest_account();
            assert_eq!(cost, testee.block_cost);
            assert_eq!(3, testee.cost_by_writable_accounts.len());
            assert_eq!(cost, costliest_account_cost);
        }

        // case 2: add tx writes to acct2 with $cost, should succeed, result to
        // | acct1 | $cost |
        // | acct2 | $cost * 2 |
        // | acct3 | $cost |
        // and block_cost = $cost * 2
        {
            let transaction = WritableKeysTransaction(vec![acct2]);
            let tx_cost = simple_transaction_cost(&transaction, cost);
            assert!(testee.try_add(&tx_cost, 0).is_ok());
            let (costliest_account, costliest_account_cost) = testee.find_costliest_account();
            assert_eq!(cost * 2, testee.block_cost);
            assert_eq!(3, testee.cost_by_writable_accounts.len());
            assert_eq!(cost * 2, costliest_account_cost);
            assert_eq!(acct2, costliest_account);
        }

        // case 3: add tx writes to [acct1, acct2], acct2 exceeds limit, should failed atomically,
        // we should still have:
        // | acct1 | $cost |
        // | acct2 | $cost * 2 |
        // | acct3 | $cost |
        // and block_cost = $cost * 2
        {
            let transaction = WritableKeysTransaction(vec![acct1, acct2]);
            let tx_cost = simple_transaction_cost(&transaction, cost);
            assert!(testee.try_add(&tx_cost, 0).is_err());
            let (costliest_account, costliest_account_cost) = testee.find_costliest_account();
            assert_eq!(cost * 2, testee.block_cost);
            assert_eq!(3, testee.cost_by_writable_accounts.len());
            assert_eq!(cost * 2, costliest_account_cost);
            assert_eq!(acct2, costliest_account);
        }
    }

    #[test]
    fn test_adjust_transaction_execution_cost() {
        let acct1 = Pubkey::new_unique();
        let acct2 = Pubkey::new_unique();
        let acct3 = Pubkey::new_unique();
        let cost = 100;
        let account_max = cost * 2;
        let block_max = account_max * 3; // for three accts

        let mut testee = CostTracker::new(account_max, block_max, block_max);
        let transaction = WritableKeysTransaction(vec![acct1, acct2, acct3]);
        let tx_cost = simple_transaction_cost(&transaction, cost);
        let mut expected_block_cost = tx_cost.sum();
        let expected_tx_count = 1;
        assert!(testee.try_add(&tx_cost, 0).is_ok());
        assert_eq!(expected_block_cost, testee.block_cost());
        assert_eq!(expected_tx_count, testee.transaction_count());
        testee
            .cost_by_writable_accounts
            .iter()
            .for_each(|(_key, units)| {
                assert_eq!(expected_block_cost, *units);
            });

        // adjust up
        {
            let adjustment = 50u64;
            testee.add_transaction_execution_cost(&tx_cost, adjustment);
            expected_block_cost += 50;
            assert_eq!(expected_block_cost, testee.block_cost());
            assert_eq!(expected_tx_count, testee.transaction_count());
            testee
                .cost_by_writable_accounts
                .iter()
                .for_each(|(_key, units)| {
                    assert_eq!(expected_block_cost, *units);
                });
        }

        // adjust down
        {
            let adjustment = 50u64;
            testee.sub_transaction_execution_cost(&tx_cost, adjustment);
            expected_block_cost -= 50;
            assert_eq!(expected_block_cost, testee.block_cost());
            assert_eq!(expected_tx_count, testee.transaction_count());
            testee
                .cost_by_writable_accounts
                .iter()
                .for_each(|(_key, units)| {
                    assert_eq!(expected_block_cost, *units);
                });
        }

        // adjust overflow
        {
            testee.add_transaction_execution_cost(&tx_cost, u64::MAX);
            // expect block cost set to limit
            assert_eq!(u64::MAX, testee.block_cost());
            assert_eq!(expected_tx_count, testee.transaction_count());
            testee
                .cost_by_writable_accounts
                .iter()
                .for_each(|(_key, units)| {
                    assert_eq!(u64::MAX, *units);
                });
        }

        // adjust underflow
        {
            testee.sub_transaction_execution_cost(&tx_cost, u64::MAX);
            // expect block cost set to limit
            assert_eq!(u64::MIN, testee.block_cost());
            assert_eq!(expected_tx_count, testee.transaction_count());
            testee
                .cost_by_writable_accounts
                .iter()
                .for_each(|(_key, units)| {
                    assert_eq!(u64::MIN, *units);
                });
            // assert the number of non-empty accounts is zero, but map
            // still contains 3 account
            assert_eq!(0, testee.number_of_accounts());
            assert_eq!(3, testee.cost_by_writable_accounts.len());
        }
    }

    #[test]
    fn test_update_execution_cost() {
        let estimated_programs_execution_cost = 100;
        let estimated_loaded_accounts_data_size_cost = 200;
        let number_writeble_accounts = 3;
        let transaction = WritableKeysTransaction(
            std::iter::repeat_with(Pubkey::new_unique)
                .take(number_writeble_accounts)
                .collect(),
        );

        let mut usage_cost =
            simple_usage_cost_details(&transaction, estimated_programs_execution_cost);
        usage_cost.loaded_accounts_data_size_cost = estimated_loaded_accounts_data_size_cost;
        let tx_cost = TransactionCost::Transaction(usage_cost);
        // confirm tx_cost is only made up by programs_execution_cost and
        // loaded_accounts_data_size_cost
        let estimated_tx_cost = tx_cost.sum();
        assert_eq!(
            estimated_tx_cost,
            estimated_programs_execution_cost + estimated_loaded_accounts_data_size_cost
        );

        let test_update_cost_tracker =
            |execution_cost_adjust: i64, loaded_acounts_data_size_cost_adjust: i64| {
                let mut cost_tracker = CostTracker::default();
                assert!(cost_tracker.try_add(&tx_cost, 0).is_ok());

                let actual_programs_execution_cost =
                    (estimated_programs_execution_cost as i64 + execution_cost_adjust) as u64;
                let actual_loaded_accounts_data_size_cost =
                    (estimated_loaded_accounts_data_size_cost as i64
                        + loaded_acounts_data_size_cost_adjust) as u64;
                let expected_cost = (estimated_tx_cost as i64
                    + execution_cost_adjust
                    + loaded_acounts_data_size_cost_adjust)
                    as u64;

                cost_tracker.update_execution_cost(
                    &tx_cost,
                    actual_programs_execution_cost,
                    actual_loaded_accounts_data_size_cost,
                );

                assert_eq!(expected_cost, cost_tracker.block_cost);
                assert_eq!(0, cost_tracker.vote_cost);
                assert_eq!(
                    number_writeble_accounts,
                    cost_tracker.cost_by_writable_accounts.len()
                );
                for writable_account_cost in cost_tracker.cost_by_writable_accounts.values() {
                    assert_eq!(expected_cost, *writable_account_cost);
                }
                assert_eq!(1, cost_tracker.transaction_count.0);
            };

        test_update_cost_tracker(0, 0);
        test_update_cost_tracker(0, 9);
        test_update_cost_tracker(0, -9);
        test_update_cost_tracker(9, 0);
        test_update_cost_tracker(9, 9);
        test_update_cost_tracker(9, -9);
        test_update_cost_tracker(-9, 0);
        test_update_cost_tracker(-9, 9);
        test_update_cost_tracker(-9, -9);
    }

    #[test]
    fn test_remove_transaction_cost() {
        let mut cost_tracker = CostTracker::default();

        let cost = 100u64;
        let transaction = WritableKeysTransaction(vec![Pubkey::new_unique()]);
        let tx_cost = simple_transaction_cost(&transaction, cost);
        cost_tracker.add_transaction_cost(&tx_cost);
        // assert cost_tracker is reverted to default
        assert_eq!(1, cost_tracker.transaction_count.0);
        assert_eq!(1, cost_tracker.number_of_accounts());
        assert_eq!(cost, cost_tracker.block_cost);
        assert_eq!(0, cost_tracker.vote_cost);
        assert_eq!(0, cost_tracker.allocated_accounts_data_size.0);

        cost_tracker.remove_transaction_cost(&tx_cost);
        // assert cost_tracker is reverted to default
        assert_eq!(0, cost_tracker.transaction_count.0);
        assert_eq!(0, cost_tracker.number_of_accounts());
        assert_eq!(0, cost_tracker.block_cost);
        assert_eq!(0, cost_tracker.vote_cost);
        assert_eq!(0, cost_tracker.allocated_accounts_data_size.0);
    }

    #[test]
    fn test_cost_tracker_try_add_with_reservation() {
        let mut cost_tracker = CostTracker {
            block_cost_limit: 100,
            ..CostTracker::default()
        };

        let transaction = WritableKeysTransaction(vec![Pubkey::new_unique()]);
        let tx_cost = simple_transaction_cost(&transaction, 100);
        assert_eq!(
            cost_tracker.try_add(&tx_cost, 1).unwrap_err(),
            CostTrackerError::WouldExceedBlockMaxLimit
        );
        cost_tracker.try_add(&tx_cost, 0).unwrap();
    }
}
