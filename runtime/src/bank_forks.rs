//! The `bank_forks` module implements BankForks a DAG of checkpointed Banks

use {
    crate::{
        accounts_background_service::{AbsRequestSender, SnapshotRequest},
        bank::Bank,
        snapshot_config::SnapshotConfig,
    },
    log::*,
    solana_measure::measure::Measure,
    solana_sdk::{clock::Slot, hash::Hash, timing},
    std::{
        collections::{hash_map::Entry, HashMap, HashSet},
        ops::Index,
        sync::Arc,
        time::Instant,
    },
};

#[derive(Debug, Default, Copy, Clone)]
struct SetRootMetrics {
    timings: SetRootTimings,
    total_parent_banks: i64,
    tx_count: i64,
    dropped_banks_len: i64,
    accounts_data_len: i64,
}

#[derive(Debug, Default, Copy, Clone)]
struct SetRootTimings {
    total_squash_cache_ms: i64,
    total_squash_accounts_ms: i64,
    total_squash_accounts_index_ms: i64,
    total_squash_accounts_cache_ms: i64,
    total_squash_accounts_store_ms: i64,
    total_snapshot_ms: i64,
    prune_non_rooted_ms: i64,
    drop_parent_banks_ms: i64,
    prune_slots_ms: i64,
    prune_remove_ms: i64,
}

pub struct BankForks {
    banks: HashMap<Slot, Arc<Bank>>,
    descendants: HashMap<Slot, HashSet<Slot>>,
    root: Slot,
    pub snapshot_config: Option<SnapshotConfig>,

    pub accounts_hash_interval_slots: Slot,
    last_accounts_hash_slot: Slot,
}

impl Index<u64> for BankForks {
    type Output = Arc<Bank>;
    fn index(&self, bank_slot: Slot) -> &Self::Output {
        &self.banks[&bank_slot]
    }
}

impl BankForks {
    pub fn new(bank: Bank) -> Self {
        let root = bank.slot();
        Self::new_from_banks(&[Arc::new(bank)], root)
    }

    pub fn banks(&self) -> HashMap<Slot, Arc<Bank>> {
        self.banks.clone()
    }

    /// Create a map of bank slot id to the set of ancestors for the bank slot.
    pub fn ancestors(&self) -> HashMap<Slot, HashSet<Slot>> {
        let root = self.root;
        self.banks
            .iter()
            .map(|(slot, bank)| {
                let ancestors = bank.proper_ancestors().filter(|k| *k >= root);
                (*slot, ancestors.collect())
            })
            .collect()
    }

    /// Create a map of bank slot id to the set of all of its descendants
    pub fn descendants(&self) -> HashMap<Slot, HashSet<Slot>> {
        self.descendants.clone()
    }

    pub fn frozen_banks(&self) -> HashMap<Slot, Arc<Bank>> {
        self.banks
            .iter()
            .filter(|(_, b)| b.is_frozen())
            .map(|(k, b)| (*k, b.clone()))
            .collect()
    }

    pub fn active_banks(&self) -> Vec<Slot> {
        self.banks
            .iter()
            .filter(|(_, v)| !v.is_frozen())
            .map(|(k, _v)| *k)
            .collect()
    }

    pub fn get(&self, bank_slot: Slot) -> Option<Arc<Bank>> {
        self.banks.get(&bank_slot).cloned()
    }

    pub fn get_with_checked_hash(
        &self,
        (bank_slot, expected_hash): (Slot, Hash),
    ) -> Option<Arc<Bank>> {
        let maybe_bank = self.get(bank_slot);
        if let Some(bank) = &maybe_bank {
            assert_eq!(bank.hash(), expected_hash);
        }
        maybe_bank
    }

    pub fn bank_hash(&self, slot: Slot) -> Option<Hash> {
        self.get(slot).map(|bank| bank.hash())
    }

    pub fn root_bank(&self) -> Arc<Bank> {
        self[self.root()].clone()
    }

    pub fn new_from_banks(initial_forks: &[Arc<Bank>], root: Slot) -> Self {
        let mut banks = HashMap::new();

        // Iterate through the heads of all the different forks
        for bank in initial_forks {
            banks.insert(bank.slot(), bank.clone());
            let parents = bank.parents();
            for parent in parents {
                if banks.insert(parent.slot(), parent.clone()).is_some() {
                    // All ancestors have already been inserted by another fork
                    break;
                }
            }
        }
        let mut descendants = HashMap::<_, HashSet<_>>::new();
        for (slot, bank) in &banks {
            descendants.entry(*slot).or_default();
            for parent in bank.proper_ancestors() {
                descendants.entry(parent).or_default().insert(*slot);
            }
        }
        Self {
            root,
            banks,
            descendants,
            snapshot_config: None,
            accounts_hash_interval_slots: std::u64::MAX,
            last_accounts_hash_slot: root,
        }
    }

    pub fn insert(&mut self, bank: Bank) -> Arc<Bank> {
        let bank = Arc::new(bank);
        let prev = self.banks.insert(bank.slot(), bank.clone());
        assert!(prev.is_none());
        let slot = bank.slot();
        self.descendants.entry(slot).or_default();
        for parent in bank.proper_ancestors() {
            self.descendants.entry(parent).or_default().insert(slot);
        }
        bank
    }

    pub fn remove(&mut self, slot: Slot) -> Option<Arc<Bank>> {
        let bank = self.banks.remove(&slot)?;
        for parent in bank.proper_ancestors() {
            let mut entry = match self.descendants.entry(parent) {
                Entry::Vacant(_) => panic!("this should not happen!"),
                Entry::Occupied(entry) => entry,
            };
            entry.get_mut().remove(&slot);
            if entry.get().is_empty() && !self.banks.contains_key(&parent) {
                entry.remove_entry();
            }
        }
        let entry = match self.descendants.entry(slot) {
            Entry::Vacant(_) => panic!("this should not happen!"),
            Entry::Occupied(entry) => entry,
        };
        if entry.get().is_empty() {
            entry.remove_entry();
        }
        Some(bank)
    }

    pub fn highest_slot(&self) -> Slot {
        self.banks.values().map(|bank| bank.slot()).max().unwrap()
    }

    pub fn working_bank(&self) -> Arc<Bank> {
        self[self.highest_slot()].clone()
    }

    fn do_set_root_return_metrics(
        &mut self,
        root: Slot,
        accounts_background_request_sender: &AbsRequestSender,
        highest_confirmed_root: Option<Slot>,
    ) -> (Vec<Arc<Bank>>, SetRootMetrics) {
        let old_epoch = self.root_bank().epoch();
        self.root = root;
        let root_bank = self
            .banks
            .get(&root)
            .expect("root bank didn't exist in bank_forks");
        let new_epoch = root_bank.epoch();
        if old_epoch != new_epoch {
            info!(
                "Root entering
                    epoch: {},
                    next_epoch_start_slot: {},
                    epoch_stakes: {:#?}",
                new_epoch,
                root_bank
                    .epoch_schedule()
                    .get_first_slot_in_epoch(new_epoch + 1),
                root_bank
                    .epoch_stakes(new_epoch)
                    .unwrap()
                    .node_id_to_vote_accounts()
            );
        }
        let root_tx_count = root_bank
            .parents()
            .last()
            .map(|bank| bank.transaction_count())
            .unwrap_or(0);
        // Calculate the accounts hash at a fixed interval
        let mut is_root_bank_squashed = false;
        let mut banks = vec![root_bank];
        let parents = root_bank.parents();
        banks.extend(parents.iter());
        let total_parent_banks = banks.len();
        let mut total_squash_accounts_ms = 0;
        let mut total_squash_accounts_index_ms = 0;
        let mut total_squash_accounts_cache_ms = 0;
        let mut total_squash_accounts_store_ms = 0;
        let mut total_squash_cache_ms = 0;
        let mut total_snapshot_ms = 0;
        for bank in banks.iter() {
            let bank_slot = bank.slot();
            if bank.block_height() % self.accounts_hash_interval_slots == 0
                && bank_slot > self.last_accounts_hash_slot
            {
                self.last_accounts_hash_slot = bank_slot;
                let squash_timing = bank.squash();
                total_squash_accounts_ms += squash_timing.squash_accounts_ms as i64;
                total_squash_accounts_index_ms += squash_timing.squash_accounts_index_ms as i64;
                total_squash_accounts_cache_ms += squash_timing.squash_accounts_cache_ms as i64;
                total_squash_accounts_store_ms += squash_timing.squash_accounts_store_ms as i64;
                total_squash_cache_ms += squash_timing.squash_cache_ms as i64;
                is_root_bank_squashed = bank_slot == root;

                let mut snapshot_time = Measure::start("squash::snapshot_time");
                if self.snapshot_config.is_some()
                    && accounts_background_request_sender.is_snapshot_creation_enabled()
                {
                    let snapshot_root_bank = self.root_bank();
                    let root_slot = snapshot_root_bank.slot();
                    if let Err(e) =
                        accounts_background_request_sender.send_snapshot_request(SnapshotRequest {
                            snapshot_root_bank,
                            // Save off the status cache because these may get pruned
                            // if another `set_root()` is called before the snapshots package
                            // can be generated
                            status_cache_slot_deltas: bank.src.slot_deltas(&bank.src.roots()),
                        })
                    {
                        warn!(
                            "Error sending snapshot request for bank: {}, err: {:?}",
                            root_slot, e
                        );
                    }
                }
                snapshot_time.stop();
                total_snapshot_ms += snapshot_time.as_ms() as i64;
                break;
            }
        }
        if !is_root_bank_squashed {
            let squash_timing = root_bank.squash();
            total_squash_accounts_ms += squash_timing.squash_accounts_ms as i64;
            total_squash_accounts_index_ms += squash_timing.squash_accounts_index_ms as i64;
            total_squash_accounts_cache_ms += squash_timing.squash_accounts_cache_ms as i64;
            total_squash_accounts_store_ms += squash_timing.squash_accounts_store_ms as i64;
            total_squash_cache_ms += squash_timing.squash_cache_ms as i64;
        }
        let new_tx_count = root_bank.transaction_count();
        let accounts_data_len = root_bank.load_accounts_data_len() as i64;
        let mut prune_time = Measure::start("set_root::prune");
        let (removed_banks, prune_slots_ms, prune_remove_ms) =
            self.prune_non_rooted(root, highest_confirmed_root);
        prune_time.stop();
        let dropped_banks_len = removed_banks.len();

        let mut drop_parent_banks_time = Measure::start("set_root::drop_banks");
        drop(parents);
        drop_parent_banks_time.stop();

        (
            removed_banks,
            SetRootMetrics {
                timings: SetRootTimings {
                    total_squash_cache_ms,
                    total_squash_accounts_ms,
                    total_squash_accounts_index_ms,
                    total_squash_accounts_cache_ms,
                    total_squash_accounts_store_ms,
                    total_snapshot_ms,
                    prune_non_rooted_ms: prune_time.as_ms() as i64,
                    drop_parent_banks_ms: drop_parent_banks_time.as_ms() as i64,
                    prune_slots_ms: prune_slots_ms as i64,
                    prune_remove_ms: prune_remove_ms as i64,
                },
                total_parent_banks: total_parent_banks as i64,
                tx_count: (new_tx_count - root_tx_count) as i64,
                dropped_banks_len: dropped_banks_len as i64,
                accounts_data_len,
            },
        )
    }

    pub fn set_root(
        &mut self,
        root: Slot,
        accounts_background_request_sender: &AbsRequestSender,
        highest_confirmed_root: Option<Slot>,
    ) -> Vec<Arc<Bank>> {
        let set_root_start = Instant::now();
        let (removed_banks, set_root_metrics) = self.do_set_root_return_metrics(
            root,
            accounts_background_request_sender,
            highest_confirmed_root,
        );
        datapoint_info!(
            "bank-forks_set_root",
            (
                "elapsed_ms",
                timing::duration_as_ms(&set_root_start.elapsed()) as usize,
                i64
            ),
            ("slot", root, i64),
            (
                "total_parent_banks",
                set_root_metrics.total_parent_banks,
                i64
            ),
            ("total_banks", self.banks.len(), i64),
            (
                "total_squash_cache_ms",
                set_root_metrics.timings.total_squash_cache_ms,
                i64
            ),
            (
                "total_squash_accounts_ms",
                set_root_metrics.timings.total_squash_accounts_ms,
                i64
            ),
            (
                "total_squash_accounts_index_ms",
                set_root_metrics.timings.total_squash_accounts_index_ms,
                i64
            ),
            (
                "total_squash_accounts_cache_ms",
                set_root_metrics.timings.total_squash_accounts_cache_ms,
                i64
            ),
            (
                "total_squash_accounts_store_ms",
                set_root_metrics.timings.total_squash_accounts_store_ms,
                i64
            ),
            (
                "total_snapshot_ms",
                set_root_metrics.timings.total_snapshot_ms,
                i64
            ),
            ("tx_count", set_root_metrics.tx_count, i64),
            (
                "prune_non_rooted_ms",
                set_root_metrics.timings.prune_non_rooted_ms,
                i64
            ),
            (
                "drop_parent_banks_ms",
                set_root_metrics.timings.drop_parent_banks_ms,
                i64
            ),
            (
                "prune_slots_ms",
                set_root_metrics.timings.prune_slots_ms,
                i64
            ),
            (
                "prune_remove_ms",
                set_root_metrics.timings.prune_remove_ms,
                i64
            ),
            ("dropped_banks_len", set_root_metrics.dropped_banks_len, i64),
            ("accounts_data_len", set_root_metrics.accounts_data_len, i64),
        );
        removed_banks
    }

    pub fn root(&self) -> Slot {
        self.root
    }

    /// After setting a new root, prune the banks that are no longer on rooted paths
    ///
    /// Given the following banks and slots...
    ///
    /// ```text
    /// slot 6                   * (G)
    ///                         /
    /// slot 5        (F)  *   /
    ///                    |  /
    /// slot 4    (E) *    | /
    ///               |    |/
    /// slot 3        |    * (D) <-- root, from set_root()
    ///               |    |
    /// slot 2    (C) *    |
    ///                \   |
    /// slot 1          \  * (B)
    ///                  \ |
    /// slot 0             * (A)  <-- highest confirmed root [1]
    /// ```
    ///
    /// ...where (D) is set as root, clean up (C) and (E), since they are not rooted.
    ///
    /// (A) is kept because it is greater-than-or-equal-to the highest confirmed root, and (D) is
    ///     one of its descendants
    /// (B) is kept for the same reason as (A)
    /// (C) is pruned since it is a lower slot than (D), but (D) is _not_ one of its descendants
    /// (D) is kept since it is the root
    /// (E) is pruned since it is not a descendant of (D)
    /// (F) is kept since it is a descendant of (D)
    /// (G) is kept for the same reason as (F)
    ///
    /// and in table form...
    ///
    /// ```text
    ///       |          |  is root a  | is a descendant ||
    ///  slot | is root? | descendant? |    of root?     || keep?
    /// ------+----------+-------------+-----------------++-------
    ///   (A) |     N    |      Y      |        N        ||   Y
    ///   (B) |     N    |      Y      |        N        ||   Y
    ///   (C) |     N    |      N      |        N        ||   N
    ///   (D) |     Y    |      N      |        N        ||   Y
    ///   (E) |     N    |      N      |        N        ||   N
    ///   (F) |     N    |      N      |        Y        ||   Y
    ///   (G) |     N    |      N      |        Y        ||   Y
    /// ```
    ///
    /// [1] RPC has the concept of commitment level, which is based on the highest confirmed root,
    /// i.e. the cluster-confirmed root.  This commitment is stronger than the local node's root.
    /// So (A) and (B) are kept to facilitate RPC at different commitment levels.  Everything below
    /// the highest confirmed root can be pruned.
    fn prune_non_rooted(
        &mut self,
        root: Slot,
        highest_confirmed_root: Option<Slot>,
    ) -> (Vec<Arc<Bank>>, u64, u64) {
        // Clippy doesn't like separating the two collects below,
        // but we want to collect timing separately, and the 2nd requires
        // a unique borrow to self which is already borrowed by self.banks
        #![allow(clippy::needless_collect)]
        let mut prune_slots_time = Measure::start("prune_slots");
        let highest_confirmed_root = highest_confirmed_root.unwrap_or(root);
        let prune_slots: Vec<_> = self
            .banks
            .keys()
            .copied()
            .filter(|slot| {
                let keep = *slot == root
                    || self.descendants[&root].contains(slot)
                    || (*slot < root
                        && *slot >= highest_confirmed_root
                        && self.descendants[slot].contains(&root));
                !keep
            })
            .collect();
        prune_slots_time.stop();

        let mut prune_remove_time = Measure::start("prune_slots");
        let removed_banks = prune_slots
            .into_iter()
            .filter_map(|slot| self.remove(slot))
            .collect();
        prune_remove_time.stop();

        (
            removed_banks,
            prune_slots_time.as_ms(),
            prune_remove_time.as_ms(),
        )
    }

    pub fn set_snapshot_config(&mut self, snapshot_config: Option<SnapshotConfig>) {
        self.snapshot_config = snapshot_config;
    }

    pub fn set_accounts_hash_interval_slots(&mut self, accounts_interval_slots: u64) {
        self.accounts_hash_interval_slots = accounts_interval_slots;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bank::tests::update_vote_account_timestamp,
            genesis_utils::{
                create_genesis_config, create_genesis_config_with_leader, GenesisConfigInfo,
            },
        },
        solana_sdk::{
            clock::UnixTimestamp,
            hash::Hash,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            sysvar::epoch_schedule::EpochSchedule,
        },
        solana_vote_program::vote_state::BlockTimestamp,
    };

    #[test]
    fn test_bank_forks_new() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank_forks = BankForks::new(bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        child_bank.register_tick(&Hash::default());
        bank_forks.insert(child_bank);
        assert_eq!(bank_forks[1u64].tick_height(), 1);
        assert_eq!(bank_forks.working_bank().tick_height(), 1);
    }

    #[test]
    fn test_bank_forks_new_from_banks() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let child_bank = Arc::new(Bank::new_from_parent(&bank, &Pubkey::default(), 1));

        let bank_forks = BankForks::new_from_banks(&[bank.clone(), child_bank.clone()], 0);
        assert_eq!(bank_forks.root(), 0);
        assert_eq!(bank_forks.working_bank().slot(), 1);

        let bank_forks = BankForks::new_from_banks(&[child_bank, bank], 0);
        assert_eq!(bank_forks.root(), 0);
        assert_eq!(bank_forks.working_bank().slot(), 1);
    }

    #[test]
    fn test_bank_forks_descendants() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank_forks = BankForks::new(bank);
        let bank0 = bank_forks[0].clone();
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.insert(bank);
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);
        bank_forks.insert(bank);
        let descendants = bank_forks.descendants();
        let children: HashSet<u64> = [1u64, 2u64].iter().copied().collect();
        assert_eq!(children, *descendants.get(&0).unwrap());
        assert!(descendants[&1].is_empty());
        assert!(descendants[&2].is_empty());
    }

    #[test]
    fn test_bank_forks_ancestors() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank_forks = BankForks::new(bank);
        let bank0 = bank_forks[0].clone();
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 1);
        bank_forks.insert(bank);
        let bank = Bank::new_from_parent(&bank0, &Pubkey::default(), 2);
        bank_forks.insert(bank);
        let ancestors = bank_forks.ancestors();
        assert!(ancestors[&0].is_empty());
        let parents: Vec<u64> = ancestors[&1].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
        let parents: Vec<u64> = ancestors[&2].iter().cloned().collect();
        assert_eq!(parents, vec![0]);
    }

    #[test]
    fn test_bank_forks_frozen_banks() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank_forks = BankForks::new(bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        bank_forks.insert(child_bank);
        assert!(bank_forks.frozen_banks().get(&0).is_some());
        assert!(bank_forks.frozen_banks().get(&1).is_none());
    }

    #[test]
    fn test_bank_forks_active_banks() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let bank = Bank::new_for_tests(&genesis_config);
        let mut bank_forks = BankForks::new(bank);
        let child_bank = Bank::new_from_parent(&bank_forks[0u64], &Pubkey::default(), 1);
        bank_forks.insert(child_bank);
        assert_eq!(bank_forks.active_banks(), vec![1]);
    }

    #[test]
    fn test_bank_forks_different_set_root() {
        solana_logger::setup();
        let leader_keypair = Keypair::new();
        let GenesisConfigInfo {
            mut genesis_config,
            voting_keypair,
            ..
        } = create_genesis_config_with_leader(10_000, &leader_keypair.pubkey(), 1_000);
        let slots_in_epoch = 32;
        genesis_config.epoch_schedule = EpochSchedule::new(slots_in_epoch);

        let bank0 = Bank::new_for_tests(&genesis_config);
        let mut bank_forks0 = BankForks::new(bank0);
        bank_forks0.set_root(0, &AbsRequestSender::default(), None);

        let bank1 = Bank::new_for_tests(&genesis_config);
        let mut bank_forks1 = BankForks::new(bank1);

        let additional_timestamp_secs = 2;

        let num_slots = slots_in_epoch + 1; // Advance past first epoch boundary
        for slot in 1..num_slots {
            // Just after the epoch boundary, timestamp a vote that will shift
            // Clock::unix_timestamp from Bank::unix_timestamp_from_genesis()
            let update_timestamp_case = slot == slots_in_epoch;

            let child1 = Bank::new_from_parent(&bank_forks0[slot - 1], &Pubkey::default(), slot);
            let child2 = Bank::new_from_parent(&bank_forks1[slot - 1], &Pubkey::default(), slot);

            if update_timestamp_case {
                for child in &[&child1, &child2] {
                    let recent_timestamp: UnixTimestamp = child.unix_timestamp_from_genesis();
                    update_vote_account_timestamp(
                        BlockTimestamp {
                            slot: child.slot(),
                            timestamp: recent_timestamp + additional_timestamp_secs,
                        },
                        child,
                        &voting_keypair.pubkey(),
                    );
                }
            }

            // Set root in bank_forks0 to truncate the ancestor history
            bank_forks0.insert(child1);
            bank_forks0.set_root(slot, &AbsRequestSender::default(), None);

            // Don't set root in bank_forks1 to keep the ancestor history
            bank_forks1.insert(child2);
        }
        let child1 = &bank_forks0.working_bank();
        let child2 = &bank_forks1.working_bank();

        child1.freeze();
        child2.freeze();

        info!("child0.ancestors: {:?}", child1.ancestors);
        info!("child1.ancestors: {:?}", child2.ancestors);
        assert_eq!(child1.hash(), child2.hash());
    }

    fn make_hash_map(data: Vec<(Slot, Vec<Slot>)>) -> HashMap<Slot, HashSet<Slot>> {
        data.into_iter()
            .map(|(k, v)| (k, v.into_iter().collect()))
            .collect()
    }

    #[test]
    fn test_bank_forks_with_set_root() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let mut banks = vec![Arc::new(Bank::new_for_tests(&genesis_config))];
        assert_eq!(banks[0].slot(), 0);
        let mut bank_forks = BankForks::new_from_banks(&banks, 0);
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[0], &Pubkey::default(), 1)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[1], &Pubkey::default(), 2)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[0], &Pubkey::default(), 3)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[3], &Pubkey::default(), 4)));
        assert_eq!(
            bank_forks.ancestors(),
            make_hash_map(vec![
                (0, vec![]),
                (1, vec![0]),
                (2, vec![0, 1]),
                (3, vec![0]),
                (4, vec![0, 3]),
            ])
        );
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![
                (0, vec![1, 2, 3, 4]),
                (1, vec![2]),
                (2, vec![]),
                (3, vec![4]),
                (4, vec![]),
            ])
        );
        bank_forks.set_root(
            2,
            &AbsRequestSender::default(),
            None, // highest confirmed root
        );
        banks[2].squash();
        assert_eq!(bank_forks.ancestors(), make_hash_map(vec![(2, vec![]),]));
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![(0, vec![2]), (1, vec![2]), (2, vec![]),])
        );
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[2], &Pubkey::default(), 5)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[5], &Pubkey::default(), 6)));
        assert_eq!(
            bank_forks.ancestors(),
            make_hash_map(vec![(2, vec![]), (5, vec![2]), (6, vec![2, 5])])
        );
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![
                (0, vec![2]),
                (1, vec![2]),
                (2, vec![5, 6]),
                (5, vec![6]),
                (6, vec![])
            ])
        );
    }

    #[test]
    fn test_bank_forks_with_highest_confirmed_root() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(10_000);
        let mut banks = vec![Arc::new(Bank::new_for_tests(&genesis_config))];
        assert_eq!(banks[0].slot(), 0);
        let mut bank_forks = BankForks::new_from_banks(&banks, 0);
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[0], &Pubkey::default(), 1)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[1], &Pubkey::default(), 2)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[0], &Pubkey::default(), 3)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[3], &Pubkey::default(), 4)));
        assert_eq!(
            bank_forks.ancestors(),
            make_hash_map(vec![
                (0, vec![]),
                (1, vec![0]),
                (2, vec![0, 1]),
                (3, vec![0]),
                (4, vec![0, 3]),
            ])
        );
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![
                (0, vec![1, 2, 3, 4]),
                (1, vec![2]),
                (2, vec![]),
                (3, vec![4]),
                (4, vec![]),
            ])
        );
        bank_forks.set_root(
            2,
            &AbsRequestSender::default(),
            Some(1), // highest confirmed root
        );
        banks[2].squash();
        assert_eq!(
            bank_forks.ancestors(),
            make_hash_map(vec![(1, vec![]), (2, vec![]),])
        );
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![(0, vec![1, 2]), (1, vec![2]), (2, vec![]),])
        );
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[2], &Pubkey::default(), 5)));
        banks.push(bank_forks.insert(Bank::new_from_parent(&banks[5], &Pubkey::default(), 6)));
        assert_eq!(
            bank_forks.ancestors(),
            make_hash_map(vec![
                (1, vec![]),
                (2, vec![]),
                (5, vec![2]),
                (6, vec![2, 5])
            ])
        );
        assert_eq!(
            bank_forks.descendants(),
            make_hash_map(vec![
                (0, vec![1, 2]),
                (1, vec![2]),
                (2, vec![5, 6]),
                (5, vec![6]),
                (6, vec![])
            ])
        );
    }
}
