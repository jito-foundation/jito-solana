use {
    crate::{
        blockstore::Blockstore,
        leader_schedule::{FixedSchedule, LeaderSchedule},
        leader_schedule_utils,
    },
    itertools::Itertools,
    log::*,
    solana_runtime::bank::Bank,
    solana_sdk::{
        clock::{Epoch, Slot},
        epoch_schedule::EpochSchedule,
        pubkey::Pubkey,
    },
    std::{
        collections::{hash_map::Entry, HashMap, VecDeque},
        sync::{Arc, RwLock},
    },
};

type CachedSchedules = (HashMap<Epoch, Arc<LeaderSchedule>>, VecDeque<u64>);
const MAX_SCHEDULES: usize = 10;

struct CacheCapacity(usize);
impl Default for CacheCapacity {
    fn default() -> Self {
        CacheCapacity(MAX_SCHEDULES)
    }
}

#[derive(Default)]
pub struct LeaderScheduleCache {
    // Map from an epoch to a leader schedule for that epoch
    pub cached_schedules: RwLock<CachedSchedules>,
    epoch_schedule: EpochSchedule,
    max_epoch: RwLock<Epoch>,
    max_schedules: CacheCapacity,
    fixed_schedule: Option<Arc<FixedSchedule>>,
}

impl LeaderScheduleCache {
    pub fn new_from_bank(bank: &Bank) -> Self {
        Self::new(*bank.epoch_schedule(), bank)
    }

    pub fn new(epoch_schedule: EpochSchedule, root_bank: &Bank) -> Self {
        let cache = Self {
            cached_schedules: RwLock::new((HashMap::new(), VecDeque::new())),
            epoch_schedule,
            max_epoch: RwLock::new(0),
            max_schedules: CacheCapacity::default(),
            fixed_schedule: None,
        };

        // This sets the root and calculates the schedule at leader_schedule_epoch(root)
        cache.set_root(root_bank);

        // Calculate the schedule for all epochs between 0 and leader_schedule_epoch(root)
        let leader_schedule_epoch = epoch_schedule.get_leader_schedule_epoch(root_bank.slot());
        for epoch in 0..leader_schedule_epoch {
            let first_slot_in_epoch = epoch_schedule.get_first_slot_in_epoch(epoch);
            cache.slot_leader_at(first_slot_in_epoch, Some(root_bank));
        }
        cache
    }

    pub fn set_max_schedules(&mut self, max_schedules: usize) {
        if max_schedules > 0 {
            self.max_schedules = CacheCapacity(max_schedules);
        }
    }

    pub fn max_schedules(&self) -> usize {
        self.max_schedules.0
    }

    pub fn set_root(&self, root_bank: &Bank) {
        let new_max_epoch = self
            .epoch_schedule
            .get_leader_schedule_epoch(root_bank.slot());
        let old_max_epoch = {
            let mut max_epoch = self.max_epoch.write().unwrap();
            let old_max_epoch = *max_epoch;
            *max_epoch = new_max_epoch;
            assert!(new_max_epoch >= old_max_epoch);
            old_max_epoch
        };

        // Calculate the epoch as soon as it's rooted
        if new_max_epoch > old_max_epoch {
            self.compute_epoch_schedule(new_max_epoch, root_bank);
        }
    }

    pub fn slot_leader_at(&self, slot: Slot, bank: Option<&Bank>) -> Option<Pubkey> {
        if let Some(bank) = bank {
            self.slot_leader_at_else_compute(slot, bank)
        } else if self.epoch_schedule.slots_per_epoch == 0 {
            None
        } else {
            self.slot_leader_at_no_compute(slot)
        }
    }

    /// Returns the (next slot, last slot) consecutive range of slots after
    /// the given current_slot that the given node will be leader.
    pub fn next_leader_slot(
        &self,
        pubkey: &Pubkey,
        current_slot: Slot,
        bank: &Bank,
        blockstore: Option<&Blockstore>,
        max_slot_range: u64,
    ) -> Option<(Slot, Slot)> {
        let (epoch, start_index) = bank.get_epoch_and_slot_index(current_slot + 1);
        let max_epoch = *self.max_epoch.read().unwrap();
        if epoch > max_epoch {
            debug!(
                "Requested next leader in slot: {} of unconfirmed epoch: {}",
                current_slot + 1,
                epoch
            );
            return None;
        }
        // Slots after current_slot where pubkey is the leader.
        let mut schedule = (epoch..=max_epoch)
            .map(|epoch| self.get_epoch_schedule_else_compute(epoch, bank))
            .while_some()
            .zip(epoch..)
            .flat_map(|(leader_schedule, k)| {
                let offset = if k == epoch { start_index as usize } else { 0 };
                let num_slots = bank.get_slots_in_epoch(k) as usize;
                let first_slot = bank.epoch_schedule().get_first_slot_in_epoch(k);
                leader_schedule
                    .get_indices(pubkey, offset)
                    .take_while(move |i| *i < num_slots)
                    .map(move |i| i as Slot + first_slot)
            })
            .skip_while(|slot| {
                match blockstore {
                    None => false,
                    // Skip slots we have already sent a shred for.
                    Some(blockstore) => match blockstore.meta(*slot).unwrap() {
                        Some(meta) => meta.received > 0,
                        None => false,
                    },
                }
            });
        let first_slot = schedule.next()?;
        let max_slot = first_slot.saturating_add(max_slot_range);
        let last_slot = schedule
            .take_while(|slot| *slot < max_slot)
            .zip(first_slot + 1..)
            .take_while(|(a, b)| a == b)
            .map(|(s, _)| s)
            .last()
            .unwrap_or(first_slot);
        Some((first_slot, last_slot))
    }

    pub fn set_fixed_leader_schedule(&mut self, fixed_schedule: Option<FixedSchedule>) {
        self.fixed_schedule = fixed_schedule.map(Arc::new);
    }

    fn slot_leader_at_no_compute(&self, slot: Slot) -> Option<Pubkey> {
        let (epoch, slot_index) = self.epoch_schedule.get_epoch_and_slot_index(slot);
        if let Some(ref fixed_schedule) = self.fixed_schedule {
            return Some(fixed_schedule.leader_schedule[slot_index]);
        }
        self.cached_schedules
            .read()
            .unwrap()
            .0
            .get(&epoch)
            .map(|schedule| schedule[slot_index])
    }

    fn slot_leader_at_else_compute(&self, slot: Slot, bank: &Bank) -> Option<Pubkey> {
        let cache_result = self.slot_leader_at_no_compute(slot);
        // Forbid asking for slots in an unconfirmed epoch
        let bank_epoch = self.epoch_schedule.get_epoch_and_slot_index(slot).0;
        if bank_epoch > *self.max_epoch.read().unwrap() {
            debug!(
                "Requested leader in slot: {} of unconfirmed epoch: {}",
                slot, bank_epoch
            );
            return None;
        }
        if cache_result.is_some() {
            cache_result
        } else {
            let (epoch, slot_index) = bank.get_epoch_and_slot_index(slot);
            self.compute_epoch_schedule(epoch, bank)
                .map(|epoch_schedule| epoch_schedule[slot_index])
        }
    }

    pub fn get_epoch_leader_schedule(&self, epoch: Epoch) -> Option<Arc<LeaderSchedule>> {
        self.cached_schedules.read().unwrap().0.get(&epoch).cloned()
    }

    fn get_epoch_schedule_else_compute(
        &self,
        epoch: Epoch,
        bank: &Bank,
    ) -> Option<Arc<LeaderSchedule>> {
        if let Some(ref fixed_schedule) = self.fixed_schedule {
            return Some(fixed_schedule.leader_schedule.clone());
        }
        let epoch_schedule = self.get_epoch_leader_schedule(epoch);
        if epoch_schedule.is_some() {
            epoch_schedule
        } else {
            self.compute_epoch_schedule(epoch, bank)
        }
    }

    fn compute_epoch_schedule(&self, epoch: Epoch, bank: &Bank) -> Option<Arc<LeaderSchedule>> {
        let leader_schedule = leader_schedule_utils::leader_schedule(epoch, bank);
        leader_schedule.map(|leader_schedule| {
            let leader_schedule = Arc::new(leader_schedule);
            let (ref mut cached_schedules, ref mut order) = *self.cached_schedules.write().unwrap();
            // Check to see if schedule exists in case somebody already inserted in the time we were
            // waiting for the lock
            let entry = cached_schedules.entry(epoch);
            if let Entry::Vacant(v) = entry {
                v.insert(leader_schedule.clone());
                order.push_back(epoch);
                Self::retain_latest(cached_schedules, order, self.max_schedules());
            }
            leader_schedule
        })
    }

    fn retain_latest(
        schedules: &mut HashMap<Epoch, Arc<LeaderSchedule>>,
        order: &mut VecDeque<u64>,
        max_schedules: usize,
    ) {
        while schedules.len() > max_schedules {
            let first = order.pop_front().unwrap();
            schedules.remove(&first);
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            blockstore::make_slot_entries,
            genesis_utils::{
                bootstrap_validator_stake_lamports, create_genesis_config,
                create_genesis_config_with_leader, GenesisConfigInfo,
            },
            get_tmp_ledger_path_auto_delete,
            staking_utils::tests::setup_vote_and_stake_accounts,
        },
        crossbeam_channel::unbounded,
        solana_runtime::bank::Bank,
        solana_sdk::{
            clock::NUM_CONSECUTIVE_LEADER_SLOTS,
            epoch_schedule::{
                EpochSchedule, DEFAULT_LEADER_SCHEDULE_SLOT_OFFSET, DEFAULT_SLOTS_PER_EPOCH,
                MINIMUM_SLOTS_PER_EPOCH,
            },
            signature::{Keypair, Signer},
        },
        std::{sync::Arc, thread::Builder},
    };

    #[test]
    fn test_new_cache() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Bank::new_for_tests(&genesis_config);
        let cache = LeaderScheduleCache::new_from_bank(&bank);
        assert_eq!(bank.slot(), 0);
        assert_eq!(cache.max_schedules(), MAX_SCHEDULES);

        // Epoch schedule for all epochs in the range:
        // [0, leader_schedule_epoch(bank.slot())] should
        // be calculated by constructor
        let epoch_schedule = bank.epoch_schedule();
        let leader_schedule_epoch = bank.get_leader_schedule_epoch(bank.slot());
        for epoch in 0..=leader_schedule_epoch {
            let first_slot_in_leader_schedule_epoch = epoch_schedule.get_first_slot_in_epoch(epoch);
            let last_slot_in_leader_schedule_epoch = epoch_schedule.get_last_slot_in_epoch(epoch);
            assert!(cache
                .slot_leader_at(first_slot_in_leader_schedule_epoch, None)
                .is_some());
            assert!(cache
                .slot_leader_at(last_slot_in_leader_schedule_epoch, None)
                .is_some());
            if epoch == leader_schedule_epoch {
                assert!(cache
                    .slot_leader_at(last_slot_in_leader_schedule_epoch + 1, None)
                    .is_none());
            }
        }

        // Should be a schedule for every epoch just checked
        assert_eq!(
            cache.cached_schedules.read().unwrap().0.len() as u64,
            leader_schedule_epoch + 1
        );
    }

    #[test]
    fn test_retain_latest() {
        let mut cached_schedules = HashMap::new();
        let mut order = VecDeque::new();
        for i in 0..=MAX_SCHEDULES {
            cached_schedules.insert(i as u64, Arc::new(LeaderSchedule::default()));
            order.push_back(i as u64);
        }
        LeaderScheduleCache::retain_latest(&mut cached_schedules, &mut order, MAX_SCHEDULES);
        assert_eq!(cached_schedules.len(), MAX_SCHEDULES);
        let mut keys: Vec<_> = cached_schedules.keys().cloned().collect();
        keys.sort_unstable();
        let expected: Vec<_> = (1..=MAX_SCHEDULES as u64).collect();
        let expected_order: VecDeque<_> = (1..=MAX_SCHEDULES as u64).collect();
        assert_eq!(expected, keys);
        assert_eq!(expected_order, order);
    }

    #[test]
    fn test_thread_race_leader_schedule_cache() {
        let num_runs = 10;
        for _ in 0..num_runs {
            run_thread_race()
        }
    }

    fn run_thread_race() {
        let slots_per_epoch = MINIMUM_SLOTS_PER_EPOCH as u64;
        let epoch_schedule = EpochSchedule::custom(slots_per_epoch, slots_per_epoch / 2, true);
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let cache = Arc::new(LeaderScheduleCache::new(epoch_schedule, &bank));

        let num_threads = 10;
        let (threads, senders): (Vec<_>, Vec<_>) = (0..num_threads)
            .map(|_| {
                let cache = cache.clone();
                let bank = bank.clone();
                let (sender, receiver) = unbounded();
                (
                    Builder::new()
                        .name("test_thread_race_leader_schedule_cache".to_string())
                        .spawn(move || {
                            let _ = receiver.recv();
                            cache.slot_leader_at(bank.slot(), Some(&bank));
                        })
                        .unwrap(),
                    sender,
                )
            })
            .unzip();

        for sender in &senders {
            sender.send(true).unwrap();
        }

        for t in threads.into_iter() {
            t.join().unwrap();
        }

        let (ref cached_schedules, ref order) = *cache.cached_schedules.read().unwrap();
        assert_eq!(cached_schedules.len(), 1);
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn test_next_leader_slot() {
        let pubkey = solana_sdk::pubkey::new_rand();
        let mut genesis_config =
            create_genesis_config_with_leader(42, &pubkey, bootstrap_validator_stake_lamports())
                .genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::custom(
            DEFAULT_SLOTS_PER_EPOCH,
            DEFAULT_LEADER_SCHEDULE_SLOT_OFFSET,
            false,
        );

        let bank = Bank::new_for_tests(&genesis_config);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        assert_eq!(
            cache.slot_leader_at(bank.slot(), Some(&bank)).unwrap(),
            pubkey
        );
        assert_eq!(
            cache.next_leader_slot(&pubkey, 0, &bank, None, std::u64::MAX),
            Some((1, 863_999))
        );
        assert_eq!(
            cache.next_leader_slot(&pubkey, 1, &bank, None, std::u64::MAX),
            Some((2, 863_999))
        );
        assert_eq!(
            cache.next_leader_slot(
                &pubkey,
                2 * genesis_config.epoch_schedule.slots_per_epoch - 1, // no schedule generated for epoch 2
                &bank,
                None,
                std::u64::MAX
            ),
            None
        );

        assert_eq!(
            cache.next_leader_slot(
                &solana_sdk::pubkey::new_rand(), // not in leader_schedule
                0,
                &bank,
                None,
                std::u64::MAX
            ),
            None
        );
    }

    #[test]
    fn test_next_leader_slot_blockstore() {
        let pubkey = solana_sdk::pubkey::new_rand();
        let mut genesis_config =
            create_genesis_config_with_leader(42, &pubkey, bootstrap_validator_stake_lamports())
                .genesis_config;
        genesis_config.epoch_schedule.warmup = false;

        let bank = Bank::new_for_tests(&genesis_config);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));
        let ledger_path = get_tmp_ledger_path_auto_delete!();

        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");

        assert_eq!(
            cache.slot_leader_at(bank.slot(), Some(&bank)).unwrap(),
            pubkey
        );
        // Check that the next leader slot after 0 is slot 1
        assert_eq!(
            cache
                .next_leader_slot(&pubkey, 0, &bank, Some(&blockstore), std::u64::MAX)
                .unwrap()
                .0,
            1
        );

        // Write a shred into slot 2 that chains to slot 1,
        // but slot 1 is empty so should not be skipped
        let (shreds, _) = make_slot_entries(2, 1, 1);
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert_eq!(
            cache
                .next_leader_slot(&pubkey, 0, &bank, Some(&blockstore), std::u64::MAX)
                .unwrap()
                .0,
            1
        );

        // Write a shred into slot 1
        let (shreds, _) = make_slot_entries(1, 0, 1);

        // Check that slot 1 and 2 are skipped
        blockstore.insert_shreds(shreds, None, false).unwrap();
        assert_eq!(
            cache
                .next_leader_slot(&pubkey, 0, &bank, Some(&blockstore), std::u64::MAX)
                .unwrap()
                .0,
            3
        );

        // Integrity checks
        assert_eq!(
            cache.next_leader_slot(
                &pubkey,
                2 * genesis_config.epoch_schedule.slots_per_epoch - 1, // no schedule generated for epoch 2
                &bank,
                Some(&blockstore),
                std::u64::MAX
            ),
            None
        );

        assert_eq!(
            cache.next_leader_slot(
                &solana_sdk::pubkey::new_rand(), // not in leader_schedule
                0,
                &bank,
                Some(&blockstore),
                std::u64::MAX
            ),
            None
        );
    }

    #[test]
    fn test_next_leader_slot_next_epoch() {
        let GenesisConfigInfo {
            mut genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000 * bootstrap_validator_stake_lamports());
        genesis_config.epoch_schedule.warmup = false;

        let bank = Bank::new_for_tests(&genesis_config);
        let cache = Arc::new(LeaderScheduleCache::new_from_bank(&bank));

        // Create new vote account
        let validator_identity = Keypair::new();
        let vote_account = Keypair::new();
        setup_vote_and_stake_accounts(
            &bank,
            &mint_keypair,
            &vote_account,
            &validator_identity,
            bootstrap_validator_stake_lamports()
                + solana_stake_program::get_minimum_delegation(&bank.feature_set),
        );
        let node_pubkey = validator_identity.pubkey();

        // Have to wait until the epoch at after the epoch stakes generated at genesis
        // for the new votes to take effect.
        let mut target_slot = 1;
        let epoch = bank.get_leader_schedule_epoch(0);
        while bank.get_leader_schedule_epoch(target_slot) == epoch {
            target_slot += 1;
        }

        let bank = Bank::new_from_parent(&Arc::new(bank), &Pubkey::default(), target_slot);
        let mut expected_slot = 0;
        let epoch = bank.get_leader_schedule_epoch(target_slot);
        for i in 0..epoch {
            expected_slot += bank.get_slots_in_epoch(i);
        }

        let schedule = cache.compute_epoch_schedule(epoch, &bank).unwrap();
        let mut index = 0;
        while schedule[index] != node_pubkey {
            index += 1;
            assert_ne!(index, genesis_config.epoch_schedule.slots_per_epoch);
        }
        expected_slot += index;

        // If the max root isn't set, we'll get None
        assert!(cache
            .next_leader_slot(&node_pubkey, 0, &bank, None, std::u64::MAX)
            .is_none());

        cache.set_root(&bank);
        let res = cache
            .next_leader_slot(&node_pubkey, 0, &bank, None, std::u64::MAX)
            .unwrap();

        assert_eq!(res.0, expected_slot);
        assert!(res.1 >= expected_slot + NUM_CONSECUTIVE_LEADER_SLOTS - 1);

        let res = cache
            .next_leader_slot(
                &node_pubkey,
                0,
                &bank,
                None,
                NUM_CONSECUTIVE_LEADER_SLOTS - 1,
            )
            .unwrap();

        assert_eq!(res.0, expected_slot);
        assert_eq!(res.1, expected_slot + NUM_CONSECUTIVE_LEADER_SLOTS - 2);
    }

    #[test]
    fn test_schedule_for_unconfirmed_epoch() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let cache = LeaderScheduleCache::new_from_bank(&bank);

        assert_eq!(*cache.max_epoch.read().unwrap(), 1);

        // Asking for the leader for the last slot in epoch 1 is ok b/c
        // epoch 1 is confirmed
        assert_eq!(bank.get_epoch_and_slot_index(95).0, 1);
        assert!(cache.slot_leader_at(95, Some(&bank)).is_some());

        // Asking for the lader for the first slot in epoch 2 is not ok
        // b/c epoch 2 is unconfirmed
        assert_eq!(bank.get_epoch_and_slot_index(96).0, 2);
        assert!(cache.slot_leader_at(96, Some(&bank)).is_none());

        let bank2 = Bank::new_from_parent(&bank, &solana_sdk::pubkey::new_rand(), 95);
        assert!(bank2.epoch_vote_accounts(2).is_some());

        // Set root for a slot in epoch 1, so that epoch 2 is now confirmed
        cache.set_root(&bank2);
        assert_eq!(*cache.max_epoch.read().unwrap(), 2);
        assert!(cache.slot_leader_at(96, Some(&bank2)).is_some());
        assert_eq!(bank2.get_epoch_and_slot_index(223).0, 2);
        assert!(cache.slot_leader_at(223, Some(&bank2)).is_some());
        assert_eq!(bank2.get_epoch_and_slot_index(224).0, 3);
        assert!(cache.slot_leader_at(224, Some(&bank2)).is_none());
    }

    #[test]
    fn test_set_max_schedules() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let mut cache = LeaderScheduleCache::new_from_bank(&bank);

        // Max schedules must be greater than 0
        cache.set_max_schedules(0);
        assert_eq!(cache.max_schedules(), MAX_SCHEDULES);

        cache.set_max_schedules(std::usize::MAX);
        assert_eq!(cache.max_schedules(), std::usize::MAX);
    }
}
