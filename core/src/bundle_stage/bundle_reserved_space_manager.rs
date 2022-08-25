use {solana_runtime::bank::Bank, solana_sdk::clock::Slot, std::sync::Arc};

/// Manager responsible for reserving `bundle_reserved_cost` during the first `reserved_ticks` of a bank
/// and resetting the block cost limit to `block_cost_limit` after the reserved tick period is over
pub struct BundleReservedSpaceManager {
    // the bank's cost limit
    block_cost_limit: u64,
    // bundles get this much reserved space for the first reserved_ticks
    bundle_reserved_cost: u64,
    // a reduced block_compute_limit is reserved for this many ticks, afterwards it goes back to full cost
    reserved_ticks: u64,
    last_slot_updated: Slot,
}

impl BundleReservedSpaceManager {
    pub fn new(block_cost_limit: u64, bundle_reserved_cost: u64, reserved_ticks: u64) -> Self {
        Self {
            block_cost_limit,
            bundle_reserved_cost,
            reserved_ticks,
            last_slot_updated: u64::MAX,
        }
    }

    /// Call this on creation of new bank and periodically while bundle processing
    /// to manage the block_cost_limits
    pub fn tick(&mut self, bank: &Arc<Bank>) {
        if self.last_slot_updated == bank.slot() && !self.is_in_reserved_tick_period(bank) {
            // new slot logic already ran, need to revert the block cost limit to original if
            // ticks are past the reserved tick mark
            debug!(
                "slot: {} ticks: {}, resetting block_cost_limit to {}",
                bank.slot(),
                bank.tick_height(),
                self.block_cost_limit
            );
            bank.write_cost_tracker()
                .unwrap()
                .set_block_cost_limit(self.block_cost_limit);
        } else if self.last_slot_updated != bank.slot() && self.is_in_reserved_tick_period(bank) {
            // new slot, if in the first max_tick - tick_height slots reserve space
            // otherwise can leave the current block limit as is
            let new_block_cost_limit = self.reduced_block_cost_limit();
            debug!(
                "slot: {} ticks: {}, reserving block_cost_limit with block_cost_limit of {}",
                bank.slot(),
                bank.tick_height(),
                new_block_cost_limit
            );
            bank.write_cost_tracker()
                .unwrap()
                .set_block_cost_limit(new_block_cost_limit);
            self.last_slot_updated = bank.slot();
        }
    }

    /// return true if the bank is still in the period where block_cost_limits is reduced
    pub fn is_in_reserved_tick_period(&self, bank: &Bank) -> bool {
        bank.tick_height() % bank.ticks_per_slot() < self.reserved_ticks
    }

    /// return the block_cost_limits as determined by the tick height of the bank
    pub fn expected_block_cost_limits(&self, bank: &Bank) -> u64 {
        if self.is_in_reserved_tick_period(bank) {
            self.reduced_block_cost_limit()
        } else {
            self.block_cost_limit()
        }
    }

    pub fn reduced_block_cost_limit(&self) -> u64 {
        self.block_cost_limit
            .saturating_sub(self.bundle_reserved_cost)
    }

    pub fn block_cost_limit(&self) -> u64 {
        self.block_cost_limit
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::bundle_stage::bundle_reserved_space_manager::BundleReservedSpaceManager,
        solana_ledger::genesis_utils::create_genesis_config, solana_runtime::bank::Bank,
        solana_sdk::pubkey::Pubkey, std::sync::Arc,
    };

    #[test]
    fn test_reserve_block_cost_limits_during_reserved_ticks() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;

        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        let block_cost_limits = bank.read_cost_tracker().unwrap().block_cost_limit();

        let mut reserved_space = BundleReservedSpaceManager::new(
            block_cost_limits,
            BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            5,
        );
        reserved_space.tick(&bank);

        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            block_cost_limits - BUNDLE_BLOCK_COST_LIMITS_RESERVATION
        );
    }

    #[test]
    fn test_dont_reserve_block_cost_limits_after_reserved_ticks() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;

        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        let block_cost_limits = bank.read_cost_tracker().unwrap().block_cost_limit();

        for _ in 0..5 {
            bank.register_default_tick_for_test();
        }

        let mut reserved_space = BundleReservedSpaceManager::new(
            block_cost_limits,
            BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            5,
        );
        reserved_space.tick(&bank);

        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            block_cost_limits
        );
    }

    #[test]
    fn test_dont_reset_block_cost_limits_during_reserved_ticks() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;

        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        let block_cost_limits = bank.read_cost_tracker().unwrap().block_cost_limit();

        let mut reserved_space = BundleReservedSpaceManager::new(
            block_cost_limits,
            BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            5,
        );

        reserved_space.tick(&bank);
        bank.register_default_tick_for_test();
        reserved_space.tick(&bank);

        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            block_cost_limits - BUNDLE_BLOCK_COST_LIMITS_RESERVATION
        );
    }

    #[test]
    fn test_reset_block_cost_limits_after_reserved_ticks() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;

        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        let block_cost_limits = bank.read_cost_tracker().unwrap().block_cost_limit();

        let mut reserved_space = BundleReservedSpaceManager::new(
            block_cost_limits,
            BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            5,
        );

        reserved_space.tick(&bank);

        for _ in 0..5 {
            bank.register_default_tick_for_test();
        }
        reserved_space.tick(&bank);

        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            block_cost_limits
        );
    }

    #[test]
    fn test_block_limits_after_first_slot() {
        const BUNDLE_BLOCK_COST_LIMITS_RESERVATION: u64 = 100;
        const RESERVED_TICKS: u64 = 5;
        let genesis_config_info = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config_info.genesis_config));

        for _ in 0..genesis_config_info.genesis_config.ticks_per_slot {
            bank.register_default_tick_for_test();
        }
        assert!(bank.is_complete());
        bank.freeze();
        assert_eq!(
            bank.read_cost_tracker().unwrap().block_cost_limit(),
            solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS,
        );

        let bank1 = Arc::new(Bank::new_from_parent(bank.clone(), &Pubkey::default(), 1));
        assert_eq!(bank1.slot(), 1);
        assert_eq!(bank1.tick_height(), 64);
        assert_eq!(bank1.max_tick_height(), 128);

        // reserve space
        let block_cost_limits = bank1.read_cost_tracker().unwrap().block_cost_limit();
        let mut reserved_space = BundleReservedSpaceManager::new(
            block_cost_limits,
            BUNDLE_BLOCK_COST_LIMITS_RESERVATION,
            RESERVED_TICKS,
        );
        reserved_space.tick(&bank1);

        // wait for reservation to be over
        (0..RESERVED_TICKS).for_each(|_| {
            bank1.register_default_tick_for_test();
            assert_eq!(
                bank1.read_cost_tracker().unwrap().block_cost_limit(),
                block_cost_limits - BUNDLE_BLOCK_COST_LIMITS_RESERVATION
            );
        });
        reserved_space.tick(&bank1);

        // after reservation, revert back to normal limit
        assert_eq!(
            bank1.read_cost_tracker().unwrap().block_cost_limit(),
            solana_cost_model::block_cost_limits::MAX_BLOCK_UNITS,
        );
    }
}
