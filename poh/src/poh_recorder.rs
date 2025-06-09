//! The `poh_recorder` module provides an object for synchronizing with Proof of History.
//! It synchronizes PoH, bank's register_tick and the ledger
//!
//! PohRecorder will send ticks or entries to a WorkingBank, if the current range of ticks is
//! within the specified WorkingBank range.
//!
//! For Ticks:
//! * new tick_height must be > WorkingBank::min_tick_height && new tick_height must be <= WorkingBank::max_tick_height
//!
//! For Entries:
//! * recorded entry must be >= WorkingBank::min_tick_height && entry must be < WorkingBank::max_tick_height
//!
#[cfg(feature = "dev-context-only-utils")]
use solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo};
use {
    crate::{leader_bank_notifier::LeaderBankNotifier, poh_service::PohService},
    crossbeam_channel::{
        bounded, unbounded, Receiver, RecvTimeoutError, SendError, Sender, TrySendError,
    },
    log::*,
    solana_entry::{
        entry::{hash_transactions, Entry},
        poh::Poh,
    },
    solana_ledger::{blockstore::Blockstore, leader_schedule_cache::LeaderScheduleCache},
    solana_measure::measure_us,
    solana_metrics::poh_timing_point::{send_poh_timing_point, PohTimingSender, SlotPohTimingInfo},
    solana_runtime::{bank::Bank, installed_scheduler_pool::BankWithScheduler},
    solana_sdk::{
        clock::{Slot, NUM_CONSECUTIVE_LEADER_SLOTS},
        hash::Hash,
        poh_config::PohConfig,
        pubkey::Pubkey,
        saturating_add_assign,
        transaction::VersionedTransaction,
    },
    std::{
        cmp,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex, RwLock,
        },
        time::{Duration, Instant},
    },
    thiserror::Error,
};

pub const GRACE_TICKS_FACTOR: u64 = 2;
pub const MAX_GRACE_SLOTS: u64 = 2;

#[derive(Error, Debug, Clone)]
pub enum PohRecorderError {
    #[error("max height reached")]
    MaxHeightReached,

    #[error("min height not reached")]
    MinHeightNotReached,

    #[error("send WorkingBankEntry error")]
    SendError(#[from] SendError<WorkingBankEntry>),
}

pub type Result<T> = std::result::Result<T, PohRecorderError>;

#[derive(Clone, Debug)]
pub struct WorkingBankEntry {
    pub bank: Arc<Bank>,
    // normal entries have len == 1, bundles have len > 1
    pub entries_ticks: Vec<(Entry, u64)>,
}

#[derive(Debug, Clone)]
pub struct BankStart {
    pub working_bank: Arc<Bank>,
    pub bank_creation_time: Arc<Instant>,
}

impl BankStart {
    fn get_working_bank_if_not_expired(&self) -> Option<&Bank> {
        if self.should_working_bank_still_be_processing_txs() {
            Some(&self.working_bank)
        } else {
            None
        }
    }

    pub fn should_working_bank_still_be_processing_txs(&self) -> bool {
        Bank::should_bank_still_be_processing_txs(
            &self.bank_creation_time,
            self.working_bank.ns_per_slot,
        )
    }
}

// Sends the Result of the record operation, including the index in the slot of the first
// transaction, if being tracked by WorkingBank
type RecordResultSender = Sender<Result<Option<usize>>>;

pub struct Record {
    // non-bundles shall have mixins_txs.len() == 1, bundles shall have mixins_txs.len() > 1
    pub mixins_txs: Vec<(Hash, Vec<VersionedTransaction>)>,
    pub slot: Slot,
    pub sender: RecordResultSender,
}
impl Record {
    pub fn new(
        mixins_txs: Vec<(Hash, Vec<VersionedTransaction>)>,
        slot: Slot,
        sender: RecordResultSender,
    ) -> Self {
        Self {
            mixins_txs,
            slot,
            sender,
        }
    }
}

#[derive(Default, Debug)]
pub struct RecordTransactionsTimings {
    pub processing_results_to_transactions_us: u64,
    pub hash_us: u64,
    pub poh_record_us: u64,
}

impl RecordTransactionsTimings {
    pub fn accumulate(&mut self, other: &RecordTransactionsTimings) {
        saturating_add_assign!(
            self.processing_results_to_transactions_us,
            other.processing_results_to_transactions_us
        );
        saturating_add_assign!(self.hash_us, other.hash_us);
        saturating_add_assign!(self.poh_record_us, other.poh_record_us);
    }
}

pub struct RecordTransactionsSummary {
    // Metrics describing how time was spent recording transactions
    pub record_transactions_timings: RecordTransactionsTimings,
    // Result of trying to record the transactions into the PoH stream
    pub result: Result<()>,
    // Index in the slot of the first transaction recorded
    pub starting_transaction_index: Option<usize>,
}

#[derive(Clone)]
pub struct TransactionRecorder {
    // shared by all users of PohRecorder
    pub record_sender: Sender<Record>,
    pub is_exited: Arc<AtomicBool>,
}

impl TransactionRecorder {
    pub fn new(record_sender: Sender<Record>, is_exited: Arc<AtomicBool>) -> Self {
        Self {
            record_sender,
            is_exited,
        }
    }

    /// Hashes `transactions` and sends to PoH service for recording. Waits for response up to 1s.
    /// Panics on unexpected (non-`MaxHeightReached`) errors.
    pub fn record_transactions(
        &self,
        bank_slot: Slot,
        batches: Vec<Vec<VersionedTransaction>>,
    ) -> RecordTransactionsSummary {
        let mut record_transactions_timings = RecordTransactionsTimings::default();
        let mut starting_transaction_index = None;

        if !batches.is_empty() && !batches.iter().any(|b| b.is_empty()) {
            let (hashes, hash_us) = measure_us!(batches
                .iter()
                .map(|b| hash_transactions(b))
                .collect::<Vec<_>>());
            record_transactions_timings.hash_us = hash_us;

            let hashes_transactions: Vec<_> = hashes.into_iter().zip(batches).collect();

            let (res, poh_record_us) = measure_us!(self.record(bank_slot, hashes_transactions));
            record_transactions_timings.poh_record_us = poh_record_us;

            match res {
                Ok(starting_index) => {
                    starting_transaction_index = starting_index;
                }
                Err(PohRecorderError::MaxHeightReached) => {
                    return RecordTransactionsSummary {
                        record_transactions_timings,
                        result: Err(PohRecorderError::MaxHeightReached),
                        starting_transaction_index: None,
                    };
                }
                Err(PohRecorderError::SendError(e)) => {
                    return RecordTransactionsSummary {
                        record_transactions_timings,
                        result: Err(PohRecorderError::SendError(e)),
                        starting_transaction_index: None,
                    };
                }
                Err(e) => panic!("Poh recorder returned unexpected error: {e:?}"),
            }
        }

        RecordTransactionsSummary {
            record_transactions_timings,
            result: Ok(()),
            starting_transaction_index,
        }
    }

    // Returns the index of `transactions.first()` in the slot, if being tracked by WorkingBank
    pub fn record(
        &self,
        bank_slot: Slot,
        mixins_txs: Vec<(Hash, Vec<VersionedTransaction>)>,
    ) -> Result<Option<usize>> {
        // create a new channel so that there is only 1 sender and when it goes out of scope, the receiver fails
        let (result_sender, result_receiver) = bounded(1);
        let res = self
            .record_sender
            .send(Record::new(mixins_txs, bank_slot, result_sender));
        if res.is_err() {
            // If the channel is dropped, then the validator is shutting down so return that we are hitting
            //  the max tick height to stop transaction processing and flush any transactions in the pipeline.
            return Err(PohRecorderError::MaxHeightReached);
        }
        // Besides validator exit, this timeout should primarily be seen to affect test execution environments where the various pieces can be shutdown abruptly
        let mut is_exited = false;
        loop {
            let res = result_receiver.recv_timeout(Duration::from_millis(1000));
            match res {
                Err(RecvTimeoutError::Timeout) => {
                    if is_exited {
                        return Err(PohRecorderError::MaxHeightReached);
                    } else {
                        // A result may have come in between when we timed out checking this
                        // bool, so check the channel again, even if is_exited == true
                        is_exited = self.is_exited.load(Ordering::SeqCst);
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    return Err(PohRecorderError::MaxHeightReached);
                }
                Ok(result) => {
                    return result;
                }
            }
        }
    }
}

pub enum PohRecorderBank {
    WorkingBank(BankStart),
    LastResetBank(Arc<Bank>),
}

impl PohRecorderBank {
    pub fn bank(&self) -> &Bank {
        match self {
            PohRecorderBank::WorkingBank(bank_start) => &bank_start.working_bank,
            PohRecorderBank::LastResetBank(last_reset_bank) => last_reset_bank,
        }
    }

    pub fn working_bank_start(&self) -> Option<&BankStart> {
        match self {
            PohRecorderBank::WorkingBank(bank_start) => Some(bank_start),
            PohRecorderBank::LastResetBank(_last_reset_bank) => None,
        }
    }
}

pub struct WorkingBank {
    pub bank: BankWithScheduler,
    pub start: Arc<Instant>,
    pub min_tick_height: u64,
    pub max_tick_height: u64,
    pub transaction_index: Option<usize>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum PohLeaderStatus {
    NotReached,
    Reached { poh_slot: Slot, parent_slot: Slot },
}

pub struct PohRecorder {
    pub poh: Arc<Mutex<Poh>>,
    tick_height: u64,
    clear_bank_signal: Option<Sender<bool>>,
    start_bank: Arc<Bank>, // parent slot
    start_bank_active_descendants: Vec<Slot>,
    start_tick_height: u64, // first tick_height this recorder will observe
    tick_cache: Vec<(Entry, u64)>, // cache of entry and its tick_height
    working_bank: Option<WorkingBank>,
    sender: Sender<WorkingBankEntry>,
    poh_timing_point_sender: Option<PohTimingSender>,
    leader_first_tick_height_including_grace_ticks: Option<u64>,
    leader_last_tick_height: u64, // zero if none
    grace_ticks: u64,
    blockstore: Arc<Blockstore>,
    leader_schedule_cache: Arc<LeaderScheduleCache>,
    ticks_per_slot: u64,
    target_ns_per_tick: u64,
    record_lock_contention_us: u64,
    flush_cache_no_tick_us: u64,
    flush_cache_tick_us: u64,
    send_entry_us: u64,
    tick_lock_contention_us: u64,
    total_sleep_us: u64,
    record_us: u64,
    report_metrics_us: u64,
    ticks_from_record: u64,
    last_metric: Instant,
    record_sender: Sender<Record>,
    leader_bank_notifier: Arc<LeaderBankNotifier>,
    delay_leader_block_for_pending_fork: bool,
    last_reported_slot_for_pending_fork: Arc<Mutex<Slot>>,
    pub is_exited: Arc<AtomicBool>,
}

impl PohRecorder {
    fn clear_bank(&mut self) {
        if let Some(WorkingBank { bank, start, .. }) = self.working_bank.take() {
            self.leader_bank_notifier.set_completed(bank.slot());
            let next_leader_slot = self.leader_schedule_cache.next_leader_slot(
                bank.collector_id(),
                bank.slot(),
                &bank,
                Some(&self.blockstore),
                GRACE_TICKS_FACTOR * MAX_GRACE_SLOTS,
            );
            assert_eq!(self.ticks_per_slot, bank.ticks_per_slot());
            let (
                leader_first_tick_height_including_grace_ticks,
                leader_last_tick_height,
                grace_ticks,
            ) = Self::compute_leader_slot_tick_heights(next_leader_slot, self.ticks_per_slot);
            self.grace_ticks = grace_ticks;
            self.leader_first_tick_height_including_grace_ticks =
                leader_first_tick_height_including_grace_ticks;
            self.leader_last_tick_height = leader_last_tick_height;

            datapoint_info!(
                "leader-slot-start-to-cleared-elapsed-ms",
                ("slot", bank.slot(), i64),
                ("elapsed", start.elapsed().as_millis(), i64),
            );
        }

        if let Some(ref signal) = self.clear_bank_signal {
            match signal.try_send(true) {
                Ok(_) => {}
                Err(TrySendError::Full(_)) => {
                    trace!("replay wake up signal channel is full.")
                }
                Err(TrySendError::Disconnected(_)) => {
                    trace!("replay wake up signal channel is disconnected.")
                }
            }
        }
    }

    pub fn would_be_leader(&self, within_next_n_ticks: u64) -> bool {
        self.has_bank()
            || self.leader_first_tick_height_including_grace_ticks.map_or(
                false,
                |leader_first_tick_height_including_grace_ticks| {
                    let ideal_leader_tick_height = leader_first_tick_height_including_grace_ticks
                        .saturating_sub(self.grace_ticks);
                    self.tick_height + within_next_n_ticks >= ideal_leader_tick_height
                        && self.tick_height <= self.leader_last_tick_height
                },
            )
    }

    // Return the slot for a given tick height
    fn slot_for_tick_height(&self, tick_height: u64) -> Slot {
        // We need to subtract by one here because, assuming ticks per slot is 64,
        // tick heights [1..64] correspond to slot 0. The last tick height of a slot
        // is always a multiple of 64.
        tick_height.saturating_sub(1) / self.ticks_per_slot
    }

    pub fn leader_after_n_slots(&self, slots: u64) -> Option<Pubkey> {
        let current_slot = self.slot_for_tick_height(self.tick_height);
        self.leader_schedule_cache
            .slot_leader_at(current_slot + slots, None)
    }

    /// Return the leader and slot pair after `slots_in_the_future` slots.
    pub fn leader_and_slot_after_n_slots(
        &self,
        slots_in_the_future: u64,
    ) -> Option<(Pubkey, Slot)> {
        let target_slot = self
            .slot_for_tick_height(self.tick_height)
            .checked_add(slots_in_the_future)?;
        self.leader_schedule_cache
            .slot_leader_at(target_slot, None)
            .map(|leader| (leader, target_slot))
    }

    pub fn next_slot_leader(&self) -> Option<Pubkey> {
        self.leader_after_n_slots(1)
    }

    pub fn bank(&self) -> Option<Arc<Bank>> {
        self.working_bank.as_ref().map(|w| w.bank.clone())
    }

    pub fn bank_start(&self) -> Option<BankStart> {
        self.working_bank.as_ref().map(|w| BankStart {
            working_bank: w.bank.clone(),
            bank_creation_time: w.start.clone(),
        })
    }

    pub fn working_bank_end_slot(&self) -> Option<Slot> {
        self.working_bank.as_ref().and_then(|w| {
            if w.max_tick_height == self.tick_height {
                Some(w.bank.slot())
            } else {
                None
            }
        })
    }

    pub fn working_slot(&self) -> Option<Slot> {
        self.working_bank.as_ref().map(|w| w.bank.slot())
    }

    pub fn has_bank(&self) -> bool {
        self.working_bank.is_some()
    }

    pub fn tick_height(&self) -> u64 {
        self.tick_height
    }

    pub fn ticks_per_slot(&self) -> u64 {
        self.ticks_per_slot
    }

    pub fn new_recorder(&self) -> TransactionRecorder {
        TransactionRecorder::new(self.record_sender.clone(), self.is_exited.clone())
    }

    pub fn new_leader_bank_notifier(&self) -> Arc<LeaderBankNotifier> {
        self.leader_bank_notifier.clone()
    }

    fn is_same_fork_as_previous_leader(&self, slot: Slot) -> bool {
        (slot.saturating_sub(NUM_CONSECUTIVE_LEADER_SLOTS)..slot).any(|slot| {
            // Check if the last slot Poh reset to was any of the
            // previous leader's slots.
            // If so, PoH is currently building on the previous leader's blocks
            // If not, PoH is building on a different fork
            slot == self.start_slot()
        })
    }

    fn start_slot_was_mine(&self, my_pubkey: &Pubkey) -> bool {
        self.start_bank.collector_id() == my_pubkey
    }

    // Active descendants of the last reset bank that are smaller than the
    // next leader slot could soon become the new reset bank.
    fn is_new_reset_bank_pending(&self, next_slot: Slot) -> bool {
        self.start_bank_active_descendants
            .iter()
            .any(|pending_slot| *pending_slot < next_slot)
    }

    fn can_skip_grace_ticks(&self, my_pubkey: &Pubkey) -> bool {
        let next_tick_height = self.tick_height.saturating_add(1);
        let next_slot = self.slot_for_tick_height(next_tick_height);

        if self.start_slot_was_mine(my_pubkey) {
            // Building off my own block. No need to wait.
            return true;
        }

        if self.is_same_fork_as_previous_leader(next_slot) {
            // Planning to build off block produced by the leader previous to
            // me. Need to wait.
            return false;
        }

        if !self.is_new_reset_bank_pending(next_slot) {
            // No pending blocks from previous leader have been observed. No
            // need to wait.
            return true;
        }

        self.report_pending_fork_was_detected(next_slot);
        if !self.delay_leader_block_for_pending_fork {
            // Not configured to wait for pending blocks from previous leader.
            return true;
        }

        // Wait for grace ticks
        false
    }

    fn reached_leader_tick(
        &self,
        my_pubkey: &Pubkey,
        leader_first_tick_height_including_grace_ticks: u64,
    ) -> bool {
        if self.start_tick_height + self.grace_ticks
            == leader_first_tick_height_including_grace_ticks
        {
            // PoH was reset to run immediately.
            return true;
        }

        let target_tick_height = leader_first_tick_height_including_grace_ticks.saturating_sub(1);
        if self.tick_height >= target_tick_height {
            // We have finished waiting for grace ticks.
            return true;
        }

        let ideal_target_tick_height = target_tick_height.saturating_sub(self.grace_ticks);
        if self.tick_height < ideal_target_tick_height {
            // We haven't ticked to our leader slot yet.
            return false;
        }

        // We're in the grace tick zone. Check if we can skip grace ticks.
        self.can_skip_grace_ticks(my_pubkey)
    }

    // Report metrics when poh recorder detects a pending fork that could
    // soon lead to poh reset.
    fn report_pending_fork_was_detected(&self, next_slot: Slot) {
        // Only report once per next leader slot to avoid spamming metrics. It's
        // enough to know that a leader decided to delay or not once per slot
        let mut last_slot = self.last_reported_slot_for_pending_fork.lock().unwrap();
        if *last_slot == next_slot {
            return;
        }
        *last_slot = next_slot;

        datapoint_info!(
            "poh_recorder-detected_pending_fork",
            ("next_leader_slot", next_slot, i64),
            (
                "did_delay_leader_slot",
                self.delay_leader_block_for_pending_fork,
                bool
            ),
        );
    }

    pub fn start_slot(&self) -> Slot {
        self.start_bank.slot()
    }

    /// Returns if the leader slot has been reached along with the current poh
    /// slot and the parent slot (could be a few slots ago if any previous
    /// leaders needed to be skipped).
    pub fn reached_leader_slot(&self, my_pubkey: &Pubkey) -> PohLeaderStatus {
        trace!(
            "tick_height {}, start_tick_height {}, leader_first_tick_height_including_grace_ticks {:?}, grace_ticks {}, has_bank {}",
            self.tick_height,
            self.start_tick_height,
            self.leader_first_tick_height_including_grace_ticks,
            self.grace_ticks,
            self.has_bank()
        );

        let next_tick_height = self.tick_height + 1;
        let next_poh_slot = self.slot_for_tick_height(next_tick_height);
        let Some(leader_first_tick_height_including_grace_ticks) =
            self.leader_first_tick_height_including_grace_ticks
        else {
            // No next leader slot, so no leader slot has been reached.
            return PohLeaderStatus::NotReached;
        };

        if !self.reached_leader_tick(my_pubkey, leader_first_tick_height_including_grace_ticks) {
            // PoH hasn't ticked far enough yet.
            return PohLeaderStatus::NotReached;
        }

        if self.blockstore.has_existing_shreds_for_slot(next_poh_slot) {
            // We already have existing shreds for this slot. This can happen when this block was previously
            // created and added to BankForks, however a recent PoH reset caused this bank to be removed
            // as it was not part of the rooted fork. If this slot is not the first slot for this leader,
            // and the first slot was previously ticked over, the check in `leader_schedule_cache::next_leader_slot`
            // will not suffice, as it only checks if there are shreds for the first slot.
            return PohLeaderStatus::NotReached;
        }

        assert!(next_tick_height >= self.start_tick_height);
        let poh_slot = next_poh_slot;
        let parent_slot = self.start_slot();
        PohLeaderStatus::Reached {
            poh_slot,
            parent_slot,
        }
    }

    // returns (leader_first_tick_height_including_grace_ticks, leader_last_tick_height, grace_ticks) given the next
    //  slot this recorder will lead
    fn compute_leader_slot_tick_heights(
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
    ) -> (Option<u64>, u64, u64) {
        next_leader_slot
            .map(|(first_slot, last_slot)| {
                let leader_first_tick_height = first_slot * ticks_per_slot + 1;
                let last_tick_height = (last_slot + 1) * ticks_per_slot;
                let num_slots = last_slot - first_slot + 1;
                let grace_ticks = cmp::min(
                    ticks_per_slot * MAX_GRACE_SLOTS,
                    ticks_per_slot * num_slots / GRACE_TICKS_FACTOR,
                );
                let leader_first_tick_height_including_grace_ticks =
                    leader_first_tick_height + grace_ticks;
                (
                    Some(leader_first_tick_height_including_grace_ticks),
                    last_tick_height,
                    grace_ticks,
                )
            })
            .unwrap_or((
                None,
                0,
                cmp::min(
                    ticks_per_slot * MAX_GRACE_SLOTS,
                    ticks_per_slot * NUM_CONSECUTIVE_LEADER_SLOTS / GRACE_TICKS_FACTOR,
                ),
            ))
    }

    fn reset_poh(&mut self, reset_bank: Arc<Bank>, reset_start_bank: bool) {
        let blockhash = reset_bank.last_blockhash();
        let poh_hash = {
            let mut poh = self.poh.lock().unwrap();
            poh.reset(blockhash, *reset_bank.hashes_per_tick());
            poh.hash
        };
        info!(
            "reset poh from: {},{},{} to: {},{}",
            poh_hash,
            self.tick_height,
            self.start_slot(),
            blockhash,
            reset_bank.slot()
        );

        self.tick_cache = vec![];
        if reset_start_bank {
            self.start_bank = reset_bank;
            self.start_bank_active_descendants = vec![];
        }
        self.tick_height = (self.start_slot() + 1) * self.ticks_per_slot;
        self.start_tick_height = self.tick_height + 1;
    }

    // update the list of active descendants of the start bank to make a better
    // decision about whether to use grace ticks
    pub fn update_start_bank_active_descendants(&mut self, active_descendants: &[Slot]) {
        self.start_bank_active_descendants = active_descendants.to_vec();
    }

    // synchronize PoH with a bank
    pub fn reset(&mut self, reset_bank: Arc<Bank>, next_leader_slot: Option<(Slot, Slot)>) {
        self.clear_bank();
        self.reset_poh(reset_bank, true);

        if let Some(ref sender) = self.poh_timing_point_sender {
            // start_slot() is the parent slot. current slot is start_slot() + 1.
            send_poh_timing_point(
                sender,
                SlotPohTimingInfo::new_slot_start_poh_time_point(
                    self.start_slot() + 1,
                    None,
                    solana_sdk::timing::timestamp(),
                ),
            );
        }

        let (leader_first_tick_height_including_grace_ticks, leader_last_tick_height, grace_ticks) =
            Self::compute_leader_slot_tick_heights(next_leader_slot, self.ticks_per_slot);
        self.grace_ticks = grace_ticks;
        self.leader_first_tick_height_including_grace_ticks =
            leader_first_tick_height_including_grace_ticks;
        self.leader_last_tick_height = leader_last_tick_height;
    }

    pub fn set_bank(&mut self, bank: BankWithScheduler, track_transaction_indexes: bool) {
        assert!(self.working_bank.is_none());
        self.leader_bank_notifier.set_in_progress(&bank);
        let working_bank = WorkingBank {
            min_tick_height: bank.tick_height(),
            max_tick_height: bank.max_tick_height(),
            bank,
            start: Arc::new(Instant::now()),
            transaction_index: track_transaction_indexes.then_some(0),
        };
        trace!("new working bank");
        assert_eq!(working_bank.bank.ticks_per_slot(), self.ticks_per_slot());
        if let Some(hashes_per_tick) = *working_bank.bank.hashes_per_tick() {
            if self.poh.lock().unwrap().hashes_per_tick() != hashes_per_tick {
                // We must clear/reset poh when changing hashes per tick because it's
                // possible there are ticks in the cache created with the old hashes per
                // tick value that would get flushed later. This would corrupt the leader's
                // block and it would be disregarded by the network.
                info!(
                    "resetting poh due to hashes per tick change detected at {}",
                    working_bank.bank.slot()
                );
                self.reset_poh(working_bank.bank.clone(), false);
            }
        }
        self.working_bank = Some(working_bank);

        // send poh slot start timing point
        if let Some(ref sender) = self.poh_timing_point_sender {
            if let Some(slot) = self.working_slot() {
                send_poh_timing_point(
                    sender,
                    SlotPohTimingInfo::new_slot_start_poh_time_point(
                        slot,
                        None,
                        solana_sdk::timing::timestamp(),
                    ),
                );
            }
        }

        // TODO: adjust the working_bank.start time based on number of ticks
        // that have already elapsed based on current tick height.
        let _ = self.flush_cache(false);
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn set_bank_for_test(&mut self, bank: Arc<Bank>) {
        self.set_bank(BankWithScheduler::new_without_scheduler(bank), false)
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn set_bank_with_transaction_index_for_test(&mut self, bank: Arc<Bank>) {
        self.set_bank(BankWithScheduler::new_without_scheduler(bank), true)
    }

    #[cfg(feature = "dev-context-only-utils")]
    pub fn clear_bank_for_test(&mut self) {
        self.clear_bank();
    }

    // Flush cache will delay flushing the cache for a bank until it past the WorkingBank::min_tick_height
    // On a record flush will flush the cache at the WorkingBank::min_tick_height, since a record
    // occurs after the min_tick_height was generated
    fn flush_cache(&mut self, tick: bool) -> Result<()> {
        // check_tick_height is called before flush cache, so it cannot overrun the bank
        // so a bank that is so late that it's slot fully generated before it starts recording
        // will fail instead of broadcasting any ticks
        let working_bank = self
            .working_bank
            .as_ref()
            .ok_or(PohRecorderError::MaxHeightReached)?;
        if self.tick_height < working_bank.min_tick_height {
            return Err(PohRecorderError::MinHeightNotReached);
        }
        if tick && self.tick_height == working_bank.min_tick_height {
            return Err(PohRecorderError::MinHeightNotReached);
        }

        let entry_count = self
            .tick_cache
            .iter()
            .take_while(|x| x.1 <= working_bank.max_tick_height)
            .count();
        let mut send_result: std::result::Result<(), SendError<WorkingBankEntry>> = Ok(());

        if entry_count > 0 {
            trace!(
                "flush_cache: bank_slot: {} tick_height: {} max: {} sending: {}",
                working_bank.bank.slot(),
                working_bank.bank.tick_height(),
                working_bank.max_tick_height,
                entry_count,
            );

            for tick in &self.tick_cache[..entry_count] {
                working_bank.bank.register_tick(&tick.0.hash);
                send_result = self.sender.send(WorkingBankEntry {
                    bank: working_bank.bank.clone(),
                    entries_ticks: vec![tick.clone()],
                });
                if send_result.is_err() {
                    break;
                }
            }
        }
        if self.tick_height >= working_bank.max_tick_height {
            info!(
                "poh_record: max_tick_height {} reached, clearing working_bank {}",
                working_bank.max_tick_height,
                working_bank.bank.slot()
            );
            self.start_bank = working_bank.bank.clone();
            let working_slot = self.start_slot();
            self.start_tick_height = working_slot * self.ticks_per_slot + 1;
            self.clear_bank();
        }
        if send_result.is_err() {
            info!("WorkingBank::sender disconnected {:?}", send_result);
            // revert the cache, but clear the working bank
            self.clear_bank();
        } else {
            // commit the flush
            let _ = self.tick_cache.drain(..entry_count);
        }

        Ok(())
    }

    fn report_poh_timing_point_by_tick(&self) {
        match self.tick_height % self.ticks_per_slot {
            // reaching the end of the slot
            0 => {
                if let Some(ref sender) = self.poh_timing_point_sender {
                    send_poh_timing_point(
                        sender,
                        SlotPohTimingInfo::new_slot_end_poh_time_point(
                            self.slot_for_tick_height(self.tick_height),
                            None,
                            solana_sdk::timing::timestamp(),
                        ),
                    );
                }
            }
            // beginning of a slot
            1 => {
                if let Some(ref sender) = self.poh_timing_point_sender {
                    send_poh_timing_point(
                        sender,
                        SlotPohTimingInfo::new_slot_start_poh_time_point(
                            self.slot_for_tick_height(self.tick_height),
                            None,
                            solana_sdk::timing::timestamp(),
                        ),
                    );
                }
            }
            _ => {}
        }
    }

    fn report_poh_timing_point_by_working_bank(&self, slot: Slot) {
        if let Some(ref sender) = self.poh_timing_point_sender {
            send_poh_timing_point(
                sender,
                SlotPohTimingInfo::new_slot_end_poh_time_point(
                    slot,
                    None,
                    solana_sdk::timing::timestamp(),
                ),
            );
        }
    }

    fn report_poh_timing_point(&self) {
        // send poh slot end timing point
        if let Some(slot) = self.working_bank_end_slot() {
            //  bank producer
            self.report_poh_timing_point_by_working_bank(slot)
        } else {
            // validator
            self.report_poh_timing_point_by_tick()
        }
    }

    pub fn tick(&mut self) {
        let ((poh_entry, target_time), tick_lock_contention_us) = measure_us!({
            let mut poh_l = self.poh.lock().unwrap();
            let poh_entry = poh_l.tick();
            let target_time = if poh_entry.is_some() {
                Some(poh_l.target_poh_time(self.target_ns_per_tick))
            } else {
                None
            };
            (poh_entry, target_time)
        });
        self.tick_lock_contention_us += tick_lock_contention_us;

        if let Some(poh_entry) = poh_entry {
            self.tick_height += 1;
            trace!("tick_height {}", self.tick_height);
            self.report_poh_timing_point();

            if self
                .leader_first_tick_height_including_grace_ticks
                .is_none()
            {
                return;
            }

            self.tick_cache.push((
                Entry {
                    num_hashes: poh_entry.num_hashes,
                    hash: poh_entry.hash,
                    transactions: vec![],
                },
                self.tick_height,
            ));

            let (_flush_res, flush_cache_and_tick_us) = measure_us!(self.flush_cache(true));
            self.flush_cache_tick_us += flush_cache_and_tick_us;

            let (_, sleep_us) = measure_us!({
                let target_time = target_time.unwrap();
                // sleep is not accurate enough to get a predictable time.
                // Kernel can not schedule the thread for a while.
                while Instant::now() < target_time {
                    // TODO: a caller could possibly desire to reset or record while we're spinning here
                    std::hint::spin_loop();
                }
            });
            self.total_sleep_us += sleep_us;
        }
    }

    fn report_metrics(&mut self, bank_slot: Slot) {
        if self.last_metric.elapsed().as_millis() > 1000 {
            datapoint_info!(
                "poh_recorder",
                ("slot", bank_slot, i64),
                ("tick_lock_contention", self.tick_lock_contention_us, i64),
                ("record_us", self.record_us, i64),
                ("flush_cache_no_tick_us", self.flush_cache_no_tick_us, i64),
                ("flush_cache_tick_us", self.flush_cache_tick_us, i64),
                ("send_entry_us", self.send_entry_us, i64),
                ("ticks_from_record", self.ticks_from_record, i64),
                ("total_sleep_us", self.total_sleep_us, i64),
                (
                    "record_lock_contention_us",
                    self.record_lock_contention_us,
                    i64
                ),
                ("report_metrics_us", self.report_metrics_us, i64),
            );

            self.tick_lock_contention_us = 0;
            self.record_us = 0;
            self.total_sleep_us = 0;
            self.record_lock_contention_us = 0;
            self.flush_cache_no_tick_us = 0;
            self.flush_cache_tick_us = 0;
            self.send_entry_us = 0;
            self.ticks_from_record = 0;
            self.report_metrics_us = 0;
            self.last_metric = Instant::now();
        }
    }

    // Returns the index of `transactions.first()` in the slot, if being tracked by WorkingBank
    pub fn record(
        &mut self,
        bank_slot: Slot,
        mixins_txs: &[(Hash, Vec<VersionedTransaction>)],
    ) -> Result<Option<usize>> {
        // Entries without transactions are used to track real-time passing in the ledger and
        // cannot be generated by `record()`
        assert!(!mixins_txs.is_empty(), "No transactions provided");
        assert!(
            !mixins_txs.iter().any(|(_, txs)| txs.is_empty()),
            "One of mixins is missing txs"
        );

        let ((), report_metrics_us) = measure_us!(self.report_metrics(bank_slot));
        self.report_metrics_us += report_metrics_us;

        let mixins: Vec<Hash> = mixins_txs.iter().map(|(m, _)| *m).collect();
        let transactions: Vec<Vec<VersionedTransaction>> =
            mixins_txs.iter().map(|(_, tx)| tx.clone()).collect();

        loop {
            let (flush_cache_res, flush_cache_us) = measure_us!(self.flush_cache(false));
            self.flush_cache_no_tick_us += flush_cache_us;
            flush_cache_res?;

            let working_bank = self
                .working_bank
                .as_mut()
                .ok_or(PohRecorderError::MaxHeightReached)?;
            if bank_slot != working_bank.bank.slot() {
                return Err(PohRecorderError::MaxHeightReached);
            }

            let (mut poh_lock, poh_lock_us) = measure_us!(self.poh.lock().unwrap());
            self.record_lock_contention_us += poh_lock_us;

            let (maybe_entries, record_mixin_time_us) =
                measure_us!(poh_lock.record_bundle(&mixins));
            self.record_us += record_mixin_time_us;

            drop(poh_lock);

            if let Some(entries) = maybe_entries {
                assert_eq!(entries.len(), transactions.len());
                let num_transactions = transactions.iter().map(|txs| txs.len()).sum();
                let (send_entry_res, send_entry_time_us) = measure_us!({
                    let entries_tick_heights: Vec<(Entry, u64)> = entries
                        .into_iter()
                        .zip(transactions.into_iter())
                        .map(|(poh_entry, transactions)| {
                            (
                                Entry {
                                    num_hashes: poh_entry.num_hashes,
                                    hash: poh_entry.hash,
                                    transactions,
                                },
                                self.tick_height,
                            )
                        })
                        .collect();
                    let bank_clone = working_bank.bank.clone();
                    self.sender.send(WorkingBankEntry {
                        bank: bank_clone,
                        entries_ticks: entries_tick_heights,
                    })
                });
                self.send_entry_us += send_entry_time_us;
                send_entry_res?;
                let starting_transaction_index =
                    working_bank.transaction_index.inspect(|transaction_index| {
                        let next_starting_transaction_index =
                            transaction_index.saturating_add(num_transactions);
                        working_bank.transaction_index = Some(next_starting_transaction_index);
                    });
                return Ok(starting_transaction_index);
            }

            // record() might fail if the next PoH hash needs to be a tick.  But that's ok, tick()
            // and re-record()
            self.ticks_from_record += 1;
            self.tick();
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_clear_signal(
        tick_height: u64,
        last_entry_hash: Hash,
        start_bank: Arc<Bank>,
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
        delay_leader_block_for_pending_fork: bool,
        blockstore: Arc<Blockstore>,
        clear_bank_signal: Option<Sender<bool>>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        poh_config: &PohConfig,
        poh_timing_point_sender: Option<PohTimingSender>,
        is_exited: Arc<AtomicBool>,
    ) -> (Self, Receiver<WorkingBankEntry>, Receiver<Record>) {
        let tick_number = 0;
        let poh = Arc::new(Mutex::new(Poh::new_with_slot_info(
            last_entry_hash,
            poh_config.hashes_per_tick,
            tick_number,
        )));

        let target_ns_per_tick = PohService::target_ns_per_tick(
            ticks_per_slot,
            poh_config.target_tick_duration.as_nanos() as u64,
        );
        let (sender, receiver) = unbounded();
        let (record_sender, record_receiver) = unbounded();
        let (leader_first_tick_height_including_grace_ticks, leader_last_tick_height, grace_ticks) =
            Self::compute_leader_slot_tick_heights(next_leader_slot, ticks_per_slot);
        (
            Self {
                poh,
                tick_height,
                tick_cache: vec![],
                working_bank: None,
                sender,
                poh_timing_point_sender,
                clear_bank_signal,
                start_bank,
                start_bank_active_descendants: vec![],
                start_tick_height: tick_height + 1,
                leader_first_tick_height_including_grace_ticks,
                leader_last_tick_height,
                grace_ticks,
                blockstore,
                leader_schedule_cache: leader_schedule_cache.clone(),
                ticks_per_slot,
                target_ns_per_tick,
                record_lock_contention_us: 0,
                flush_cache_tick_us: 0,
                flush_cache_no_tick_us: 0,
                send_entry_us: 0,
                tick_lock_contention_us: 0,
                record_us: 0,
                report_metrics_us: 0,
                total_sleep_us: 0,
                ticks_from_record: 0,
                last_metric: Instant::now(),
                record_sender,
                leader_bank_notifier: Arc::default(),
                delay_leader_block_for_pending_fork,
                last_reported_slot_for_pending_fork: Arc::default(),
                is_exited,
            },
            receiver,
            record_receiver,
        )
    }

    /// A recorder to synchronize PoH with the following data structures
    /// * bank - the LastId's queue is updated on `tick` and `record` events
    /// * sender - the Entry channel that outputs to the ledger
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        tick_height: u64,
        last_entry_hash: Hash,
        start_bank: Arc<Bank>,
        next_leader_slot: Option<(Slot, Slot)>,
        ticks_per_slot: u64,
        blockstore: Arc<Blockstore>,
        leader_schedule_cache: &Arc<LeaderScheduleCache>,
        poh_config: &PohConfig,
        is_exited: Arc<AtomicBool>,
    ) -> (Self, Receiver<WorkingBankEntry>, Receiver<Record>) {
        let delay_leader_block_for_pending_fork = false;
        Self::new_with_clear_signal(
            tick_height,
            last_entry_hash,
            start_bank,
            next_leader_slot,
            ticks_per_slot,
            delay_leader_block_for_pending_fork,
            blockstore,
            None,
            leader_schedule_cache,
            poh_config,
            None,
            is_exited,
        )
    }

    pub fn get_poh_recorder_bank(&self) -> PohRecorderBank {
        let bank_start = self.bank_start();
        if let Some(bank_start) = bank_start {
            PohRecorderBank::WorkingBank(bank_start)
        } else {
            PohRecorderBank::LastResetBank(self.start_bank.clone())
        }
    }

    // Filters the return result of PohRecorder::bank_start(), returns the bank
    // if it's still processing transactions
    pub fn get_working_bank_if_not_expired<'a>(
        bank_start: &Option<&'a BankStart>,
    ) -> Option<&'a Bank> {
        bank_start
            .as_ref()
            .and_then(|bank_start| bank_start.get_working_bank_if_not_expired())
    }

    // Used in tests
    #[cfg(feature = "dev-context-only-utils")]
    pub fn schedule_dummy_max_height_reached_failure(&mut self) {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        self.reset(bank, None);
    }
}

pub fn create_test_recorder(
    bank: Arc<Bank>,
    blockstore: Arc<Blockstore>,
    poh_config: Option<PohConfig>,
    leader_schedule_cache: Option<Arc<LeaderScheduleCache>>,
) -> (
    Arc<AtomicBool>,
    Arc<RwLock<PohRecorder>>,
    PohService,
    Receiver<WorkingBankEntry>,
) {
    let leader_schedule_cache = match leader_schedule_cache {
        Some(provided_cache) => provided_cache,
        None => Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
    };
    let exit = Arc::new(AtomicBool::new(false));
    let poh_config = poh_config.unwrap_or_default();
    let (mut poh_recorder, entry_receiver, record_receiver) = PohRecorder::new(
        bank.tick_height(),
        bank.last_blockhash(),
        bank.clone(),
        Some((4, 4)),
        bank.ticks_per_slot(),
        blockstore,
        &leader_schedule_cache,
        &poh_config,
        exit.clone(),
    );
    let ticks_per_slot = bank.ticks_per_slot();

    poh_recorder.set_bank(BankWithScheduler::new_without_scheduler(bank), false);
    let poh_recorder = Arc::new(RwLock::new(poh_recorder));
    let poh_service = PohService::new(
        poh_recorder.clone(),
        &poh_config,
        exit.clone(),
        ticks_per_slot,
        crate::poh_service::DEFAULT_PINNED_CPU_CORE,
        crate::poh_service::DEFAULT_HASHES_PER_BATCH,
        record_receiver,
    );

    (exit, poh_recorder, poh_service, entry_receiver)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        bincode::serialize,
        crossbeam_channel::bounded,
        solana_ledger::{
            blockstore::Blockstore, blockstore_meta::SlotMeta, get_tmp_ledger_path_auto_delete,
        },
        solana_perf::test_tx::test_tx,
        solana_sdk::{clock::DEFAULT_TICKS_PER_SLOT, hash::hash},
    };

    #[test]
    fn test_poh_recorder_no_zero_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank,
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 1);
        assert_eq!(poh_recorder.tick_cache[0].1, 1);
        assert_eq!(poh_recorder.tick_height, 1);
    }

    #[test]
    fn test_poh_recorder_tick_height_is_last_tick() {
        let prev_hash = Hash::default();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");

        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank,
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 2);
        assert_eq!(poh_recorder.tick_cache[1].1, 2);
        assert_eq!(poh_recorder.tick_height, 2);
    }

    #[test]
    fn test_poh_recorder_reset_clears_cache() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            Hash::default(),
            bank0.clone(),
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 1);
        poh_recorder.reset(bank0, Some((4, 4)));
        assert_eq!(poh_recorder.tick_cache.len(), 0);
    }

    #[test]
    fn test_poh_recorder_clear() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_for_test(bank);
        assert!(poh_recorder.working_bank.is_some());
        poh_recorder.clear_bank();
        assert!(poh_recorder.working_bank.is_none());
    }

    #[test]
    fn test_poh_recorder_tick_sent_after_min() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank0.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank0.clone(),
            Some((4, 4)),
            bank0.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank0)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        bank0.fill_bank_with_ticks_for_tests();
        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));

        // Set a working bank
        poh_recorder.set_bank_for_test(bank1.clone());

        // Tick until poh_recorder.tick_height == working bank's min_tick_height
        let num_new_ticks = bank1.tick_height() - poh_recorder.tick_height();
        println!("{} {}", bank1.tick_height(), poh_recorder.tick_height());
        assert!(num_new_ticks > 0);
        for _ in 0..num_new_ticks {
            poh_recorder.tick();
        }

        // Check that poh_recorder.tick_height == working bank's min_tick_height
        let min_tick_height = poh_recorder.working_bank.as_ref().unwrap().min_tick_height;
        assert_eq!(min_tick_height, bank1.tick_height());
        assert_eq!(poh_recorder.tick_height(), min_tick_height);

        //poh_recorder.tick height == working bank's min_tick_height,
        // so no ticks should have been flushed yet
        assert_eq!(poh_recorder.tick_cache.last().unwrap().1, num_new_ticks);
        assert!(entry_receiver.try_recv().is_err());

        // all ticks are sent after height > min
        let tick_height_before = poh_recorder.tick_height();
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_height, tick_height_before + 1);
        assert_eq!(poh_recorder.tick_cache.len(), 0);
        let mut num_entries = 0;
        while let Ok(WorkingBankEntry {
            bank: wbank,
            entries_ticks: _,
        }) = entry_receiver.try_recv()
        {
            assert_eq!(wbank.slot(), bank1.slot());
            num_entries += 1;
        }

        // All the cached ticks, plus the new tick above should have been flushed
        assert_eq!(num_entries, num_new_ticks + 1);
    }

    #[test]
    fn test_poh_recorder_tick_sent_upto_and_including_max() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        // Tick further than the bank's max height
        for _ in 0..bank.max_tick_height() + 1 {
            poh_recorder.tick();
        }
        assert_eq!(
            poh_recorder.tick_cache.last().unwrap().1,
            bank.max_tick_height() + 1
        );
        assert_eq!(poh_recorder.tick_height, bank.max_tick_height() + 1);

        poh_recorder.set_bank_for_test(bank.clone());
        poh_recorder.tick();

        assert_eq!(poh_recorder.tick_height, bank.max_tick_height() + 2);
        assert!(poh_recorder.working_bank.is_none());
        let mut num_entries = 0;
        while entry_receiver.try_recv().is_ok() {
            num_entries += 1;
        }

        // Should only flush up to bank's max tick height, despite the tick cache
        // having many more entries
        assert_eq!(num_entries, bank.max_tick_height());
    }

    #[test]
    fn test_poh_recorder_record_to_early() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank0.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank0.clone(),
            Some((4, 4)),
            bank0.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank0)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        bank0.fill_bank_with_ticks_for_tests();
        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
        poh_recorder.set_bank_for_test(bank1.clone());
        // Let poh_recorder tick up to bank1.tick_height() - 1
        for _ in 0..bank1.tick_height() - 1 {
            poh_recorder.tick()
        }
        let tx = test_tx();
        let h1 = hash(b"hello world!");

        // We haven't yet reached the minimum tick height for the working bank,
        // so record should fail
        assert_matches!(
            poh_recorder.record(bank1.slot(), &[(h1, vec![tx.into()])]),
            Err(PohRecorderError::MinHeightNotReached)
        );
        assert!(entry_receiver.try_recv().is_err());
    }

    #[test]
    fn test_poh_recorder_record_bad_slot() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_for_test(bank.clone());
        let tx = test_tx();
        let h1 = hash(b"hello world!");

        // Fulfills min height criteria for a successful record
        assert_eq!(
            poh_recorder.tick_height(),
            poh_recorder.working_bank.as_ref().unwrap().min_tick_height
        );

        // However we hand over a bad slot so record fails
        let bad_slot = bank.slot() + 1;
        assert_matches!(
            poh_recorder.record(bad_slot, &[(h1, vec![tx.into()])]),
            Err(PohRecorderError::MaxHeightReached)
        );
    }

    #[test]
    fn test_poh_recorder_record_at_min_passes() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank0.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank0.clone(),
            Some((4, 4)),
            bank0.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank0)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        bank0.fill_bank_with_ticks_for_tests();
        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
        poh_recorder.set_bank_for_test(bank1.clone());

        // Record up to exactly min tick height
        let min_tick_height = poh_recorder.working_bank.as_ref().unwrap().min_tick_height;
        while poh_recorder.tick_height() < min_tick_height {
            poh_recorder.tick();
        }

        assert_eq!(poh_recorder.tick_cache.len() as u64, min_tick_height);

        // Check record succeeds on boundary condition where
        // poh_recorder.tick height == poh_recorder.working_bank.min_tick_height
        assert_eq!(poh_recorder.tick_height, min_tick_height);
        let tx = test_tx();
        let h1 = hash(b"hello world!");
        assert!(poh_recorder
            .record(bank1.slot(), &[(h1, vec![tx.into()])])
            .is_ok());
        assert_eq!(poh_recorder.tick_cache.len(), 0);

        //tick in the cache + entry
        for _ in 0..min_tick_height {
            let WorkingBankEntry {
                bank: _,
                entries_ticks,
            } = entry_receiver.recv().unwrap();
            assert_eq!(entries_ticks.len(), 1);
            assert!(entries_ticks[0].0.is_tick());
        }

        let WorkingBankEntry {
            bank: _,
            entries_ticks,
        } = entry_receiver.recv().unwrap();
        assert_eq!(entries_ticks.len(), 1);
        assert!(!entries_ticks[0].0.is_tick());
    }

    #[test]
    fn test_poh_recorder_record_at_max_fails() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_for_test(bank.clone());
        let num_ticks_to_max = bank.max_tick_height() - poh_recorder.tick_height;
        for _ in 0..num_ticks_to_max {
            poh_recorder.tick();
        }
        let tx = test_tx();
        let h1 = hash(b"hello world!");
        assert!(poh_recorder
            .record(bank.slot(), &[(h1, vec![tx.into()])])
            .is_err());
        for _ in 0..num_ticks_to_max {
            let WorkingBankEntry {
                bank: _,
                entries_ticks,
            } = entry_receiver.recv().unwrap();
            assert_eq!(entries_ticks.len(), 1);
            assert!(entries_ticks[0].0.is_tick());
        }
    }

    #[test]
    fn test_poh_recorder_record_transaction_index() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_with_transaction_index_for_test(bank.clone());
        poh_recorder.tick();
        assert_eq!(
            poh_recorder
                .working_bank
                .as_ref()
                .unwrap()
                .transaction_index
                .unwrap(),
            0
        );

        let tx0 = test_tx();
        let tx1 = test_tx();
        let h1 = hash(b"hello world!");
        let record_result = poh_recorder
            .record(bank.slot(), &[(h1, vec![tx0.into(), tx1.into()])])
            .unwrap()
            .unwrap();
        assert_eq!(record_result, 0);
        assert_eq!(
            poh_recorder
                .working_bank
                .as_ref()
                .unwrap()
                .transaction_index
                .unwrap(),
            2
        );

        let tx = test_tx();
        let h2 = hash(b"foobar");
        let record_result = poh_recorder
            .record(bank.slot(), &[(h2, vec![tx.into()])])
            .unwrap()
            .unwrap();
        assert_eq!(record_result, 2);
        assert_eq!(
            poh_recorder
                .working_bank
                .as_ref()
                .unwrap()
                .transaction_index
                .unwrap(),
            3
        );
    }

    #[test]
    fn test_poh_cache_on_disconnect() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank0.last_blockhash();
        let (mut poh_recorder, entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank0.clone(),
            Some((4, 4)),
            bank0.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank0)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        bank0.fill_bank_with_ticks_for_tests();
        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
        poh_recorder.set_bank_for_test(bank1);

        // Check we can make two ticks without hitting min_tick_height
        let remaining_ticks_to_min = poh_recorder.working_bank.as_ref().unwrap().min_tick_height
            - poh_recorder.tick_height();
        for _ in 0..remaining_ticks_to_min {
            poh_recorder.tick();
        }
        assert_eq!(poh_recorder.tick_height, remaining_ticks_to_min);
        assert_eq!(
            poh_recorder.tick_cache.len(),
            remaining_ticks_to_min as usize
        );
        assert!(poh_recorder.working_bank.is_some());

        // Drop entry receiver, and try to tick again. Because
        // the receiver is closed, the ticks will not be drained from the cache,
        // and the working bank will be cleared
        drop(entry_receiver);
        poh_recorder.tick();

        // Check everything is cleared
        assert!(poh_recorder.working_bank.is_none());
        // Extra +1 for the tick that happened after the drop of the entry receiver.
        assert_eq!(
            poh_recorder.tick_cache.len(),
            remaining_ticks_to_min as usize + 1
        );
    }

    #[test]
    fn test_reset_current() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            Hash::default(),
            bank.clone(),
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 2);
        poh_recorder.reset(bank, Some((4, 4)));
        assert_eq!(poh_recorder.tick_cache.len(), 0);
    }

    #[test]
    fn test_reset_with_cached() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            Hash::default(),
            bank.clone(),
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 2);
        poh_recorder.reset(bank, Some((4, 4)));
        assert_eq!(poh_recorder.tick_cache.len(), 0);
    }

    #[test]
    fn test_reset_to_new_value() {
        solana_logger::setup();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            Hash::default(),
            bank.clone(),
            Some((4, 4)),
            DEFAULT_TICKS_PER_SLOT,
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::default()),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        poh_recorder.tick();
        poh_recorder.tick();
        poh_recorder.tick();
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_cache.len(), 4);
        assert_eq!(poh_recorder.tick_height, 4);
        poh_recorder.reset(bank, Some((4, 4))); // parent slot 0 implies tick_height of 3
        assert_eq!(poh_recorder.tick_cache.len(), 0);
        poh_recorder.tick();
        assert_eq!(poh_recorder.tick_height, DEFAULT_TICKS_PER_SLOT + 1);
    }

    #[test]
    fn test_reset_clear_bank() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            Hash::default(),
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_for_test(bank.clone());
        assert_eq!(bank.slot(), 0);
        poh_recorder.reset(bank, Some((4, 4)));
        assert!(poh_recorder.working_bank.is_none());
    }

    #[test]
    pub fn test_clear_signal() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let (sender, receiver) = bounded(1);
        let (mut poh_recorder, _entry_receiver, _record_receiver) =
            PohRecorder::new_with_clear_signal(
                0,
                Hash::default(),
                bank.clone(),
                None,
                bank.ticks_per_slot(),
                false,
                Arc::new(blockstore),
                Some(sender),
                &Arc::new(LeaderScheduleCache::default()),
                &PohConfig::default(),
                None,
                Arc::new(AtomicBool::default()),
            );
        poh_recorder.set_bank_for_test(bank);
        poh_recorder.clear_bank();
        assert!(receiver.try_recv().is_ok());
    }

    #[test]
    fn test_poh_recorder_record_sets_start_slot() {
        solana_logger::setup();
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let ticks_per_slot = 5;
        let GenesisConfigInfo {
            mut genesis_config, ..
        } = create_genesis_config(2);
        genesis_config.ticks_per_slot = ticks_per_slot;
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            Some((4, 4)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        poh_recorder.set_bank_for_test(bank.clone());

        // Simulate ticking much further than working_bank.max_tick_height
        let max_tick_height = poh_recorder.working_bank.as_ref().unwrap().max_tick_height;
        for _ in 0..3 * max_tick_height {
            poh_recorder.tick();
        }

        let tx = test_tx();
        let h1 = hash(b"hello world!");
        assert!(poh_recorder
            .record(bank.slot(), &[(h1, vec![tx.into()])])
            .is_err());
        assert!(poh_recorder.working_bank.is_none());

        // Even thought we ticked much further than working_bank.max_tick_height,
        // the `start_slot` is still the slot of the last workign bank set by
        // the earlier call to `poh_recorder.set_bank()`
        assert_eq!(poh_recorder.start_slot(), bank.slot());
    }

    #[test]
    fn test_reached_leader_tick() {
        solana_logger::setup();

        // Setup genesis.
        let GenesisConfigInfo {
            genesis_config,
            validator_pubkey,
            ..
        } = create_genesis_config(2);

        // Setup start bank.
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();

        // Setup leader schedule.
        let leader_a_pubkey = validator_pubkey;
        let leader_b_pubkey = Pubkey::new_unique();
        let leader_c_pubkey = Pubkey::new_unique();
        let consecutive_leader_slots = NUM_CONSECUTIVE_LEADER_SLOTS as usize;
        let mut slot_leaders = Vec::with_capacity(consecutive_leader_slots * 3);
        slot_leaders.extend(std::iter::repeat(leader_a_pubkey).take(consecutive_leader_slots));
        slot_leaders.extend(std::iter::repeat(leader_b_pubkey).take(consecutive_leader_slots));
        slot_leaders.extend(std::iter::repeat(leader_c_pubkey).take(consecutive_leader_slots));
        let mut leader_schedule_cache = LeaderScheduleCache::new_from_bank(&bank);
        let fixed_schedule = solana_ledger::leader_schedule::FixedSchedule {
            leader_schedule: Arc::new(
                solana_ledger::leader_schedule::LeaderSchedule::new_from_schedule(slot_leaders),
            ),
        };
        leader_schedule_cache.set_fixed_leader_schedule(Some(fixed_schedule));

        // Setup PoH recorder.
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            None,
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(leader_schedule_cache),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        let grace_ticks = bank.ticks_per_slot() * MAX_GRACE_SLOTS;
        poh_recorder.grace_ticks = grace_ticks;

        // Setup leader start ticks.
        let ticks_in_leader_slot_set = bank.ticks_per_slot() * NUM_CONSECUTIVE_LEADER_SLOTS;
        let leader_a_start_tick = 0;
        let leader_b_start_tick = leader_a_start_tick + ticks_in_leader_slot_set;
        let leader_c_start_tick = leader_b_start_tick + ticks_in_leader_slot_set;

        // True, because we've ticked through all the grace ticks
        assert!(poh_recorder.reached_leader_tick(&leader_a_pubkey, leader_a_start_tick));

        // True, because from Leader A's perspective, the previous slot was also
        // it's own slot, and validators don't give grace periods if previous
        // slot was also their own.
        assert!(
            poh_recorder.reached_leader_tick(&leader_a_pubkey, leader_a_start_tick + grace_ticks)
        );

        // False, because we haven't ticked to our slot yet.
        assert!(!poh_recorder.reached_leader_tick(&leader_b_pubkey, leader_b_start_tick));

        // Tick through Leader A's slots.
        for _ in 0..ticks_in_leader_slot_set {
            poh_recorder.tick();
        }

        // False, because the Poh was reset on slot 0, which is a block produced
        // by previous leader A, so a grace period must be given.
        assert!(
            !poh_recorder.reached_leader_tick(&leader_b_pubkey, leader_b_start_tick + grace_ticks)
        );

        // Tick through Leader B's grace period.
        for _ in 0..grace_ticks {
            poh_recorder.tick();
        }

        // True, because we've ticked through all the grace ticks
        assert!(
            poh_recorder.reached_leader_tick(&leader_b_pubkey, leader_b_start_tick + grace_ticks)
        );

        // Tick through Leader B's remaining slots.
        for _ in 0..ticks_in_leader_slot_set - grace_ticks {
            poh_recorder.tick();
        }

        // True, because Leader C is not building on any of Leader B's slots.
        // The Poh was reset on slot 0, built by Leader A.
        assert!(
            poh_recorder.reached_leader_tick(&leader_c_pubkey, leader_c_start_tick + grace_ticks)
        );

        // Add some active (partially received) blocks to the active fork.
        let active_descendants = vec![NUM_CONSECUTIVE_LEADER_SLOTS];
        poh_recorder.update_start_bank_active_descendants(&active_descendants);

        // True, because there are pending blocks from Leader B on the active
        // fork, but the config to delay for these is not set.
        assert!(
            poh_recorder.reached_leader_tick(&leader_c_pubkey, leader_c_start_tick + grace_ticks)
        );

        // Flip the config to delay for pending blocks.
        poh_recorder.delay_leader_block_for_pending_fork = true;

        // False, because there are pending blocks from Leader B on the active
        // fork, and the config to delay for these is set.
        assert!(
            !poh_recorder.reached_leader_tick(&leader_c_pubkey, leader_c_start_tick + grace_ticks)
        );
    }

    #[test]
    fn test_reached_leader_slot() {
        solana_logger::setup();

        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");

        let GenesisConfigInfo {
            genesis_config,
            validator_pubkey,
            ..
        } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank0.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank0.clone(),
            None,
            bank0.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank0)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        // Test that with no next leader slot, we don't reach the leader slot
        assert_eq!(
            poh_recorder.reached_leader_slot(&validator_pubkey),
            PohLeaderStatus::NotReached
        );

        // Test that with no next leader slot in reset(), we don't reach the leader slot
        assert_eq!(bank0.slot(), 0);
        poh_recorder.reset(bank0.clone(), None);
        assert_eq!(
            poh_recorder.reached_leader_slot(&validator_pubkey),
            PohLeaderStatus::NotReached
        );

        // Provide a leader slot one slot down
        poh_recorder.reset(bank0.clone(), Some((2, 2)));

        let init_ticks = poh_recorder.tick_height();

        // Send one slot worth of ticks
        for _ in 0..bank0.ticks_per_slot() {
            poh_recorder.tick();
        }

        // Tick should be recorded
        assert_eq!(
            poh_recorder.tick_height(),
            init_ticks + bank0.ticks_per_slot()
        );

        let parent_meta = SlotMeta {
            received: 1,
            ..SlotMeta::default()
        };
        poh_recorder
            .blockstore
            .put_meta_bytes(0, &serialize(&parent_meta).unwrap())
            .unwrap();

        // Use a key that's different from the previous leader so that grace
        // ticks are enforced.
        let test_validator_pubkey = Pubkey::new_unique();

        // Test that we don't reach the leader slot because of grace ticks
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::NotReached
        );

        // reset poh now. we should immediately be leader
        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
        assert_eq!(bank1.slot(), 1);
        poh_recorder.reset(bank1.clone(), Some((2, 2)));
        assert_eq!(
            poh_recorder.reached_leader_slot(&validator_pubkey),
            PohLeaderStatus::Reached {
                poh_slot: 2,
                parent_slot: 1,
            }
        );

        // Now test that with grace ticks we can reach leader slot
        // Set the leader slot one slot down
        poh_recorder.reset(bank1.clone(), Some((3, 3)));

        // Send one slot worth of ticks ("skips" slot 2)
        for _ in 0..bank1.ticks_per_slot() {
            poh_recorder.tick();
        }

        // We are not the leader yet, as expected
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::NotReached
        );
        // Check that if prev slot was mine, grace ticks are ignored
        assert_eq!(
            poh_recorder.reached_leader_slot(bank1.collector_id()),
            PohLeaderStatus::Reached {
                poh_slot: 3,
                parent_slot: 1
            }
        );

        // Send the grace ticks
        for _ in 0..bank1.ticks_per_slot() / GRACE_TICKS_FACTOR {
            poh_recorder.tick();
        }

        // We should be the leader now
        // without sending more ticks, we should be leader now
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::Reached {
                poh_slot: 3,
                parent_slot: 1,
            }
        );

        // Let's test that correct grace ticks are reported
        // Set the leader slot one slot down
        let bank2 = Arc::new(Bank::new_from_parent(bank1.clone(), &Pubkey::default(), 2));
        poh_recorder.reset(bank2.clone(), Some((4, 4)));

        // send ticks for a slot
        for _ in 0..bank1.ticks_per_slot() {
            poh_recorder.tick();
        }

        // We are not the leader yet, as expected
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::NotReached
        );
        let bank3 = Arc::new(Bank::new_from_parent(bank2, &Pubkey::default(), 3));
        assert_eq!(bank3.slot(), 3);
        poh_recorder.reset(bank3.clone(), Some((4, 4)));

        // without sending more ticks, we should be leader now
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::Reached {
                poh_slot: 4,
                parent_slot: 3,
            }
        );

        // Let's test that if a node overshoots the ticks for its target
        // leader slot, reached_leader_slot() will return true, because it's overdue
        // Set the leader slot one slot down
        let bank4 = Arc::new(Bank::new_from_parent(bank3, &Pubkey::default(), 4));
        poh_recorder.reset(bank4.clone(), Some((5, 5)));

        // Overshoot ticks for the slot
        let overshoot_factor = 4;
        for _ in 0..overshoot_factor * bank4.ticks_per_slot() {
            poh_recorder.tick();
        }

        // We are overdue to lead
        assert_eq!(
            poh_recorder.reached_leader_slot(&test_validator_pubkey),
            PohLeaderStatus::Reached {
                poh_slot: 9,
                parent_slot: 4,
            }
        );

        // Test that grace ticks are not required if the previous leader's 4
        // slots got skipped.
        {
            poh_recorder.reset(bank4.clone(), Some((9, 9)));

            // Tick until leader slot
            for _ in 0..4 * bank4.ticks_per_slot() {
                poh_recorder.tick();
            }

            // We are due to lead
            assert_eq!(
                poh_recorder.reached_leader_slot(&test_validator_pubkey),
                PohLeaderStatus::Reached {
                    poh_slot: 9,
                    parent_slot: 4,
                }
            );

            // Add an active descendant which is considered to be a pending new
            // reset bank
            poh_recorder.update_start_bank_active_descendants(&[5]);
            assert!(poh_recorder.is_new_reset_bank_pending(8));

            // Without setting delay_leader_block_for_pending_fork, skip grace ticks
            assert_eq!(
                poh_recorder.reached_leader_slot(&test_validator_pubkey),
                PohLeaderStatus::Reached {
                    poh_slot: 9,
                    parent_slot: 4,
                }
            );

            // After setting delay_leader_block_for_pending_fork, grace ticks are required
            poh_recorder.delay_leader_block_for_pending_fork = true;
            assert_eq!(
                poh_recorder.reached_leader_slot(&test_validator_pubkey),
                PohLeaderStatus::NotReached,
            );

            // Tick through grace ticks
            for _ in 0..poh_recorder.grace_ticks {
                poh_recorder.tick();
            }

            // After grace ticks, we are due to lead
            assert_eq!(
                poh_recorder.reached_leader_slot(&test_validator_pubkey),
                PohLeaderStatus::Reached {
                    poh_slot: 9,
                    parent_slot: 4,
                }
            );
        }
    }

    #[test]
    fn test_would_be_leader_soon() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let prev_hash = bank.last_blockhash();
        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            prev_hash,
            bank.clone(),
            None,
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );

        // Test that with no leader slot, we don't reach the leader tick
        assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

        assert_eq!(bank.slot(), 0);
        poh_recorder.reset(bank.clone(), None);

        assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

        // We reset with leader slot after 3 slots
        let bank_slot = bank.slot() + 3;
        poh_recorder.reset(bank.clone(), Some((bank_slot, bank_slot)));

        // Test that the node won't be leader in next 2 slots
        assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

        // Test that the node will be leader in next 3 slots
        assert!(poh_recorder.would_be_leader(3 * bank.ticks_per_slot()));

        assert!(!poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));

        // Move the bank up a slot (so that max_tick_height > slot 0's tick_height)
        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 1));
        // If we set the working bank, the node should be leader within next 2 slots
        poh_recorder.set_bank_for_test(bank.clone());
        assert!(poh_recorder.would_be_leader(2 * bank.ticks_per_slot()));
    }

    #[test]
    fn test_flush_virtual_ticks() {
        let ledger_path = get_tmp_ledger_path_auto_delete!();
        // test that virtual ticks are flushed into a newly set bank asap
        let blockstore = Blockstore::open(ledger_path.path())
            .expect("Expected to be able to open database ledger");
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(2);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let genesis_hash = bank.last_blockhash();

        let (mut poh_recorder, _entry_receiver, _record_receiver) = PohRecorder::new(
            0,
            bank.last_blockhash(),
            bank.clone(),
            Some((2, 2)),
            bank.ticks_per_slot(),
            Arc::new(blockstore),
            &Arc::new(LeaderScheduleCache::new_from_bank(&bank)),
            &PohConfig::default(),
            Arc::new(AtomicBool::default()),
        );
        //create a new bank
        let bank = Arc::new(Bank::new_from_parent(bank, &Pubkey::default(), 2));
        // add virtual ticks into poh for slots 0, 1, and 2
        for _ in 0..(bank.ticks_per_slot() * 3) {
            poh_recorder.tick();
        }
        poh_recorder.set_bank_for_test(bank.clone());
        assert!(!bank.is_hash_valid_for_age(&genesis_hash, 0));
        assert!(bank.is_hash_valid_for_age(&genesis_hash, 1));
    }

    #[test]
    fn test_compute_leader_slot_tick_heights() {
        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(None, 0),
            (None, 0, 0)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((4, 4)), 8),
            (Some(37), 40, 4)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((4, 7)), 8),
            (Some(49), 64, 2 * 8)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((6, 7)), 8),
            (Some(57), 64, 8)
        );

        assert_eq!(
            PohRecorder::compute_leader_slot_tick_heights(Some((6, 7)), 4),
            (Some(29), 32, 4)
        );
    }
}
