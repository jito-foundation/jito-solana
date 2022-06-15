use {
    crate::result::Result,
    crossbeam_channel::Receiver,
    solana_entry::entry::Entry,
    solana_ledger::shred::Shred,
    solana_poh::poh_recorder::WorkingBankEntry,
    solana_runtime::bank::Bank,
    solana_sdk::clock::Slot,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

pub(super) struct ReceiveResults {
    pub entries: Vec<Entry>,
    pub time_elapsed: Duration,
    pub bank: Arc<Bank>,
    pub last_tick_height: u64,
}

#[derive(Clone)]
pub struct UnfinishedSlotInfo {
    pub next_shred_index: u32,
    pub(crate) next_code_index: u32,
    pub slot: Slot,
    pub parent: Slot,
    // Data shreds buffered to make a batch of size
    // MAX_DATA_SHREDS_PER_FEC_BLOCK.
    pub(crate) data_shreds_buffer: Vec<Shred>,
}

/// This parameter tunes how many entries are received in one iteration of recv loop
/// This will prevent broadcast stage from consuming more entries, that could have led
/// to delays in shredding, and broadcasting shreds to peer validators
const RECEIVE_ENTRY_COUNT_THRESHOLD: usize = 8;

pub(super) fn recv_slot_entries(receiver: &Receiver<WorkingBankEntry>) -> Result<ReceiveResults> {
    let timer = Duration::new(1, 0);
    let recv_start = Instant::now();
    let WorkingBankEntry {
        mut bank,
        entries_ticks,
    } = receiver.recv_timeout(timer)?;

    let mut max_tick_height = bank.max_tick_height();
    let mut slot = bank.slot();

    let ticks: Vec<u64> = entries_ticks.iter().map(|(_, tick)| *tick).collect();
    let mut highest_entry_tick = *ticks.iter().max().unwrap();
    assert!(highest_entry_tick <= max_tick_height);

    let mut entries: Vec<Entry> = entries_ticks.into_iter().map(|(e, _)| e).collect();

    // drain channel if not at max tick height for this slot yet
    if !ticks.iter().any(|t| *t == max_tick_height) {
        while let Ok(WorkingBankEntry {
            bank: try_bank,
            entries_ticks,
        }) = receiver.try_recv()
        {
            if try_bank.slot() != slot {
                warn!("Broadcast for slot: {} interrupted", bank.slot());
                entries.clear();
                bank = try_bank;
                slot = bank.slot();
                max_tick_height = bank.max_tick_height();
            }

            let ticks: Vec<u64> = entries_ticks.iter().map(|(_, tick)| *tick).collect();
            highest_entry_tick = *ticks.iter().max().unwrap();

            entries.extend(entries_ticks.into_iter().map(|(entry, _)| entry));
            if entries.len() >= RECEIVE_ENTRY_COUNT_THRESHOLD {
                break;
            }

            assert!(highest_entry_tick <= max_tick_height);
            if highest_entry_tick == max_tick_height {
                break;
            }
        }
    }

    Ok(ReceiveResults {
        entries,
        time_elapsed: recv_start.elapsed(),
        bank,
        last_tick_height: highest_entry_tick,
    })
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo},
        solana_sdk::{
            genesis_config::GenesisConfig, pubkey::Pubkey, system_transaction,
            transaction::Transaction,
        },
    };

    fn setup_test() -> (GenesisConfig, Arc<Bank>, Transaction) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(2);
        let bank0 = Arc::new(Bank::new_for_tests(&genesis_config));
        let tx = system_transaction::transfer(
            &mint_keypair,
            &solana_sdk::pubkey::new_rand(),
            1,
            genesis_config.hash(),
        );

        (genesis_config, bank0, tx)
    }

    #[test]
    fn test_recv_slot_entries_1() {
        let (genesis_config, bank0, tx) = setup_test();

        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        let (s, r) = unbounded();
        let mut last_hash = genesis_config.hash();

        assert!(bank1.max_tick_height() > 1);
        let entries: Vec<_> = (1..bank1.max_tick_height() + 1)
            .map(|i| {
                let entry = Entry::new(&last_hash, 1, vec![tx.clone()]);
                last_hash = entry.hash;
                s.send(WorkingBankEntry {
                    bank: bank1.clone(),
                    entries_ticks: vec![(entry.clone(), i)],
                })
                .unwrap();
                entry
            })
            .collect();

        let mut res_entries = vec![];
        let mut last_tick_height = 0;
        while let Ok(result) = recv_slot_entries(&r) {
            assert_eq!(result.bank.slot(), bank1.slot());
            last_tick_height = result.last_tick_height;
            res_entries.extend(result.entries);
        }
        assert_eq!(last_tick_height, bank1.max_tick_height());
        assert_eq!(res_entries, entries);
    }

    #[test]
    fn test_recv_slot_entries_2() {
        let (genesis_config, bank0, tx) = setup_test();

        let bank1 = Arc::new(Bank::new_from_parent(&bank0, &Pubkey::default(), 1));
        let bank2 = Arc::new(Bank::new_from_parent(&bank1, &Pubkey::default(), 2));
        let (s, r) = unbounded();

        let mut last_hash = genesis_config.hash();
        assert!(bank1.max_tick_height() > 1);
        // Simulate slot 2 interrupting slot 1's transmission
        let expected_last_height = bank1.max_tick_height();
        let last_entry = (1..=bank1.max_tick_height())
            .map(|tick_height| {
                let entry = Entry::new(&last_hash, 1, vec![tx.clone()]);
                last_hash = entry.hash;
                // Interrupt slot 1 right before the last tick
                if tick_height == expected_last_height {
                    s.send(WorkingBankEntry {
                        bank: bank2.clone(),
                        entries_ticks: vec![(entry.clone(), tick_height)],
                    })
                    .unwrap();
                    Some(entry)
                } else {
                    s.send(WorkingBankEntry {
                        bank: bank1.clone(),
                        entries_ticks: vec![(entry, tick_height)],
                    })
                    .unwrap();
                    None
                }
            })
            .last()
            .unwrap()
            .unwrap();

        let mut res_entries = vec![];
        let mut last_tick_height = 0;
        let mut bank_slot = 0;
        while let Ok(result) = recv_slot_entries(&r) {
            bank_slot = result.bank.slot();
            last_tick_height = result.last_tick_height;
            res_entries = result.entries;
        }
        assert_eq!(bank_slot, bank2.slot());
        assert_eq!(last_tick_height, expected_last_height);
        assert_eq!(res_entries, vec![last_entry]);
    }
}
