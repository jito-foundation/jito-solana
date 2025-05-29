use {
    super::{Error, Result},
    bincode::serialized_size,
    crossbeam_channel::Receiver,
    solana_entry::entry::Entry,
    solana_ledger::{
        blockstore::Blockstore,
        shred::{self, ShredData},
    },
    solana_poh::poh_recorder::WorkingBankEntry,
    solana_runtime::bank::Bank,
    solana_sdk::{clock::Slot, hash::Hash},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const ENTRY_COALESCE_DURATION: Duration = Duration::from_millis(50);

pub(super) struct ReceiveResults {
    pub entries: Vec<Entry>,
    pub time_elapsed: Duration,
    pub time_coalesced: Duration,
    pub bank: Arc<Bank>,
    pub last_tick_height: u64,
}

pub(super) fn recv_slot_entries(receiver: &Receiver<WorkingBankEntry>) -> Result<ReceiveResults> {
    let target_serialized_batch_byte_count: u64 =
        32 * ShredData::capacity(/*merkle_proof_size*/ None).unwrap() as u64;
    let timer = Duration::new(1, 0);
    let recv_start = Instant::now();

    let WorkingBankEntry {
        mut bank,
        entries_ticks,
    } = receiver.recv_timeout(timer)?;
    let mut last_tick_height = entries_ticks.iter().last().unwrap().1;
    let mut entries: Vec<Entry> = entries_ticks.into_iter().map(|(e, _)| e).collect();

    assert!(last_tick_height <= bank.max_tick_height());

    // Drain channel
    while last_tick_height != bank.max_tick_height() {
        let Ok(WorkingBankEntry {
            bank: try_bank,
            entries_ticks: new_entries_ticks,
        }) = receiver.try_recv()
        else {
            break;
        };
        // If the bank changed, that implies the previous slot was interrupted and we do not have to
        // broadcast its entries.
        if try_bank.slot() != bank.slot() {
            warn!("Broadcast for slot: {} interrupted", bank.slot());
            entries.clear();
            bank = try_bank;
        }
        last_tick_height = new_entries_ticks.iter().last().unwrap().1;
        entries.extend(new_entries_ticks.into_iter().map(|(entry, _)| entry));
        assert!(last_tick_height <= bank.max_tick_height());
    }

    let mut serialized_batch_byte_count = serialized_size(&entries)?;

    // Wait up to `ENTRY_COALESCE_DURATION` to try to coalesce entries into a 32 shred batch
    let mut coalesce_start = Instant::now();
    while last_tick_height != bank.max_tick_height()
        && serialized_batch_byte_count < target_serialized_batch_byte_count
    {
        let Ok(WorkingBankEntry {
            bank: try_bank,
            entries_ticks: new_entries_ticks,
        }) = receiver.recv_deadline(coalesce_start + ENTRY_COALESCE_DURATION)
        else {
            break;
        };
        // If the bank changed, that implies the previous slot was interrupted and we do not have to
        // broadcast its entries.
        if try_bank.slot() != bank.slot() {
            warn!("Broadcast for slot: {} interrupted", bank.slot());
            entries.clear();
            serialized_batch_byte_count = 8; // Vec len
            bank = try_bank;
            coalesce_start = Instant::now();
        }
        last_tick_height = new_entries_ticks.iter().last().unwrap().1;

        for (entry, _) in &new_entries_ticks {
            serialized_batch_byte_count += serialized_size(entry)?;
        }
        entries.extend(new_entries_ticks.into_iter().map(|(entry, _)| entry));
        assert!(last_tick_height <= bank.max_tick_height());
    }
    let time_coalesced = coalesce_start.elapsed();

    let time_elapsed = recv_start.elapsed();
    Ok(ReceiveResults {
        entries,
        time_elapsed,
        time_coalesced,
        bank,
        last_tick_height,
    })
}

// Returns the Merkle root of the last erasure batch of the parent slot.
pub(super) fn get_chained_merkle_root_from_parent(
    slot: Slot,
    parent: Slot,
    blockstore: &Blockstore,
) -> Result<Hash> {
    if slot == parent {
        debug_assert_eq!(slot, 0u64);
        return Ok(Hash::default());
    }
    debug_assert!(parent < slot, "parent: {parent} >= slot: {slot}");
    let index = blockstore
        .meta(parent)?
        .ok_or_else(|| Error::UnknownSlotMeta(parent))?
        .last_index
        .ok_or_else(|| Error::UnknownLastIndex(parent))?;
    let shred = blockstore
        .get_data_shred(parent, index)?
        .ok_or(Error::ShredNotFound {
            slot: parent,
            index,
        })?;
    shred::layout::get_merkle_root(&shred).ok_or(Error::InvalidMerkleRoot {
        slot: parent,
        index,
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

        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
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

        let bank1 = Arc::new(Bank::new_from_parent(bank0, &Pubkey::default(), 1));
        let bank2 = Arc::new(Bank::new_from_parent(bank1.clone(), &Pubkey::default(), 2));
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
