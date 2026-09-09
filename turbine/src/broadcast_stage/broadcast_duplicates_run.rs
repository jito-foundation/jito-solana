use {
    super::*,
    crate::{ShredReceiverAddresses, cluster_nodes::ClusterNodesCache},
    agave_votor::event::VotorEventSender,
    agave_votor_messages::migration::MigrationStatus,
    crossbeam_channel::Sender,
    itertools::Itertools,
    solana_entry::{block_component::BlockComponent, entry::Entry},
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, ShredId, Shredder},
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{borrow::Cow, collections::HashSet, net::SocketAddr},
};

// Shreds in a Merkle FEC set share a signature, while duplicate variants share shred IDs.
// The pair uniquely identifies every shred from both versions.
type DuplicateShredKey = (Signature, ShredId);

fn duplicate_shred_key(shred: &Shred) -> DuplicateShredKey {
    (*shred.signature(), shred.id())
}

pub const MINIMUM_DUPLICATE_SLOT: Slot = 20;
pub const DUPLICATE_RATE: usize = 10;

#[derive(PartialEq, Eq, Clone, Debug)]
pub enum ClusterPartition {
    Stake(u64),
    Pubkey(Vec<Pubkey>),
}

#[derive(Clone, Debug)]
pub struct BroadcastDuplicatesConfig {
    /// Amount of stake (excluding the leader) or a set of validator pubkeys
    /// to send a duplicate version of some slots to.
    /// Note this is sampled from a list of stakes sorted least to greatest.
    pub partition: ClusterPartition,
    /// If passed `Some(receiver)`, will signal all the duplicate slots via the given
    /// `receiver`
    pub duplicate_slot_sender: Option<Sender<Slot>>,
}

#[derive(Clone)]
pub(super) struct BroadcastDuplicatesRun {
    config: BroadcastDuplicatesConfig,
    current_slot: Slot,
    chained_merkle_root: Hash,
    carryover_entry: Option<WorkingBankEntryOrMarker>,
    next_shred_index: u32,
    next_code_index: u32,
    shred_version: u16,
    recent_blockhash: Option<Hash>,
    prev_entry_hash: Option<Hash>,
    num_slots_broadcasted: usize,
    cluster_nodes_cache: Arc<ClusterNodesCache<BroadcastStage>>,
    original_last_data_shreds: Arc<Mutex<HashSet<DuplicateShredKey>>>,
    partition_last_data_shreds: Arc<Mutex<HashSet<DuplicateShredKey>>>,
    reed_solomon_cache: Arc<ReedSolomonCache>,
    migration_status: Arc<MigrationStatus>,
    votor_event_sender: VotorEventSender,
}

impl BroadcastDuplicatesRun {
    pub(super) fn new(
        shred_version: u16,
        config: BroadcastDuplicatesConfig,
        migration_status: Arc<MigrationStatus>,
        votor_event_sender: VotorEventSender,
    ) -> Self {
        let cluster_nodes_cache = Arc::new(ClusterNodesCache::<BroadcastStage>::new(
            CLUSTER_NODES_CACHE_NUM_EPOCH_CAP,
            CLUSTER_NODES_CACHE_TTL,
        ));
        Self {
            config,
            chained_merkle_root: Hash::default(),
            carryover_entry: None,
            next_shred_index: u32::MAX,
            next_code_index: 0,
            shred_version,
            current_slot: 0,
            recent_blockhash: None,
            prev_entry_hash: None,
            num_slots_broadcasted: 0,
            cluster_nodes_cache,
            original_last_data_shreds: Arc::<Mutex<HashSet<DuplicateShredKey>>>::default(),
            partition_last_data_shreds: Arc::<Mutex<HashSet<DuplicateShredKey>>>::default(),
            reed_solomon_cache: Arc::<ReedSolomonCache>::default(),
            migration_status,
            votor_event_sender,
        }
    }
}

impl BroadcastRun for BroadcastDuplicatesRun {
    fn run<'db>(
        &mut self,
        keypair: &Keypair,
        blockstore: &'db Blockstore,
        _pinnable_slice: &mut DBPinnableSlice<'db>,
        _write_batch: &mut WriteBatch,
        receiver: &Receiver<WorkingBankEntryOrMarker>,
        socket_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
        blockstore_sender: &Sender<(Arc<Vec<Shred>>, Option<BroadcastShredBatchInfo>)>,
    ) -> Result<()> {
        // 1) Pull entries from banking stage
        let mut stats = ProcessShredsStats::default();
        let mut receive_results =
            broadcast_utils::recv_slot_components(receiver, &mut self.carryover_entry, &mut stats)?;
        let bank = receive_results.bank.clone();
        let last_tick_height = receive_results.last_tick_height;

        if bank.slot() != self.current_slot {
            self.chained_merkle_root = broadcast_utils::get_chained_merkle_root_from_parent(
                bank.slot(),
                bank.parent_slot(),
                blockstore,
            )
            .unwrap();
            self.next_shred_index = 0;
            self.next_code_index = 0;
            self.current_slot = bank.slot();
            self.prev_entry_hash = None;
            self.num_slots_broadcasted += 1;
        }

        let BlockComponent::EntryBatch(ref mut entries) = receive_results.component else {
            // This test only TowerBFT implementation does not use block markers
            return Ok(());
        };
        // We are guarenteed by coalesce that this is not empty
        assert!(!entries.is_empty());
        // Update the recent blockhash based on transactions in the entries
        for entry in entries.iter() {
            if !entry.transactions.is_empty() {
                self.recent_blockhash = Some(*entry.transactions[0].message.recent_blockhash());
                break;
            }
        }

        // 2) Convert entries to shreds + generate coding shreds. Set a garbage PoH on the last entry
        // in the slot to make verification fail on validators
        let last_entries = {
            if last_tick_height == bank.max_tick_height()
                && bank.slot() > MINIMUM_DUPLICATE_SLOT
                && self.num_slots_broadcasted.is_multiple_of(DUPLICATE_RATE)
                && let Some(recent_blockhash) = self.recent_blockhash
            {
                let entry_batch_len = entries.len();
                let prev_entry_hash =
                    // Try to get second-to-last entry before last tick
                    if entry_batch_len > 1 {
                        Some(entries[entry_batch_len - 2].hash)
                    } else {
                        self.prev_entry_hash
                    };

                if let Some(prev_entry_hash) = prev_entry_hash {
                    info!(
                        target: "lc2_diagnostic",
                        "duplicate-batch leader={} slot={} parent={} entries={} tick_height={} \
                         max_tick_height={} next_data_index={} next_code_index={} prefix_root={}",
                        keypair.pubkey(),
                        bank.slot(),
                        bank.parent_slot(),
                        entry_batch_len,
                        last_tick_height,
                        bank.max_tick_height(),
                        self.next_shred_index,
                        self.next_code_index,
                        self.chained_merkle_root,
                    );
                    let original_last_entry = entries.pop().unwrap();

                    // Last entry has to be a tick
                    assert!(original_last_entry.is_tick());

                    // Inject an extra entry before the last tick
                    let extra_tx = system_transaction::transfer(
                        keypair,
                        &Pubkey::new_unique(),
                        1,
                        recent_blockhash,
                    );
                    let new_extra_entry = Entry::new(&prev_entry_hash, 1, vec![extra_tx]);

                    // This will only work with sleepy tick producer where the hashing
                    // checks in replay are turned off, because we're introducing an extra
                    // hash for the last tick in the `new_extra_entry`.
                    let new_last_entry = Entry::new(
                        &new_extra_entry.hash,
                        original_last_entry.num_hashes,
                        vec![],
                    );

                    Some((original_last_entry, vec![new_extra_entry, new_last_entry]))
                } else {
                    None
                }
            } else {
                None
            }
        };

        self.prev_entry_hash = last_entries
            .as_ref()
            .map(|(original_last_entry, _)| original_last_entry.hash)
            .or_else(|| entries.last().map(|e| e.hash));

        let shredder = Shredder::new(
            bank.slot(),
            bank.parent().unwrap().slot(),
            (bank.tick_height() % bank.ticks_per_slot()) as u8,
            self.shred_version,
        )
        .expect("Expected to create a new shredder");

        // A solitary final tick leaves no prefix after it is reserved for the two
        // variants below. Serializing that empty prefix would emit a block-abort
        // marker into both variants.
        let (data_shreds, coding_shreds) = if entries.is_empty() {
            (Vec::new(), Vec::new())
        } else {
            shredder.component_to_merkle_shreds_for_tests(
                keypair,
                &receive_results.component,
                last_tick_height == bank.max_tick_height() && last_entries.is_none(),
                self.chained_merkle_root,
                self.next_shred_index,
                self.next_code_index,
                &self.reed_solomon_cache,
                &mut stats,
            )
        };
        if let Some(shred) = data_shreds.iter().max_by_key(|shred| shred.index()) {
            self.chained_merkle_root = shred.merkle_root().unwrap();
        }
        self.next_shred_index += data_shreds.len() as u32;
        if let Some(index) = coding_shreds.iter().map(Shred::index).max() {
            self.next_code_index = index + 1;
        }
        let last_shreds =
            last_entries.map(|(original_last_entry, duplicate_extra_last_entries)| {
                let (original_last_data_shred, _) = shredder.component_to_merkle_shreds_for_tests(
                    keypair,
                    &BlockComponent::EntryBatch(vec![original_last_entry]),
                    true,
                    self.chained_merkle_root,
                    self.next_shred_index,
                    self.next_code_index,
                    &self.reed_solomon_cache,
                    &mut stats,
                );
                // Both variants are complete blocks; the extra entry produces a
                // different bank hash for the partition to resolve through repair.
                let (partition_last_data_shred, _) = shredder.component_to_merkle_shreds_for_tests(
                    keypair,
                    &BlockComponent::EntryBatch(duplicate_extra_last_entries),
                    true,
                    self.chained_merkle_root,
                    self.next_shred_index,
                    self.next_code_index,
                    &self.reed_solomon_cache,
                    &mut stats,
                );
                let sigs: Vec<_> = partition_last_data_shred
                    .iter()
                    .map(|s| (s.signature(), s.index()))
                    .collect();
                info!(
                    "duplicate signatures for slot {}, sigs: {:?}",
                    bank.slot(),
                    sigs,
                );

                assert_eq!(
                    original_last_data_shred.len(),
                    partition_last_data_shred.len()
                );
                info!(
                    target: "lc2_diagnostic",
                    "duplicate-variants leader={} slot={} original_root={} partition_root={} \
                     first_data_index={} final_data_index={}",
                    keypair.pubkey(),
                    bank.slot(),
                    original_last_data_shred[0].merkle_root().unwrap(),
                    partition_last_data_shred[0].merkle_root().unwrap(),
                    original_last_data_shred[0].index(),
                    original_last_data_shred.last().unwrap().index(),
                );
                self.next_shred_index += u32::try_from(original_last_data_shred.len()).unwrap();
                // Update chained_merkle_root to the merkle root of the original last FEC set
                if let Some(shred) = original_last_data_shred
                    .iter()
                    .max_by_key(|shred| shred.index())
                {
                    self.chained_merkle_root = shred.merkle_root().unwrap();
                }
                (original_last_data_shred, partition_last_data_shred)
            });

        if !data_shreds.is_empty() {
            let data_shreds = Arc::new(data_shreds);
            // 3) Start broadcast step
            info!(
                "{} Sending good shreds for slot {} to network",
                keypair.pubkey(),
                data_shreds.first().unwrap().slot()
            );
            assert!(data_shreds.iter().all(|shred| shred.slot() == bank.slot()));
            dispatch_shreds(blockstore_sender, socket_sender, data_shreds, None)?;
        }

        // Special handling of last shred to cause partition
        if let Some((original_last_data_shred, partition_last_data_shred)) = last_shreds {
            let pubkey = keypair.pubkey();
            self.original_last_data_shreds.lock().unwrap().extend(
                original_last_data_shred.iter().map(|shred| {
                    assert!(shred.verify(&pubkey));
                    duplicate_shred_key(shred)
                }),
            );
            self.partition_last_data_shreds.lock().unwrap().extend(
                partition_last_data_shred.iter().map(|shred| {
                    info!("adding {} to partition set", shred.signature());
                    assert!(shred.verify(&pubkey));
                    duplicate_shred_key(shred)
                }),
            );
            let original_last_data_shred = Arc::new(original_last_data_shred);
            let partition_last_data_shred = Arc::new(partition_last_data_shred);

            assert!(
                original_last_data_shred
                    .iter()
                    .all(|shred| shred.slot() == bank.slot())
            );
            assert!(
                partition_last_data_shred
                    .iter()
                    .all(|shred| shred.slot() == bank.slot())
            );

            if let Some(duplicate_slot_sender) = &self.config.duplicate_slot_sender {
                let _ = duplicate_slot_sender.send(bank.slot());
            }
            // Store the original shreds that this node replayed;
            dispatch_shreds(
                blockstore_sender,
                socket_sender,
                original_last_data_shred,
                None,
            )?;
            // the partition shreds go only over the wire to create the duplicate slot.
            // blocking here is ok since this is only ever used in tests.
            socket_sender.send((partition_last_data_shred, None))?;
        }

        if last_tick_height == bank.max_tick_height() {
            broadcast_utils::set_block_id_and_send(
                &self.migration_status,
                &self.votor_event_sender,
                bank,
                self.chained_merkle_root,
            )?;
        }
        Ok(())
    }

    fn transmit(
        &mut self,
        receiver: &TransmitReceiver,
        cluster_info: &ClusterInfo,
        sock: BroadcastSocket,
        bank_forks: &RwLock<BankForks>,
        _shredstream_receiver_address: &ArcSwap<Option<SocketAddr>>,
        _shred_receiver_addresses: &ArcSwap<ShredReceiverAddresses>,
        _bam_shred_receiver_addresses: &ArcSwap<ShredReceiverAddresses>,
        _multicast_receiver_address: &ArcSwap<Option<SocketAddr>>,
        _shred_receiver_socket: &UdpSocket,
    ) -> Result<()> {
        let (shreds, _) = receiver.recv()?;
        if shreds.is_empty() {
            return Ok(());
        }
        let slot = shreds.first().unwrap().slot();
        assert!(shreds.iter().all(|shred| shred.slot() == slot));
        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            (bank_forks.root_bank(), bank_forks.working_bank())
        };
        let self_pubkey = cluster_info.id();
        // Create cluster partition.
        let cluster_partition: HashSet<Pubkey> = {
            match &self.config.partition {
                ClusterPartition::Stake(partition_total_stake) => {
                    let mut cumulative_stake = 0;
                    let epoch = root_bank.get_leader_schedule_epoch(slot);
                    root_bank
                        .epoch_staked_nodes(epoch)
                        .unwrap()
                        .iter()
                        .filter(|(pubkey, _)| **pubkey != self_pubkey)
                        .sorted_by_key(|(pubkey, stake)| (**stake, **pubkey))
                        .take_while(|(_, stake)| {
                            cumulative_stake += *stake;
                            cumulative_stake <= *partition_total_stake
                        })
                        .map(|(pubkey, _)| *pubkey)
                        .collect()
                }
                ClusterPartition::Pubkey(pubkeys) => pubkeys.iter().cloned().collect(),
            }
        };

        // Broadcast data
        let cluster_nodes =
            self.cluster_nodes_cache
                .get(slot, &root_bank, &working_bank, cluster_info);
        let socket_addr_space = cluster_info.socket_addr_space();
        let packets: Vec<_> = shreds
            .iter()
            .filter_map(|shred| {
                if self
                    .partition_last_data_shreds
                    .lock()
                    .unwrap()
                    .remove(&duplicate_shred_key(shred))
                {
                    // Partition shreds bypass Turbine and are sent directly to the partition.
                    // Do this before selecting a Turbine peer so that an unrelated peer with no
                    // usable TVU address cannot cause the partition shred to be dropped.
                    return Some(
                        cluster_partition
                            .iter()
                            .filter_map(|pubkey| {
                                info!(
                                    "Broadcasting partition shred index {}, slot {} to partition \
                                     node {}",
                                    shred.index(),
                                    shred.slot(),
                                    pubkey,
                                );
                                let tvu = cluster_info.lookup_contact_info(pubkey, |node| {
                                    node.tvu(Protocol::UDP)
                                })??;
                                socket_addr_space
                                    .check(&tvu)
                                    .then_some((shred.payload(), tvu))
                            })
                            .collect(),
                    );
                }

                let node = cluster_nodes.get_broadcast_peer(&shred.id())?;
                if !socket_addr_space.check(&node.tvu(Protocol::UDP)?) {
                    return None;
                }
                if self
                    .original_last_data_shreds
                    .lock()
                    .unwrap()
                    .remove(&duplicate_shred_key(shred))
                    && cluster_partition.contains(node.pubkey())
                {
                    info!(
                        "Not broadcasting original shred index {}, slot {} to partition node {}",
                        shred.index(),
                        shred.slot(),
                        node.pubkey(),
                    );
                    return None;
                }

                Some(vec![(shred.payload(), node.tvu(Protocol::UDP)?)])
            })
            .flatten()
            .collect();

        let sock = match sock {
            BroadcastSocket::Udp(sock) => sock,
            BroadcastSocket::Xdp(_) => {
                panic!("Xdp not supported for duplicate shreds run");
            }
        };
        batch_send(sock, packets).map_err(|SendPktsError::IoError(err, _)| Error::Io(err))
    }

    fn record<'db>(
        &mut self,
        receiver: &RecordReceiver,
        blockstore: &'db Blockstore,
        pinnable_slice: &mut DBPinnableSlice<'db>,
        write_batch: &mut WriteBatch,
    ) -> Result<()> {
        let (all_shreds, _) = receiver.recv()?;
        blockstore
            .insert_cow_shreds(
                all_shreds.iter().map(Cow::Borrowed),
                true,
                pinnable_slice,
                write_batch,
            )
            .expect("Failed to insert shreds in blockstore");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_entry::entry::create_ticks,
        solana_ledger::{genesis_utils::create_genesis_config, get_tmp_ledger_path},
        solana_runtime::bank::{Bank, SlotLeader},
    };

    #[test]
    fn test_duplicate_shreds_solitary_final_tick() {
        check_duplicate_shreds_final_tick_batch(1);
    }

    #[test]
    fn test_duplicate_shreds_coalesced_final_tick() {
        check_duplicate_shreds_final_tick_batch(2);
    }

    fn check_duplicate_shreds_final_tick_batch(final_batch_ticks: usize) {
        let genesis = create_genesis_config(10_000);
        let keypair = genesis.mint_keypair;
        let initial_hash = genesis.genesis_config.hash();
        let (parent, bank_forks) =
            Bank::new_for_tests(&genesis.genesis_config).wrap_with_bank_forks_for_tests();
        parent.set_tick_height(parent.max_tick_height());
        Bank::calculate_and_set_block_id_for_dcou(&parent);
        let original_blockstore = Blockstore::open(&get_tmp_ledger_path!()).unwrap();
        original_blockstore.insert_shreds_for_bank(parent.clone());
        let slot = MINIMUM_DUPLICATE_SLOT + DUPLICATE_RATE as u64;
        let bank =
            Bank::new_from_parent_with_bank_forks(&bank_forks, parent, SlotLeader::default(), slot);
        bank.set_tick_height(bank.max_tick_height() - bank.ticks_per_slot());
        let transaction =
            system_transaction::transfer(&keypair, &Pubkey::new_unique(), 1, initial_hash);
        let transaction_entry = Entry::new(&initial_hash, 1, vec![transaction]);
        let ticks = create_ticks(bank.ticks_per_slot(), 0, transaction_entry.hash);
        let prefix_ticks = ticks.len() - final_batch_ticks;
        let expected_original_entries: Vec<_> = std::iter::once(transaction_entry.clone())
            .chain(ticks.iter().cloned())
            .collect();
        let (duplicate_slot_sender, duplicate_slot_receiver) = bounded(1);
        let (votor_event_sender, _votor_event_receiver) = bounded(1);
        let mut run = BroadcastDuplicatesRun::new(
            0,
            BroadcastDuplicatesConfig {
                partition: ClusterPartition::Pubkey(vec![Pubkey::new_unique()]),
                duplicate_slot_sender: Some(duplicate_slot_sender),
            },
            Arc::new(MigrationStatus::default()),
            votor_event_sender,
        );
        run.num_slots_broadcasted = DUPLICATE_RATE - 1;
        let (socket_sender, socket_receiver) = bounded(16);
        let (blockstore_sender, blockstore_receiver) = bounded(16);
        let mut pinnable_slice = original_blockstore.new_pinnable_slice();
        let mut write_batch = original_blockstore.get_write_batch();

        // End the first channel after the prefix, so coalescing cannot consume the
        // final tick. The second run starts with the desired final batch exactly.
        let (entry_sender, entry_receiver) = bounded(1024);
        entry_sender
            .send((bank.clone(), (transaction_entry.into(), bank.tick_height())))
            .unwrap();
        for (index, tick) in ticks[..prefix_ticks].iter().enumerate() {
            entry_sender
                .send((
                    bank.clone(),
                    (tick.clone().into(), bank.tick_height() + index as u64 + 1),
                ))
                .unwrap();
        }
        drop(entry_sender);
        run.run(
            &keypair,
            &original_blockstore,
            &mut pinnable_slice,
            &mut write_batch,
            &entry_receiver,
            &socket_sender,
            &blockstore_sender,
        )
        .unwrap();
        assert!(duplicate_slot_receiver.try_recv().is_err());

        let (entry_sender, entry_receiver) = bounded(1024);
        for (index, tick) in ticks[prefix_ticks..].iter().enumerate() {
            entry_sender
                .send((
                    bank.clone(),
                    (
                        tick.clone().into(),
                        bank.tick_height() + (prefix_ticks + index) as u64 + 1,
                    ),
                ))
                .unwrap();
        }
        drop(entry_sender);
        run.run(
            &keypair,
            &original_blockstore,
            &mut pinnable_slice,
            &mut write_batch,
            &entry_receiver,
            &socket_sender,
            &blockstore_sender,
        )
        .unwrap();
        assert_eq!(duplicate_slot_receiver.try_recv().unwrap(), slot);

        let original_keys = run.original_last_data_shreds.lock().unwrap();
        let partition_keys = run.partition_last_data_shreds.lock().unwrap();
        assert!(!original_keys.is_empty());
        assert_eq!(original_keys.len(), partition_keys.len());
        assert!(original_keys.is_disjoint(&partition_keys));
        let original_shreds: Vec<_> = blockstore_receiver
            .try_iter()
            .flat_map(|(shreds, _)| shreds.as_ref().clone())
            .collect();
        let partition_shreds: Vec<_> = socket_receiver
            .try_iter()
            .flat_map(|(shreds, _)| shreds.as_ref().clone())
            .filter(|shred| !original_keys.contains(&duplicate_shred_key(shred)))
            .collect();
        assert!(
            original_shreds
                .iter()
                .chain(&partition_shreds)
                .all(|shred| shred.verify(&keypair.pubkey()))
        );
        let partition_blockstore = Blockstore::open(&get_tmp_ledger_path!()).unwrap();
        original_blockstore
            .insert_shreds(original_shreds, false)
            .unwrap();
        partition_blockstore
            .insert_shreds(partition_shreds, false)
            .unwrap();
        assert!(original_blockstore.meta(slot).unwrap().unwrap().is_full());
        assert!(partition_blockstore.meta(slot).unwrap().unwrap().is_full());
        assert_ne!(
            original_blockstore
                .get_last_shred_merkle_root(slot)
                .unwrap(),
            partition_blockstore
                .get_last_shred_merkle_root(slot)
                .unwrap(),
        );

        // Both complete variants must be replayable entry streams. In particular,
        // reserving a solitary final tick must not serialize a block-abort marker.
        let original_entries = original_blockstore.get_slot_entries(slot, 0);
        let partition_entries = partition_blockstore.get_slot_entries(slot, 0);
        assert!(
            original_entries.is_ok() && partition_entries.is_ok(),
            "entry decoding failed: original={:?}, partition={:?}",
            original_entries.as_ref().err(),
            partition_entries.as_ref().err(),
        );
        let original_entries = original_entries.unwrap();
        let partition_entries = partition_entries.unwrap();
        assert_eq!(original_entries, expected_original_entries);
        assert_eq!(partition_entries.len(), original_entries.len() + 1);
        assert_eq!(
            &partition_entries[..original_entries.len() - 1],
            &original_entries[..original_entries.len() - 1],
        );
        for entries in [&original_entries, &partition_entries] {
            let mut previous_hash = initial_hash;
            for entry in entries {
                assert!(entry.verify(&previous_hash));
                previous_hash = entry.hash;
            }
            assert!(entries.last().unwrap().is_tick());
        }
        assert_ne!(
            original_entries.last().unwrap().hash,
            partition_entries.last().unwrap().hash,
        );
    }

    #[test]
    fn test_special_shred_key_distinguishes_shreds_with_shared_signature() {
        let keypair = Keypair::new();
        let shredder = Shredder::new(1, 0, 0, 0).unwrap();
        let reed_solomon_cache = ReedSolomonCache::default();
        let mut stats = ProcessShredsStats::default();
        let (data_shreds, _) = shredder.component_to_merkle_shreds_for_tests(
            &keypair,
            &BlockComponent::EntryBatch(create_ticks(1, 0, Hash::default())),
            true,
            Hash::default(),
            0,
            0,
            &reed_solomon_cache,
            &mut stats,
        );
        let (duplicate_data_shreds, _) = shredder.component_to_merkle_shreds_for_tests(
            &keypair,
            &BlockComponent::EntryBatch(create_ticks(1, 0, Hash::new_unique())),
            true,
            Hash::default(),
            0,
            0,
            &reed_solomon_cache,
            &mut stats,
        );

        assert!(data_shreds.len() > 1);
        assert_eq!(data_shreds.len(), duplicate_data_shreds.len());
        assert_eq!(
            data_shreds
                .iter()
                .map(|shred| *shred.signature())
                .collect::<HashSet<_>>()
                .len(),
            1,
        );
        assert!(
            data_shreds
                .iter()
                .zip(&duplicate_data_shreds)
                .all(|(shred, duplicate)| {
                    shred.id() == duplicate.id()
                        && shred.signature() != duplicate.signature()
                        && duplicate_shred_key(shred) != duplicate_shred_key(duplicate)
                })
        );

        let mut tracked: HashSet<DuplicateShredKey> = data_shreds
            .iter()
            .chain(&duplicate_data_shreds)
            .map(duplicate_shred_key)
            .collect();
        assert_eq!(
            tracked.len(),
            data_shreds.len() + duplicate_data_shreds.len()
        );
        for shred in data_shreds.iter().chain(&duplicate_data_shreds) {
            assert!(tracked.remove(&duplicate_shred_key(shred)));
        }
        assert!(tracked.is_empty());
    }
}
