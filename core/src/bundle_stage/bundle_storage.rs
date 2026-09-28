use {
    crate::{
        banking_stage::{
            scheduler_messages::MaxAge,
            transaction_scheduler::{
                receive_and_buffer::{PacketHandlingError, TransactionViewReceiveAndBuffer},
                transaction_state_container::{
                    RuntimeTransactionView, StateContainer, TransactionViewStateContainer,
                },
            },
        },
        packet_bundle::VerifiedPacketBundle,
    },
    ahash::HashSet,
    arrayvec::ArrayVec,
    bytes::Bytes,
    smallvec::SmallVec,
    solana_clock::{BankId, Slot},
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
    solana_runtime_transaction::{
        sanitize_config::sanitize_config, transaction_meta::TransactionMeta,
    },
    std::collections::VecDeque,
};

#[derive(Debug, PartialEq, Eq)]
pub enum BundleStorageError {
    EmptyBatch,
    ContainerFull,
    PacketMarkedDiscard(usize),
    PacketFilterError((PacketHandlingError, usize /* packet index */)),
    BundleTooLarge,
    DuplicateTransaction,
}

struct BundleTransactionId {
    container_ids: SmallVec<[usize; 5]>,
    sanitized_bank_id: BankId,
    sanitized_bank_slot: Slot,
}

pub struct BundleStorageEntry {
    pub container_ids: SmallVec<[usize; 5]>,
    pub transactions: SmallVec<[RuntimeTransactionView; 5]>,
    pub max_ages: SmallVec<[MaxAge; 5]>,
    sanitized_bank_id: BankId,
    sanitized_bank_slot: Slot,
}

/// Bundle storage has two deques: one for unprocessed bundles and another for ones that exceeded
/// the cost model and need to get retried next slot.
pub struct BundleStorage {
    last_slot: Slot,
    transaction_capacity: usize,
    transaction_view_state_container: TransactionViewStateContainer,
    unprocessed_bundles: VecDeque<BundleTransactionId>,
    // Storage for bundles that exceeded the cost model for the slot they were last attempted
    // execution on
    cost_model_buffered_bundles: VecDeque<BundleTransactionId>,
}

impl BundleStorage {
    const MAX_PACKETS_PER_BUNDLE: usize = 5;

    #[allow(unused)]
    pub fn with_capacity(transaction_capacity: usize) -> Self {
        Self {
            last_slot: Slot::default(),
            transaction_capacity,
            transaction_view_state_container: TransactionViewStateContainer::with_capacity(
                transaction_capacity,
            ),
            unprocessed_bundles: VecDeque::with_capacity(transaction_capacity),
            cost_model_buffered_bundles: VecDeque::with_capacity(transaction_capacity),
        }
    }

    pub fn unprocessed_bundles_len(&self) -> usize {
        self.unprocessed_bundles.len()
    }

    pub fn cost_model_buffered_bundles_len(&self) -> usize {
        self.cost_model_buffered_bundles.len()
    }

    pub fn num_packets_buffered(&self) -> usize {
        self.transaction_view_state_container.buffer_size()
    }

    /// Retries a bundle by inserting the transactions back into the transaction_view_state_container.
    /// The bundle is then pushed back to the cost_model_buffered_bundles queue.
    pub fn retry_bundle(&mut self, bundle: BundleStorageEntry) {
        for (container_id, transaction) in bundle.container_ids.iter().zip(bundle.transactions) {
            self.transaction_view_state_container
                .get_mut_transaction_state(*container_id)
                .unwrap()
                .retry_transaction(transaction);
        }
        self.cost_model_buffered_bundles
            .push_back(BundleTransactionId {
                container_ids: bundle.container_ids,
                sanitized_bank_id: bundle.sanitized_bank_id,
                sanitized_bank_slot: bundle.sanitized_bank_slot,
            });
    }

    /// Destroys a bundle by removing the transactions from the transaction_view_state_container.
    /// It's important that transactions in the BundleStorageEntry are not used after this call
    /// as it will lead to panic inside the TransactionViewStateContainer.
    pub fn destroy_bundle(&mut self, bundle: BundleStorageEntry) {
        for container_id in bundle.container_ids.into_iter() {
            self.transaction_view_state_container
                .remove_by_id(container_id);
        }
    }

    /// Pops a bundle from the unprocessed_bundles queue and returns it as a BundleStorageEntry.
    /// Returns None if there are no bundles to pop.
    pub fn pop_bundle(&mut self, slot: Slot, bank_id: BankId) -> Option<BundleStorageEntry> {
        if slot != self.last_slot {
            // the cost_model_buffered_bundles has the oldest bundles at the front of the queue
            // we need to pop from the back of that queue and insert to the front of the unprocessed_bundles queue so by the time we reach the front,
            // the oldest bundle is at the front of the unprocessed_bundles queue
            while let Some(bundle) = self.cost_model_buffered_bundles.pop_back() {
                self.unprocessed_bundles.push_front(bundle);
            }

            self.last_slot = slot;
        }

        // only want to pop from the unprocessed bundles queue and wait for slot boundary to refresh from cost_model_buffered_bundles
        while let Some(bundle) = self.unprocessed_bundles.pop_front() {
            if bundle.sanitized_bank_slot == slot && bundle.sanitized_bank_id != bank_id {
                for container_id in bundle.container_ids {
                    self.transaction_view_state_container
                        .remove_by_id(container_id);
                }
                continue;
            }

            let (bundle_transactions, bundle_max_ages): (
                SmallVec<[RuntimeTransactionView; 5]>,
                SmallVec<[MaxAge; 5]>,
            ) = bundle
                .container_ids
                .iter()
                .map(|id| {
                    self.transaction_view_state_container
                        .get_mut_transaction_state(*id)
                        .unwrap()
                        .take_transaction_for_scheduling()
                })
                .unzip();

            return Some(BundleStorageEntry {
                container_ids: bundle.container_ids,
                transactions: bundle_transactions,
                max_ages: bundle_max_ages,
                sanitized_bank_id: bundle.sanitized_bank_id,
                sanitized_bank_slot: bundle.sanitized_bank_slot,
            });
        }

        None
    }

    pub fn insert_bundle(
        &mut self,
        bundle: VerifiedPacketBundle,
        root_bank: &Bank,
        working_bank: &Bank,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<(), BundleStorageError> {
        let batch = bundle.take();

        // Packet checks
        if batch.is_empty() {
            return Err(BundleStorageError::EmptyBatch);
        }
        if batch.len() > Self::MAX_PACKETS_PER_BUNDLE {
            return Err(BundleStorageError::BundleTooLarge);
        }
        if let Some(idx) = batch
            .iter()
            .enumerate()
            .find_map(|(idx, packet)| packet.meta().discard().then_some(idx))
        {
            return Err(BundleStorageError::PacketMarkedDiscard(idx));
        }

        // Container checks
        if self
            .transaction_view_state_container
            .buffer_size()
            .saturating_add(batch.len())
            > self.transaction_capacity
        {
            return Err(BundleStorageError::ContainerFull);
        }

        let mut container_ids = SmallVec::<[usize; 5]>::new();
        let mut maybe_error = Ok(());
        let sanitize_config = sanitize_config();
        let transaction_account_lock_limit = working_bank
            .get_transaction_account_lock_limit()
            .min(root_bank.get_transaction_account_lock_limit());

        for (idx, packet) in batch.iter().enumerate() {
            // bundles shall contain all valid packets; checked above
            let packet_data = packet.data(..).unwrap();

            // try to insert the packet into the container
            let insert_result = match TransactionViewReceiveAndBuffer::try_handle_packet(
                Bytes::copy_from_slice(packet_data),
                root_bank,
                working_bank,
                transaction_account_lock_limit,
                &sanitize_config,
                blacklisted_accounts,
            ) {
                Ok(state) => self
                    .transaction_view_state_container
                    .try_insert_map_only(state),
                Err(e) => {
                    maybe_error = Err(e);
                    None
                }
            };
            if let Some(container_id) = insert_result {
                container_ids.push(container_id);
            } else {
                // any error shall rollback any transactions added to the container
                for container_id in container_ids.iter() {
                    self.transaction_view_state_container
                        .remove_by_id(*container_id);
                }
                return match maybe_error {
                    Err(e) => Err(BundleStorageError::PacketFilterError((e, idx))),
                    Ok(()) => Err(BundleStorageError::ContainerFull),
                };
            }
        }

        let is_duplicate_hashes = self.does_contain_duplicate_hashes(&container_ids);
        if is_duplicate_hashes {
            for container_id in container_ids.iter() {
                self.transaction_view_state_container
                    .remove_by_id(*container_id);
            }
            return Err(BundleStorageError::DuplicateTransaction);
        }

        self.unprocessed_bundles.push_back(BundleTransactionId {
            container_ids,
            sanitized_bank_id: working_bank.bank_id(),
            sanitized_bank_slot: working_bank.slot(),
        });

        Ok(())
    }

    fn does_contain_duplicate_hashes(&self, container_ids: &[usize]) -> bool {
        let mut transaction_hashes = ArrayVec::<_, { Self::MAX_PACKETS_PER_BUNDLE }>::new();
        for container_id in container_ids.iter() {
            let transaction_hash = self
                .transaction_view_state_container
                .get_transaction(*container_id)
                .unwrap()
                .message_hash();
            if transaction_hashes.contains(&transaction_hash) {
                return true;
            }
            transaction_hashes.push(transaction_hash);
        }
        false
    }

    pub fn clear(&mut self) {
        for bundle in self.unprocessed_bundles.drain(..) {
            for id in bundle.container_ids.iter() {
                self.transaction_view_state_container.remove_by_id(*id);
            }
        }
        for bundle in self.cost_model_buffered_bundles.drain(..) {
            for id in bundle.container_ids.iter() {
                self.transaction_view_state_container.remove_by_id(*id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        crate::{
            banking_stage::transaction_scheduler::{
                receive_and_buffer::PacketHandlingError,
                transaction_state_container::StateContainer,
            },
            bundle_stage::bundle_storage::{BundleStorage, BundleStorageError},
            packet_bundle::VerifiedPacketBundle,
        },
        ahash::{HashSet, HashSetExt},
        solana_account::AccountSharedData,
        solana_address_lookup_table_interface::{
            self as address_lookup_table,
            state::{AddressLookupTable, LookupTableMeta},
        },
        solana_genesis_config::GenesisConfig,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_leader_schedule::SlotLeader,
        solana_message::{AddressLoader, AddressLookupTableAccount, VersionedMessage, v0},
        solana_perf::packet::{BytesPacket, PacketBatch},
        solana_pubkey::Pubkey,
        solana_runtime::bank::{Bank, NewBankOptions},
        solana_signer::Signer,
        solana_system_interface::instruction as system_instruction,
        solana_transaction::{Transaction, versioned::VersionedTransaction},
        std::borrow::Cow,
    };

    pub fn test_tx() -> Transaction {
        let keypair1 = Keypair::new();
        let pubkey1 = keypair1.pubkey();
        solana_system_transaction::transfer(&keypair1, &pubkey1, 42, Hash::default())
    }

    #[test]
    fn test_bundle_alt_resolution_uses_root_bank() {
        let (root_bank, _bank_forks) =
            Bank::new_with_bank_forks_for_tests(&GenesisConfig::default());
        let working_bank = Bank::new_from_parent(
            root_bank.clone(),
            SlotLeader::new_unique(),
            root_bank.slot() + 1,
        );
        let payer = Keypair::new();
        let recipient = Pubkey::new_unique();
        let address_lookup_table_key = Pubkey::new_unique();
        let address_lookup_table = AddressLookupTable {
            meta: LookupTableMeta::default(),
            addresses: Cow::Borrowed(&[recipient]),
        };
        let data = address_lookup_table.serialize_for_tests().unwrap();
        let mut account =
            AccountSharedData::new(1, data.len(), &address_lookup_table::program::id());
        account.set_data_from_slice(&data);
        working_bank.store_account(&address_lookup_table_key, &account);

        let message = v0::Message::try_compile(
            &payer.pubkey(),
            &[system_instruction::transfer(&payer.pubkey(), &recipient, 1)],
            &[AddressLookupTableAccount {
                key: address_lookup_table_key,
                addresses: vec![recipient],
            }],
            working_bank.last_blockhash(),
        )
        .unwrap();

        assert!(
            AddressLoader::load_addresses(&working_bank, &message.address_table_lookups).is_ok()
        );

        let transaction =
            VersionedTransaction::try_new(VersionedMessage::V0(message), &[&payer]).unwrap();
        let packet = BytesPacket::from_data(transaction).unwrap();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet]));
        let mut bundle_storage = BundleStorage::with_capacity(1);

        assert_eq!(
            bundle_storage.insert_bundle(
                bundle,
                root_bank.as_ref(),
                &working_bank,
                &HashSet::new(),
            ),
            Err(BundleStorageError::PacketFilterError((
                PacketHandlingError::ALTResolution,
                0,
            )))
        );
    }

    #[test]
    fn test_bundle_vote_only_check_uses_working_bank() {
        let (root_bank, _bank_forks) =
            Bank::new_with_bank_forks_for_tests(&GenesisConfig::default());
        let working_bank = Bank::new_from_parent_with_options(
            root_bank.clone(),
            SlotLeader::new_unique(),
            root_bank.slot() + 1,
            NewBankOptions {
                vote_only_bank: true,
            },
        );
        let packet = BytesPacket::from_data(test_tx()).unwrap();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet]));
        let mut bundle_storage = BundleStorage::with_capacity(1);

        assert_eq!(
            bundle_storage.insert_bundle(
                bundle,
                root_bank.as_ref(),
                &working_bank,
                &HashSet::new(),
            ),
            Err(BundleStorageError::PacketFilterError((
                PacketHandlingError::Sanitization,
                0,
            )))
        );
    }

    #[test]
    fn test_bundle_too_large() {
        let mut bundle_storage = BundleStorage::with_capacity(10);

        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let packets: Vec<BytesPacket> = (0..BundleStorage::MAX_PACKETS_PER_BUNDLE + 1)
            .map(|_| BytesPacket::from_data(test_tx()).unwrap())
            .collect();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(packets));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());

        assert_matches!(result, Err(BundleStorageError::BundleTooLarge));
        assert_eq!(bundle_storage.unprocessed_bundles.len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles.len(), 0);
        assert!(bundle_storage.transaction_view_state_container.is_empty());
    }

    #[test]
    fn test_bundle_marked_discard() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let packet_1 = BytesPacket::from_data(test_tx()).unwrap();
        let mut packet_2 = BytesPacket::from_data(test_tx()).unwrap();
        packet_2.meta_mut().set_discard(true);
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_matches!(result, Err(BundleStorageError::PacketMarkedDiscard(1)));
    }

    #[test]
    fn test_bundle_storage_exceeds_capacity() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());

        for i in 0..10 {
            let packet = BytesPacket::from_data(test_tx()).unwrap();
            let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet]));
            bundle_storage
                .insert_bundle(bundle, &bank, &bank, &HashSet::new())
                .unwrap();
            assert_eq!(bundle_storage.unprocessed_bundles.len(), i + 1);
            assert_eq!(
                bundle_storage
                    .transaction_view_state_container
                    .buffer_size(),
                i + 1
            );
        }

        let packet = BytesPacket::from_data(test_tx()).unwrap();

        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_eq!(result, Err(BundleStorageError::ContainerFull));
        assert_eq!(bundle_storage.unprocessed_bundles.len(), 10);
        assert_eq!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size(),
            10
        );
    }

    #[test]
    fn test_bundle_empty() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_matches!(result, Err(BundleStorageError::EmptyBatch));
    }

    #[test]
    fn test_bundle_duplicate_hashes() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let packet_1 = BytesPacket::from_data(test_tx()).unwrap();
        let packet_2 = packet_1.clone();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_matches!(result, Err(BundleStorageError::DuplicateTransaction));
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 0
        );
        assert!(bundle_storage.unprocessed_bundles.is_empty());
        assert!(bundle_storage.cost_model_buffered_bundles.is_empty());
    }

    #[test]
    fn test_retry_bundle() {
        let mut bundle_storage = BundleStorage::with_capacity(10);

        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let bank_id = bank.bank_id();
        let packet_1 = BytesPacket::from_data(test_tx()).unwrap();
        let packet_2 = BytesPacket::from_data(test_tx()).unwrap();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert!(result.is_ok());

        let bundle_storage_entry = bundle_storage.pop_bundle(bank.slot(), bank_id).unwrap();
        bundle_storage.retry_bundle(bundle_storage_entry);

        assert!(bundle_storage.pop_bundle(bank.slot(), bank_id).is_none());
        assert!(bundle_storage.unprocessed_bundles.is_empty());
        assert_eq!(bundle_storage.cost_model_buffered_bundles.len(), 1);
        assert_eq!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size(),
            2
        );

        let bundle = bundle_storage.pop_bundle(bank.slot() + 1, bank_id).unwrap();
        bundle_storage.destroy_bundle(bundle);

        let packet = BytesPacket::from_data(test_tx()).unwrap();
        bundle_storage
            .insert_bundle(
                VerifiedPacketBundle::new(PacketBatch::from(vec![packet])),
                &bank,
                &bank,
                &HashSet::new(),
            )
            .unwrap();

        assert!(
            bundle_storage
                .pop_bundle(bank.slot(), bank.bank_id() + 1)
                .is_none()
        );
        assert!(bundle_storage.transaction_view_state_container.is_empty());
    }

    #[test]
    fn test_bundle_blacklisted_account() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let tx = test_tx();
        let pubkey = tx.message().account_keys[0];
        let blacklisted_accounts = HashSet::from_iter([pubkey]);
        let packet = BytesPacket::from_data(tx).unwrap();
        let bundle = VerifiedPacketBundle::new(PacketBatch::from(vec![packet]));
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &blacklisted_accounts);
        assert_matches!(
            result,
            Err(BundleStorageError::PacketFilterError((
                PacketHandlingError::FilterKey,
                0
            )))
        );
    }

    #[test]
    fn test_retry_bundle_ordering_preserved() {
        let mut bundle_storage = BundleStorage::with_capacity(100);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let bank_id = bank.bank_id();

        let tx_1 = test_tx();
        let tx_2 = test_tx();
        let tx_3 = test_tx();
        let tx_4 = test_tx();

        let packet_batch_1 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_1).unwrap(),
        ]));
        let packet_batch_2 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_2).unwrap(),
        ]));
        let packet_batch_3 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_3).unwrap(),
        ]));
        let packet_batch_4 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_4).unwrap(),
        ]));

        bundle_storage
            .insert_bundle(packet_batch_1, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_2, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_3, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_4, &bank, &bank, &HashSet::new())
            .unwrap();

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot(), bank_id).unwrap();
        assert_eq!(
            bundle_storage_entry_1.transactions[0].signatures()[0],
            tx_1.signatures[0]
        );
        let bundle_storage_entry_2 = bundle_storage.pop_bundle(bank.slot(), bank_id).unwrap();
        assert_eq!(
            bundle_storage_entry_2.transactions[0].signatures()[0],
            tx_2.signatures[0]
        );

        bundle_storage.retry_bundle(bundle_storage_entry_1);
        bundle_storage.destroy_bundle(bundle_storage_entry_2);

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot() + 1, bank_id).unwrap();
        assert_eq!(
            bundle_storage_entry_1.transactions[0].signatures()[0],
            tx_1.signatures[0]
        );
        let bundle_storage_entry_3 = bundle_storage.pop_bundle(bank.slot() + 1, bank_id).unwrap();
        assert_eq!(
            bundle_storage_entry_3.transactions[0].signatures()[0],
            tx_3.signatures[0]
        );
        let bundle_storage_entry_4 = bundle_storage.pop_bundle(bank.slot() + 1, bank_id).unwrap();
        assert_eq!(
            bundle_storage_entry_4.transactions[0].signatures()[0],
            tx_4.signatures[0]
        );
    }

    #[test]
    fn test_destroy_bundle() {
        let mut bundle_storage = BundleStorage::with_capacity(100);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let bank_id = bank.bank_id();

        let tx_1 = test_tx();
        let tx_2 = test_tx();

        let packet_batch_1 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_1).unwrap(),
        ]));
        let packet_batch_2 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_2).unwrap(),
        ]));

        bundle_storage
            .insert_bundle(packet_batch_1, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_2, &bank, &bank, &HashSet::new())
            .unwrap();

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot(), bank_id).unwrap();
        bundle_storage.destroy_bundle(bundle_storage_entry_1);
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 1
        );
        let bundle_storage_entry_2 = bundle_storage.pop_bundle(bank.slot(), bank_id).unwrap();
        bundle_storage.destroy_bundle(bundle_storage_entry_2);
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 0
        );
    }

    #[test]
    fn test_clear() {
        let mut bundle_storage = BundleStorage::with_capacity(100);
        let bank = Bank::new_for_tests(&GenesisConfig::default());

        let tx_1 = test_tx();
        let tx_2 = test_tx();

        let packet_batch_1 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_1).unwrap(),
        ]));
        let packet_batch_2 = VerifiedPacketBundle::new(PacketBatch::from(vec![
            BytesPacket::from_data(&tx_2).unwrap(),
        ]));

        bundle_storage
            .insert_bundle(packet_batch_1, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_2, &bank, &bank, &HashSet::new())
            .unwrap();

        bundle_storage.clear();
        assert!(bundle_storage.unprocessed_bundles.is_empty());
        assert!(bundle_storage.cost_model_buffered_bundles.is_empty());
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 0
        );
    }
}
