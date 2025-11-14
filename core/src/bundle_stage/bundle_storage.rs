use crate::banking_stage::{
    scheduler_messages::MaxAge,
    transaction_scheduler::transaction_state_container::RuntimeTransactionView,
};
use arrayvec::ArrayVec;
use solana_runtime_transaction::transaction_meta::StaticMeta;
use solana_svm_transaction::svm_message::SVMMessage;

use {
    crate::{
        banking_stage::transaction_scheduler::{
            receive_and_buffer::{
                calculate_max_age, calculate_priority_and_cost, translate_to_runtime_view,
                PacketHandlingError,
            },
            transaction_state::TransactionState,
            transaction_state_container::{
                SharedBytes, StateContainer, TransactionViewState, TransactionViewStateContainer,
            },
        },
        packet_bundle::PacketBundle,
    },
    ahash::HashSet,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_clock::Slot,
    solana_fee_structure::FeeBudgetLimits,
    solana_hash::Hash,
    solana_pubkey::Pubkey,
    solana_runtime::bank::Bank,
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
    container_ids: Vec<usize>,
}

struct BundleStorageEntry {
    container_ids: Vec<usize>,
    transactions: Vec<RuntimeTransactionView>,
    max_ages: Vec<MaxAge>,
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
    pub const MAX_PACKETS_PER_BUNDLE: usize = 5;

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

    /// Retries a bundle by inserting the transactions back into the transaction_view_state_container.
    /// The bundle is then pushed back to the cost_model_buffered_bundles queue.
    pub fn retry_bundle(&mut self, bundle: BundleStorageEntry) {
        for (container_id, transaction) in bundle
            .container_ids
            .iter()
            .zip(bundle.transactions.into_iter())
        {
            self.transaction_view_state_container
                .get_mut_transaction_state(*container_id)
                .unwrap()
                .retry_transaction(transaction);
        }
        self.cost_model_buffered_bundles
            .push_back(BundleTransactionId {
                container_ids: bundle.container_ids,
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
    pub fn pop_bundle(&mut self, slot: Slot) -> Option<BundleStorageEntry> {
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
        let bundle = match self.unprocessed_bundles.pop_front() {
            Some(bundle) => bundle,
            None => return None,
        };

        let (bundle_transactions, bundle_max_ages): (Vec<RuntimeTransactionView>, Vec<MaxAge>) =
            bundle
                .container_ids
                .iter()
                .map(|id| {
                    self.transaction_view_state_container
                        .get_mut_transaction_state(*id)
                        .unwrap()
                        .take_transaction_for_scheduling()
                })
                .collect();

        Some(BundleStorageEntry {
            container_ids: bundle.container_ids,
            transactions: bundle_transactions,
            max_ages: bundle_max_ages,
        })
    }

    pub fn insert_bundle(
        &mut self,
        bundle: PacketBundle,
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

        let mut container_ids: Vec<usize> = Vec::with_capacity(batch.len());
        let mut maybe_error = Ok(());

        for (idx, packet) in batch.iter().enumerate() {
            // bundles shall contain all valid packets; checked above
            let packet_data = packet.data(..).unwrap();

            // try to insert the packet into the container
            if let Some(container_id) = self
                .transaction_view_state_container
                .try_insert_map_only_with_data(packet_data, |bytes| {
                    match Self::try_handle_packet(
                        bytes,
                        root_bank,
                        working_bank,
                        working_bank
                            .feature_set
                            .is_active(&agave_feature_set::static_instruction_limit::id()),
                        working_bank.get_transaction_account_lock_limit(),
                        &blacklisted_accounts,
                    ) {
                        Ok(state) => Ok(state),
                        Err(e) => {
                            maybe_error = Err(e);
                            Err(())
                        }
                    }
                })
            {
                container_ids.push(container_id);
            } else {
                // any error shall rollback any transactions added to the container
                for container_id in container_ids.iter() {
                    self.transaction_view_state_container
                        .remove_by_id(*container_id);
                }
                return Err(BundleStorageError::PacketFilterError((
                    maybe_error.unwrap_err(),
                    idx,
                )));
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

        self.unprocessed_bundles
            .push_back(BundleTransactionId { container_ids });

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

    fn try_handle_packet(
        bytes: SharedBytes,
        root_bank: &Bank,
        working_bank: &Bank,
        enable_static_instruction_limit: bool,
        transaction_account_lock_limit: usize,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Result<TransactionViewState, PacketHandlingError> {
        let (view, deactivation_slot) = translate_to_runtime_view(
            bytes,
            root_bank,
            enable_static_instruction_limit,
            transaction_account_lock_limit,
        )?;
        if validate_account_locks(
            view.account_keys(),
            root_bank.get_transaction_account_lock_limit(),
        )
        .is_err()
        {
            return Err(PacketHandlingError::LockValidation);
        }

        if view
            .account_keys()
            .iter()
            .any(|account| blacklisted_accounts.contains(account))
        {
            return Err(PacketHandlingError::BlacklistedAccount);
        }

        let Ok(compute_budget_limits) = view
            .compute_budget_instruction_details()
            .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set)
        else {
            return Err(PacketHandlingError::ComputeBudget);
        };

        let max_age = calculate_max_age(root_bank.epoch(), deactivation_slot, root_bank.slot());
        let fee_budget_limits = FeeBudgetLimits::from(compute_budget_limits);
        let (priority, cost) = calculate_priority_and_cost(&view, &fee_budget_limits, working_bank);

        Ok(TransactionState::new(view, max_age, priority, cost))
    }
}

#[cfg(test)]
mod tests {
    use ahash::{HashSet, HashSetExt};
    use solana_genesis_config::GenesisConfig;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_perf::packet::{BytesPacket, PacketBatch};
    use solana_runtime::bank::Bank;
    use solana_signer::Signer;
    use solana_transaction::Transaction;

    use crate::banking_stage::transaction_scheduler::receive_and_buffer::PacketHandlingError;
    use crate::banking_stage::transaction_scheduler::transaction_state_container::StateContainer;
    use crate::bundle_stage::bundle_storage::{BundleStorage, BundleStorageError};
    use crate::packet_bundle::PacketBundle;

    pub fn test_tx() -> Transaction {
        let keypair1 = Keypair::new();
        let pubkey1 = keypair1.pubkey();
        solana_system_transaction::transfer(&keypair1, &pubkey1, 42, Hash::default())
    }

    #[test]
    fn test_bundle_too_large() {
        let mut bundle_storage = BundleStorage::with_capacity(10);

        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let packets: Vec<BytesPacket> = (0..BundleStorage::MAX_PACKETS_PER_BUNDLE + 1)
            .map(|_| BytesPacket::from_data(None, &test_tx()).unwrap())
            .collect();
        let bundle = PacketBundle::new(PacketBatch::from(packets), "".to_string());
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
        let packet_1 = BytesPacket::from_data(None, &test_tx()).unwrap();
        let mut packet_2 = BytesPacket::from_data(None, &test_tx()).unwrap();
        packet_2.meta_mut().set_discard(true);
        let bundle = PacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]), "".to_string());
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_matches!(result, Err(BundleStorageError::PacketMarkedDiscard(1)));
    }

    #[test]
    fn test_bundle_storage_exceeds_capacity() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());

        for i in 0..10 {
            let packet = BytesPacket::from_data(None, &test_tx()).unwrap();
            let bundle = PacketBundle::new(PacketBatch::from(vec![packet]), "".to_string());
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

        let packet = BytesPacket::from_data(None, &test_tx()).unwrap();

        let bundle = PacketBundle::new(PacketBatch::from(vec![packet]), "".to_string());
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
        let bundle = PacketBundle::new(PacketBatch::from(vec![]), "".to_string());
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert_matches!(result, Err(BundleStorageError::EmptyBatch));
    }

    #[test]
    fn test_bundle_duplicate_hashes() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let packet_1 = BytesPacket::from_data(None, &test_tx()).unwrap();
        let packet_2 = packet_1.clone();
        let bundle = PacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]), "".to_string());
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
        let packet_1 = BytesPacket::from_data(None, &test_tx()).unwrap();
        let packet_2 = BytesPacket::from_data(None, &test_tx()).unwrap();
        let bundle = PacketBundle::new(PacketBatch::from(vec![packet_1, packet_2]), "".to_string());
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &HashSet::new());
        assert!(result.is_ok());

        let bundle_storage_entry = bundle_storage.pop_bundle(bank.slot()).unwrap();
        bundle_storage.retry_bundle(bundle_storage_entry);

        assert!(bundle_storage.pop_bundle(bank.slot()).is_none());
        assert!(bundle_storage.unprocessed_bundles.is_empty());
        assert_eq!(bundle_storage.cost_model_buffered_bundles.len(), 1);
        assert_eq!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size(),
            2
        );

        bundle_storage.pop_bundle(bank.slot() + 1).unwrap();
    }

    #[test]
    fn test_bundle_blacklisted_account() {
        let mut bundle_storage = BundleStorage::with_capacity(10);
        let bank = Bank::new_for_tests(&GenesisConfig::default());
        let tx = test_tx();
        let pubkey = tx.message().account_keys[0];
        let blacklisted_accounts = HashSet::from_iter([pubkey]);
        let packet = BytesPacket::from_data(None, tx).unwrap();
        let bundle = PacketBundle::new(PacketBatch::from(vec![packet]), "".to_string());
        let result = bundle_storage.insert_bundle(bundle, &bank, &bank, &blacklisted_accounts);
        assert_matches!(
            result,
            Err(BundleStorageError::PacketFilterError((
                PacketHandlingError::BlacklistedAccount,
                0
            )))
        );
    }

    #[test]
    fn test_retry_bundle_ordering_preserved() {
        let mut bundle_storage = BundleStorage::with_capacity(100);
        let bank = Bank::new_for_tests(&GenesisConfig::default());

        let tx_1 = test_tx();
        let tx_2 = test_tx();
        let tx_3 = test_tx();
        let tx_4 = test_tx();

        let packet_batch_1 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_1).unwrap()]),
            "".to_string(),
        );
        let packet_batch_2 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_2).unwrap()]),
            "".to_string(),
        );
        let packet_batch_3 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_3).unwrap()]),
            "".to_string(),
        );
        let packet_batch_4 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_4).unwrap()]),
            "".to_string(),
        );

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

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot()).unwrap();
        assert_eq!(
            bundle_storage_entry_1.transactions[0].signatures()[0],
            tx_1.signatures[0]
        );
        let bundle_storage_entry_2 = bundle_storage.pop_bundle(bank.slot()).unwrap();
        assert_eq!(
            bundle_storage_entry_2.transactions[0].signatures()[0],
            tx_2.signatures[0]
        );

        bundle_storage.retry_bundle(bundle_storage_entry_1);
        bundle_storage.destroy_bundle(bundle_storage_entry_2);

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot() + 1).unwrap();
        assert_eq!(
            bundle_storage_entry_1.transactions[0].signatures()[0],
            tx_1.signatures[0]
        );
        let bundle_storage_entry_3 = bundle_storage.pop_bundle(bank.slot() + 1).unwrap();
        assert_eq!(
            bundle_storage_entry_3.transactions[0].signatures()[0],
            tx_3.signatures[0]
        );
        let bundle_storage_entry_4 = bundle_storage.pop_bundle(bank.slot() + 1).unwrap();
        assert_eq!(
            bundle_storage_entry_4.transactions[0].signatures()[0],
            tx_4.signatures[0]
        );
    }

    #[test]
    fn test_destroy_bundle() {
        let mut bundle_storage = BundleStorage::with_capacity(100);
        let bank = Bank::new_for_tests(&GenesisConfig::default());

        let tx_1 = test_tx();
        let tx_2 = test_tx();

        let packet_batch_1 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_1).unwrap()]),
            "".to_string(),
        );
        let packet_batch_2 = PacketBundle::new(
            PacketBatch::from(vec![BytesPacket::from_data(None, &tx_2).unwrap()]),
            "".to_string(),
        );

        bundle_storage
            .insert_bundle(packet_batch_1, &bank, &bank, &HashSet::new())
            .unwrap();
        bundle_storage
            .insert_bundle(packet_batch_2, &bank, &bank, &HashSet::new())
            .unwrap();

        let bundle_storage_entry_1 = bundle_storage.pop_bundle(bank.slot()).unwrap();
        bundle_storage.destroy_bundle(bundle_storage_entry_1);
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 1
        );
        let bundle_storage_entry_2 = bundle_storage.pop_bundle(bank.slot()).unwrap();
        bundle_storage.destroy_bundle(bundle_storage_entry_2);
        assert!(
            bundle_storage
                .transaction_view_state_container
                .buffer_size()
                == 0
        );
    }
}
