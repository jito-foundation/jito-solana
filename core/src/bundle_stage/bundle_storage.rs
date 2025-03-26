use {
    crate::{
        bundle_stage::bundle_stage_leader_metrics::BundleStageLeaderMetrics,
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
    },
    solana_bundle::{
        bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
    },
    solana_runtime::bank::Bank,
    solana_sdk::{clock::Slot, feature_set, pubkey::Pubkey},
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    std::{
        collections::{HashSet, VecDeque},
        sync::Arc,
        time::Instant,
    },
};

pub struct InsertPacketBundlesSummary {
    pub num_bundles_inserted: usize,
    pub num_packets_inserted: usize,
    pub num_bundles_dropped: usize,
    pub num_packets_dropped: usize,
}

/// Bundle storage has two deques: one for unprocessed bundles and another for ones that exceeded
/// the cost model and need to get retried next slot.
#[derive(Debug)]
pub struct BundleStorage {
    last_update_slot: Slot,
    unprocessed_bundle_storage: VecDeque<ImmutableDeserializedBundle>,
    // Storage for bundles that exceeded the cost model for the slot they were last attempted
    // execution on
    cost_model_buffered_bundle_storage: VecDeque<ImmutableDeserializedBundle>,
}

impl Default for BundleStorage {
    fn default() -> Self {
        Self {
            last_update_slot: Slot::default(),
            unprocessed_bundle_storage: VecDeque::with_capacity(Self::BUNDLE_STORAGE_CAPACITY),
            cost_model_buffered_bundle_storage: VecDeque::with_capacity(
                Self::BUNDLE_STORAGE_CAPACITY,
            ),
        }
    }
}

impl BundleStorage {
    pub const BUNDLE_STORAGE_CAPACITY: usize = 1000;

    pub fn is_empty(&self) -> bool {
        self.unprocessed_bundle_storage.is_empty()
            && self.cost_model_buffered_bundle_storage.is_empty()
    }

    pub fn len(&self) -> usize {
        self.unprocessed_bundles_len() + self.cost_model_buffered_bundles_len()
    }

    pub fn unprocessed_bundles_len(&self) -> usize {
        self.unprocessed_bundle_storage.len()
    }

    pub fn unprocessed_packets_len(&self) -> usize {
        self.unprocessed_bundle_storage
            .iter()
            .map(|b| b.len())
            .sum::<usize>()
    }

    pub(crate) fn cost_model_buffered_bundles_len(&self) -> usize {
        self.cost_model_buffered_bundle_storage.len()
    }

    pub(crate) fn cost_model_buffered_packets_len(&self) -> usize {
        self.cost_model_buffered_bundle_storage
            .iter()
            .map(|b| b.len())
            .sum()
    }

    pub(crate) fn max_receive_size(&self) -> usize {
        self.unprocessed_bundle_storage.capacity() - self.unprocessed_bundle_storage.len()
    }

    /// Returns the number of unprocessed bundles + cost model buffered cleared
    pub fn reset(&mut self) -> (usize, usize) {
        let num_unprocessed_bundles = self.unprocessed_bundle_storage.len();
        let num_cost_model_buffered_bundles = self.cost_model_buffered_bundle_storage.len();
        self.unprocessed_bundle_storage.clear();
        self.cost_model_buffered_bundle_storage.clear();
        (num_unprocessed_bundles, num_cost_model_buffered_bundles)
    }

    fn insert_bundles(
        deque: &mut VecDeque<ImmutableDeserializedBundle>,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
        push_back: bool,
    ) -> InsertPacketBundlesSummary {
        // deque should be initialized with size [Self::BUNDLE_STORAGE_CAPACITY]
        let deque_free_space = Self::BUNDLE_STORAGE_CAPACITY
            .checked_sub(deque.len())
            .unwrap();
        let num_bundles_inserted = std::cmp::min(deque_free_space, deserialized_bundles.len());
        let num_bundles_dropped = deserialized_bundles
            .len()
            .checked_sub(num_bundles_inserted)
            .unwrap();
        let num_packets_inserted = deserialized_bundles
            .iter()
            .take(num_bundles_inserted)
            .map(|b| b.len())
            .sum::<usize>();
        let num_packets_dropped = deserialized_bundles
            .iter()
            .skip(num_bundles_inserted)
            .map(|b| b.len())
            .sum::<usize>();

        let to_insert = deserialized_bundles.into_iter().take(num_bundles_inserted);
        if push_back {
            deque.extend(to_insert)
        } else {
            to_insert.for_each(|b| deque.push_front(b));
        }

        InsertPacketBundlesSummary {
            num_bundles_inserted,
            num_packets_inserted,
            num_bundles_dropped,
            num_packets_dropped,
        }
    }

    fn push_front_unprocessed_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.unprocessed_bundle_storage,
            deserialized_bundles,
            false,
        )
    }

    fn push_back_cost_model_buffered_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.cost_model_buffered_bundle_storage,
            deserialized_bundles,
            true,
        )
    }

    pub fn insert_unprocessed_bundles(
        &mut self,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
        push_back: bool,
    ) -> InsertPacketBundlesSummary {
        Self::insert_bundles(
            &mut self.unprocessed_bundle_storage,
            deserialized_bundles,
            push_back,
        )
    }

    /// Drains bundles from the queue, sanitizes them to prepare for execution, executes them by
    /// calling `processing_function`, then potentially rebuffer them.
    pub fn process_bundles<F>(
        &mut self,
        bank: Arc<Bank>,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        blacklisted_accounts: &HashSet<Pubkey>,
        mut processing_function: F,
    ) -> bool
    where
        F: FnMut(
            &[(ImmutableDeserializedBundle, SanitizedBundle)],
            &mut BundleStageLeaderMetrics,
        ) -> Vec<Result<(), BundleExecutionError>>,
    {
        let sanitized_bundles = self.drain_and_sanitize_bundles(
            bank,
            bundle_stage_leader_metrics,
            blacklisted_accounts,
        );

        debug!("processing {} bundles", sanitized_bundles.len());
        let bundle_execution_results =
            processing_function(&sanitized_bundles, bundle_stage_leader_metrics);

        let mut is_slot_over = false;

        let mut rebuffered_bundles = Vec::new();

        sanitized_bundles
            .into_iter()
            .zip(bundle_execution_results)
            .for_each(
                |((deserialized_bundle, sanitized_bundle), result)| match result {
                    Ok(_) => {
                        debug!("bundle={} executed ok", sanitized_bundle.bundle_id);
                        // yippee
                    }
                    Err(BundleExecutionError::PohRecordError(e)) => {
                        // buffer the bundle to the front of the queue to be attempted next slot
                        debug!(
                            "bundle={} poh record error: {e:?}",
                            sanitized_bundle.bundle_id
                        );
                        rebuffered_bundles.push(deserialized_bundle);
                        is_slot_over = true;
                    }
                    Err(BundleExecutionError::BankProcessingTimeLimitReached) => {
                        // buffer the bundle to the front of the queue to be attempted next slot
                        debug!("bundle={} bank processing done", sanitized_bundle.bundle_id);
                        rebuffered_bundles.push(deserialized_bundle);
                        is_slot_over = true;
                    }
                    Err(BundleExecutionError::ExceedsCostModel) => {
                        // cost model buffered bundles contain most recent bundles at the front of the queue
                        debug!(
                            "bundle={} exceeds cost model, rebuffering",
                            sanitized_bundle.bundle_id
                        );
                        self.push_back_cost_model_buffered_bundles(vec![deserialized_bundle]);
                    }
                    Err(BundleExecutionError::TransactionFailure(
                        LoadAndExecuteBundleError::ProcessingTimeExceeded(_),
                    )) => {
                        // these are treated the same as exceeds cost model and are rebuferred to be completed
                        // at the beginning of the next slot
                        debug!(
                            "bundle={} processing time exceeded, rebuffering",
                            sanitized_bundle.bundle_id
                        );
                        self.push_back_cost_model_buffered_bundles(vec![deserialized_bundle]);
                    }
                    Err(BundleExecutionError::TransactionFailure(e)) => {
                        debug!(
                            "bundle={} execution error: {:?}",
                            sanitized_bundle.bundle_id, e
                        );
                        // do nothing
                    }
                    Err(BundleExecutionError::TipError(e)) => {
                        debug!("bundle={} tip error: {}", sanitized_bundle.bundle_id, e);
                        // Tip errors are _typically_ due to misconfiguration (except for poh record error, bank processing error, exceeds cost model)
                        // in order to prevent buffering too many bundles, we'll just drop the bundle
                    }
                    Err(BundleExecutionError::LockError) => {
                        // lock errors are irrecoverable due to malformed transactions
                        debug!("bundle={} lock error", sanitized_bundle.bundle_id);
                    }
                },
            );

        // rebuffered bundles are pushed onto deque in reverse order so the first bundle is at the front
        for bundle in rebuffered_bundles.into_iter().rev() {
            self.push_front_unprocessed_bundles(vec![bundle]);
        }

        is_slot_over
    }

    /// Drains the unprocessed_bundle_storage, converting bundle packets into SanitizedBundles
    fn drain_and_sanitize_bundles(
        &mut self,
        bank: Arc<Bank>,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        blacklisted_accounts: &HashSet<Pubkey>,
    ) -> Vec<(ImmutableDeserializedBundle, SanitizedBundle)> {
        let mut error_metrics = TransactionErrorMetrics::default();

        let start = Instant::now();

        let mut sanitized_bundles = Vec::new();

        let move_precompile_verification_to_svm = bank
            .feature_set
            .is_active(&feature_set::move_precompile_verification_to_svm::id());

        // on new slot, drain anything that was buffered from last slot
        if bank.slot() != self.last_update_slot {
            sanitized_bundles.extend(
                self.cost_model_buffered_bundle_storage
                    .drain(..)
                    .filter_map(|packet_bundle| {
                        let r = packet_bundle.build_sanitized_bundle(
                            &bank,
                            blacklisted_accounts,
                            &mut error_metrics,
                            move_precompile_verification_to_svm,
                        );
                        bundle_stage_leader_metrics
                            .bundle_stage_metrics_tracker()
                            .increment_sanitize_transaction_result(&r);

                        match r {
                            Ok(sanitized_bundle) => Some((packet_bundle, sanitized_bundle)),
                            Err(e) => {
                                debug!(
                                    "bundle id: {} error sanitizing: {}",
                                    packet_bundle.bundle_id(),
                                    e
                                );
                                None
                            }
                        }
                    }),
            );

            self.last_update_slot = bank.slot();
        }

        sanitized_bundles.extend(self.unprocessed_bundle_storage.drain(..).filter_map(
            |packet_bundle| {
                let r = packet_bundle.build_sanitized_bundle(
                    &bank,
                    blacklisted_accounts,
                    &mut error_metrics,
                    move_precompile_verification_to_svm,
                );
                bundle_stage_leader_metrics
                    .bundle_stage_metrics_tracker()
                    .increment_sanitize_transaction_result(&r);
                match r {
                    Ok(sanitized_bundle) => Some((packet_bundle, sanitized_bundle)),
                    Err(e) => {
                        debug!(
                            "bundle id: {} error sanitizing: {}",
                            packet_bundle.bundle_id(),
                            e
                        );
                        None
                    }
                }
            },
        ));

        let elapsed = start.elapsed().as_micros();
        bundle_stage_leader_metrics
            .bundle_stage_metrics_tracker()
            .increment_sanitize_bundle_elapsed_us(elapsed as u64);
        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_transactions_from_packets_us(elapsed as u64);

        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .accumulate_transaction_errors(&error_metrics);

        sanitized_bundles
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        itertools::iproduct,
        solana_ledger::genesis_utils::{create_genesis_config, GenesisConfigInfo},
        solana_perf::packet::{Packet, PacketFlags},
        solana_runtime::genesis_utils,
        solana_sdk::{
            hash::Hash,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::Transaction,
        },
        solana_vote_program::{
            vote_state::TowerSync, vote_transaction::new_tower_sync_transaction,
        },
        std::error::Error,
    };

    #[test]
    fn test_filter_processed_packets() {
        let retryable_indexes = [0, 1, 2, 3];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert!(non_retryable_indexes.is_empty());

        let retryable_indexes = [0, 1, 2, 3, 5];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert_eq!(non_retryable_indexes, vec![(4, 5)]);

        let retryable_indexes = [1, 2, 3];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert_eq!(non_retryable_indexes, vec![(0, 1)]);

        let retryable_indexes = [1, 2, 3, 5];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert_eq!(non_retryable_indexes, vec![(0, 1), (4, 5)]);

        let retryable_indexes = [1, 2, 3, 5, 8];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert_eq!(non_retryable_indexes, vec![(0, 1), (4, 5), (6, 8)]);

        let retryable_indexes = [1, 2, 3, 5, 8, 8];
        let mut non_retryable_indexes = vec![];
        let f = |start, end| {
            non_retryable_indexes.push((start, end));
        };
        filter_processed_packets(retryable_indexes.iter(), f);
        assert_eq!(non_retryable_indexes, vec![(0, 1), (4, 5), (6, 8)]);
    }

    #[test]
    fn test_filter_and_forward_with_account_limits() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10);
        let (current_bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);

        let simple_transactions: Vec<Transaction> = (0..256)
            .map(|_id| {
                // packets are deserialized upon receiving, failed packets will not be
                // forwarded; Therefore we need to create real packets here.
                let key1 = Keypair::new();
                system_transaction::transfer(
                    &mint_keypair,
                    &key1.pubkey(),
                    genesis_config.rent.minimum_balance(0),
                    genesis_config.hash(),
                )
            })
            .collect_vec();

        let mut packets: Vec<DeserializedPacket> = simple_transactions
            .iter()
            .enumerate()
            .map(|(packets_id, transaction)| {
                let mut p = Packet::from_data(None, transaction).unwrap();
                p.meta_mut().port = packets_id as u16;
                DeserializedPacket::new(p).unwrap()
            })
            .collect_vec();

        // all packets are forwarded
        {
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let mut transaction_storage = UnprocessedTransactionStorage::new_transaction_storage(
                buffered_packet_batches,
                ThreadType::Transactions,
            );
            let mut forward_packet_batches_by_accounts =
                ForwardPacketBatchesByAccounts::new_with_default_batch_limits();

            let FilterForwardingResults {
                total_forwardable_packets,
                ..
            } = transaction_storage.filter_forwardable_packets_and_add_batches(
                current_bank.clone(),
                &mut forward_packet_batches_by_accounts,
            );
            assert_eq!(total_forwardable_packets, 256);

            // packets in a batch are forwarded in arbitrary order; verify the ports match after
            // sorting
            let expected_ports: Vec<_> = (0..256).collect();
            let mut forwarded_ports: Vec<_> = forward_packet_batches_by_accounts
                .iter_batches()
                .flat_map(|batch| batch.get_forwardable_packets().map(|p| p.meta().port))
                .collect();
            forwarded_ports.sort_unstable();
            assert_eq!(expected_ports, forwarded_ports);
        }

        // some packets are forwarded
        {
            let num_already_forwarded = 16;
            for packet in &mut packets[0..num_already_forwarded] {
                packet.forwarded = true;
            }
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let mut transaction_storage = UnprocessedTransactionStorage::new_transaction_storage(
                buffered_packet_batches,
                ThreadType::Transactions,
            );
            let mut forward_packet_batches_by_accounts =
                ForwardPacketBatchesByAccounts::new_with_default_batch_limits();
            let FilterForwardingResults {
                total_forwardable_packets,
                ..
            } = transaction_storage.filter_forwardable_packets_and_add_batches(
                current_bank.clone(),
                &mut forward_packet_batches_by_accounts,
            );
            assert_eq!(
                total_forwardable_packets,
                packets.len() - num_already_forwarded
            );
        }

        // some packets are invalid (already processed)
        {
            let num_already_processed = 16;
            for tx in &simple_transactions[0..num_already_processed] {
                assert_eq!(current_bank.process_transaction(tx), Ok(()));
            }
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let mut transaction_storage = UnprocessedTransactionStorage::new_transaction_storage(
                buffered_packet_batches,
                ThreadType::Transactions,
            );
            let mut forward_packet_batches_by_accounts =
                ForwardPacketBatchesByAccounts::new_with_default_batch_limits();
            let FilterForwardingResults {
                total_forwardable_packets,
                ..
            } = transaction_storage.filter_forwardable_packets_and_add_batches(
                current_bank,
                &mut forward_packet_batches_by_accounts,
            );
            assert_eq!(
                total_forwardable_packets,
                packets.len() - num_already_processed
            );
        }
    }

    #[test]
    fn test_unprocessed_transaction_storage_insert() -> Result<(), Box<dyn Error>> {
        let keypair = Keypair::new();
        let vote_keypair = Keypair::new();
        let pubkey = solana_pubkey::new_rand();

        let small_transfer = Packet::from_data(
            None,
            system_transaction::transfer(&keypair, &pubkey, 1, Hash::new_unique()),
        )?;
        let mut vote = Packet::from_data(
            None,
            new_tower_sync_transaction(
                TowerSync::default(),
                Hash::new_unique(),
                &keypair,
                &vote_keypair,
                &vote_keypair,
                None,
            ),
        )?;
        vote.meta_mut().flags.set(PacketFlags::SIMPLE_VOTE_TX, true);
        let big_transfer = Packet::from_data(
            None,
            system_transaction::transfer(&keypair, &pubkey, 1000000, Hash::new_unique()),
        )?;

        for thread_type in [
            ThreadType::Transactions,
            ThreadType::Voting(VoteSource::Gossip),
            ThreadType::Voting(VoteSource::Tpu),
        ] {
            let mut transaction_storage = UnprocessedTransactionStorage::new_transaction_storage(
                UnprocessedPacketBatches::with_capacity(100),
                thread_type,
            );
            transaction_storage.insert_batch(vec![
                ImmutableDeserializedPacket::new(small_transfer.clone())?,
                ImmutableDeserializedPacket::new(vote.clone())?,
                ImmutableDeserializedPacket::new(big_transfer.clone())?,
            ]);
            let deserialized_packets = transaction_storage
                .iter()
                .map(|packet| packet.immutable_section().original_packet().clone())
                .collect_vec();
            assert_eq!(3, deserialized_packets.len());
            assert!(deserialized_packets.contains(&small_transfer));
            assert!(deserialized_packets.contains(&vote));
            assert!(deserialized_packets.contains(&big_transfer));
        }

        for (vote_source, staked) in iproduct!(
            [VoteSource::Gossip, VoteSource::Tpu].into_iter(),
            [true, false].into_iter()
        ) {
            let staked_keys = if staked {
                vec![vote_keypair.pubkey()]
            } else {
                vec![]
            };
            let latest_unprocessed_votes = LatestUnprocessedVotes::new_for_tests(&staked_keys);
            let mut transaction_storage = UnprocessedTransactionStorage::new_vote_storage(
                Arc::new(latest_unprocessed_votes),
                vote_source,
            );
            transaction_storage.insert_batch(vec![
                ImmutableDeserializedPacket::new(small_transfer.clone())?,
                ImmutableDeserializedPacket::new(vote.clone())?,
                ImmutableDeserializedPacket::new(big_transfer.clone())?,
            ]);
            assert_eq!(if staked { 1 } else { 0 }, transaction_storage.len());
        }
        Ok(())
    }

    #[test]
    fn test_process_packets_retryable_indexes_reinserted() -> Result<(), Box<dyn Error>> {
        let node_keypair = Keypair::new();
        let genesis_config =
            genesis_utils::create_genesis_config_with_leader(100, &node_keypair.pubkey(), 200)
                .genesis_config;
        let (bank, _bank_forks) = Bank::new_with_bank_forks_for_tests(&genesis_config);
        let vote_keypair = Keypair::new();
        let mut vote = Packet::from_data(
            None,
            new_tower_sync_transaction(
                TowerSync::default(),
                Hash::new_unique(),
                &node_keypair,
                &vote_keypair,
                &vote_keypair,
                None,
            ),
        )?;
        vote.meta_mut().flags.set(PacketFlags::SIMPLE_VOTE_TX, true);

        let latest_unprocessed_votes =
            LatestUnprocessedVotes::new_for_tests(&[vote_keypair.pubkey()]);
        let mut transaction_storage = UnprocessedTransactionStorage::new_vote_storage(
            Arc::new(latest_unprocessed_votes),
            VoteSource::Tpu,
        );

        transaction_storage.insert_batch(vec![ImmutableDeserializedPacket::new(vote.clone())?]);
        assert_eq!(1, transaction_storage.len());

        // When processing packets, return all packets as retryable so that they
        // are reinserted into storage
        let _ = transaction_storage.process_packets(
            bank.clone(),
            &BankingStageStats::default(),
            &mut LeaderSlotMetricsTracker::new(0),
            |packets, _payload| {
                // Return all packets indexes as retryable
                Some(
                    packets
                        .iter()
                        .enumerate()
                        .map(|(index, _packet)| index)
                        .collect_vec(),
                )
            },
            &HashSet::default(),
        );

        // All packets should remain in the transaction storage
        assert_eq!(1, transaction_storage.len());
        Ok(())
    }

    #[test]
    fn test_prepare_packets_to_forward() {
        solana_logger::setup();
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10);

        let simple_transactions: Vec<Transaction> = (0..256)
            .map(|_id| {
                // packets are deserialized upon receiving, failed packets will not be
                // forwarded; Therefore we need to create real packets here.
                let key1 = Keypair::new();
                system_transaction::transfer(
                    &mint_keypair,
                    &key1.pubkey(),
                    genesis_config.rent.minimum_balance(0),
                    genesis_config.hash(),
                )
            })
            .collect_vec();

        let mut packets: Vec<DeserializedPacket> = simple_transactions
            .iter()
            .enumerate()
            .map(|(packets_id, transaction)| {
                let mut p = Packet::from_data(None, transaction).unwrap();
                p.meta_mut().port = packets_id as u16;
                DeserializedPacket::new(p).unwrap()
            })
            .collect_vec();

        // test preparing buffered packets for forwarding
        let test_prepareing_buffered_packets_for_forwarding =
            |buffered_packet_batches: UnprocessedPacketBatches| -> usize {
                let mut total_packets_to_forward: usize = 0;

                let mut unprocessed_transactions = ThreadLocalUnprocessedPackets {
                    unprocessed_packet_batches: buffered_packet_batches,
                    thread_type: ThreadType::Transactions,
                };

                let mut original_priority_queue = unprocessed_transactions.take_priority_queue();
                let _ = original_priority_queue
                    .drain_desc()
                    .chunks(128usize)
                    .into_iter()
                    .flat_map(|packets_to_process| {
                        let (_, packets_to_forward) =
                            unprocessed_transactions.prepare_packets_to_forward(packets_to_process);
                        total_packets_to_forward += packets_to_forward.len();
                        packets_to_forward
                    })
                    .collect::<MinMaxHeap<Arc<ImmutableDeserializedPacket>>>();
                total_packets_to_forward
            };

        {
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let total_packets_to_forward =
                test_prepareing_buffered_packets_for_forwarding(buffered_packet_batches);
            assert_eq!(total_packets_to_forward, 256);
        }

        // some packets are forwarded
        {
            let num_already_forwarded = 16;
            for packet in &mut packets[0..num_already_forwarded] {
                packet.forwarded = true;
            }
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let total_packets_to_forward =
                test_prepareing_buffered_packets_for_forwarding(buffered_packet_batches);
            assert_eq!(total_packets_to_forward, 256 - num_already_forwarded);
        }

        // all packets are forwarded
        {
            for packet in &mut packets {
                packet.forwarded = true;
            }
            let buffered_packet_batches: UnprocessedPacketBatches =
                UnprocessedPacketBatches::from_iter(packets.clone(), packets.len());
            let total_packets_to_forward =
                test_prepareing_buffered_packets_for_forwarding(buffered_packet_batches);
            assert_eq!(total_packets_to_forward, 0);
        }
    }
}
