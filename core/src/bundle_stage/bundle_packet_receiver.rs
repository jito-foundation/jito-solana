use {
    super::BundleStageLoopMetrics,
    crate::{
        banking_stage::{
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            unprocessed_transaction_storage::UnprocessedTransactionStorage,
        },
        bundle_stage::{
            bundle_packet_deserializer::{BundlePacketDeserializer, ReceiveBundleResults},
            bundle_stage_leader_metrics::BundleStageLeaderMetrics,
        },
        immutable_deserialized_bundle::ImmutableDeserializedBundle,
        packet_bundle::PacketBundle,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_measure::{measure::Measure, measure_us},
    solana_sdk::timing::timestamp,
    std::time::Duration,
};

pub struct BundleReceiver {
    id: u32,
    bundle_packet_deserializer: BundlePacketDeserializer,
}

impl BundleReceiver {
    pub fn new(
        id: u32,
        bundle_packet_receiver: Receiver<Vec<PacketBundle>>,
        max_packets_per_bundle: Option<usize>,
    ) -> Self {
        Self {
            id,
            bundle_packet_deserializer: BundlePacketDeserializer::new(
                bundle_packet_receiver,
                max_packets_per_bundle,
            ),
        }
    }

    /// Receive incoming packets, push into unprocessed buffer with packet indexes
    pub fn receive_and_buffer_bundles(
        &mut self,
        unprocessed_bundle_storage: &mut UnprocessedTransactionStorage,
        bundle_stage_metrics: &mut BundleStageLoopMetrics,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) -> Result<(), RecvTimeoutError> {
        let (result, recv_time_us) = measure_us!({
            let recv_timeout = Self::get_receive_timeout(unprocessed_bundle_storage);
            let mut recv_and_buffer_measure = Measure::start("recv_and_buffer");
            self.bundle_packet_deserializer
                .receive_bundles(
                    recv_timeout,
                    unprocessed_bundle_storage.max_receive_size(),
                    &|packet: ImmutableDeserializedPacket| {
                        // see packet_receiver.rs
                        packet.check_insufficent_compute_unit_limit()?;
                        packet.check_excessive_precompiles()?;
                        Ok(packet)
                    },
                )
                // Consumes results if Ok, otherwise we keep the Err
                .map(|receive_bundle_results| {
                    self.buffer_bundles(
                        receive_bundle_results,
                        unprocessed_bundle_storage,
                        bundle_stage_metrics,
                        // tracer_packet_stats,
                        bundle_stage_leader_metrics,
                    );
                    recv_and_buffer_measure.stop();
                    bundle_stage_metrics.increment_receive_and_buffer_bundles_elapsed_us(
                        recv_and_buffer_measure.as_us(),
                    );
                })
        });

        bundle_stage_leader_metrics
            .leader_slot_metrics_tracker()
            .increment_receive_and_buffer_packets_us(recv_time_us);

        result
    }

    fn get_receive_timeout(
        unprocessed_transaction_storage: &UnprocessedTransactionStorage,
    ) -> Duration {
        // Gossip thread will almost always not wait because the transaction storage will most likely not be empty
        if !unprocessed_transaction_storage.is_empty() {
            // If there are buffered packets, run the equivalent of try_recv to try reading more
            // packets. This prevents starving BankingStage::consume_buffered_packets due to
            // buffered_packet_batches containing transactions that exceed the cost model for
            // the current bank.
            Duration::from_millis(0)
        } else {
            // BundleStage should pick up a working_bank as fast as possible
            Duration::from_millis(100)
        }
    }

    fn buffer_bundles(
        &self,
        ReceiveBundleResults {
            deserialized_bundles,
            num_dropped_bundles,
        }: ReceiveBundleResults,
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        bundle_stage_stats: &mut BundleStageLoopMetrics,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
    ) {
        let bundle_count = deserialized_bundles.len();
        let packet_count: usize = deserialized_bundles.iter().map(|b| b.len()).sum();

        bundle_stage_stats.increment_num_bundles_received(bundle_count as u64);
        bundle_stage_stats.increment_num_packets_received(packet_count as u64);
        bundle_stage_stats.increment_num_bundles_dropped(num_dropped_bundles as u64);
        // TODO (LB): fix this
        // bundle_stage_leader_metrics
        //     .leader_slot_metrics_tracker()
        //     .increment_total_new_valid_packets(packet_count as u64);

        debug!(
            "@{:?} bundles: {} txs: {} id: {}",
            timestamp(),
            bundle_count,
            packet_count,
            self.id
        );

        Self::push_unprocessed(
            unprocessed_transaction_storage,
            deserialized_bundles,
            bundle_stage_leader_metrics,
            bundle_stage_stats,
        );
    }

    fn push_unprocessed(
        unprocessed_transaction_storage: &mut UnprocessedTransactionStorage,
        deserialized_bundles: Vec<ImmutableDeserializedBundle>,
        bundle_stage_leader_metrics: &mut BundleStageLeaderMetrics,
        bundle_stage_stats: &mut BundleStageLoopMetrics,
    ) {
        if !deserialized_bundles.is_empty() {
            let insert_bundles_summary =
                unprocessed_transaction_storage.insert_bundles(deserialized_bundles);

            bundle_stage_stats.increment_newly_buffered_bundles_count(
                insert_bundles_summary.num_bundles_inserted as u64,
            );
            bundle_stage_stats
                .increment_num_bundles_dropped(insert_bundles_summary.num_bundles_dropped as u64);

            bundle_stage_leader_metrics
                .leader_slot_metrics_tracker()
                .increment_newly_buffered_packets_count(
                    insert_bundles_summary.num_packets_inserted as u64,
                );

            bundle_stage_leader_metrics
                .leader_slot_metrics_tracker()
                .accumulate_insert_packet_batches_summary(
                    &insert_bundles_summary.insert_packets_summary,
                );
        }
    }
}

/// This tests functionality of BundlePacketReceiver and the internals of BundleStorage because
/// they're tightly intertwined
#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::banking_stage::unprocessed_transaction_storage::BundleStorage,
        crossbeam_channel::unbounded,
        rand::{thread_rng, RngCore},
        solana_bundle::{
            bundle_execution::LoadAndExecuteBundleError, BundleExecutionError, SanitizedBundle,
            TipError,
        },
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_poh::poh_recorder::PohRecorderError,
        solana_runtime::{bank::Bank, genesis_utils::GenesisConfigInfo},
        solana_sdk::{
            bundle::derive_bundle_id,
            hash::Hash,
            packet::Packet,
            signature::{Keypair, Signer},
            system_transaction::transfer,
            transaction::VersionedTransaction,
        },
        std::{collections::HashSet, sync::Arc},
    };

    /// Makes `num_bundles` random bundles with `num_packets_per_bundle` packets per bundle.
    fn make_random_bundles(
        mint_keypair: &Keypair,
        num_bundles: usize,
        num_packets_per_bundle: usize,
        hash: Hash,
    ) -> Vec<PacketBundle> {
        let mut rng = thread_rng();

        (0..num_bundles)
            .map(|_| {
                let transfers: Vec<_> = (0..num_packets_per_bundle)
                    .map(|_| {
                        VersionedTransaction::from(transfer(
                            mint_keypair,
                            &mint_keypair.pubkey(),
                            rng.next_u64(),
                            hash,
                        ))
                    })
                    .collect();
                let bundle_id = derive_bundle_id(&transfers);

                PacketBundle {
                    batch: PacketBatch::new(
                        transfers
                            .iter()
                            .map(|tx| Packet::from_data(None, tx).unwrap())
                            .collect(),
                    ),
                    bundle_id,
                }
            })
            .collect()
    }

    fn assert_bundles_same(
        packet_bundles: &[PacketBundle],
        bundles_to_process: &[(ImmutableDeserializedBundle, SanitizedBundle)],
    ) {
        assert_eq!(packet_bundles.len(), bundles_to_process.len());
        packet_bundles
            .iter()
            .zip(bundles_to_process.iter())
            .for_each(|(packet_bundle, (_, sanitized_bundle))| {
                assert_eq!(packet_bundle.bundle_id, sanitized_bundle.bundle_id);
                assert_eq!(
                    packet_bundle.batch.len(),
                    sanitized_bundle.transactions.len()
                );
            });
    }

    #[test]
    fn test_receive_bundles() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        let bundles = make_random_bundles(&mint_keypair, 10, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 10);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 20);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 990);

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                (0..bundles_to_process.len()).map(|_| Ok(())).collect()
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);
        assert_eq!(bundle_storage.max_receive_size(), 1000);
    }

    #[test]
    fn test_receive_more_bundles_than_capacity() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 more than capacity
        let bundles = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY + 5,
            2,
            genesis_config.hash(),
        );

        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();
        // 1005 bundles were sent, but the capacity is 1000
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 1000);
        assert_eq!(bundle_storage.unprocessed_packets_len(), 2000);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_packets_len(), 0);

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure the first 1000 bundles are the ones to process
                assert_bundles_same(
                    &bundles[0..BundleStorage::BUNDLE_STORAGE_CAPACITY],
                    bundles_to_process,
                );
                (0..bundles_to_process.len()).map(|_| Ok(())).collect()
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_poh_record_error_rebuffered() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let poh_max_height_reached_index = 3;

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        // make sure poh end of slot reached + the correct bundles are buffered for the next time.
        // bundles at index 3 + 4 are rebuffered
        assert!(bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);

                let mut results = vec![Ok(()); bundles_to_process.len()];

                (poh_max_height_reached_index..bundles_to_process.len()).for_each(|index| {
                    results[index] = Err(BundleExecutionError::PohRecordError(
                        PohRecorderError::MaxHeightReached,
                    ));
                });
                results
            }
        ));

        assert_eq!(bundle_storage.unprocessed_bundles_len(), 2);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles[poh_max_height_reached_index..], bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_bank_processing_done_rebuffered() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bank_processing_done_index = 3;

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        // bundles at index 3 + 4 are rebuffered
        assert!(bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);

                let mut results = vec![Ok(()); bundles_to_process.len()];

                (bank_processing_done_index..bundles_to_process.len()).for_each(|index| {
                    results[index] = Err(BundleExecutionError::BankProcessingTimeLimitReached);
                });
                results
            }
        ));

        // 0, 1, 2 processed; 3, 4 buffered
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 2);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles[bank_processing_done_index..], bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_bank_execution_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![
                    Err(BundleExecutionError::TransactionFailure(
                        LoadAndExecuteBundleError::ProcessingTimeExceeded(Duration::from_secs(1)),
                    ));
                    bundles_to_process.len()
                ]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_tip_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![
                    Err(BundleExecutionError::TipError(TipError::LockError));
                    bundles_to_process.len()
                ]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_lock_error_dropped() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                vec![Err(BundleExecutionError::LockError); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_cost_model_exceeded_set_aside_and_requeued() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 5 bundles across the queue
        let bundles = make_random_bundles(&mint_keypair, 5, 2, genesis_config.hash());
        sender.send(bundles.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();

        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 5);

        // double check there's no bundles to process
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert!(bundles_to_process.is_empty());
                vec![Ok(()); bundles_to_process.len()]
            }
        ));

        // create a new bank w/ new slot number, cost model buffered packets should move back onto queue
        // in the same order they were originally
        let bank = bank_forks.read().unwrap().working_bank();
        let new_bank = Arc::new(Bank::new_from_parent(
            bank.clone(),
            bank.collector_id(),
            bank.slot() + 1,
        ));
        assert!(!bundle_storage.process_bundles(
            new_bank,
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure same order as original
                assert_bundles_same(&bundles, bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }

    #[test]
    fn test_process_bundles_cost_model_exceeded_buffer_capacity() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (_, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let mut unprocessed_storage = UnprocessedTransactionStorage::new_bundle_storage();

        let (sender, receiver) = unbounded();
        let mut bundle_receiver = BundleReceiver::new(0, receiver, Some(5));

        // send 500 bundles across the queue
        let bundles0 = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY / 2,
            2,
            genesis_config.hash(),
        );
        sender.send(bundles0.clone()).unwrap();

        let mut bundle_stage_stats = BundleStageLoopMetrics::default();
        let mut bundle_stage_leader_metrics = BundleStageLeaderMetrics::new(0);

        // receive and buffer bundles to the cost model reserve to test the capacity/dropped bundles there
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();
        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles0, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 500);

        let bundles1 = make_random_bundles(
            &mint_keypair,
            BundleStorage::BUNDLE_STORAGE_CAPACITY / 2,
            2,
            genesis_config.hash(),
        );
        sender.send(bundles1.clone()).unwrap();
        // should get 500 more bundles, cost model buffered length should be 1000
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();
        // buffered bundles are moved to cost model side deque
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles1, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 1000); // full now

        // send 10 bundles to go over capacity
        let bundles2 = make_random_bundles(&mint_keypair, 10, 2, genesis_config.hash());
        sender.send(bundles2.clone()).unwrap();

        // this set will get dropped from cost model buffered bundles
        let result = bundle_receiver.receive_and_buffer_bundles(
            &mut unprocessed_storage,
            &mut bundle_stage_stats,
            &mut bundle_stage_leader_metrics,
        );
        assert!(result.is_ok());

        let bundle_storage = unprocessed_storage.bundle_storage().unwrap();
        // buffered bundles are moved to cost model side deque, but its at capacity so stays the same size
        assert!(!bundle_storage.process_bundles(
            bank_forks.read().unwrap().working_bank(),
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                assert_bundles_same(&bundles2, bundles_to_process);
                vec![Err(BundleExecutionError::ExceedsCostModel); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 1000);

        // create new bank then call process_bundles again, expect to see [bundles1,bundles2]
        let bank = bank_forks.read().unwrap().working_bank();
        let new_bank = Arc::new(Bank::new_from_parent(
            bank.clone(),
            bank.collector_id(),
            bank.slot() + 1,
        ));
        assert!(!bundle_storage.process_bundles(
            new_bank,
            &mut bundle_stage_leader_metrics,
            &HashSet::default(),
            |bundles_to_process, _stats| {
                // make sure same order as original
                let expected_bundles: Vec<_> =
                    bundles0.iter().chain(bundles1.iter()).cloned().collect();
                assert_bundles_same(&expected_bundles, bundles_to_process);
                vec![Ok(()); bundles_to_process.len()]
            }
        ));
        assert_eq!(bundle_storage.unprocessed_bundles_len(), 0);
        assert_eq!(bundle_storage.cost_model_buffered_bundles_len(), 0);
    }
}
