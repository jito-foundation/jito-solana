//! An implementation of the `ReceiveAndBuffer` trait that receives messages from BAM
//! and buffers from into the the `TransactionStateContainer`. Key thing to note:
//! this implementation only functions during the `Consume/Hold` phase; otherwise it will send them back
//! to BAM with a `Retryable` result.
use {
    super::{
        receive_and_buffer::ReceiveAndBuffer,
        transaction_state_container::TransactionStateContainer,
    },
    crate::{
        bam_dependencies::{BamConnectionState, BamOutboundMessage},
        banking_stage::{
            consumer::Consumer,
            decision_maker::BufferedPacketsDecision,
            scheduler_messages::MaxAge,
            transaction_scheduler::{
                bam_scheduler::MAX_PACKETS_PER_BUNDLE,
                bam_utils::convert_txn_error_to_proto,
                receive_and_buffer::{
                    calculate_max_age, calculate_priority_and_cost, DisconnectedError,
                    ReceivingStats,
                },
                transaction_state_container::{SharedBytes, StateContainer},
            },
        },
    },
    agave_transaction_view::{
        resolved_transaction_view::ResolvedTransactionView,
        transaction_version::TransactionVersion, transaction_view::SanitizedTransactionView,
    },
    ahash::HashSet,
    bytes::Bytes,
    crossbeam_channel::{RecvTimeoutError, Sender, TryRecvError},
    histogram::Histogram,
    itertools::Itertools,
    jito_protos::proto::bam_types::{
        atomic_txn_batch_result, not_committed::Reason, AtomicTxnBatch, DeserializationErrorReason,
        Packet, SchedulingError, TransactionErrorReason,
    },
    smallvec::SmallVec,
    solana_accounts_db::account_locks::validate_account_locks,
    solana_clock::{Slot, MAX_PROCESSING_AGE},
    solana_measure::{measure::Measure, measure_us},
    solana_packet::{Meta, PacketFlags, PACKET_DATA_SIZE},
    solana_perf::{
        packet::{BytesPacket, BytesPacketBatch},
        sigverify::ed25519_verify,
    },
    solana_poh::poh_recorder::SharedLeaderState,
    solana_pubkey::Pubkey,
    solana_runtime::bank_forks::BankForks,
    solana_runtime_transaction::{
        runtime_transaction::RuntimeTransaction, transaction_meta::StaticMeta,
    },
    solana_svm::transaction_error_metrics::TransactionErrorMetrics,
    solana_svm_transaction::svm_message::SVMMessage,
    solana_transaction::sanitized::MessageHash,
    std::{
        cmp::min,
        sync::{
            atomic::{AtomicBool, AtomicU8, Ordering},
            Arc, RwLock,
        },
        time::{Duration, Instant},
    },
};

type PrevalidationResult = Result<(usize, bool, u32, u64), (Reason, u32)>;
type VerifyResult = Result<(Vec<SharedBytes>, bool, u32, u64), (Reason, u32)>;

pub struct BamReceiveAndBuffer {
    bam_enabled: Arc<AtomicU8>,
    response_sender: Sender<BamOutboundMessage>,
    parsed_batch_receiver: crossbeam_channel::Receiver<ParsedBatch>,
    recv_stats_receiver: crossbeam_channel::Receiver<ReceivingStats>,
    parsing_thread: Option<std::thread::JoinHandle<()>>,
}

struct ParsedBatch {
    pub txns_max_age: SmallVec<
        [(
            RuntimeTransaction<ResolvedTransactionView<SharedBytes>>,
            MaxAge,
        ); MAX_PACKETS_PER_BUNDLE],
    >,
    pub cost: u64,
    priority: u64,
    pub revert_on_error: bool,
    pub max_schedule_slot: u64,
    pub seq_id: u32,
}

impl BamReceiveAndBuffer {
    pub fn new(
        exit: Arc<AtomicBool>,
        bam_enabled: Arc<AtomicU8>,
        bundle_receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
        response_sender: Sender<BamOutboundMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
        shared_leader_state: Option<SharedLeaderState>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> Self {
        let (parsed_batch_sender, parsed_batch_receiver) =
            crossbeam_channel::unbounded::<ParsedBatch>();
        let (recv_stats_sender, recv_stats_receiver) =
            crossbeam_channel::unbounded::<ReceivingStats>();

        let response_sender_clone = response_sender.clone();
        let parsing_thread = std::thread::spawn(move || {
            Self::run_parsing(
                exit,
                bundle_receiver,
                parsed_batch_sender,
                recv_stats_sender,
                response_sender_clone,
                bank_forks,
                shared_leader_state,
                blacklisted_accounts,
            )
        });

        Self {
            bam_enabled,
            response_sender,
            parsed_batch_receiver,
            recv_stats_receiver,
            parsing_thread: Some(parsing_thread),
        }
    }

    fn run_parsing(
        exit: Arc<AtomicBool>,
        bundle_receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
        parsed_batch_sender: crossbeam_channel::Sender<ParsedBatch>,
        recv_stats_sender: crossbeam_channel::Sender<ReceivingStats>,
        response_sender: Sender<BamOutboundMessage>,
        bank_forks: Arc<RwLock<BankForks>>,
        shared_leader_state: Option<SharedLeaderState>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) {
        let mut last_metrics_report = Instant::now();
        let mut metrics = BamReceiveAndBufferMetrics::default();
        let mut stats = ReceivingStats::default();
        let mut prevalidated = Vec::with_capacity(ATOMIC_TXN_BATCH_BURST);
        let mut packet_batches = Vec::with_capacity(ATOMIC_TXN_BATCH_BURST);
        let mut verify_results = Vec::with_capacity(ATOMIC_TXN_BATCH_BURST);
        const METRICS_REPORT_INTERVAL: Duration = Duration::from_millis(20);
        let mut recv_buffer = Vec::with_capacity(ATOMIC_TXN_BATCH_BURST * 2);

        while !exit.load(Ordering::Relaxed) {
            let loop_start = Instant::now();
            if loop_start.duration_since(last_metrics_report) > METRICS_REPORT_INTERVAL {
                metrics.report();
                last_metrics_report = loop_start;
                let _ = recv_stats_sender.try_send(stats);
                stats = ReceivingStats::default();
            }

            let start = Instant::now();
            let (recv_info, receive_time_us) = measure_us!(Self::batch_receive_until(
                &bundle_receiver,
                &mut recv_buffer,
                &start,
                TIMEOUT,
                ATOMIC_TXN_BATCH_BURST
            ));
            stats.receive_time_us += receive_time_us;

            match recv_info {
                Ok((_, num_batches_received)) => {
                    stats.num_received += num_batches_received;
                }
                Err(RecvTimeoutError::Disconnected) => return,
                Err(RecvTimeoutError::Timeout) => {
                    // No more work to do
                    continue;
                }
            }

            let current_slot = shared_leader_state
                .as_ref()
                .and_then(|leader_state| leader_state.load().working_bank().map(|b| b.slot()))
                .unwrap_or_else(|| bank_forks.read().unwrap().working_bank().slot());

            let (deserialize_stats, duration_us) = measure_us!(Self::batch_verify(
                &recv_buffer,
                current_slot,
                &mut metrics,
                &mut prevalidated,
                &mut packet_batches,
                &mut verify_results,
            ));
            stats.accumulate(deserialize_stats);
            metrics.increment_total_us(duration_us);
            recv_buffer.clear();

            for result in verify_results.drain(..) {
                match result {
                    Ok((verified_batch, revert_on_error, seq_id, max_schedule_slot)) => {
                        metrics
                            .sigverify_metrics
                            .increment_total_batches_verified(1);
                        let ((parse_result, parse_stats), duration_us) =
                            measure_us!(Self::parse_batch(
                                verified_batch,
                                seq_id,
                                revert_on_error,
                                max_schedule_slot,
                                &bank_forks,
                                &blacklisted_accounts,
                                &mut metrics,
                            ));
                        stats.accumulate(parse_stats);
                        metrics.increment_total_us(duration_us);

                        let parsed_batch = match parse_result {
                            Ok(batch) => batch,
                            Err(reason) => {
                                let _ =
                                    response_sender
                                        .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                                        jito_protos::proto::bam_types::AtomicTxnBatchResult {
                                            seq_id,
                                            result: Some(
                                                atomic_txn_batch_result::Result::NotCommitted(
                                                    jito_protos::proto::bam_types::NotCommitted {
                                                        reason: Some(reason),
                                                    },
                                                ),
                                            ),
                                        },
                                    ));
                                continue;
                            }
                        };

                        stats.num_buffered = stats
                            .num_buffered
                            .saturating_add(parsed_batch.txns_max_age.len());
                        let _ = parsed_batch_sender.try_send(parsed_batch);
                    }
                    Err((reason, seq_id)) => {
                        let _ = response_sender.try_send(BamOutboundMessage::AtomicTxnBatchResult(
                            jito_protos::proto::bam_types::AtomicTxnBatchResult {
                                seq_id,
                                result: Some(atomic_txn_batch_result::Result::NotCommitted(
                                    jito_protos::proto::bam_types::NotCommitted {
                                        reason: Some(reason),
                                    },
                                )),
                            },
                        ));
                        continue;
                    }
                }
            }
        }
    }

    fn send_no_leader_slot_txn_batch_result(&self, seq_id: u32) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::OutsideLeaderSlot as i32,
                            )),
                        },
                    )),
                },
            ));
    }

    fn send_container_full_txn_batch_result(&self, seq_id: u32) {
        let _ = self
            .response_sender
            .try_send(BamOutboundMessage::AtomicTxnBatchResult(
                jito_protos::proto::bam_types::AtomicTxnBatchResult {
                    seq_id,
                    result: Some(atomic_txn_batch_result::Result::NotCommitted(
                        jito_protos::proto::bam_types::NotCommitted {
                            reason: Some(Reason::SchedulingError(
                                SchedulingError::ContainerFull as i32,
                            )),
                        },
                    )),
                },
            ));
    }

    fn parse_batch(
        verified_batch: Vec<SharedBytes>,
        seq_id: u32,
        revert_on_error: bool,
        max_schedule_slot: u64,
        bank_forks: &Arc<RwLock<BankForks>>,
        blacklisted_accounts: &HashSet<Pubkey>,
        metrics: &mut BamReceiveAndBufferMetrics,
    ) -> (Result<ParsedBatch, Reason>, ReceivingStats) {
        let mut stats = ReceivingStats::default();

        let (root_bank, working_bank) = {
            let bank_forks = bank_forks.read().unwrap();
            let root_bank = bank_forks.root_bank();
            let working_bank = bank_forks.working_bank();
            (root_bank, working_bank)
        };
        let alt_resolved_slot = root_bank.slot();
        let sanitized_epoch = root_bank.epoch();
        let transaction_account_lock_limit = working_bank.get_transaction_account_lock_limit();
        let vote_only = working_bank.vote_only_bank();
        let enable_static_instruction_limit = root_bank
            .feature_set
            .is_active(&agave_feature_set::static_instruction_limit::ID);
        let mut cost: u64 = 0;
        let mut txns_max_age = SmallVec::with_capacity(verified_batch.len());

        if vote_only {
            return (
                Err(Reason::DeserializationError(
                    jito_protos::proto::bam_types::DeserializationError {
                        index: 0,
                        reason: DeserializationErrorReason::SanitizeError as i32,
                    },
                )),
                stats,
            );
        }

        // Checks are taken from receive_and_buffer.rs:
        // SanitizedTransactionReceiveAndBuffer::buffer_packets
        for (index, verified_packet) in verified_batch.into_iter().enumerate() {
            // Parsing and basic sanitization checks
            let sanitization_start = Instant::now();
            let Ok(view) = SanitizedTransactionView::try_new_sanitized(
                verified_packet,
                enable_static_instruction_limit,
            ) else {
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            };

            let Ok(view) = RuntimeTransaction::<SanitizedTransactionView<_>>::try_new(
                view,
                MessageHash::Compute,
                None,
            ) else {
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            };
            let sanitization_end = Instant::now();
            metrics.increment_sanitization_us(
                sanitization_end
                    .duration_since(sanitization_start)
                    .as_micros() as u64,
            );

            // Check 0: Reject vote transactions
            if view.is_simple_vote_transaction() {
                stats.num_dropped_on_parsing_and_sanitization += 1;
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::VoteTransactionFailure as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 1: Load addresses for transaction and convert to ResolvedTransactionView
            let resolution_start = sanitization_end;
            let load_addresses_result = match view.version() {
                TransactionVersion::Legacy => Ok((None, u64::MAX)),
                TransactionVersion::V0 => root_bank
                    .load_addresses_from_ref(view.address_table_lookup_iter())
                    .map(|(loaded_addresses, deactivation_slot)| {
                        (Some(loaded_addresses), deactivation_slot)
                    }),
            };
            let Ok((loaded_addresses, deactivation_slot)) = load_addresses_result else {
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            };

            let Ok(view) = RuntimeTransaction::<ResolvedTransactionView<_>>::try_new(
                view,
                loaded_addresses,
                root_bank.get_reserved_account_keys(),
            ) else {
                return (
                    Err(Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: index as u32,
                            reason: DeserializationErrorReason::SanitizeError as i32,
                        },
                    )),
                    stats,
                );
            };
            let resolution_end = Instant::now();
            metrics.increment_resolution_us(
                resolution_end.duration_since(resolution_start).as_micros() as u64,
            );

            // Check 2: Ensure no duplicates and valid number of account locks
            let start = Instant::now();
            if let Err(err) =
                validate_account_locks(view.account_keys(), transaction_account_lock_limit)
            {
                let reason = convert_txn_error_to_proto(err);
                stats.num_dropped_on_lock_validation += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    )),
                    stats,
                );
            }
            metrics.increment_lock_validation_us(start.elapsed().as_micros() as u64);

            // Check 3: Ensure the compute budget limits are valid
            let (result, duration_us) = measure_us!(view
                .compute_budget_instruction_details()
                .sanitize_and_convert_to_compute_budget_limits(&working_bank.feature_set));
            metrics.increment_fee_budget_extraction_us(duration_us);
            let fee_budget_limits = match result {
                Ok(fee_budget_limits) => fee_budget_limits,
                Err(err) => {
                    let reason = convert_txn_error_to_proto(err);
                    stats.num_dropped_on_compute_budget += 1;
                    return (
                        Err(Reason::TransactionError(
                            jito_protos::proto::bam_types::TransactionError {
                                index: index as u32,
                                reason: reason as i32,
                            },
                        )),
                        stats,
                    );
                }
            };

            // Check 4: Ensure valid blockhash and blockhash is not too old
            let lock_results: [_; 1] = core::array::from_fn(|_| Ok(()));
            let (check_results, duration_us) = measure_us!(working_bank.check_transactions(
                std::slice::from_ref(&view),
                &lock_results,
                MAX_PROCESSING_AGE,
                &mut TransactionErrorMetrics::default(),
            ));
            metrics.increment_check_transactions_us(duration_us);
            if let Some(Err(err)) = check_results.first() {
                let reason = convert_txn_error_to_proto(err.clone());
                stats.num_dropped_on_age += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: reason as i32,
                        },
                    )),
                    stats,
                );
            }

            // Check 5: Ensure the fee payer has enough to pay for the transaction fee
            // Only check the first transaction in the batch because some fee payers in transaction index 0 seed indices 1..N
            if index == 0 {
                let (result, duration_us) = measure_us!(Consumer::check_fee_payer_unlocked(
                    &working_bank,
                    &view,
                    &mut TransactionErrorMetrics::default(),
                ));
                metrics.increment_fee_payer_check_us(duration_us);
                if let Err(err) = result {
                    let reason = convert_txn_error_to_proto(err);
                    stats.num_dropped_on_fee_payer += 1;
                    return (
                        Err(Reason::TransactionError(
                            jito_protos::proto::bam_types::TransactionError {
                                index: index as u32,
                                reason: reason as i32,
                            },
                        )),
                        stats,
                    );
                }
            }

            // Check 6: Ensure none of the accounts touch blacklisted accounts
            let (contains_blacklisted_account, duration_us) = measure_us!(view
                .account_keys()
                .iter()
                .any(|key| blacklisted_accounts.contains(key)));
            metrics.increment_blacklist_check_us(duration_us);
            if contains_blacklisted_account {
                stats.num_dropped_on_blacklisted_account += 1;
                return (
                    Err(Reason::TransactionError(
                        jito_protos::proto::bam_types::TransactionError {
                            index: index as u32,
                            reason: TransactionErrorReason::SanitizeFailure as i32,
                        },
                    )),
                    stats,
                );
            }

            let max_age = calculate_max_age(sanitized_epoch, deactivation_slot, alt_resolved_slot);

            let (_, txn_cost) =
                calculate_priority_and_cost(&view, &fee_budget_limits.into(), &working_bank);
            cost = cost.saturating_add(txn_cost);
            txns_max_age.push((view, max_age));
        }

        let priority = seq_id_to_priority(seq_id);

        (
            Ok(ParsedBatch {
                txns_max_age,
                cost,
                priority,
                revert_on_error,
                max_schedule_slot,
                seq_id,
            }),
            stats,
        )
    }

    fn batch_receive_until(
        bundle_receiver: &crossbeam_channel::Receiver<AtomicTxnBatch>,
        recv_buffer: &mut Vec<AtomicTxnBatch>,
        &start: &Instant,
        recv_timeout: Duration,
        batch_count_upperbound: usize,
    ) -> Result<(usize, usize), RecvTimeoutError> {
        let batch = bundle_receiver.recv_timeout(recv_timeout)?;
        let mut num_packets_received = batch.packets.len();
        let mut num_atomic_txn_batches_received = 1;
        recv_buffer.push(batch);

        while let Ok(batch) = bundle_receiver.try_recv() {
            trace!("got more packet batches in bam receive and buffer");
            num_packets_received += batch.packets.len();
            num_atomic_txn_batches_received += 1;
            recv_buffer.push(batch);
            if start.elapsed() > recv_timeout || recv_buffer.len() >= batch_count_upperbound {
                break;
            }
        }

        Ok((num_packets_received, num_atomic_txn_batches_received))
    }

    /// Check basic constraints and extract revert_on_error flags
    fn prevalidate_batches(
        atomic_txn_batches: &[AtomicTxnBatch],
        current_slot: Slot,
        prevalidated: &mut Vec<PrevalidationResult>,
    ) -> ReceivingStats {
        let mut stats = ReceivingStats::default();

        prevalidated.clear();
        prevalidated.extend(atomic_txn_batches.iter().enumerate().map(
            |(batch_index, atomic_txn_batch)| {
                if atomic_txn_batch.max_schedule_slot < current_slot {
                    stats.num_dropped_without_parsing += 1;
                    return Err((
                        Reason::SchedulingError(SchedulingError::OutsideLeaderSlot as i32),
                        atomic_txn_batch.seq_id,
                    ));
                }

                if atomic_txn_batch.packets.is_empty() {
                    stats.num_dropped_without_parsing += 1;
                    return Err((
                        Reason::DeserializationError(
                            jito_protos::proto::bam_types::DeserializationError {
                                index: 0,
                                reason: DeserializationErrorReason::Empty as i32,
                            },
                        ),
                        atomic_txn_batch.seq_id,
                    ));
                }

                if atomic_txn_batch.packets.len() > MAX_PACKETS_PER_BUNDLE {
                    stats.num_dropped_without_parsing += 1;
                    return Err((
                        Reason::DeserializationError(
                            jito_protos::proto::bam_types::DeserializationError {
                                index: 0,
                                reason: DeserializationErrorReason::SanitizeError as i32,
                            },
                        ),
                        atomic_txn_batch.seq_id,
                    ));
                }

                let Ok(revert_on_error) = atomic_txn_batch
                    .packets
                    .iter()
                    .map(|p| {
                        p.meta
                            .as_ref()
                            .and_then(|meta| meta.flags.as_ref())
                            .is_some_and(|flags| flags.revert_on_error)
                    })
                    .all_equal_value()
                else {
                    stats.num_dropped_without_parsing += 1;
                    return Err((
                        Reason::DeserializationError(
                            jito_protos::proto::bam_types::DeserializationError {
                                index: 0,
                                reason: DeserializationErrorReason::InconsistentBundle as i32,
                            },
                        ),
                        atomic_txn_batch.seq_id,
                    ));
                };

                Ok((
                    batch_index,
                    revert_on_error,
                    atomic_txn_batch.seq_id,
                    atomic_txn_batch.max_schedule_slot,
                ))
            },
        ));

        stats
    }

    fn batch_verify(
        atomic_txn_batches: &[AtomicTxnBatch],
        current_slot: Slot,
        metrics: &mut BamReceiveAndBufferMetrics,
        prevalidated: &mut Vec<PrevalidationResult>,
        packet_batches: &mut Vec<solana_perf::packet::PacketBatch>,
        results: &mut Vec<VerifyResult>,
    ) -> ReceivingStats {
        fn proto_packet_to_packet(from_packet: &Packet) -> BytesPacket {
            let copy_len = min(PACKET_DATA_SIZE, from_packet.data.len());
            let mut to_packet = BytesPacket::new(
                Bytes::copy_from_slice(&from_packet.data[0..copy_len]),
                Meta::default(),
            );
            to_packet.meta_mut().size = from_packet.data.len();
            to_packet.meta_mut().set_discard(false);

            if let Some(meta) = &from_packet.meta {
                to_packet.meta_mut().size = meta.size as usize;
                if let Some(flags) = &meta.flags {
                    if flags.simple_vote_tx {
                        to_packet
                            .meta_mut()
                            .flags
                            .insert(PacketFlags::SIMPLE_VOTE_TX);
                    }
                }
            }
            to_packet
        }

        fn pkt_to_shared_bytes(
            solana_packet_ref: &solana_perf::packet::PacketRef,
            i: usize,
            seq_id: u32,
            metrics: &mut BamReceiveAndBufferMetrics,
        ) -> Result<SharedBytes, (Reason, u32)> {
            if solana_packet_ref.meta().discard() {
                let reason = DeserializationErrorReason::SanitizeError;
                return Err((
                    Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: i as u32,
                            reason: reason as i32,
                        },
                    ),
                    seq_id,
                ));
            }

            metrics
                .sigverify_metrics
                .increment_total_packets_verified(1);

            let Some(data) = solana_packet_ref.data(..) else {
                let reason = DeserializationErrorReason::SanitizeError;
                return Err((
                    Reason::DeserializationError(
                        jito_protos::proto::bam_types::DeserializationError {
                            index: i as u32,
                            reason: reason as i32,
                        },
                    ),
                    seq_id,
                ));
            };

            Ok(SharedBytes::new(data.to_vec()))
        }

        let mut stats = ReceivingStats::default();

        let preverify_stats =
            Self::prevalidate_batches(atomic_txn_batches, current_slot, prevalidated);
        stats.accumulate(preverify_stats);

        packet_batches.clear();
        packet_batches.reserve(prevalidated.len());
        let mut packet_count = 0;
        prevalidated.iter().flatten().for_each(|result| {
            let solana_packet_batch: Vec<_> = atomic_txn_batches[result.0]
                .packets
                .iter()
                .map(proto_packet_to_packet)
                .collect();
            packet_count += solana_packet_batch.len();
            packet_batches.push(solana_perf::packet::PacketBatch::Bytes(
                BytesPacketBatch::from(solana_packet_batch),
            ));
        });

        let mut verify_packet_batch_time_us = Measure::start("verify_packet_batch_time_us");
        ed25519_verify(packet_batches, false, packet_count);
        verify_packet_batch_time_us.stop();

        metrics
            .sigverify_metrics
            .increment_verify_batches_pp_us(verify_packet_batch_time_us.as_us(), packet_count);
        metrics
            .sigverify_metrics
            .increment_batch_packets_len(packet_count);
        metrics
            .sigverify_metrics
            .increment_total_verify_time(verify_packet_batch_time_us.as_us());

        results.clear();
        results.reserve(prevalidated.len());
        let mut packet_batch_iter = packet_batches.iter();
        for pre_result in prevalidated.drain(..) {
            let result = pre_result.and_then(|(_, revert_on_error, seq_id, max_schedule_slot)| {
                let batch = packet_batch_iter.next().unwrap();
                let deserialized = batch
                    .iter()
                    .enumerate()
                    .map(|(i, pkt)| pkt_to_shared_bytes(&pkt, i, seq_id, metrics))
                    .collect::<Result<Vec<_>, _>>()?;

                Ok((deserialized, revert_on_error, seq_id, max_schedule_slot))
            });
            results.push(result);
        }

        stats
    }
}

// nomenclature taken from the agave versions of these constants
const ATOMIC_TXN_BATCH_BURST: usize = 128;
const TIMEOUT: Duration = Duration::from_millis(1);

impl ReceiveAndBuffer for BamReceiveAndBuffer {
    type Transaction = RuntimeTransaction<ResolvedTransactionView<SharedBytes>>;
    type Container = TransactionStateContainer<Self::Transaction>;

    fn receive_and_buffer_packets(
        &mut self,
        container: &mut Self::Container,
        decision: &BufferedPacketsDecision,
    ) -> Result<ReceivingStats, DisconnectedError> {
        let is_bam_enabled = BamConnectionState::from_u8(self.bam_enabled.load(Ordering::Relaxed))
            == BamConnectionState::Connected;

        // Receive all stats
        let mut stats = ReceivingStats::default();
        while let Ok(batch_stats) = self.recv_stats_receiver.try_recv() {
            stats.accumulate(batch_stats);
        }

        match decision {
            BufferedPacketsDecision::Consume(_) | BufferedPacketsDecision::Hold => loop {
                let batch = match self.parsed_batch_receiver.try_recv() {
                    Ok(batch) => batch,
                    Err(TryRecvError::Disconnected) => return Err(DisconnectedError),
                    Err(TryRecvError::Empty) => {
                        // If the channel is empty, work here is done.
                        break;
                    }
                };

                // If BAM is not enabled, drain the channel
                if !is_bam_enabled {
                    stats.num_dropped_without_parsing += batch.txns_max_age.len();
                    continue;
                }

                let ParsedBatch {
                    txns_max_age,
                    cost,
                    priority,
                    revert_on_error,
                    max_schedule_slot,
                    seq_id,
                } = batch;

                if container
                    .insert_new_batch(
                        txns_max_age,
                        priority,
                        cost,
                        revert_on_error,
                        max_schedule_slot,
                    )
                    .is_none()
                {
                    stats.num_dropped_on_capacity += 1;
                    self.send_container_full_txn_batch_result(seq_id);
                    continue;
                };
            },
            BufferedPacketsDecision::ForwardAndHold | BufferedPacketsDecision::Forward => {
                // Ensure nothing is left in the container
                while let Some(next_batch_id) = container.pop() {
                    let seq_id = priority_to_seq_id(next_batch_id.priority);
                    self.send_no_leader_slot_txn_batch_result(seq_id);
                    container.remove_by_id(next_batch_id.id);
                }

                // Send back any batches that were received while in Forward/Hold state
                let deadline = Instant::now() + Duration::from_millis(100);
                loop {
                    let (batch, receive_time_us) =
                        measure_us!(self.parsed_batch_receiver.recv_deadline(deadline));
                    stats.receive_time_us += receive_time_us;

                    let batch = match batch {
                        Ok(batch) => batch,
                        Err(RecvTimeoutError::Disconnected) => return Err(DisconnectedError),
                        Err(RecvTimeoutError::Timeout) => {
                            break;
                        }
                    };
                    self.send_no_leader_slot_txn_batch_result(batch.seq_id);
                    stats.num_dropped_without_parsing += 1;
                }
            }
        }

        Ok(stats)
    }
}

impl Drop for BamReceiveAndBuffer {
    fn drop(&mut self) {
        if let Some(parsing_thread) = self.parsing_thread.take() {
            parsing_thread.join().unwrap();
        }
    }
}

pub fn seq_id_to_priority(seq_id: u32) -> u64 {
    u64::MAX.saturating_sub(seq_id as u64)
}

pub fn priority_to_seq_id(priority: u64) -> u32 {
    u32::try_from(u64::MAX.saturating_sub(priority)).unwrap_or(u32::MAX)
}

#[derive(Default)]
struct BamReceiveAndBufferMetrics {
    total_us: u64,
    sanitization_us: u64,
    lock_validation_us: u64,
    resolution_us: u64,
    fee_budget_extraction_us: u64,
    check_transactions_us: u64,
    fee_payer_check_us: u64,
    blacklist_check_us: u64,
    pub sigverify_metrics: SigverifyMetrics,
}

impl BamReceiveAndBufferMetrics {
    fn has_data(&self) -> bool {
        self.total_us > 0
            || self.sanitization_us > 0
            || self.lock_validation_us > 0
            || self.resolution_us > 0
            || self.fee_budget_extraction_us > 0
            || self.check_transactions_us > 0
            || self.fee_payer_check_us > 0
            || self.blacklist_check_us > 0
            || self.sigverify_metrics.total_packets_verified > 0
    }

    fn report(&mut self) {
        if !self.has_data() {
            return;
        }

        datapoint_info!(
            "bam-receive-and-buffer",
            ("total_us", self.total_us, i64),
            ("sanitization_us", self.sanitization_us, i64),
            ("lock_validation_us", self.lock_validation_us, i64),
            ("resolution_us", self.resolution_us, i64),
            (
                "fee_budget_extraction_us",
                self.fee_budget_extraction_us,
                i64
            ),
            ("check_transactions_us", self.check_transactions_us, i64),
            ("fee_payer_check_us", self.fee_payer_check_us, i64),
            ("blacklist_check_us", self.blacklist_check_us, i64),
        );
        self.sigverify_metrics.report();
        *self = Self::default();
    }

    fn increment_total_us(&mut self, us: u64) {
        self.total_us = self.total_us.saturating_add(us);
    }

    fn increment_sanitization_us(&mut self, us: u64) {
        self.sanitization_us = self.sanitization_us.saturating_add(us);
    }

    fn increment_resolution_us(&mut self, us: u64) {
        self.resolution_us = self.resolution_us.saturating_add(us);
    }

    fn increment_lock_validation_us(&mut self, us: u64) {
        self.lock_validation_us = self.lock_validation_us.saturating_add(us);
    }

    fn increment_fee_budget_extraction_us(&mut self, us: u64) {
        self.fee_budget_extraction_us = self.fee_budget_extraction_us.saturating_add(us);
    }

    fn increment_check_transactions_us(&mut self, us: u64) {
        self.check_transactions_us = self.check_transactions_us.saturating_add(us);
    }

    fn increment_fee_payer_check_us(&mut self, us: u64) {
        self.fee_payer_check_us = self.fee_payer_check_us.saturating_add(us);
    }

    fn increment_blacklist_check_us(&mut self, us: u64) {
        self.blacklist_check_us = self.blacklist_check_us.saturating_add(us);
    }
}

struct SigverifyMetrics {
    pub verify_batches_pp_us_hist: Histogram,
    pub batch_packets_len_hist: Histogram,
    pub total_verify_time_us: u64,
    pub total_packets_verified: usize,
    pub total_batches_verified: usize,
}

impl Default for SigverifyMetrics {
    fn default() -> Self {
        Self {
            verify_batches_pp_us_hist: Histogram::new(),
            batch_packets_len_hist: Histogram::new(),
            total_verify_time_us: 0,
            total_packets_verified: 0,
            total_batches_verified: 0,
        }
    }
}

impl SigverifyMetrics {
    pub fn report(&self) {
        if self.total_packets_verified == 0 {
            return;
        }

        datapoint_info!(
            "bam-receive-and-buffer_sigverify-stats",
            ("total_verify_time_us", self.total_verify_time_us, i64),
            ("total_packets_verified", self.total_packets_verified, i64),
            ("total_batches_verified", self.total_batches_verified, i64),
            (
                "verify_batches_pp_us_p50",
                self.verify_batches_pp_us_hist.percentile(50.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_p75",
                self.verify_batches_pp_us_hist.percentile(75.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_p90",
                self.verify_batches_pp_us_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "verify_batches_pp_us_p99",
                self.verify_batches_pp_us_hist.percentile(99.0).unwrap_or(0),
                i64
            ),
            (
                "batch_packets_len_p50",
                self.batch_packets_len_hist.percentile(50.0).unwrap_or(0),
                i64
            ),
            (
                "batch_packets_len_p75",
                self.batch_packets_len_hist.percentile(75.0).unwrap_or(0),
                i64
            ),
            (
                "batch_packets_len_p90",
                self.batch_packets_len_hist.percentile(90.0).unwrap_or(0),
                i64
            ),
            (
                "batch_packets_len_p99",
                self.batch_packets_len_hist.percentile(99.0).unwrap_or(0),
                i64
            ),
        );
    }

    pub fn increment_verify_batches_pp_us(&mut self, us: u64, packet_count: usize) {
        if packet_count > 0 {
            let per_packet_us = (us as f64 / packet_count as f64).round() as u64;
            self.verify_batches_pp_us_hist
                .increment(per_packet_us)
                .unwrap();
        }
    }

    pub fn increment_batch_packets_len(&mut self, packet_count: usize) {
        if packet_count > 0 {
            self.batch_packets_len_hist
                .increment(packet_count as u64)
                .unwrap();
        }
    }

    pub fn increment_total_verify_time(&mut self, us: u64) {
        self.total_verify_time_us += us;
    }

    pub fn increment_total_packets_verified(&mut self, count: usize) {
        self.total_packets_verified += count;
    }

    pub fn increment_total_batches_verified(&mut self, count: usize) {
        self.total_batches_verified += count;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            bam_dependencies::BamConnectionState,
            banking_stage::{
                tests::create_slow_genesis_config,
                transaction_scheduler::transaction_state_container::StateContainer,
            },
        },
        ahash::HashSetExt,
        crossbeam_channel::{unbounded, Receiver},
        solana_keypair::Keypair,
        solana_ledger::genesis_utils::GenesisConfigInfo,
        solana_message::Message,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        solana_runtime_transaction::transaction_with_meta::TransactionWithMeta,
        solana_signer::Signer,
        solana_system_transaction::transfer,
        solana_transaction::{versioned::VersionedTransaction, Transaction},
        std::sync::atomic::AtomicU8,
        test_case::test_case,
    };

    #[test]
    fn test_seq_id_to_priority() {
        assert_eq!(seq_id_to_priority(0), u64::MAX);
        assert_eq!(seq_id_to_priority(1), u64::MAX - 1);
    }

    #[test]
    fn test_priority_to_seq_id() {
        assert_eq!(priority_to_seq_id(u64::MAX), 0);
        assert_eq!(priority_to_seq_id(u64::MAX - 1), 1);
    }

    fn test_bank_forks() -> (Arc<RwLock<BankForks>>, Keypair) {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_slow_genesis_config(u64::MAX);

        let (_bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);
        (bank_forks, mint_keypair)
    }

    #[allow(clippy::type_complexity)]
    fn setup_bam_receive_and_buffer(
        receiver: crossbeam_channel::Receiver<AtomicTxnBatch>,
        bank_forks: Arc<RwLock<BankForks>>,
        blacklisted_accounts: HashSet<Pubkey>,
    ) -> (
        Arc<AtomicBool>,
        BamReceiveAndBuffer,
        TransactionStateContainer<RuntimeTransaction<ResolvedTransactionView<SharedBytes>>>,
        crossbeam_channel::Receiver<BamOutboundMessage>,
    ) {
        let exit: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));
        let (response_sender, response_receiver) =
            crossbeam_channel::unbounded::<BamOutboundMessage>();
        let receive_and_buffer = BamReceiveAndBuffer::new(
            exit.clone(),
            Arc::new(AtomicU8::new(BamConnectionState::Connected as u8)),
            receiver,
            response_sender,
            bank_forks,
            None,
            blacklisted_accounts,
        );
        let container = TransactionStateContainer::with_capacity(100);
        (exit, receive_and_buffer, container, response_receiver)
    }

    fn verify_container<Tx: TransactionWithMeta>(
        container: &mut impl StateContainer<Tx>,
        expected_length: usize,
    ) {
        let mut actual_length: usize = 0;
        while let Some(id) = container.pop() {
            let Some((ids, _, _)) = container.get_batch(id.id) else {
                panic!(
                    "transaction in queue position {} with id {} must exist.",
                    actual_length, id.id
                );
            };
            for id in ids {
                assert!(
                    container.get_transaction(*id).is_some(),
                    "Transaction ID {id} not found in container",
                );
            }
            actual_length += 1;
        }

        assert_eq!(actual_length, expected_length);
    }

    fn run_batch_verify(
        batches: &[AtomicTxnBatch],
        current_slot: Slot,
        metrics: &mut BamReceiveAndBufferMetrics,
    ) -> (Vec<VerifyResult>, ReceivingStats) {
        let mut prevalidated = Vec::new();
        let mut packet_batches = Vec::new();
        let mut results = Vec::new();
        let stats = BamReceiveAndBuffer::batch_verify(
            batches,
            current_slot,
            metrics,
            &mut prevalidated,
            &mut packet_batches,
            &mut results,
        );
        (results, stats)
    }

    #[test_case(setup_bam_receive_and_buffer; "testcase-bam")]
    fn test_receive_and_buffer_simple_transfer<R: ReceiveAndBuffer>(
        setup_receive_and_buffer: impl FnOnce(
            Receiver<AtomicTxnBatch>,
            Arc<RwLock<BankForks>>,
            HashSet<Pubkey>,
        ) -> (
            Arc<AtomicBool>,
            R,
            R::Container,
            Receiver<BamOutboundMessage>,
        ),
    ) {
        let (sender, receiver) = unbounded();
        let (bank_forks, mint_keypair) = test_bank_forks();
        let (exit, mut receive_and_buffer, mut container, _response_sender) =
            setup_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());
        let transaction = transfer(
            &mint_keypair,
            &Pubkey::new_unique(),
            1,
            bank_forks.read().unwrap().root_bank().last_blockhash(),
        );
        let data = bincode::serialize(&transaction).expect("serializes");
        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet { data, meta: None }],
            max_schedule_slot: Slot::MAX,
        };
        sender.send(bundle).unwrap();

        let start = Instant::now();
        while start.elapsed() < Duration::from_secs(2) {
            let ReceivingStats { num_received, .. } = receive_and_buffer
                .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
                .unwrap();
            if num_received > 0 {
                break;
            }
        }

        verify_container(&mut container, 1);
        exit.store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_receive_and_buffer_invalid_packet() {
        let (bank_forks, _mint_keypair) = test_bank_forks();
        let (sender, receiver) = unbounded();
        let (exit, mut receive_and_buffer, mut container, response_receiver) =
            setup_bam_receive_and_buffer(receiver, bank_forks.clone(), HashSet::new());

        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![],
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };
        sender.send(bundle).unwrap();

        let ReceivingStats { num_received, .. } = receive_and_buffer
            .receive_and_buffer_packets(&mut container, &BufferedPacketsDecision::Hold)
            .unwrap();

        assert_eq!(num_received, 0);
        verify_container(&mut container, 0);
        let response = response_receiver.recv().unwrap();
        assert!(matches!(
            response,
            BamOutboundMessage::AtomicTxnBatchResult(txn_batch_result) if txn_batch_result.seq_id == 1 &&
            matches!(&txn_batch_result.result, Some(atomic_txn_batch_result::Result::NotCommitted(not_committed)) if
                matches!(not_committed.reason, Some(Reason::DeserializationError(_))))
        ));
        exit.store(true, Ordering::Relaxed);
    }

    #[test]
    fn test_batch_deserialize_success() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &mint_keypair,
                    &Pubkey::new_unique(),
                    1,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[bundle], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
        if let Ok((deserialized_packets, _, seq_id, _max_schedule_slot)) = &results[0] {
            assert_eq!(deserialized_packets.len(), 1);
            assert_eq!(*seq_id, 1);
        }
    }

    #[test]
    fn test_batch_deserialize_empty() {
        let (_bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        assert_eq!(batch_stats.num_dropped_without_parsing, 1);
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_invalid_packet() {
        let (_bank_forks, _mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vec![0; PACKET_DATA_SIZE + 1],
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_fee_payer_doesnt_exist() {
        let (bank_forks, _) = test_bank_forks();
        let fee_payer = Keypair::new();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &fee_payer,
                    &Pubkey::new_unique(),
                    1,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &HashSet::new(),
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_fee_payer, 1);
            assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_inconsistent() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let bundle = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![
                Packet {
                    data: bincode::serialize(&transfer(
                        &mint_keypair,
                        &Pubkey::new_unique(),
                        1,
                        bank_forks.read().unwrap().root_bank().last_blockhash(),
                    ))
                    .unwrap(),
                    meta: None,
                },
                Packet {
                    data: bincode::serialize(&transfer(
                        &mint_keypair,
                        &Pubkey::new_unique(),
                        1,
                        bank_forks.read().unwrap().root_bank().last_blockhash(),
                    ))
                    .unwrap(),
                    meta: Some(jito_protos::proto::bam_types::Meta {
                        flags: Some(jito_protos::proto::bam_types::PacketFlags {
                            revert_on_error: true,
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                },
            ],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, batch_stats) = run_batch_verify(&[bundle], Slot::MAX, &mut stats);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
        assert_eq!(batch_stats.num_dropped_without_parsing, 1);
        if let Err((reason, seq_id)) = &results[0] {
            assert_eq!(*seq_id, 1);
            assert!(matches!(reason, Reason::DeserializationError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_blacklisted_account() {
        let keypair = Keypair::new();
        let blacklisted_accounts = HashSet::from_iter(std::iter::once(keypair.pubkey()));

        let (bank_forks, mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &mint_keypair,
                    &keypair.pubkey(),
                    100,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &blacklisted_accounts,
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_blacklisted_account, 1);
            assert!(matches!(result.err().unwrap(), Reason::TransactionError(_)));
        }
    }

    #[test]
    fn test_batch_deserialize_rejects_vote_transactions() {
        let (bank_forks, _mint_keypair) = test_bank_forks();

        let vote_keypair = Keypair::new();
        let node_keypair = Keypair::new();
        let authorized_voter = Keypair::new();
        let recent_blockhash = bank_forks.read().unwrap().root_bank().last_blockhash();

        let vote_tx = Transaction::new(
            &[&node_keypair, &authorized_voter],
            Message::new(
                &[solana_vote_program::vote_instruction::vote(
                    &vote_keypair.pubkey(),
                    &authorized_voter.pubkey(),
                    solana_vote_program::vote_state::Vote::new(vec![1], recent_blockhash),
                )],
                Some(&node_keypair.pubkey()),
            ),
            recent_blockhash,
        );

        let vote_data = bincode::serialize(&VersionedTransaction::from(vote_tx)).unwrap();

        let meta = jito_protos::proto::bam_types::Meta {
            flags: Some(jito_protos::proto::bam_types::PacketFlags {
                simple_vote_tx: true,
                ..Default::default()
            }),
            size: vote_data.len() as u64,
        };

        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: vote_data,
                meta: Some(meta),
            }],
            max_schedule_slot: Slot::MAX,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());

        if let Ok((deserialized_packets, revert_on_error, seq_id, max_schedule_slot)) = &results[0]
        {
            let (result, stats) = BamReceiveAndBuffer::parse_batch(
                deserialized_packets.clone(),
                *seq_id,
                *revert_on_error,
                *max_schedule_slot,
                &bank_forks,
                &HashSet::new(),
                &mut stats,
            );

            assert!(result.is_err());
            assert_eq!(stats.num_dropped_on_parsing_and_sanitization, 1);
            assert!(matches!(
                result.err().unwrap(),
                Reason::DeserializationError(_)
            ));
        }
    }

    #[test]
    fn test_batch_deserialize_reject_wrong_slot() {
        let (bank_forks, mint_keypair) = test_bank_forks();
        let batch = AtomicTxnBatch {
            seq_id: 1,
            packets: vec![Packet {
                data: bincode::serialize(&transfer(
                    &mint_keypair,
                    &Pubkey::new_unique(),
                    1,
                    bank_forks.read().unwrap().root_bank().last_blockhash(),
                ))
                .unwrap(),
                meta: None,
            }],
            max_schedule_slot: 0,
        };

        let mut stats = BamReceiveAndBufferMetrics::default();
        let (results, _batch_stats) = run_batch_verify(&[batch], Slot::MAX, &mut stats);

        assert_eq!(results.len(), 1);
        assert!(results[0].is_err());
    }
}
