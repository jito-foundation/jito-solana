//! Validator-owned ingress, response translation, and external-session liveness.
use {
    super::JitoBindingsDependencies,
    crate::{
        bam_dependencies::{BamConnectionState, BamDependencies, BamOutboundMessage},
        banking_stage::{
            consume_worker::run_tip_programs,
            consumer::{Consumer, TipProcessingDependencies},
            scheduler_messages::{NotCommittedReason, TransactionResult},
            transaction_scheduler::{
                bam_receive_and_buffer::BamReceiveAndBuffer,
                bam_scheduler::{BamScheduler, MAX_PACKETS_PER_BUNDLE},
            },
        },
    },
    agave_scheduler_bindings::{
        SharableTransactionBatchRegion, processed_codes,
        worker_message_types::not_included_reasons as reason,
    },
    agave_transaction_view::resolved_transaction_view::ResolvedTransactionView,
    bytes::Bytes,
    jito_protos::proto::bam_types::{
        self, AtomicTxnBatch, AtomicTxnBatchResult, SchedulingError, atomic_txn_batch_result,
        not_committed::Reason,
    },
    jito_scheduler_bindings::{
        JitoExecutionResponse, JitoIngressMessage, JitoTransactionResult, SOURCE_BAM,
        SOURCE_LEGACY_BUNDLE, allocate_batch, execution_flags, free_batch, free_results,
        read_results,
    },
    rts_alloc::Allocator,
    solana_poh::poh_recorder::SharedLeaderState,
    solana_runtime_transaction::runtime_transaction::RuntimeTransaction,
    solana_transaction_error::TransactionError,
    std::{
        collections::{HashMap, VecDeque},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::JoinHandle,
        time::{Duration, Instant},
    },
    tokio::sync::mpsc::error::TrySendError,
};

const MAX_PENDING: usize = 2_000;
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(5);
type BamPolicy = BamScheduler<RuntimeTransaction<ResolvedTransactionView<Bytes>>>;

struct Pending {
    batch: SharableTransactionBatchRegion,
    seq_id: Option<u32>,
    bam_generation: u64,
    revert_on_error: bool,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn(
    exit: Arc<AtomicBool>,
    dependencies: JitoBindingsDependencies,
    bam: BamDependencies,
    allocator: Allocator,
    ingress: shaq::spsc::Producer<JitoIngressMessage>,
    completion: shaq::spsc::Consumer<JitoExecutionResponse>,
    leader: SharedLeaderState,
    consumer: Consumer,
    tip_dependencies: Option<TipProcessingDependencies>,
    filter_keys: Arc<ahash::HashSet<solana_pubkey::Pubkey>>,
) -> JoinHandle<()> {
    std::thread::Builder::new()
        .name("solJitoBridge".to_string())
        .spawn(move || {
            let mut bridge = Bridge {
                exit,
                dependencies,
                bam,
                allocator,
                ingress,
                completion,
                leader,
                consumer,
                tip_dependencies,
                filter_keys,
                pending: HashMap::new(),
                responses: VecDeque::new(),
                incoming: VecDeque::new(),
                next_id: 1,
                last_heartbeat: Instant::now(),
            };
            if let Err(error) = bridge.run() {
                error!("Jito scheduler bridge stopped: {error}");
            }
            // A dead peer may have submitted work whose response was not delivered.
            // The banking manager joins every consumer before releasing BundleStage.
            // Retain borrowed allocations until all mappings of the old session close.
            if bridge
                .pending
                .values()
                .any(|pending| pending.seq_id.is_some())
                || !bridge.responses.is_empty()
                || !bridge.incoming.is_empty()
            {
                bridge
                    .dependencies
                    .control
                    .reconnect_bam
                    .store(true, Ordering::Release);
            }
        })
        .unwrap()
}

struct Bridge {
    exit: Arc<AtomicBool>,
    dependencies: JitoBindingsDependencies,
    bam: BamDependencies,
    allocator: Allocator,
    ingress: shaq::spsc::Producer<JitoIngressMessage>,
    completion: shaq::spsc::Consumer<JitoExecutionResponse>,
    leader: SharedLeaderState,
    consumer: Consumer,
    tip_dependencies: Option<TipProcessingDependencies>,
    filter_keys: Arc<ahash::HashSet<solana_pubkey::Pubkey>>,
    pending: HashMap<u64, Pending>,
    responses: VecDeque<(u64, AtomicTxnBatchResult)>,
    incoming: VecDeque<(u64, AtomicTxnBatch)>,
    next_id: u64,
    last_heartbeat: Instant,
}

impl Bridge {
    fn run(&mut self) -> Result<(), &'static str> {
        let mut last_upkeep = Instant::now();
        while !self.exit.load(Ordering::Acquire) {
            for _ in 0..MAX_PENDING {
                let Some(response) = self.completion.try_read() else {
                    break;
                };
                if response.id == 0 {
                    if response.batch.num_transactions != 0
                        || response.responses.allocation_size != 0
                    {
                        return Err("invalid scheduler heartbeat");
                    }
                    self.last_heartbeat = Instant::now();
                    continue;
                }
                self.complete(response)?;
            }
            self.flush_responses()?;
            if self.last_heartbeat.elapsed() > HEARTBEAT_TIMEOUT {
                return Err("scheduler heartbeat timed out");
            }

            let state = self.bam.bam_enabled.load(Ordering::Acquire);
            let legacy_pending = self
                .pending
                .values()
                .any(|pending| pending.seq_id.is_none());
            if state == BamConnectionState::DrainingBlockEngine as u8 && !legacy_pending {
                let _ = self.bam.bam_enabled.compare_exchange(
                    state,
                    BamConnectionState::BlockEngineDrained as u8,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }

            // Legacy tip maintenance runs even when no bundles arrive.
            if state <= BamConnectionState::Connecting as u8
                && last_upkeep.elapsed() >= Duration::from_millis(10)
            {
                let leader = self.leader.load();
                if leader.atomic_batches_enabled()
                    && let Some(bank) = leader.working_bank()
                    && let Some(tips) = self.tip_dependencies.as_ref()
                {
                    run_tip_programs(&self.consumer, tips, bank, true);
                }
                last_upkeep = Instant::now();
            }

            if self.pending.len() + self.responses.len() < MAX_PENDING {
                if self.incoming.is_empty()
                    && let Ok(group) = self.dependencies.control.bam_batches.1.try_recv()
                {
                    let generation = group.generation;
                    self.incoming.extend(
                        group
                            .batches
                            .batches
                            .into_iter()
                            .map(|batch| (generation, batch)),
                    );
                }
                for _ in 0..32 {
                    if self.pending.len() + self.responses.len() >= MAX_PENDING {
                        break;
                    }
                    let leader = self.leader.load();
                    if leader.bank_slot().is_some() && leader.working_bank().is_none() {
                        break;
                    }
                    let Some((generation, batch)) = self.incoming.pop_front() else {
                        break;
                    };
                    if generation
                        == self
                            .dependencies
                            .control
                            .bam_generation
                            .load(Ordering::Acquire)
                    {
                        self.bam_batch(batch, generation);
                    }
                }
                for _ in 0..32 {
                    if self.pending.len() + self.responses.len() >= MAX_PENDING {
                        break;
                    }
                    let Ok(bundle) = self.dependencies.bundles.try_recv() else {
                        break;
                    };
                    if self.bam.bam_enabled.load(Ordering::Acquire)
                        > BamConnectionState::Connecting as u8
                    {
                        continue;
                    }
                    let transactions: Vec<_> = bundle
                        .batch()
                        .iter()
                        .filter_map(|packet| packet.data(..))
                        .collect();
                    if transactions.len() != bundle.batch().len()
                        || transactions.is_empty()
                        || transactions.len() > MAX_PACKETS_PER_BUNDLE
                    {
                        continue;
                    }
                    // The external client bounds retention by wall time and rechecks blockhash age.
                    self.submit(&transactions, None, true, u64::MAX, 0);
                }
            }
            std::thread::sleep(Duration::from_micros(100));
        }
        Ok(())
    }

    fn bam_batch(&mut self, batch: AtomicTxnBatch, generation: u64) {
        let seq_id = batch.seq_id;
        let leader = self.leader.load();
        let bank = leader
            .working_bank()
            .cloned()
            .unwrap_or_else(|| self.bam.bank_forks.read().unwrap().working_bank());
        let root_bank = self.bam.bank_forks.read().unwrap().root_bank();
        let validated = BamReceiveAndBuffer::validate_for_bindings(
            &batch,
            (&root_bank, &bank),
            leader.working_bank().is_some() && leader.atomic_batches_enabled(),
            &self.filter_keys,
        );
        let revert_on_error = match validated {
            Ok(revert) => revert,
            Err(reason) => {
                self.reject(seq_id, generation, reason);
                return;
            }
        };
        if self.bam.bam_enabled.load(Ordering::Acquire) != BamConnectionState::Connected as u8 {
            self.reject(
                seq_id,
                generation,
                Reason::SchedulingError(SchedulingError::OutsideLeaderSlot as i32),
            );
            return;
        }
        let transactions: Vec<_> = batch
            .packets
            .iter()
            .map(|packet| packet.data.as_ref())
            .collect();
        if !self.submit(
            &transactions,
            Some(seq_id),
            revert_on_error,
            batch.max_schedule_slot,
            generation,
        ) {
            self.reject(
                seq_id,
                generation,
                Reason::SchedulingError(SchedulingError::ContainerFull as i32),
            );
        }
    }

    fn submit(
        &mut self,
        transactions: &[&[u8]],
        seq_id: Option<u32>,
        revert: bool,
        max_slot: u64,
        bam_generation: u64,
    ) -> bool {
        if self.pending.len() + self.responses.len() >= MAX_PENDING {
            return false;
        }
        let Some(batch) = allocate_batch(&self.allocator, transactions) else {
            return false;
        };
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .expect("Jito ingress ID exhausted");
        let request = JitoIngressMessage {
            id,
            source: if seq_id.is_some() {
                SOURCE_BAM
            } else {
                SOURCE_LEGACY_BUNDLE
            },
            flags: if revert {
                execution_flags::ALL_OR_NOTHING | execution_flags::DROP_ON_FAILURE
            } else {
                0
            },
            max_slot,
            bam_generation,
            batch,
        };
        if self.ingress.try_write(request).is_err() {
            // No successful queue write, so ownership never left the validator.
            unsafe { free_batch(&self.allocator, batch) };
            return false;
        }
        self.pending.insert(
            id,
            Pending {
                batch,
                seq_id,
                bam_generation,
                revert_on_error: revert,
            },
        );
        true
    }

    fn complete(&mut self, response: JitoExecutionResponse) -> Result<(), &'static str> {
        let Some(pending) = self.pending.get(&response.id) else {
            return Err("completion for an unknown ingress ID");
        };
        if pending.batch != response.batch {
            return Err("completion batch does not match ingress");
        }
        let result = if pending.seq_id.is_some() {
            Some(bam_result(
                &self.allocator,
                &response,
                pending.revert_on_error,
            )?)
        } else {
            None
        };
        let pending = self.pending.remove(&response.id).unwrap();
        if let (Some(seq_id), Some(result)) = (pending.seq_id, result) {
            self.responses.push_back((
                pending.bam_generation,
                AtomicTxnBatchResult {
                    seq_id,
                    result: Some(result),
                },
            ));
        }
        // Completion transfers every allocation back. Workers no longer borrow them.
        unsafe {
            free_results(&self.allocator, response.responses);
            free_batch(&self.allocator, response.batch);
        }
        Ok(())
    }

    fn reject(&mut self, seq_id: u32, generation: u64, reason: Reason) {
        self.responses.push_back((
            generation,
            AtomicTxnBatchResult {
                seq_id,
                result: Some(atomic_txn_batch_result::Result::NotCommitted(
                    bam_types::NotCommitted {
                        reason: Some(reason),
                    },
                )),
            },
        ));
    }

    fn flush_responses(&mut self) -> Result<(), &'static str> {
        while let Some((generation, response)) = self.responses.pop_front() {
            if generation
                != self
                    .dependencies
                    .control
                    .bam_generation
                    .load(Ordering::Acquire)
            {
                continue;
            }
            match self.bam.outbound_sender.try_send(
                BamOutboundMessage::GenerationBoundAtomicTxnBatchResult {
                    generation,
                    result: response,
                },
            ) {
                Ok(()) => {}
                Err(TrySendError::Full(
                    BamOutboundMessage::GenerationBoundAtomicTxnBatchResult {
                        generation,
                        result: response,
                    },
                )) => {
                    self.responses.push_front((generation, response));
                    break;
                }
                Err(_) => return Err("BAM response channel disconnected"),
            }
        }
        Ok(())
    }
}

fn bam_result(
    allocator: &Allocator,
    response: &JitoExecutionResponse,
    revert: bool,
) -> Result<atomic_txn_batch_result::Result, &'static str> {
    if response.processed_code != processed_codes::PROCESSED {
        return Ok(atomic_txn_batch_result::Result::NotCommitted(
            bam_types::NotCommitted {
                reason: Some(Reason::SchedulingError(SchedulingError::PohTimeout as i32)),
            },
        ));
    }
    // The trusted scheduler has transferred a live immutable result allocation.
    let values = unsafe { read_results(allocator, &response.responses) }
        .ok_or("invalid result allocation")?;
    if values.len() != usize::from(response.batch.num_transactions) {
        return Err("wrong number of transaction results");
    }
    let results: Result<Vec<_>, _> = values.into_iter().map(decode_result).collect();
    let results = results?;
    if revert {
        Ok(BamPolicy::generate_revert_on_error_bundle_result(results))
    } else {
        // Existing BAM non-reverting batches report their first transaction.
        let result = results.into_iter().next().ok_or("empty execution result")?;
        Ok(BamPolicy::generate_bundle_result(result))
    }
}

fn decode_result(
    (result, error): (JitoTransactionResult, Vec<u8>),
) -> Result<TransactionResult, &'static str> {
    if result.not_included_reason == reason::NONE {
        return Ok(TransactionResult::Committed(
            bam_types::TransactionCommittedResult {
                cus_consumed: u32::try_from(result.executed_units)
                    .map_err(|_| "compute units overflow")?,
                feepayer_balance_lamports: result.fee_payer_balance,
                loaded_accounts_data_size: result.loaded_accounts_data_size,
                execution_success: result.execution_success != 0,
            },
        ));
    }
    if !error.is_empty() {
        let error: TransactionError =
            bincode::deserialize(&error).map_err(|_| "invalid typed transaction error")?;
        return Ok(TransactionResult::NotCommitted(NotCommittedReason::Error(
            error,
        )));
    }
    if result.not_included_reason == reason::BANK_NOT_AVAILABLE {
        return Ok(TransactionResult::NotCommitted(
            NotCommittedReason::PohTimeout,
        ));
    }
    let error = match result.not_included_reason {
        reason::ALL_OR_NOTHING_BATCH_FAILURE | reason::PARTIAL_BATCH_CANCELLED => {
            TransactionError::CommitCancelled
        }
        reason::SANITIZE_FAILURE => TransactionError::SanitizeFailure,
        reason::ALREADY_PROCESSED => TransactionError::AlreadyProcessed,
        reason::BLOCKHASH_NOT_FOUND => TransactionError::BlockhashNotFound,
        reason::UNSUPPORTED_VERSION => TransactionError::UnsupportedVersion,
        reason::ADDRESS_LOOKUP_TABLE_NOT_FOUND => TransactionError::AddressLookupTableNotFound,
        _ => return Err("missing typed worker error"),
    };
    Ok(TransactionResult::NotCommitted(NotCommittedReason::Error(
        error,
    )))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        jito_scheduler_bindings::{JitoResponseRegion, allocate_results},
    };

    fn allocator() -> Allocator {
        let file = tempfile::tempfile().unwrap();
        // Fresh exclusively initialized backing file.
        unsafe { Allocator::create(&file, 64 * 1024 * 1024, 1, 2 * 1024 * 1024) }.unwrap()
    }

    fn response(
        allocator: &Allocator,
        values: &[(JitoTransactionResult, &[u8])],
    ) -> JitoExecutionResponse {
        JitoExecutionResponse {
            id: 1,
            // bam_result only reads the declared count; this test does not dereference the batch.
            batch: SharableTransactionBatchRegion {
                num_transactions: values.len() as u8,
                transactions_offset: 0,
            },
            processed_code: processed_codes::PROCESSED,
            execution_slot: 17,
            bank_id: 123,
            responses: allocate_results(allocator, values).unwrap(),
        }
    }

    #[test]
    fn bam_completion_preserves_execution_metadata_and_failure_index() {
        let allocator = allocator();
        let committed = JitoTransactionResult {
            executed_units: 321,
            fee_payer_balance: 98_765,
            loaded_accounts_data_size: 456,
            not_included_reason: reason::NONE,
            execution_success: 1,
            ..Default::default()
        };
        let values = [
            (committed, &[][..]),
            (
                JitoTransactionResult {
                    execution_success: 0,
                    ..committed
                },
                &[][..],
            ),
        ];
        let completed = response(&allocator, &values);
        let atomic_txn_batch_result::Result::Committed(result) =
            bam_result(&allocator, &completed, true).unwrap()
        else {
            panic!("expected committed result");
        };
        assert_eq!(
            result.transaction_results,
            vec![
                bam_types::TransactionCommittedResult {
                    cus_consumed: 321,
                    feepayer_balance_lamports: 98_765,
                    loaded_accounts_data_size: 456,
                    execution_success: true,
                },
                bam_types::TransactionCommittedResult {
                    cus_consumed: 321,
                    feepayer_balance_lamports: 98_765,
                    loaded_accounts_data_size: 456,
                    execution_success: false,
                },
            ]
        );
        unsafe {
            free_results(&allocator, completed.responses);
        }

        let cancelled = bincode::serialize(&TransactionError::CommitCancelled).unwrap();
        let error = bincode::serialize(&TransactionError::AccountNotFound).unwrap();
        let values = [
            (
                JitoTransactionResult {
                    not_included_reason: reason::ALL_OR_NOTHING_BATCH_FAILURE,
                    ..Default::default()
                },
                cancelled.as_slice(),
            ),
            (
                JitoTransactionResult {
                    not_included_reason: reason::ACCOUNT_NOT_FOUND,
                    ..Default::default()
                },
                error.as_slice(),
            ),
        ];
        let completed = response(&allocator, &values);
        let atomic_txn_batch_result::Result::NotCommitted(result) =
            bam_result(&allocator, &completed, true).unwrap()
        else {
            panic!("expected failed atomic result");
        };
        assert_eq!(
            result.reason,
            Some(Reason::TransactionError(bam_types::TransactionError {
                index: 1,
                reason: bam_types::TransactionErrorReason::AccountNotFound as i32,
            }))
        );
        unsafe {
            free_results(&allocator, completed.responses);
        }
    }

    #[test]
    fn invalid_completion_is_not_reported_as_committed() {
        let allocator = allocator();
        let values = [(
            JitoTransactionResult::default(),
            b"malformed typed error".as_slice(),
        )];
        let mut completed = response(&allocator, &values);
        completed.batch.num_transactions = 2;
        assert!(bam_result(&allocator, &completed, true).is_err());
        unsafe {
            free_results(&allocator, completed.responses);
        }
        completed.responses = JitoResponseRegion::default();
        completed.processed_code = processed_codes::MAX_WORKING_SLOT_EXCEEDED;
        assert!(matches!(
            bam_result(&allocator, &completed, true).unwrap(),
            atomic_txn_batch_result::Result::NotCommitted(_)
        ));
        assert!(
            decode_result((
                JitoTransactionResult {
                    not_included_reason: reason::ACCOUNT_NOT_FOUND,
                    ..Default::default()
                },
                b"invalid bincode".to_vec()
            ))
            .is_err()
        );
    }
}
