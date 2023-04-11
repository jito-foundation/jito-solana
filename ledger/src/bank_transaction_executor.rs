use {
    crate::token_balances::collect_token_balances,
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        bank::{
            Bank, RentDebits, TransactionBalancesSet, TransactionExecutionDetails,
            TransactionExecutionResult, TransactionResults,
        },
        bank_utils,
        block_cost_limits::{MAX_ACCOUNT_DATA_BLOCK_LEN, MAX_BLOCK_UNITS},
        transaction_batch::TransactionBatch,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{
        clock::{Slot, MAX_PROCESSING_AGE},
        feature_set,
        instruction::InstructionError,
        pubkey::Pubkey,
        signature::Signature,
        transaction::{self, SanitizedTransaction, TransactionError},
    },
    solana_transaction_status::token_balances::TransactionTokenBalancesSet,
    std::{
        borrow::Cow,
        collections::HashMap,
        sync::{Arc, RwLock},
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

type TransactionExecutionSender = Sender<(
    TransactionExecutionResponseSender,
    BankTransactionExecutionRequest,
)>;
type TransactionExecutionReceiver = Receiver<(
    TransactionExecutionResponseSender,
    BankTransactionExecutionRequest,
)>;

type TransactionExecutionResponseSender = Sender<BankTransactionExecutionResponse>;
type TransactionExecutionResponseReceiver = Receiver<BankTransactionExecutionResponse>;

/// Callback for accessing bank state while processing the blockstore
pub type ProcessCallback = Arc<dyn Fn(&Bank) + Sync + Send>;

// it tracks the block cost available capacity - number of compute-units allowed
// by max block cost limit
#[derive(Debug)]
pub struct BlockCostCapacityMeter {
    pub capacity: u64,
    pub accumulated_cost: u64,
}

impl Default for BlockCostCapacityMeter {
    fn default() -> Self {
        BlockCostCapacityMeter::new(MAX_BLOCK_UNITS)
    }
}

impl BlockCostCapacityMeter {
    pub fn new(capacity_limit: u64) -> Self {
        Self {
            capacity: capacity_limit,
            accumulated_cost: 0_u64,
        }
    }

    // return the remaining capacity
    pub fn accumulate(&mut self, cost: u64) -> u64 {
        self.accumulated_cost += cost;
        self.capacity.saturating_sub(self.accumulated_cost)
    }
}

#[allow(clippy::large_enum_variant)]
pub enum TransactionStatusMessage {
    Batch(TransactionStatusBatch),
    Freeze(Slot),
}

pub struct TransactionStatusBatch {
    pub bank: Arc<Bank>,
    pub transactions: Vec<SanitizedTransaction>,
    pub execution_results: Vec<Option<TransactionExecutionDetails>>,
    pub balances: TransactionBalancesSet,
    pub token_balances: TransactionTokenBalancesSet,
    pub rent_debits: Vec<RentDebits>,
}

#[derive(Clone)]
pub struct TransactionStatusSender {
    pub sender: Sender<TransactionStatusMessage>,
}

impl TransactionStatusSender {
    pub fn send_transaction_status_batch(
        &self,
        bank: Arc<Bank>,
        transactions: Vec<SanitizedTransaction>,
        execution_results: Vec<TransactionExecutionResult>,
        balances: TransactionBalancesSet,
        token_balances: TransactionTokenBalancesSet,
        rent_debits: Vec<RentDebits>,
    ) {
        let slot = bank.slot();

        if let Err(e) = self
            .sender
            .send(TransactionStatusMessage::Batch(TransactionStatusBatch {
                bank,
                transactions,
                execution_results: execution_results
                    .into_iter()
                    .map(|result| match result {
                        TransactionExecutionResult::Executed { details, .. } => Some(details),
                        TransactionExecutionResult::NotExecuted(_) => None,
                    })
                    .collect(),
                balances,
                token_balances,
                rent_debits,
            }))
        {
            trace!(
                "Slot {} transaction_status send batch failed: {:?}",
                slot,
                e
            );
        }
    }

    pub fn send_transaction_status_freeze_message(&self, bank: &Arc<Bank>) {
        let slot = bank.slot();
        if let Err(e) = self.sender.send(TransactionStatusMessage::Freeze(slot)) {
            trace!(
                "Slot {} transaction_status send freeze message failed: {:?}",
                slot,
                e
            );
        }
    }
}

pub struct BankTransactionExecutionRequest {
    pub bank: Arc<Bank>,
    pub tx: SanitizedTransaction,
    pub transaction_status_sender: Option<TransactionStatusSender>,
    pub replay_vote_sender: Option<ReplayVoteSender>,
    pub cost_capacity_meter: Arc<RwLock<BlockCostCapacityMeter>>,
}

pub struct BankTransactionExecutionResponse {
    pub result: transaction::Result<()>,
    pub timings: ExecuteTimings,
    pub signature: Signature,
}

#[derive(Debug)]
pub enum SchedulerError {
    ChannelClosed,
}

pub struct BankTransactionExecutorHandle {
    request_sender: TransactionExecutionSender,
    response_sender: TransactionExecutionResponseSender,
    response_receiver: TransactionExecutionResponseReceiver,
}

/// A BankTransactionExecutorHandle provides a handle to schedule transactions on and receive
/// results on a receiver.
impl BankTransactionExecutorHandle {
    pub fn new(request_sender: TransactionExecutionSender) -> BankTransactionExecutorHandle {
        let (response_sender, response_receiver) = unbounded();
        BankTransactionExecutorHandle {
            request_sender,
            response_sender,
            response_receiver,
        }
    }

    /// Used to schedule transactions on the BankTransactionExecutor
    /// One can receive the results back over the response receiver channel, which is unique per handle
    pub fn schedule(&self, request: BankTransactionExecutionRequest) -> Result<(), SchedulerError> {
        self.request_sender
            .send((self.response_sender.clone(), request))
            .map_err(|_| SchedulerError::ChannelClosed)
    }

    pub fn response_receiver(&self) -> &TransactionExecutionResponseReceiver {
        &self.response_receiver
    }
}

/// The BankTransactionExecutor provides an executor used to process transactions against a bank
/// in a multi-threaded environment.
pub struct BankTransactionExecutor {
    sender: TransactionExecutionSender,
    threads: Vec<JoinHandle<()>>,
}

impl BankTransactionExecutor {
    pub fn new(num_executors: usize) -> BankTransactionExecutor {
        let (sender, receiver) = unbounded();
        let threads = Self::start_execution_threads(receiver, num_executors);
        BankTransactionExecutor { sender, threads }
    }

    /// This method can be used to grab a handle into the BankTransactionExecutor, which sends
    /// and receives results over a unique crossbeam channel.
    pub fn handle(&self) -> BankTransactionExecutorHandle {
        BankTransactionExecutorHandle::new(self.sender.clone())
    }

    /// Drops the sender + joins threads
    /// Note: this will block unless all instances of BankTransactionExecutorHandle are dropped
    pub fn join(self) -> thread::Result<()> {
        drop(self.sender);
        for t in self.threads {
            t.join()?;
        }
        Ok(())
    }

    fn start_execution_threads(
        receiver: TransactionExecutionReceiver,
        num_executors: usize,
    ) -> Vec<JoinHandle<()>> {
        (0..num_executors)
            .map(|idx| {
                let receiver = receiver.clone();

                Builder::new()
                    .name(format!("solBankTransactionExecutor-{}", idx))
                    .spawn(move || Self::transaction_execution_thread(receiver))
                    .unwrap()
            })
            .collect()
    }

    fn transaction_execution_thread(receiver: TransactionExecutionReceiver) {
        const TIMEOUT: Duration = Duration::from_secs(1);

        loop {
            match receiver.recv_timeout(TIMEOUT) {
                Ok((
                    response_sender,
                    BankTransactionExecutionRequest {
                        bank,
                        tx,
                        transaction_status_sender,
                        replay_vote_sender,
                        cost_capacity_meter,
                    },
                )) => {
                    let signature = *tx.signature();

                    let txs = vec![tx];
                    let mut batch = TransactionBatch::new(vec![Ok(())], &bank, Cow::Owned(txs));
                    batch.set_needs_unlock(false);

                    let mut timings = ExecuteTimings::default();
                    let execution_result = execute_batch(
                        &batch,
                        &bank,
                        transaction_status_sender.as_ref(),
                        replay_vote_sender.as_ref(),
                        &mut timings,
                        cost_capacity_meter,
                    );

                    if response_sender
                        .send(BankTransactionExecutionResponse {
                            result: execution_result,
                            timings,
                            signature,
                        })
                        .is_err()
                    {
                        warn!("error sending back results");
                    }
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }
}

fn execute_batch(
    batch: &TransactionBatch,
    bank: &Arc<Bank>,
    transaction_status_sender: Option<&TransactionStatusSender>,
    replay_vote_sender: Option<&ReplayVoteSender>,
    timings: &mut ExecuteTimings,
    cost_capacity_meter: Arc<RwLock<BlockCostCapacityMeter>>,
) -> transaction::Result<()> {
    let record_token_balances = transaction_status_sender.is_some();

    let mut mint_decimals: HashMap<Pubkey, u8> = HashMap::new();

    let pre_token_balances = if record_token_balances {
        collect_token_balances(bank, batch, &mut mint_decimals, None)
    } else {
        vec![]
    };

    let pre_process_units: u64 = aggregate_total_execution_units(timings);

    let (tx_results, balances) = batch.bank().load_execute_and_commit_transactions(
        batch,
        MAX_PROCESSING_AGE,
        transaction_status_sender.is_some(),
        transaction_status_sender.is_some(),
        transaction_status_sender.is_some(),
        timings,
    );

    if bank
        .feature_set
        .is_active(&feature_set::gate_large_block::id())
    {
        let execution_cost_units = aggregate_total_execution_units(timings) - pre_process_units;
        let remaining_block_cost_cap = cost_capacity_meter
            .write()
            .unwrap()
            .accumulate(execution_cost_units);

        debug!(
            "bank {} executed a batch, number of transactions {}, total execute cu {}, remaining block cost cap {}",
            bank.slot(),
            batch.sanitized_transactions().len(),
            execution_cost_units,
            remaining_block_cost_cap,
        );

        if remaining_block_cost_cap == 0_u64 {
            return Err(TransactionError::WouldExceedMaxBlockCostLimit);
        }
    }

    bank_utils::find_and_send_votes(
        batch.sanitized_transactions(),
        &tx_results,
        replay_vote_sender,
    );

    let TransactionResults {
        fee_collection_results,
        execution_results,
        rent_debits,
        ..
    } = tx_results;

    check_accounts_data_size(bank, &execution_results)?;

    if let Some(transaction_status_sender) = transaction_status_sender {
        let transactions = batch.sanitized_transactions().to_vec();
        let post_token_balances = if record_token_balances {
            collect_token_balances(bank, batch, &mut mint_decimals, None)
        } else {
            vec![]
        };

        let token_balances =
            TransactionTokenBalancesSet::new(pre_token_balances, post_token_balances);

        transaction_status_sender.send_transaction_status_batch(
            bank.clone(),
            transactions,
            execution_results,
            balances,
            token_balances,
            rent_debits,
        );
    }

    let first_err = get_first_error(batch, fee_collection_results);
    first_err.map(|(result, _)| result).unwrap_or(Ok(()))
}

fn aggregate_total_execution_units(execute_timings: &ExecuteTimings) -> u64 {
    let mut execute_cost_units: u64 = 0;
    for (program_id, timing) in &execute_timings.details.per_program_timings {
        if timing.count < 1 {
            continue;
        }
        execute_cost_units =
            execute_cost_units.saturating_add(timing.accumulated_units / timing.count as u64);
        trace!("aggregated execution cost of {:?} {:?}", program_id, timing);
    }
    execute_cost_units
}

/// Check to see if the transactions exceeded the accounts data size limits
fn check_accounts_data_size<'a>(
    bank: &Bank,
    execution_results: impl IntoIterator<Item = &'a TransactionExecutionResult>,
) -> transaction::Result<()> {
    check_accounts_data_block_size(bank)?;
    check_accounts_data_total_size(bank, execution_results)
}

/// Check to see if transactions exceeded the accounts data size limit per block
fn check_accounts_data_block_size(bank: &Bank) -> transaction::Result<()> {
    if !bank
        .feature_set
        .is_active(&feature_set::cap_accounts_data_size_per_block::id())
    {
        return Ok(());
    }

    debug_assert!(MAX_ACCOUNT_DATA_BLOCK_LEN <= i64::MAX as u64);
    if bank.load_accounts_data_size_delta_on_chain() > MAX_ACCOUNT_DATA_BLOCK_LEN as i64 {
        Err(TransactionError::WouldExceedAccountDataBlockLimit)
    } else {
        Ok(())
    }
}

/// Check the transaction execution results to see if any instruction errored by exceeding the max
/// accounts data size limit for all slots.  If yes, the whole block needs to be failed.
fn check_accounts_data_total_size<'a>(
    bank: &Bank,
    execution_results: impl IntoIterator<Item = &'a TransactionExecutionResult>,
) -> transaction::Result<()> {
    if !bank
        .feature_set
        .is_active(&feature_set::cap_accounts_data_len::id())
    {
        return Ok(());
    }

    if let Some(result) = execution_results
        .into_iter()
        .map(|execution_result| execution_result.flattened_result())
        .find(|result| {
            matches!(
                result,
                Err(TransactionError::InstructionError(
                    _,
                    InstructionError::MaxAccountsDataSizeExceeded
                )),
            )
        })
    {
        return result;
    }

    Ok(())
}

// Includes transaction signature for unit-testing
fn get_first_error(
    batch: &TransactionBatch,
    fee_collection_results: Vec<transaction::Result<()>>,
) -> Option<(transaction::Result<()>, Signature)> {
    let mut first_err = None;
    for (result, transaction) in fee_collection_results
        .iter()
        .zip(batch.sanitized_transactions())
    {
        if let Err(ref err) = result {
            if first_err.is_none() {
                first_err = Some((result.clone(), *transaction.signature()));
            }
            warn!(
                "Unexpected validator error: {:?}, transaction: {:?}",
                err, transaction
            );
            datapoint_error!(
                "validator_process_entry_error",
                (
                    "error",
                    format!("error: {:?}, transaction: {:?}", err, transaction),
                    String
                )
            );
        }
    }
    first_err
}

#[cfg(test)]
pub mod tests {
    use {
        crate::{bank_transaction_executor::get_first_error, genesis_utils::create_genesis_config},
        solana_program_runtime::timings::ExecuteTimings,
        solana_runtime::{
            bank::{Bank, TransactionResults},
            genesis_utils::GenesisConfigInfo,
        },
        solana_sdk::{
            account::AccountSharedData,
            clock::MAX_PROCESSING_AGE,
            hash::Hash,
            pubkey::Pubkey,
            signature::{Keypair, Signer},
            system_transaction,
            transaction::TransactionError,
        },
        std::sync::Arc,
    };

    #[test]
    fn test_get_first_error() {
        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(1_000_000_000);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));

        let present_account_key = Keypair::new();
        let present_account = AccountSharedData::new(1, 10, &Pubkey::default());
        bank.store_account(&present_account_key.pubkey(), &present_account);

        let keypair = Keypair::new();

        // Create array of two transactions which throw different errors
        let account_not_found_tx = system_transaction::transfer(
            &keypair,
            &solana_sdk::pubkey::new_rand(),
            42,
            bank.last_blockhash(),
        );
        let account_not_found_sig = account_not_found_tx.signatures[0];
        let invalid_blockhash_tx = system_transaction::transfer(
            &mint_keypair,
            &solana_sdk::pubkey::new_rand(),
            42,
            Hash::default(),
        );
        let txs = vec![account_not_found_tx, invalid_blockhash_tx];
        let batch = bank.prepare_batch_for_tests(txs);
        let (
            TransactionResults {
                fee_collection_results,
                ..
            },
            _balances,
        ) = batch.bank().load_execute_and_commit_transactions(
            &batch,
            MAX_PROCESSING_AGE,
            false,
            false,
            false,
            &mut ExecuteTimings::default(),
        );
        let (err, signature) = get_first_error(&batch, fee_collection_results).unwrap();
        assert_eq!(err.unwrap_err(), TransactionError::AccountNotFound);
        assert_eq!(signature, account_not_found_sig);
    }
}
