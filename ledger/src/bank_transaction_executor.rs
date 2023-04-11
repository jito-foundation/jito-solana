use {
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    solana_program_runtime::timings::ExecuteTimings,
    solana_runtime::{
        bank::{
            Bank, RentDebits, TransactionBalancesSet, TransactionExecutionDetails,
            TransactionExecutionResult,
        },
        block_cost_limits::MAX_BLOCK_UNITS,
        vote_sender_types::ReplayVoteSender,
    },
    solana_sdk::{clock::Slot, transaction::SanitizedTransaction},
    solana_transaction_status::token_balances::TransactionTokenBalancesSet,
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, RwLock,
        },
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
    // entry_callback: Option<&ProcessCallback>,
    // transaction_status_sender: Option<&TransactionStatusSender>,
    // replay_vote_sender: Option<&ReplayVoteSender>,
    // timings: &mut ExecuteTimings,
    // cost_capacity_meter: Arc<RwLock<BlockCostCapacityMeter>>,
}

pub struct BankTransactionExecutionResponse {}

// pub struct ReplayResponse {
//     pub result: transaction::Result<()>,
//     pub timings: ExecuteTimings,
// }

/// Request for replay, sends responses back on this channel
// pub struct ReplayRequest {
//     pub bank: Arc<Bank>,
//     pub tx: SanitizedTransaction,
//     pub transaction_status_sender: Option<TransactionStatusSender>,
//     pub replay_vote_sender: Option<ReplayVoteSender>,
//     pub cost_capacity_meter: Arc<RwLock<BlockCostCapacityMeter>>,
//     pub entry_callback: Option<ProcessCallback>,
// }

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
    pub fn new(num_executors: usize, exit: &Arc<AtomicBool>) -> BankTransactionExecutor {
        let (sender, receiver) = unbounded();
        let threads = Self::start_execution_threads(receiver, num_executors, exit);
        BankTransactionExecutor { sender, threads }
    }

    /// This method can be used to grab a handle into the BankTransactionExecutor, which sends
    /// and receives results over a unique crossbeam channel.
    pub fn handle(&self) -> BankTransactionExecutorHandle {
        BankTransactionExecutorHandle::new(self.sender.clone())
    }

    pub fn join(self) -> thread::Result<()> {
        for t in self.threads {
            t.join()?;
        }
        Ok(())
    }

    fn start_execution_threads(
        receiver: TransactionExecutionReceiver,
        num_executors: usize,
        exit: &Arc<AtomicBool>,
    ) -> Vec<JoinHandle<()>> {
        (0..num_executors)
            .map(|idx| {
                let exit = exit.clone();
                let receiver = receiver.clone();

                Builder::new()
                    .name(format!("solBankTransactionExecutor-{}", idx))
                    .spawn(move || Self::transaction_execution_thread(receiver, exit))
                    .unwrap()
            })
            .collect()
    }

    fn transaction_execution_thread(receiver: TransactionExecutionReceiver, exit: Arc<AtomicBool>) {
        const TIMEOUT: Duration = Duration::from_secs(1);

        while !exit.load(Ordering::Relaxed) {
            match receiver.recv_timeout(TIMEOUT) {
                Ok((response_sender, request)) => {}
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }
}
