use {
    crossbeam_channel::{unbounded, Receiver, RecvTimeoutError, Sender},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

type TransactionExecutionSender = Sender<BankTransactionExecutionRequest>;
type TransactionExecutionReceiver = Receiver<BankTransactionExecutionRequest>;

type TransactionExecutionResponseSender = Sender<BankTransactionExecutionResponse>;
type TransactionExecutionResponseReceiver = Receiver<BankTransactionExecutionResponse>;

pub struct BankTransactionExecutionRequest {}

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

pub struct BankTransactionExecutorHandle {
    sender: TransactionExecutionSender,
}

impl BankTransactionExecutorHandle {
    pub fn new(sender: TransactionExecutionSender) -> BankTransactionExecutorHandle {
        BankTransactionExecutorHandle { sender }
    }
}

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

    pub fn handle(&self) -> BankTransactionExecutorHandle {
        BankTransactionExecutorHandle {
            sender: self.sender.clone(),
        }
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
                Ok(request) => {}
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => {
                    break;
                }
            }
        }
    }
}
