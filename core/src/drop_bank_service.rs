use {
    crossbeam_channel::Receiver,
    solana_measure::measure::Measure,
    solana_metrics::datapoint::DataPoint,
    solana_runtime::installed_scheduler_pool::BankWithScheduler,
    std::thread::{self, Builder, JoinHandle},
};

pub struct DropBankService {
    thread_hdl: JoinHandle<()>,
}

impl DropBankService {
    pub fn new(bank_receiver: Receiver<Vec<BankWithScheduler>>) -> Self {
        Self::new_named(
            bank_receiver,
            "solDropBankSrvc",
            "handle_new_root-dropped_banks",
        )
    }

    /// Like `new`, but with its own thread and datapoint names, so a second service's metrics
    /// stay apart from replay's.
    pub fn new_named(
        bank_receiver: Receiver<Vec<BankWithScheduler>>,
        thread_name: &'static str,
        datapoint_name: &'static str,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name(thread_name.to_string())
            .spawn(move || {
                for banks in bank_receiver.iter() {
                    let len = banks.len();
                    let mut dropped_banks_time = Measure::start("drop_banks");
                    // Drop BankWithScheduler with no alive lock to avoid deadlocks. That's because
                    // BankWithScheduler::drop() could block on transaction execution if unified
                    // scheduler is installed. As a historical context, it's dropped early inside
                    // the replaying stage not here and that caused a deadlock for BankForks.
                    drop(banks);
                    dropped_banks_time.stop();
                    if dropped_banks_time.as_ms() > 10 {
                        solana_metrics::submit(
                            DataPoint::new(datapoint_name)
                                .add_field_i64("elapsed_ms", dropped_banks_time.as_ms() as i64)
                                .add_field_i64("len", len as i64)
                                .to_owned(),
                            log::Level::Info,
                        );
                    }
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
