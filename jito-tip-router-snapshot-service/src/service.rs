use {
    crate::config::TipRouterSnapshotConfig,
    log::info,
    solana_runtime::bank_forks::BankForks,
    std::{
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle, sleep},
        time::Duration,
    },
};

const IDLE_INTERVAL: Duration = Duration::from_millis(500);

pub struct TipRouterSnapshotService {
    thread_hdl: JoinHandle<()>,
}

impl TipRouterSnapshotService {
    pub fn new(
        config: TipRouterSnapshotConfig,
        bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("tipRtSnapshot".to_string())
            .spawn(move || {
                info!("TipRouterSnapshotService has started");
                Self::run(config, bank_forks, exit);
                info!("TipRouterSnapshotService has stopped");
            })
            .unwrap();

        Self { thread_hdl }
    }

    fn run(
        _config: TipRouterSnapshotConfig,
        _bank_forks: Arc<RwLock<BankForks>>,
        exit: Arc<AtomicBool>,
    ) {
        while !exit.load(Ordering::Relaxed) {
            sleep(IDLE_INTERVAL);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
