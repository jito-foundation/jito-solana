mod context;
mod publication_state;
mod worker_pool;

use {
    self::context::TipRouterSnapshotServiceContext,
    crate::{
        candidate_store::{CandidateStore, CandidateStoreInitializationError},
        config::TipRouterSnapshotConfig,
    },
    crossbeam_channel::unbounded,
    log::info,
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_rpc::optimistically_confirmed_bank_tracker::BankNotificationReceiver,
    std::{
        error, fmt, io,
        path::PathBuf,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

/// How frequently to check for node-shutdown notification
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(1000);

pub struct TipRouterSnapshotService {
    thread_hdl: JoinHandle<Result<(), TipRouterSnapshotServiceError>>,
}

// Any of these errors result in full shutdown of the service
#[derive(Debug)]
pub enum TipRouterSnapshotServiceError {
    BankNotificationChannelDisconnected,
    WorkerCompletionChannelDisconnected,
    RootedCandidateFailed {
        epoch: Epoch,
        slot: Slot,
        bank_hash: Hash,
    },
    CandidateStoreInitialization {
        path: PathBuf,
        source: io::Error,
    },
    CandidateStoreUnavailable {
        path: PathBuf,
        source: io::Error,
    },
    ArtifactWorkersShutdownTimeout {
        worker_count: usize,
        timeout: Duration,
    },
}

impl fmt::Display for TipRouterSnapshotServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BankNotificationChannelDisconnected => write!(
                formatter,
                "tip-router snapshot bank notification channel disconnected"
            ),
            Self::WorkerCompletionChannelDisconnected => write!(
                formatter,
                "tip-router snapshot worker completion channel disconnected"
            ),
            Self::RootedCandidateFailed {
                epoch,
                slot,
                bank_hash,
            } => write!(
                formatter,
                "rooted tip-router snapshot candidate failed for epoch {epoch} at slot {slot} \
                 with bank hash {bank_hash}"
            ),
            Self::CandidateStoreInitialization { path, source } => write!(
                formatter,
                "failed to initialize tip-router snapshot candidate store {}: {source}",
                path.display()
            ),
            Self::CandidateStoreUnavailable { path, source } => write!(
                formatter,
                "tip-router snapshot candidate store {} became unavailable: {source}",
                path.display()
            ),
            Self::ArtifactWorkersShutdownTimeout {
                worker_count,
                timeout,
            } => write!(
                formatter,
                "{worker_count} tip-router snapshot workers did not stop within {timeout:?}"
            ),
        }
    }
}

impl error::Error for TipRouterSnapshotServiceError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::CandidateStoreInitialization { source, .. }
            | Self::CandidateStoreUnavailable { source, .. } => Some(source),
            Self::BankNotificationChannelDisconnected
            | Self::WorkerCompletionChannelDisconnected
            | Self::RootedCandidateFailed { .. }
            | Self::ArtifactWorkersShutdownTimeout { .. } => None,
        }
    }
}

pub type TipRouterSnapshotServiceResult = Result<(), TipRouterSnapshotServiceError>;

impl TipRouterSnapshotService {
    pub fn new(
        config: TipRouterSnapshotConfig,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, TipRouterSnapshotServiceError> {
        let candidate_store = CandidateStore::new(config.output_dir.clone()).map_err(
            |CandidateStoreInitializationError { path, source }| {
                TipRouterSnapshotServiceError::CandidateStoreInitialization { path, source }
            },
        )?;

        let thread_hdl = Builder::new()
            .name("tipRtSnapshot".to_string())
            .spawn(move || {
                info!("TipRouterSnapshotService has started");
                let result = Self::run(config, candidate_store, bank_notification_receiver, exit);
                if let Err(e) = result.as_ref() {
                    log::error!("TipRouterSnapshotService critical error: {:?}", e);
                }
                info!("TipRouterSnapshotService has stopped");
                result
            })
            .expect("Failed to spawn tipRtSnapshot thread");

        Ok(Self { thread_hdl })
    }

    fn run(
        config: TipRouterSnapshotConfig,
        candidate_store: CandidateStore,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let (completion_sender, completion_receiver) = unbounded();

        let mut context = TipRouterSnapshotServiceContext::new(completion_sender);
        let mut service_result = Ok(());

        while !exit.load(Ordering::Relaxed) {
            let iteration_result = crossbeam_channel::select! {

                // Frozen and Rooted bank notifications
                recv(bank_notification_receiver) -> notification => match notification {
                    Ok(notification) => context.handle_bank_notification(
                        &config,
                        &candidate_store,
                        notification,
                    ),
                    Err(_) if exit.load(Ordering::Relaxed) => Ok(()),
                    Err(_) => Err(TipRouterSnapshotServiceError::BankNotificationChannelDisconnected),
                },

                // StakeMeta generation / writer threads finishing
                recv(completion_receiver) -> completion => match completion {
                    Ok(completion) => context.handle_worker_completion(
                        completion,
                        &exit,
                    ),
                    Err(_) => Err(
                        // This should be technically unreachable
                        TipRouterSnapshotServiceError::WorkerCompletionChannelDisconnected,
                    ),
                },

                // Needed so that the service checks the `exit` bool every so often
                default(EXIT_POLL_INTERVAL) => Ok(()),
            };

            // If the service breaks, break out of the loop and shutdown
            if iteration_result.is_err() {
                service_result = iteration_result;
                break;
            }
        }

        // Wait for any inflight workers/writers
        match context.shutdown_workers(&completion_receiver, &exit) {
            Ok(()) => service_result,
            Err(shutdown_err) => Err(shutdown_err),
        }
    }

    pub fn join(self) -> thread::Result<TipRouterSnapshotServiceResult> {
        self.thread_hdl.join()
    }
}
