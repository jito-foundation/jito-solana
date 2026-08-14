mod context;
mod publication_state;
mod worker_pool;

use {
    self::context::TipRouterSnapshotServiceContext,
    crate::{
        artifact_store::{ArtifactStore, ArtifactStoreError},
        config::TipRouterSnapshotConfig,
    },
    crossbeam_channel::unbounded,
    log::info,
    solana_clock::{BankId, Epoch, Slot},
    solana_rpc::optimistically_confirmed_bank_tracker::BankNotificationReceiver,
    std::{
        io,
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
#[derive(Debug, thiserror::Error)]
pub enum TipRouterSnapshotServiceError {
    #[error("bank notification channel disconnected")]
    BankNotificationChannelDisconnected,
    #[error("worker completion channel disconnected")]
    WorkerCompletionChannelDisconnected,
    #[error("rooted snapshot candidate failed for epoch {epoch}, slot {slot}, bank {bank_id}")]
    RootedCandidateFailed {
        epoch: Epoch,
        slot: Slot,
        bank_id: BankId,
    },
    #[error("failed to initialize artifact store at {}: {source}", path.display())]
    ArtifactStoreInitialization {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("artifact store became unavailable at {}: {source}", path.display())]
    ArtifactStoreUnavailable {
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("{worker_count} artifact workers did not shut down within {timeout:?}")]
    ArtifactWorkersShutdownTimeout {
        worker_count: usize,
        timeout: Duration,
    },
}

pub type TipRouterSnapshotServiceResult = Result<(), TipRouterSnapshotServiceError>;

impl TipRouterSnapshotService {
    pub fn new(
        config: TipRouterSnapshotConfig,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, TipRouterSnapshotServiceError> {
        let artifact_store = ArtifactStore::new(config.output_dir.clone()).map_err(|error| {
            let ArtifactStoreError::DirectoryUnavailable { path, source } = error else {
                unreachable!("ArtifactStore::new only returns directory-unavailable errors");
            };
            TipRouterSnapshotServiceError::ArtifactStoreInitialization { path, source }
        })?;

        let thread_hdl = Builder::new()
            .name("tipRtSnapshot".to_string())
            .spawn(move || {
                info!("TipRouterSnapshotService has started");
                let result = Self::run(config, artifact_store, bank_notification_receiver, exit);
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
        artifact_store: ArtifactStore,
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
                        &artifact_store,
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
