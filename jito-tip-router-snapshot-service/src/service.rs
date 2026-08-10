use {
    crate::{
        config::TipRouterSnapshotConfig,
        snapshot_artifact::{
            ArtifactDirectoryError, ArtifactResult, SnapshotArtifactError,
            SnapshotArtifactWorkerHandle, SnapshotArtifactWriter, WorkerCompletion,
        },
    },
    crossbeam_channel::{Receiver, never, select},
    log::{debug, error, info, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationReceiver, BankNotificationWithDependencyWork,
    },
    solana_runtime::bank::Bank,
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

const SHUTDOWN_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const ARTIFACT_WORKER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);

/// Wrapper type around handle
pub struct TipRouterSnapshotService {
    thread_hdl: JoinHandle<Result<(), TipRouterSnapshotServiceError>>,
}

#[derive(Debug)]
pub enum TipRouterSnapshotServiceError {
    BankNotificationChannelDisconnected,
    ArtifactDirectoryInitialization { path: PathBuf, source: io::Error },
    ArtifactDirectoryUnavailable { path: PathBuf, source: io::Error },
    ArtifactWorkerShutdownTimeout { epoch: Epoch, timeout: Duration },
}

impl fmt::Display for TipRouterSnapshotServiceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BankNotificationChannelDisconnected => {
                write!(
                    formatter,
                    "tip-router snapshot bank notification channel disconnected"
                )
            }
            Self::ArtifactDirectoryInitialization { path, source } => write!(
                formatter,
                "failed to initialize tip-router snapshot artifact directory {}: {source}",
                path.display()
            ),
            Self::ArtifactDirectoryUnavailable { path, source } => write!(
                formatter,
                "tip-router snapshot artifact directory {} became unavailable: {source}",
                path.display()
            ),
            Self::ArtifactWorkerShutdownTimeout { epoch, timeout } => write!(
                formatter,
                "tip-router snapshot worker for epoch {epoch} did not stop within {timeout:?}"
            ),
        }
    }
}

impl error::Error for TipRouterSnapshotServiceError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::BankNotificationChannelDisconnected => None,
            Self::ArtifactDirectoryInitialization { source, .. }
            | Self::ArtifactDirectoryUnavailable { source, .. } => Some(source),
            Self::ArtifactWorkerShutdownTimeout { .. } => None,
        }
    }
}

pub type TipRouterSnapshotServiceResult = Result<(), TipRouterSnapshotServiceError>;

/// State owned exclusively by the tip-router notification thread.
#[derive(Default)]
struct TipRouterSnapshotServiceContext {
    /// The last epoch boundary this tip router has processed
    /// None on startup. Always Some after the first claim
    // TODO: Can we inject the current epoch on startup?
    last_claimed_epoch: Option<Epoch>,
    /// Whether a worker thread is actively creating an artifact for the current epoch boundary
    active_worker: Option<SnapshotArtifactWorkerHandle>,
    /// The highest root observed by this service.
    highest_rooted_slot: Option<Slot>,
    /// A fully written artifact waiting for its candidate slot to become rooted.
    pending_publish: Option<PendingPublish>,
}

struct PendingPublish {
    epoch: Epoch,
    slot: Slot,
    bank_hash: Hash,
    temp_path: PathBuf,
}

impl TipRouterSnapshotService {
    pub fn new(
        config: TipRouterSnapshotConfig,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> Result<Self, TipRouterSnapshotServiceError> {
        let artifact_writer = SnapshotArtifactWriter::new(config.output_dir.clone()).map_err(
            |ArtifactDirectoryError { path, source }| {
                TipRouterSnapshotServiceError::ArtifactDirectoryInitialization { path, source }
            },
        )?;
        let thread_hdl = Builder::new()
            .name("tipRtSnapshot".to_string())
            .spawn(move || {
                info!("TipRouterSnapshotService has started");

                let result = Self::run(config, artifact_writer, bank_notification_receiver, exit);

                info!("TipRouterSnapshotService has stopped");
                result
            })
            //TODO: We prob want to trigger restart of the whole machine here
            .expect("Failed to spawn tipRtSnapshot thread");

        Ok(Self { thread_hdl })
    }

    fn run(
        config: TipRouterSnapshotConfig,
        artifact_writer: SnapshotArtifactWriter,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let mut context = TipRouterSnapshotServiceContext::default();

        while !exit.load(Ordering::Relaxed) {
            // The `never` is a neat little trick to branch on an optional rx
            let artifact_result_receiver = context
                .active_artifact_result_receiver()
                .unwrap_or_else(never);

            select! {
                recv(bank_notification_receiver) -> notification => match notification {
                    Ok(notification) => context.handle_bank_notification(
                        &config,
                        &artifact_writer,
                        notification,
                        &exit,
                    )?,

                    // Notifications channel shutdown and this service has also been ordered to
                    // shutdown
                    Err(_) if exit.load(Ordering::Relaxed) => break,

                    // Notifications channel shutdown unexpectantly
                    Err(_) => {
                        context.wait_for_inflight_artifact_worker(&artifact_writer, &exit)?;
                        return Err(TipRouterSnapshotServiceError::BankNotificationChannelDisconnected);
                    }
                },

                // Background artifact worker is finished running
                recv(artifact_result_receiver) -> received_artifact_result => {
                    // TODO: What if it does not exist? Would that be bad?
                    if let Some(active_worker) = context.active_worker.take() {
                        context.record_worker_completion(
                            active_worker.join_and_classify(received_artifact_result),
                            &artifact_writer,
                            &exit,
                        )?;
                    }
                }
                default(SHUTDOWN_CHECK_INTERVAL) => {}
            }
        }

        // TODO: Don't log here. We log at entry and exit. BUT we should make sure active worker
        // logs
        context.wait_for_inflight_artifact_worker(&artifact_writer, &exit)?;
        Ok(())
    }

    pub fn join(self) -> thread::Result<TipRouterSnapshotServiceResult> {
        self.thread_hdl.join()
    }
}

impl TipRouterSnapshotServiceContext {
    //TODO: We wrap it for now since this will include more logic eventually
    fn handle_bank_notification(
        &mut self,
        config: &TipRouterSnapshotConfig,
        artifact_writer: &SnapshotArtifactWriter,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        match notification {
            BankNotification::Frozen(bank) => {
                self.handle_frozen_bank(config, artifact_writer, bank);
                Ok(())
            }
            BankNotification::NewRootBank(bank) => {
                self.handle_new_root(bank.slot(), artifact_writer, exit)
            }
            BankNotification::OptimisticallyConfirmed(_) | BankNotification::NewRootedChain(_) => {
                Ok(())
            }
        }
    }

    fn handle_new_root(
        &mut self,
        rooted_slot: Slot,
        artifact_writer: &SnapshotArtifactWriter,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        self.highest_rooted_slot = Some(
            self.highest_rooted_slot
                .map_or(rooted_slot, |current| current.max(rooted_slot)),
        );
        self.try_publish_pending(artifact_writer, exit)
    }

    fn handle_frozen_bank(
        &mut self,
        config: &TipRouterSnapshotConfig,
        artifact_writer: &SnapshotArtifactWriter,
        boundary_child_bank: Arc<Bank>,
    ) {
        let Some((_epoch, parent_bank)) = self.claimable_epoch_boundary(boundary_child_bank) else {
            return;
        };

        debug!(
            "claiming tip-router snapshot at slot={}, bank_hash={}, epoch={}",
            parent_bank.slot(),
            parent_bank.hash(),
            parent_bank.epoch(),
        );

        let Ok(active_worker) = SnapshotArtifactWorkerHandle::spawn(
            config.clone(),
            artifact_writer.clone(),
            parent_bank,
        )
        .map_err(|err| error!("failed to spawn tip-router snapshot worker: {err}")) else {
            return;
        };

        self.last_claimed_epoch = Some(active_worker.artifact_epoch());
        self.active_worker = Some(active_worker);
    }

    /// Grabs the Arc<Bank> of the boundary parent if it is valid
    fn claimable_epoch_boundary(
        &self,
        boundary_child_bank: Arc<Bank>,
    ) -> Option<(Epoch, Arc<Bank>)> {
        let Some(parent_bank) = boundary_child_bank.parent() else {
            warn!("frozen bank has no parent");
            return None;
        };

        if boundary_child_bank.epoch() <= parent_bank.epoch() {
            return None;
        }

        let epoch = parent_bank.epoch();
        if self
            .last_claimed_epoch
            .is_some_and(|last_claimed_epoch| epoch <= last_claimed_epoch)
        {
            return None;
        }

        if let Some(active_worker) = self.active_worker.as_ref() {
            // TODO: Fork Handling
            warn!(
                "tip-router snapshot worker for epoch {} is still running at epoch {} boundary",
                active_worker.artifact_epoch(),
                epoch,
            );
            return None;
        }

        Some((epoch, parent_bank))
    }

    fn active_artifact_result_receiver(&self) -> Option<Receiver<ArtifactResult>> {
        self.active_worker
            .as_ref()
            .map(SnapshotArtifactWorkerHandle::artifact_result_receiver)
    }

    fn wait_for_inflight_artifact_worker(
        &mut self,
        artifact_writer: &SnapshotArtifactWriter,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let Some(active_worker) = self.active_worker.take() else {
            return Ok(());
        };

        self.record_worker_completion(
            active_worker.wait_for_completion_or_timeout(ARTIFACT_WORKER_SHUTDOWN_TIMEOUT),
            artifact_writer,
            exit,
        )
    }

    fn record_worker_completion(
        &mut self,
        completion: WorkerCompletion,
        artifact_writer: &SnapshotArtifactWriter,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        match completion {
            WorkerCompletion::Written {
                epoch,
                slot,
                bank_hash,
                temp_path,
            } => {
                info!(
                    "wrote temporary tip-router snapshot artifact for epoch {} at slot {} with \
                     bank hash {} to {}",
                    epoch,
                    slot,
                    bank_hash,
                    temp_path.display(),
                );
                self.pending_publish = Some(PendingPublish {
                    epoch,
                    slot,
                    bank_hash,
                    temp_path,
                });
                self.try_publish_pending(artifact_writer, exit)
            }
            WorkerCompletion::Failed {
                epoch,
                err: SnapshotArtifactError::DirectoryUnavailable { path, source },
            } => {
                error!(
                    "tip-router snapshot artifact directory became unavailable while writing epoch \
                     {}: {}: {}",
                    epoch,
                    path.display(),
                    source
                );
                exit.store(true, Ordering::Relaxed);
                Err(TipRouterSnapshotServiceError::ArtifactDirectoryUnavailable { path, source })
            }
            WorkerCompletion::Failed { epoch, err } => {
                error!(
                    "tip-router snapshot artifact failed for epoch {}: {err}",
                    epoch,
                );
                Ok(())
            }
            WorkerCompletion::Panicked { epoch } => {
                error!("tip-router snapshot worker panicked for epoch {}", epoch,);
                Ok(())
            }
            WorkerCompletion::MissingResult { epoch } => {
                error!(
                    "tip-router snapshot worker exited without a result for epoch {}",
                    epoch,
                );
                Ok(())
            }
            WorkerCompletion::TimedOut { epoch, timeout } => {
                error!(
                    "tip-router snapshot worker for epoch {epoch} did not stop within {timeout:?}; \
                     detaching worker"
                );
                exit.store(true, Ordering::Relaxed);
                Err(TipRouterSnapshotServiceError::ArtifactWorkerShutdownTimeout { epoch, timeout })
            }
        }
    }

    fn try_publish_pending(
        &mut self,
        artifact_writer: &SnapshotArtifactWriter,
        exit: &Arc<AtomicBool>,
    ) -> TipRouterSnapshotServiceResult {
        let Some(pending) = self.pending_publish.as_ref() else {
            return Ok(());
        };
        if self
            .highest_rooted_slot
            .is_none_or(|rooted_slot| rooted_slot < pending.slot)
        {
            return Ok(());
        }

        match artifact_writer.publish(&pending.temp_path, pending.epoch) {
            Ok(artifact_path) => {
                info!(
                    "published tip-router snapshot artifact for epoch {} from slot {} with bank \
                     hash {} to {}",
                    pending.epoch,
                    pending.slot,
                    pending.bank_hash,
                    artifact_path.display(),
                );
                self.pending_publish = None;
                Ok(())
            }
            Err(SnapshotArtifactError::DirectoryUnavailable { path, source }) => {
                error!(
                    "tip-router snapshot artifact directory became unavailable while publishing \
                     epoch {}: {}: {}",
                    pending.epoch,
                    path.display(),
                    source
                );
                exit.store(true, Ordering::Relaxed);
                Err(TipRouterSnapshotServiceError::ArtifactDirectoryUnavailable { path, source })
            }
            Err(err) => {
                error!(
                    "failed to publish tip-router snapshot artifact for epoch {} from {}: {err}",
                    pending.epoch,
                    pending.temp_path.display(),
                );
                Ok(())
            }
        }
    }
}
