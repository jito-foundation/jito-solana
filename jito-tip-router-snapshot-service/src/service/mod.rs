mod context;
mod metrics;
mod publication_state;
mod worker_pool;

use {
    self::{context::TipRouterSnapshotServiceContext, metrics::report_fatal_exit},
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
    pub fn init(
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
                    report_fatal_exit(e);
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
            service_result = crossbeam_channel::select! {

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
                    Ok(completion) => context.handle_worker_completion(&artifact_store, completion),
                    Err(_) => Err(
                        TipRouterSnapshotServiceError::WorkerCompletionChannelDisconnected,
                    ),
                },

                // Needed so that the service checks the `exit` bool every so often
                // Though in theory, this never fires, bc bank-notifs should fire much faster
                default(EXIT_POLL_INTERVAL) => Ok(()),
            };

            // If the service breaks, break out of the loop and shutdown
            if service_result.is_err() {
                break;
            }
        }

        // Wait for any inflight workers/writers
        context.shutdown_workers(&artifact_store, &completion_receiver, &exit)?;
        service_result
    }

    pub fn join(self) -> thread::Result<TipRouterSnapshotServiceResult> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::publication_state::SnapshotPublicationTracker,
        crate::notification_filter::TipRouterEpochBoundaryFilter,
        solana_clock::Slot,
        solana_epoch_schedule::EpochSchedule,
        solana_hash::Hash,
        solana_rpc::optimistically_confirmed_bank_tracker::{
            BankNotification, BankNotificationBroadcaster, BankNotificationFilter,
            BankNotificationSender,
        },
        solana_runtime::{
            bank::{Bank, SlotLeader},
            genesis_utils::create_genesis_config,
        },
        std::sync::Arc,
    };

    fn genesis_bank() -> Arc<Bank> {
        let mut genesis_config = create_genesis_config(1_000_000).genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::custom(32, 32, false);
        Arc::new(Bank::new_for_tests(&genesis_config))
    }

    fn boundary_child_bank(parent_slot: Slot, child_slot: Slot) -> Arc<Bank> {
        let mut genesis_config = create_genesis_config(1_000_000).genesis_config;
        genesis_config.epoch_schedule = EpochSchedule::custom(32, 32, false);
        let (genesis_bank, _bank_forks) =
            Bank::new_for_tests(&genesis_config).wrap_with_bank_forks_for_tests();
        let parent_bank = if parent_slot == 0 {
            genesis_bank
        } else {
            Arc::new(Bank::new_from_parent(
                genesis_bank,
                SlotLeader::default(),
                parent_slot,
            ))
        };
        Arc::new(Bank::new_from_parent(
            parent_bank,
            SlotLeader::default(),
            child_slot,
        ))
    }

    fn assert_frozen_bank_wiring(bank: Arc<Bank>, should_forward: bool) {
        let notification = BankNotification::Frozen(bank.clone());
        let filter_result = TipRouterEpochBoundaryFilter.should_forward(&notification);
        assert_eq!(filter_result, should_forward);

        let publication_result = SnapshotPublicationTracker::new()
            .eligible_candidate_from_boundary_child(bank)
            .is_some();
        assert_eq!(publication_result, filter_result);

        let (sender, receiver) = BankNotificationSender::channel_with_filter(
            "tip-router-epoch-boundary-test",
            TipRouterEpochBoundaryFilter,
        );
        let broadcaster = BankNotificationBroadcaster::new(vec![sender]);
        assert_eq!(broadcaster.send((notification, None)), Ok(()));
        assert_eq!(receiver.try_recv().is_ok(), should_forward);
    }

    #[test]
    fn frozen_bank_at_exact_epoch_boundary_is_forwarded() {
        assert_frozen_bank_wiring(boundary_child_bank(31, 32), true);
    }

    #[test]
    fn frozen_bank_with_skipped_leading_slots_of_next_epoch_is_forwarded() {
        assert_frozen_bank_wiring(boundary_child_bank(31, 34), true);
    }

    #[test]
    fn frozen_bank_with_skipped_trailing_slots_of_candidate_epoch_is_forwarded() {
        assert_frozen_bank_wiring(boundary_child_bank(29, 32), true);
    }

    #[test]
    fn frozen_bank_with_an_entire_skipped_epoch_is_forwarded() {
        assert_frozen_bank_wiring(boundary_child_bank(29, 64), true);
    }

    #[test]
    fn frozen_bank_with_parent_in_same_epoch_is_rejected() {
        assert_frozen_bank_wiring(boundary_child_bank(5, 6), false);
    }

    #[test]
    fn frozen_genesis_bank_is_rejected() {
        assert_frozen_bank_wiring(genesis_bank(), false);
    }

    #[test]
    fn non_frozen_notification_variants_are_filtered_as_expected() {
        let bank = genesis_bank();
        let cases = [
            (
                BankNotification::OptimisticallyConfirmed(0, Hash::default()),
                false,
            ),
            (BankNotification::NewRootBank(bank), false),
            (BankNotification::NewRootedChain(vec![(0, 0)], 0), true),
        ];

        for (notification, should_forward) in cases {
            let (sender, receiver) = BankNotificationSender::channel_with_filter(
                "tip-router-non-frozen-test",
                TipRouterEpochBoundaryFilter,
            );
            let broadcaster = BankNotificationBroadcaster::new(vec![sender]);
            assert_eq!(broadcaster.send((notification, None)), Ok(()));
            assert_eq!(receiver.try_recv().is_ok(), should_forward);
        }
    }
}
