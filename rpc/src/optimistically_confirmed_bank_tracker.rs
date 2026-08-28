//! The `optimistically_confirmed_bank_tracker` module implements a threaded service to track the
//! most recent optimistically confirmed bank for use in rpc services, and triggers gossip
//! subscription notifications.
//! This module also supports notifying of slot status for subscribers using the SlotNotificationSender.
//! It receives the BankNotification events, transforms them into SlotNotification and sends them via
//! SlotNotificationSender in the following way:
//! BankNotification::OptimisticallyConfirmed --> SlotNotification::OptimisticallyConfirmed
//! BankNotification::Frozen --> SlotNotification::Frozen
//! BankNotification::NewRootedChain --> SlotNotification::Root for the roots in the chain.

use {
    crate::rpc_subscriptions::RpcSubscriptions,
    crossbeam_channel::{Receiver, RecvTimeoutError, SendError, Sender, unbounded},
    solana_clock::Slot,
    solana_metrics::datapoint_info,
    solana_rpc_client_api::response::{SlotTransactionStats, SlotUpdate},
    solana_runtime::{
        bank::Bank, bank_forks::BankForks, dependency_tracker::DependencyTracker,
        prioritization_fee_cache::PrioritizationFeeCache,
    },
    solana_time_utils::timestamp,
    std::{
        collections::HashSet,
        sync::{
            Arc, RwLock,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

pub struct OptimisticallyConfirmedBank {
    pub bank: Arc<Bank>,
}

impl OptimisticallyConfirmedBank {
    pub fn locked_from_bank_forks_root(bank_forks: &RwLock<BankForks>) -> Arc<RwLock<Self>> {
        Arc::new(RwLock::new(Self {
            bank: bank_forks.read().unwrap().root_bank(),
        }))
    }
}

#[derive(Clone)]
pub enum BankNotification {
    OptimisticallyConfirmed(Slot),
    Frozen(Arc<Bank>),
    NewRootBank(Arc<Bank>),
    /// The newly rooted slot chain including the parent slot of the oldest bank in the rooted chain.
    NewRootedChain(Vec<Slot>),
}

#[derive(Clone, Debug)]
pub enum SlotNotification {
    OptimisticallyConfirmed(Slot),
    /// The (Slot, Parent Slot) pair for the slot frozen
    Frozen((Slot, Slot)),
    /// The (Slot, Parent Slot) pair for the root slot
    Root((Slot, Slot)),
}

impl std::fmt::Debug for BankNotification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BankNotification::OptimisticallyConfirmed(slot) => {
                write!(f, "OptimisticallyConfirmed({slot:?})")
            }
            BankNotification::Frozen(bank) => write!(f, "Frozen({})", bank.slot()),
            BankNotification::NewRootBank(bank) => write!(f, "Root({})", bank.slot()),
            BankNotification::NewRootedChain(chain) => write!(f, "RootedChain({chain:?})"),
        }
    }
}

pub type BankNotificationWithDependencyWork = (
    BankNotification,
    Option<u64>, // dependency work id
);

pub type BankNotificationReceiver = Receiver<BankNotificationWithDependencyWork>;

/// Decides whether a subscriber should receive a given bank notification.
///
/// This method runs synchronously on replay, gossip, and root-processing producer threads.
/// Implementations must return quickly and must not perform blocking I/O, wait on contended locks,
/// do expensive work, or panic. A slow, blocking, or panicking filter delays or disrupts
/// bank-notification production for every subscriber.
///
/// Filters and subscribers at this layer observe raw producer notifications, before the
/// optimistically confirmed bank tracker applies deduplication, defers notifications for banks
/// that are not yet frozen, or handles bank hash mismatches. Duplicate and out-of-order optimistic
/// confirmation notifications for the same slot are normal and must be handled by subscribers.
pub trait BankNotificationFilter: Send + Sync + 'static {
    fn should_forward(&self, notification: &BankNotification) -> bool;
}

pub struct BankNotificationSender {
    label: &'static str,
    tx: Sender<BankNotificationWithDependencyWork>,
    filter: Option<Box<dyn BankNotificationFilter>>,
    disconnected: AtomicBool,
}

impl BankNotificationSender {
    /// Create a subscriber that receives every notification.
    ///
    /// Prefer [`Self::channel_with_filter`] whenever the subscriber can ignore some
    /// notifications. The primary benefit is avoiding retention of `Arc<Bank>` values in this
    /// unbounded channel, not reducing optimistic-confirmation message throughput.
    /// `label` must uniquely identify this subscriber; it is emitted as the `subscriber` metric
    /// tag for queue-depth telemetry.
    pub fn channel(label: &'static str) -> (Self, BankNotificationReceiver) {
        Self::new_channel(label, None)
    }

    /// Create a subscriber that receives only notifications accepted by `filter`.
    /// `label` must uniquely identify this subscriber; it is emitted as the `subscriber` metric
    /// tag for queue-depth telemetry.
    pub fn channel_with_filter<F: BankNotificationFilter>(
        label: &'static str,
        filter: F,
    ) -> (Self, BankNotificationReceiver) {
        Self::new_channel(label, Some(Box::new(filter)))
    }

    fn new_channel(
        label: &'static str,
        filter: Option<Box<dyn BankNotificationFilter>>,
    ) -> (Self, BankNotificationReceiver) {
        // All subscriber channels are unbounded so sending to one subscriber cannot block
        // consensus-critical producers or delay notification delivery to other subscribers.
        let (tx, rx) = unbounded();
        (
            Self {
                label,
                tx,
                filter,
                disconnected: AtomicBool::new(false),
            },
            rx,
        )
    }

    /// Forward `notification` if accepted by this subscriber's filter, returning whether the
    /// subscriber is assumed connected.
    fn forward(&self, notification: &BankNotificationWithDependencyWork) -> bool {
        // A disconnected receiver cannot reconnect. Avoid all subsequent filter, clone, send,
        // and logging work once a failed send establishes disconnection.
        if self.disconnected.load(Ordering::Relaxed) {
            return false;
        }

        if !self
            .filter
            .as_ref()
            .is_none_or(|filter| filter.should_forward(&notification.0))
        {
            return true;
        }

        match self.tx.send(notification.clone()) {
            Ok(()) => {
                datapoint_info!(
                    "bank-notification-queue-depth",
                    "subscriber" => self.label,
                    ("depth", self.tx.len(), i64),
                );
                true
            }
            Err(SendError(notification)) => {
                // Concurrent producers may observe the first failure together. `swap` ensures
                // only one of them emits the disconnection warning.
                if !self.disconnected.swap(true, Ordering::Relaxed) {
                    warn!(
                        "bank notification subscriber '{}' disconnected, dropping {:?}",
                        self.label, notification.0
                    );
                }
                false
            }
        }
    }
}

#[derive(Clone)]
pub struct BankNotificationBroadcaster {
    subscriber_senders: Arc<[BankNotificationSender]>,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BankNotificationBroadcastError;

impl BankNotificationBroadcaster {
    pub fn new(subscriber_senders: Vec<BankNotificationSender>) -> Self {
        Self {
            subscriber_senders: subscriber_senders.into(),
        }
    }

    /// Broadcasts a notification to each matching subscriber.
    ///
    /// Concurrent calls are not serialized across subscribers. Each subscriber's channel
    /// preserves its own send order, but different subscribers may observe concurrent
    /// notifications in different relative orders because their fan-out loops can interleave.
    /// Consumers must not rely on a common global ordering across subscriber channels.
    pub fn send(
        &self,
        notification: BankNotificationWithDependencyWork,
    ) -> Result<(), BankNotificationBroadcastError> {
        let mut connected_subscriber_count = 0;

        for sender in self.subscriber_senders.iter() {
            // A subscriber that filters the notification out is still assumed connected, since its
            // channel was never touched.
            if sender.forward(&notification) {
                connected_subscriber_count += 1;
            }
        }

        if connected_subscriber_count == 0 {
            warn!("bank notification broadcast failed: no connected subscribers");
            return Err(BankNotificationBroadcastError);
        }

        Ok(())
    }
}

#[derive(Clone)]
pub struct BankNotificationSenderConfig {
    pub sender: BankNotificationBroadcaster,
    pub should_send_parents: bool,
    pub dependency_tracker: Option<Arc<DependencyTracker>>,
}

pub type SlotNotificationReceiver = Receiver<SlotNotification>;
pub type SlotNotificationSender = Sender<SlotNotification>;

pub struct OptimisticallyConfirmedBankTracker {
    thread_hdl: JoinHandle<()>,
}

impl OptimisticallyConfirmedBankTracker {
    pub fn new(
        receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
        bank_forks: Arc<RwLock<BankForks>>,
        optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>>,
        subscriptions: Arc<RpcSubscriptions>,
        slot_notification_subscribers: Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        prioritization_fee_cache: Option<Arc<PrioritizationFeeCache>>,
        dependency_tracker: Option<Arc<DependencyTracker>>,
    ) -> Self {
        let mut pending_optimistically_confirmed_banks = HashSet::new();
        let mut last_notified_confirmed_slot: Slot = 0;
        let mut highest_confirmed_slot: Slot = 0;
        let mut newest_root_slot: Slot = 0;
        let thread_hdl = Builder::new()
            .name("solOpConfBnkTrk".to_string())
            .spawn(move || {
                loop {
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }

                    if let Err(RecvTimeoutError::Disconnected) = Self::recv_notification(
                        &receiver,
                        &bank_forks,
                        &optimistically_confirmed_bank,
                        &subscriptions,
                        &mut pending_optimistically_confirmed_banks,
                        &mut last_notified_confirmed_slot,
                        &mut highest_confirmed_slot,
                        &mut newest_root_slot,
                        &slot_notification_subscribers,
                        prioritization_fee_cache.as_deref(),
                        &dependency_tracker,
                    ) {
                        break;
                    }
                }
            })
            .unwrap();
        Self { thread_hdl }
    }

    #[allow(clippy::too_many_arguments)]
    fn recv_notification(
        receiver: &Receiver<BankNotificationWithDependencyWork>,
        bank_forks: &RwLock<BankForks>,
        optimistically_confirmed_bank: &RwLock<OptimisticallyConfirmedBank>,
        subscriptions: &RpcSubscriptions,
        pending_optimistically_confirmed_banks: &mut HashSet<Slot>,
        last_notified_confirmed_slot: &mut Slot,
        highest_confirmed_slot: &mut Slot,
        newest_root_slot: &mut Slot,
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        prioritization_fee_cache: Option<&PrioritizationFeeCache>,
        dependency_tracker: &Option<Arc<DependencyTracker>>,
    ) -> Result<(), RecvTimeoutError> {
        let notification = receiver.recv_timeout(Duration::from_secs(1))?;
        Self::process_notification(
            notification,
            bank_forks,
            optimistically_confirmed_bank,
            subscriptions,
            pending_optimistically_confirmed_banks,
            last_notified_confirmed_slot,
            highest_confirmed_slot,
            newest_root_slot,
            slot_notification_subscribers,
            prioritization_fee_cache,
            dependency_tracker,
        );
        Ok(())
    }

    fn notify_slot_status(
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        notification: SlotNotification,
    ) {
        if let Some(slot_notification_subscribers) = slot_notification_subscribers {
            for sender in slot_notification_subscribers.read().unwrap().iter() {
                match sender.send(notification.clone()) {
                    Ok(_) => {}
                    Err(err) => {
                        info!("Failed to send notification {notification:?}, error: {err:?}");
                    }
                }
            }
        }
    }

    fn notify_or_defer(
        subscriptions: &RpcSubscriptions,
        bank_forks: &RwLock<BankForks>,
        bank: &Bank,
        last_notified_confirmed_slot: &mut Slot,
        pending_optimistically_confirmed_banks: &mut HashSet<Slot>,
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        prioritization_fee_cache: Option<&PrioritizationFeeCache>,
    ) {
        if bank.is_frozen() {
            if bank.slot() > *last_notified_confirmed_slot {
                debug!(
                    "notify_or_defer notifying via notify_gossip_subscribers for slot {:?}",
                    bank.slot()
                );
                subscriptions.notify_gossip_subscribers(bank.slot());
                *last_notified_confirmed_slot = bank.slot();
                Self::notify_slot_status(
                    slot_notification_subscribers,
                    SlotNotification::OptimisticallyConfirmed(bank.slot()),
                );

                // finalize block's minimum prioritization fee cache for this bank
                if let Some(prioritization_fee_cache) = prioritization_fee_cache {
                    prioritization_fee_cache.finalize_priority_fee(bank.slot(), bank.bank_id());
                }
            }
        } else if bank.slot() > bank_forks.read().unwrap().root() {
            pending_optimistically_confirmed_banks.insert(bank.slot());
            debug!("notify_or_defer defer notifying for slot {:?}", bank.slot());
        }
    }

    fn notify_or_defer_confirmed_banks(
        subscriptions: &RpcSubscriptions,
        bank_forks: &RwLock<BankForks>,
        bank: Arc<Bank>,
        slot_threshold: Slot,
        last_notified_confirmed_slot: &mut Slot,
        pending_optimistically_confirmed_banks: &mut HashSet<Slot>,
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        prioritization_fee_cache: Option<&PrioritizationFeeCache>,
    ) {
        for confirmed_bank in bank.parents_inclusive().iter().rev() {
            if confirmed_bank.slot() > slot_threshold {
                debug!(
                    "Calling notify_or_defer for confirmed_bank {:?}",
                    confirmed_bank.slot()
                );
                Self::notify_or_defer(
                    subscriptions,
                    bank_forks,
                    confirmed_bank,
                    last_notified_confirmed_slot,
                    pending_optimistically_confirmed_banks,
                    slot_notification_subscribers,
                    prioritization_fee_cache,
                );
            }
        }
    }

    fn notify_new_root_slots(
        roots: &mut [Slot],
        newest_root_slot: &mut Slot,
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
    ) {
        if slot_notification_subscribers.is_none() {
            return;
        }
        roots.sort_unstable();
        // The chain are sorted already and must contain at least the parent of a newly rooted slot as the first element
        assert!(roots.len() >= 2);
        for i in 1..roots.len() {
            let root = roots[i];
            if root > *newest_root_slot {
                let parent = roots[i - 1];
                debug!("Doing SlotNotification::Root for root {root}, parent: {parent}");
                Self::notify_slot_status(
                    slot_notification_subscribers,
                    SlotNotification::Root((root, parent)),
                );
                *newest_root_slot = root;
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn process_notification(
        (notification, dependency_work): BankNotificationWithDependencyWork,
        bank_forks: &RwLock<BankForks>,
        optimistically_confirmed_bank: &RwLock<OptimisticallyConfirmedBank>,
        subscriptions: &RpcSubscriptions,
        pending_optimistically_confirmed_banks: &mut HashSet<Slot>,
        last_notified_confirmed_slot: &mut Slot,
        highest_confirmed_slot: &mut Slot,
        newest_root_slot: &mut Slot,
        slot_notification_subscribers: &Option<Arc<RwLock<Vec<SlotNotificationSender>>>>,
        prioritization_fee_cache: Option<&PrioritizationFeeCache>,
        dependency_tracker: &Option<Arc<DependencyTracker>>,
    ) {
        debug!("received bank notification: {notification:?} event: {dependency_work:?}");

        if let Some(tracker) = dependency_tracker.as_ref()
            && let Some(dependency_work) = dependency_work
        {
            tracker.wait_for_dependency(dependency_work);
        }
        match notification {
            BankNotification::OptimisticallyConfirmed(slot) => {
                let bank = bank_forks.read().unwrap().get(slot);
                if let Some(bank) = bank {
                    let mut w_optimistically_confirmed_bank =
                        optimistically_confirmed_bank.write().unwrap();

                    if bank.slot() > w_optimistically_confirmed_bank.bank.slot() && bank.is_frozen()
                    {
                        w_optimistically_confirmed_bank.bank = bank.clone();
                    }

                    if slot > *highest_confirmed_slot {
                        Self::notify_or_defer_confirmed_banks(
                            subscriptions,
                            bank_forks,
                            bank,
                            *highest_confirmed_slot,
                            last_notified_confirmed_slot,
                            pending_optimistically_confirmed_banks,
                            slot_notification_subscribers,
                            prioritization_fee_cache,
                        );

                        *highest_confirmed_slot = slot;
                    }
                    drop(w_optimistically_confirmed_bank);
                } else if slot > bank_forks.read().unwrap().root() {
                    pending_optimistically_confirmed_banks.insert(slot);
                } else {
                    inc_new_counter_info!("dropped-already-rooted-optimistic-bank-notification", 1);
                }

                // Send slot notification regardless of whether the bank is replayed
                subscriptions.notify_slot_update(SlotUpdate::OptimisticConfirmation {
                    slot,
                    timestamp: timestamp(),
                });
                // NOTE: replay of `slot` may or may not be complete. Therefore, most new
                // functionality to be triggered on optimistic confirmation should go in
                // `notify_or_defer()` under the `bank.is_frozen()` case instead of here.
            }
            BankNotification::Frozen(bank) => {
                let frozen_slot = bank.slot();
                if let Some(parent) = bank.parent() {
                    let num_successful_transactions = bank
                        .transaction_count()
                        .saturating_sub(parent.transaction_count());
                    subscriptions.notify_slot_update(SlotUpdate::Frozen {
                        slot: frozen_slot,
                        timestamp: timestamp(),
                        stats: SlotTransactionStats {
                            num_transaction_entries: bank.transaction_entries_count(),
                            num_successful_transactions,
                            num_failed_transactions: bank.transaction_error_count(),
                            max_transactions_per_entry: bank.transactions_per_entry_max(),
                        },
                    });

                    Self::notify_slot_status(
                        slot_notification_subscribers,
                        SlotNotification::Frozen((bank.slot(), bank.parent_slot())),
                    );
                }

                if pending_optimistically_confirmed_banks.remove(&bank.slot()) {
                    debug!(
                        "Calling notify_gossip_subscribers to send deferred notification \
                         {frozen_slot:?}"
                    );

                    Self::notify_or_defer_confirmed_banks(
                        subscriptions,
                        bank_forks,
                        bank.clone(),
                        *last_notified_confirmed_slot,
                        last_notified_confirmed_slot,
                        pending_optimistically_confirmed_banks,
                        slot_notification_subscribers,
                        prioritization_fee_cache,
                    );

                    let mut w_optimistically_confirmed_bank =
                        optimistically_confirmed_bank.write().unwrap();
                    if frozen_slot > w_optimistically_confirmed_bank.bank.slot() {
                        w_optimistically_confirmed_bank.bank = bank;
                    }
                    drop(w_optimistically_confirmed_bank);
                }
            }
            BankNotification::NewRootBank(bank) => {
                let root_slot = bank.slot();
                let mut w_optimistically_confirmed_bank =
                    optimistically_confirmed_bank.write().unwrap();
                if root_slot > w_optimistically_confirmed_bank.bank.slot() {
                    w_optimistically_confirmed_bank.bank = bank;
                }
                drop(w_optimistically_confirmed_bank);

                pending_optimistically_confirmed_banks.retain(|&s| s > root_slot);
            }
            BankNotification::NewRootedChain(mut roots) => {
                Self::notify_new_root_slots(
                    &mut roots,
                    newest_root_slot,
                    slot_notification_subscribers,
                );
            }
        }
    }

    pub fn close(self) -> thread::Result<()> {
        self.join()
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::bounded,
        solana_ledger::genesis_utils::{GenesisConfigInfo, create_genesis_config},
        solana_runtime::{bank::SlotLeader, commitment::BlockCommitmentCache, dependency_tracker},
        std::sync::atomic::{AtomicU64, AtomicUsize},
    };

    struct FixedBankNotificationFilter(bool);
    impl BankNotificationFilter for FixedBankNotificationFilter {
        fn should_forward(&self, _notification: &BankNotification) -> bool {
            self.0
        }
    }

    struct RejectingBankNotificationFilter {
        observed_bank_strong_count: Arc<AtomicUsize>,
    }

    impl BankNotificationFilter for RejectingBankNotificationFilter {
        fn should_forward(&self, notification: &BankNotification) -> bool {
            let BankNotification::Frozen(bank) = notification else {
                panic!("expected frozen bank notification");
            };
            self.observed_bank_strong_count
                .store(Arc::strong_count(bank), Ordering::Relaxed);
            false
        }
    }

    /// Receive the Root notifications from the channel, if no item received within 100 ms, break and return all
    /// of those received.
    fn get_root_notifications(receiver: &Receiver<SlotNotification>) -> Vec<SlotNotification> {
        let mut notifications = Vec::new();
        while let Ok(notification) = receiver.recv_timeout(Duration::from_millis(100)) {
            notifications.push(notification);
        }
        notifications
    }

    #[test]
    fn test_bank_notification_subscriber_channels_are_unbounded() {
        let (sender, _receiver) = BankNotificationSender::channel("test-subscriber");
        assert_eq!(sender.tx.capacity(), None);
    }

    #[test]
    fn test_bank_notification_sender_tracks_enqueued_depth() {
        let (sender, receiver) = BankNotificationSender::channel("test-subscriber");
        assert!(sender.forward(&(BankNotification::OptimisticallyConfirmed(1), None)));
        assert_eq!(sender.tx.len(), 1);
        assert_eq!(receiver.len(), 1);
    }

    #[test]
    fn test_bank_notification_filter_accepts_notification() {
        let (sender, receiver) = BankNotificationSender::channel_with_filter(
            "accepting-subscriber",
            FixedBankNotificationFilter(true),
        );
        let broadcaster = BankNotificationBroadcaster::new(vec![sender]);
        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), Some(7))),
            Ok(())
        );
        let (notification, dependency_work) = receiver.try_recv().unwrap();
        assert_eq!(dependency_work, Some(7));
        assert!(matches!(
            notification,
            BankNotification::OptimisticallyConfirmed(42)
        ));
    }

    #[test]
    fn test_bank_notification_filter_rejects_notification() {
        let (sender, receiver) = BankNotificationSender::channel_with_filter(
            "rejecting-subscriber",
            FixedBankNotificationFilter(false),
        );
        let broadcaster = BankNotificationBroadcaster::new(vec![sender]);

        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), None,)),
            Ok(())
        );
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn test_bank_notification_filters_are_applied_per_subscriber() {
        let (accepting_sender, accepting_receiver) = BankNotificationSender::channel_with_filter(
            "accepting-subscriber",
            FixedBankNotificationFilter(true),
        );
        let (rejecting_sender, rejecting_receiver) = BankNotificationSender::channel_with_filter(
            "rejecting-subscriber",
            FixedBankNotificationFilter(false),
        );
        let broadcaster =
            BankNotificationBroadcaster::new(vec![rejecting_sender, accepting_sender]);
        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), Some(7))),
            Ok(())
        );
        let (notification, dependency_work) = accepting_receiver.try_recv().unwrap();
        assert_eq!(dependency_work, Some(7));
        assert!(matches!(
            notification,
            BankNotification::OptimisticallyConfirmed(42)
        ));
        assert!(rejecting_receiver.try_recv().is_err());
    }

    #[test]
    fn test_bank_notification_broadcast_fails_when_all_subscribers_are_disconnected() {
        let (sender, receiver) = BankNotificationSender::channel("disconnected-subscriber");
        drop(receiver);
        let broadcaster = BankNotificationBroadcaster::new(vec![sender]);

        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), None,)),
            Err(BankNotificationBroadcastError)
        );
    }

    #[test]
    fn test_bank_notification_broadcast_fans_out_to_all_connected_subscribers() {
        let (first_sender, first_receiver) = BankNotificationSender::channel("first-subscriber");
        let (second_sender, second_receiver) = BankNotificationSender::channel("second-subscriber");
        let broadcaster = BankNotificationBroadcaster::new(vec![first_sender, second_sender]);
        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), Some(7))),
            Ok(())
        );
        for receiver in [first_receiver, second_receiver] {
            let (notification, dependency_work) = receiver.try_recv().unwrap();
            assert_eq!(dependency_work, Some(7));
            assert!(matches!(
                notification,
                BankNotification::OptimisticallyConfirmed(42)
            ));
        }
    }

    #[test]
    fn test_bank_notification_broadcast_succeeds_with_one_connected_subscriber() {
        let (disconnected_sender, disconnected_receiver) =
            BankNotificationSender::channel("disconnected-subscriber");
        drop(disconnected_receiver);
        let (connected_sender, connected_receiver) =
            BankNotificationSender::channel("connected-subscriber");
        let broadcaster =
            BankNotificationBroadcaster::new(vec![disconnected_sender, connected_sender]);
        assert_eq!(
            broadcaster.send((BankNotification::OptimisticallyConfirmed(42), Some(7))),
            Ok(())
        );
        let (notification, dependency_work) = connected_receiver.try_recv().unwrap();
        assert_eq!(dependency_work, Some(7));
        assert!(matches!(
            notification,
            BankNotification::OptimisticallyConfirmed(42)
        ));
    }

    #[test]
    fn test_filtered_out_subscriber_does_not_clone_notification() {
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100);
        let bank = Arc::new(Bank::new_for_tests(&genesis_config));
        let baseline_bank_strong_count = Arc::strong_count(&bank);
        let observed_bank_strong_count = Arc::new(AtomicUsize::new(0));
        let (sender, receiver) = BankNotificationSender::channel_with_filter(
            "rejecting-subscriber",
            RejectingBankNotificationFilter {
                observed_bank_strong_count: observed_bank_strong_count.clone(),
            },
        );
        let broadcaster = BankNotificationBroadcaster::new(vec![sender]);

        assert_eq!(
            broadcaster.send((BankNotification::Frozen(bank.clone()), None)),
            Ok(())
        );
        assert_eq!(
            observed_bank_strong_count.load(Ordering::Relaxed),
            baseline_bank_strong_count + 1
        );
        assert_eq!(Arc::strong_count(&bank), baseline_bank_strong_count);
        assert!(receiver.try_recv().is_err());
    }

    #[test]
    fn test_process_notification() {
        let exit = Arc::new(AtomicBool::new(false));
        let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100);
        let bank = Bank::new_for_tests(&genesis_config);
        let bank_forks = BankForks::new_rw_arc(bank);
        let bank0 = bank_forks.read().unwrap().get(0).unwrap();
        let bank1 = Bank::new_from_parent(bank0, SlotLeader::default(), 1);
        bank_forks.write().unwrap().insert(bank1);
        let bank1 = bank_forks.read().unwrap().get(1).unwrap();
        let bank2 = Bank::new_from_parent(bank1, SlotLeader::default(), 2);
        bank_forks.write().unwrap().insert(bank2);
        let bank2 = bank_forks.read().unwrap().get(2).unwrap();
        let bank3 = Bank::new_from_parent(bank2, SlotLeader::default(), 3);
        bank_forks.write().unwrap().insert(bank3);

        let optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>> =
            OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);

        let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));
        let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());
        let subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
            exit,
            max_complete_transaction_status_slot,
            bank_forks.clone(),
            block_commitment_cache,
            optimistically_confirmed_bank.clone(),
        ));
        let mut pending_optimistically_confirmed_banks: HashSet<u64> = HashSet::new();

        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 0);

        let mut highest_confirmed_slot: Slot = 0;
        let mut newest_root_slot: Slot = 0;

        let mut last_notified_confirmed_slot: Slot = 0;
        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::OptimisticallyConfirmed(2),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 2);
        assert_eq!(highest_confirmed_slot, 2);

        // Test max optimistically confirmed bank remains in the cache
        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::OptimisticallyConfirmed(1),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 2);
        assert_eq!(highest_confirmed_slot, 2);

        // Test bank will only be cached when frozen
        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::OptimisticallyConfirmed(3),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 2);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 1);
        assert!(pending_optimistically_confirmed_banks.contains(&3));
        assert_eq!(highest_confirmed_slot, 3);

        // Test bank will only be cached when frozen
        let bank3 = bank_forks.read().unwrap().get(3).unwrap();
        bank3.freeze();

        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::Frozen(bank3),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 3);
        assert_eq!(highest_confirmed_slot, 3);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 0);

        // Test higher root will be cached and clear pending_optimistically_confirmed_banks
        let bank3 = bank_forks.read().unwrap().get(3).unwrap();
        let bank4 = Bank::new_from_parent(bank3, SlotLeader::default(), 4);
        bank_forks.write().unwrap().insert(bank4);
        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::OptimisticallyConfirmed(4),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 3);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 1);
        assert!(pending_optimistically_confirmed_banks.contains(&4));
        assert_eq!(highest_confirmed_slot, 4);

        let bank4 = bank_forks.read().unwrap().get(4).unwrap();
        let bank5 = Bank::new_from_parent(bank4, SlotLeader::default(), 5);
        bank_forks.write().unwrap().insert(bank5);
        let bank5 = bank_forks.read().unwrap().get(5).unwrap();

        let mut bank_notification_senders = Vec::new();
        let (sender, receiver) = bounded(1024);
        bank_notification_senders.push(sender);

        let subscribers = Some(Arc::new(RwLock::new(bank_notification_senders)));
        let parent_roots = bank5.ancestors.keys();

        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::NewRootBank(bank5),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &subscribers,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 5);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 0);
        assert!(!pending_optimistically_confirmed_banks.contains(&4));
        assert_eq!(highest_confirmed_slot, 4);
        // The newest_root_slot is updated via NewRootedChain only
        assert_eq!(newest_root_slot, 0);

        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::NewRootedChain(parent_roots),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &subscribers,
            None,
            &None, // No dependency tracker
        );

        assert_eq!(newest_root_slot, 5);

        // Obtain the root notifications, we expect 5, including that for bank5.
        let notifications = get_root_notifications(&receiver);
        assert_eq!(notifications.len(), 5);

        // Banks <= root do not get added to pending list, even if not frozen
        let bank5 = bank_forks.read().unwrap().get(5).unwrap();
        let bank6 = Bank::new_from_parent(bank5, SlotLeader::default(), 6);
        bank_forks.write().unwrap().insert(bank6);
        let bank5 = bank_forks.read().unwrap().get(5).unwrap();
        let bank7 = Bank::new_from_parent(bank5, SlotLeader::default(), 7);
        bank_forks.write().unwrap().insert(bank7);
        bank_forks.write().unwrap().set_root(7, None, None);
        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::OptimisticallyConfirmed(6),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &None,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 5);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 0);
        assert!(!pending_optimistically_confirmed_banks.contains(&6));
        assert_eq!(highest_confirmed_slot, 4);
        assert_eq!(newest_root_slot, 5);

        let bank7 = bank_forks.read().unwrap().get(7).unwrap();
        let parent_roots = bank7.ancestors.keys();

        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::NewRootBank(bank7),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &subscribers,
            None,
            &None, // No dependency tracker
        );
        assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 7);
        assert_eq!(pending_optimistically_confirmed_banks.len(), 0);
        assert!(!pending_optimistically_confirmed_banks.contains(&6));
        assert_eq!(highest_confirmed_slot, 4);
        assert_eq!(newest_root_slot, 5);

        OptimisticallyConfirmedBankTracker::process_notification(
            (
                BankNotification::NewRootedChain(parent_roots),
                None, /* no dependency work */
            ),
            &bank_forks,
            &optimistically_confirmed_bank,
            &subscriptions,
            &mut pending_optimistically_confirmed_banks,
            &mut last_notified_confirmed_slot,
            &mut highest_confirmed_slot,
            &mut newest_root_slot,
            &subscribers,
            None,
            &None, // No dependency tracker
        );

        assert_eq!(newest_root_slot, 7);

        // Obtain the root notifications, we expect 1, which is for bank7 only as its parent bank5 is already notified.
        let notifications = get_root_notifications(&receiver);
        assert_eq!(notifications.len(), 1);
    }

    #[test]
    fn test_event_synchronization() {
        let exit = Arc::new(AtomicBool::new(false));
        let dependency_tracker: Arc<DependencyTracker> =
            Arc::new(dependency_tracker::DependencyTracker::default());
        let work_id_1 = 345;
        let work_id_2 = 678;
        let tracker_clone = dependency_tracker.clone();
        let handle = thread::spawn(move || {
            let GenesisConfigInfo { genesis_config, .. } = create_genesis_config(100);
            let bank = Bank::new_for_tests(&genesis_config);
            let bank_forks = BankForks::new_rw_arc(bank);

            // Test bank will only be cached when frozen
            let bank0 = bank_forks.read().unwrap().get(0).unwrap();
            let bank1 = Bank::new_from_parent(bank0, SlotLeader::default(), 1);
            bank_forks.write().unwrap().insert(bank1);

            let mut pending_optimistically_confirmed_banks: HashSet<u64> = HashSet::new();
            let max_complete_transaction_status_slot = Arc::new(AtomicU64::default());

            let block_commitment_cache = Arc::new(RwLock::new(BlockCommitmentCache::default()));

            let mut highest_confirmed_slot: Slot = 0;
            let mut newest_root_slot: Slot = 0;

            let mut last_notified_confirmed_slot: Slot = 0;

            let optimistically_confirmed_bank: Arc<RwLock<OptimisticallyConfirmedBank>> =
                OptimisticallyConfirmedBank::locked_from_bank_forks_root(&bank_forks);

            let subscriptions = Arc::new(RpcSubscriptions::new_for_tests(
                exit,
                max_complete_transaction_status_slot,
                bank_forks.clone(),
                block_commitment_cache,
                optimistically_confirmed_bank.clone(),
            ));

            // confirmed without fronzen received
            OptimisticallyConfirmedBankTracker::process_notification(
                (
                    BankNotification::OptimisticallyConfirmed(1),
                    Some(work_id_1), /* dependency work id */
                ),
                &bank_forks,
                &optimistically_confirmed_bank,
                &subscriptions,
                &mut pending_optimistically_confirmed_banks,
                &mut last_notified_confirmed_slot,
                &mut highest_confirmed_slot,
                &mut newest_root_slot,
                &None,
                None,
                &Some(tracker_clone.clone()),
            );

            assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 0);
            // highest_confirmed_slot is updated even when we have not received the frozen event
            assert_eq!(highest_confirmed_slot, 1);
            assert_eq!(pending_optimistically_confirmed_banks.len(), 1);

            let bank1 = bank_forks.read().unwrap().get(1).unwrap();
            bank1.freeze();

            OptimisticallyConfirmedBankTracker::process_notification(
                (
                    BankNotification::Frozen(bank1),
                    Some(work_id_2), /* dependency work id */
                ),
                &bank_forks,
                &optimistically_confirmed_bank,
                &subscriptions,
                &mut pending_optimistically_confirmed_banks,
                &mut last_notified_confirmed_slot,
                &mut highest_confirmed_slot,
                &mut newest_root_slot,
                &None,
                None,
                &Some(tracker_clone),
            );

            assert_eq!(optimistically_confirmed_bank.read().unwrap().bank.slot(), 1);
            assert_eq!(highest_confirmed_slot, 1);
            assert_eq!(pending_optimistically_confirmed_banks.len(), 0);
        });

        dependency_tracker.mark_this_and_all_previous_work_processed(work_id_1);
        dependency_tracker.mark_this_and_all_previous_work_processed(work_id_2);

        handle.join().unwrap();
    }
}
