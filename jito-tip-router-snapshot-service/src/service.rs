use {
    crate::{
        artifact_writer,
        bank_collector::{self, TipRouterSnapshotArtifacts},
        config::TipRouterSnapshotConfig,
    },
    crossbeam_channel::RecvTimeoutError,
    log::{debug, error, info, warn},
    solana_clock::Slot,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationReceiver, BankNotificationWithDependencyWork,
    },
    solana_runtime::bank::Bank,
    std::{
        collections::{HashMap, HashSet},
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        thread::{self, Builder, JoinHandle},
        time::Duration,
    },
};

const IDLE_INTERVAL: Duration = Duration::from_millis(500);
const MAX_WRITE_ATTEMPTS: u8 = 3;

pub struct TipRouterSnapshotService {
    thread_hdl: JoinHandle<()>,
}

impl TipRouterSnapshotService {
    pub fn new(
        config: TipRouterSnapshotConfig,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread_hdl = Builder::new()
            .name("tipRtSnapshot".to_string())
            .spawn(move || {
                info!("TipRouterSnapshotService has started");
                Self::run(config, bank_notification_receiver, exit);
                info!("TipRouterSnapshotService has stopped");
            })
            .expect("Failed to spawn tipRtSnapshot thread");

        Self { thread_hdl }
    }

    fn run(
        config: TipRouterSnapshotConfig,
        bank_notification_receiver: BankNotificationReceiver,
        exit: Arc<AtomicBool>,
    ) {
        let mut context = TipRouterSnapshotServiceContext::default();
        while !exit.load(Ordering::Relaxed) {
            match bank_notification_receiver.recv_timeout(IDLE_INTERVAL) {
                Ok(notification) => {
                    Self::handle_bank_notification(&config, notification, &mut context);
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    // On shutdown the senders drop before this thread observes
                    // the exit flag; only an unexpected disconnect is fatal.
                    if exit.load(Ordering::Relaxed) {
                        break;
                    }
                    todo!("Crash node on this error")
                }
            }
        }
    }

    fn handle_bank_notification(
        config: &TipRouterSnapshotConfig,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        match notification {
            BankNotification::OptimisticallyConfirmed(_) => {}
            BankNotification::Frozen(bank) => {
                Self::handle_frozen_bank(config, bank, context);
            }
            BankNotification::NewRootBank(bank) => {
                Self::handle_new_root_bank(config, bank, context);
            }
            BankNotification::NewRootedChain(_) => {}
        }
    }

    fn handle_frozen_bank(
        config: &TipRouterSnapshotConfig,
        bank: Arc<Bank>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let Some(parent) = bank.parent() else {
            return;
        };

        if bank.epoch() <= parent.epoch() {
            return;
        }

        let candidate_slot = parent.slot();
        if context.written_slots.contains(&candidate_slot) {
            return;
        }

        let parent_hash = parent.hash();
        if let Some(existing) = context.candidates.get(&candidate_slot) {
            if existing.artifacts.bank_hash == parent_hash {
                // Another boundary child of the same parent version; keep the
                // existing entry so rooted/write_attempts state is preserved.
                return;
            }
            if existing.rooted {
                warn!(
                    "ignoring tip-router snapshot candidate replacement for slot={candidate_slot}: \
                     existing bank_hash={} is already rooted, new bank_hash={parent_hash}",
                    existing.artifacts.bank_hash
                );
                return;
            }
            warn!(
                "replacing tip-router snapshot candidate artifacts for slot={candidate_slot}: \
                 bank_hash changed old={} new={parent_hash}",
                existing.artifacts.bank_hash
            );
        }

        debug!(
            "computing tip-router snapshot candidate artifacts: slot={candidate_slot}, \
             epoch={}, child_slot={}, child_epoch={}",
            parent.epoch(),
            bank.slot(),
            bank.epoch()
        );
        let artifacts = bank_collector::collect_tip_router_snapshot_artifacts(config, &parent);

        context.candidates.insert(
            candidate_slot,
            PendingCandidate {
                artifacts,
                rooted: false,
                write_attempts: 0,
            },
        );
        Self::prune_candidate_cache(config, context);
    }

    fn handle_new_root_bank(
        config: &TipRouterSnapshotConfig,
        root_bank: Arc<Bank>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let root_slot = root_bank.slot();

        let mut dead_fork_slots = Vec::new();
        for (&slot, candidate) in context.candidates.iter_mut() {
            if candidate.rooted || slot > root_slot {
                continue;
            }
            if root_bank.ancestors.contains_key(&slot) {
                candidate.rooted = true;
            } else {
                dead_fork_slots.push(slot);
            }
        }
        for slot in dead_fork_slots {
            debug!("dropping tip-router snapshot candidate on dead fork: slot={slot}");
            context.candidates.remove(&slot);
        }

        let rooted_slots: Vec<Slot> = context
            .candidates
            .iter()
            .filter(|(_, candidate)| candidate.rooted)
            .map(|(&slot, _)| slot)
            .collect();
        for slot in rooted_slots {
            let Some(candidate) = context.candidates.get_mut(&slot) else {
                continue;
            };
            match artifact_writer::write_tip_router_snapshot_artifacts(
                &config.output_dir,
                &candidate.artifacts,
            ) {
                Ok(path) => {
                    info!(
                        "wrote tip-router snapshot artifact for rooted slot={slot} to {}",
                        path.display()
                    );
                    context.written_slots.insert(slot);
                    context.candidates.remove(&slot);
                }
                Err(err) => {
                    candidate.write_attempts = candidate.write_attempts.saturating_add(1);
                    error!(
                        "failed to write tip-router snapshot artifact for rooted slot={slot} \
                         (attempt {}): {err}",
                        candidate.write_attempts
                    );
                    if candidate.write_attempts >= MAX_WRITE_ATTEMPTS {
                        error!(
                            "dropping tip-router snapshot candidate for slot={slot} after \
                             {MAX_WRITE_ATTEMPTS} failed write attempts"
                        );
                        context.candidates.remove(&slot);
                    }
                }
            }
        }
    }

    fn prune_candidate_cache(
        config: &TipRouterSnapshotConfig,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let Some(max_candidates) = config.max_candidates else {
            return;
        };

        let overflow = context.candidates.len().saturating_sub(max_candidates);
        if overflow == 0 {
            return;
        }

        // Rooted candidates are pending a write and must not be evicted.
        let mut candidate_slots: Vec<_> = context
            .candidates
            .iter()
            .filter(|(_, candidate)| !candidate.rooted)
            .map(|(&slot, _)| slot)
            .collect();
        candidate_slots.sort_unstable();
        for slot in candidate_slots.into_iter().take(overflow) {
            warn!("pruning tip-router snapshot candidate: slot={slot}");
            context.candidates.remove(&slot);
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

struct PendingCandidate {
    artifacts: TipRouterSnapshotArtifacts,
    rooted: bool,
    write_attempts: u8,
}

/// Used by the background thread
#[derive(Default)]
struct TipRouterSnapshotServiceContext {
    candidates: HashMap<Slot, PendingCandidate>,
    written_slots: HashSet<Slot>,
}
