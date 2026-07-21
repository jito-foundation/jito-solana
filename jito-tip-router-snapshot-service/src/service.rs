use {
    crate::{
        artifact_writer,
        bank_collector::{self, TipRouterSnapshotArtifacts},
        config::TipRouterSnapshotConfig,
    },
    crossbeam_channel::{
        Receiver, RecvTimeoutError, Sender, TrySendError, bounded, select, tick, unbounded,
    },
    log::{debug, error, info, warn},
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_rpc::optimistically_confirmed_bank_tracker::{
        BankNotification, BankNotificationReceiver, BankNotificationWithDependencyWork,
        RootedBankIdentity,
    },
    solana_runtime::bank::Bank,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
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
const GENERATION_QUEUE_CAPACITY: usize = 1;
const ROOTED_IDENTITY_RETENTION_EPOCHS: Epoch = 1;

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
        let (generation_task_sender, generation_task_receiver) = bounded(GENERATION_QUEUE_CAPACITY);
        let (generation_result_sender, generation_result_receiver) = unbounded();
        let generation_worker = Self::spawn_generation_worker(
            config.clone(),
            generation_task_receiver,
            generation_result_sender,
            exit.clone(),
        );
        let maintenance_tick = tick(IDLE_INTERVAL);
        let mut context = TipRouterSnapshotServiceContext::default();

        while !exit.load(Ordering::Relaxed) {
            select! {
                recv(bank_notification_receiver) -> notification => {
                    match notification {
                        Ok(notification) => Self::handle_bank_notification(
                            &config,
                            notification,
                            &generation_task_sender,
                            &mut context,
                        ),
                        Err(_) if exit.load(Ordering::Relaxed) => break,
                        Err(_) => todo!("Crash node when the bank notification channel disconnects"),
                    }
                }
                recv(generation_result_receiver) -> result => {
                    match result {
                        Ok(result) => Self::handle_generation_result(&config, result, &mut context),
                        Err(_) if exit.load(Ordering::Relaxed) => break,
                        Err(_) => todo!("Crash node when the generation worker disconnects"),
                    }
                }
                recv(maintenance_tick) -> _ => {
                    Self::dispatch_awaiting_generation(&generation_task_sender, &mut context);
                    Self::write_accepted_candidates(&config, &mut context);
                    Self::prune_candidate_cache(&config, &mut context);
                }
            }
        }

        drop(generation_task_sender);
        generation_worker
            .join()
            .expect("tip-router snapshot generation worker panicked");
    }

    fn spawn_generation_worker(
        config: TipRouterSnapshotConfig,
        generation_task_receiver: Receiver<GenerationTask>,
        generation_result_sender: Sender<GenerationResult>,
        exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        Builder::new()
            .name("tipRtSnapshotGen".to_string())
            .spawn(move || {
                while !exit.load(Ordering::Relaxed) {
                    let task = match generation_task_receiver.recv_timeout(IDLE_INTERVAL) {
                        Ok(task) => task,
                        Err(RecvTimeoutError::Timeout) => continue,
                        Err(RecvTimeoutError::Disconnected) => break,
                    };
                    // Phase 5 must establish cleanup protection for any direct AccountsDB reads
                    // performed by the collector. Retaining this Arc<Bank> alone is not that pin.
                    let artifacts = bank_collector::collect_tip_router_snapshot_artifacts(
                        &config,
                        &task.parent_bank,
                        task.boundary_child.slot,
                        task.boundary_child.bank_hash,
                        task.boundary_child.epoch,
                    );
                    if generation_result_sender
                        .send(GenerationResult {
                            candidate_identity: task.candidate_identity,
                            artifacts,
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .expect("Failed to spawn tipRtSnapshotGen thread")
    }

    fn handle_bank_notification(
        config: &TipRouterSnapshotConfig,
        (notification, _dependency_work): BankNotificationWithDependencyWork,
        generation_task_sender: &Sender<GenerationTask>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        match notification {
            BankNotification::OptimisticallyConfirmed(_) => {}
            BankNotification::Frozen(bank) => {
                Self::handle_frozen_bank(config, bank, generation_task_sender, context);
            }
            BankNotification::NewRootBank(bank) => {
                Self::handle_new_root_bank(bank, context);
            }
            BankNotification::NewRootedChain(rooted_bank_identities) => {
                Self::handle_new_rooted_chain(config, rooted_bank_identities, context);
            }
        }
    }

    fn handle_frozen_bank(
        config: &TipRouterSnapshotConfig,
        boundary_child_bank: Arc<Bank>,
        generation_task_sender: &Sender<GenerationTask>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let Some(parent_bank) = boundary_child_bank.parent() else {
            return;
        };
        if boundary_child_bank.epoch() <= parent_bank.epoch() {
            return;
        }

        let candidate_identity = CandidateBankIdentity::from_bank(&parent_bank);
        if context.written_candidates.contains(&candidate_identity)
            || context.candidates.contains_key(&candidate_identity)
        {
            return;
        }

        if let Some(rooted_hash) = context.recent_rooted_banks.get(&candidate_identity.slot) {
            if *rooted_hash != candidate_identity.bank_hash {
                debug!(
                    "dropping tip-router snapshot candidate on rooted losing fork: slot={}, \
                     bank_hash={}, rooted_bank_hash={rooted_hash}",
                    candidate_identity.slot, candidate_identity.bank_hash,
                );
                return;
            }
        }

        let boundary_child = BoundaryChildIdentity::from_bank(&boundary_child_bank);
        let root_status = context
            .recent_rooted_banks
            .get(&candidate_identity.slot)
            .is_some_and(|rooted_hash| *rooted_hash == candidate_identity.bank_hash)
            .then_some(CandidateRootStatus::Accepted)
            .unwrap_or(CandidateRootStatus::AwaitingRoot);
        debug!(
            "registering tip-router snapshot candidate: slot={}, bank_hash={}, epoch={}, \
             child_slot={}, child_hash={}, child_epoch={}",
            candidate_identity.slot,
            candidate_identity.bank_hash,
            parent_bank.epoch(),
            boundary_child.slot,
            boundary_child.bank_hash,
            boundary_child.epoch,
        );
        context.candidates.insert(
            candidate_identity,
            PendingCandidate {
                boundary_child,
                generation: CandidateGeneration::AwaitingDispatch(parent_bank),
                root_status,
                write_attempts: 0,
            },
        );
        Self::prune_candidate_cache(config, context);
        Self::dispatch_awaiting_generation(generation_task_sender, context);
    }

    fn handle_new_root_bank(root_bank: Arc<Bank>, context: &mut TipRouterSnapshotServiceContext) {
        context.highest_observed_root = context.highest_observed_root.max(root_bank.slot());
        let retained_epoch = root_bank
            .epoch()
            .saturating_sub(ROOTED_IDENTITY_RETENTION_EPOCHS);
        context.rooted_identity_retention_floor = root_bank
            .epoch_schedule()
            .get_first_slot_in_epoch(retained_epoch);
    }

    fn handle_new_rooted_chain(
        config: &TipRouterSnapshotConfig,
        rooted_bank_identities: Vec<RootedBankIdentity>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        for rooted_bank_identity in rooted_bank_identities {
            if let Some(previous_hash) = context
                .recent_rooted_banks
                .insert(rooted_bank_identity.slot, rooted_bank_identity.bank_hash)
            {
                if previous_hash != rooted_bank_identity.bank_hash {
                    warn!(
                        "rooted bank identity changed for slot={}: old_bank_hash={previous_hash}, \
                         new_bank_hash={}",
                        rooted_bank_identity.slot, rooted_bank_identity.bank_hash,
                    );
                }
            }
            Self::reconcile_candidates_at_rooted_slot(rooted_bank_identity, context);
        }

        Self::write_accepted_candidates(config, context);
        Self::prune_rooted_identity_cache(context);
        Self::prune_candidate_cache(config, context);
    }

    fn reconcile_candidates_at_rooted_slot(
        rooted_bank_identity: RootedBankIdentity,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let candidate_identities = context
            .candidates
            .keys()
            .copied()
            .filter(|candidate_identity| candidate_identity.slot == rooted_bank_identity.slot)
            .collect::<Vec<_>>();

        for candidate_identity in candidate_identities {
            if candidate_identity.bank_hash == rooted_bank_identity.bank_hash {
                let candidate = context
                    .candidates
                    .get_mut(&candidate_identity)
                    .expect("candidate disappeared while reconciling rooted identity");
                candidate.root_status = CandidateRootStatus::Accepted;
            } else {
                debug!(
                    "dropping tip-router snapshot candidate on rooted losing fork: slot={}, \
                     bank_hash={}, rooted_bank_hash={}",
                    candidate_identity.slot,
                    candidate_identity.bank_hash,
                    rooted_bank_identity.bank_hash,
                );
                context.candidates.remove(&candidate_identity);
            }
        }
    }

    fn dispatch_awaiting_generation(
        generation_task_sender: &Sender<GenerationTask>,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let candidate_identities = context
            .candidates
            .iter()
            .filter_map(|(candidate_identity, candidate)| {
                matches!(
                    candidate.generation,
                    CandidateGeneration::AwaitingDispatch(_)
                )
                .then_some(*candidate_identity)
            })
            .collect::<Vec<_>>();

        for candidate_identity in candidate_identities {
            let candidate = context
                .candidates
                .get_mut(&candidate_identity)
                .expect("candidate disappeared while dispatching generation");
            let CandidateGeneration::AwaitingDispatch(parent_bank) = &candidate.generation else {
                continue;
            };
            let generation_task = GenerationTask {
                candidate_identity,
                boundary_child: candidate.boundary_child,
                parent_bank: parent_bank.clone(),
            };
            match generation_task_sender.try_send(generation_task) {
                Ok(()) => candidate.generation = CandidateGeneration::Running,
                Err(TrySendError::Full(_)) => break,
                Err(TrySendError::Disconnected(_)) => {
                    error!(
                        "tip-router snapshot generation worker disconnected while dispatching \
                         slot={}, bank_hash={}",
                        candidate_identity.slot, candidate_identity.bank_hash,
                    );
                    break;
                }
            }
        }
    }

    fn handle_generation_result(
        config: &TipRouterSnapshotConfig,
        GenerationResult {
            candidate_identity,
            artifacts,
        }: GenerationResult,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let Some(candidate) = context.candidates.get_mut(&candidate_identity) else {
            debug!(
                "ignoring tip-router snapshot artifacts for losing candidate: slot={}, \
                 bank_hash={}",
                candidate_identity.slot, candidate_identity.bank_hash,
            );
            return;
        };
        candidate.generation = CandidateGeneration::Complete(artifacts);
        if let Some(rooted_hash) = context.recent_rooted_banks.get(&candidate_identity.slot) {
            if *rooted_hash == candidate_identity.bank_hash {
                candidate.root_status = CandidateRootStatus::Accepted;
            } else {
                debug!(
                    "dropping completed tip-router snapshot candidate on rooted losing fork: \
                     slot={}, bank_hash={}, rooted_bank_hash={rooted_hash}",
                    candidate_identity.slot, candidate_identity.bank_hash,
                );
                context.candidates.remove(&candidate_identity);
                return;
            }
        }
        Self::write_accepted_candidates(config, context);
    }

    fn write_accepted_candidates(
        config: &TipRouterSnapshotConfig,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let ready_candidate_identities = context
            .candidates
            .iter()
            .filter_map(|(candidate_identity, candidate)| {
                (candidate.root_status == CandidateRootStatus::Accepted
                    && matches!(candidate.generation, CandidateGeneration::Complete(_)))
                .then_some(*candidate_identity)
            })
            .collect::<Vec<_>>();

        for candidate_identity in ready_candidate_identities {
            let write_result = {
                let candidate = context
                    .candidates
                    .get(&candidate_identity)
                    .expect("candidate disappeared before artifact write");
                let CandidateGeneration::Complete(artifacts) = &candidate.generation else {
                    continue;
                };
                artifact_writer::write_tip_router_snapshot_artifacts(&config.output_dir, artifacts)
            };

            match write_result {
                Ok(path) => {
                    info!(
                        "wrote tip-router snapshot artifact for rooted slot={}, bank_hash={} to {}",
                        candidate_identity.slot,
                        candidate_identity.bank_hash,
                        path.display(),
                    );
                    context.written_candidates.insert(candidate_identity);
                    context.candidates.remove(&candidate_identity);
                }
                Err(err) => {
                    let candidate = context
                        .candidates
                        .get_mut(&candidate_identity)
                        .expect("candidate disappeared after artifact write");
                    candidate.write_attempts = candidate.write_attempts.saturating_add(1);
                    error!(
                        "failed to write tip-router snapshot artifact for rooted slot={}, \
                         bank_hash={} (attempt {}): {err}",
                        candidate_identity.slot,
                        candidate_identity.bank_hash,
                        candidate.write_attempts,
                    );
                    if candidate.write_attempts >= MAX_WRITE_ATTEMPTS {
                        error!(
                            "dropping accepted tip-router snapshot candidate after \
                             {MAX_WRITE_ATTEMPTS} failed writes: slot={}, bank_hash={}",
                            candidate_identity.slot, candidate_identity.bank_hash,
                        );
                        context.candidates.remove(&candidate_identity);
                    }
                }
            }
        }
    }

    fn prune_rooted_identity_cache(context: &mut TipRouterSnapshotServiceContext) {
        let retention_floor = context.rooted_identity_retention_floor;
        context
            .recent_rooted_banks
            .retain(|slot, _| *slot >= retention_floor);
    }

    fn prune_candidate_cache(
        config: &TipRouterSnapshotConfig,
        context: &mut TipRouterSnapshotServiceContext,
    ) {
        let stale_candidate_identities = context
            .candidates
            .iter()
            .filter_map(|(candidate_identity, candidate)| {
                (candidate.root_status == CandidateRootStatus::AwaitingRoot
                    && candidate_identity.slot < context.rooted_identity_retention_floor)
                    .then_some(*candidate_identity)
            })
            .collect::<Vec<_>>();
        for candidate_identity in stale_candidate_identities {
            warn!(
                "dropping tip-router snapshot candidate without exact root proof after retention \
                 window: slot={}, bank_hash={}",
                candidate_identity.slot, candidate_identity.bank_hash,
            );
            context.candidates.remove(&candidate_identity);
        }

        let Some(max_candidates) = config.max_candidates else {
            return;
        };
        let overflow = context.candidates.len().saturating_sub(max_candidates);
        if overflow == 0 {
            return;
        }

        let mut evictable_candidate_identities = context
            .candidates
            .iter()
            .filter_map(|(candidate_identity, candidate)| {
                (candidate.root_status == CandidateRootStatus::AwaitingRoot
                    && matches!(
                        candidate.generation,
                        CandidateGeneration::AwaitingDispatch(_)
                    ))
                .then_some(*candidate_identity)
            })
            .collect::<Vec<_>>();
        evictable_candidate_identities
            .sort_unstable_by_key(|candidate_identity| candidate_identity.slot);
        for candidate_identity in evictable_candidate_identities.into_iter().take(overflow) {
            warn!(
                "dropping undispatched tip-router snapshot candidate due to resource limit: \
                 slot={}, bank_hash={}",
                candidate_identity.slot, candidate_identity.bank_hash,
            );
            context.candidates.remove(&candidate_identity);
        }

        let remaining_overflow = context.candidates.len().saturating_sub(max_candidates);
        if remaining_overflow > 0 {
            warn!(
                "tip-router snapshot candidate limit exceeded by {remaining_overflow}; retaining \
                 candidates with active generation or exact root acceptance"
            );
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct CandidateBankIdentity {
    slot: Slot,
    bank_hash: Hash,
}

impl CandidateBankIdentity {
    fn from_bank(bank: &Bank) -> Self {
        Self {
            slot: bank.slot(),
            bank_hash: bank.hash(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct BoundaryChildIdentity {
    slot: Slot,
    bank_hash: Hash,
    epoch: Epoch,
}

impl BoundaryChildIdentity {
    fn from_bank(bank: &Bank) -> Self {
        Self {
            slot: bank.slot(),
            bank_hash: bank.hash(),
            epoch: bank.epoch(),
        }
    }
}

struct GenerationTask {
    candidate_identity: CandidateBankIdentity,
    boundary_child: BoundaryChildIdentity,
    parent_bank: Arc<Bank>,
}

struct GenerationResult {
    candidate_identity: CandidateBankIdentity,
    artifacts: TipRouterSnapshotArtifacts,
}

struct PendingCandidate {
    boundary_child: BoundaryChildIdentity,
    generation: CandidateGeneration,
    root_status: CandidateRootStatus,
    write_attempts: u8,
}

enum CandidateGeneration {
    AwaitingDispatch(Arc<Bank>),
    Running,
    Complete(TipRouterSnapshotArtifacts),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CandidateRootStatus {
    AwaitingRoot,
    Accepted,
}

/// State owned exclusively by the tip-router notification thread.
#[derive(Default)]
struct TipRouterSnapshotServiceContext {
    candidates: HashMap<CandidateBankIdentity, PendingCandidate>,
    recent_rooted_banks: BTreeMap<Slot, Hash>,
    written_candidates: HashSet<CandidateBankIdentity>,
    highest_observed_root: Slot,
    rooted_identity_retention_floor: Slot,
}
