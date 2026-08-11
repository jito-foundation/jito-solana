use {
    crate::{
        candidate::CandidateIdentity,
        candidate_store::CandidateStore,
        config::TipRouterSnapshotConfig,
        snapshot_worker::{SnapshotWorkerHandle, WorkerCompletion},
    },
    crossbeam_channel::{Receiver, RecvTimeoutError, Sender},
    log::warn,
    solana_runtime::bank::Bank,
    std::{
        collections::HashMap,
        io,
        sync::Arc,
        time::{Duration, Instant},
    },
};

pub(super) struct WorkerShutdownTimeout {
    pub(super) worker_count: usize,
    pub(super) timeout: Duration,
    pub(super) completions: Vec<WorkerCompletion>,
}

pub(super) struct SnapshotWorkerPool {
    workers: HashMap<CandidateIdentity, SnapshotWorkerHandle>,
    completion_sender: Sender<WorkerCompletion>,
}

impl SnapshotWorkerPool {
    pub(super) fn new(completion_sender: Sender<WorkerCompletion>) -> Self {
        Self {
            workers: HashMap::new(),
            completion_sender,
        }
    }

    pub(super) fn spawn(
        &mut self,
        config: TipRouterSnapshotConfig,
        candidate_store: CandidateStore,
        candidate: CandidateIdentity,
        parent_bank: Arc<Bank>,
    ) -> io::Result<()> {
        let worker = SnapshotWorkerHandle::spawn(
            config,
            candidate_store,
            candidate,
            parent_bank,
            self.completion_sender.clone(),
        )?;
        self.workers.insert(candidate, worker);
        Ok(())
    }

    pub(super) fn complete_worker(
        &mut self,
        completion: WorkerCompletion,
    ) -> Option<WorkerCompletion> {
        let candidate = completion.candidate;
        let Some(worker) = self.workers.remove(&candidate) else {
            warn!("received duplicate or unknown worker completion for {candidate:?}");
            return None;
        };
        Some(worker.join_after_completion(completion))
    }

    pub(super) fn shutdown_with_timeout(
        &mut self,
        completion_receiver: &Receiver<WorkerCompletion>,
        timeout: Duration,
    ) -> Result<Vec<WorkerCompletion>, WorkerShutdownTimeout> {
        let deadline = Instant::now() + timeout;
        let mut completions = Vec::with_capacity(self.workers.len());

        while !self.workers.is_empty() {
            let now = Instant::now();
            if now >= deadline {
                return Err(WorkerShutdownTimeout {
                    worker_count: self.workers.len(),
                    timeout,
                    completions,
                });
            }

            match completion_receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                Ok(worker_completion) => {
                    if let Some(completion) = self.complete_worker(worker_completion) {
                        completions.push(completion);
                    }
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    let finished = self
                        .workers
                        .iter()
                        .filter_map(|(candidate, worker)| {
                            worker.is_finished().then_some(*candidate)
                        })
                        .collect::<Vec<_>>();
                    for candidate in finished {
                        if let Some(worker) = self.workers.remove(&candidate) {
                            completions.push(worker.join_without_report());
                        }
                    }
                }
            }
        }

        Ok(completions)
    }

    #[cfg(test)]
    pub(super) fn spawn_test_worker(&mut self, candidate: CandidateIdentity, duration: Duration) {
        let worker = SnapshotWorkerHandle::spawn_test_worker(
            candidate,
            duration,
            self.completion_sender.clone(),
        );
        self.workers.insert(candidate, worker);
    }
}
