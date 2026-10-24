use {
    crate::{CandidateIdentity, snapshot_worker::WorkerOutcome},
    solana_metrics::datapoint_error,
    std::fmt::Display,
};

const ERROR_METRIC: &str = "tip_router_snapshot_service-error";

enum CandidateFailureEvent {
    Publication,
    Spawn,
    Worker,
}

impl CandidateFailureEvent {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Publication => "publication_failed",
            Self::Spawn => "spawn_failed",
            Self::Worker => "worker_failed",
        }
    }
}

fn report_candidate_failure(
    event: CandidateFailureEvent,
    error: impl Display,
    candidate: CandidateIdentity,
) {
    datapoint_error!(
        ERROR_METRIC,
        ("event", event.as_str(), String),
        ("error", error.to_string(), String),
        ("epoch", candidate.epoch as i64, i64),
        ("slot", candidate.slot as i64, i64),
    );
}

pub(super) fn report_publication_failure(error: impl Display, candidate: CandidateIdentity) {
    report_candidate_failure(CandidateFailureEvent::Publication, error, candidate);
}

pub(super) fn report_spawn_failure(error: impl Display, candidate: CandidateIdentity) {
    report_candidate_failure(CandidateFailureEvent::Spawn, error, candidate);
}

pub(super) fn report_worker_outcome(outcome: &WorkerOutcome, candidate: CandidateIdentity) {
    match outcome {
        WorkerOutcome::Written(_) => {}
        WorkerOutcome::Failed(error) => {
            report_candidate_failure(CandidateFailureEvent::Worker, error, candidate);
        }
        WorkerOutcome::Panicked => {
            report_candidate_failure(CandidateFailureEvent::Worker, "worker panicked", candidate);
        }
        WorkerOutcome::MissingResult => report_candidate_failure(
            CandidateFailureEvent::Worker,
            "worker returned no result",
            candidate,
        ),
    }
}

pub(super) fn report_fatal_exit(error: impl Display) {
    datapoint_error!(
        ERROR_METRIC,
        ("event", "fatal_exit", String),
        ("error", error.to_string(), String),
    );
}
