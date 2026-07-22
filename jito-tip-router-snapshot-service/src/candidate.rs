use {
    crate::bank_collector::TipRouterSnapshotArtifacts,
    solana_clock::{Epoch, Slot},
    solana_hash::Hash,
    solana_runtime::bank::Bank,
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        sync::Arc,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CandidateBankIdentity {
    pub(crate) slot: Slot,
    pub(crate) bank_hash: Hash,
}

impl CandidateBankIdentity {
    pub(crate) fn from_bank(bank: &Bank) -> Self {
        Self {
            slot: bank.slot(),
            bank_hash: bank.hash(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct BoundaryChildIdentity {
    pub(crate) slot: Slot,
    pub(crate) bank_hash: Hash,
    pub(crate) epoch: Epoch,
}

impl BoundaryChildIdentity {
    pub(crate) fn from_bank(bank: &Bank) -> Self {
        Self {
            slot: bank.slot(),
            bank_hash: bank.hash(),
            epoch: bank.epoch(),
        }
    }
}

pub(crate) struct GenerationTask {
    pub(crate) candidate_identity: CandidateBankIdentity,
    pub(crate) boundary_child: BoundaryChildIdentity,
    pub(crate) parent_bank: Arc<Bank>,
}

pub(crate) struct GenerationResult {
    pub(crate) candidate_identity: CandidateBankIdentity,
    pub(crate) artifacts: TipRouterSnapshotArtifacts,
}

pub(crate) struct CandidateRegistration {
    pub(crate) identity: CandidateBankIdentity,
    pub(crate) boundary_child: BoundaryChildIdentity,
    pub(crate) parent_bank: Arc<Bank>,
    pub(crate) root_status: CandidateRootStatus,
}

pub(crate) struct PendingCandidate {
    pub(crate) boundary_child: BoundaryChildIdentity,
    pub(crate) generation: CandidateGeneration,
    pub(crate) root_status: CandidateRootStatus,
    pub(crate) write_attempts: u8,
}

pub(crate) enum CandidateGeneration {
    AwaitingDispatch(Arc<Bank>),
    Running,
    Complete(TipRouterSnapshotArtifacts),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CandidateRootStatus {
    AwaitingRoot,
    Accepted,
}

/// State owned exclusively by the tip-router notification thread.
#[derive(Default)]
pub(crate) struct TipRouterSnapshotServiceContext {
    pub(crate) candidates: HashMap<CandidateBankIdentity, PendingCandidate>,
    pub(crate) recent_rooted_banks: BTreeMap<Slot, Hash>,
    pub(crate) written_candidates: HashSet<CandidateBankIdentity>,
    pub(crate) highest_observed_root: Slot,
    pub(crate) rooted_identity_retention_floor: Slot,
}
