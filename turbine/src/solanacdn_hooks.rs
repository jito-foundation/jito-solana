use bytes::Bytes;
use std::sync::{Arc, OnceLock};

/// Hook for broadcasting leader-produced TVU shreds to an external system (e.g. SolanaCDN).
///
/// This lives in `solana-turbine` to avoid dependency cycles: `solana-core` can register an
/// implementation that forwards to its SolanaCDN client, and turbine can call it on the hot path
/// without depending on `solana-core`.
pub trait LeaderTvuShredPublisher: Send + Sync + 'static {
    fn publish_tvu_shred(&self, payload: Bytes);
}

static LEADER_TVU_SHRED_PUBLISHER: OnceLock<Arc<dyn LeaderTvuShredPublisher>> = OnceLock::new();

pub fn set_leader_tvu_shred_publisher(publisher: Arc<dyn LeaderTvuShredPublisher>) -> bool {
    LEADER_TVU_SHRED_PUBLISHER.set(publisher).is_ok()
}

pub fn try_publish_leader_tvu_shred(payload: Bytes) {
    if let Some(publisher) = LEADER_TVU_SHRED_PUBLISHER.get() {
        publisher.publish_tvu_shred(payload);
    }
}

