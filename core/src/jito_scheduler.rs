//! Coordination between the external scheduler and validator services.
use {
    crate::{bam_dependencies::GenerationBoundBamBatch, packet_bundle::VerifiedPacketBundle},
    crossbeam_channel::{Receiver, Sender},
    std::sync::{
        Arc, RwLock,
        atomic::{AtomicBool, AtomicU64},
    },
};

pub struct JitoSchedulerControl {
    /// Request exclusive access to the verified legacy bundle receiver.
    pub active: AtomicBool,
    /// BundleStage acknowledges only after completing its current execution.
    pub bundle_stage_paused: AtomicBool,
    /// Mode transitions and unknown execution outcomes require a fresh BAM stream.
    pub reconnect_bam: AtomicBool,
    /// Increases before each new BAM authentication attempt.
    pub bam_generation: AtomicU64,
    /// Old-generation execution must finish before a new stream can become active.
    pub bam_generation_lock: RwLock<()>,
    /// Ingress carries the immutable generation of its originating connection.
    pub bam_batches: (
        Sender<GenerationBoundBamBatch>,
        Receiver<GenerationBoundBamBatch>,
    ),
}

impl Default for JitoSchedulerControl {
    fn default() -> Self {
        Self {
            active: AtomicBool::new(false),
            bundle_stage_paused: AtomicBool::new(false),
            reconnect_bam: AtomicBool::new(false),
            bam_generation: AtomicU64::new(0),
            bam_generation_lock: RwLock::new(()),
            bam_batches: crossbeam_channel::bounded(100_000),
        }
    }
}

#[derive(Clone)]
pub struct JitoBindingsDependencies {
    pub bundles: Receiver<VerifiedPacketBundle>,
    pub control: Arc<JitoSchedulerControl>,
}

#[cfg(unix)]
pub(crate) mod bridge;
