use solana_perf::packet::PacketBatch;

#[derive(Clone, Debug)]
pub struct PacketBundle {
    batch: PacketBatch,
    #[allow(unused)]
    bundle_id: String,
}

impl PacketBundle {
    pub fn new(batch: PacketBatch, bundle_id: String) -> Self {
        Self { batch, bundle_id }
    }

    pub fn batch(&self) -> &PacketBatch {
        &self.batch
    }

    pub fn take(self) -> PacketBatch {
        self.batch
    }
}

#[derive(Clone, Debug)]
pub struct VerifiedPacketBundle {
    batch: PacketBatch,
}

impl VerifiedPacketBundle {
    pub fn new(batch: PacketBatch) -> Self {
        Self { batch }
    }

    pub fn take(self) -> PacketBatch {
        self.batch
    }

    pub fn batch(&self) -> &PacketBatch {
        &self.batch
    }
}
