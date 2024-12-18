use solana_perf::packet::PacketBatch;

#[derive(Clone, Debug)]
pub struct PacketBundle {
    pub batch: PacketBatch,
    pub bundle_id: String,
}
