use solana_perf::packet::PacketBatch;

#[derive(Clone, Debug)]
pub struct Bundle {
    pub batch: PacketBatch,
}
