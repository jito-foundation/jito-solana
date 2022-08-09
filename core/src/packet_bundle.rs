use {solana_perf::packet::PacketBatch, uuid::Uuid};

#[derive(Clone, Debug)]
pub struct PacketBundle {
    pub batch: PacketBatch,
    pub uuid: Uuid,
}
