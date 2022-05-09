use {
    min_max_heap::MinMaxHeap,
    solana_perf::packet::{limited_deserialize, Packet, PacketBatch},
    solana_runtime::bank::Bank,
    solana_sdk::{
        hash::Hash,
        message::{Message, VersionedMessage},
        short_vec::decode_shortu16_len,
        signature::Signature,
        transaction::{Transaction, VersionedTransaction},
    },
    std::{
        cmp::Ordering,
        collections::{hash_map::Entry, HashMap},
        mem::size_of,
        rc::Rc,
        sync::Arc,
    },
    thiserror::Error,
};

#[derive(Debug, Error)]
pub enum DeserializedPacketError {
    #[error("ShortVec Failed to Deserialize")]
    // short_vec::decode_shortu16_len() currently returns () on error
    ShortVecError(()),
    #[error("Deserialization Error: {0}")]
    DeserializationError(#[from] bincode::Error),
    #[error("overflowed on signature size {0}")]
    SignatureOverflowed(usize),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct ImmutableDeserializedPacket {
    original_packet: Packet,
    versioned_transaction: VersionedTransaction,
    message_hash: Hash,
    is_simple_vote: bool,
    fee_per_cu: u64,
}

impl ImmutableDeserializedPacket {
    pub fn original_packet(&self) -> &Packet {
        &self.original_packet
    }

    pub fn versioned_transaction(&self) -> &VersionedTransaction {
        &self.versioned_transaction
    }

    pub fn sender_stake(&self) -> u64 {
        self.original_packet.meta.sender_stake
    }

    pub fn message_hash(&self) -> &Hash {
        &self.message_hash
    }

    pub fn is_simple_vote(&self) -> bool {
        self.is_simple_vote
    }

    pub fn fee_per_cu(&self) -> u64 {
        self.fee_per_cu
    }
}

/// Holds deserialized messages, as well as computed message_hash and other things needed to create
/// SanitizedTransaction
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DeserializedPacket {
    immutable_section: Rc<ImmutableDeserializedPacket>,
    pub forwarded: bool,
}

impl DeserializedPacket {
    pub fn new(packet: Packet, bank: Option<&Arc<Bank>>) -> Result<Self, DeserializedPacketError> {
        Self::new_internal(packet, bank, None)
    }

    #[cfg(test)]
    fn new_with_fee_per_cu(
        packet: Packet,
        fee_per_cu: u64,
    ) -> Result<Self, DeserializedPacketError> {
        Self::new_internal(packet, None, Some(fee_per_cu))
    }

    pub fn new_internal(
        packet: Packet,
        bank: Option<&Arc<Bank>>,
        fee_per_cu: Option<u64>,
    ) -> Result<Self, DeserializedPacketError> {
        let versioned_transaction: VersionedTransaction =
            limited_deserialize(&packet.data[0..packet.meta.size])?;
        let message_bytes = packet_message(&packet)?;
        let message_hash = Message::hash_raw_message(message_bytes);
        let is_simple_vote = packet.meta.is_simple_vote_tx();

        let fee_per_cu = fee_per_cu.unwrap_or_else(|| {
            bank.as_ref()
                .map(|bank| compute_fee_per_cu(&versioned_transaction.message, bank))
                .unwrap_or(0)
        });
        Ok(Self {
            immutable_section: Rc::new(ImmutableDeserializedPacket {
                original_packet: packet,
                versioned_transaction,
                message_hash,
                is_simple_vote,
                fee_per_cu,
            }),
            forwarded: false,
        })
    }

    pub fn immutable_section(&self) -> &Rc<ImmutableDeserializedPacket> {
        &self.immutable_section
    }
}

impl PartialOrd for DeserializedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DeserializedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        match self
            .immutable_section()
            .fee_per_cu()
            .cmp(&other.immutable_section().fee_per_cu())
        {
            Ordering::Equal => self
                .immutable_section()
                .sender_stake()
                .cmp(&other.immutable_section().sender_stake()),
            ordering => ordering,
        }
    }
}

impl PartialOrd for ImmutableDeserializedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ImmutableDeserializedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.fee_per_cu().cmp(&other.fee_per_cu()) {
            Ordering::Equal => self.sender_stake().cmp(&other.sender_stake()),
            ordering => ordering,
        }
    }
}

/// Currently each banking_stage thread has a `UnprocessedPacketBatches` buffer to store
/// PacketBatch's received from sigverify. Banking thread continuously scans the buffer
/// to pick proper packets to add to the block.
#[derive(Default)]
pub struct UnprocessedPacketBatches {
    pub packet_priority_queue: MinMaxHeap<Rc<ImmutableDeserializedPacket>>,
    pub message_hash_to_transaction: HashMap<Hash, DeserializedPacket>,
    batch_limit: usize,
}

impl UnprocessedPacketBatches {
    pub fn from_iter<I: IntoIterator<Item = DeserializedPacket>>(iter: I, capacity: usize) -> Self {
        let mut unprocessed_packet_batches = Self::with_capacity(capacity);
        for deserialized_packet in iter.into_iter() {
            unprocessed_packet_batches.push(deserialized_packet);
        }

        unprocessed_packet_batches
    }

    pub fn with_capacity(capacity: usize) -> Self {
        UnprocessedPacketBatches {
            packet_priority_queue: MinMaxHeap::with_capacity(capacity),
            message_hash_to_transaction: HashMap::with_capacity(capacity),
            batch_limit: capacity,
        }
    }

    pub fn clear(&mut self) {
        self.packet_priority_queue.clear();
        self.message_hash_to_transaction.clear();
    }

    /// Insert new `deserizlized_packet_batch` into inner `MinMaxHeap<DeserializedPacket>`,
    /// weighted first by the fee-per-cu, then the stake of the sender.
    /// If buffer is at the max limit, the lowest weighted packet is dropped
    ///
    /// Returns tuple of number of packets dropped
    pub fn insert_batch(
        &mut self,
        deserialized_packets: impl Iterator<Item = DeserializedPacket>,
    ) -> usize {
        let mut num_dropped_packets = 0;
        for deserialized_packet in deserialized_packets {
            if self.push(deserialized_packet).is_some() {
                num_dropped_packets += 1;
            }
        }
        num_dropped_packets
    }

    pub fn push(&mut self, deserialized_packet: DeserializedPacket) -> Option<DeserializedPacket> {
        if self
            .message_hash_to_transaction
            .contains_key(deserialized_packet.immutable_section().message_hash())
        {
            return None;
        }

        if self.len() == self.batch_limit {
            // Optimized to not allocate by calling `MinMaxHeap::push_pop_min()`
            Some(self.push_pop_min(deserialized_packet))
        } else {
            self.push_internal(deserialized_packet);
            None
        }
    }

    pub fn iter(&mut self) -> impl Iterator<Item = &DeserializedPacket> {
        self.message_hash_to_transaction.values()
    }

    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut DeserializedPacket> {
        self.message_hash_to_transaction.iter_mut().map(|(_k, v)| v)
    }

    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut DeserializedPacket) -> bool,
    {
        // TODO: optimize this only when number of packets
        // with oudated blockhash is high
        let new_packet_priority_queue: MinMaxHeap<Rc<ImmutableDeserializedPacket>> = self
            .packet_priority_queue
            .drain()
            .filter(|immutable_packet| {
                match self
                    .message_hash_to_transaction
                    .entry(*immutable_packet.message_hash())
                {
                    Entry::Vacant(_vacant_entry) => {
                        panic!(
                            "entry {} must exist to be consistent with `packet_priority_queue`",
                            immutable_packet.message_hash()
                        );
                    }
                    Entry::Occupied(mut occupied_entry) => {
                        let should_retain = f(occupied_entry.get_mut());
                        if !should_retain {
                            occupied_entry.remove_entry();
                        }
                        should_retain
                    }
                }
            })
            .collect();
        self.packet_priority_queue = new_packet_priority_queue;
    }

    pub fn len(&self) -> usize {
        self.packet_priority_queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.packet_priority_queue.is_empty()
    }

    fn push_internal(&mut self, deserialized_packet: DeserializedPacket) {
        // Push into the priority queue
        self.packet_priority_queue
            .push(deserialized_packet.immutable_section().clone());

        // Keep track of the original packet in the tracking hashmap
        self.message_hash_to_transaction.insert(
            *deserialized_packet.immutable_section().message_hash(),
            deserialized_packet,
        );
    }

    /// Returns the popped minimum packet from the priority queue.
    fn push_pop_min(&mut self, deserialized_packet: DeserializedPacket) -> DeserializedPacket {
        let immutable_packet = deserialized_packet.immutable_section().clone();

        // Push into the priority queue
        let popped_immutable_packet = self.packet_priority_queue.push_pop_min(immutable_packet);

        if popped_immutable_packet.message_hash()
            != deserialized_packet.immutable_section().message_hash()
        {
            // Remove the popped entry from the tracking hashmap. Unwrap call is safe
            // because the priority queue and hashmap are kept consistent at all times.
            let removed_min = self
                .message_hash_to_transaction
                .remove(popped_immutable_packet.message_hash())
                .unwrap();

            // Keep track of the original packet in the tracking hashmap
            self.message_hash_to_transaction.insert(
                *deserialized_packet.immutable_section().message_hash(),
                deserialized_packet,
            );
            removed_min
        } else {
            deserialized_packet
        }
    }

    pub fn pop_max(&mut self) -> Option<DeserializedPacket> {
        self.packet_priority_queue
            .pop_max()
            .map(|immutable_packet| {
                self.message_hash_to_transaction
                    .remove(immutable_packet.message_hash())
                    .unwrap()
            })
    }

    /// Pop up to the next `n` highest priority transactions from the queue.
    /// Returns `None` if the queue is empty
    pub fn pop_max_n(&mut self, n: usize) -> Option<Vec<DeserializedPacket>> {
        let current_len = self.len();
        if self.is_empty() {
            None
        } else {
            let num_to_pop = std::cmp::min(current_len, n);
            Some(
                std::iter::from_fn(|| Some(self.pop_max().unwrap()))
                    .take(num_to_pop)
                    .collect::<Vec<DeserializedPacket>>(),
            )
        }
    }

    pub fn capacity(&self) -> usize {
        self.packet_priority_queue.capacity()
    }
}

pub fn deserialize_packets<'a>(
    packet_batch: &'a PacketBatch,
    packet_indexes: &'a [usize],
    bank: Option<&'a Arc<Bank>>,
) -> impl Iterator<Item = DeserializedPacket> + 'a {
    packet_indexes.iter().filter_map(move |packet_index| {
        DeserializedPacket::new(packet_batch.packets[*packet_index].clone(), bank).ok()
    })
}

/// Read the transaction message from packet data
pub fn packet_message(packet: &Packet) -> Result<&[u8], DeserializedPacketError> {
    let (sig_len, sig_size) =
        decode_shortu16_len(&packet.data).map_err(DeserializedPacketError::ShortVecError)?;
    sig_len
        .checked_mul(size_of::<Signature>())
        .and_then(|v| v.checked_add(sig_size))
        .map(|msg_start| {
            let msg_end = packet.meta.size;
            &packet.data[msg_start..msg_end]
        })
        .ok_or(DeserializedPacketError::SignatureOverflowed(sig_size))
}

/// Computes `(addition_fee + base_fee / requested_cu)` for `deserialized_packet`
fn compute_fee_per_cu(_message: &VersionedMessage, _bank: &Bank) -> u64 {
    1
}

pub fn transactions_to_deserialized_packets(
    transactions: &[Transaction],
) -> Result<Vec<DeserializedPacket>, DeserializedPacketError> {
    transactions
        .iter()
        .map(|transaction| {
            let packet = Packet::from_data(None, transaction)?;
            DeserializedPacket::new(packet, None)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        solana_sdk::{signature::Keypair, system_transaction},
        std::net::IpAddr,
    };

    fn packet_with_sender_stake(sender_stake: u64, ip: Option<IpAddr>) -> DeserializedPacket {
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            1,
            Hash::new_unique(),
        );
        let mut packet = Packet::from_data(None, &tx).unwrap();
        packet.meta.sender_stake = sender_stake;
        if let Some(ip) = ip {
            packet.meta.addr = ip;
        }
        DeserializedPacket::new(packet, None).unwrap()
    }

    fn packet_with_fee_per_cu(fee_per_cu: u64) -> DeserializedPacket {
        let tx = system_transaction::transfer(
            &Keypair::new(),
            &solana_sdk::pubkey::new_rand(),
            1,
            Hash::new_unique(),
        );
        let packet = Packet::from_data(None, &tx).unwrap();
        DeserializedPacket::new_with_fee_per_cu(packet, fee_per_cu).unwrap()
    }

    #[test]
    fn test_unprocessed_packet_batches_insert_pop_same_packet() {
        let packet = packet_with_sender_stake(1, None);
        let mut unprocessed_packet_batches = UnprocessedPacketBatches::with_capacity(2);
        unprocessed_packet_batches.push(packet.clone());
        unprocessed_packet_batches.push(packet.clone());

        // There was only one unique packet, so that one should be the
        // only packet returned
        assert_eq!(
            unprocessed_packet_batches.pop_max_n(2).unwrap(),
            vec![packet]
        );
    }

    #[test]
    fn test_unprocessed_packet_batches_insert_minimum_packet_over_capacity() {
        let heavier_packet_weight = 2;
        let heavier_packet = packet_with_fee_per_cu(heavier_packet_weight);

        let lesser_packet_weight = heavier_packet_weight - 1;
        let lesser_packet = packet_with_fee_per_cu(lesser_packet_weight);

        // Test that the heavier packet is actually heavier
        let mut unprocessed_packet_batches = UnprocessedPacketBatches::with_capacity(2);
        unprocessed_packet_batches.push(heavier_packet.clone());
        unprocessed_packet_batches.push(lesser_packet.clone());
        assert_eq!(
            unprocessed_packet_batches.pop_max().unwrap(),
            heavier_packet
        );

        let mut unprocessed_packet_batches = UnprocessedPacketBatches::with_capacity(1);
        unprocessed_packet_batches.push(heavier_packet);

        // Buffer is now at capacity, pushing the smaller weighted
        // packet should immediately pop it
        assert_eq!(
            unprocessed_packet_batches
                .push(lesser_packet.clone())
                .unwrap(),
            lesser_packet
        );
    }

    #[test]
    fn test_unprocessed_packet_batches_pop_max_n() {
        let num_packets = 10;
        let packets_iter =
            std::iter::repeat_with(|| packet_with_sender_stake(1, None)).take(num_packets);
        let mut unprocessed_packet_batches =
            UnprocessedPacketBatches::from_iter(packets_iter.clone(), num_packets);

        // Test with small step size
        let step_size = 1;
        for _ in 0..num_packets {
            assert_eq!(
                unprocessed_packet_batches
                    .pop_max_n(step_size)
                    .unwrap()
                    .len(),
                step_size
            );
        }

        assert!(unprocessed_packet_batches.is_empty());
        assert!(unprocessed_packet_batches.pop_max_n(0).is_none());
        assert!(unprocessed_packet_batches.pop_max_n(1).is_none());

        // Test with step size larger than `num_packets`
        let step_size = num_packets + 1;
        let mut unprocessed_packet_batches =
            UnprocessedPacketBatches::from_iter(packets_iter.clone(), num_packets);
        assert_eq!(
            unprocessed_packet_batches
                .pop_max_n(step_size)
                .unwrap()
                .len(),
            num_packets
        );
        assert!(unprocessed_packet_batches.is_empty());
        assert!(unprocessed_packet_batches.pop_max_n(0).is_none());

        // Test with step size equal to `num_packets`
        let step_size = num_packets;
        let mut unprocessed_packet_batches =
            UnprocessedPacketBatches::from_iter(packets_iter, num_packets);
        assert_eq!(
            unprocessed_packet_batches
                .pop_max_n(step_size)
                .unwrap()
                .len(),
            step_size
        );
        assert!(unprocessed_packet_batches.is_empty());
        assert!(unprocessed_packet_batches.pop_max_n(0).is_none());
    }
}
