use {
    crate::shred::{Shred, ShredType},
    serde::{Deserialize, Deserializer, Serialize, Serializer},
    solana_sdk::{clock::Slot, hash::Hash},
    std::{
        collections::BTreeSet,
        ops::{Range, RangeBounds},
    },
};

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
// The Meta column family
pub struct SlotMeta {
    // The number of slots above the root (the genesis block). The first
    // slot has slot 0.
    pub slot: Slot,
    // The total number of consecutive shreds starting from index 0
    // we have received for this slot.
    pub consumed: u64,
    // The index *plus one* of the highest shred received for this slot.  Useful
    // for checking if the slot has received any shreds yet, and to calculate the
    // range where there is one or more holes: `(consumed..received)`.
    pub received: u64,
    // The timestamp of the first time a shred was added for this slot
    pub first_shred_timestamp: u64,
    // The index of the shred that is flagged as the last shred for this slot.
    // None until the shred with LAST_SHRED_IN_SLOT flag is received.
    #[serde(with = "serde_compat")]
    pub last_index: Option<u64>,
    // The slot height of the block this one derives from.
    // The parent slot of the head of a detached chain of slots is None.
    #[serde(with = "serde_compat")]
    pub parent_slot: Option<Slot>,
    // The list of slots, each of which contains a block that derives
    // from this one.
    pub next_slots: Vec<Slot>,
    // True if this slot is full (consumed == last_index + 1) and if every
    // slot that is a parent of this slot is also connected.
    pub is_connected: bool,
    // Shreds indices which are marked data complete.
    pub completed_data_indexes: BTreeSet<u32>,
}

// Serde implementation of serialize and deserialize for Option<u64>
// where None is represented as u64::MAX; for backward compatibility.
mod serde_compat {
    use super::*;

    pub(super) fn serialize<S>(val: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        val.unwrap_or(u64::MAX).serialize(serializer)
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let val = u64::deserialize(deserializer)?;
        Ok((val != u64::MAX).then(|| val))
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
/// Index recording presence/absence of shreds
pub struct Index {
    pub slot: Slot,
    data: ShredIndex,
    coding: ShredIndex,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ShredIndex {
    /// Map representing presence/absence of shreds
    index: BTreeSet<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
/// Erasure coding information
pub struct ErasureMeta {
    /// Which erasure set in the slot this is
    set_index: u64,
    /// First coding index in the FEC set
    first_coding_index: u64,
    /// Size of shards in this erasure set
    #[serde(rename = "size")]
    __unused_size: usize,
    /// Erasure configuration for this erasure set
    config: ErasureConfig,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct ErasureConfig {
    num_data: usize,
    num_coding: usize,
}

#[derive(Deserialize, Serialize)]
pub struct DuplicateSlotProof {
    #[serde(with = "serde_bytes")]
    pub shred1: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub shred2: Vec<u8>,
}

#[derive(Debug, PartialEq)]
pub enum ErasureMetaStatus {
    CanRecover,
    DataFull,
    StillNeed(usize),
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub enum FrozenHashVersioned {
    Current(FrozenHashStatus),
}

impl FrozenHashVersioned {
    pub fn frozen_hash(&self) -> Hash {
        match self {
            FrozenHashVersioned::Current(frozen_hash_status) => frozen_hash_status.frozen_hash,
        }
    }

    pub fn is_duplicate_confirmed(&self) -> bool {
        match self {
            FrozenHashVersioned::Current(frozen_hash_status) => {
                frozen_hash_status.is_duplicate_confirmed
            }
        }
    }
}

#[derive(Deserialize, Serialize, Debug, PartialEq)]
pub struct FrozenHashStatus {
    pub frozen_hash: Hash,
    pub is_duplicate_confirmed: bool,
}

impl Index {
    pub(crate) fn new(slot: Slot) -> Self {
        Index {
            slot,
            data: ShredIndex::default(),
            coding: ShredIndex::default(),
        }
    }

    pub fn data(&self) -> &ShredIndex {
        &self.data
    }
    pub fn coding(&self) -> &ShredIndex {
        &self.coding
    }

    pub(crate) fn data_mut(&mut self) -> &mut ShredIndex {
        &mut self.data
    }
    pub(crate) fn coding_mut(&mut self) -> &mut ShredIndex {
        &mut self.coding
    }
}

impl ShredIndex {
    pub fn num_shreds(&self) -> usize {
        self.index.len()
    }

    pub(crate) fn range<R>(&self, bounds: R) -> impl Iterator<Item = &u64>
    where
        R: RangeBounds<u64>,
    {
        self.index.range(bounds)
    }

    pub(crate) fn contains(&self, index: u64) -> bool {
        self.index.contains(&index)
    }

    pub(crate) fn insert(&mut self, index: u64) {
        self.index.insert(index);
    }
}

impl SlotMeta {
    pub fn is_full(&self) -> bool {
        // last_index is None when it has no information about how
        // many shreds will fill this slot.
        // Note: A full slot with zero shreds is not possible.
        // Should never happen
        if self
            .last_index
            .map(|ix| self.consumed > ix + 1)
            .unwrap_or_default()
        {
            datapoint_error!(
                "blockstore_error",
                (
                    "error",
                    format!(
                        "Observed a slot meta with consumed: {} > meta.last_index + 1: {:?}",
                        self.consumed,
                        self.last_index.map(|ix| ix + 1),
                    ),
                    String
                )
            );
        }

        Some(self.consumed) == self.last_index.map(|ix| ix + 1)
    }

    pub fn clear_unconfirmed_slot(&mut self) {
        let mut new_self = SlotMeta::new_orphan(self.slot);
        std::mem::swap(&mut new_self.next_slots, &mut self.next_slots);
        std::mem::swap(self, &mut new_self);
    }

    pub(crate) fn new(slot: Slot, parent_slot: Option<Slot>) -> Self {
        SlotMeta {
            slot,
            parent_slot,
            is_connected: slot == 0,
            ..SlotMeta::default()
        }
    }

    pub(crate) fn new_orphan(slot: Slot) -> Self {
        Self::new(slot, /*parent_slot:*/ None)
    }
}

impl ErasureMeta {
    pub(crate) fn from_coding_shred(shred: &Shred) -> Option<Self> {
        match shred.shred_type() {
            ShredType::Data => None,
            ShredType::Code => {
                let config = ErasureConfig {
                    num_data: usize::from(shred.num_data_shreds().ok()?),
                    num_coding: usize::from(shred.num_coding_shreds().ok()?),
                };
                let first_coding_index = u64::from(shred.first_coding_index()?);
                let erasure_meta = ErasureMeta {
                    set_index: u64::from(shred.fec_set_index()),
                    config,
                    first_coding_index,
                    __unused_size: 0,
                };
                Some(erasure_meta)
            }
        }
    }

    // Returns true if the erasure fields on the shred
    // are consistent with the erasure-meta.
    pub(crate) fn check_coding_shred(&self, shred: &Shred) -> bool {
        let mut other = match Self::from_coding_shred(shred) {
            Some(erasure_meta) => erasure_meta,
            None => return false,
        };
        other.__unused_size = self.__unused_size;
        self == &other
    }

    pub(crate) fn config(&self) -> ErasureConfig {
        self.config
    }

    pub(crate) fn data_shreds_indices(&self) -> Range<u64> {
        let num_data = self.config.num_data as u64;
        self.set_index..self.set_index + num_data
    }

    pub(crate) fn coding_shreds_indices(&self) -> Range<u64> {
        let num_coding = self.config.num_coding as u64;
        self.first_coding_index..self.first_coding_index + num_coding
    }

    pub(crate) fn status(&self, index: &Index) -> ErasureMetaStatus {
        use ErasureMetaStatus::*;

        let num_coding = index.coding().range(self.coding_shreds_indices()).count();
        let num_data = index.data().range(self.data_shreds_indices()).count();

        let (data_missing, num_needed) = (
            self.config.num_data.saturating_sub(num_data),
            self.config.num_data.saturating_sub(num_data + num_coding),
        );

        if data_missing == 0 {
            DataFull
        } else if num_needed == 0 {
            CanRecover
        } else {
            StillNeed(num_needed)
        }
    }
}

impl DuplicateSlotProof {
    pub(crate) fn new(shred1: Vec<u8>, shred2: Vec<u8>) -> Self {
        DuplicateSlotProof { shred1, shred2 }
    }
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TransactionStatusIndexMeta {
    pub max_slot: Slot,
    pub frozen: bool,
}

#[derive(Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct AddressSignatureMeta {
    pub writeable: bool,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct PerfSample {
    pub num_transactions: u64,
    pub num_slots: u64,
    pub sample_period_secs: u16,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ProgramCost {
    pub cost: u64,
}

#[cfg(test)]
mod test {
    use {
        super::*,
        rand::{seq::SliceRandom, thread_rng},
    };

    #[test]
    fn test_erasure_meta_status() {
        use ErasureMetaStatus::*;

        let set_index = 0;
        let erasure_config = ErasureConfig {
            num_data: 8,
            num_coding: 16,
        };
        let e_meta = ErasureMeta {
            set_index,
            first_coding_index: set_index,
            config: erasure_config,
            __unused_size: 0,
        };
        let mut rng = thread_rng();
        let mut index = Index::new(0);

        let data_indexes = 0..erasure_config.num_data as u64;
        let coding_indexes = 0..erasure_config.num_coding as u64;

        assert_eq!(e_meta.status(&index), StillNeed(erasure_config.num_data));

        for ix in data_indexes.clone() {
            index.data_mut().insert(ix);
        }

        assert_eq!(e_meta.status(&index), DataFull);

        for ix in coding_indexes.clone() {
            index.coding_mut().insert(ix);
        }

        for &idx in data_indexes
            .clone()
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, erasure_config.num_data)
        {
            index.data_mut().index.remove(&idx);

            assert_eq!(e_meta.status(&index), CanRecover);
        }

        for ix in data_indexes {
            index.data_mut().insert(ix);
        }

        for &idx in coding_indexes
            .collect::<Vec<_>>()
            .choose_multiple(&mut rng, erasure_config.num_coding)
        {
            index.coding_mut().index.remove(&idx);

            assert_eq!(e_meta.status(&index), DataFull);
        }
    }

    #[test]
    fn test_clear_unconfirmed_slot() {
        let mut slot_meta = SlotMeta::new_orphan(5);
        slot_meta.consumed = 5;
        slot_meta.received = 5;
        slot_meta.next_slots = vec![6, 7];
        slot_meta.clear_unconfirmed_slot();

        let mut expected = SlotMeta::new_orphan(5);
        expected.next_slots = vec![6, 7];
        assert_eq!(slot_meta, expected);
    }
}
