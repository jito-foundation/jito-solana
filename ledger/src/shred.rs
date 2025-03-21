//! The `shred` module defines data structures and methods to pull MTU sized data frames from the
//! network. There are two types of shreds: data and coding. Data shreds contain entry information
//! while coding shreds provide redundancy to protect against dropped network packets (erasures).
//!
//! +---------------------------------------------------------------------------------------------+
//! | Data Shred                                                                                  |
//! +---------------------------------------------------------------------------------------------+
//! | common       | data       | payload                                                         |
//! | header       | header     |                                                                 |
//! |+---+---+---  |+---+---+---|+----------------------------------------------------------+----+|
//! || s | s | .   || p | f | s || data (ie ledger entries)                                 | r  ||
//! || i | h | .   || a | l | i ||                                                          | e  ||
//! || g | r | .   || r | a | z || See notes immediately after shred diagrams for an        | s  ||
//! || n | e |     || e | g | e || explanation of the "restricted" section in this payload  | t  ||
//! || a | d |     || n | s |   ||                                                          | r  ||
//! || t |   |     || t |   |   ||                                                          | i  ||
//! || u | t |     ||   |   |   ||                                                          | c  ||
//! || r | y |     || o |   |   ||                                                          | t  ||
//! || e | p |     || f |   |   ||                                                          | e  ||
//! ||   | e |     || f |   |   ||                                                          | d  ||
//! |+---+---+---  |+---+---+---+|----------------------------------------------------------+----+|
//! +---------------------------------------------------------------------------------------------+
//!
//! +---------------------------------------------------------------------------------------------+
//! | Coding Shred                                                                                |
//! +---------------------------------------------------------------------------------------------+
//! | common       | coding     | payload                                                         |
//! | header       | header     |                                                                 |
//! |+---+---+---  |+---+---+---+----------------------------------------------------------------+|
//! || s | s | .   || n | n | p || data (encoded data shred data)                                ||
//! || i | h | .   || u | u | o ||                                                               ||
//! || g | r | .   || m | m | s ||                                                               ||
//! || n | e |     ||   |   | i ||                                                               ||
//! || a | d |     || d | c | t ||                                                               ||
//! || t |   |     ||   |   | i ||                                                               ||
//! || u | t |     || s | s | o ||                                                               ||
//! || r | y |     || h | h | n ||                                                               ||
//! || e | p |     || r | r |   ||                                                               ||
//! ||   | e |     || e | e |   ||                                                               ||
//! ||   |   |     || d | d |   ||                                                               ||
//! |+---+---+---  |+---+---+---+|+--------------------------------------------------------------+|
//! +---------------------------------------------------------------------------------------------+
//!
//! Notes:
//! a) Coding shreds encode entire data shreds: both of the headers AND the payload.
//! b) Coding shreds require their own headers for identification and etc.
//! c) The erasure algorithm requires data shred and coding shred bytestreams to be equal in length.
//!
//! So, given a) - c), we must restrict data shred's payload length such that the entire coding
//! payload can fit into one coding shred / packet.

#[cfg(test)]
pub(crate) use self::shred_code::MAX_CODE_SHREDS_PER_SLOT;
pub(crate) use self::{merkle::SIZE_OF_MERKLE_ROOT, payload::serde_bytes_payload};
pub use {
    self::{
        payload::Payload,
        shred_data::ShredData,
        stats::{ProcessShredsStats, ShredFetchStats},
    },
    crate::shredder::{ReedSolomonCache, Shredder},
};
use {
    self::{shred_code::ShredCode, traits::Shred as _},
    crate::blockstore::{self, MAX_DATA_SHREDS_PER_SLOT},
    assert_matches::debug_assert_matches,
    bitflags::bitflags,
    num_enum::{IntoPrimitive, TryFromPrimitive},
    rayon::ThreadPool,
    serde::{Deserialize, Serialize},
    solana_entry::entry::{create_ticks, Entry},
    solana_perf::packet::Packet,
    solana_sdk::{
        clock::Slot,
        hash::{hashv, Hash},
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer, SIGNATURE_BYTES},
    },
    static_assertions::const_assert_eq,
    std::{fmt::Debug, time::Instant},
    thiserror::Error,
};

mod common;
mod legacy;
pub mod merkle;
pub mod payload;
pub mod shred_code;
pub mod shred_data;
mod stats;
pub mod traits;
pub mod wire;

// Alias for shred::wire::* for the old code.
// New code should use shred::wire::*.
pub mod layout {
    pub use super::wire::*;
}

pub type Nonce = u32;
const_assert_eq!(SIZE_OF_NONCE, 4);
pub const SIZE_OF_NONCE: usize = std::mem::size_of::<Nonce>();

/// The following constants are computed by hand, and hardcoded.
/// `test_shred_constants` ensures that the values are correct.
/// Constants are used over lazy_static for performance reasons.
const SIZE_OF_COMMON_SHRED_HEADER: usize = 83;
const SIZE_OF_DATA_SHRED_HEADERS: usize = 88;
const SIZE_OF_CODING_SHRED_HEADERS: usize = 89;
pub const SIZE_OF_SIGNATURE: usize = SIGNATURE_BYTES;

// Shreds are uniformly split into erasure batches with a "target" number of
// data shreds per each batch as below. The actual number of data shreds in
// each erasure batch depends on the number of shreds obtained from serializing
// a &[Entry].
pub const DATA_SHREDS_PER_FEC_BLOCK: usize = 32;

// For legacy tests and benchmarks.
const_assert_eq!(LEGACY_SHRED_DATA_CAPACITY, 1051);
pub const LEGACY_SHRED_DATA_CAPACITY: usize = legacy::ShredData::CAPACITY;

// LAST_SHRED_IN_SLOT also implies DATA_COMPLETE_SHRED.
// So it cannot be LAST_SHRED_IN_SLOT if not also DATA_COMPLETE_SHRED.
bitflags! {
    #[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
    pub struct ShredFlags:u8 {
        const SHRED_TICK_REFERENCE_MASK = 0b0011_1111;
        const DATA_COMPLETE_SHRED       = 0b0100_0000;
        const LAST_SHRED_IN_SLOT        = 0b1100_0000;
    }
}

impl ShredFlags {
    /// Creates a new ShredFlags from the given reference_tick
    ///
    /// SHRED_TICK_REFERENCE_MASK is comprised of only six bits whereas the
    /// reference_tick has 8 bits (u8). The reference_tick bits will saturate
    /// in the event that reference_tick > SHRED_TICK_REFERENCE_MASK
    pub(crate) fn from_reference_tick(reference_tick: u8) -> Self {
        Self::from_bits_retain(Self::SHRED_TICK_REFERENCE_MASK.bits().min(reference_tick))
    }
}

#[derive(Debug, Error)]
pub enum Error {
    #[error(transparent)]
    BincodeError(#[from] bincode::Error),
    #[error(transparent)]
    ErasureError(#[from] reed_solomon_erasure::Error),
    #[error("Invalid data size: {size}, payload: {payload}")]
    InvalidDataSize { size: u16, payload: usize },
    #[error("Invalid deshred set")]
    InvalidDeshredSet,
    #[error("Invalid erasure shard index: {0:?}")]
    InvalidErasureShardIndex(/*headers:*/ Box<dyn Debug + Send>),
    #[error("Invalid merkle proof")]
    InvalidMerkleProof,
    #[error("Invalid Merkle root")]
    InvalidMerkleRoot,
    #[error("Invalid num coding shreds: {0}")]
    InvalidNumCodingShreds(u16),
    #[error("Invalid parent_offset: {parent_offset}, slot: {slot}")]
    InvalidParentOffset { slot: Slot, parent_offset: u16 },
    #[error("Invalid parent slot: {parent_slot}, slot: {slot}")]
    InvalidParentSlot { slot: Slot, parent_slot: Slot },
    #[error("Invalid payload size: {0}")]
    InvalidPayloadSize(/*payload size:*/ usize),
    #[error("Invalid proof size: {0}")]
    InvalidProofSize(/*proof_size:*/ u8),
    #[error("Invalid recovered shred")]
    InvalidRecoveredShred,
    #[error("Invalid shard size: {0}")]
    InvalidShardSize(/*shard_size:*/ usize),
    #[error("Invalid shred flags: {0}")]
    InvalidShredFlags(u8),
    #[error("Invalid {0:?} shred index: {1}")]
    InvalidShredIndex(ShredType, /*shred index:*/ u32),
    #[error("Invalid shred type")]
    InvalidShredType,
    #[error("Invalid shred variant")]
    InvalidShredVariant,
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Unknown proof size")]
    UnknownProofSize,
}

#[repr(u8)]
#[cfg_attr(feature = "frozen-abi", derive(AbiExample, AbiEnumVisitor))]
#[derive(
    Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, IntoPrimitive, Serialize, TryFromPrimitive,
)]
#[serde(into = "u8", try_from = "u8")]
pub enum ShredType {
    Data = 0b1010_0101,
    Code = 0b0101_1010,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Deserialize, Serialize)]
#[serde(into = "u8", try_from = "u8")]
pub enum ShredVariant {
    LegacyCode, // 0b0101_1010
    LegacyData, // 0b1010_0101
    // proof_size is the number of Merkle proof entries, and is encoded in the
    // lowest 4 bits of the binary representation. The first 4 bits identify
    // the shred variant:
    //   0b0100_????  MerkleCode
    //   0b0110_????  MerkleCode chained
    //   0b0111_????  MerkleCode chained resigned
    //   0b1000_????  MerkleData
    //   0b1001_????  MerkleData chained
    //   0b1011_????  MerkleData chained resigned
    MerkleCode {
        proof_size: u8,
        chained: bool,
        resigned: bool,
    }, // 0b01??_????
    MerkleData {
        proof_size: u8,
        chained: bool,
        resigned: bool,
    }, // 0b10??_????
}

/// A common header that is present in data and code shred headers
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct ShredCommonHeader {
    pub signature: Signature,
    pub shred_variant: ShredVariant,
    pub slot: Slot,
    pub index: u32,
    pub version: u16,
    pub fec_set_index: u32,
}

/// The data shred header has parent offset and flags
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct DataShredHeader {
    pub parent_offset: u16,
    pub flags: ShredFlags,
    pub size: u16, // common shred header + data shred header + data
}

/// The coding shred header has FEC information
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, Hash)]
pub struct CodingShredHeader {
    pub num_data_shreds: u16,
    pub num_coding_shreds: u16,
    pub position: u16, // [0..num_coding_shreds)
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Shred {
    ShredCode(ShredCode),
    ShredData(ShredData),
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SignedData<'a> {
    Chunk(&'a [u8]), // Chunk of payload past signature.
    MerkleRoot(Hash),
}

impl AsRef<[u8]> for SignedData<'_> {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Chunk(chunk) => chunk,
            Self::MerkleRoot(root) => root.as_ref(),
        }
    }
}

/// Tuple which uniquely identifies a shred should it exists.
#[derive(Clone, Copy, Eq, Debug, Hash, PartialEq)]
pub struct ShredId(Slot, /*shred index:*/ u32, ShredType);

impl ShredId {
    #[inline]
    pub(crate) fn new(slot: Slot, index: u32, shred_type: ShredType) -> ShredId {
        ShredId(slot, index, shred_type)
    }

    #[inline]
    pub fn slot(&self) -> Slot {
        self.0
    }

    #[inline]
    pub(crate) fn unpack(&self) -> (Slot, /*shred index:*/ u32, ShredType) {
        (self.0, self.1, self.2)
    }

    pub fn seed(&self, leader: &Pubkey) -> [u8; 32] {
        let ShredId(slot, index, shred_type) = self;
        hashv(&[
            &slot.to_le_bytes(),
            &u8::from(*shred_type).to_le_bytes(),
            &index.to_le_bytes(),
            AsRef::<[u8]>::as_ref(leader),
        ])
        .to_bytes()
    }
}

/// Tuple which identifies erasure coding set that the shred belongs to.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub(crate) struct ErasureSetId(Slot, /*fec_set_index:*/ u32);

impl ErasureSetId {
    pub(crate) fn new(slot: Slot, fec_set_index: u32) -> Self {
        Self(slot, fec_set_index)
    }

    pub(crate) fn slot(&self) -> Slot {
        self.0
    }

    // Storage key for ErasureMeta and MerkleRootMeta in blockstore db.
    // Note: ErasureMeta column uses u64 so this will need to be typecast
    pub(crate) fn store_key(&self) -> (Slot, /*fec_set_index:*/ u32) {
        (self.0, self.1)
    }
}

/// To be used with the [`Shred`] enum.
///
/// Writes a function implementation that forwards the invocation to an identically defined function
/// in one of the two enum branches.
///
/// Due to an inability of a macro to match on the `self` shorthand syntax, this macro has 3
/// branches.  But they are only different in the `self` argument matching.  Make sure to keep the
/// identical otherwise.
macro_rules! dispatch {
    ($vis:vis fn $name:ident(&self $(, $arg:ident : $ty:ty)?) $(-> $out:ty)?) => {
        #[inline]
        $vis fn $name(&self $(, $arg:$ty)?) $(-> $out)? {
            match self {
                Self::ShredCode(shred) => shred.$name($($arg, )?),
                Self::ShredData(shred) => shred.$name($($arg, )?),
            }
        }
    };
    ($vis:vis fn $name:ident(self $(, $arg:ident : $ty:ty)?) $(-> $out:ty)?) => {
        #[inline]
        $vis fn $name(self $(, $arg:$ty)?) $(-> $out)? {
            match self {
                Self::ShredCode(shred) => shred.$name($($arg, )?),
                Self::ShredData(shred) => shred.$name($($arg, )?),
            }
        }
    };
    ($vis:vis fn $name:ident(&mut self $(, $arg:ident : $ty:ty)?) $(-> $out:ty)?) => {
        #[inline]
        $vis fn $name(&mut self $(, $arg:$ty)?) $(-> $out)? {
            match self {
                Self::ShredCode(shred) => shred.$name($($arg, )?),
                Self::ShredData(shred) => shred.$name($($arg, )?),
            }
        }
    }
}

use dispatch;

impl Shred {
    dispatch!(fn common_header(&self) -> &ShredCommonHeader);
    dispatch!(fn set_signature(&mut self, signature: Signature));
    dispatch!(fn signed_data(&self) -> Result<SignedData, Error>);

    dispatch!(pub fn chained_merkle_root(&self) -> Result<Hash, Error>);
    // Returns the portion of the shred's payload which is erasure coded.
    dispatch!(pub(crate) fn erasure_shard(&self) -> Result<&[u8], Error>);
    // Returns the shard index within the erasure coding set.
    dispatch!(pub(crate) fn erasure_shard_index(&self) -> Result<usize, Error>);
    dispatch!(pub(crate) fn retransmitter_signature(&self) -> Result<Signature, Error>);

    dispatch!(pub fn into_payload(self) -> Payload);
    dispatch!(pub fn merkle_root(&self) -> Result<Hash, Error>);
    dispatch!(pub fn payload(&self) -> &Payload);
    dispatch!(pub fn sanitize(&self) -> Result<(), Error>);

    // Only for tests.
    dispatch!(pub fn set_index(&mut self, index: u32));
    dispatch!(pub fn set_slot(&mut self, slot: Slot));

    pub fn copy_to_packet(&self, packet: &mut Packet) {
        let payload = self.payload();
        let size = payload.len();
        packet.buffer_mut()[..size].copy_from_slice(&payload[..]);
        packet.meta_mut().size = size;
    }

    // TODO: Should this sanitize output?
    pub fn new_from_data(
        slot: Slot,
        index: u32,
        parent_offset: u16,
        data: &[u8],
        flags: ShredFlags,
        reference_tick: u8,
        version: u16,
        fec_set_index: u32,
    ) -> Self {
        Self::from(ShredData::new_from_data(
            slot,
            index,
            parent_offset,
            data,
            flags,
            reference_tick,
            version,
            fec_set_index,
        ))
    }

    pub fn new_from_serialized_shred<T>(shred: T) -> Result<Self, Error>
    where
        T: AsRef<[u8]> + Into<Payload>,
        Payload: From<T>,
    {
        Ok(match layout::get_shred_variant(shred.as_ref())? {
            ShredVariant::LegacyCode => {
                let shred = legacy::ShredCode::from_payload(shred)?;
                Self::from(ShredCode::from(shred))
            }
            ShredVariant::LegacyData => {
                let shred = legacy::ShredData::from_payload(shred)?;
                Self::from(ShredData::from(shred))
            }
            ShredVariant::MerkleCode { .. } => {
                let shred = merkle::ShredCode::from_payload(shred)?;
                Self::from(ShredCode::from(shred))
            }
            ShredVariant::MerkleData { .. } => {
                let shred = merkle::ShredData::from_payload(shred)?;
                Self::from(ShredData::from(shred))
            }
        })
    }

    pub fn new_from_parity_shard(
        slot: Slot,
        index: u32,
        parity_shard: &[u8],
        fec_set_index: u32,
        num_data_shreds: u16,
        num_coding_shreds: u16,
        position: u16,
        version: u16,
    ) -> Self {
        Self::from(ShredCode::new_from_parity_shard(
            slot,
            index,
            parity_shard,
            fec_set_index,
            num_data_shreds,
            num_coding_shreds,
            position,
            version,
        ))
    }

    /// Unique identifier for each shred.
    pub fn id(&self) -> ShredId {
        ShredId(self.slot(), self.index(), self.shred_type())
    }

    pub fn slot(&self) -> Slot {
        self.common_header().slot
    }

    pub fn parent(&self) -> Result<Slot, Error> {
        match self {
            Self::ShredCode(_) => Err(Error::InvalidShredType),
            Self::ShredData(shred) => shred.parent(),
        }
    }

    pub fn index(&self) -> u32 {
        self.common_header().index
    }

    // Possibly trimmed payload;
    // Should only be used when storing shreds to blockstore.
    pub(crate) fn bytes_to_store(&self) -> &[u8] {
        match self {
            Self::ShredCode(shred) => shred.payload(),
            Self::ShredData(shred) => shred.bytes_to_store(),
        }
    }

    pub fn fec_set_index(&self) -> u32 {
        self.common_header().fec_set_index
    }

    pub(crate) fn first_coding_index(&self) -> Option<u32> {
        match self {
            Self::ShredCode(shred) => shred.first_coding_index(),
            Self::ShredData(_) => None,
        }
    }

    pub fn version(&self) -> u16 {
        self.common_header().version
    }

    // Identifier for the erasure coding set that the shred belongs to.
    pub(crate) fn erasure_set(&self) -> ErasureSetId {
        ErasureSetId(self.slot(), self.fec_set_index())
    }

    pub fn signature(&self) -> &Signature {
        &self.common_header().signature
    }

    pub fn sign(&mut self, keypair: &Keypair) {
        let data = self.signed_data().unwrap();
        let signature = keypair.sign_message(data.as_ref());
        self.set_signature(signature);
    }

    #[inline]
    pub fn shred_type(&self) -> ShredType {
        ShredType::from(self.common_header().shred_variant)
    }

    #[inline]
    pub fn is_data(&self) -> bool {
        self.shred_type() == ShredType::Data
    }

    #[inline]
    pub fn is_code(&self) -> bool {
        self.shred_type() == ShredType::Code
    }

    pub fn last_in_slot(&self) -> bool {
        match self {
            Self::ShredCode(_) => false,
            Self::ShredData(shred) => shred.last_in_slot(),
        }
    }

    /// This is not a safe function. It only changes the meta information.
    /// Use this only for test code which doesn't care about actual shred
    pub fn set_last_in_slot(&mut self) {
        match self {
            Self::ShredCode(_) => (),
            Self::ShredData(shred) => shred.set_last_in_slot(),
        }
    }

    pub fn data_complete(&self) -> bool {
        match self {
            Self::ShredCode(_) => false,
            Self::ShredData(shred) => shred.data_complete(),
        }
    }

    pub(crate) fn reference_tick(&self) -> u8 {
        match self {
            Self::ShredCode(_) => ShredFlags::SHRED_TICK_REFERENCE_MASK.bits(),
            Self::ShredData(shred) => shred.reference_tick(),
        }
    }

    #[must_use]
    pub fn verify(&self, pubkey: &Pubkey) -> bool {
        match self.signed_data() {
            Ok(data) => self.signature().verify(pubkey.as_ref(), data.as_ref()),
            Err(_) => false,
        }
    }

    // Returns true if the erasure coding of the two shreds mismatch.
    pub(crate) fn erasure_mismatch(&self, other: &Self) -> Result<bool, Error> {
        match (self, other) {
            (Self::ShredCode(shred), Self::ShredCode(other)) => Ok(shred.erasure_mismatch(other)),
            _ => Err(Error::InvalidShredType),
        }
    }

    pub(crate) fn num_data_shreds(&self) -> Result<u16, Error> {
        match self {
            Self::ShredCode(shred) => Ok(shred.num_data_shreds()),
            Self::ShredData(_) => Err(Error::InvalidShredType),
        }
    }

    pub(crate) fn num_coding_shreds(&self) -> Result<u16, Error> {
        match self {
            Self::ShredCode(shred) => Ok(shred.num_coding_shreds()),
            Self::ShredData(_) => Err(Error::InvalidShredType),
        }
    }

    /// Returns true if the other shred has the same ShredId, i.e. (slot, index,
    /// shred-type), but different payload.
    /// Retransmitter's signature is ignored when comparing payloads.
    pub fn is_shred_duplicate(&self, other: &Shred) -> bool {
        if self.id() != other.id() {
            return false;
        }
        fn get_payload(shred: &Shred) -> &[u8] {
            let Ok(offset) = shred.retransmitter_signature_offset() else {
                return shred.payload();
            };
            // Assert that the retransmitter's signature is at the very end of
            // the shred payload.
            debug_assert_eq!(offset + SIZE_OF_SIGNATURE, shred.payload().len());
            shred
                .payload()
                .get(..offset)
                .unwrap_or_else(|| shred.payload())
        }
        get_payload(self) != get_payload(other)
    }

    fn retransmitter_signature_offset(&self) -> Result<usize, Error> {
        match self {
            Self::ShredCode(ShredCode::Merkle(shred)) => shred.retransmitter_signature_offset(),
            Self::ShredData(ShredData::Merkle(shred)) => shred.retransmitter_signature_offset(),
            Self::ShredCode(ShredCode::Legacy(_)) => Err(Error::InvalidShredVariant),
            Self::ShredData(ShredData::Legacy(_)) => Err(Error::InvalidShredVariant),
        }
    }
}

impl From<ShredCode> for Shred {
    fn from(shred: ShredCode) -> Self {
        Self::ShredCode(shred)
    }
}

impl From<ShredData> for Shred {
    fn from(shred: ShredData) -> Self {
        Self::ShredData(shred)
    }
}

impl From<merkle::Shred> for Shred {
    fn from(shred: merkle::Shred) -> Self {
        match shred {
            merkle::Shred::ShredCode(shred) => Self::ShredCode(ShredCode::Merkle(shred)),
            merkle::Shred::ShredData(shred) => Self::ShredData(ShredData::Merkle(shred)),
        }
    }
}

impl TryFrom<Shred> for merkle::Shred {
    type Error = Error;

    fn try_from(shred: Shred) -> Result<Self, Self::Error> {
        match shred {
            Shred::ShredCode(ShredCode::Legacy(_)) => Err(Error::InvalidShredVariant),
            Shred::ShredCode(ShredCode::Merkle(shred)) => Ok(Self::ShredCode(shred)),
            Shred::ShredData(ShredData::Legacy(_)) => Err(Error::InvalidShredVariant),
            Shred::ShredData(ShredData::Merkle(shred)) => Ok(Self::ShredData(shred)),
        }
    }
}

impl From<ShredVariant> for ShredType {
    #[inline]
    fn from(shred_variant: ShredVariant) -> Self {
        match shred_variant {
            ShredVariant::LegacyCode => ShredType::Code,
            ShredVariant::LegacyData => ShredType::Data,
            ShredVariant::MerkleCode { .. } => ShredType::Code,
            ShredVariant::MerkleData { .. } => ShredType::Data,
        }
    }
}

impl From<ShredVariant> for u8 {
    #[inline]
    fn from(shred_variant: ShredVariant) -> u8 {
        match shred_variant {
            ShredVariant::LegacyCode => u8::from(ShredType::Code),
            ShredVariant::LegacyData => u8::from(ShredType::Data),
            ShredVariant::MerkleCode {
                proof_size,
                chained: false,
                resigned: false,
            } => proof_size | 0x40,
            ShredVariant::MerkleCode {
                proof_size,
                chained: true,
                resigned: false,
            } => proof_size | 0x60,
            ShredVariant::MerkleCode {
                proof_size,
                chained: true,
                resigned: true,
            } => proof_size | 0x70,
            ShredVariant::MerkleData {
                proof_size,
                chained: false,
                resigned: false,
            } => proof_size | 0x80,
            ShredVariant::MerkleData {
                proof_size,
                chained: true,
                resigned: false,
            } => proof_size | 0x90,
            ShredVariant::MerkleData {
                proof_size,
                chained: true,
                resigned: true,
            } => proof_size | 0xb0,
            ShredVariant::MerkleCode {
                proof_size: _,
                chained: false,
                resigned: true,
            }
            | ShredVariant::MerkleData {
                proof_size: _,
                chained: false,
                resigned: true,
            } => panic!("Invalid shred variant: {shred_variant:?}"),
        }
    }
}

impl TryFrom<u8> for ShredVariant {
    type Error = Error;
    #[inline]
    fn try_from(shred_variant: u8) -> Result<Self, Self::Error> {
        if shred_variant == u8::from(ShredType::Code) {
            Ok(ShredVariant::LegacyCode)
        } else if shred_variant == u8::from(ShredType::Data) {
            Ok(ShredVariant::LegacyData)
        } else {
            let proof_size = shred_variant & 0x0F;
            match shred_variant & 0xF0 {
                0x40 => Ok(ShredVariant::MerkleCode {
                    proof_size,
                    chained: false,
                    resigned: false,
                }),
                0x60 => Ok(ShredVariant::MerkleCode {
                    proof_size,
                    chained: true,
                    resigned: false,
                }),
                0x70 => Ok(ShredVariant::MerkleCode {
                    proof_size,
                    chained: true,
                    resigned: true,
                }),
                0x80 => Ok(ShredVariant::MerkleData {
                    proof_size,
                    chained: false,
                    resigned: false,
                }),
                0x90 => Ok(ShredVariant::MerkleData {
                    proof_size,
                    chained: true,
                    resigned: false,
                }),
                0xb0 => Ok(ShredVariant::MerkleData {
                    proof_size,
                    chained: true,
                    resigned: true,
                }),
                _ => Err(Error::InvalidShredVariant),
            }
        }
    }
}

pub(crate) fn recover(
    shreds: impl IntoIterator<Item = Shred>,
    reed_solomon_cache: &ReedSolomonCache,
) -> Result<impl Iterator<Item = Result<Shred, Error>>, Error> {
    let shreds = shreds
        .into_iter()
        .map(|shred| {
            debug_assert_matches!(
                shred.common_header().shred_variant,
                ShredVariant::MerkleCode { .. } | ShredVariant::MerkleData { .. }
            );
            merkle::Shred::try_from(shred)
        })
        .collect::<Result<_, _>>()?;
    // With Merkle shreds, leader signs the Merkle root of the erasure batch
    // and all shreds within the same erasure batch have the same signature.
    // For recovered shreds, the (unique) signature is copied from shreds which
    // were received from turbine (or repair) and are already sig-verified.
    // The same signature also verifies for recovered shreds because when
    // reconstructing the Merkle tree for the erasure batch, we will obtain the
    // same Merkle root.
    let shreds = merkle::recover(shreds, reed_solomon_cache)?;
    Ok(shreds.map(|shred| shred.map(Shred::from)))
}

#[allow(clippy::too_many_arguments)]
pub fn make_merkle_shreds_from_entries(
    thread_pool: &ThreadPool,
    keypair: &Keypair,
    entries: &[Entry],
    slot: Slot,
    parent_slot: Slot,
    shred_version: u16,
    reference_tick: u8,
    is_last_in_slot: bool,
    chained_merkle_root: Option<Hash>,
    next_shred_index: u32,
    next_code_index: u32,
    reed_solomon_cache: &ReedSolomonCache,
    stats: &mut ProcessShredsStats,
) -> Result<Vec<Shred>, Error> {
    let now = Instant::now();
    let entries = bincode::serialize(entries)?;
    stats.serialize_elapsed += now.elapsed().as_micros() as u64;
    let shreds = merkle::make_shreds_from_data(
        thread_pool,
        keypair,
        chained_merkle_root,
        &entries[..],
        slot,
        parent_slot,
        shred_version,
        reference_tick,
        is_last_in_slot,
        next_shred_index,
        next_code_index,
        reed_solomon_cache,
        stats,
    )?;
    Ok(shreds.into_iter().map(Shred::from).collect())
}

// Accepts shreds in the slot range [root + 1, max_slot].
#[must_use]
pub fn should_discard_shred(
    packet: &Packet,
    root: Slot,
    max_slot: Slot,
    shred_version: u16,
    drop_unchained_merkle_shreds: impl Fn(Slot) -> bool,
    stats: &mut ShredFetchStats,
) -> bool {
    debug_assert!(root < max_slot);
    let shred = match layout::get_shred(packet) {
        None => {
            stats.index_overrun += 1;
            return true;
        }
        Some(shred) => shred,
    };
    match layout::get_version(shred) {
        None => {
            stats.index_overrun += 1;
            return true;
        }
        Some(version) => {
            if version != shred_version {
                stats.shred_version_mismatch += 1;
                return true;
            }
        }
    }
    let Ok(shred_variant) = layout::get_shred_variant(shred) else {
        stats.bad_shred_type += 1;
        return true;
    };
    let slot = match layout::get_slot(shred) {
        Some(slot) => {
            if slot > max_slot {
                stats.slot_out_of_range += 1;
                return true;
            }
            slot
        }
        None => {
            stats.slot_bad_deserialize += 1;
            return true;
        }
    };
    let Some(index) = layout::get_index(shred) else {
        stats.index_bad_deserialize += 1;
        return true;
    };
    match ShredType::from(shred_variant) {
        ShredType::Code => {
            if index >= shred_code::MAX_CODE_SHREDS_PER_SLOT as u32 {
                stats.index_out_of_bounds += 1;
                return true;
            }
            if slot <= root {
                stats.slot_out_of_range += 1;
                return true;
            }
        }
        ShredType::Data => {
            if index >= MAX_DATA_SHREDS_PER_SLOT as u32 {
                stats.index_out_of_bounds += 1;
                return true;
            }
            let Some(parent_offset) = layout::get_parent_offset(shred) else {
                stats.bad_parent_offset += 1;
                return true;
            };
            let Some(parent) = slot.checked_sub(Slot::from(parent_offset)) else {
                stats.bad_parent_offset += 1;
                return true;
            };
            if !blockstore::verify_shred_slots(slot, parent, root) {
                stats.slot_out_of_range += 1;
                return true;
            }
        }
    }
    match shred_variant {
        ShredVariant::LegacyCode | ShredVariant::LegacyData => {
            return true;
        }
        ShredVariant::MerkleCode { chained: false, .. } => {
            if drop_unchained_merkle_shreds(slot) {
                return true;
            }
            stats.num_shreds_merkle_code = stats.num_shreds_merkle_code.saturating_add(1);
        }
        ShredVariant::MerkleCode { chained: true, .. } => {
            stats.num_shreds_merkle_code_chained =
                stats.num_shreds_merkle_code_chained.saturating_add(1);
        }
        ShredVariant::MerkleData { chained: false, .. } => {
            if drop_unchained_merkle_shreds(slot) {
                return true;
            }
            stats.num_shreds_merkle_data = stats.num_shreds_merkle_data.saturating_add(1);
        }
        ShredVariant::MerkleData { chained: true, .. } => {
            stats.num_shreds_merkle_data_chained =
                stats.num_shreds_merkle_data_chained.saturating_add(1);
        }
    }
    false
}

pub fn max_ticks_per_n_shreds(num_shreds: u64, shred_data_size: Option<usize>) -> u64 {
    let ticks = create_ticks(1, 0, Hash::default());
    max_entries_per_n_shred(&ticks[0], num_shreds, shred_data_size)
}

pub fn max_entries_per_n_shred(
    entry: &Entry,
    num_shreds: u64,
    shred_data_size: Option<usize>,
) -> u64 {
    // Default 32:32 erasure batches yields 64 shreds; log2(64) = 6.
    let merkle_variant = Some((
        /*proof_size:*/ 6, /*chained:*/ true, /*resigned:*/ true,
    ));
    let data_buffer_size = ShredData::capacity(merkle_variant).unwrap();
    let shred_data_size = shred_data_size.unwrap_or(data_buffer_size) as u64;
    let vec_size = bincode::serialized_size(&vec![entry]).unwrap();
    let entry_size = bincode::serialized_size(entry).unwrap();
    let count_size = vec_size - entry_size;

    (shred_data_size * num_shreds - count_size) / entry_size
}

pub fn verify_test_data_shred(
    shred: &Shred,
    index: u32,
    slot: Slot,
    parent: Slot,
    pk: &Pubkey,
    verify: bool,
    is_last_in_slot: bool,
    is_last_data: bool,
) {
    shred.sanitize().unwrap();
    assert!(shred.is_data());
    assert_eq!(shred.index(), index);
    assert_eq!(shred.slot(), slot);
    assert_eq!(shred.parent().unwrap(), parent);
    assert_eq!(verify, shred.verify(pk));
    if is_last_in_slot {
        assert!(shred.last_in_slot());
    } else {
        assert!(!shred.last_in_slot());
    }
    if is_last_data {
        assert!(shred.data_complete());
    } else {
        assert!(!shred.data_complete());
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert_matches::assert_matches,
        bincode::serialized_size,
        itertools::Itertools,
        rand::Rng,
        rand_chacha::{rand_core::SeedableRng, ChaChaRng},
        rayon::ThreadPoolBuilder,
        solana_sdk::{shred_version, signature::Signer, signer::keypair::keypair_from_seed},
        std::io::{Cursor, Seek, SeekFrom, Write},
        test_case::test_case,
    };

    const SIZE_OF_SHRED_INDEX: usize = 4;
    const SIZE_OF_SHRED_SLOT: usize = 8;
    const SIZE_OF_SHRED_VARIANT: usize = 1;

    const OFFSET_OF_SHRED_SLOT: usize = SIZE_OF_SIGNATURE + SIZE_OF_SHRED_VARIANT;
    const OFFSET_OF_SHRED_INDEX: usize = OFFSET_OF_SHRED_SLOT + SIZE_OF_SHRED_SLOT;
    const OFFSET_OF_SHRED_VARIANT: usize = SIZE_OF_SIGNATURE;

    fn bs58_decode<T: AsRef<[u8]>>(data: T) -> Vec<u8> {
        bs58::decode(data).into_vec().unwrap()
    }

    pub(super) fn make_merkle_shreds_for_tests<R: Rng>(
        rng: &mut R,
        slot: Slot,
        data_size: usize,
        chained: bool,
        is_last_in_slot: bool,
    ) -> Result<Vec<merkle::Shred>, Error> {
        let thread_pool = ThreadPoolBuilder::new().num_threads(2).build().unwrap();
        let chained_merkle_root = chained.then(|| Hash::new_from_array(rng.gen()));
        let parent_offset = rng.gen_range(1..=u16::try_from(slot).unwrap_or(u16::MAX));
        let parent_slot = slot.checked_sub(u64::from(parent_offset)).unwrap();
        let mut data = vec![0u8; data_size];
        rng.fill(&mut data[..]);
        merkle::make_shreds_from_data(
            &thread_pool,
            &Keypair::new(),
            chained_merkle_root,
            &data[..],
            slot,
            parent_slot,
            rng.gen(),            // shred_version
            rng.gen_range(1..64), // reference_tick
            is_last_in_slot,
            rng.gen_range(0..671), // next_shred_index
            rng.gen_range(0..781), // next_code_index
            &ReedSolomonCache::default(),
            &mut ProcessShredsStats::default(),
        )
    }

    #[test]
    fn test_shred_constants() {
        let common_header = ShredCommonHeader {
            signature: Signature::default(),
            shred_variant: ShredVariant::LegacyCode,
            slot: Slot::MAX,
            index: u32::MAX,
            version: u16::MAX,
            fec_set_index: u32::MAX,
        };
        let data_shred_header = DataShredHeader {
            parent_offset: u16::MAX,
            flags: ShredFlags::all(),
            size: u16::MAX,
        };
        let coding_shred_header = CodingShredHeader {
            num_data_shreds: u16::MAX,
            num_coding_shreds: u16::MAX,
            position: u16::MAX,
        };
        assert_eq!(
            SIZE_OF_COMMON_SHRED_HEADER,
            serialized_size(&common_header).unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_CODING_SHRED_HEADERS - SIZE_OF_COMMON_SHRED_HEADER,
            serialized_size(&coding_shred_header).unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_DATA_SHRED_HEADERS - SIZE_OF_COMMON_SHRED_HEADER,
            serialized_size(&data_shred_header).unwrap() as usize
        );
        let data_shred_header_with_size = DataShredHeader {
            size: 1000,
            ..data_shred_header
        };
        assert_eq!(
            SIZE_OF_DATA_SHRED_HEADERS - SIZE_OF_COMMON_SHRED_HEADER,
            serialized_size(&data_shred_header_with_size).unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_SIGNATURE,
            bincode::serialized_size(&Signature::default()).unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_SHRED_VARIANT,
            bincode::serialized_size(&ShredVariant::MerkleCode {
                proof_size: 15,
                chained: true,
                resigned: true
            })
            .unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_SHRED_SLOT,
            bincode::serialized_size(&Slot::default()).unwrap() as usize
        );
        assert_eq!(
            SIZE_OF_SHRED_INDEX,
            bincode::serialized_size(&common_header.index).unwrap() as usize
        );
    }

    #[test]
    fn test_shred_flags_reference_tick_saturates() {
        const MAX_REFERENCE_TICK: u8 = ShredFlags::SHRED_TICK_REFERENCE_MASK.bits();
        for tick in 0..=u8::MAX {
            let flags = ShredFlags::from_reference_tick(tick);
            assert_eq!(flags.bits(), tick.min(MAX_REFERENCE_TICK));
        }
    }

    #[test]
    fn test_version_from_hash() {
        let hash = [
            0xa5u8, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5,
            0x5a, 0x5a, 0xa5, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5, 0x5a, 0x5a,
            0xa5, 0xa5, 0x5a, 0x5a,
        ];
        let version = shred_version::version_from_hash(&Hash::new_from_array(hash));
        assert_eq!(version, 1);
        let hash = [
            0xa5u8, 0xa5, 0x5a, 0x5a, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let version = shred_version::version_from_hash(&Hash::new_from_array(hash));
        assert_eq!(version, 0xffff);
        let hash = [
            0xa5u8, 0xa5, 0x5a, 0x5a, 0xa5, 0xa5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        let version = shred_version::version_from_hash(&Hash::new_from_array(hash));
        assert_eq!(version, 0x5a5b);
    }

    #[test]
    fn test_invalid_parent_offset() {
        let shred = Shred::new_from_data(10, 0, 1000, &[1, 2, 3], ShredFlags::empty(), 0, 1, 0);
        let mut packet = Packet::default();
        shred.copy_to_packet(&mut packet);
        let shred_res = Shred::new_from_serialized_shred(packet.data(..).unwrap().to_vec());
        assert_matches!(
            shred.parent(),
            Err(Error::InvalidParentOffset {
                slot: 10,
                parent_offset: 1000
            })
        );
        assert_matches!(
            shred_res,
            Err(Error::InvalidParentOffset {
                slot: 10,
                parent_offset: 1000
            })
        );
    }

    #[test_case(false, false)]
    #[test_case(false, true)]
    #[test_case(true, false)]
    #[test_case(true, true)]
    fn test_should_discard_shred(chained: bool, is_last_in_slot: bool) {
        solana_logger::setup();
        let mut rng = rand::thread_rng();
        let slot = 18_291;
        let shreds = make_merkle_shreds_for_tests(
            &mut rng,
            slot,
            1200 * 5, // data_size
            chained,
            is_last_in_slot,
        )
        .unwrap();
        let shreds: Vec<_> = shreds.into_iter().map(Shred::from).collect();
        assert_eq!(shreds.iter().map(Shred::fec_set_index).dedup().count(), 1);

        assert_matches!(shreds[0].shred_type(), ShredType::Data);
        let parent_slot = shreds[0].parent().unwrap();
        let shred_version = shreds[0].common_header().version;

        let root = rng.gen_range(0..parent_slot);
        let max_slot = slot + rng.gen_range(1..65536);
        let mut packet = Packet::default();

        // Data shred sanity checks!
        {
            let shred = shreds.first().unwrap();
            assert_eq!(shred.shred_type(), ShredType::Data);
            shred.copy_to_packet(&mut packet);
            let mut stats = ShredFetchStats::default();
            assert!(!should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
        }
        {
            let mut packet = packet.clone();
            let mut stats = ShredFetchStats::default();
            packet.meta_mut().size = OFFSET_OF_SHRED_VARIANT;
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_overrun, 1);

            packet.meta_mut().size = OFFSET_OF_SHRED_INDEX;
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_overrun, 2);

            packet.meta_mut().size = OFFSET_OF_SHRED_INDEX + 1;
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_overrun, 3);

            packet.meta_mut().size = OFFSET_OF_SHRED_INDEX + SIZE_OF_SHRED_INDEX - 1;
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_overrun, 4);

            packet.meta_mut().size = OFFSET_OF_SHRED_INDEX + SIZE_OF_SHRED_INDEX + 2;
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_overrun, 5);
        }
        {
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version.wrapping_add(1),
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.shred_version_mismatch, 1);
        }
        {
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                parent_slot + 1, // root
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.slot_out_of_range, 1);
        }
        {
            let parent_offset = 0u16;
            {
                let mut cursor = Cursor::new(packet.buffer_mut());
                cursor.seek(SeekFrom::Start(83)).unwrap();
                cursor.write_all(&parent_offset.to_le_bytes()).unwrap();
            }
            assert_eq!(
                layout::get_parent_offset(packet.data(..).unwrap()),
                Some(parent_offset)
            );
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.slot_out_of_range, 1);
        }
        {
            let parent_offset = u16::try_from(slot + 1).unwrap();
            {
                let mut cursor = Cursor::new(packet.buffer_mut());
                cursor.seek(SeekFrom::Start(83)).unwrap();
                cursor.write_all(&parent_offset.to_le_bytes()).unwrap();
            }
            assert_eq!(
                layout::get_parent_offset(packet.data(..).unwrap()),
                Some(parent_offset)
            );
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.bad_parent_offset, 1);
        }
        {
            let index = u32::MAX - 10;
            {
                let mut cursor = Cursor::new(packet.buffer_mut());
                cursor
                    .seek(SeekFrom::Start(OFFSET_OF_SHRED_INDEX as u64))
                    .unwrap();
                cursor.write_all(&index.to_le_bytes()).unwrap();
            }
            assert_eq!(layout::get_index(packet.data(..).unwrap()), Some(index));
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_out_of_bounds, 1);
        }

        // Coding shred sanity checks!
        {
            let shred = shreds.last().unwrap();
            assert_eq!(shred.shred_type(), ShredType::Code);
            shreds.last().unwrap().copy_to_packet(&mut packet);
            let mut stats = ShredFetchStats::default();
            assert!(!should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
        }
        {
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version.wrapping_add(1),
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.shred_version_mismatch, 1);
        }
        {
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                slot, // root
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.slot_out_of_range, 1);
        }
        {
            let index = u32::try_from(MAX_CODE_SHREDS_PER_SLOT).unwrap();
            {
                let mut cursor = Cursor::new(packet.buffer_mut());
                cursor
                    .seek(SeekFrom::Start(OFFSET_OF_SHRED_INDEX as u64))
                    .unwrap();
                cursor.write_all(&index.to_le_bytes()).unwrap();
            }
            assert_eq!(layout::get_index(packet.data(..).unwrap()), Some(index));
            let mut stats = ShredFetchStats::default();
            assert!(should_discard_shred(
                &packet,
                root,
                max_slot,
                shred_version,
                |_| false, // drop_unchained_merkle_shreds
                &mut stats
            ));
            assert_eq!(stats.index_out_of_bounds, 1);
        }
    }

    // Asserts that ShredType is backward compatible with u8.
    #[test]
    fn test_shred_type_compat() {
        assert_eq!(std::mem::size_of::<ShredType>(), std::mem::size_of::<u8>());
        assert_matches!(ShredType::try_from(0u8), Err(_));
        assert_matches!(ShredType::try_from(1u8), Err(_));
        assert_matches!(bincode::deserialize::<ShredType>(&[0u8]), Err(_));
        assert_matches!(bincode::deserialize::<ShredType>(&[1u8]), Err(_));
        // data shred
        assert_eq!(ShredType::Data as u8, 0b1010_0101);
        assert_eq!(u8::from(ShredType::Data), 0b1010_0101);
        assert_eq!(ShredType::try_from(0b1010_0101), Ok(ShredType::Data));
        let buf = bincode::serialize(&ShredType::Data).unwrap();
        assert_eq!(buf, vec![0b1010_0101]);
        assert_matches!(
            bincode::deserialize::<ShredType>(&[0b1010_0101]),
            Ok(ShredType::Data)
        );
        // coding shred
        assert_eq!(ShredType::Code as u8, 0b0101_1010);
        assert_eq!(u8::from(ShredType::Code), 0b0101_1010);
        assert_eq!(ShredType::try_from(0b0101_1010), Ok(ShredType::Code));
        let buf = bincode::serialize(&ShredType::Code).unwrap();
        assert_eq!(buf, vec![0b0101_1010]);
        assert_matches!(
            bincode::deserialize::<ShredType>(&[0b0101_1010]),
            Ok(ShredType::Code)
        );
    }

    #[test]
    fn test_shred_variant_compat() {
        assert_matches!(ShredVariant::try_from(0u8), Err(_));
        assert_matches!(ShredVariant::try_from(1u8), Err(_));
        assert_matches!(ShredVariant::try_from(0b0101_0000), Err(_));
        assert_matches!(ShredVariant::try_from(0b1010_0000), Err(_));
        assert_matches!(bincode::deserialize::<ShredVariant>(&[0b0101_0000]), Err(_));
        assert_matches!(bincode::deserialize::<ShredVariant>(&[0b1010_0000]), Err(_));
        // Legacy coding shred.
        assert_eq!(u8::from(ShredVariant::LegacyCode), 0b0101_1010);
        assert_eq!(ShredType::from(ShredVariant::LegacyCode), ShredType::Code);
        assert_matches!(
            ShredVariant::try_from(0b0101_1010),
            Ok(ShredVariant::LegacyCode)
        );
        let buf = bincode::serialize(&ShredVariant::LegacyCode).unwrap();
        assert_eq!(buf, vec![0b0101_1010]);
        assert_matches!(
            bincode::deserialize::<ShredVariant>(&[0b0101_1010]),
            Ok(ShredVariant::LegacyCode)
        );
        // Legacy data shred.
        assert_eq!(u8::from(ShredVariant::LegacyData), 0b1010_0101);
        assert_eq!(ShredType::from(ShredVariant::LegacyData), ShredType::Data);
        assert_matches!(
            ShredVariant::try_from(0b1010_0101),
            Ok(ShredVariant::LegacyData)
        );
        let buf = bincode::serialize(&ShredVariant::LegacyData).unwrap();
        assert_eq!(buf, vec![0b1010_0101]);
        assert_matches!(
            bincode::deserialize::<ShredVariant>(&[0b1010_0101]),
            Ok(ShredVariant::LegacyData)
        );
    }

    #[test_case(false, false, 0b0100_0000)]
    #[test_case(true, false, 0b0110_0000)]
    #[test_case(true, true, 0b0111_0000)]
    fn test_shred_variant_compat_merkle_code(chained: bool, resigned: bool, byte: u8) {
        for proof_size in 0..=15u8 {
            let byte = byte | proof_size;
            assert_eq!(
                u8::from(ShredVariant::MerkleCode {
                    proof_size,
                    chained,
                    resigned,
                }),
                byte
            );
            assert_eq!(
                ShredType::from(ShredVariant::MerkleCode {
                    proof_size,
                    chained,
                    resigned,
                }),
                ShredType::Code
            );
            assert_eq!(
                ShredVariant::try_from(byte).unwrap(),
                ShredVariant::MerkleCode {
                    proof_size,
                    chained,
                    resigned,
                },
            );
            let buf = bincode::serialize(&ShredVariant::MerkleCode {
                proof_size,
                chained,
                resigned,
            })
            .unwrap();
            assert_eq!(buf, vec![byte]);
            assert_eq!(
                bincode::deserialize::<ShredVariant>(&[byte]).unwrap(),
                ShredVariant::MerkleCode {
                    proof_size,
                    chained,
                    resigned,
                }
            );
        }
    }

    #[test_case(false, false, 0b1000_0000)]
    #[test_case(true, false, 0b1001_0000)]
    #[test_case(true, true, 0b1011_0000)]
    fn test_shred_variant_compat_merkle_data(chained: bool, resigned: bool, byte: u8) {
        for proof_size in 0..=15u8 {
            let byte = byte | proof_size;
            assert_eq!(
                u8::from(ShredVariant::MerkleData {
                    proof_size,
                    chained,
                    resigned,
                }),
                byte
            );
            assert_eq!(
                ShredType::from(ShredVariant::MerkleData {
                    proof_size,
                    chained,
                    resigned,
                }),
                ShredType::Data
            );
            assert_eq!(
                ShredVariant::try_from(byte).unwrap(),
                ShredVariant::MerkleData {
                    proof_size,
                    chained,
                    resigned
                }
            );
            let buf = bincode::serialize(&ShredVariant::MerkleData {
                proof_size,
                chained,
                resigned,
            })
            .unwrap();
            assert_eq!(buf, vec![byte]);
            assert_eq!(
                bincode::deserialize::<ShredVariant>(&[byte]).unwrap(),
                ShredVariant::MerkleData {
                    proof_size,
                    chained,
                    resigned
                }
            );
        }
    }

    #[test]
    fn test_shred_seed() {
        let mut rng = ChaChaRng::from_seed([147u8; 32]);
        let leader = Pubkey::new_from_array(rng.gen());
        let key = ShredId(
            141939602, // slot
            28685,     // index
            ShredType::Data,
        );
        assert_eq!(
            bs58::encode(key.seed(&leader)).into_string(),
            "Gp4kUM4ZpWGQN5XSCyM9YHYWEBCAZLa94ZQuSgDE4r56"
        );
        let leader = Pubkey::new_from_array(rng.gen());
        let key = ShredId(
            141945197, // slot
            23418,     // index
            ShredType::Code,
        );
        assert_eq!(
            bs58::encode(key.seed(&leader)).into_string(),
            "G1gmFe1QUM8nhDApk6BqvPgw3TQV2Qc5bpKppa96qbVb"
        );
    }

    fn verify_shred_layout(shred: &Shred, packet: &Packet) {
        let data = layout::get_shred(packet).unwrap();
        assert_eq!(data, packet.data(..).unwrap());
        assert_eq!(layout::get_slot(data), Some(shred.slot()));
        assert_eq!(layout::get_index(data), Some(shred.index()));
        assert_eq!(layout::get_version(data), Some(shred.version()));
        assert_eq!(layout::get_shred_id(data), Some(shred.id()));
        assert_eq!(layout::get_signature(data), Some(*shred.signature()));
        assert_eq!(layout::get_shred_type(data).unwrap(), shred.shred_type());
        match shred.shred_type() {
            ShredType::Code => {
                assert_matches!(
                    layout::get_reference_tick(data),
                    Err(Error::InvalidShredType)
                );
            }
            ShredType::Data => {
                assert_eq!(
                    layout::get_reference_tick(data).unwrap(),
                    shred.reference_tick()
                );
                let parent_offset = layout::get_parent_offset(data).unwrap();
                let slot = layout::get_slot(data).unwrap();
                let parent = slot.checked_sub(Slot::from(parent_offset)).unwrap();
                assert_eq!(parent, shred.parent().unwrap());
            }
        }
    }

    #[test]
    fn test_serde_compat_shred_data() {
        const SEED: &str = "6qG9NGWEtoTugS4Zgs46u8zTccEJuRHtrNMiUayLHCxt";
        const PAYLOAD: &str = "hNX8YgJCQwSFGJkZ6qZLiepwPjpctC9UCsMD1SNNQurBXv\
        rm7KKfLmPRMM9CpWHt6MsJuEWpDXLGwH9qdziJzGKhBMfYH63avcchjdaUiMqzVip7cUD\
        kqZ9zZJMrHCCUDnxxKMupsJWKroUSjKeo7hrug2KfHah85VckXpRna4R9QpH7tf2WVBTD\
        M4m3EerctsEQs8eZaTRxzTVkhtJYdNf74KZbH58dc3Yn2qUxF1mexWoPS6L5oZBatx";
        let mut rng = {
            let seed = <[u8; 32]>::try_from(bs58_decode(SEED)).unwrap();
            ChaChaRng::from_seed(seed)
        };
        let mut data = [0u8; legacy::ShredData::CAPACITY];
        rng.fill(&mut data[..]);

        let mut seed = [0u8; Keypair::SECRET_KEY_LENGTH];
        rng.fill(&mut seed[..]);
        let keypair = keypair_from_seed(&seed).unwrap();
        let mut shred = Shred::new_from_data(
            141939602, // slot
            28685,     // index
            36390,     // parent_offset
            &data,     // data
            ShredFlags::LAST_SHRED_IN_SLOT,
            37,    // reference_tick
            45189, // version
            28657, // fec_set_index
        );
        shred.sign(&keypair);
        assert!(shred.verify(&keypair.pubkey()));
        assert_matches!(shred.sanitize(), Ok(()));
        let mut payload = bs58_decode(PAYLOAD);
        payload.extend({
            let skip = payload.len() - SIZE_OF_DATA_SHRED_HEADERS;
            data.iter().skip(skip).copied()
        });
        let mut packet = Packet::default();
        packet.buffer_mut()[..payload.len()].copy_from_slice(&payload);
        packet.meta_mut().size = payload.len();
        assert_eq!(shred.bytes_to_store(), payload);
        assert_eq!(shred, Shred::new_from_serialized_shred(payload).unwrap());
        verify_shred_layout(&shred, &packet);
    }

    #[test]
    fn test_serde_compat_shred_data_empty() {
        const SEED: &str = "E3M5hm8yAEB7iPhQxFypAkLqxNeZCTuGBDMa8Jdrghoo";
        const PAYLOAD: &str = "nRNFVBEsV9FEM5KfmsCXJsgELRSkCV55drTavdy5aZPnsp\
        B8WvsgY99ZuNHDnwkrqe6Lx7ARVmercwugR5HwDcLA9ivKMypk9PNucDPLs67TXWy6k9R\
        ozKmy";
        let mut rng = {
            let seed = <[u8; 32]>::try_from(bs58_decode(SEED)).unwrap();
            ChaChaRng::from_seed(seed)
        };
        let mut seed = [0u8; Keypair::SECRET_KEY_LENGTH];
        rng.fill(&mut seed[..]);
        let keypair = keypair_from_seed(&seed).unwrap();
        let mut shred = Shred::new_from_data(
            142076266, // slot
            21443,     // index
            51279,     // parent_offset
            &[],       // data
            ShredFlags::DATA_COMPLETE_SHRED,
            49,    // reference_tick
            59445, // version
            21414, // fec_set_index
        );
        shred.sign(&keypair);
        assert!(shred.verify(&keypair.pubkey()));
        assert_matches!(shred.sanitize(), Ok(()));
        let payload = bs58_decode(PAYLOAD);
        let mut packet = Packet::default();
        packet.buffer_mut()[..payload.len()].copy_from_slice(&payload);
        packet.meta_mut().size = payload.len();
        assert_eq!(shred.bytes_to_store(), payload);
        assert_eq!(shred, Shred::new_from_serialized_shred(payload).unwrap());
        verify_shred_layout(&shred, &packet);
    }

    #[test]
    fn test_serde_compat_shred_code() {
        const SEED: &str = "4jfjh3UZVyaEgvyG9oQmNyFY9yHDmbeH9eUhnBKkrcrN";
        const PAYLOAD: &str = "3xGsXwzkPpLFuKwbbfKMUxt1B6VqQPzbvvAkxRNCX9kNEP\
        sa2VifwGBtFuNm3CWXdmQizDz5vJjDHu6ZqqaBCSfrHurag87qAXwTtjNPhZzKEew5pLc\
        aY6cooiAch2vpfixNYSDjnirozje5cmUtGuYs1asXwsAKSN3QdWHz3XGParWkZeUMAzRV\
        1UPEDZ7vETKbxeNixKbzZzo47Lakh3C35hS74ocfj23CWoW1JpkETkXjUpXcfcv6cS";
        let mut rng = {
            let seed = <[u8; 32]>::try_from(bs58_decode(SEED)).unwrap();
            ChaChaRng::from_seed(seed)
        };
        let mut parity_shard = vec![0u8; legacy::SIZE_OF_ERASURE_ENCODED_SLICE];
        rng.fill(&mut parity_shard[..]);
        let mut seed = [0u8; Keypair::SECRET_KEY_LENGTH];
        rng.fill(&mut seed[..]);
        let keypair = keypair_from_seed(&seed).unwrap();
        let mut shred = Shred::new_from_parity_shard(
            141945197, // slot
            23418,     // index
            &parity_shard,
            21259, // fec_set_index
            32,    // num_data_shreds
            58,    // num_coding_shreds
            43,    // position
            47298, // version
        );
        shred.sign(&keypair);
        assert!(shred.verify(&keypair.pubkey()));
        assert_matches!(shred.sanitize(), Ok(()));
        let mut payload = bs58_decode(PAYLOAD);
        payload.extend({
            let skip = payload.len() - SIZE_OF_CODING_SHRED_HEADERS;
            parity_shard.iter().skip(skip).copied()
        });
        let mut packet = Packet::default();
        packet.buffer_mut()[..payload.len()].copy_from_slice(&payload);
        packet.meta_mut().size = payload.len();
        assert_eq!(shred.bytes_to_store(), payload);
        assert_eq!(shred, Shred::new_from_serialized_shred(payload).unwrap());
        verify_shred_layout(&shred, &packet);
    }

    #[test]
    fn test_shred_flags() {
        fn make_shred(is_last_data: bool, is_last_in_slot: bool, reference_tick: u8) -> Shred {
            let flags = if is_last_in_slot {
                assert!(is_last_data);
                ShredFlags::LAST_SHRED_IN_SLOT
            } else if is_last_data {
                ShredFlags::DATA_COMPLETE_SHRED
            } else {
                ShredFlags::empty()
            };
            Shred::new_from_data(
                0,   // slot
                0,   // index
                0,   // parent_offset
                &[], // data
                flags,
                reference_tick,
                0, // version
                0, // fec_set_index
            )
        }
        fn check_shred_flags(
            shred: &Shred,
            is_last_data: bool,
            is_last_in_slot: bool,
            reference_tick: u8,
        ) {
            assert_eq!(shred.data_complete(), is_last_data);
            assert_eq!(shred.last_in_slot(), is_last_in_slot);
            assert_eq!(shred.reference_tick(), reference_tick.min(63u8));
            assert_eq!(
                layout::get_reference_tick(shred.payload()).unwrap(),
                reference_tick.min(63u8),
            );
        }
        for is_last_data in [false, true] {
            for is_last_in_slot in [false, true] {
                // LAST_SHRED_IN_SLOT also implies DATA_COMPLETE_SHRED. So it
                // cannot be LAST_SHRED_IN_SLOT if not DATA_COMPLETE_SHRED.
                let is_last_in_slot = is_last_in_slot && is_last_data;
                for reference_tick in [0, 37, 63, 64, 80, 128, 255] {
                    let mut shred = make_shred(is_last_data, is_last_in_slot, reference_tick);
                    check_shred_flags(&shred, is_last_data, is_last_in_slot, reference_tick);
                    shred.set_last_in_slot();
                    check_shred_flags(&shred, true, true, reference_tick);
                }
            }
        }
    }

    #[test]
    fn test_shred_flags_serde() {
        let flags: ShredFlags = bincode::deserialize(&[0b0001_0101]).unwrap();
        assert_eq!(flags, ShredFlags::from_bits(0b0001_0101).unwrap());
        assert!(!flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        assert_eq!((flags & ShredFlags::SHRED_TICK_REFERENCE_MASK).bits(), 21u8);
        assert_eq!(bincode::serialize(&flags).unwrap(), [0b0001_0101]);

        let flags: ShredFlags = bincode::deserialize(&[0b0111_0001]).unwrap();
        assert_eq!(flags, ShredFlags::from_bits(0b0111_0001).unwrap());
        assert!(flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        assert_eq!((flags & ShredFlags::SHRED_TICK_REFERENCE_MASK).bits(), 49u8);
        assert_eq!(bincode::serialize(&flags).unwrap(), [0b0111_0001]);

        let flags: ShredFlags = bincode::deserialize(&[0b1110_0101]).unwrap();
        assert_eq!(flags, ShredFlags::from_bits(0b1110_0101).unwrap());
        assert!(flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        assert_eq!((flags & ShredFlags::SHRED_TICK_REFERENCE_MASK).bits(), 37u8);
        assert_eq!(bincode::serialize(&flags).unwrap(), [0b1110_0101]);

        let flags: ShredFlags = bincode::deserialize(&[0b1011_1101]).unwrap();
        assert_eq!(flags, ShredFlags::from_bits(0b1011_1101).unwrap());
        assert!(!flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        assert_eq!((flags & ShredFlags::SHRED_TICK_REFERENCE_MASK).bits(), 61u8);
        assert_eq!(bincode::serialize(&flags).unwrap(), [0b1011_1101]);
    }

    // Verifies that LAST_SHRED_IN_SLOT also implies DATA_COMPLETE_SHRED.
    #[test]
    fn test_shred_flags_data_complete() {
        let mut flags = ShredFlags::empty();
        assert!(!flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        flags.insert(ShredFlags::LAST_SHRED_IN_SLOT);
        assert!(flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));

        let mut flags = ShredFlags::from_bits(0b0011_1111).unwrap();
        assert!(!flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        flags |= ShredFlags::LAST_SHRED_IN_SLOT;
        assert!(flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));

        let mut flags: ShredFlags = bincode::deserialize(&[0b1011_1111]).unwrap();
        assert!(!flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(!flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
        flags.insert(ShredFlags::LAST_SHRED_IN_SLOT);
        assert!(flags.contains(ShredFlags::DATA_COMPLETE_SHRED));
        assert!(flags.contains(ShredFlags::LAST_SHRED_IN_SLOT));
    }

    #[test_case(false, false)]
    #[test_case(false, true)]
    #[test_case(true, false)]
    #[test_case(true, true)]
    fn test_is_shred_duplicate(chained: bool, is_last_in_slot: bool) {
        fn fill_retransmitter_signature<R: Rng>(
            rng: &mut R,
            shred: Shred,
            chained: bool,
            is_last_in_slot: bool,
        ) -> Shred {
            let mut shred = shred.into_payload();
            let mut signature = [0u8; SIGNATURE_BYTES];
            rng.fill(&mut signature[..]);
            let out = layout::set_retransmitter_signature(&mut shred, &Signature::from(signature));
            if chained && is_last_in_slot {
                assert_matches!(out, Ok(()));
            } else {
                assert_matches!(out, Err(Error::InvalidShredVariant));
            }
            Shred::new_from_serialized_shred(shred).unwrap()
        }
        let mut rng = rand::thread_rng();
        let slot = 285_376_049 + rng.gen_range(0..100_000);
        let shreds: Vec<_> = make_merkle_shreds_for_tests(
            &mut rng,
            slot,
            1200 * 5, // data_size
            chained,
            is_last_in_slot,
        )
        .unwrap()
        .into_iter()
        .map(Shred::from)
        .map(|shred| fill_retransmitter_signature(&mut rng, shred, chained, is_last_in_slot))
        .collect();
        {
            let num_data_shreds = shreds.iter().filter(|shred| shred.is_data()).count();
            let num_coding_shreds = shreds.iter().filter(|shred| shred.is_code()).count();
            assert!(num_data_shreds > if is_last_in_slot { 31 } else { 5 });
            assert!(num_coding_shreds > if is_last_in_slot { 31 } else { 20 });
        }
        // Shreds of different (slot, index, shred-type) are not duplicate.
        // A shred is not a duplicate of itself either.
        for shred in &shreds {
            for other in &shreds {
                assert!(!shred.is_shred_duplicate(other));
            }
        }
        // Different retransmitter signature does not make shreds duplicate.
        for shred in &shreds {
            let other =
                fill_retransmitter_signature(&mut rng, shred.clone(), chained, is_last_in_slot);
            if chained && is_last_in_slot {
                assert_ne!(shred.payload(), other.payload());
            }
            assert!(!shred.is_shred_duplicate(&other));
            assert!(!other.is_shred_duplicate(shred));
        }
        // Shreds of the same (slot, index, shred-type) with different payload
        // (ignoring retransmitter signature) are duplicate.
        for shred in &shreds {
            let mut other = shred.payload().clone();
            other[90] = other[90].wrapping_add(1);
            let other = Shred::new_from_serialized_shred(other).unwrap();
            assert_ne!(shred.payload(), other.payload());
            assert_eq!(
                layout::get_retransmitter_signature(shred.payload()).ok(),
                layout::get_retransmitter_signature(other.payload()).ok()
            );
            assert!(shred.is_shred_duplicate(&other));
            assert!(other.is_shred_duplicate(shred));
        }
    }
}
