use {
    crate::shred::{
        payload::Payload, CodingShredHeader, DataShredHeader, Error, ShredCommonHeader,
    },
    solana_sdk::{clock::Slot, signature::Signature},
};

pub trait Shred<'a>: Sized {
    // Total size of payload including headers, merkle
    // branches (if any), zero paddings, etc.
    const SIZE_OF_PAYLOAD: usize;
    // Size of common and code/data headers.
    const SIZE_OF_HEADERS: usize;

    type SignedData: AsRef<[u8]>;

    fn from_payload<T>(shred: T) -> Result<Self, Error>
    where
        Payload: From<T>;
    fn common_header(&self) -> &ShredCommonHeader;
    fn sanitize(&self) -> Result<(), Error>;

    fn set_signature(&mut self, signature: Signature);

    fn payload(&self) -> &Payload;
    fn into_payload(self) -> Payload;

    // Returns the shard index within the erasure coding set.
    fn erasure_shard_index(&self) -> Result<usize, Error>;
    // Returns the portion of the shred's payload which is erasure coded.
    fn erasure_shard(&self) -> Result<&[u8], Error>;

    // Portion of the payload which is signed.
    fn signed_data(&'a self) -> Result<Self::SignedData, Error>;

    // Only for tests.
    fn set_index(&mut self, index: u32);
    fn set_slot(&mut self, slot: Slot);
}

pub trait ShredData: for<'a> Shred<'a> {
    fn data_header(&self) -> &DataShredHeader;

    fn parent(&self) -> Result<Slot, Error> {
        let slot = self.common_header().slot;
        let parent_offset = self.data_header().parent_offset;
        if parent_offset == 0 && slot != 0 {
            return Err(Error::InvalidParentOffset {
                slot,
                parent_offset,
            });
        }
        slot.checked_sub(Slot::from(parent_offset))
            .ok_or(Error::InvalidParentOffset {
                slot,
                parent_offset,
            })
    }

    fn data(&self) -> Result<&[u8], Error>;
}

pub trait ShredCode: for<'a> Shred<'a> {
    fn coding_header(&self) -> &CodingShredHeader;

    fn first_coding_index(&self) -> Option<u32> {
        let position = u32::from(self.coding_header().position);
        self.common_header().index.checked_sub(position)
    }
}
