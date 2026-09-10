//! Opt-in Jito messages carried alongside the unchanged Agave scheduler ABI.
//!
//! The validator allocates BAM/legacy ingress batches. The scheduler forwards the
//! original batch and its result allocation on the completion queue; the validator
//! frees both after receiving completion. A failed queue write does not transfer
//! ownership. Worker execution replies do not free input batches. The scheduler
//! owns and frees TPU inputs itself. Regions must remain live and immutable while
//! another process uses them. Sessions are trusted peers, not a memory sandbox.

use {
    agave_scheduler_bindings::{
        MAX_TRANSACTIONS_PER_MESSAGE, ProgressMessage, SharableTransactionBatchRegion,
        SharableTransactionRegion,
    },
    rts_alloc::Allocator,
};

pub const JITO_PROTOCOL_VERSION: u16 = 1;
/// Reserved completion ID, sent at least every 500 ms while the scheduler is live.
/// Heartbeats have an empty batch and default response region; ingress IDs start at 1.
pub const HEARTBEAT_ID: u64 = 0;
pub const SOURCE_TPU: u8 = 0;
pub const SOURCE_BAM: u8 = 1;
pub const SOURCE_LEGACY_BUNDLE: u8 = 2;
pub const SOURCE_VOTE: u8 = 3;

pub mod execution_flags {
    pub const DROP_ON_FAILURE: u8 =
        agave_scheduler_bindings::execution_message_flags::DROP_ON_FAILURE as u8;
    pub const ALL_OR_NOTHING: u8 =
        agave_scheduler_bindings::execution_message_flags::ALL_OR_NOTHING as u8;
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct JitoIngressMessage {
    /// Authenticated BAM stream generation; non-BAM inputs use zero.
    pub bam_generation: u64,
    pub id: u64,
    pub source: u8,
    pub flags: u8,
    pub max_slot: u64,
    pub batch: SharableTransactionBatchRegion,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct JitoProgressMessage {
    /// Authenticated BAM stream generation; non-BAM inputs use zero.
    pub bam_generation: u64,
    pub progress: ProgressMessage,
    pub bank_id: u64,
    pub atomic_batches_enabled: u8,
    pub bam_connected: u8,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct JitoExecutionRequest {
    /// Authenticated BAM stream generation; non-BAM inputs use zero.
    pub bam_generation: u64,
    pub id: u64,
    pub source: u8,
    pub flags: u8,
    pub slot: u64,
    pub bank_id: u64,
    pub batch: SharableTransactionBatchRegion,
}

#[derive(Clone, Copy, Debug)]
#[repr(C)]
pub struct JitoExecutionResponse {
    pub id: u64,
    pub batch: SharableTransactionBatchRegion,
    pub processed_code: u8,
    pub execution_slot: u64,
    pub bank_id: u64,
    pub responses: JitoResponseRegion,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct SharedBytes {
    pub offset: usize,
    pub length: u32,
}

/// One result-array allocation owning each nonempty diagnostic allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct JitoResponseRegion {
    pub num_transaction_responses: u8,
    pub transaction_responses_offset: usize,
    pub allocation_size: u32,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(C)]
pub struct JitoTransactionResult {
    pub executed_units: u64,
    pub fee_payer_balance: u64,
    /// Exact bincode-serialized `TransactionError` for transaction/translation errors.
    /// Empty for lifecycle, unavailable-bank, ParentReady, or recorder failures.
    pub error: SharedBytes,
    pub loaded_accounts_data_size: u32,
    pub not_included_reason: u8,
    pub execution_success: u8,
}

/// Allocate results with exact diagnostics in separately owned allocations.
/// Failure rolls back every allocation. Supplied `error` descriptors are replaced.
/// Each diagnostic must fit one allocator allocation (`MAX_ALLOC_SIZE` bytes).
pub fn allocate_results(
    allocator: &Allocator,
    results: &[(JitoTransactionResult, &[u8])],
) -> Option<JitoResponseRegion> {
    if results.is_empty() || results.len() > MAX_TRANSACTIONS_PER_MESSAGE {
        return None;
    }
    let array_size = results
        .len()
        .checked_mul(size_of::<JitoTransactionResult>())?;
    let allocation_size = u32::try_from(array_size).ok()?;
    let ptr = allocator.allocate(allocation_size)?;
    let mut diagnostics = Vec::with_capacity(results.len());
    for (index, (result, error)) in results.iter().enumerate() {
        let mut result = *result;
        result.error = SharedBytes::default();
        if !error.is_empty() {
            let allocation = u32::try_from(error.len())
                .ok()
                .and_then(|size| allocator.allocate(size));
            let Some(diagnostic) = allocation else {
                for diagnostic in diagnostics {
                    // SAFETY: all allocations are fresh, unpublished, and exclusively owned.
                    unsafe { allocator.free(diagnostic) };
                }
                // SAFETY: the array is fresh and unpublished.
                unsafe { allocator.free(ptr) };
                return None;
            };
            // SAFETY: fresh allocation has exactly the requested capacity.
            unsafe {
                std::ptr::copy_nonoverlapping(error.as_ptr(), diagnostic.as_ptr(), error.len())
            };
            result.error = SharedBytes {
                // SAFETY: diagnostic came from this allocator.
                offset: unsafe { allocator.offset(diagnostic) },
                length: error.len() as u32,
            };
            diagnostics.push(diagnostic);
        }
        // SAFETY: the array was sized for all results and allocator alignment suffices.
        unsafe { ptr.cast::<JitoTransactionResult>().add(index).write(result) };
    }
    Some(JitoResponseRegion {
        num_transaction_responses: results.len() as u8,
        // SAFETY: ptr came from this allocator.
        transaction_responses_offset: unsafe { allocator.offset(ptr) },
        allocation_size,
    })
}

/// Copy a result region and all exact diagnostics after checking declared sizes.
///
/// # Safety
/// The region and every nonempty error descriptor must name live allocations
/// from `allocator`, owned by the caller and not concurrently modified. The
/// allocations must be at least their declared sizes. Bounds checks cannot
/// authenticate arbitrary shared-memory offsets or allocation sizes.
pub unsafe fn read_results(
    allocator: &Allocator,
    region: &JitoResponseRegion,
) -> Option<Vec<(JitoTransactionResult, Vec<u8>)>> {
    let count = usize::from(region.num_transaction_responses);
    if count == 0 || count > MAX_TRANSACTIONS_PER_MESSAGE {
        return None;
    }
    let array_size = count.checked_mul(size_of::<JitoTransactionResult>())?;
    let size = usize::try_from(region.allocation_size).ok()?;
    if array_size != size || size > rts_alloc::MAX_ALLOC_SIZE {
        return None;
    }
    // SAFETY: caller guarantees the array is live and sufficiently sized.
    let ptr = unsafe { allocator.ptr_from_offset(region.transaction_responses_offset) };
    if !ptr.cast::<JitoTransactionResult>().is_aligned() {
        return None;
    }
    let mut results = Vec::with_capacity(count);
    for index in 0..count {
        // SAFETY: array_size matches the valid allocation size.
        let result = unsafe { ptr.cast::<JitoTransactionResult>().add(index).read() };
        let length = result.error.length as usize;
        if length > rts_alloc::MAX_ALLOC_SIZE {
            return None;
        }
        let error = if length == 0 {
            Vec::new()
        } else {
            // SAFETY: caller guarantees every diagnostic allocation's provenance and size.
            unsafe {
                std::slice::from_raw_parts(
                    allocator.ptr_from_offset(result.error.offset).as_ptr(),
                    length,
                )
            }
            .to_vec()
        };
        results.push((result, error));
    }
    Some(results)
}

/// Free the result array and every diagnostic allocation it owns exactly once.
///
/// # Safety
/// The region must have been produced by `allocate_results` in this allocator,
/// with unmodified descriptors. Caller exclusively owns all allocations and no
/// peer may still read them. The region must not have been freed previously.
pub unsafe fn free_results(allocator: &Allocator, region: JitoResponseRegion) {
    if region.allocation_size == 0 {
        return;
    }
    // SAFETY: the caller guarantees allocator provenance and exclusive ownership.
    unsafe {
        let ptr = allocator.ptr_from_offset(region.transaction_responses_offset);
        for index in 0..usize::from(region.num_transaction_responses) {
            let result = ptr.cast::<JitoTransactionResult>().add(index).read();
            if result.error.length != 0 {
                allocator.free(allocator.ptr_from_offset(result.error.offset));
            }
        }
        allocator.free(ptr);
    }
}

/// Allocate a batch descriptor and transaction bytes, rolling back on failure.
pub fn allocate_batch(
    allocator: &Allocator,
    transactions: &[impl AsRef<[u8]>],
) -> Option<SharableTransactionBatchRegion> {
    if transactions.is_empty() || transactions.len() > MAX_TRANSACTIONS_PER_MESSAGE {
        return None;
    }
    // Agave's TransactionPtrBatch derives its metadata pointer after a full
    // MAX_TRANSACTIONS_PER_MESSAGE descriptor array, even with zero-sized metadata.
    let size = MAX_TRANSACTIONS_PER_MESSAGE.checked_mul(size_of::<SharableTransactionRegion>())?;
    let descriptor = allocator.allocate(u32::try_from(size).ok()?)?;
    let mut allocated = Vec::with_capacity(transactions.len());
    for (index, transaction) in transactions.iter().enumerate() {
        let bytes = transaction.as_ref();
        let ptr = u32::try_from(bytes.len()).ok().and_then(|size| {
            if size == 0 {
                None
            } else {
                allocator.allocate(size)
            }
        });
        let Some(ptr) = ptr else {
            for ptr in allocated {
                // SAFETY: these fresh allocations have not been published or freed.
                unsafe { allocator.free(ptr) };
            }
            // SAFETY: descriptor is fresh and exclusively owned.
            unsafe { allocator.free(descriptor) };
            return None;
        };
        // SAFETY: both allocations are fresh and sized for their contents.
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), bytes.len());
            descriptor
                .cast::<SharableTransactionRegion>()
                .add(index)
                .write(SharableTransactionRegion {
                    offset: allocator.offset(ptr),
                    length: bytes.len() as u32,
                });
        }
        allocated.push(ptr);
    }
    Some(SharableTransactionBatchRegion {
        num_transactions: transactions.len() as u8,
        // SAFETY: descriptor was allocated above from this allocator.
        transactions_offset: unsafe { allocator.offset(descriptor) },
    })
}

/// Free the descriptor and every transaction it owns.
///
/// # Safety
/// `batch` must be an exclusively owned live batch allocated by `allocate_batch`
/// from this allocator. No peer may still access any constituent allocation.
pub unsafe fn free_batch(allocator: &Allocator, batch: SharableTransactionBatchRegion) {
    // SAFETY: caller guarantees live batch provenance and exclusive ownership.
    unsafe {
        let ptr = allocator.ptr_from_offset(batch.transactions_offset);
        for index in 0..usize::from(batch.num_transactions) {
            let transaction = ptr.cast::<SharableTransactionRegion>().add(index).read();
            allocator.free(allocator.ptr_from_offset(transaction.offset));
        }
        allocator.free(ptr);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allocator() -> Allocator {
        let file = tempfile::tempfile().unwrap();
        // SAFETY: file is newly created and uniquely initialized.
        unsafe { Allocator::create(&file, 64 * 1024 * 1024, 1, 2 * 1024 * 1024) }.unwrap()
    }

    #[test]
    fn result_roundtrip_and_bounds() {
        let allocator = allocator();
        let result = JitoTransactionResult {
            executed_units: 123,
            loaded_accounts_data_size: 456,
            fee_payer_balance: 789,
            execution_success: 1,
            ..JitoTransactionResult::default()
        };
        let region =
            allocate_results(&allocator, &[(result, b"specific error"), (result, b"")]).unwrap();
        // SAFETY: this test owns the fresh, unmodified allocation.
        let decoded = unsafe { read_results(&allocator, &region) }.unwrap();
        assert_eq!(decoded[0].0.executed_units, 123);
        assert_eq!(decoded[0].0.loaded_accounts_data_size, 456);
        assert_eq!(decoded[0].1, b"specific error");
        assert!(decoded[1].1.is_empty());
        let bad = JitoResponseRegion {
            num_transaction_responses: 64,
            ..region
        };
        // SAFETY: allocation size and provenance are unchanged; decoder rejects the bad count.
        assert!(unsafe { read_results(&allocator, &bad) }.is_none());
        // SAFETY: no other peer holds this allocation.
        unsafe { free_results(&allocator, region) };
    }

    #[test]
    fn maximum_batch_preserves_all_diagnostics() {
        assert!(size_of::<JitoTransactionResult>() <= 40);
        let allocator = allocator();
        let error = "long diagnostic text with unicode 日本語 ".repeat(10);
        let results = vec![
            (JitoTransactionResult::default(), error.as_bytes());
            MAX_TRANSACTIONS_PER_MESSAGE
        ];
        let too_large = vec![0u8; rts_alloc::MAX_ALLOC_SIZE + 1];
        assert!(
            allocate_results(
                &allocator,
                &[
                    (JitoTransactionResult::default(), b"already allocated"),
                    (JitoTransactionResult::default(), &too_large),
                ]
            )
            .is_none()
        );
        let region = allocate_results(&allocator, &results).unwrap();
        assert!(region.allocation_size as usize <= rts_alloc::MAX_ALLOC_SIZE);
        // SAFETY: this test owns the fresh allocation.
        let decoded = unsafe { read_results(&allocator, &region) }.unwrap();
        assert_eq!(decoded.len(), MAX_TRANSACTIONS_PER_MESSAGE);
        for (_, diagnostic) in decoded {
            assert_eq!(diagnostic, error.as_bytes());
        }
        // SAFETY: no other peer holds this allocation.
        unsafe { free_results(&allocator, region) };
    }

    #[test]
    fn batch_roundtrip_and_failed_allocation_cleanup() {
        let allocator = allocator();
        assert!(allocate_batch(&allocator, &[b"".as_slice()]).is_none());
        assert!(allocate_batch(&allocator, &[b"valid".as_slice(), b"".as_slice()]).is_none());
        let batch =
            allocate_batch(&allocator, &[b"first".as_slice(), b"second".as_slice()]).unwrap();
        assert_eq!(batch.num_transactions, 2);
        // SAFETY: fresh descriptor has room for all 64 headers, including the one-past pointer.
        unsafe {
            let ptr = allocator.ptr_from_offset(batch.transactions_offset);
            let _end = ptr
                .cast::<SharableTransactionRegion>()
                .add(MAX_TRANSACTIONS_PER_MESSAGE);
            let first = ptr.cast::<SharableTransactionRegion>().read();
            assert_eq!(
                std::slice::from_raw_parts(
                    allocator.ptr_from_offset(first.offset).as_ptr(),
                    first.length as usize
                ),
                b"first"
            );
            free_batch(&allocator, batch);
        }
    }
}
