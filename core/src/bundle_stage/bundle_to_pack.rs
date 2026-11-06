//! Forwards block engine bundles to the external scheduler.

use {
    crate::{
        bundle_stage::bundle_storage::BundleStorageError, packet_bundle::VerifiedPacketBundle,
    },
    agave_scheduler_bindings::{
        BundleToPackMessage, MAX_TRANSACTIONS_PER_BUNDLE, SharableTransactionBatchRegion,
        SharableTransactionRegion,
    },
    rts_alloc::Allocator,
    smallvec::SmallVec,
    std::ptr::NonNull,
};

const MAX_ALLOCATIONS_PER_BUNDLE: usize = MAX_TRANSACTIONS_PER_BUNDLE + 1;

/// Shared memory allocated for one bundle: its transactions and the region array.
type Allocations = SmallVec<[NonNull<u8>; MAX_ALLOCATIONS_PER_BUNDLE]>;

/// Copies bundles into shared memory and hands them to the external scheduler, which frees them.
pub struct BundleToPack {
    allocator: Allocator,
    producer: shaq::spsc::Producer<BundleToPackMessage>,
}

#[cfg(unix)]
impl From<agave_scheduler_handshake::AgaveBundleToPackSession> for BundleToPack {
    fn from(session: agave_scheduler_handshake::AgaveBundleToPackSession) -> Self {
        Self {
            allocator: session.allocator,
            producer: session.producer,
        }
    }
}

impl BundleToPack {
    /// Forwards `bundle`, or says why it is dropped: malformed, as the in-process stage drops
    /// it, or `ContainerFull` when the queue or the shared memory is full.
    pub fn forward(&mut self, bundle: &VerifiedPacketBundle) -> Result<(), BundleStorageError> {
        let packets = bundle.batch();
        if packets.is_empty() {
            return Err(BundleStorageError::EmptyBatch);
        }
        if packets.len() > MAX_TRANSACTIONS_PER_BUNDLE {
            return Err(BundleStorageError::BundleTooLarge);
        }
        // Packets marked for discard have no data.
        let transactions = packets
            .iter()
            .enumerate()
            .map(|(index, packet)| {
                packet
                    .data(..)
                    .ok_or(BundleStorageError::PacketMarkedDiscard(index))
            })
            .collect::<Result<SmallVec<[&[u8]; MAX_TRANSACTIONS_PER_BUNDLE]>, _>>()?;
        // Clean all remote frees in allocator so we have as much room as possible.
        self.allocator.clean_remote_frees();

        let mut allocations = Allocations::new();
        let forwarded = self.try_forward(&transactions, &mut allocations);
        if forwarded.is_none() {
            for allocation in allocations {
                // SAFETY: `allocation` was allocated by `self.allocator` and not freed.
                unsafe { self.allocator.free(allocation) };
            }
        }
        forwarded.ok_or(BundleStorageError::ContainerFull)
    }

    /// On success the external scheduler owns `allocations`, which is left empty; `None` means
    /// shared memory or the queue is full.
    fn try_forward(&mut self, transactions: &[&[u8]], allocations: &mut Allocations) -> Option<()> {
        let mut regions =
            SmallVec::<[SharableTransactionRegion; MAX_TRANSACTIONS_PER_BUNDLE]>::new();
        for &bytes in transactions {
            let allocation = self.allocate(bytes.len(), allocations)?;
            // SAFETY: `allocation` is valid for `bytes.len()` bytes and does not overlap `bytes`.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), allocation.as_ptr(), bytes.len())
            };
            regions.push(SharableTransactionRegion {
                // SAFETY: `allocation` was allocated by `self.allocator`.
                offset: unsafe { self.allocator.offset(allocation) },
                length: bytes.len() as u32,
            });
        }

        let array_size = regions
            .len()
            .checked_mul(size_of::<SharableTransactionRegion>())?;
        let array = self.allocate(array_size, allocations)?;
        // SAFETY: `array` is a fresh allocation sized for `regions.len()` regions, aligned for
        // them like every allocation, and does not overlap `regions`.
        unsafe {
            std::ptr::copy_nonoverlapping(regions.as_ptr(), array.cast().as_ptr(), regions.len())
        };
        let message = BundleToPackMessage {
            bundle: SharableTransactionBatchRegion {
                num_transactions: regions.len() as u8,
                // SAFETY: `array` was allocated by `self.allocator`.
                transactions_offset: unsafe { self.allocator.offset(array) },
            },
        };
        self.producer.try_write(message).ok()?;
        allocations.clear();
        Some(())
    }

    fn allocate(&self, size: usize, allocations: &mut Allocations) -> Option<NonNull<u8>> {
        let allocation = self.allocator.allocate(u32::try_from(size).ok()?)?;
        allocations.push(allocation);
        Some(allocation)
    }
}

#[cfg(all(test, unix))]
mod tests {
    use {
        super::*,
        crate::bundle_stage::{BundleStage, BundleStageLoopMetrics, bundle_storage::BundleStorage},
        agave_scheduler_handshake::{ClientLogon, setup_local_session},
        ahash::HashSet,
        crossbeam_channel::unbounded,
        solana_genesis_config::GenesisConfig,
        solana_perf::{
            packet::{BytesPacket, PacketBatch},
            test_tx::test_tx,
        },
        solana_runtime::bank::Bank,
    };

    fn setup() -> (
        BundleToPack,
        Allocator,
        shaq::spsc::Consumer<BundleToPackMessage>,
    ) {
        let (agave, client) = setup_local_session(ClientLogon {
            worker_count: 1,
            allocator_size: 64 * 1024 * 1024,
            allocator_handles: 1,
            tpu_to_pack_capacity: 1024,
            progress_tracker_capacity: 256,
            pack_to_worker_capacity: 1024,
            worker_to_pack_capacity: 1024,
            bundle_to_pack_capacity: 1024,
            ..ClientLogon::default()
        })
        .unwrap();
        (
            agave.bundle_to_pack.unwrap().into(),
            client.allocator,
            client.bundle_to_pack.unwrap(),
        )
    }

    fn transactions(bundle: &VerifiedPacketBundle) -> Vec<Vec<u8>> {
        bundle
            .batch()
            .iter()
            .map(|packet| packet.data(..).unwrap().to_vec())
            .collect()
    }

    /// Reads the next forwarded bundle's transactions and frees them.
    fn read_bundle(
        allocator: &Allocator,
        consumer: &mut shaq::spsc::Consumer<BundleToPackMessage>,
    ) -> Vec<Vec<u8>> {
        let message = consumer.try_read().unwrap();
        // SAFETY: the offsets were produced by the shared allocator, and each region is read
        // before it is freed.
        unsafe {
            let regions = allocator
                .ptr_from_offset(message.bundle.transactions_offset)
                .cast::<SharableTransactionRegion>();
            let transactions = (0..usize::from(message.bundle.num_transactions))
                .map(|index| {
                    let region = regions.add(index).read();
                    let bytes = std::slice::from_raw_parts(
                        allocator.ptr_from_offset(region.offset).as_ptr(),
                        region.length as usize,
                    )
                    .to_vec();
                    allocator.free_offset(region.offset);
                    bytes
                })
                .collect();
            allocator.free_offset(message.bundle.transactions_offset);
            transactions
        }
    }

    fn packet() -> BytesPacket {
        BytesPacket::from_data(test_tx()).unwrap()
    }

    fn bundle(packets: Vec<BytesPacket>) -> VerifiedPacketBundle {
        VerifiedPacketBundle::new(PacketBatch::from(packets))
    }

    #[test]
    fn test_forward() {
        let (mut forwarder, allocator, mut consumer) = setup();
        let bundle = bundle(vec![packet(), packet()]);
        forwarder.forward(&bundle).unwrap();

        assert_eq!(
            read_bundle(&allocator, &mut consumer),
            transactions(&bundle)
        );
        assert!(consumer.try_read().is_none());
    }

    #[test]
    fn test_forward_drops_invalid_bundles() {
        let (mut forwarder, _allocator, mut consumer) = setup();
        let mut discarded = packet();
        discarded.meta_mut().set_discard(true);
        for (packets, error) in [
            (vec![], BundleStorageError::EmptyBatch),
            (
                (0..MAX_ALLOCATIONS_PER_BUNDLE).map(|_| packet()).collect(),
                BundleStorageError::BundleTooLarge,
            ),
            (
                vec![packet(), discarded],
                BundleStorageError::PacketMarkedDiscard(1),
            ),
        ] {
            assert_eq!(forwarder.forward(&bundle(packets)), Err(error));
            assert!(consumer.try_read().is_none());
        }
    }

    #[test]
    fn test_receive_forwards_instead_of_buffering() {
        let (mut forwarder, _allocator, mut consumer) = setup();
        let (_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&GenesisConfig::default());
        let (sender, mut receiver) = unbounded();
        sender.send(bundle(vec![packet()])).unwrap();
        let mut storage = BundleStorage::with_capacity(2);

        BundleStage::receive_and_buffer_bundles(
            &bank_forks,
            &mut receiver,
            &mut storage,
            &HashSet::default(),
            &mut BundleStageLoopMetrics::default(),
            Some(&mut forwarder),
        )
        .unwrap();

        assert_eq!(storage.unprocessed_bundles_len(), 0);
        assert!(consumer.try_read().is_some());
    }

    #[test]
    fn test_full_queue_drops_bundles() {
        let (mut forwarder, _allocator, _consumer) = setup();
        let (_bank, bank_forks) = Bank::new_with_bank_forks_for_tests(&GenesisConfig::default());
        let (sender, mut receiver) = unbounded();
        let mut storage = BundleStorage::with_capacity(16);
        let mut metrics = BundleStageLoopMetrics::default();

        let filled = (0..=forwarder.producer.capacity())
            .take_while(|_| forwarder.forward(&bundle(vec![packet()])).is_ok())
            .count();
        assert!(filled > 0);
        assert_eq!(
            forwarder.forward(&bundle(vec![packet()])),
            Err(BundleStorageError::ContainerFull)
        );

        // A refused arrival is dropped and counted, not buffered.
        sender.send(bundle(vec![packet()])).unwrap();
        BundleStage::receive_and_buffer_bundles(
            &bank_forks,
            &mut receiver,
            &mut storage,
            &HashSet::default(),
            &mut metrics,
            Some(&mut forwarder),
        )
        .unwrap();
        assert_eq!(storage.unprocessed_bundles_len(), 0);
        assert_eq!(metrics.num_bundles_dropped_container_full.0, 1);
        assert_eq!(metrics.num_bundles_dropped.0, 1);
    }
}
