//! Deserializes PacketBundles
use {
    crate::{
        banking_stage::{
            immutable_deserialized_packet::ImmutableDeserializedPacket,
            packet_filter::PacketFilterFailure,
        },
        immutable_deserialized_bundle::{DeserializedBundleError, ImmutableDeserializedBundle},
        packet_bundle::PacketBundle,
    },
    crossbeam_channel::{Receiver, RecvTimeoutError},
    solana_sdk::saturating_add_assign,
    std::time::{Duration, Instant},
};

/// Results from deserializing packet batches.
#[derive(Debug)]
pub struct ReceiveBundleResults {
    /// Deserialized bundles from all received bundle packets
    pub deserialized_bundles: Vec<ImmutableDeserializedBundle>,
    /// Number of dropped bundles
    pub num_dropped_bundles: usize,
}

pub struct BundlePacketDeserializer {
    /// Receiver for bundle packets
    bundle_packet_receiver: Receiver<Vec<PacketBundle>>,
    /// Max packets per bundle
    max_packets_per_bundle: Option<usize>,
}

impl BundlePacketDeserializer {
    pub fn new(
        bundle_packet_receiver: Receiver<Vec<PacketBundle>>,
        max_packets_per_bundle: Option<usize>,
    ) -> Self {
        Self {
            bundle_packet_receiver,
            max_packets_per_bundle,
        }
    }

    /// Handles receiving bundles and deserializing them
    pub fn receive_bundles(
        &self,
        recv_timeout: Duration,
        capacity: usize,
        packet_filter: &impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> Result<ReceiveBundleResults, RecvTimeoutError> {
        let (bundle_count, _packet_count, mut bundles) =
            self.receive_until(recv_timeout, capacity)?;

        Ok(Self::deserialize_and_collect_bundles(
            bundle_count,
            &mut bundles,
            self.max_packets_per_bundle,
            packet_filter,
        ))
    }

    /// Deserialize packet batches, aggregates tracer packet stats, and collect
    /// them into ReceivePacketResults
    fn deserialize_and_collect_bundles(
        bundle_count: usize,
        bundles: &mut [PacketBundle],
        max_packets_per_bundle: Option<usize>,
        packet_filter: &impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> ReceiveBundleResults {
        let mut deserialized_bundles = Vec::with_capacity(bundle_count);
        let mut num_dropped_bundles: usize = 0;

        for bundle in bundles.iter_mut() {
            match Self::deserialize_bundle(bundle, max_packets_per_bundle, packet_filter) {
                Ok(deserialized_bundle) => {
                    deserialized_bundles.push(deserialized_bundle);
                }
                Err(_) => {
                    saturating_add_assign!(num_dropped_bundles, 1);
                }
            }
        }

        ReceiveBundleResults {
            deserialized_bundles,
            num_dropped_bundles,
        }
    }

    /// Receives bundle packets
    fn receive_until(
        &self,
        recv_timeout: Duration,
        bundle_count_upperbound: usize,
    ) -> Result<(usize, usize, Vec<PacketBundle>), RecvTimeoutError> {
        let start = Instant::now();

        let mut bundles = self.bundle_packet_receiver.recv_timeout(recv_timeout)?;
        let mut num_packets_received: usize = bundles.iter().map(|pb| pb.batch.len()).sum();
        let mut num_bundles_received: usize = bundles.len();

        if num_bundles_received <= bundle_count_upperbound {
            while let Ok(bundle_packets) = self.bundle_packet_receiver.try_recv() {
                trace!("got more packet batches in bundle packet deserializer");

                saturating_add_assign!(
                    num_packets_received,
                    bundle_packets
                        .iter()
                        .map(|pb| pb.batch.len())
                        .sum::<usize>()
                );
                saturating_add_assign!(num_bundles_received, bundle_packets.len());

                bundles.extend(bundle_packets);

                if start.elapsed() >= recv_timeout
                    || num_bundles_received >= bundle_count_upperbound
                {
                    break;
                }
            }
        }

        Ok((num_bundles_received, num_packets_received, bundles))
    }

    /// Deserializes the Bundle into DeserializedBundlePackets, returning None if any packet in the
    /// bundle failed to deserialize
    pub fn deserialize_bundle(
        bundle: &mut PacketBundle,
        max_packets_per_bundle: Option<usize>,
        packet_filter: &impl Fn(
            ImmutableDeserializedPacket,
        ) -> Result<ImmutableDeserializedPacket, PacketFilterFailure>,
    ) -> Result<ImmutableDeserializedBundle, DeserializedBundleError> {
        ImmutableDeserializedBundle::new(bundle, max_packets_per_bundle, packet_filter)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crossbeam_channel::unbounded,
        solana_ledger::genesis_utils::create_genesis_config,
        solana_perf::packet::PacketBatch,
        solana_runtime::genesis_utils::GenesisConfigInfo,
        solana_sdk::{packet::Packet, signature::Signer, system_transaction::transfer},
    };

    #[test]
    fn test_deserialize_and_collect_bundles_empty() {
        let results =
            BundlePacketDeserializer::deserialize_and_collect_bundles(0, &mut [], Some(5), &|p| {
                Ok(p)
            });
        assert_eq!(results.deserialized_bundles.len(), 0);
        assert_eq!(results.num_dropped_bundles, 0);
    }

    #[test]
    fn test_receive_bundles_capacity() {
        solana_logger::setup();

        let GenesisConfigInfo {
            genesis_config,
            mint_keypair,
            ..
        } = create_genesis_config(10_000);
        let (sender, receiver) = unbounded();

        let deserializer = BundlePacketDeserializer::new(receiver, Some(10));

        let packet_bundles: Vec<_> = (0..10)
            .map(|_| PacketBundle {
                batch: PacketBatch::new(vec![Packet::from_data(
                    None,
                    transfer(
                        &mint_keypair,
                        &mint_keypair.pubkey(),
                        100,
                        genesis_config.hash(),
                    ),
                )
                .unwrap()]),
                bundle_id: String::default(),
            })
            .collect();

        sender.send(packet_bundles.clone()).unwrap();

        let bundles = deserializer
            .receive_bundles(Duration::from_millis(100), 5, &Ok)
            .unwrap();
        // this is confusing, but it's sent as one batch
        assert_eq!(bundles.deserialized_bundles.len(), 10);
        assert_eq!(bundles.num_dropped_bundles, 0);

        // make sure empty
        assert_matches!(
            deserializer.receive_bundles(Duration::from_millis(100), 5, &Ok),
            Err(RecvTimeoutError::Timeout)
        );

        // send 2x 10 size batches. capacity is 5, but will return 10 since that's the batch size
        sender.send(packet_bundles.clone()).unwrap();
        sender.send(packet_bundles).unwrap();
        let bundles = deserializer
            .receive_bundles(Duration::from_millis(100), 5, &Ok)
            .unwrap();
        assert_eq!(bundles.deserialized_bundles.len(), 10);
        assert_eq!(bundles.num_dropped_bundles, 0);

        let bundles = deserializer
            .receive_bundles(Duration::from_millis(100), 5, &Ok)
            .unwrap();
        assert_eq!(bundles.deserialized_bundles.len(), 10);
        assert_eq!(bundles.num_dropped_bundles, 0);

        assert_matches!(
            deserializer.receive_bundles(Duration::from_millis(100), 5, &Ok),
            Err(RecvTimeoutError::Timeout)
        );
    }

    #[test]
    fn test_receive_bundles_bad_bundles() {
        solana_logger::setup();
        let (sender, receiver) = unbounded();

        let deserializer = BundlePacketDeserializer::new(receiver, Some(10));

        let packet_bundles: Vec<_> = (0..10)
            .map(|_| PacketBundle {
                batch: PacketBatch::new(vec![]),
                bundle_id: String::default(),
            })
            .collect();
        sender.send(packet_bundles).unwrap();

        let bundles = deserializer
            .receive_bundles(Duration::from_millis(100), 5, &Ok)
            .unwrap();
        // this is confusing, but it's sent as one batch
        assert_eq!(bundles.deserialized_bundles.len(), 0);
        assert_eq!(bundles.num_dropped_bundles, 10);
    }
}
