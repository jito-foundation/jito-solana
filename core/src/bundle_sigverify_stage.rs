use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, spawn, JoinHandle},
    time::Duration,
};

use crossbeam_channel::{Receiver, RecvTimeoutError, Sender};
use solana_perf::sigverify::ed25519_verify_cpu;

use crate::packet_bundle::{PacketBundle, VerifiedPacketBundle};

pub struct BundleSigverifyStage {
    thread: JoinHandle<()>,
}

impl BundleSigverifyStage {
    pub fn new(
        receiver: Receiver<Vec<PacketBundle>>,
        sender: Sender<VerifiedPacketBundle>,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let thread = spawn(move || Self::sigverify_service(receiver, sender, exit));
        Self { thread }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread.join()
    }

    fn sigverify_service(
        receiver: Receiver<Vec<PacketBundle>>,
        sender: Sender<VerifiedPacketBundle>,
        exit: Arc<AtomicBool>,
    ) {
        let mut workspace = Vec::with_capacity(100);

        while !exit.load(Ordering::Relaxed) {
            let bundles = match receiver.recv_timeout(Duration::from_millis(10)) {
                Ok(bundles) => bundles,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            };

            workspace.extend(bundles.into_iter().map(|bundle| bundle.take()));

            let packet_count = workspace.iter().map(|bundle| bundle.len()).sum();

            ed25519_verify_cpu(&mut workspace, false, packet_count);

            for bundle in workspace.drain(..) {
                // all the transactions in the bundle need to be verified to be valid
                if bundle.iter().all(|packet| !packet.meta().discard()) {
                    if sender.send(VerifiedPacketBundle::new(bundle)).is_err() {
                        warn!("failed to send verified packet bundle");
                        break;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_channel::bounded;
    use solana_keypair::Signature;
    use solana_perf::{
        packet::{BytesPacket, PacketBatch},
        test_tx::test_tx,
    };
    use solana_transaction::Transaction;

    use super::*;

    #[test]
    fn test_bundle_sigverify_stage_exit() {
        let (_unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, _verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));
        let stage = BundleSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());
        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test]
    fn test_bundle_sigverify_stage_many_packets_all_valid() {
        let (unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));

        let txs_1 = (0..3).map(|_| test_tx()).collect::<Vec<_>>();
        let packet_bundle_1 = PacketBundle::new(
            PacketBatch::from(
                txs_1
                    .iter()
                    .map(|tx| BytesPacket::from_data(None, tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        let txs_2 = (0..4).map(|_| test_tx()).collect::<Vec<_>>();
        let packet_bundle_2 = PacketBundle::new(
            PacketBatch::from(
                txs_2
                    .iter()
                    .map(|tx| BytesPacket::from_data(None, tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        unverified_sender
            .send(vec![packet_bundle_1, packet_bundle_2])
            .unwrap();

        let stage = BundleSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());

        let verified_bundle_1 = verified_receiver.recv().unwrap();
        assert_eq!(verified_bundle_1.batch().len(), 3);
        assert!(verified_bundle_1
            .batch()
            .iter()
            .all(|packet| !packet.meta().discard()));
        let txs_1_after: Vec<Transaction> = verified_bundle_1
            .batch()
            .iter()
            .map(|packet| bincode::deserialize(&packet.data(..).unwrap()).unwrap())
            .collect();
        assert_eq!(txs_1, txs_1_after);

        let verified_bundle_2 = verified_receiver.recv().unwrap();
        assert_eq!(verified_bundle_2.batch().len(), 4);
        assert!(verified_bundle_2
            .batch()
            .iter()
            .all(|packet| !packet.meta().discard()));
        let txs_2_after: Vec<Transaction> = verified_bundle_2
            .batch()
            .iter()
            .map(|packet| bincode::deserialize(&packet.data(..).unwrap()).unwrap())
            .collect();
        assert_eq!(txs_2, txs_2_after);

        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }

    #[test]
    fn test_bundle_sigverify_stage_many_packets_some_invalid() {
        let (unverified_sender, unverified_receiver) = bounded(1024);
        let (verified_sender, verified_receiver) = bounded(1024);
        let exit = Arc::new(AtomicBool::new(false));

        let mut txs_1 = (0..3).map(|_| test_tx()).collect::<Vec<_>>();
        txs_1[0].signatures[0] = Signature::default();

        let packet_bundle_1 = PacketBundle::new(
            PacketBatch::from(
                txs_1
                    .iter()
                    .map(|tx| BytesPacket::from_data(None, tx).unwrap())
                    .collect::<Vec<_>>(),
            ),
            "".to_string(),
        );

        unverified_sender.send(vec![packet_bundle_1]).unwrap();

        let stage = BundleSigverifyStage::new(unverified_receiver, verified_sender, exit.clone());

        assert_eq!(
            verified_receiver
                .recv_timeout(Duration::from_millis(10))
                .unwrap_err(),
            RecvTimeoutError::Timeout
        );

        exit.store(true, Ordering::Relaxed);
        stage.join().unwrap();
    }
}
