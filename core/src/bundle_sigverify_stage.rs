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
        panic!("not implemented");
    }

    #[test]
    fn test_bundle_sigverify_stage_many_packets_some_invalid() {
        panic!("not implemented");
    }
}
