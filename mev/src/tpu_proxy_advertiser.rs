//! The `tpu_proxy_advertiser` listens on a notification
//! channel for connect/disconnect to validator interface
//! and advertises TPU proxy addresses when connected.

use {
    log::*,
    solana_gossip::cluster_info::ClusterInfo,
    std::{
        net::SocketAddr,
        sync::Arc,
        thread::{self, JoinHandle},
    },
    tokio::sync::mpsc::UnboundedReceiver,
};

pub struct TpuProxyAdvertiser {
    thread_hdl: JoinHandle<()>,
}

impl TpuProxyAdvertiser {
    pub fn new(
        cluster_info: &Arc<ClusterInfo>,
        tpu_notify_receiver: UnboundedReceiver<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) -> TpuProxyAdvertiser {
        let cluster_info = cluster_info.clone();
        let thread_hdl = thread::spawn(move || {
            info!("[MEV] Started TPU proxy advertiser");
            let saved_contact_info = cluster_info.my_contact_info();
            Self::advertise(
                &cluster_info,
                saved_contact_info.tpu,
                saved_contact_info.tpu_forwards,
                tpu_notify_receiver,
            )
        });
        Self { thread_hdl }
    }

    fn advertise(
        cluster_info: &Arc<ClusterInfo>,
        saved_tpu: SocketAddr,
        saved_tpu_forwards: SocketAddr,
        mut tpu_notify_receiver: UnboundedReceiver<(Option<SocketAddr>, Option<SocketAddr>)>,
    ) {
        let mut last_advertise = false;
        loop {
            match tpu_notify_receiver.blocking_recv() {
                Some((Some(tpu_address), Some(tpu_forward_address))) => {
                    info!("[MEV] TPU proxy connect, advertising remote TPU ports");
                    Self::set_tpu_addresses(cluster_info, tpu_address, tpu_forward_address);
                    last_advertise = true;
                }
                Some((None, None)) => {
                    if last_advertise {
                        info!("[MEV] TPU proxy disconnected, advertising local TPU ports");
                        Self::set_tpu_addresses(cluster_info, saved_tpu, saved_tpu_forwards);
                        last_advertise = false;
                    }
                }
                None => {
                    warn!("[MEV] Advertising local TPU ports and shutting down.");
                    Self::set_tpu_addresses(cluster_info, saved_tpu, saved_tpu_forwards);
                    return;
                }
                _ => {
                    unreachable!()
                } // Channel only receives internal messages
            }
        }
    }

    fn set_tpu_addresses(
        cluster_info: &Arc<ClusterInfo>,
        tpu_address: SocketAddr,
        tpu_forward_address: SocketAddr,
    ) {
        let mut new_contact_info = cluster_info.my_contact_info();
        new_contact_info.tpu = tpu_address;
        new_contact_info.tpu_forwards = tpu_forward_address;
        cluster_info.set_my_contact_info(new_contact_info);
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
