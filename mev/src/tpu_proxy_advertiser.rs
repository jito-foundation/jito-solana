//! The `tpu_proxy_advertiser` listens on a notification
//! channel for connect/disconnect to validator interface
//! and advertises TPU proxy addresses when connected.

use {
    log::*,
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
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
        tpu_addr: SocketAddr,
        tpu_forwards_addr: SocketAddr,
        cluster_info: &Arc<ClusterInfo>,
        tpu_notify_reciver: UnboundedReceiver<bool>,
    ) -> TpuProxyAdvertiser {
        let cluster_info = cluster_info.clone();
        let thread_hdl = thread::spawn(move || {
            info!("[Jito] Started TPU proxy advertiser");
            Self::advertise(
                tpu_addr,
                tpu_forwards_addr,
                &cluster_info,
                cluster_info.my_contact_info(),
                tpu_notify_reciver,
            )
        });
        Self { thread_hdl }
    }

    fn advertise(
        tpu_addr: SocketAddr,
        tpu_forwards_addr: SocketAddr,
        cluster_info: &Arc<ClusterInfo>,
        saved_contact_info: ContactInfo,
        mut tpu_notify_receiver: UnboundedReceiver<bool>,
    ) {
        let mut last_advertise = false;
        loop {
            if let Some(should_advertise) = tpu_notify_receiver.blocking_recv() {
                if should_advertise {
                    if !last_advertise {
                        info!("[Jito] TPU proxy connect, advertising remote TPU ports");
                        let mut new_contact_info = saved_contact_info.clone();
                        new_contact_info.tpu = tpu_addr;
                        new_contact_info.tpu_forwards = tpu_forwards_addr;
                        cluster_info.set_my_contact_info(new_contact_info);
                        last_advertise = true;
                    }
                } else if last_advertise {
                    info!("[Jito] TPU proxy disconnected, advertising local TPU ports");
                    cluster_info.set_my_contact_info(saved_contact_info.clone());
                    last_advertise = false;
                }
            }
        }
    }

    pub fn join(self) -> thread::Result<()> {
        self.thread_hdl.join()
    }
}
