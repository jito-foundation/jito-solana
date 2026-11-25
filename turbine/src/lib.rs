#![cfg(feature = "agave-unstable-api")]
#![allow(clippy::arithmetic_side_effects)]

use {
    smallvec::SmallVec,
    solana_net_utils::sockets::{SocketConfiguration, UdpSocketBufferLimits, bind_to_with_config},
    std::{
        io,
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
    },
};

pub mod broadcast_stage;

pub mod cluster_nodes;

pub mod retransmit_stage;

pub mod sigverify_shreds;

pub use solana_net_utils::PinnedXdpSender as XdpSender;

#[macro_use]
extern crate log;

#[macro_use]
extern crate solana_metrics;

pub type ShredReceiverAddresses = SmallVec<[SocketAddr; 5]>;

pub const MAX_SHRED_RECEIVER_ADDRESSES: usize = 32;

/// Binds the 0.0.0.0:0 socket that sends shreds to BAM, multicast,
/// shredstream and the shred receiver addresses. Like the node's own
/// primarily-write UDP sockets (gossip's `Node::create_socket_configs`), it
/// gets the largest send buffer the kernel allows. Linux only: UDP sends on
/// FreeBSD and macOS are unbuffered.
pub(crate) fn bind_shred_sender_socket() -> io::Result<UdpSocket> {
    let send_buffer_size = if cfg!(target_os = "linux") {
        UdpSocketBufferLimits::new().send_limit
    } else {
        None
    };
    bind_to_with_config(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        0,
        SocketConfiguration::default().send_buffer_size(send_buffer_size),
    )
}
