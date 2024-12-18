//! This module contains functionality required to create tests parameterized
//! with the client type.

use {
    crate::{tpu_info::NullTpuInfo, transaction_client::TpuClientNextClient},
    solana_gossip::{cluster_info::ClusterInfo, contact_info::ContactInfo},
    solana_keypair::Keypair,
    solana_net_utils::{
        sockets::{bind_to, localhost_port_range_for_tests},
        SocketAddrSpace,
    },
    solana_signer::Signer,
    std::{
        net::{IpAddr, Ipv4Addr, SocketAddr},
        sync::Arc,
    },
    tokio::runtime::Handle,
    tokio_util::sync::CancellationToken,
};

pub fn create_test_cluster_info() -> Arc<ClusterInfo> {
    let keypair = Keypair::new();
    let contact_info = ContactInfo::new_with_socketaddr(
        &keypair.pubkey(),
        &SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 1234),
    );
    Arc::new(ClusterInfo::new(
        contact_info,
        Arc::new(keypair),
        SocketAddrSpace::Unspecified,
    ))
}

pub fn create_client_for_tests(
    runtime_handle: Handle,
    cluster_info: Arc<ClusterInfo>,
    tpu_peers: Option<Vec<SocketAddr>>,
    leader_forward_count: u64,
) -> TpuClientNextClient {
    let port_range = localhost_port_range_for_tests();
    let bind_socket = bind_to(IpAddr::V4(Ipv4Addr::LOCALHOST), port_range.0)
        .expect("Should be able to open UdpSocket for tests.");
    TpuClientNextClient::new::<NullTpuInfo>(
        runtime_handle,
        cluster_info,
        tpu_peers,
        None,
        leader_forward_count,
        None,
        bind_socket,
        CancellationToken::new(),
    )
}
