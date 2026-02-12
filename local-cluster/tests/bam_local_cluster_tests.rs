use {
    log::info,
    solana_commitment_config::CommitmentConfig,
    solana_core::{
        mock_bam_node::{MockBamNode, MockBamNodeConfig},
        validator::ValidatorConfig,
    },
    solana_gossip::{
        contact_info::{ContactInfo, Protocol},
        gossip_service::discover_validators,
    },
    solana_keypair::Keypair,
    solana_local_cluster::{
        cluster_tests::new_tpu_quic_client,
        local_cluster::{ClusterConfig, LocalCluster},
        validator_configs::make_identical_validator_configs,
    },
    solana_native_token::LAMPORTS_PER_SOL,
    solana_net_utils::SocketAddrSpace,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    },
};

const DEFAULT_NODE_STAKE: u64 = 10 * LAMPORTS_PER_SOL;
const BAM_CONNECTION_TIMEOUT: Duration = Duration::from_secs(30);
const GOSSIP_PROPAGATION_TIMEOUT: Duration = Duration::from_secs(30);

fn wait_for_bam_auth(mock_bam: &MockBamNode, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if mock_bam.auth_proofs_received() > 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

fn wait_for_gossip_tpu_update(
    cluster: &LocalCluster,
    expected_tpu: SocketAddr,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    let entry_point = cluster.entry_point_info.gossip().unwrap();

    while start.elapsed() < timeout {
        if let Ok(nodes) = discover_validators(
            &entry_point,
            1,
            cluster.shred_version(),
            SocketAddrSpace::Unspecified,
        ) {
            for node in &nodes {
                if let Some(tpu_quic) = node.tpu(solana_gossip::contact_info::Protocol::QUIC) {
                    if tpu_quic == expected_tpu {
                        return true;
                    }
                    info!("Current TPU QUIC: {tpu_quic}, expected: {expected_tpu}");
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    false
}

fn get_fresh_contact_info(cluster: &LocalCluster) -> Option<ContactInfo> {
    let entry_point = cluster.entry_point_info.gossip().unwrap();
    let nodes = discover_validators(
        &entry_point,
        1,
        cluster.shred_version(),
        SocketAddrSpace::Unspecified,
    )
    .ok()?;

    nodes
        .into_iter()
        .find(|n| n.pubkey() == cluster.entry_point_info.pubkey())
}

fn wait_for_gossip_tpu_changed(
    cluster: &LocalCluster,
    old_tpu: SocketAddr,
    timeout: Duration,
) -> Option<SocketAddr> {
    let start = Instant::now();
    let entry_point = cluster.entry_point_info.gossip().unwrap();

    while start.elapsed() < timeout {
        if let Ok(nodes) = discover_validators(
            &entry_point,
            1,
            cluster.shred_version(),
            SocketAddrSpace::Unspecified,
        ) {
            for node in &nodes {
                if let Some(tpu_quic) = node.tpu(solana_gossip::contact_info::Protocol::QUIC) {
                    if tpu_quic != old_tpu {
                        return Some(tpu_quic);
                    }
                }
            }
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    None
}

fn send_and_verify_transaction(
    cluster: &LocalCluster,
    funding_keypair: &Keypair,
    num_transactions: usize,
) {
    let fresh_contact_info =
        get_fresh_contact_info(cluster).expect("should find validator in gossip");

    let client = new_tpu_quic_client(&fresh_contact_info, cluster.connection_cache.clone())
        .expect("Failed to create TPU client");

    let tpu_addr = fresh_contact_info.tpu(Protocol::QUIC);
    info!("send_and_verify: {num_transactions} txns, TPU: {tpu_addr:?}");

    for i in 0..num_transactions {
        let recipient = Keypair::new();

        let (blockhash, _) = client
            .rpc_client()
            .get_latest_blockhash_with_commitment(CommitmentConfig::processed())
            .expect("Failed to get blockhash");

        let mut transaction = system_transaction::transfer(
            funding_keypair,
            &recipient.pubkey(),
            LAMPORTS_PER_SOL,
            blockhash,
        );

        let signature = transaction.signatures[0];
        info!(
            "Tx {}/{}: {}",
            i.saturating_add(1),
            num_transactions,
            signature
        );

        LocalCluster::send_transaction_with_retries(
            &client,
            &[funding_keypair],
            &mut transaction,
            3,
        )
        .expect("transaction should succeed");

        info!("Tx {}/{} confirmed", i.saturating_add(1), num_transactions);
    }
}

fn create_bam_cluster_config(bam_url: Arc<Mutex<Option<String>>>) -> ClusterConfig {
    let mut validator_config = ValidatorConfig::default_for_test();
    validator_config.bam_url = bam_url;

    ClusterConfig {
        mint_lamports: 100_000 * LAMPORTS_PER_SOL,
        node_stakes: vec![DEFAULT_NODE_STAKE],
        validator_configs: make_identical_validator_configs(&validator_config, 1),
        skip_warmup_slots: true,
        ..ClusterConfig::default()
    }
}

#[test]
#[serial_test::serial]
fn test_validator_connects_to_bam_and_updates_gossip() {
    agave_logger::setup_with_default("info");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut mock_bam = runtime
        .block_on(MockBamNode::start(MockBamNodeConfig::default()))
        .expect("Failed to start mock BAM node");

    info!("Mock BAM node started at {}", mock_bam.grpc_url());
    info!("Mock BAM TPU at {}", mock_bam.tpu_addr());

    let bam_url = Arc::new(Mutex::new(Some(mock_bam.grpc_url())));
    let mut cluster_config = create_bam_cluster_config(bam_url);
    let cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::Unspecified);
    info!("Local cluster started");

    assert!(
        wait_for_bam_auth(&mock_bam, BAM_CONNECTION_TIMEOUT),
        "Validator did not authenticate with BAM within {BAM_CONNECTION_TIMEOUT:?}",
    );
    info!("Validator authenticated with BAM");

    assert!(
        wait_for_gossip_tpu_update(&cluster, mock_bam.tpu_addr(), GOSSIP_PROPAGATION_TIMEOUT),
        "Gossip TPU was not updated to mock BAM's TPU within {GOSSIP_PROPAGATION_TIMEOUT:?}",
    );
    info!("Gossip TPU updated to mock BAM's TPU address");

    runtime.block_on(mock_bam.shutdown());
}

#[test]
#[serial_test::serial]
fn test_block_production_while_connected_to_bam() {
    agave_logger::setup_with_default("info");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut mock_bam = runtime
        .block_on(MockBamNode::start(MockBamNodeConfig::default()))
        .expect("Failed to start mock BAM node");

    info!(
        "Mock BAM started - gRPC: {}, TPU: {}",
        mock_bam.grpc_url(),
        mock_bam.tpu_addr()
    );

    let bam_url = Arc::new(Mutex::new(Some(mock_bam.grpc_url())));
    let mut cluster_config = create_bam_cluster_config(bam_url.clone());
    let cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::Unspecified);

    assert!(
        wait_for_bam_auth(&mock_bam, BAM_CONNECTION_TIMEOUT),
        "BAM auth failed"
    );

    assert!(
        wait_for_gossip_tpu_update(&cluster, mock_bam.tpu_addr(), GOSSIP_PROPAGATION_TIMEOUT),
        "Gossip TPU not updated"
    );

    info!("Sending transactions through BAM...");
    send_and_verify_transaction(&cluster, &cluster.funding_keypair, 3);

    info!("Verifying blocks are being finalized...");
    cluster.check_for_new_roots(
        5,
        "bam_connected_block_production",
        SocketAddrSpace::Unspecified,
    );
    info!("Cluster produced and finalized blocks with transactions while connected to BAM");
    runtime.block_on(mock_bam.shutdown());
}

#[test]
#[serial_test::serial]
fn test_block_production_after_bam_disconnect() {
    agave_logger::setup_with_default("info");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut mock_bam = runtime
        .block_on(MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_millis(500),
            ..MockBamNodeConfig::default()
        }))
        .expect("Failed to start mock BAM node");

    let bam_tpu = mock_bam.tpu_addr();
    info!(
        "Mock BAM started - gRPC: {}, TPU: {}",
        mock_bam.grpc_url(),
        bam_tpu
    );

    let bam_url = Arc::new(Mutex::new(Some(mock_bam.grpc_url())));
    let mut cluster_config = create_bam_cluster_config(bam_url.clone());
    let cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::Unspecified);

    assert!(
        wait_for_bam_auth(&mock_bam, BAM_CONNECTION_TIMEOUT),
        "BAM auth failed"
    );

    assert!(
        wait_for_gossip_tpu_update(&cluster, bam_tpu, GOSSIP_PROPAGATION_TIMEOUT),
        "Gossip TPU not updated to BAM"
    );
    info!("Validator connected to BAM, gossip updated");

    info!("Sending transactions while connected to BAM...");
    send_and_verify_transaction(&cluster, &cluster.funding_keypair, 2);

    info!("Verifying block production while connected...");
    cluster.check_for_new_roots(3, "bam_connected", SocketAddrSpace::Unspecified);

    info!("Shutting down mock BAM to simulate disconnect...");
    runtime.block_on(mock_bam.shutdown());
    drop(mock_bam);

    info!("Waiting for validator to detect BAM unhealthy and update gossip...");
    let new_tpu = wait_for_gossip_tpu_changed(&cluster, bam_tpu, Duration::from_secs(30));
    assert!(
        new_tpu.is_some(),
        "Validator did not update gossip TPU after BAM disconnect"
    );
    info!("Gossip TPU changed from {bam_tpu} to {new_tpu:?}");

    info!("Sending transactions after BAM disconnect...");
    send_and_verify_transaction(&cluster, &cluster.funding_keypair, 2);

    info!("Verifying block production continues after disconnect...");
    cluster.check_for_new_roots(5, "bam_disconnected", SocketAddrSpace::Unspecified);
    info!(
        "Cluster continued producing and finalizing blocks with transactions after BAM disconnect"
    );
}

#[test]
#[serial_test::serial]
fn test_validator_receives_auth_proof() {
    agave_logger::setup_with_default("info");

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut mock_bam = runtime
        .block_on(MockBamNode::start(MockBamNodeConfig::default()))
        .expect("Failed to start mock BAM node");

    let bam_url = Arc::new(Mutex::new(Some(mock_bam.grpc_url())));
    let mut cluster_config = create_bam_cluster_config(bam_url);
    let _cluster = LocalCluster::new(&mut cluster_config, SocketAddrSpace::Unspecified);

    assert!(
        wait_for_bam_auth(&mock_bam, BAM_CONNECTION_TIMEOUT),
        "Validator did not authenticate with BAM"
    );

    let auth_count = mock_bam.auth_proofs_received();
    assert!(
        auth_count >= 1,
        "Expected at least 1 auth proof, got {auth_count}",
    );
    info!("Validator authenticated {auth_count} time(s)");
    runtime.block_on(mock_bam.shutdown());
}

#[cfg(test)]
mod unit_tests {
    use {
        super::*,
        jito_protos::proto::bam_api::{bam_node_api_client::BamNodeApiClient, ConfigRequest},
    };

    #[tokio::test]
    async fn test_mock_bam_node_serves_config() {
        let mock_bam = MockBamNode::start(MockBamNodeConfig::default())
            .await
            .expect("should start");

        let mut client = BamNodeApiClient::connect(mock_bam.grpc_url())
            .await
            .expect("should connect");

        let response = client
            .get_builder_config(ConfigRequest {})
            .await
            .expect("should get config");

        let config = response.into_inner();
        assert!(config.bam_config.is_some());

        let bam_config = config.bam_config.unwrap();
        let tpu_sock = bam_config.tpu_sock.unwrap();

        assert_eq!(tpu_sock.port, mock_bam.tpu_addr().port() as u32);
    }
}
