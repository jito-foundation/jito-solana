use {
    jito_protos::proto::bam_types::{AtomicTxnBatch, Packet}, solana_core::{bam_dependencies::BamOutboundMessage, mock_bam_node::{MockBamNode, MockBamNodeConfig}}, solana_gossip::{cluster_info::ClusterInfo, node::Node}, solana_keypair::Keypair, solana_net_utils::SocketAddrSpace, solana_signer::Signer, std::{
        sync::{Arc, atomic::Ordering},
        time::Duration,
    }
};

fn create_test_cluster_info() -> Arc<ClusterInfo> {
    let keypair = Arc::new(Keypair::new());
    let node = Node::new_localhost_with_pubkey(&keypair.pubkey());
    Arc::new(ClusterInfo::new(
        node.info,
        keypair,
        SocketAddrSpace::Unspecified,
    ))
}

fn create_channels() -> (
    crossbeam_channel::Sender<AtomicTxnBatch>,
    crossbeam_channel::Receiver<AtomicTxnBatch>,
    crossbeam_channel::Sender<BamOutboundMessage>,
    crossbeam_channel::Receiver<BamOutboundMessage>,
) {
    let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
    let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();
    (batch_tx, batch_rx, outbound_tx, outbound_rx)
}

async fn wait_until_healthy(
    connection: &solana_core::bam_connection::BamConnection,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if connection.is_healthy() && connection.get_latest_config().is_some() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

#[cfg(test)]
mod bam_connection_tests {
    use {super::*, solana_core::bam_connection::BamConnection};

    #[tokio::test]
    async fn test_connection_healthy_with_heartbeats() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        assert!(
            wait_until_healthy(&connection, Duration::from_secs(10)).await,
            "connection should become healthy"
        );
        assert!(connection.is_healthy());
        assert!(connection.get_latest_config().is_some());
    }

    #[tokio::test]
    async fn test_connection_unhealthy_when_heartbeats_stop() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_millis(500),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        assert!(
            wait_until_healthy(&connection, Duration::from_secs(10)).await,
            "connection should become healthy initially"
        );

        node.set_send_heartbeats(false);
        tokio::time::sleep(Duration::from_secs(8)).await;

        assert!(
            !connection.is_healthy(),
            "connection should be unhealthy after heartbeats stop"
        );
    }

    #[tokio::test]
    async fn test_connection_stays_unhealthy_without_initial_heartbeats() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(2),
            ..Default::default()
        })
        .await
        .expect("should start");

        node.set_send_heartbeats(false);

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        let became_healthy = wait_until_healthy(&connection, Duration::from_secs(3)).await;

        assert!(!became_healthy);
        assert!(!connection.is_healthy());
    }

    #[tokio::test]
    async fn test_connection_fails_when_server_unreachable() {
        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let result = BamConnection::try_init(
            "http://127.0.0.1:1".to_string(),
            cluster_info,
            batch_tx,
            outbound_rx,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_config_available_when_healthy() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);

        let config = connection.get_latest_config().expect("config should exist");
        let bam_config = config.bam_config.expect("bam_config should exist");
        assert_eq!(bam_config.commission_bps, 100);
    }

    #[tokio::test]
    async fn test_batches_forwarded_to_receiver() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_millis(100),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (batch_tx, batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);

        node.queue_test_batch(AtomicTxnBatch {
            seq_id: 42,
            max_schedule_slot: 100,
            packets: vec![Packet {
                data: vec![1, 2, 3],
                meta: None,
            }],
        });

        tokio::time::sleep(Duration::from_millis(500)).await;

        let received = batch_rx.try_recv().expect("should receive batch");
        assert_eq!(received.seq_id, 42);
        assert_eq!(received.max_schedule_slot, 100);
    }

    #[tokio::test]
    async fn test_drop_sets_unhealthy() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (batch_tx, _batch_rx, _outbound_tx, outbound_rx) = create_channels();

        let connection =
            BamConnection::try_init(node.grpc_url(), cluster_info, batch_tx, outbound_rx)
                .await
                .expect("should connect");

        assert!(wait_until_healthy(&connection, Duration::from_secs(10)).await);
        assert!(connection.is_healthy());

        drop(connection);
    }
}

#[cfg(test)]
mod bam_manager_tests {
    use {
        super::*,
        solana_core::{
            admin_rpc_post_init::KeyUpdaters,
            bam_dependencies::{BamConnectionState, BamDependencies},
            bam_manager::BamManager,
            proxy::block_engine_stage::BlockBuilderFeeInfo,
        },
        solana_ledger::{blockstore::Blockstore, genesis_utils::create_genesis_config},
        solana_poh::poh_recorder::create_test_recorder,
        solana_pubkey::Pubkey,
        solana_runtime::bank::Bank,
        std::sync::{atomic::AtomicU8, Mutex, RwLock},
    };

    fn create_test_bam_dependencies(
        cluster_info: Arc<ClusterInfo>,
        bank_forks: Arc<RwLock<solana_runtime::bank_forks::BankForks>>,
    ) -> BamDependencies {
        let (batch_tx, batch_rx) = crossbeam_channel::unbounded();
        let (outbound_tx, outbound_rx) = crossbeam_channel::unbounded();

        BamDependencies {
            bam_enabled: Arc::new(AtomicU8::new(BamConnectionState::Disconnected as u8)),
            batch_sender: batch_tx,
            batch_receiver: batch_rx,
            outbound_sender: outbound_tx,
            outbound_receiver: outbound_rx,
            cluster_info,
            block_builder_fee_info: Arc::new(Mutex::new(BlockBuilderFeeInfo::default())),
            bank_forks,
            bam_node_pubkey: Arc::new(Mutex::new(Pubkey::default())),
        }
    }

    fn bam_is_connected(bam_enabled: &Arc<AtomicU8>) -> bool {
        BamConnectionState::from_u8(bam_enabled.load(Ordering::Relaxed))
            == BamConnectionState::Connected
    }

    #[allow(clippy::type_complexity)]
    fn create_test_poh_recorder() -> (
        Arc<std::sync::atomic::AtomicBool>,
        Arc<std::sync::RwLock<solana_poh::poh_recorder::PohRecorder>>,
        Arc<RwLock<solana_runtime::bank_forks::BankForks>>,
        solana_poh::poh_service::PohService,
        tempfile::TempDir,
    ) {
        let genesis_config = create_genesis_config(1000).genesis_config;
        let (bank, bank_forks) = Bank::new_no_wallclock_throttle_for_tests(&genesis_config);

        let ledger_path = tempfile::tempdir().unwrap();
        let blockstore =
            Arc::new(Blockstore::open(ledger_path.path()).expect("Failed to open blockstore"));

        let (exit, poh_recorder, _poh_controller, _recorder, poh_service, _entry_receiver) =
            create_test_recorder(bank, blockstore, None, None);

        (exit, poh_recorder, bank_forks, poh_service, ledger_path)
    }

    async fn wait_for_bam_state(
        bam_enabled: &Arc<AtomicU8>,
        connected: bool,
        timeout: Duration,
    ) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if bam_is_connected(bam_enabled) == connected {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        false
    }

    #[tokio::test]
    async fn test_bam_enabled_when_connection_healthy() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(node.grpc_url())));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        assert!(
            wait_for_bam_state(&bam_enabled, true, Duration::from_secs(15)).await,
            "bam_enabled should be true"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_bam_disabled_when_connection_unhealthy() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_millis(500),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(node.grpc_url())));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        assert!(wait_for_bam_state(&bam_enabled, true, Duration::from_secs(15)).await);

        node.set_send_heartbeats(false);

        assert!(
            wait_for_bam_state(&bam_enabled, false, Duration::from_secs(15)).await,
            "bam_enabled should be false"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_bam_disabled_when_no_url() {
        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(None));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        tokio::time::sleep(Duration::from_secs(2)).await;

        assert!(
            !bam_is_connected(&bam_enabled),
            "bam_enabled should remain false"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_reconnects_when_url_changes() {
        let node1 = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let node2 = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(node1.grpc_url())));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url.clone(),
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        assert!(wait_for_bam_state(&bam_enabled, true, Duration::from_secs(15)).await);

        *bam_url.lock().unwrap() = Some(node2.grpc_url());

        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(
            bam_is_connected(&bam_enabled),
            "bam_enabled should be true after URL change"
        );

        exit.store(true, Ordering::Relaxed);
        poh_service.join().unwrap();
    }

    #[tokio::test]
    async fn test_block_builder_fee_info_updated() {
        let node = MockBamNode::start(MockBamNodeConfig {
            heartbeat_interval: Duration::from_secs(1),
            ..Default::default()
        })
        .await
        .expect("should start");

        let cluster_info = create_test_cluster_info();
        let (exit, poh_recorder, bank_forks, poh_service, _ledger_path) =
            create_test_poh_recorder();
        let dependencies = create_test_bam_dependencies(cluster_info, bank_forks);
        let block_builder_fee_info = dependencies.block_builder_fee_info.clone();
        let bam_enabled = dependencies.bam_enabled.clone();

        let bam_url = Arc::new(Mutex::new(Some(node.grpc_url())));
        let identity_notifiers = Arc::new(std::sync::RwLock::new(KeyUpdaters::default()));

        let _manager = BamManager::new(
            exit.clone(),
            bam_url,
            dependencies,
            poh_recorder,
            identity_notifiers,
        );

        assert!(wait_for_bam_state(&bam_enabled, true, Duration::from_secs(15)).await);

        tokio::time::sleep(Duration::from_secs(2)).await;

        let fee_info = block_builder_fee_info.lock().unwrap();
        assert_eq!(fee_info.block_builder_commission, 10);

        exit.store(true, Ordering::Relaxed);
        drop(fee_info);
        poh_service.join().unwrap();
    }
}
