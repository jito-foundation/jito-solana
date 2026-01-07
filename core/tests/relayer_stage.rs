use {
    solana_core::proxy::relayer_stage::{RelayerConfig, RelayerStage},
    std::{
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        time::Duration,
    },
    tokio::{sync::watch::Receiver, time::timeout},
};

/// Mimics RelayerStage::start waiting loop: waits for valid config or sender drop
async fn wait_for_valid_config(
    mut rx: Receiver<RelayerConfig>,
    on_valid: Option<Arc<AtomicBool>>,
    on_dropped: Option<Arc<AtomicBool>>,
) {
    loop {
        let config = rx.borrow_and_update().clone();
        if RelayerStage::is_valid_relayer_config(&config) {
            if let Some(flag) = on_valid {
                flag.store(true, Ordering::SeqCst);
            }
            break;
        }
        if rx.changed().await.is_err() {
            if let Some(flag) = on_dropped {
                flag.store(true, Ordering::SeqCst);
            }
            return;
        }
    }
}

#[tokio::test]
async fn test_watch_channel_wakes_on_config_change() {
    let (tx, rx) = tokio::sync::watch::channel(RelayerConfig::default());
    let woke_up = Arc::new(AtomicBool::new(false));

    let handle = tokio::spawn(wait_for_valid_config(rx, Some(woke_up.clone()), None));

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert!(!woke_up.load(Ordering::SeqCst));

    tx.send(RelayerConfig {
        relayer_url: "http://localhost:8080".to_string(),
        expected_heartbeat_interval: Duration::from_millis(500),
        oldest_allowed_heartbeat: Duration::from_millis(2000),
    })
    .unwrap();

    assert!(timeout(Duration::from_millis(100), handle).await.is_ok());
    assert!(woke_up.load(Ordering::SeqCst));
}

#[tokio::test]
async fn test_watch_channel_exits_when_sender_dropped() {
    let (tx, rx) = tokio::sync::watch::channel(RelayerConfig::default());
    let exited_cleanly = Arc::new(AtomicBool::new(false));

    let handle = tokio::spawn(wait_for_valid_config(rx, None, Some(exited_cleanly.clone())));

    drop(tx);

    assert!(timeout(Duration::from_millis(100), handle).await.is_ok());
    assert!(exited_cleanly.load(Ordering::SeqCst));
}
