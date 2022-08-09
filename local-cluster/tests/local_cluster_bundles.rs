use serial_test::serial;

#[test]
#[serial]
fn test_bundles_accepted_by_cluster() {
    solana_logger::setup_with_default(RUST_LOG_FILTER);
}
