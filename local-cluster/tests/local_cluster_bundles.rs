use serial_test::serial;

#[test]
#[serial]
fn test_bundles_accepted_by_cluster() {
    // solana_logger::setup_with_default(RUST_LOG_FILTER);

    // TODO (LB): want to test the following:
    // - bundle ordering is preserved.
    // - all valid bundles are executed. non-valid bundles are not executed
    // - the cluster can reach consensus on bundles from bundle stage
    // - banking stage continues to work with bundle_account_locker
}
