#![allow(clippy::arithmetic_side_effects)]

use {
    // --- IMPORTS ---
    serial_test::serial,
    solana_account::{Account, AccountSharedData},
    solana_bench_tps::{
        bench::{do_bench_tps, generate_and_fund_keypairs},
        cli::{Config, InstructionPaddingConfig},
        send_batch::generate_durable_nonce_accounts,
    },
    solana_commitment_config::CommitmentConfig,
    solana_core::validator::ValidatorConfig,
    solana_faucet::faucet::run_local_faucet_for_tests,
    solana_fee_calculator::FeeRateGovernor,
    solana_keypair::Keypair,
    solana_local_cluster::{
        cluster::Cluster,
        local_cluster::{ClusterConfig, LocalCluster},
        validator_configs::make_identical_validator_configs,
    },
    solana_net_utils::SocketAddrSpace,
    solana_quic_client::{QuicConnectionManager, QuicConfig},
    solana_rent::Rent,
    solana_rpc::rpc::JsonRpcConfig,
    solana_rpc_client::rpc_client::RpcClient,
    solana_signer::Signer,
    solana_test_validator::TestValidatorGenesis,
    solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig},
    std::{sync::Arc, time::Duration},
};

// --- HELPER FUNCTIONS ---

/// Creates an executable AccountSharedData structure for a BPF program.
fn program_account(program_data: &[u8]) -> AccountSharedData {
    AccountSharedData::from(Account {
        // Ensure the program account has enough lamports to cover rent for its data size.
        // It should have at least 1 lamport to be considered existing.
        lamports: Rent::default().minimum_balance(program_data.len()).min(1),
        data: program_data.to_vec(),
        owner: solana_sdk_ids::bpf_loader::id(),
        executable: true,
        rent_epoch: 0,
    })
}

/// Runs TPS benchmark tests within a simulated single-node LocalCluster environment.
fn test_bench_tps_local_cluster(config: Config) {
    // 1. Setup Environment
    let program_data = include_bytes!("fixtures/spl_instruction_padding.so");
    let additional_accounts = vec![(
        spl_instruction_padding_interface::ID,
        program_account(program_data),
    )];

    agave_logger::setup();

    let faucet_keypair = Keypair::new();
    let faucet_pubkey = faucet_keypair.pubkey();
    let faucet_addr = run_local_faucet_for_tests(
        faucet_keypair,
        None, /* per_time_cap */
        0,    /* port */
    );

    // 2. Initialize LocalCluster
    const NUM_NODES: usize = 1;
    let cluster = LocalCluster::new(
        &mut ClusterConfig {
            node_stakes: vec![999_990; NUM_NODES],
            mint_lamports: 200_000_000,
            validator_configs: make_identical_validator_configs(
                &ValidatorConfig {
                    rpc_config: JsonRpcConfig {
                        faucet_addr: Some(faucet_addr),
                        ..JsonRpcConfig::default_for_test()
                    },
                    ..ValidatorConfig::default_for_test()
                },
                NUM_NODES,
            ),
            additional_accounts,
            ..ClusterConfig::default()
        },
        SocketAddrSpace::Unspecified,
    );

    // Fund the faucet for test accounts.
    cluster.transfer(&cluster.funding_keypair, &faucet_pubkey, 100_000_000);

    // 3. Create TPU Client (QUIC preferred for performance)
    let client = Arc::new(
        cluster
            .build_validator_tpu_quic_client(cluster.entry_point_info.pubkey())
            .unwrap_or_else(|err| {
                panic!("Failed to create TpuClient with Quic Cache: {:?}", err);
            }),
    );

    // 4. Generate & Fund Keypairs
    let lamports_per_account = 100;
    let keypair_count = config.tx_count * config.keypair_multiplier;
    let keypairs = generate_and_fund_keypairs(
        client.clone(),
        &config.id,
        keypair_count,
        lamports_per_account,
        false,
        false,
    )
    .expect("Failed to generate and fund keypairs");

    // 5. Run Benchmark
    let _total = do_bench_tps(client, config, keypairs, None);

    // Assertion for non-debug builds
    #[cfg(not(debug_assertions))]
    assert!(_total > 100);
}

/// Runs TPS benchmark tests within a single TestValidator instance.
fn test_bench_tps_test_validator(config: Config) {
    agave_logger::setup();

    // 1. Setup Faucet
    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let faucet_addr = run_local_faucet_for_tests(
        mint_keypair,
        None, /* per_time_cap */
        0,    /* port */
    );

    // 2. Initialize TestValidator
    let test_validator = TestValidatorGenesis::default()
        .fee_rate_governor(FeeRateGovernor::new(0, 0))
        .rent(Rent {
            // Configure simple rent for predictable test environment
            lamports_per_byte_year: 1,
            exemption_threshold: 1.0,
            ..Rent::default()
        })
        .faucet_addr(Some(faucet_addr))
        // Add the Instruction Padding program required for some tests
        .add_program(
            "spl_instruction_padding",
            spl_instruction_padding_interface::ID,
        )
        .start_with_mint_address(mint_pubkey, SocketAddrSpace::Unspecified)
        .expect("Test Validator failed to start");

    // 3. Create Tpu Client
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        test_validator.rpc_url(),
        CommitmentConfig::processed(),
    ));
    let websocket_url = test_validator.rpc_pubsub_url();

    let client = Arc::new(
        TpuClient::new(
            "tpu_client_quic_bench_tps",
            rpc_client,
            &websocket_url,
            TpuClientConfig::default(),
            QuicConnectionManager::new_with_connection_config(QuicConfig::new().unwrap()),
        )
        .expect("Failed to build Quic Tpu Client"),
    );

    // 4. Generate & Fund Keypairs
    let lamports_per_account = 1000;
    let keypair_count = config.tx_count * config.keypair_multiplier;
    let keypairs = generate_and_fund_keypairs(
        client.clone(),
        &config.id,
        keypair_count,
        lamports_per_account,
        false,
        false,
    )
    .expect("Failed to generate and fund keypairs");
    
    // Generate Nonce accounts if required by the configuration
    let nonce_keypairs = if config.use_durable_nonce {
        Some(generate_durable_nonce_accounts(client.clone(), &keypairs))
    } else {
        None
    };

    // 5. Run Benchmark
    let _total = do_bench_tps(client, config, keypairs, nonce_keypairs);

    // Assertion for non-debug builds
    #[cfg(not(debug_assertions))]
    assert!(_total > 100);
}

// --- TEST CASES ---

#[test]
#[serial]
fn test_bench_tps_local_cluster_solana() {
    test_bench_tps_local_cluster(Config {
        tx_count: 100,
        duration: Duration::from_secs(10),
        ..Config::default()
    });
}

#[test]
#[serial]
fn test_bench_tps_tpu_client() {
    test_bench_tps_test_validator(Config {
        tx_count: 100,
        duration: Duration::from_secs(10),
        ..Config::default()
    });
}

#[test]
#[serial]
fn test_bench_tps_tpu_client_nonce() {
    test_bench_tps_test_validator(Config {
        tx_count: 100,
        duration: Duration::from_secs(10),
        use_durable_nonce: true, // Enables Durable Nonce transactions
        ..Config::default()
    });
}

#[test]
#[serial]
fn test_bench_tps_local_cluster_with_padding() {
    test_bench_tps_local_cluster(Config {
        tx_count: 100,
        duration: Duration::from_secs(10),
        instruction_padding_config: Some(InstructionPaddingConfig {
            program_id: spl_instruction_padding_interface::ID,
            data_size: 0, // Padding instruction data size
        }),
        ..Config::default()
    });
}

#[test]
#[serial]
fn test_bench_tps_tpu_client_with_padding() {
    test_bench_tps_test_validator(Config {
        tx_count: 100,
        duration: Duration::from_secs(10),
        instruction_padding_config: Some(InstructionPaddingConfig {
            program_id: spl_instruction_padding_interface::ID,
            data_size: 0, // Padding instruction data size
        }),
        ..Config::default()
    });
}
