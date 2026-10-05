use {
    assert_matches::assert_matches,
    solana_account_decoder::UiAccountEncoding,
    solana_fee_calculator::FeeRateGovernor,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_net_utils::SocketAddrSpace,
    solana_pubkey::{Pubkey, new_rand},
    solana_rpc_client::rpc_client::RpcClient,
    solana_rpc_client_api::{
        bundles::{
            RpcBundleExecutionError, RpcBundleSimulationSummary, RpcSimulateBundleConfig,
            SimulationSlotConfig,
        },
        client_error::{Error as ClientError, ErrorKind as ClientErrorKind},
        config::RpcSimulateTransactionAccountsConfig,
        request::{RpcError, RpcResponseErrorData},
    },
    solana_signature::Signature,
    solana_signer::Signer,
    solana_system_transaction as system_transaction,
    solana_test_validator::TestValidatorGenesis,
    solana_transaction::TransactionError,
    solana_transaction_status::UiTransactionEncoding,
};

fn bundle_transaction_inputs(rpc_client: &RpcClient) -> (Hash, u64) {
    (
        rpc_client.get_latest_blockhash().unwrap(),
        rpc_client
            .get_minimum_balance_for_rent_exemption(0)
            .unwrap(),
    )
}

fn bundle_config(transaction_count: usize) -> RpcSimulateBundleConfig {
    RpcSimulateBundleConfig {
        pre_execution_accounts_configs: vec![None; transaction_count],
        post_execution_accounts_configs: vec![None; transaction_count],
        transaction_encoding: Some(UiTransactionEncoding::Base64),
        simulation_bank: Some(SimulationSlotConfig::Tip),
        skip_sig_verify: false,
        replace_recent_blockhash: false,
    }
}

fn rpc_response_error(error: &ClientError) -> (i64, &str, &RpcResponseErrorData) {
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = error.kind()
    else {
        panic!("unexpected error: {error}");
    };
    (*code, message, data)
}

#[test]
fn test_simulate_bundle() {
    agave_logger::setup();

    let mint_keypair = Keypair::new();
    let validator = TestValidatorGenesis::default_for_tests()
        .fee_rate_governor(FeeRateGovernor::new(0, 0))
        .start_with_mint_address(mint_keypair.pubkey(), SocketAddrSpace::Unspecified)
        .expect("validator start failed");
    let rpc_client = RpcClient::new(validator.rpc_url());

    test_too_many_bundles(&rpc_client, &mint_keypair);
    test_wrong_number_pre_accounts(&rpc_client, &mint_keypair);
    test_wrong_number_post_accounts(&rpc_client, &mint_keypair);
    test_invalid_transaction_encoding(&rpc_client, &mint_keypair);
    test_wrong_pre_account_encoding(&rpc_client, &mint_keypair);
    test_wrong_post_account_encoding(&rpc_client, &mint_keypair);
    test_replace_recent_blockhash_with_sig_verify(&rpc_client, &mint_keypair);
    test_bad_signature(&rpc_client, &mint_keypair);
    test_bad_pubkey_pre_accounts(&rpc_client, &mint_keypair);
    test_bad_pubkey_post_accounts(&rpc_client, &mint_keypair);
    test_single_tx_ok(&rpc_client, &mint_keypair);
    test_chained_transfers_ok(&rpc_client, &mint_keypair);
    test_single_bad_tx(&rpc_client, &mint_keypair);
    test_last_tx_fails(&rpc_client, &mint_keypair);
    test_duplicate_transactions(&rpc_client, &mint_keypair);
    test_program_execution_error(&rpc_client, &mint_keypair);
}

fn test_too_many_bundles(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions: Vec<_> = (0..21)
        .map(|_| {
            system_transaction::transfer(
                mint_keypair,
                &Pubkey::new_unique(),
                rent,
                latest_blockhash,
            )
        })
        .collect();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(&transactions, RpcSimulateBundleConfig::default())
        .unwrap_err();

    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(message, "bundle size too large, max 20 transactions");
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_number_pre_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len().saturating_add(1)],
                post_execution_accounts_configs: vec![None; transactions.len()],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();

    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "pre/post_execution_accounts_configs must be equal in length to the number of transactions"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_number_post_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len().saturating_add(1)],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "pre/post_execution_accounts_configs must be equal in length to the number of transactions"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_invalid_transaction_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len()],
                transaction_encoding: Some(UiTransactionEncoding::Base58),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "Base64 is the only supported encoding for transactions"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_pre_account_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base58),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![None; transactions.len()],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "Base64 is the only supported encoding for pre-execution accounts"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_post_account_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base58),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "Base64 is the only supported encoding for post-execution accounts"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_duplicate_transactions(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let pubkey = Pubkey::new_unique();
    let transactions = vec![
        system_transaction::transfer(mint_keypair, &pubkey, rent, latest_blockhash),
        system_transaction::transfer(mint_keypair, &pubkey, rent, latest_blockhash),
    ];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(&transactions, bundle_config(transactions.len()))
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(message, "duplicate transactions");
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_replace_recent_blockhash_with_sig_verify(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base58),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: true,
                replace_recent_blockhash: true,
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "Base64 is the only supported encoding for post-execution accounts"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_signature(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let mut transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];
    transactions.get_mut(0).unwrap().signatures[0] = Signature::default();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "transaction signature is invalid: Transaction did not pass signature verification"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_pubkey_pre_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec!["testing123".to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "invalid pubkey for pre/post accounts provided: testing123"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_pubkey_post_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &Pubkey::new_unique(),
        rent,
        latest_blockhash,
    )];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec!["testing123".to_string()],
                })],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap_err();
    let (code, message, data) = rpc_response_error(&simulate_result);
    assert_eq!(
        message,
        "invalid pubkey for pre/post accounts provided: testing123"
    );
    assert_eq!(code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_single_tx_ok(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let bob = Keypair::new();
    let transactions = vec![system_transaction::transfer(
        mint_keypair,
        &bob.pubkey(),
        rent,
        latest_blockhash,
    )];

    let mint_balance = rpc_client.get_balance(&mint_keypair.pubkey()).unwrap();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string(), bob.pubkey().to_string()],
                })],
                post_execution_accounts_configs: vec![Some(RpcSimulateTransactionAccountsConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    addresses: vec![mint_keypair.pubkey().to_string(), bob.pubkey().to_string()],
                })],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap()
        .value;
    assert_eq!(
        simulate_result.summary,
        RpcBundleSimulationSummary::Succeeded
    );
    assert_eq!(simulate_result.transaction_results.len(), 1);
    let result = simulate_result.transaction_results.first().unwrap();
    assert_eq!(result.err, None);
    assert_eq!(result.fee, Some(5000));
    assert_eq!(result.pre_balances, Some(vec![mint_balance, 0, 1]));
    assert_eq!(
        result.post_balances,
        Some(vec![
            mint_balance.saturating_sub(rent).saturating_sub(5000),
            rent,
            1
        ])
    );
    assert_eq!(result.pre_token_balances, Some(vec![]));
    assert_eq!(result.post_token_balances, Some(vec![]));
    assert!(result.loaded_accounts_data_size.is_some());
    let loaded_addresses = result.loaded_addresses.as_ref().unwrap();
    assert!(loaded_addresses.readonly.is_empty());
    assert!(loaded_addresses.writable.is_empty());

    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 2);
    assert_eq!(pre_execution_accounts[0].lamports, mint_balance); // mint keypair balance
    assert_eq!(pre_execution_accounts[1].lamports, 0); // bob balance

    // mint keypair covers cost of rent for bob
    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 2);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance.saturating_sub(rent).saturating_sub(5000)
    );
    assert_eq!(post_execution_accounts[1].lamports, rent);
}

fn test_chained_transfers_ok(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let bob = Keypair::new();
    let alice = Keypair::new();
    let transactions = vec![
        system_transaction::transfer(
            mint_keypair,
            &bob.pubkey(),
            rent.saturating_mul(2).saturating_add(5000),
            latest_blockhash,
        ),
        system_transaction::transfer(&bob, &alice.pubkey(), rent, latest_blockhash),
    ];

    let mint_balance = rpc_client.get_balance(&mint_keypair.pubkey()).unwrap();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![
                    Some(RpcSimulateTransactionAccountsConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        addresses: vec![mint_keypair.pubkey().to_string()],
                    }),
                    Some(RpcSimulateTransactionAccountsConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        addresses: vec![
                            mint_keypair.pubkey().to_string(),
                            bob.pubkey().to_string(),
                        ],
                    }),
                ],
                post_execution_accounts_configs: vec![
                    Some(RpcSimulateTransactionAccountsConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        addresses: vec![
                            mint_keypair.pubkey().to_string(),
                            bob.pubkey().to_string(),
                        ],
                    }),
                    Some(RpcSimulateTransactionAccountsConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        addresses: vec![
                            mint_keypair.pubkey().to_string(),
                            bob.pubkey().to_string(),
                            alice.pubkey().to_string(),
                        ],
                    }),
                ],
                ..bundle_config(transactions.len())
            },
        )
        .unwrap()
        .value;

    assert_eq!(
        simulate_result.summary,
        RpcBundleSimulationSummary::Succeeded
    );
    assert_eq!(simulate_result.transaction_results.len(), 2);

    let result = simulate_result.transaction_results.first().unwrap();
    assert_eq!(result.err, None);
    assert_eq!(result.fee, Some(5000));
    assert_eq!(result.pre_balances, Some(vec![mint_balance, 0, 1]));
    assert_eq!(
        result.post_balances,
        Some(vec![
            mint_balance
                .saturating_sub(rent.saturating_mul(2))
                .saturating_sub(10_000), // mint tx fee + extra fees for bob tx
            rent.saturating_mul(2).saturating_add(5000),
            1,
        ])
    );
    assert_eq!(result.pre_token_balances, Some(vec![]));
    assert_eq!(result.post_token_balances, Some(vec![]));
    assert!(result.loaded_accounts_data_size.is_some());
    let loaded_addresses = result.loaded_addresses.as_ref().unwrap();
    assert!(loaded_addresses.readonly.is_empty());
    assert!(loaded_addresses.writable.is_empty());
    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 1);
    assert_eq!(pre_execution_accounts[0].lamports, mint_balance); // mint
    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 2);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance
            .saturating_sub(rent.saturating_mul(2))
            .saturating_sub(10_000)
    );
    assert_eq!(
        post_execution_accounts[1].lamports,
        rent.saturating_mul(2).saturating_add(5000)
    ); // bob now has 2x rent

    let result = simulate_result.transaction_results.get(1).unwrap();
    assert_eq!(result.err, None);
    assert_eq!(result.fee, Some(5000));
    assert_eq!(
        result.pre_balances,
        Some(vec![rent.saturating_mul(2).saturating_add(5000), 0, 1])
    );
    assert_eq!(result.post_balances, Some(vec![rent, rent, 1]));
    assert_eq!(result.pre_token_balances, Some(vec![]));
    assert_eq!(result.post_token_balances, Some(vec![]));
    assert!(result.loaded_accounts_data_size.is_some());
    let loaded_addresses = result.loaded_addresses.as_ref().unwrap();
    assert!(loaded_addresses.readonly.is_empty());
    assert!(loaded_addresses.writable.is_empty());
    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 2);
    assert_eq!(
        pre_execution_accounts[0].lamports,
        mint_balance
            .saturating_sub(rent.saturating_mul(2))
            .saturating_sub(10_000)
    ); // mint
    assert_eq!(
        pre_execution_accounts[1].lamports,
        rent.saturating_mul(2).saturating_add(5000)
    ); // bob

    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 3);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance
            .saturating_sub(rent.saturating_mul(2))
            .saturating_sub(10_000)
    ); // mint
    assert_eq!(post_execution_accounts[1].lamports, rent); // bob sent rent to alice
    assert_eq!(post_execution_accounts[2].lamports, rent); // alice
}

fn test_single_bad_tx(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let account_not_found_tx = system_transaction::transfer(
        &Keypair::new(),
        &mint_keypair.pubkey(),
        rent.saturating_mul(2),
        latest_blockhash,
    );

    let transactions = vec![account_not_found_tx.clone()];
    let simulate_result = rpc_client
        .simulate_bundle_with_config(&transactions, bundle_config(transactions.len()))
        .unwrap()
        .value;

    assert_eq!(
        simulate_result.summary,
        RpcBundleSimulationSummary::Failed {
            error: RpcBundleExecutionError::TransactionFailure(
                account_not_found_tx.signatures[0],
                "Attempt to debit an account but found no record of a prior credit.".to_string()
            ),
            tx_signature: Some(account_not_found_tx.signatures[0].to_string())
        }
    );
    assert_eq!(simulate_result.transaction_results.len(), 1);
    let result = simulate_result.transaction_results.first().unwrap();
    assert_eq!(result.err, Some(TransactionError::AccountNotFound));
    assert!(result.loaded_accounts_data_size.is_some());
    let loaded_addresses = result.loaded_addresses.as_ref().unwrap();
    assert!(loaded_addresses.readonly.is_empty());
    assert!(loaded_addresses.writable.is_empty());
}

fn test_last_tx_fails(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let transactions = vec![
        system_transaction::transfer(mint_keypair, &Pubkey::new_unique(), rent, latest_blockhash),
        system_transaction::transfer(
            &Keypair::new(),
            &mint_keypair.pubkey(),
            rent,
            latest_blockhash,
        ),
        system_transaction::transfer(mint_keypair, &Pubkey::new_unique(), rent, latest_blockhash),
    ];

    let bad_tx_signature = *transactions.get(1).unwrap().signatures.first().unwrap();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(&transactions, bundle_config(transactions.len()))
        .unwrap()
        .value;

    assert_eq!(
        simulate_result.summary,
        RpcBundleSimulationSummary::Failed {
            error: RpcBundleExecutionError::TransactionFailure(
                bad_tx_signature,
                "Attempt to debit an account but found no record of a prior credit.".to_string()
            ),
            tx_signature: Some(bad_tx_signature.to_string())
        }
    );
    // should get results back for only the first and second one
    assert_eq!(simulate_result.transaction_results.len(), 2);
    let result = simulate_result.transaction_results.first().unwrap();
    assert_eq!(result.err, None);

    let result = simulate_result.transaction_results.get(1).unwrap();
    assert_eq!(result.err, Some(TransactionError::AccountNotFound));
}

fn test_program_execution_error(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let (latest_blockhash, rent) = bundle_transaction_inputs(rpc_client);

    let kp = Keypair::new();
    let transactions = vec![
        system_transaction::transfer(
            mint_keypair,
            &kp.pubkey(),
            rent.saturating_mul(2),
            latest_blockhash,
        ),
        system_transaction::transfer(&kp, &new_rand(), rent.saturating_add(1), latest_blockhash),
    ];

    let bad_tx_signature = *transactions.get(1).unwrap().signatures.first().unwrap();

    let simulate_result = rpc_client
        .simulate_bundle_with_config(&transactions, bundle_config(transactions.len()))
        .unwrap()
        .value;

    assert_eq!(
        simulate_result.summary,
        RpcBundleSimulationSummary::Failed {
            error: RpcBundleExecutionError::TransactionFailure(
                bad_tx_signature,
                "Transaction results in an account (0) with insufficient funds for rent"
                    .to_string()
            ),
            tx_signature: Some(bad_tx_signature.to_string())
        }
    );
    // should get results back for only the first and second one
    assert_eq!(simulate_result.transaction_results.len(), 2);
    let result = simulate_result.transaction_results.first().unwrap();
    assert_eq!(result.err, None);

    let result = simulate_result.transaction_results.get(1).unwrap();
    assert_eq!(
        result.err,
        Some(TransactionError::InsufficientFundsForRent { account_index: 0 })
    );
}
