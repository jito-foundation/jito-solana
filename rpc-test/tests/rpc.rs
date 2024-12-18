use {
    assert_matches::assert_matches,
    bincode::serialize,
    crossbeam_channel::unbounded,
    futures_util::StreamExt,
    log::*,
    reqwest::{self, header::CONTENT_TYPE},
    serde_json::{json, Value},
    solana_account_decoder::{UiAccount, UiAccountEncoding},
    solana_client::{
        connection_cache::ConnectionCache, rpc_config::RpcSimulateTransactionAccountsConfig,
        rpc_request::RpcResponseErrorData,
    },
    solana_commitment_config::CommitmentConfig,
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_net_utils::sockets::bind_to_localhost_unique,
    solana_pubkey::{new_rand, Pubkey},
    solana_pubsub_client::nonblocking::pubsub_client::PubsubClient,
    solana_rent::Rent,
    solana_rpc_client::rpc_client::RpcClient,
    solana_rpc_client_api::{
        bundles::{
            RpcBundleExecutionError, RpcBundleSimulationSummary, RpcSimulateBundleConfig,
            SimulationSlotConfig,
        },
        client_error::{ErrorKind as ClientErrorKind, Result as ClientResult},
        config::{RpcAccountInfoConfig, RpcSignatureSubscribeConfig, RpcSimulateTransactionConfig},
        request::RpcError,
        response::{Response as RpcResponse, RpcSignatureResult, SlotUpdate},
    },
    solana_signature::Signature,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_system_transaction as system_transaction,
    solana_test_validator::TestValidator,
    solana_tpu_client::tpu_client::{TpuClient, TpuClientConfig, DEFAULT_TPU_CONNECTION_POOL_SIZE},
    solana_transaction::{Transaction, TransactionError},
    solana_transaction_status::{TransactionStatus, UiTransactionEncoding},
    std::{
        collections::HashSet,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
        vec,
    },
    tokio::runtime::Runtime,
};

macro_rules! json_req {
    ($method: expr, $params: expr) => {{
        json!({
           "jsonrpc": "2.0",
           "id": 1,
           "method": $method,
           "params": $params,
        })
    }}
}

fn post_rpc(request: Value, rpc_url: &str) -> Value {
    let client = reqwest::blocking::Client::new();
    let response = client
        .post(rpc_url)
        .header(CONTENT_TYPE, "application/json")
        .body(request.to_string())
        .send()
        .unwrap();
    serde_json::from_str(&response.text().unwrap()).unwrap()
}

#[test]
fn test_rpc_send_tx() {
    agave_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_url = test_validator.rpc_url();

    let bob_pubkey = solana_pubkey::new_rand();

    let req = json_req!("getLatestBlockhash", json!([]));
    let json = post_rpc(req, &rpc_url);

    let blockhash: Hash = json["result"]["value"]["blockhash"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    info!("blockhash: {blockhash:?}");
    let tx = system_transaction::transfer(
        &alice,
        &bob_pubkey,
        Rent::default().minimum_balance(0),
        blockhash,
    );
    let serialized_encoded_tx = bs58::encode(serialize(&tx).unwrap()).into_string();

    let req = json_req!("sendTransaction", json!([serialized_encoded_tx]));
    let json: Value = post_rpc(req, &rpc_url);

    let signature = &json["result"];

    let mut confirmed_tx = false;

    let request = json_req!("getSignatureStatuses", [[signature]]);

    for _ in 0..solana_clock::DEFAULT_TICKS_PER_SLOT {
        let json = post_rpc(request.clone(), &rpc_url);

        let result: Option<TransactionStatus> =
            serde_json::from_value(json["result"]["value"][0].clone()).unwrap();
        if let Some(result) = result.as_ref() {
            if result.err.is_none() {
                confirmed_tx = true;
                break;
            }
        }

        sleep(Duration::from_millis(500));
    }

    assert!(confirmed_tx);

    use {
        solana_account_decoder::UiAccountEncoding,
        solana_rpc_client_api::config::RpcAccountInfoConfig,
    };
    let config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: None,
        data_slice: None,
        min_context_slot: None,
    };
    let req = json_req!(
        "getAccountInfo",
        json!([bs58::encode(bob_pubkey).into_string(), config])
    );
    let json: Value = post_rpc(req, &rpc_url);
    info!("{:?}", json["result"]["value"]);
}

#[test]
fn test_simulation_replaced_blockhash() -> ClientResult<()> {
    agave_logger::setup();

    let alice = Keypair::new();
    let validator = TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_client = RpcClient::new(validator.rpc_url());

    let bob = Keypair::new();
    let lamports = 50;

    let res = rpc_client.simulate_transaction_with_config(
        &system_transaction::transfer(&alice, &bob.pubkey(), lamports, Hash::default()),
        RpcSimulateTransactionConfig {
            replace_recent_blockhash: true,
            ..Default::default()
        },
    )?;
    assert!(
        res.value.replacement_blockhash.is_some(),
        "replaced_blockhash response is None"
    );
    let blockhash = res.value.replacement_blockhash.unwrap();
    // ensure nothing weird is going on
    assert_ne!(
        blockhash.blockhash,
        Hash::default().to_string(),
        "replaced_blockhash is default"
    );

    let res = rpc_client.simulate_transaction_with_config(
        &system_transaction::transfer(&alice, &bob.pubkey(), lamports, Hash::default()),
        RpcSimulateTransactionConfig {
            replace_recent_blockhash: false,
            ..Default::default()
        },
    )?;
    assert!(
        res.value.replacement_blockhash.is_none(),
        "replaced_blockhash is Some when nothing should be replaced"
    );

    Ok(())
}

#[test]
fn test_rpc_invalid_requests() {
    agave_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_url = test_validator.rpc_url();

    let bob_pubkey = solana_pubkey::new_rand();

    // test invalid get_balance request
    let req = json_req!("getBalance", json!(["invalid9999"]));
    let json = post_rpc(req, &rpc_url);

    let the_error = json["error"]["message"].as_str().unwrap();
    assert_eq!(the_error, "Invalid param: Invalid");

    // test invalid get_account_info request
    let req = json_req!("getAccountInfo", json!(["invalid9999"]));
    let json = post_rpc(req, &rpc_url);

    let the_error = json["error"]["message"].as_str().unwrap();
    assert_eq!(the_error, "Invalid param: Invalid");

    // test invalid get_account_info request
    let req = json_req!("getAccountInfo", json!([bob_pubkey.to_string()]));
    let json = post_rpc(req, &rpc_url);

    let the_value = &json["result"]["value"];
    assert!(the_value.is_null());
}

#[test]
fn test_rpc_slot_updates() {
    agave_logger::setup();

    let test_validator =
        TestValidator::with_no_fees(Pubkey::new_unique(), None, SocketAddrSpace::Unspecified);

    // Track when slot updates are ready
    let (update_sender, update_receiver) = unbounded::<SlotUpdate>();
    // Create the pub sub runtime
    let rt = Runtime::new().unwrap();
    let rpc_pubsub_url = test_validator.rpc_pubsub_url();

    rt.spawn(async move {
        let pubsub_client = PubsubClient::new(&rpc_pubsub_url).await.unwrap();
        let (mut slot_notifications, slot_unsubscribe) =
            pubsub_client.slot_updates_subscribe().await.unwrap();

        while let Some(slot_update) = slot_notifications.next().await {
            update_sender.send(slot_update).unwrap();
        }
        slot_unsubscribe().await;
    });

    let first_update = update_receiver
        .recv_timeout(Duration::from_secs(2))
        .unwrap();

    // Verify that updates are received in order for an upcoming slot
    let verify_slot = first_update.slot() + 2;
    let expected_updates = vec![
        "CreatedBank",
        "Frozen",
        "OptimisticConfirmation",
        "Root", // TODO: debug why root signal is sent twice.
        "Root",
    ];
    let mut expected_updates = expected_updates.into_iter().peekable();
    // SlotUpdate::Completed is sent asynchronous to banking-stage and replay
    // when shreds are inserted into blockstore. When the leader generates
    // blocks, replay may freeze the bank before shreds are all inserted into
    // blockstore; and so SlotUpdate::Completed may be received _after_
    // SlotUpdate::Frozen.
    let mut slot_update_completed = false;

    let test_start = Instant::now();
    while expected_updates.peek().is_some() || !slot_update_completed {
        assert!(test_start.elapsed() < Duration::from_secs(30));
        let update = update_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        if update.slot() == verify_slot {
            let update_name = match update {
                SlotUpdate::CreatedBank { .. } => "CreatedBank",
                SlotUpdate::Completed { .. } => {
                    slot_update_completed = true;
                    continue;
                }
                SlotUpdate::Frozen { .. } => "Frozen",
                SlotUpdate::OptimisticConfirmation { .. } => "OptimisticConfirmation",
                SlotUpdate::Root { .. } => "Root",
                _ => continue,
            };
            assert_eq!(Some(update_name), expected_updates.next());
        }
    }
}

#[test]
fn test_rpc_subscriptions() {
    agave_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees_udp(alice.pubkey(), None, SocketAddrSpace::Unspecified);

    let transactions_socket = bind_to_localhost_unique().unwrap();
    transactions_socket.connect(test_validator.tpu()).unwrap();

    let rpc_client = RpcClient::new(test_validator.rpc_url());
    let recent_blockhash = rpc_client.get_latest_blockhash().unwrap();

    // Create transaction signatures to subscribe to
    let transfer_amount = Rent::default().minimum_balance(0);
    let transactions: Vec<Transaction> = (0..1000)
        .map(|_| {
            system_transaction::transfer(
                &alice,
                &solana_pubkey::new_rand(),
                transfer_amount,
                recent_blockhash,
            )
        })
        .collect();
    let mut signature_set: HashSet<Signature> =
        transactions.iter().map(|tx| tx.signatures[0]).collect();
    let mut account_set: HashSet<Pubkey> = transactions
        .iter()
        .map(|tx| tx.message.account_keys[1])
        .collect();

    // Track account notifications are received
    let (account_sender, account_receiver) = unbounded::<(Pubkey, RpcResponse<UiAccount>)>();
    // Track when status notifications are received
    let (status_sender, status_receiver) =
        unbounded::<(Signature, RpcResponse<RpcSignatureResult>)>();

    // Create the pub sub runtime
    let rt = Runtime::new().unwrap();
    let rpc_pubsub_url = test_validator.rpc_pubsub_url();
    let signature_set_clone = signature_set.clone();
    let account_set_clone = account_set.clone();
    let signature_subscription_ready = Arc::new(AtomicUsize::new(0));
    let account_subscription_ready = Arc::new(AtomicUsize::new(0));
    let signature_subscription_ready_clone = signature_subscription_ready.clone();
    let account_subscription_ready_clone = account_subscription_ready.clone();

    rt.spawn(async move {
        let pubsub_client = Arc::new(PubsubClient::new(&rpc_pubsub_url).await.unwrap());

        // Subscribe to signature notifications
        for signature in signature_set_clone {
            let status_sender = status_sender.clone();
            let signature_subscription_ready_clone = signature_subscription_ready_clone.clone();
            tokio::spawn({
                let pubsub_client = Arc::clone(&pubsub_client);
                async move {
                    let (mut sig_notifications, sig_unsubscribe) = pubsub_client
                        .signature_subscribe(
                            &signature,
                            Some(RpcSignatureSubscribeConfig {
                                commitment: Some(CommitmentConfig::confirmed()),
                                ..RpcSignatureSubscribeConfig::default()
                            }),
                        )
                        .await
                        .unwrap();

                    signature_subscription_ready_clone.fetch_add(1, Ordering::SeqCst);

                    let response = sig_notifications.next().await.unwrap();
                    status_sender.send((signature, response)).unwrap();
                    sig_unsubscribe().await;
                }
            });
        }

        // Subscribe to account notifications
        for pubkey in account_set_clone {
            let account_sender = account_sender.clone();
            let account_subscription_ready_clone = account_subscription_ready_clone.clone();
            tokio::spawn({
                let pubsub_client = Arc::clone(&pubsub_client);
                async move {
                    let (mut account_notifications, account_unsubscribe) = pubsub_client
                        .account_subscribe(
                            &pubkey,
                            Some(RpcAccountInfoConfig {
                                commitment: Some(CommitmentConfig::confirmed()),
                                ..RpcAccountInfoConfig::default()
                            }),
                        )
                        .await
                        .unwrap();

                    account_subscription_ready_clone.fetch_add(1, Ordering::SeqCst);

                    let response = account_notifications.next().await.unwrap();
                    account_sender.send((pubkey, response)).unwrap();
                    account_unsubscribe().await;
                }
            });
        }
    });

    let now = Instant::now();
    while (signature_subscription_ready.load(Ordering::SeqCst) != transactions.len()
        || account_subscription_ready.load(Ordering::SeqCst) != transactions.len())
        && now.elapsed() < Duration::from_secs(15)
    {
        sleep(Duration::from_millis(100))
    }

    // check signature subscription
    let num = signature_subscription_ready.load(Ordering::SeqCst);
    if num != transactions.len() {
        error!(
            "signature subscription didn't setup properly, want: {}, got: {}",
            transactions.len(),
            num
        );
    }

    // check account subscription
    let num = account_subscription_ready.load(Ordering::SeqCst);
    if num != transactions.len() {
        error!(
            "account subscriptions didn't setup properly, want: {}, got: {}",
            transactions.len(),
            num
        );
    }

    let rpc_client = RpcClient::new(test_validator.rpc_url());
    let mut mint_balance = rpc_client
        .get_balance_with_commitment(&alice.pubkey(), CommitmentConfig::processed())
        .unwrap()
        .value;
    assert!(mint_balance >= transactions.len() as u64);

    // Send all transactions to tpu socket for processing
    transactions.iter().for_each(|tx| {
        transactions_socket
            .send(&bincode::serialize(&tx).unwrap())
            .unwrap();
    });

    // Track mint balance to know when transactions have completed
    let now = Instant::now();
    let expected_mint_balance = mint_balance - (transfer_amount * transactions.len() as u64);
    while mint_balance != expected_mint_balance && now.elapsed() < Duration::from_secs(15) {
        mint_balance = rpc_client
            .get_balance_with_commitment(&alice.pubkey(), CommitmentConfig::processed())
            .unwrap()
            .value;
        sleep(Duration::from_millis(100));
    }
    if mint_balance != expected_mint_balance {
        error!("mint-check timeout. mint_balance {mint_balance:?}");
    }

    // Wait for all signature subscriptions
    /* Set a large 30-sec timeout here because the timing of the above tokio process is
     * highly non-deterministic.  The test was too flaky at 15-second timeout.  Debugging
     * show occasional multi-second delay which could come from multiple sources -- other
     * tokio tasks, tokio scheduler, OS scheduler.  The async nature makes it hard to
     * track down the origin of the delay.
     */
    let deadline = Instant::now() + Duration::from_secs(30);
    while !signature_set.is_empty() {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match status_receiver.recv_timeout(timeout) {
            Ok((sig, result)) => {
                if let RpcSignatureResult::ProcessedSignature(result) = result.value {
                    assert!(result.err.is_none());
                    assert!(signature_set.remove(&sig));
                } else {
                    panic!("Unexpected result");
                }
            }
            Err(_err) => {
                panic!(
                    "recv_timeout, {}/{} signatures remaining",
                    signature_set.len(),
                    transactions.len()
                );
            }
        }
    }

    let deadline = Instant::now() + Duration::from_secs(60);
    while !account_set.is_empty() {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match account_receiver.recv_timeout(timeout) {
            Ok((pubkey, result)) => {
                assert_eq!(result.value.lamports, Rent::default().minimum_balance(0));
                assert!(account_set.remove(&pubkey));
            }
            Err(_err) => {
                panic!(
                    "recv_timeout, {}/{} accounts remaining",
                    account_set.len(),
                    transactions.len()
                );
            }
        }
    }
}

fn run_tpu_send_transaction(tpu_use_quic: bool) {
    let mint_keypair = Keypair::new();
    let mint_pubkey = mint_keypair.pubkey();
    let test_validator =
        TestValidator::with_no_fees(mint_pubkey, None, SocketAddrSpace::Unspecified);
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        test_validator.rpc_url(),
        CommitmentConfig::processed(),
    ));
    let connection_cache = if tpu_use_quic {
        ConnectionCache::new_quic_for_tests(
            "connection_cache_test",
            DEFAULT_TPU_CONNECTION_POOL_SIZE,
        )
    } else {
        ConnectionCache::with_udp("connection_cache_test", DEFAULT_TPU_CONNECTION_POOL_SIZE)
    };
    let recent_blockhash = rpc_client.get_latest_blockhash().unwrap();
    let tx =
        system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 42, recent_blockhash);
    let success = match connection_cache {
        ConnectionCache::Quic(cache) => TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            &test_validator.rpc_pubsub_url(),
            TpuClientConfig::default(),
            cache,
        )
        .unwrap()
        .send_transaction(&tx),
        ConnectionCache::Udp(cache) => TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            &test_validator.rpc_pubsub_url(),
            TpuClientConfig::default(),
            cache,
        )
        .unwrap()
        .send_transaction(&tx),
    };
    assert!(success);
    let timeout = Duration::from_secs(5);
    let now = Instant::now();
    let signatures = vec![tx.signatures[0]];
    loop {
        assert!(now.elapsed() < timeout);
        let statuses = rpc_client.get_signature_statuses(&signatures).unwrap();
        if !statuses.value.is_empty() {
            return;
        }
    }
}

#[test]
fn test_tpu_send_transaction() {
    run_tpu_send_transaction(/*tpu_use_quic*/ false)
}

#[test]
fn test_tpu_send_transaction_with_quic() {
    run_tpu_send_transaction(/*tpu_use_quic*/ true)
}

#[test]
fn deserialize_rpc_error() -> ClientResult<()> {
    agave_logger::setup();

    let alice = Keypair::new();
    let validator = TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_client = RpcClient::new(validator.rpc_url());

    let bob = Keypair::new();
    let lamports = 50;
    let blockhash = rpc_client.get_latest_blockhash()?;
    let mut tx = system_transaction::transfer(&alice, &bob.pubkey(), lamports, blockhash);

    // This will cause an error
    tx.signatures.clear();

    let err = rpc_client.send_transaction(&tx);
    let err = err.unwrap_err();

    match err.kind() {
        ClientErrorKind::RpcError(RpcError::RpcRequestError { .. }) => {
            // This is what used to happen
            panic!()
        }
        ClientErrorKind::RpcError(RpcError::RpcResponseError { .. }) => Ok(()),
        _ => {
            panic!()
        }
    }
}

#[test]
fn test_simulate_bundle() {
    agave_logger::setup();

    let mint_keypair = Keypair::new();
    let validator = TestValidator::with_custom_fees(
        mint_keypair.pubkey(),
        0,
        None,
        SocketAddrSpace::Unspecified,
    );
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
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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

    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(message, "bundle size too large, max 20 transactions");
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_number_pre_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();

    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "pre/post_execution_accounts_configs must be equal in length to the number of transactions"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_number_post_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "pre/post_execution_accounts_configs must be equal in length to the number of transactions"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_invalid_transaction_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "Base64 is the only supported encoding for transactions"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_pre_account_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "Base64 is the only supported encoding for pre-execution accounts"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_wrong_post_account_encoding(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "Base64 is the only supported encoding for post-execution accounts"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_duplicate_transactions(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

    let pubkey = Pubkey::new_unique();
    let transactions = vec![
        system_transaction::transfer(mint_keypair, &pubkey, rent, latest_blockhash),
        system_transaction::transfer(mint_keypair, &pubkey, rent, latest_blockhash),
    ];

    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len()],
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(message, "duplicate transactions");
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_replace_recent_blockhash_with_sig_verify(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "Base64 is the only supported encoding for post-execution accounts"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_signature(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "transaction signature is invalid: Transaction did not pass signature verification"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_pubkey_pre_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "invalid pubkey for pre/post accounts provided: testing123"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_bad_pubkey_post_accounts(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
        .unwrap_err();
    let ClientErrorKind::RpcError(RpcError::RpcResponseError {
        code,
        message,
        data,
    }) = simulate_result.kind()
    else {
        panic!("unexpected error");
    };
    assert_eq!(
        message,
        "invalid pubkey for pre/post accounts provided: testing123"
    );
    assert_eq!(*code, -32602);
    assert_matches!(data, &RpcResponseErrorData::Empty);
}

fn test_single_tx_ok(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
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

    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 2);
    assert_eq!(pre_execution_accounts[0].lamports, mint_balance); // mint keypair balance
    assert_eq!(pre_execution_accounts[1].lamports, 0); // bob balance

    // mint keypair covers cost of rent for bob
    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 2);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance.saturating_sub(rent)
    );
    assert_eq!(post_execution_accounts[1].lamports, rent);
}

fn test_chained_transfers_ok(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

    let bob = Keypair::new();
    let alice = Keypair::new();
    let transactions = vec![
        system_transaction::transfer(
            mint_keypair,
            &bob.pubkey(),
            rent.saturating_mul(2),
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
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
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
    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 1);
    assert_eq!(pre_execution_accounts[0].lamports, mint_balance); // mint
    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 2);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance.saturating_sub(rent.saturating_mul(2))
    );
    assert_eq!(post_execution_accounts[1].lamports, rent.saturating_mul(2)); // bob now has 2x rent

    let result = simulate_result.transaction_results.get(1).unwrap();
    assert_eq!(result.err, None);
    let pre_execution_accounts = result.pre_execution_accounts.as_ref().unwrap();
    assert_eq!(pre_execution_accounts.len(), 2);
    assert_eq!(
        pre_execution_accounts[0].lamports,
        mint_balance.saturating_sub(rent.saturating_mul(2))
    ); // mint
    assert_eq!(pre_execution_accounts[1].lamports, rent.saturating_mul(2)); // bob

    let post_execution_accounts = result.post_execution_accounts.as_ref().unwrap();
    assert_eq!(post_execution_accounts.len(), 3);
    assert_eq!(
        post_execution_accounts[0].lamports,
        mint_balance.saturating_sub(rent.saturating_mul(2))
    ); // mint
    assert_eq!(post_execution_accounts[1].lamports, rent); // bob sent rent to alice
    assert_eq!(post_execution_accounts[2].lamports, rent); // alice
}

fn test_single_bad_tx(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

    let account_not_found_tx = system_transaction::transfer(
        &Keypair::new(),
        &mint_keypair.pubkey(),
        rent.saturating_mul(2),
        latest_blockhash,
    );

    let transactions = vec![account_not_found_tx.clone()];
    let simulate_result = rpc_client
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len()],
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
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
}

fn test_last_tx_fails(rpc_client: &RpcClient, mint_keypair: &Keypair) {
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len()],
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
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
    let latest_blockhash = rpc_client.get_latest_blockhash().unwrap();

    let rent = rpc_client
        .get_minimum_balance_for_rent_exemption(0)
        .unwrap();

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
        .simulate_bundle_with_config(
            &transactions,
            RpcSimulateBundleConfig {
                pre_execution_accounts_configs: vec![None; transactions.len()],
                post_execution_accounts_configs: vec![None; transactions.len()],
                transaction_encoding: Some(UiTransactionEncoding::Base64),
                simulation_bank: Some(SimulationSlotConfig::Tip),
                skip_sig_verify: false,
                replace_recent_blockhash: false,
            },
        )
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
