use {
    bincode::serialize,
    crossbeam_channel::unbounded,
    futures_util::StreamExt,
    log::*,
    reqwest::{self, header::CONTENT_TYPE},
    serde_json::{json, Value},
    solana_account_decoder::UiAccount,
    solana_client::{
        client_error::{ClientErrorKind, Result as ClientResult},
        connection_cache::{ConnectionCache, DEFAULT_TPU_CONNECTION_POOL_SIZE},
        nonblocking::pubsub_client::PubsubClient,
        rpc_client::RpcClient,
        rpc_config::{RpcAccountInfoConfig, RpcSignatureSubscribeConfig},
        rpc_request::RpcError,
        rpc_response::{Response as RpcResponse, RpcSignatureResult, SlotUpdate},
        tpu_client::{TpuClient, TpuClientConfig},
    },
    solana_sdk::{
        commitment_config::CommitmentConfig,
        hash::Hash,
        pubkey::Pubkey,
        rent::Rent,
        signature::{Keypair, Signature, Signer},
        system_transaction,
        transaction::Transaction,
    },
    solana_streamer::socket::SocketAddrSpace,
    solana_test_validator::TestValidator,
    solana_transaction_status::TransactionStatus,
    std::{
        collections::HashSet,
        net::UdpSocket,
        sync::Arc,
        thread::sleep,
        time::{Duration, Instant},
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
    solana_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_url = test_validator.rpc_url();

    let bob_pubkey = solana_sdk::pubkey::new_rand();

    let req = json_req!("getRecentBlockhash", json!([]));
    let json = post_rpc(req, &rpc_url);

    let blockhash: Hash = json["result"]["value"]["blockhash"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    info!("blockhash: {:?}", blockhash);
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

    for _ in 0..solana_sdk::clock::DEFAULT_TICKS_PER_SLOT {
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
        solana_account_decoder::UiAccountEncoding, solana_client::rpc_config::RpcAccountInfoConfig,
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
fn test_rpc_invalid_requests() {
    solana_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);
    let rpc_url = test_validator.rpc_url();

    let bob_pubkey = solana_sdk::pubkey::new_rand();

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
    solana_logger::setup();

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
    let mut expected_update_index = 0;
    let expected_updates = vec![
        "CreatedBank",
        "Completed",
        "Frozen",
        "OptimisticConfirmation",
        "Root",
    ];

    let test_start = Instant::now();
    loop {
        assert!(test_start.elapsed() < Duration::from_secs(30));
        let update = update_receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        if update.slot() == verify_slot {
            let update_name = match update {
                SlotUpdate::CreatedBank { .. } => "CreatedBank",
                SlotUpdate::Completed { .. } => "Completed",
                SlotUpdate::Frozen { .. } => "Frozen",
                SlotUpdate::OptimisticConfirmation { .. } => "OptimisticConfirmation",
                SlotUpdate::Root { .. } => "Root",
                _ => continue,
            };
            assert_eq!(update_name, expected_updates[expected_update_index]);
            expected_update_index += 1;
            if expected_update_index == expected_updates.len() {
                break;
            }
        }
    }
}

#[test]
fn test_rpc_subscriptions() {
    solana_logger::setup();

    let alice = Keypair::new();
    let test_validator =
        TestValidator::with_no_fees(alice.pubkey(), None, SocketAddrSpace::Unspecified);

    let transactions_socket = UdpSocket::bind("0.0.0.0:0").unwrap();
    transactions_socket.connect(test_validator.tpu()).unwrap();

    let rpc_client = RpcClient::new(test_validator.rpc_url());
    let recent_blockhash = rpc_client.get_latest_blockhash().unwrap();

    // Create transaction signatures to subscribe to
    let transfer_amount = Rent::default().minimum_balance(0);
    let transactions: Vec<Transaction> = (0..1000)
        .map(|_| {
            system_transaction::transfer(
                &alice,
                &solana_sdk::pubkey::new_rand(),
                transfer_amount,
                recent_blockhash,
            )
        })
        .collect();
    let mut signature_set: HashSet<Signature> =
        transactions.iter().map(|tx| tx.signatures[0]).collect();
    let account_set: HashSet<Pubkey> = transactions
        .iter()
        .map(|tx| tx.message.account_keys[1])
        .collect();

    // Track when subscriptions are ready
    let (ready_sender, ready_receiver) = unbounded::<()>();
    // Track account notifications are received
    let (account_sender, account_receiver) = unbounded::<RpcResponse<UiAccount>>();
    // Track when status notifications are received
    let (status_sender, status_receiver) =
        unbounded::<(Signature, RpcResponse<RpcSignatureResult>)>();

    // Create the pub sub runtime
    let rt = Runtime::new().unwrap();
    let rpc_pubsub_url = test_validator.rpc_pubsub_url();
    let signature_set_clone = signature_set.clone();
    rt.spawn(async move {
        let pubsub_client = Arc::new(PubsubClient::new(&rpc_pubsub_url).await.unwrap());

        // Subscribe to signature notifications
        for signature in signature_set_clone {
            let status_sender = status_sender.clone();
            tokio::spawn({
                let _pubsub_client = Arc::clone(&pubsub_client);
                async move {
                    let (mut sig_notifications, sig_unsubscribe) = _pubsub_client
                        .signature_subscribe(
                            &signature,
                            Some(RpcSignatureSubscribeConfig {
                                commitment: Some(CommitmentConfig::confirmed()),
                                ..RpcSignatureSubscribeConfig::default()
                            }),
                        )
                        .await
                        .unwrap();

                    let response = sig_notifications.next().await.unwrap();
                    status_sender.send((signature, response)).unwrap();
                    sig_unsubscribe().await;
                }
            });
        }

        // Subscribe to account notifications
        for pubkey in account_set {
            let account_sender = account_sender.clone();
            tokio::spawn({
                let _pubsub_client = Arc::clone(&pubsub_client);
                async move {
                    let (mut account_notifications, account_unsubscribe) = _pubsub_client
                        .account_subscribe(
                            &pubkey,
                            Some(RpcAccountInfoConfig {
                                commitment: Some(CommitmentConfig::confirmed()),
                                ..RpcAccountInfoConfig::default()
                            }),
                        )
                        .await
                        .unwrap();

                    let response = account_notifications.next().await.unwrap();
                    account_sender.send(response).unwrap();
                    account_unsubscribe().await;
                }
            });
        }

        // Signal ready after the next slot notification
        tokio::spawn({
            let _pubsub_client = Arc::clone(&pubsub_client);
            async move {
                let (mut slot_notifications, slot_unsubscribe) =
                    _pubsub_client.slot_subscribe().await.unwrap();
                let _response = slot_notifications.next().await.unwrap();
                ready_sender.send(()).unwrap();
                slot_unsubscribe().await;
            }
        });
    });

    // Wait for signature subscriptions
    ready_receiver.recv_timeout(Duration::from_secs(2)).unwrap();

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
        error!("mint-check timeout. mint_balance {:?}", mint_balance);
    }

    // Wait for all signature subscriptions
    let deadline = Instant::now() + Duration::from_secs(15);
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

    let deadline = Instant::now() + Duration::from_secs(5);
    let mut account_notifications = transactions.len();
    while account_notifications > 0 {
        let timeout = deadline.saturating_duration_since(Instant::now());
        match account_receiver.recv_timeout(timeout) {
            Ok(result) => {
                assert_eq!(result.value.lamports, Rent::default().minimum_balance(0));
                account_notifications -= 1;
            }
            Err(_err) => {
                panic!(
                    "recv_timeout, {}/{} accounts remaining",
                    account_notifications,
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
    let connection_cache = match tpu_use_quic {
        true => Arc::new(ConnectionCache::new(DEFAULT_TPU_CONNECTION_POOL_SIZE)),
        false => Arc::new(ConnectionCache::with_udp(DEFAULT_TPU_CONNECTION_POOL_SIZE)),
    };
    let tpu_client = TpuClient::new_with_connection_cache(
        rpc_client.clone(),
        &test_validator.rpc_pubsub_url(),
        TpuClientConfig::default(),
        connection_cache,
    )
    .unwrap();

    let recent_blockhash = rpc_client.get_latest_blockhash().unwrap();
    let tx =
        system_transaction::transfer(&mint_keypair, &Pubkey::new_unique(), 42, recent_blockhash);
    assert!(tpu_client.send_transaction(&tx));

    let timeout = Duration::from_secs(5);
    let now = Instant::now();
    let signatures = vec![tx.signatures[0]];
    loop {
        assert!(now.elapsed() < timeout);
        let statuses = rpc_client.get_signature_statuses(&signatures).unwrap();
        if statuses.value.get(0).is_some() {
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
    solana_logger::setup();

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

    match err.kind {
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
