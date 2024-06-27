use log::warn;
use reqwest::{header::HeaderMap, redirect::Policy, Client, Error, Response};
use serde_json::{json, Value};
use solana_sdk::{bs58, transaction::Transaction};
use thiserror::Error;

const BUNDLE_METHOD: &str = "sendBundle";

/// Reasons a transaction might be rejected.
#[derive(Error, Debug)]
pub enum BundleError {
    /// The bank has seen at least one of the included transactions before.
    #[error("This bundle contains a transaction that has already been processed")]
    AlreadyProcessed,

    /// The bank has not seen the given `recent_blockhash` or the transaction is too old and
    /// the `recent_blockhash` has been discarded.
    #[error("Blockhash not found")]
    BlockhashNotFound,

    /// Bundle contains more than the max number of transactions allowed"
    #[error("Bundle contains more than the max number of transactions allowed")]
    TooManyTransactions,

    /// Bundle contains a duplicate transaction that is not allowed
    #[error("Bundle contains a duplicate transaction")]
    DuplicateTransaction,

    /// Bundle contains no tips
    #[error("Bundle does not tip")]
    NoTip,

    /// Failed to send the rpc request
    #[error("Failed to send the rpc request")]
    HttpSendFailed,

    /// Failed to parse the rpc response
    #[error("Failed to parse the rpc response")]
    HttpResponseMalformed,

    /// Failed to parse body into json from the rpc response
    #[error("Failed to parse body into json from the rpc response")]
    HttpResponseJsonParseFailed,

    /// Other reported errors
    #[error("Other errors reported")]
    Other,
}

fn generate_json_rpc_headers(api_key: &Option<String>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("Content-Type", "application/json".parse().unwrap());
    if let Some(api_key) = api_key {
        headers.insert("x-jito-auth", api_key.parse().unwrap());
    }
    headers
}

fn generate_jsonrpc<T>(method: &str, id: i64, params: T) -> String
where
    T: serde::Serialize,
{
    let params_value: Value = serde_json::to_value(params).unwrap_or(Value::Null);

    let params_array = match params_value {
        Value::Array(_) => params_value,
        _ => json!([params_value]),
    };

    format!(
        r#"{{"jsonrpc": "2.0", "method": "{}", "params": {}, "id": {}}}"#,
        method, params_array, id
    )
}

fn generate_error_code(result: &Value) -> BundleError {
    warn!("json rpc err: {:?}", result);
    match result.get("error") {
        Some(Value::String(err)) => {
            if err.contains("bundle exceeds max transaction length") {
                return BundleError::TooManyTransactions;
            }
            if err.contains("bundles must not contain any duplicate transactions and every transaction must be signed") {
                return BundleError::DuplicateTransaction;
            }
            if err.contains("bundle does not lock any of the tip PDAs") {
                return BundleError::NoTip;
            }
            if err.contains("bundle contains an already processed transaction") {
                return BundleError::AlreadyProcessed;
            }
            if err.contains("bundle contains an expired blockhash") {
                return BundleError::BlockhashNotFound;
            }
            if err.contains("bundle contains an invalid blockhash") {
                return BundleError::BlockhashNotFound;
            }
            return BundleError::Other;
        }
        Some(_) => {
            return BundleError::HttpResponseJsonParseFailed;
        }
        None => {
            return BundleError::HttpResponseJsonParseFailed;
        }
    }
}

/// Converts a VersionedTransaction to a protobuf packet
pub fn proto_packet_from_versioned_tx(
    tx: &solana_sdk::transaction::VersionedTransaction,
) -> Vec<u8> {
    bincode::serialize(tx).expect("serializes")
}

async fn send_json_rpc_request(
    client: &Client,
    url: &str,
    payload: String,
    headers: HeaderMap,
) -> Result<Response, Error> {
    client.post(url).headers(headers).body(payload).send().await
}

pub async fn send_bundle(
    transactions: &[&Transaction],
    client: &Client,
    url: &str,
    api_key: &Option<String>,
) -> Result<String, BundleError> {
    let mut bundle = Vec::new();
    for transaction in transactions {
        bundle.push(Value::String(
            bs58::encode(proto_packet_from_versioned_tx(
                &solana_sdk::transaction::VersionedTransaction::from((*transaction).clone()),
            ))
            .into_string(),
        ))
    }

    // Generate the headers and payload and send
    let response = send_json_rpc_request(
        client,
        url,
        generate_jsonrpc(BUNDLE_METHOD, 1, vec![Value::Array(bundle)]),
        generate_json_rpc_headers(api_key),
    )
    .await
    .map_err(|err| {
        warn!("http send failed: err {:?}", err);
        BundleError::HttpSendFailed
    })?
    .text()
    .await
    .map_err(|err| {
        warn!("http response could not be parsed to text : err {:?}", err);
        BundleError::HttpResponseMalformed
    })?;

    let result: Value = serde_json::from_str(response.as_str()).map_err(|err| {
        warn!(
            "err deserialization: {:?}, response: {:?}",
            err.to_string(),
            response
        );
        BundleError::HttpResponseJsonParseFailed
    })?;

    // If bundle id present, check the value, else make sure the error is the expected one
    match result.get("result") {
        Some(Value::String(bundle_id)) => {
            return Ok(bundle_id.clone());
        }
        Some(val) => {
            warn!("err result: {:?} not string: {:?}", result, val.to_string());
            return Err(BundleError::HttpResponseJsonParseFailed);
        }
        None => {
            let err = generate_error_code(&result);
            Err(err)
        }
    }
}
