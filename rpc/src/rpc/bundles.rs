use {
    super::*,
    solana_rpc_client_api::bundles::{
        RpcBundleExecutionError, RpcBundleRequest, RpcBundleSimulationSummary,
        RpcSimulateBundleConfig, RpcSimulateBundleResult, RpcSimulateBundleTransactionResult,
        SimulationSlotConfig,
    },
    solana_transaction_status::UiLoadedAddresses,
};

pub(super) fn simulate_bundle(
    meta: JsonRpcRequestProcessor,
    rpc_bundle_request: RpcBundleRequest,
    config: Option<RpcSimulateBundleConfig>,
) -> Result<RpcResponse<RpcSimulateBundleResult>> {
    const MAX_BUNDLES_SIMULATED: usize = 20;

    if rpc_bundle_request.encoded_transactions.len() > MAX_BUNDLES_SIMULATED {
        return Err(Error::invalid_params(
            "bundle size too large, max 20 transactions",
        ));
    }

    debug!("simulate_bundle rpc request received");

    let config = config.unwrap_or_else(|| RpcSimulateBundleConfig {
        pre_execution_accounts_configs: vec![None; rpc_bundle_request.encoded_transactions.len()],
        post_execution_accounts_configs: vec![None; rpc_bundle_request.encoded_transactions.len()],
        ..RpcSimulateBundleConfig::default()
    });

    let RpcSimulateBundleConfig {
        pre_execution_accounts_configs,
        post_execution_accounts_configs,
        transaction_encoding,
        simulation_bank,
        skip_sig_verify,
        replace_recent_blockhash,
    } = config;

    // Run some request validations
    if !(pre_execution_accounts_configs.len() == rpc_bundle_request.encoded_transactions.len()
        && post_execution_accounts_configs.len() == rpc_bundle_request.encoded_transactions.len())
    {
        return Err(Error::invalid_params(
            "pre/post_execution_accounts_configs must be equal in length to the number of \
             transactions",
        ));
    }

    // base58 is slow
    if let Some(transaction_encoding) = transaction_encoding
        && transaction_encoding != UiTransactionEncoding::Base64
    {
        return Err(Error::invalid_params(
            "Base64 is the only supported encoding for transactions",
        ));
    }
    for config in pre_execution_accounts_configs.iter() {
        if config
            .as_ref()
            .and_then(|c| c.encoding)
            .unwrap_or(UiAccountEncoding::Base64)
            != UiAccountEncoding::Base64
        {
            return Err(Error::invalid_params(
                "Base64 is the only supported encoding for pre-execution accounts",
            ));
        }
    }
    for config in post_execution_accounts_configs.iter() {
        if config
            .as_ref()
            .and_then(|c| c.encoding)
            .unwrap_or(UiAccountEncoding::Base64)
            != UiAccountEncoding::Base64
        {
            return Err(Error::invalid_params(
                "Base64 is the only supported encoding for post-execution accounts",
            ));
        }
    }

    let tx_encoding = transaction_encoding.unwrap_or(UiTransactionEncoding::Base64);
    let binary_encoding = tx_encoding.into_binary_encoding().ok_or_else(|| {
        Error::invalid_params(format!(
            "Unsupported encoding: {tx_encoding:?}. Supported encodings are: base58 & \
             base64",
        ))
    })?;
    let mut packet_hashes = HashSet::with_capacity(rpc_bundle_request.encoded_transactions.len());
    let mut unsanitized_txs = Vec::with_capacity(rpc_bundle_request.encoded_transactions.len());
    for encoded_tx in rpc_bundle_request.encoded_transactions {
        let tx = decode_and_deserialize::<VersionedTransaction>(encoded_tx, binary_encoding)
            .map(|(_bytes, txn)| txn)?;
        if !packet_hashes.insert(tx.message.hash()) {
            return Err(Error::invalid_params("duplicate transactions"));
        }
        unsanitized_txs.push(tx);
    }

    let bank = match simulation_bank.unwrap_or_default() {
        SimulationSlotConfig::Commitment(commitment) => Ok(meta.bank(Some(commitment))),
        SimulationSlotConfig::Slot(slot) => meta.bank_from_slot(slot).ok_or_else(|| {
            Error::invalid_params(format!("bank not found for the provided slot: {slot}"))
        }),
        SimulationSlotConfig::Tip => Ok(meta.bank_forks.read().unwrap().working_bank()),
    }?;

    // Ensure an excessive amount of accounts are not requested per transaction
    let max_accounts = bank.get_transaction_account_lock_limit();
    if pre_execution_accounts_configs.iter().any(|config| {
        if let Some(config) = config {
            config.addresses.len() > max_accounts
        } else {
            false
        }
    }) {
        return Err(Error::invalid_params(format!(
            "Too many accounts provided; max {max_accounts}"
        )));
    }
    if post_execution_accounts_configs.iter().any(|config| {
        if let Some(config) = config {
            config.addresses.len() > max_accounts
        } else {
            false
        }
    }) {
        return Err(Error::invalid_params(format!(
            "Too many accounts provided; max {max_accounts}"
        )));
    }

    let mut blockhash: Option<RpcBlockhash> = None;
    if replace_recent_blockhash {
        if !skip_sig_verify {
            return Err(Error::invalid_params(
                "replace_recent_blockhash cannot be used with !skip_sig_verify",
            ));
        }
        let recent_blockhash = bank.last_blockhash();
        let last_valid_block_height = bank
            .get_blockhash_last_valid_block_height(&recent_blockhash)
            .expect("bank blockhash queue should contain blockhash");
        blockhash.replace(RpcBlockhash {
            blockhash: recent_blockhash.to_string(),
            last_valid_block_height,
        });
        unsanitized_txs.iter_mut().for_each(|tx| {
            tx.message.set_recent_blockhash(recent_blockhash);
        });
    }

    let is_limit_instruction_accounts_active = bank
        .feature_set
        .is_active(&agave_feature_set::limit_instruction_accounts::id());

    let transactions = unsanitized_txs
        .into_iter()
        .map(|unsanitized_tx| {
            sanitize_transaction(
                unsanitized_tx,
                bank.as_ref(),
                bank.get_reserved_account_keys(),
                is_limit_instruction_accounts_active,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    if !config.skip_sig_verify {
        for tx in &transactions {
            if let Err(e) = tx.verify() {
                return Err(Error::invalid_params(format!(
                    "transaction signature is invalid: {e}",
                )));
            }
        }
    }

    let pre_execution_accounts = account_configs_to_accounts(&pre_execution_accounts_configs)?;
    let post_execution_accounts = account_configs_to_accounts(&post_execution_accounts_configs)?;

    let results = bank.simulate_transactions_unchecked_with_pre_accounts(
        &transactions,
        &pre_execution_accounts,
        &post_execution_accounts,
        Some(1_000),
    );
    let result = RpcSimulateBundleResult {
        // if any of them errored out, return the first one that did
        summary: if let Some((tx, (_pre_accounts, result, _post_accounts))) = transactions
            .iter()
            .zip(results.iter())
            .find(|(_tx, (_pre_accounts, result, _post_accounts))| result.result.is_err())
        {
            RpcBundleSimulationSummary::Failed {
                error: RpcBundleExecutionError::TransactionFailure(
                    *tx.signature(),
                    result.result.as_ref().err().unwrap().to_string(),
                ),
                tx_signature: Some(tx.signature().to_string()),
            }
        } else {
            RpcBundleSimulationSummary::Succeeded
        },
        transaction_results: transactions
            .into_iter()
            .zip(results)
            .map(|(tx, (pre_accounts, result, post_accounts))| {
                Ok(RpcSimulateBundleTransactionResult {
                    err: result.result.err(),
                    logs: Some(result.logs),
                    pre_execution_accounts: Some(
                        pre_accounts
                            .iter()
                            .map(|(address, data)| {
                                encode_account(
                                    data,
                                    address,
                                    UiAccountEncoding::Base64,
                                    None,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                    post_execution_accounts: Some(
                        post_accounts
                            .iter()
                            .map(|(address, data)| {
                                encode_account(
                                    data,
                                    address,
                                    UiAccountEncoding::Base64,
                                    None,
                                )
                            })
                            .collect::<Result<Vec<_>>>()?,
                    ),
                    units_consumed: Some(result.units_consumed),
                    loaded_accounts_data_size: Some(result.loaded_accounts_data_size),
                    return_data: result.return_data.map(|return_data| return_data.into()),
                    replacement_blockhash: blockhash.clone(),
                    fee: result.fee,
                    pre_balances: result.pre_balances,
                    post_balances: result.post_balances,
                    pre_token_balances: result.pre_token_balances.map(|balances| {
                        balances.into_iter().map(|balance| solana_runtime::transaction_balances::svm_token_info_to_token_balance(balance).into()).collect()
                    }),
                    post_token_balances: result.post_token_balances.map(|balances| {
                        balances.into_iter().map(|balance| solana_runtime::transaction_balances::svm_token_info_to_token_balance(balance).into()).collect()
                    }),
                    loaded_addresses: Some(UiLoadedAddresses::from(
                        &tx.get_loaded_addresses(),
                    )),
                })
            })
            .collect::<Result<Vec<_>>>()?,
    };
    Ok(new_response(&bank, result))
}
