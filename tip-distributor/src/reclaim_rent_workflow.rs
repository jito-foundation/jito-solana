use {
    crate::{
        claim_mev_workflow::ClaimMevError, reclaim_rent_workflow::ClaimMevError::AnchorError,
        sign_and_send_transactions_with_retries_multi_rpc,
    },
    anchor_lang::AccountDeserialize,
    itertools::Itertools,
    jito_tip_distribution::{
        sdk::{
            derive_config_account_address,
            instruction::{
                close_claim_status_ix, close_tip_distribution_account_ix, CloseClaimStatusAccounts,
                CloseClaimStatusArgs, CloseTipDistributionAccountArgs,
                CloseTipDistributionAccounts,
            },
        },
        state::{ClaimStatus, Config, TipDistributionAccount},
    },
    log::info,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_measure::measure,
    solana_metrics::datapoint_info,
    solana_program::pubkey::Pubkey,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

/// Clear old ClaimStatus accounts
pub async fn reclaim_rent(
    rpc_url: String,
    rpc_send_connection_count: u64,
    tip_distribution_program_id: Pubkey,
    signer: Arc<Keypair>,
    max_loop_retries: u64,
    max_loop_duration: Duration,
    // Optionally reclaim TipDistributionAccount rents on behalf of validators.
    should_reclaim_tdas: bool,
) -> Result<(), ClaimMevError> {
    let blockhash_rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(180), // 3 mins
        CommitmentConfig::finalized(),
    ));
    let rpc_clients = Arc::new(
        (0..rpc_send_connection_count)
            .map(|_| {
                Arc::new(RpcClient::new_with_commitment(
                    rpc_url.clone(),
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect_vec(),
    );
    let mut retries = 0;
    let mut failed_transaction_count = 0usize;
    let signer_pubkey = signer.pubkey();
    loop {
        let (transactions, get_pa_elapsed, transaction_prepare_elaspsed) = build_transactions(
            blockhash_rpc_client.clone(),
            &tip_distribution_program_id,
            &signer_pubkey,
            should_reclaim_tdas,
        )
        .await?;
        datapoint_info!(
            "claim_mev_workflow-prepare_rent_reclaim_transactions",
            ("attempt", retries, i64),
            ("transaction_count", transactions.len(), i64),
            ("account_fetch_latency_us", get_pa_elapsed.as_micros(), i64),
            (
                "transaction_prepare_latency_us",
                transaction_prepare_elaspsed.as_micros(),
                i64
            ),
        );
        let transactions_len = transactions.len();
        if transactions.is_empty() {
            info!("Finished reclaim rent after {retries} retries, {failed_transaction_count} failed requests.");
            return Ok(());
        }

        info!("Sending {} rent reclaim transactions", transactions.len());
        let send_start = Instant::now();
        let (remaining_transaction_count, new_failed_transaction_count) =
            sign_and_send_transactions_with_retries_multi_rpc(
                &signer,
                &blockhash_rpc_client,
                &rpc_clients,
                transactions,
                max_loop_duration,
            )
            .await;
        failed_transaction_count =
            failed_transaction_count.saturating_add(new_failed_transaction_count);

        datapoint_info!(
            "claim_mev_workflow-send_reclaim_rent_transactions",
            ("attempt", retries, i64),
            ("transaction_count", transactions_len, i64),
            (
                "successful_transaction_count",
                transactions_len.saturating_sub(remaining_transaction_count),
                i64
            ),
            (
                "remaining_transaction_count",
                remaining_transaction_count,
                i64
            ),
            (
                "failed_transaction_count",
                new_failed_transaction_count,
                i64
            ),
            ("send_latency_us", send_start.elapsed().as_micros(), i64),
        );

        if retries >= max_loop_retries {
            return Err(ClaimMevError::MaxSendTransactionRetriesExceeded {
                attempts: max_loop_retries,
                remaining_transaction_count,
                failed_transaction_count,
            });
        }
        retries = retries.saturating_add(1);
    }
}

async fn build_transactions(
    rpc_client: Arc<RpcClient>,
    tip_distribution_program_id: &Pubkey,
    signer_pubkey: &Pubkey,
    should_reclaim_tdas: bool,
) -> Result<(Vec<Transaction>, Duration, Duration), ClaimMevError> {
    info!("Fetching program accounts");
    let (accounts, get_pa_elapsed) = measure!(
        rpc_client
            .get_program_accounts(tip_distribution_program_id)
            .await?
    );
    info!(
        "Fetch get_program_accounts took {:?} and fetched {} accounts",
        get_pa_elapsed.as_duration(),
        accounts.len()
    );

    info!("Fetching current_epoch");
    let current_epoch = rpc_client.get_epoch_info().await?.epoch;
    info!("Fetch current_epoch: {current_epoch}");

    info!("Fetching Config account");
    let config_pubkey = derive_config_account_address(tip_distribution_program_id).0;
    let (config_account, elapsed) = measure!(rpc_client.get_account(&config_pubkey).await?);
    info!("Fetch Config account took {:?}", elapsed.as_duration());
    let config_account: Config =
        Config::try_deserialize(&mut config_account.data.as_slice()).map_err(AnchorError)?;

    info!("Filtering for ClaimStatus accounts");
    let claim_status_accounts: Vec<(Pubkey, ClaimStatus)> = accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            let claim_status = ClaimStatus::try_deserialize(&mut account.data.as_slice()).ok()?;
            Some((*pubkey, claim_status))
        })
        .filter(|(_, claim_status): &(Pubkey, ClaimStatus)| {
            // Only return claim statuses that we've paid for and ones that are expired to avoid transaction failures.
            claim_status.claim_status_payer.eq(signer_pubkey)
                && current_epoch > claim_status.expires_at
        })
        .collect::<Vec<_>>();
    info!(
        "{} ClaimStatus accounts eligible for rent reclaim",
        claim_status_accounts.len()
    );

    info!("Creating CloseClaimStatusAccounts transactions");
    let transaction_now = Instant::now();
    let mut transactions = claim_status_accounts
        .into_iter()
        .map(|(claim_status_pubkey, claim_status)| {
            close_claim_status_ix(
                *tip_distribution_program_id,
                CloseClaimStatusArgs,
                CloseClaimStatusAccounts {
                    config: config_pubkey,
                    claim_status: claim_status_pubkey,
                    claim_status_payer: claim_status.claim_status_payer,
                },
            )
        })
        .collect::<Vec<_>>()
        .chunks(4)
        .map(|instructions| Transaction::new_with_payer(instructions, Some(signer_pubkey)))
        .collect::<Vec<_>>();

    info!(
        "Create CloseClaimStatusAccounts transactions took {:?}",
        transaction_now.elapsed()
    );

    if should_reclaim_tdas {
        info!("Creating CloseTipDistributionAccounts transactions");
        let now = Instant::now();
        let close_tda_txs = accounts
            .into_iter()
            .filter_map(|(pubkey, account)| {
                let tda =
                    TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).ok()?;
                Some((pubkey, tda))
            })
            .filter(|(_, tda): &(Pubkey, TipDistributionAccount)| current_epoch > tda.expires_at)
            .map(|(tip_distribution_account, tda)| {
                close_tip_distribution_account_ix(
                    *tip_distribution_program_id,
                    CloseTipDistributionAccountArgs {
                        _epoch: tda.epoch_created_at,
                    },
                    CloseTipDistributionAccounts {
                        config: config_pubkey,
                        tip_distribution_account,
                        validator_vote_account: tda.validator_vote_account,
                        expired_funds_account: config_account.expired_funds_account,
                        signer: *signer_pubkey,
                    },
                )
            })
            .collect::<Vec<_>>()
            .chunks(4)
            .map(|instructions| Transaction::new_with_payer(instructions, Some(signer_pubkey)))
            .collect::<Vec<_>>();
        info!("Create CloseTipDistributionAccounts transactions took {:?}, closing {} tip distribution accounts", now.elapsed(), close_tda_txs.len());

        transactions.extend(close_tda_txs);
    }
    Ok((
        transactions,
        get_pa_elapsed.as_duration(),
        transaction_now.elapsed(),
    ))
}
