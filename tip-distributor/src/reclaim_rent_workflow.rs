use {
    crate::send_transactions_with_retry,
    anchor_lang::AccountDeserialize,
    log::info,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_program::pubkey::Pubkey,
    solana_sdk::{
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    std::{
        error::Error,
        time::{Duration, Instant},
    },
    tip_distribution::{
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
};

pub async fn reclaim_rent(
    rpc_client: RpcClient,
    tip_distribution_program_id: Pubkey,
    signer: Keypair,
    // Optionally reclaim TipDistributionAccount rents on behalf of validators.
    should_reclaim_tdas: bool,
) -> Result<(), Box<dyn Error>> {
    info!("fetching program accounts...");
    let now = Instant::now();
    let accounts = rpc_client
        .get_program_accounts(&tip_distribution_program_id)
        .await?;
    info!(
        "get_program_accounts took {}ms and fetched {} accounts",
        now.elapsed().as_millis(),
        accounts.len()
    );

    info!("fetching current_epoch...");
    let current_epoch = rpc_client.get_epoch_info().await?.epoch;
    info!("current_epoch: {current_epoch}");

    info!("fetching config_account...");
    let now = Instant::now();
    let config_pubkey = derive_config_account_address(&tip_distribution_program_id).0;
    let config_account = rpc_client.get_account(&config_pubkey).await?;
    let config_account: Config = Config::try_deserialize(&mut config_account.data.as_slice())?;
    info!("fetch config_account took {}ms", now.elapsed().as_millis());

    info!("filtering for claim_status accounts");
    let claim_status_accounts: Vec<(Pubkey, ClaimStatus)> = accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            let claim_status = ClaimStatus::try_deserialize(&mut account.data.as_slice()).ok()?;
            Some((*pubkey, claim_status))
        })
        .filter(|(_, claim_status): &(Pubkey, ClaimStatus)| {
            // Only return claim statuses that we've paid for and ones that are expired to avoid transaction failures.
            claim_status.claim_status_payer == signer.pubkey()
                && current_epoch > claim_status.expires_at
        })
        .collect::<Vec<_>>();
    info!(
        "{} claim_status accounts eligible for rent reclaim",
        claim_status_accounts.len()
    );

    info!("fetching recent_blockhash");
    let now = Instant::now();
    let recent_blockhash = rpc_client.get_latest_blockhash().await?;
    info!(
        "fetch recent_blockhash took {}ms, hash={recent_blockhash:?}",
        now.elapsed().as_millis()
    );

    info!("creating close_claim_status_account transactions");
    let now = Instant::now();
    let mut transactions = claim_status_accounts
        .into_iter()
        .map(|(claim_status_pubkey, claim_status)| {
            close_claim_status_ix(
                tip_distribution_program_id,
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
        .into_iter()
        .map(|instructions| {
            Transaction::new_signed_with_payer(
                instructions,
                Some(&signer.pubkey()),
                &[&signer],
                recent_blockhash,
            )
        })
        .collect::<Vec<_>>();

    info!(
        "create close_claim_status_account transactions took {}us",
        now.elapsed().as_micros()
    );

    if should_reclaim_tdas {
        let tip_distribution_accounts = accounts
            .into_iter()
            .filter_map(|(pubkey, account)| {
                let tda =
                    TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).ok()?;
                Some((pubkey, tda))
            })
            .filter(|(_, tda): &(Pubkey, TipDistributionAccount)| current_epoch > tda.expires_at);

        info!("creating close_tip_distribution_account transactions");
        let now = Instant::now();
        let close_tda_txs = tip_distribution_accounts
            .map(
                |(tip_distribution_account, tda): (Pubkey, TipDistributionAccount)| {
                    close_tip_distribution_account_ix(
                        tip_distribution_program_id,
                        CloseTipDistributionAccountArgs {
                            _epoch: tda.epoch_created_at,
                        },
                        CloseTipDistributionAccounts {
                            config: config_pubkey,
                            tip_distribution_account,
                            validator_vote_account: tda.validator_vote_account,
                            expired_funds_account: config_account.expired_funds_account,
                            signer: signer.pubkey(),
                        },
                    )
                },
            )
            .collect::<Vec<_>>()
            .chunks(4)
            .map(|instructions| {
                Transaction::new_signed_with_payer(
                    instructions,
                    Some(&signer.pubkey()),
                    &[&signer],
                    recent_blockhash,
                )
            })
            .collect::<Vec<_>>();
        info!("create close_tip_distribution_account transactions took {}us, closing {} tip distribution accounts", now.elapsed().as_micros(), close_tda_txs.len());

        transactions.extend(close_tda_txs);
    }

    info!("sending {} transactions", transactions.len());
    let num_failed_txs = send_transactions_with_retry(
        &rpc_client,
        transactions.as_slice(),
        Duration::from_secs(60),
    )
    .await;
    if num_failed_txs != 0 {
        panic!("failed to send {num_failed_txs} transactions");
    }

    Ok(())
}
