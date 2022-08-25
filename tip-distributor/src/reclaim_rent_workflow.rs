use {
    crate::{
        claim_mev_workflow::ClaimMevError, get_batched_accounts,
        reclaim_rent_workflow::ClaimMevError::AnchorError, send_until_blockhash_expires,
    },
    anchor_lang::AccountDeserialize,
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
    log::{info, warn},
    rand::{prelude::SliceRandom, thread_rng},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_metrics::datapoint_info,
    solana_program::{clock::Epoch, pubkey::Pubkey},
    solana_rpc_client_api::config::RpcSimulateTransactionConfig,
    solana_sdk::{
        account::Account,
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
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
    tip_distribution_program_id: Pubkey,
    signer: Arc<Keypair>,
    max_loop_duration: Duration,
    // Optionally reclaim TipDistributionAccount rents on behalf of validators.
    should_reclaim_tdas: bool,
    micro_lamports: u64,
) -> Result<(), ClaimMevError> {
    let rpc_client = RpcClient::new_with_timeout_and_commitment(
        rpc_url.clone(),
        Duration::from_secs(300),
        CommitmentConfig::processed(),
    );

    let start = Instant::now();

    let accounts = rpc_client
        .get_program_accounts(&tip_distribution_program_id)
        .await?;

    let config_pubkey = derive_config_account_address(&tip_distribution_program_id).0;
    let config_account = rpc_client.get_account(&config_pubkey).await?;
    let config_account =
        Config::try_deserialize(&mut config_account.data.as_slice()).map_err(AnchorError)?;

    let epoch = rpc_client.get_epoch_info().await?.epoch;
    let mut claim_status_pubkeys_to_expire =
        find_expired_claim_status_accounts(&accounts, epoch, signer.pubkey());
    let mut tda_pubkeys_to_expire = find_expired_tda_accounts(&accounts, epoch);

    while start.elapsed() <= max_loop_duration {
        let mut transactions = build_close_claim_status_transactions(
            &claim_status_pubkeys_to_expire,
            tip_distribution_program_id,
            config_pubkey,
            micro_lamports,
            signer.pubkey(),
        );
        if should_reclaim_tdas {
            transactions.extend(build_close_tda_transactions(
                &tda_pubkeys_to_expire,
                tip_distribution_program_id,
                config_pubkey,
                &config_account,
                signer.pubkey(),
            ));
        }

        datapoint_info!(
            "claim_mev_workflow-prepare_rent_reclaim_transactions",
            ("transaction_count", transactions.len(), i64),
        );

        if transactions.is_empty() {
            info!("Finished reclaim rent!");
            return Ok(());
        }

        transactions.shuffle(&mut thread_rng());
        let transactions: Vec<_> = transactions.into_iter().take(10_000).collect();
        let blockhash = rpc_client.get_latest_blockhash().await?;
        send_until_blockhash_expires(&rpc_client, transactions, blockhash, &signer).await?;

        // can just refresh calling get_multiple_accounts since these operations should be subtractive and not additive
        let claim_status_pubkeys: Vec<_> = claim_status_pubkeys_to_expire
            .iter()
            .map(|(pubkey, _)| *pubkey)
            .collect();
        claim_status_pubkeys_to_expire = get_batched_accounts(&rpc_client, &claim_status_pubkeys)
            .await?
            .into_iter()
            .filter_map(|(pubkey, account)| Some((pubkey, account?)))
            .collect();

        let tda_pubkeys: Vec<_> = tda_pubkeys_to_expire
            .iter()
            .map(|(pubkey, _)| *pubkey)
            .collect();
        tda_pubkeys_to_expire = get_batched_accounts(&rpc_client, &tda_pubkeys)
            .await?
            .into_iter()
            .filter_map(|(pubkey, account)| Some((pubkey, account?)))
            .collect();
    }

    // one final refresh before double checking everything
    let claim_status_pubkeys: Vec<_> = claim_status_pubkeys_to_expire
        .iter()
        .map(|(pubkey, _)| *pubkey)
        .collect();
    claim_status_pubkeys_to_expire = get_batched_accounts(&rpc_client, &claim_status_pubkeys)
        .await?
        .into_iter()
        .filter_map(|(pubkey, account)| Some((pubkey, account?)))
        .collect();

    let tda_pubkeys: Vec<_> = tda_pubkeys_to_expire
        .iter()
        .map(|(pubkey, _)| *pubkey)
        .collect();
    tda_pubkeys_to_expire = get_batched_accounts(&rpc_client, &tda_pubkeys)
        .await?
        .into_iter()
        .filter_map(|(pubkey, account)| Some((pubkey, account?)))
        .collect();

    let mut transactions = build_close_claim_status_transactions(
        &claim_status_pubkeys_to_expire,
        tip_distribution_program_id,
        config_pubkey,
        micro_lamports,
        signer.pubkey(),
    );
    if should_reclaim_tdas {
        transactions.extend(build_close_tda_transactions(
            &tda_pubkeys_to_expire,
            tip_distribution_program_id,
            config_pubkey,
            &config_account,
            signer.pubkey(),
        ));
    }

    if transactions.is_empty() {
        return Ok(());
    }

    // if more transactions left, we'll simulate them all to make sure its not an uncaught error
    let mut is_error = false;
    let mut error_str = String::new();
    for tx in &transactions {
        match rpc_client
            .simulate_transaction_with_config(
                tx,
                RpcSimulateTransactionConfig {
                    sig_verify: false,
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::processed()),
                    ..RpcSimulateTransactionConfig::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(e) => {
                error_str = e.to_string();
                is_error = true;

                match e.get_transaction_error() {
                    None => {
                        break;
                    }
                    Some(e) => {
                        warn!("transaction error. tx: {:?} error: {:?}", tx, e);
                        break;
                    }
                }
            }
        }
    }

    if is_error {
        Err(ClaimMevError::UncaughtError { e: error_str })
    } else {
        Err(ClaimMevError::NotFinished {
            transactions_left: transactions.len(),
        })
    }
}

fn find_expired_claim_status_accounts(
    accounts: &[(Pubkey, Account)],
    epoch: Epoch,
    payer: Pubkey,
) -> Vec<(Pubkey, Account)> {
    accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            let claim_status = ClaimStatus::try_deserialize(&mut account.data.as_slice()).ok()?;
            if claim_status.claim_status_payer.eq(&payer) && epoch > claim_status.expires_at {
                Some((*pubkey, account.clone()))
            } else {
                None
            }
        })
        .collect()
}

fn find_expired_tda_accounts(
    accounts: &[(Pubkey, Account)],
    epoch: Epoch,
) -> Vec<(Pubkey, Account)> {
    accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            let tda = TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).ok()?;
            if epoch > tda.expires_at {
                Some((*pubkey, account.clone()))
            } else {
                None
            }
        })
        .collect()
}

/// Assumes accounts is already pre-filtered with checks to ensure the account can be closed
fn build_close_claim_status_transactions(
    accounts: &[(Pubkey, Account)],
    tip_distribution_program_id: Pubkey,
    config: Pubkey,
    microlamports: u64,
    payer: Pubkey,
) -> Vec<Transaction> {
    accounts
        .iter()
        .map(|(claim_status_pubkey, account)| {
            let claim_status = ClaimStatus::try_deserialize(&mut account.data.as_slice()).unwrap();
            close_claim_status_ix(
                tip_distribution_program_id,
                CloseClaimStatusArgs,
                CloseClaimStatusAccounts {
                    config,
                    claim_status: *claim_status_pubkey,
                    claim_status_payer: claim_status.claim_status_payer,
                },
            )
        })
        .collect::<Vec<_>>()
        .chunks(4)
        .map(|close_claim_status_instructions| {
            let mut instructions = vec![ComputeBudgetInstruction::set_compute_unit_price(
                microlamports,
            )];
            instructions.extend(close_claim_status_instructions.to_vec());
            Transaction::new_with_payer(&instructions, Some(&payer))
        })
        .collect()
}

fn build_close_tda_transactions(
    accounts: &[(Pubkey, Account)],
    tip_distribution_program_id: Pubkey,
    config_pubkey: Pubkey,
    config: &Config,
    payer: Pubkey,
) -> Vec<Transaction> {
    let instructions: Vec<_> = accounts
        .iter()
        .map(|(pubkey, account)| {
            let tda =
                TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).unwrap();
            close_tip_distribution_account_ix(
                tip_distribution_program_id,
                CloseTipDistributionAccountArgs {
                    _epoch: tda.epoch_created_at,
                },
                CloseTipDistributionAccounts {
                    config: config_pubkey,
                    tip_distribution_account: *pubkey,
                    validator_vote_account: tda.validator_vote_account,
                    expired_funds_account: config.expired_funds_account,
                    signer: payer,
                },
            )
        })
        .collect();

    instructions
        .chunks(4)
        .map(|ix_chunk| Transaction::new_with_payer(ix_chunk, Some(&payer)))
        .collect()
}
