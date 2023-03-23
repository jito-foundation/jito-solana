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
    std::{error::Error, time::Duration},
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
    let accounts = rpc_client
        .get_program_accounts(&tip_distribution_program_id)
        .await?;

    let current_epoch = rpc_client.get_epoch_info().await?.epoch;

    let config_pubkey = derive_config_account_address(&tip_distribution_program_id).0;
    let config_account = rpc_client.get_account(&config_pubkey).await?;
    let config_account: Config = Config::try_deserialize(&mut config_account.data.as_slice())?;

    let claim_status_accounts: Vec<(Pubkey, ClaimStatus)> = accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            if let Ok(claim_status) = ClaimStatus::try_deserialize(&mut account.data.as_slice()) {
                Some((*pubkey, claim_status))
            } else {
                None
            }
        })
        .filter(|(_, claim_status): &(Pubkey, ClaimStatus)| {
            // Only return claim statues that we've paid for and ones that are expired to avoid transaction failures.
            claim_status.claim_status_payer == signer.pubkey()
                && current_epoch > claim_status.expires_at
        })
        .collect::<Vec<_>>();

    info!("closing {} claim statuses", claim_status_accounts.len());

    let recent_blockhash = rpc_client.get_latest_blockhash().await?;

    let mut transactions = claim_status_accounts
        .into_iter()
        .map(|(claim_status_pubkey, claim_status)| {
            Transaction::new_signed_with_payer(
                &[close_claim_status_ix(
                    tip_distribution_program_id,
                    CloseClaimStatusArgs,
                    CloseClaimStatusAccounts {
                        config: config_pubkey,
                        claim_status: claim_status_pubkey,
                        claim_status_payer: claim_status.claim_status_payer,
                    },
                )],
                Some(&signer.pubkey()),
                &[&signer],
                recent_blockhash,
            )
        })
        .collect::<Vec<_>>();

    if should_reclaim_tdas {
        let tip_distribution_accounts: Vec<(Pubkey, TipDistributionAccount)> = accounts
            .iter()
            .filter_map(|(pubkey, account)| {
                if let Ok(tda) =
                    TipDistributionAccount::try_deserialize(&mut account.data.as_slice())
                {
                    Some((*pubkey, tda))
                } else {
                    None
                }
            })
            .filter(|(_, tda): &(Pubkey, TipDistributionAccount)| current_epoch > tda.expires_at)
            .collect::<Vec<_>>();

        transactions.extend(
            tip_distribution_accounts
                .into_iter()
                .map(|(tda_pubkey, tda)| {
                    Transaction::new_signed_with_payer(
                        &[close_tip_distribution_account_ix(
                            tip_distribution_program_id,
                            CloseTipDistributionAccountArgs {
                                _epoch: tda.epoch_created_at,
                            },
                            CloseTipDistributionAccounts {
                                config: config_pubkey,
                                tip_distribution_account: tda_pubkey,
                                validator_vote_account: tda.validator_vote_account,
                                expired_funds_account: config_account.expired_funds_account,
                                signer: signer.pubkey(),
                            },
                        )],
                        Some(&signer.pubkey()),
                        &[&signer],
                        recent_blockhash,
                    )
                }),
        )
    }

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
