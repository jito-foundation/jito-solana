use {
    anchor_lang::AccountDeserialize,
    log::info,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_program::pubkey::Pubkey,
    solana_sdk::signature::{Keypair, Signer},
    std::error::Error,
    tip_distribution::{
        sdk::instruction::{close_tip_distribution_account_ix, CloseTipDistributionAccountArgs},
        state::ClaimStatus,
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

    let claim_status_accounts: Vec<(Pubkey, ClaimStatus)> = accounts
        .iter()
        .filter_map(|(pubkey, account)| {
            if let Ok(claim_status) = ClaimStatus::try_deserialize(&mut account.data.as_slice()) {
                Some((*pubkey, claim_status))
            } else {
                None
            }
        })
        .filter(|(_, claim_status): (Pubkey, ClaimStatus)| {
            // Only return claim statues that we've paid for and ones that are expired to avoid transaction failures.
            claim_status.claim_status_payer == signer.pubkey()
                && current_epoch > claim_status.expires_at
        })
        .collect::<Vec<_>>();

    info!("closing {} claim statuses", claim_status_accounts.len());

    let close_claim_status_txs = claim_status_accounts
        .into_iter()
        .map(|(pubkey, claim_status)| {
            let ix = close_tip_distribution_account_ix(
                tip_distribution_program_id,
                CloseClaimStatusArgs { _epoch: 0 },
            );
        })
        .collect::<Vec<_>>();

    Ok(())
}
