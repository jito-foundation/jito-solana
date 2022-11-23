use {
    crate::{read_json_from_file, send_transactions_with_retry, GeneratedMerkleTreeCollection},
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    log::{debug, info, warn},
    solana_client::{nonblocking::rpc_client::RpcClient, rpc_request::RpcError},
    solana_program::{fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, system_program},
    solana_rpc_client_api::client_error::ErrorKind,
    solana_sdk::{
        commitment_config::CommitmentConfig,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{read_keypair_file, Signer},
        transaction::Transaction,
    },
    std::{path::PathBuf, time::Duration},
    thiserror::Error,
    tip_distribution::state::*,
    tokio::runtime::Builder,
};

#[derive(Error, Debug)]
pub enum ClaimMevError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub fn claim_mev_tips(
    merkle_root_path: &PathBuf,
    rpc_url: &str,
    tip_distribution_program_id: &Pubkey,
    keypair_path: &PathBuf,
) -> Result<(), ClaimMevError> {
    // roughly how long before blockhash expires
    const MAX_RETRY_DURATION: Duration = Duration::from_secs(60);

    let merkle_trees: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = read_keypair_file(keypair_path).expect("read keypair file");

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;

    let rpc_client =
        RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());

    let runtime = Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .unwrap();

    let mut transactions = Vec::new();

    runtime.block_on(async move {
        let blockhash = rpc_client.get_latest_blockhash().await.expect("read blockhash");
        let balance = rpc_client.get_balance(&keypair.pubkey()).await.expect("failed to get balance");
        // heuristic to make sure we have enough funds to cover the rent costs if epoch has many validators
        {
            let node_count = merkle_trees.generated_merkle_trees.iter().flat_map(|tree| &tree.tree_nodes).count();
            let min_rent_per_claim = rpc_client.get_minimum_balance_for_rent_exemption(ClaimStatus::SIZE).await.expect("Failed to calculate min rent");
            assert!(balance >= node_count as u64 * (min_rent_per_claim + DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE));
        }
        let mut below_min_rent_count = 0;
        let mut zero_lamports_count = 0;
        for tree in merkle_trees.generated_merkle_trees {
            // only claim for ones that have merkle root on-chain
            let account = rpc_client.get_account(&tree.tip_distribution_account).await.expect("expected to fetch tip distribution account");

            for node in tree.tree_nodes {
                if node.amount == 0 {
                    zero_lamports_count += 1;
                    continue;
                }
                let (claim_status_pubkey, claim_status_bump) = Pubkey::find_program_address(
                    &[
                        ClaimStatus::SEED,
                        node.claimant.as_ref(), // ordering matters here
                        tree.tip_distribution_account.as_ref(),
                    ],
                    tip_distribution_program_id,
                );
                let fetched_tip_distribution_account = TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).expect("failed to deserialize tip_distribution_account state");
                if fetched_tip_distribution_account.merkle_root.is_none() {
                    info!(
                        "not claiming because merkle root isn't uploaded yet. claimant: {:?} tda: {:?}",
                        node.claimant,
                        tree.tip_distribution_account
                    );
                    continue;
                }

                let claim_status_acc_result = rpc_client.get_account(&claim_status_pubkey).await;
                if claim_status_acc_result.is_ok() {
                    debug!("claim status account already exists: {:?}", claim_status_pubkey);
                    continue;
                }
                if !(matches!(claim_status_acc_result.unwrap_err().kind(), ErrorKind::RpcError(RpcError::ForUser(_)))) {
                    panic!("Invalid RPC Error");
                }
                let account = rpc_client.get_account(&node.claimant).await.expect("Failed to fetch account");
                let min_rent = rpc_client.get_minimum_balance_for_rent_exemption(account.data.len()).await.expect("Failed to calculate min rent");
                if node.amount < min_rent {
                    warn!("Tip claim amount={} is less than minimum rent={} for account={}.", node.amount, min_rent, node.claimant);
                    below_min_rent_count += 1;
                    continue;
                }
                let ix = Instruction {
                    program_id: *tip_distribution_program_id,
                    data: tip_distribution::instruction::Claim {
                        proof: node.proof.unwrap(),
                        amount: node.amount,
                        bump: claim_status_bump,
                    }.data(),
                    accounts: tip_distribution::accounts::Claim {
                        config: tip_distribution_config,
                        tip_distribution_account: tree.tip_distribution_account,
                        claimant: node.claimant,
                        claim_status: claim_status_pubkey,
                        payer: keypair.pubkey(),
                        system_program: system_program::id(),
                    }.to_account_metas(None),
                };
                let transaction = Transaction::new_signed_with_payer(
                    &[ix],
                    Some(&keypair.pubkey()),
                    &[&keypair],
                    blockhash,
                );
                info!("claiming for pubkey: {}, tx: {:?}", node.claimant, transaction);
                transactions.push(transaction);
            }
        }

        info!("Sending {} tip claim transactions. {} tried sending zero lamports, {} would be below minimum rent",
            &transactions.len(), zero_lamports_count, below_min_rent_count);
        send_transactions_with_retry(&rpc_client, &transactions, MAX_RETRY_DURATION).await;
    });

    Ok(())
}
