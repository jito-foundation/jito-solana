use {
    crate::{
        read_json_from_file, sign_and_send_transactions_with_retries, GeneratedMerkleTreeCollection,
    },
    anchor_lang::{AccountDeserialize, InstructionData, ToAccountMetas},
    log::{debug, info, warn},
    solana_client::{client_error, nonblocking::rpc_client::RpcClient, rpc_request::RpcError},
    solana_program::{
        fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, native_token::LAMPORTS_PER_SOL,
        stake::state::StakeState, system_program,
    },
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
    const MAX_RETRY_DURATION: Duration = Duration::from_secs(600);

    let merkle_trees: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = read_keypair_file(keypair_path).expect("read keypair file");

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;

    let rpc_client =
        RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::finalized());

    let runtime = Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .unwrap();

    let mut instructions = Vec::new();

    runtime.block_on(async move {
        let start_balance = rpc_client.get_balance(&keypair.pubkey()).await.expect("failed to get balance");
        // heuristic to make sure we have enough funds to cover the rent costs if epoch has many validators
        {
            // most amounts are for 0 lamports. had 1736 non-zero claims out of 164742
            let node_count = merkle_trees.generated_merkle_trees.iter().flat_map(|tree| &tree.tree_nodes).filter(|node| node.amount > 0).count();
            let min_rent_per_claim = rpc_client.get_minimum_balance_for_rent_exemption(ClaimStatus::SIZE).await.expect("Failed to calculate min rent");
            let desired_balance = (node_count as u64).checked_mul(min_rent_per_claim.checked_add(DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE).unwrap()).unwrap();
            if start_balance < desired_balance {
                let sol_to_deposit = desired_balance.checked_sub(start_balance).unwrap().checked_add(LAMPORTS_PER_SOL).unwrap().checked_sub(1).unwrap().checked_div(LAMPORTS_PER_SOL).unwrap(); // rounds up to nearest sol
                panic!("Expected to have at least {} lamports in {}, current balance is {} lamports, deposit {} SOL to continue.",
                       desired_balance, &keypair.pubkey(), start_balance, sol_to_deposit)
            }
        }
        let stake_acct_min_rent = rpc_client.get_minimum_balance_for_rent_exemption(StakeState::size_of()).await.expect("Failed to calculate min rent");
        let mut below_min_rent_count: usize = 0;
        let mut zero_lamports_count: usize = 0;
        for tree in merkle_trees.generated_merkle_trees {
            // only claim for ones that have merkle root on-chain
            let account = rpc_client.get_account(&tree.tip_distribution_account).await.expect("expected to fetch tip distribution account");
            let fetched_tip_distribution_account = TipDistributionAccount::try_deserialize(&mut account.data.as_slice()).expect("failed to deserialize tip_distribution_account state");
            if fetched_tip_distribution_account.merkle_root.is_none() {
                info!(
                        "not claiming because merkle root isn't uploaded yet. skipped {} claimants for tda: {:?}",
                        tree.tree_nodes.len(),
                        tree.tip_distribution_account
                    );
                continue;
            }
            for node in tree.tree_nodes {
                if node.amount == 0 {
                    zero_lamports_count = zero_lamports_count.checked_add(1).unwrap();
                    continue;
                }

                // make sure not previously claimed
                match rpc_client.get_account(&node.claim_status_pubkey).await {
                    Ok(_) => {
                        debug!("claim status account already exists, skipping pubkey {:?}.", node.claim_status_pubkey);
                        continue;
                    }
                    // expected to not find ClaimStatus account, don't skip
                    Err(client_error::ClientError { kind: client_error::ClientErrorKind::RpcError(RpcError::ForUser(err)), .. }) if err.starts_with("AccountNotFound") => {}
                    Err(err) => panic!("Unexpected RPC Error: {}", err),
                }

                let current_balance = rpc_client.get_balance(&node.claimant).await.expect("Failed to get balance");
                // some older accounts can be rent-paying
                // any new transfers will need to make the account rent-exempt (runtime enforced)
                if current_balance.checked_add(node.amount).unwrap() < stake_acct_min_rent {
                    warn!("Current balance + tip claim amount of {} is less than required rent-exempt of {} for pubkey: {}. Skipping.",
                        current_balance.checked_add(node.amount).unwrap(), stake_acct_min_rent, node.claimant);
                    below_min_rent_count = below_min_rent_count.checked_add(1).unwrap();
                    continue;
                }
                instructions.push(Instruction {
                    program_id: *tip_distribution_program_id,
                    data: tip_distribution::instruction::Claim {
                        proof: node.proof.unwrap(),
                        amount: node.amount,
                        bump: node.claim_status_bump,
                    }.data(),
                    accounts: tip_distribution::accounts::Claim {
                        config: tip_distribution_config,
                        tip_distribution_account: tree.tip_distribution_account,
                        claimant: node.claimant,
                        claim_status: node.claim_status_pubkey,
                        payer: keypair.pubkey(),
                        system_program: system_program::id(),
                    }.to_account_metas(None),
                });
            }
        }

        let transactions = instructions.into_iter().map(|ix|{
            Transaction::new_with_payer(
                &[ix],
                Some(&keypair.pubkey()),
            )
        }).collect::<Vec<_>>();

        info!("Sending {} tip claim transactions. {} tried sending zero lamports, {} would be below minimum rent",
            &transactions.len(), zero_lamports_count, below_min_rent_count);

        let failed_transactions = sign_and_send_transactions_with_retries(&keypair, &rpc_client, transactions, MAX_RETRY_DURATION).await;
        if !failed_transactions.is_empty() {
            panic!("failed to send {} transactions", failed_transactions.len());
        }
    });

    Ok(())
}
