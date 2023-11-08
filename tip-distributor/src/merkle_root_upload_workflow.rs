use {
    crate::{
        read_json_from_file, sign_and_send_transactions_with_retries, GeneratedMerkleTree,
        GeneratedMerkleTreeCollection,
    },
    anchor_lang::AccountDeserialize,
    jito_tip_distribution::{
        sdk::instruction::{upload_merkle_root_ix, UploadMerkleRootAccounts, UploadMerkleRootArgs},
        state::{Config, TipDistributionAccount},
    },
    log::{error, info},
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_program::{
        fee_calculator::DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE, native_token::LAMPORTS_PER_SOL,
    },
    solana_sdk::{
        commitment_config::CommitmentConfig,
        pubkey::Pubkey,
        signature::{read_keypair_file, Signer},
        transaction::Transaction,
    },
    std::{path::PathBuf, time::Duration},
    thiserror::Error,
    tokio::runtime::Builder,
};

#[derive(Error, Debug)]
pub enum MerkleRootUploadError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
}

pub fn upload_merkle_root(
    merkle_root_path: &PathBuf,
    keypair_path: &PathBuf,
    rpc_url: &str,
    tip_distribution_program_id: &Pubkey,
    max_concurrent_rpc_get_reqs: usize,
    txn_send_batch_size: usize,
) -> Result<(), MerkleRootUploadError> {
    const MAX_RETRY_DURATION: Duration = Duration::from_secs(600);

    let merkle_tree: GeneratedMerkleTreeCollection =
        read_json_from_file(merkle_root_path).expect("read GeneratedMerkleTreeCollection");
    let keypair = read_keypair_file(keypair_path).expect("read keypair file");

    let tip_distribution_config =
        Pubkey::find_program_address(&[Config::SEED], tip_distribution_program_id).0;

    let runtime = Builder::new_multi_thread()
        .worker_threads(16)
        .enable_all()
        .build()
        .expect("build runtime");

    runtime.block_on(async move {
        let rpc_client =
            RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());
        let trees: Vec<GeneratedMerkleTree> = merkle_tree
            .generated_merkle_trees
            .into_iter()
            .filter(|tree| tree.merkle_root_upload_authority == keypair.pubkey())
            .collect();

        info!("num trees to upload: {:?}", trees.len());

        // heuristic to make sure we have enough funds to cover execution, assumes all trees need updating 
        {
            let initial_balance = rpc_client.get_balance(&keypair.pubkey()).await.expect("failed to get balance");
            let desired_balance = (trees.len() as u64).checked_mul(DEFAULT_TARGET_LAMPORTS_PER_SIGNATURE).unwrap();
            if initial_balance < desired_balance {
                let sol_to_deposit = desired_balance.checked_sub(initial_balance).unwrap().checked_add(LAMPORTS_PER_SOL).unwrap().checked_sub(1).unwrap().checked_div(LAMPORTS_PER_SOL).unwrap(); // rounds up to nearest sol
                panic!("Expected to have at least {} lamports in {}, current balance is {} lamports, deposit {} SOL to continue.",
                       desired_balance, &keypair.pubkey(), initial_balance, sol_to_deposit)
            }
        }
        let mut trees_needing_update: Vec<GeneratedMerkleTree> = vec![];
        for tree in trees {
            let account = rpc_client
                .get_account(&tree.tip_distribution_account)
                .await
                .expect("fetch expect");

            let mut data = account.data.as_slice();
            let fetched_tip_distribution_account =
                TipDistributionAccount::try_deserialize(&mut data)
                    .expect("failed to deserialize tip_distribution_account state");

            let needs_upload = match fetched_tip_distribution_account.merkle_root {
                Some(merkle_root) => {
                    merkle_root.total_funds_claimed == 0
                        && merkle_root.root != tree.merkle_root.to_bytes()
                }
                None => true,
            };

            if needs_upload {
                trees_needing_update.push(tree);
            }
        }

        info!("num trees need uploading: {:?}", trees_needing_update.len());

        let transactions: Vec<Transaction> = trees_needing_update
            .iter()
            .map(|tree| {
                let ix = upload_merkle_root_ix(
                    *tip_distribution_program_id,
                    UploadMerkleRootArgs {
                        root: tree.merkle_root.to_bytes(),
                        max_total_claim: tree.max_total_claim,
                        max_num_nodes: tree.max_num_nodes,
                    },
                    UploadMerkleRootAccounts {
                        config: tip_distribution_config,
                        merkle_root_upload_authority: keypair.pubkey(),
                        tip_distribution_account: tree.tip_distribution_account,
                    },
                );
                Transaction::new_with_payer(
                    &[ix],
                    Some(&keypair.pubkey()),
                )
            })
            .collect();

        let (to_process, failed_transactions) = sign_and_send_transactions_with_retries(
            &keypair, &rpc_client, max_concurrent_rpc_get_reqs, transactions, txn_send_batch_size, MAX_RETRY_DURATION).await;
        if !to_process.is_empty() {
            panic!("{} remaining mev claim transactions, {} failed requests.", to_process.len(), failed_transactions.len());
        }
    });

    Ok(())
}
