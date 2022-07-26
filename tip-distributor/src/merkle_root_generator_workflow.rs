use {
    crate::{GeneratedMerkleTreeCollection, StakeMetaCollection},
    anchor_lang::AccountDeserialize,
    log::*,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_sdk::{
        commitment_config::{CommitmentConfig, CommitmentLevel},
        pubkey::Pubkey,
        signature::{Keypair, Signature},
        signer::Signer,
        transaction::Transaction,
    },
    std::{
        fmt::{Debug, Display, Formatter},
        fs::File,
        io::{BufReader, BufWriter, Write},
        path::PathBuf,
        sync::Arc,
        thread::sleep,
        time::Duration,
    },
    thiserror::Error as ThisError,
    tip_distribution::{
        sdk::instruction::{upload_merkle_root_ix, UploadMerkleRootAccounts, UploadMerkleRootArgs},
        state::{Config, TipDistributionAccount},
    },
    tokio::{
        runtime::{Builder, Runtime},
        task::JoinHandle,
    },
};

#[derive(ThisError, Debug)]
pub enum Error {
    Base58DecodeError,

    #[error(transparent)]
    IoError(#[from] std::io::Error),

    #[error(transparent)]
    RpcError(#[from] Box<solana_client::client_error::ClientError>),

    #[error(transparent)]
    SerdeJsonError(#[from] serde_json::Error),

    SnapshotSlotNotFound,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(&self, f)
    }
}

pub fn run_workflow(
    stake_meta_coll_path: PathBuf,
    out_path: String,
    rpc_url: String,
    my_keypair: Keypair,
    should_upload_roots: bool,
    // If true then uploads the merkle-roots disregarding if one already has been uploaded.
    force_upload_roots: bool,
) -> Result<(), Error> {
    let stake_meta_coll = read_stake_meta_collection(stake_meta_coll_path)?;

    let merkle_tree_coll = GeneratedMerkleTreeCollection::new_from_stake_meta_collection(
        stake_meta_coll.clone(),
        my_keypair.pubkey(),
    )?;

    write_to_json_file(&merkle_tree_coll, out_path)?;

    if should_upload_roots {
        let tip_distribution_program_id = stake_meta_coll
            .tip_distribution_program_id
            .parse()
            .map_err(|_| Error::Base58DecodeError)?;

        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig {
                commitment: CommitmentLevel::Finalized,
            },
        ));
        let (config, _) =
            Pubkey::find_program_address(&[Config::SEED], &tip_distribution_program_id);

        let recent_blockhash = {
            let rpc_client = rpc_client.clone();
            let rt = tokio::runtime::Runtime::new().unwrap();
            // TODO(seg): is there a possibility this goes stale while synchronously executing transactions?
            rt.block_on(async move { rpc_client.get_latest_blockhash().await })
        }
        .map_err(Box::new)?;

        upload_roots(
            rpc_client,
            merkle_tree_coll,
            tip_distribution_program_id,
            config,
            my_keypair,
            recent_blockhash,
            force_upload_roots,
        );
    }

    Ok(())
}

fn read_stake_meta_collection(path: PathBuf) -> Result<StakeMetaCollection, Error> {
    let file = File::open(path).unwrap();
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).map_err(|e| e.into())
}

fn write_to_json_file(
    merkle_tree_coll: &GeneratedMerkleTreeCollection,
    file_path: String,
) -> Result<(), Error> {
    let file = File::create(file_path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer(&mut writer, merkle_tree_coll)?;
    writer.flush()?;

    Ok(())
}

// TODO: Check root not already uploaded or force with a flag.
fn upload_roots(
    rpc_client: Arc<RpcClient>,
    merkle_tree_coll: GeneratedMerkleTreeCollection,
    tip_distribution_program_id: Pubkey,
    config: Pubkey,
    my_keypair: Keypair,
    recent_blockhash: solana_sdk::hash::Hash,
    force_upload_roots: bool,
) {
    let rt = Runtime::new().unwrap();

    let txs = merkle_tree_coll
        .generated_merkle_trees
        .into_iter()
        .filter(|gmt| {
            if !force_upload_roots {
                let account = rt
                    .block_on(rpc_client.get_account(&gmt.tip_distribution_account))
                    .expect(&*format!(
                        "failed to get tip_distribution_account {}",
                        gmt.tip_distribution_account
                    ));
                let mut data = account.data.as_slice();
                let fetched_tip_distribution_account =
                    TipDistributionAccount::try_deserialize(&mut data)
                        .expect("failed to deserialize tip_distribution_account state");

                fetched_tip_distribution_account.merkle_root.is_none()
            } else {
                true
            }
        })
        .map(|gmt| {
            // Get root.
            let root = gmt
                .merkle_tree
                .get_root()
                .expect(&*format!(
                    "Failed to get merkle root for TipDistributionAccount {}",
                    gmt.tip_distribution_account
                ))
                .to_bytes();

            // Create instruction.
            let ix = upload_merkle_root_ix(
                tip_distribution_program_id,
                UploadMerkleRootArgs {
                    root,
                    max_total_claim: gmt.max_total_claim,
                    max_num_nodes: gmt.max_num_nodes,
                },
                UploadMerkleRootAccounts {
                    config,
                    merkle_root_upload_authority: my_keypair.pubkey(),
                    tip_distribution_account: gmt.tip_distribution_account,
                },
            );

            // Sign and return transaction.
            Transaction::new_signed_with_payer(
                &[ix],
                Some(&my_keypair.pubkey()),
                &[&my_keypair],
                recent_blockhash,
            )
        })
        .collect::<Vec<Transaction>>();

    if txs.is_empty() {
        error!("No transactions to execute, use --force-upload-root to reupload roots");
        return;
    }

    execute_transactions(rpc_client, txs);
}

fn execute_transactions(rpc_client: Arc<RpcClient>, txs: Vec<Transaction>) {
    const DELAY: Duration = Duration::from_millis(500);
    const MAX_RETRIES: usize = 5;

    let rt = Builder::new_multi_thread()
        .worker_threads(txs.len().min(20))
        .thread_name("execute_transactions")
        .enable_time()
        .enable_io()
        .build()
        .unwrap();

    let hdls = txs
        .into_iter()
        .map(|tx| {
            let rpc_client = rpc_client.clone();
            rt.spawn(async move {
                if let Err(e) =
                    send_transaction_with_retry(rpc_client, &tx, DELAY, MAX_RETRIES).await
                {
                    error!(
                        "error sending transaction [signature={}, error={}]",
                        tx.signatures[0], e
                    );
                } else {
                    info!(
                        "successfully sent transaction: [signature={}]",
                        tx.signatures[0]
                    );
                }
            })
        })
        .collect::<Vec<JoinHandle<()>>>();

    rt.block_on(async { futures::future::join_all(hdls).await });
}

async fn send_transaction_with_retry(
    rpc_client: Arc<RpcClient>,
    tx: &Transaction,
    delay: Duration,
    max_retries: usize,
) -> solana_client::client_error::Result<Signature> {
    let mut retry_count: usize = 0;
    loop {
        match rpc_client.send_and_confirm_transaction(tx).await {
            Ok(sig) => {
                return Ok(sig);
            }
            Err(e) => {
                retry_count = retry_count.checked_add(1).unwrap();
                if retry_count == max_retries {
                    return Err(e);
                }
                sleep(delay);
            }
        }
    }
}
