use {
    clap::Parser,
    solana_sdk::pubkey::Pubkey,
    solana_tip_distributor::merkle_root_upload_workflow::upload_merkle_root,
    std::{path::PathBuf, str::FromStr},
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to JSON file containing the [StakeMetaCollection] object.
    #[clap(long, env)]
    merkle_root_path: PathBuf,

    /// The path to the keypair used to sign and pay for the `upload_merkle_root` transactions.
    #[clap(long, env)]
    keypair_path: PathBuf,

    /// The RPC to send transactions to.
    #[clap(long, env)]
    rpc_url: String,

    /// Tip distribution program ID
    #[clap(long, env)]
    tip_distribution_program_id: String,
}

fn main() {
    let args: Args = Args::parse();

    let tip_distribution_program_id = Pubkey::from_str(&args.tip_distribution_program_id)
        .expect("valid tip_distribution_program_id");

    upload_merkle_root(
        &args.merkle_root_path,
        &args.keypair_path,
        &args.rpc_url,
        &tip_distribution_program_id,
    )
    .expect("upload merkle root succeeds");

    // let merkle_tree = read_json_from_path(&args.merkle_root_path);

    // if should_upload_roots {
    //     let rpc_client = Arc::new(RpcClient::new_with_commitment(
    //         rpc_url,
    //         CommitmentConfig {
    //             commitment: CommitmentLevel::Finalized,
    //         },
    //     ));
    //     let (config, _) = Pubkey::find_program_address(
    //         &[Config::SEED],
    //         &stake_meta_coll.tip_distribution_program_id,
    //     );
    //
    //     let recent_blockhash = {
    //         let rpc_client = rpc_client.clone();
    //         let rt = tokio::runtime::Runtime::new().unwrap();
    //         // TODO(seg): is there a possibility this goes stale while synchronously executing transactions?
    //         rt.block_on(async move { rpc_client.get_latest_blockhash().await })
    //     }
    //         .map_err(Box::new)?;
    //
    //     upload_roots(
    //         rpc_client,
    //         merkle_tree_coll,
    //         stake_meta_coll.tip_distribution_program_id,
    //         config,
    //         my_keypair,
    //         recent_blockhash,
    //         force_upload_roots,
    //     );
    // }
}
