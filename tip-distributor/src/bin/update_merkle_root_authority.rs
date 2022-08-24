use {
    clap::Parser,
    log::info,
    solana_client::nonblocking::rpc_client::RpcClient,
    solana_program::pubkey::Pubkey,
    solana_sdk::{signature::read_keypair_file, signer::Signer, transaction::Transaction},
    solana_tip_distributor::merkle_root_generator_workflow::execute_transactions,
    std::{path::PathBuf, str::FromStr, sync::Arc},
    tip_distribution::sdk::instruction::{
        set_merkle_root_upload_authority_ix, SetMerkleRootUploadAuthorityAccounts,
        SetMerkleRootUploadAuthorityArgs,
    },
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to fee payer keypair
    #[clap(long, env)]
    fee_payer: PathBuf,

    /// Path to validator vote account keypair
    #[clap(long, env)]
    validator_vote_account: PathBuf,

    /// Tip distribution account pubkey
    #[clap(long, env)]
    tip_distribution_account: String,

    /// New merkle root upload authority to use
    #[clap(long, env)]
    new_authority: String,

    /// Tip distribution program
    #[clap(long, env)]
    program_id: String,

    /// The RPC to submit transactions
    #[clap(long, env)]
    rpc_url: String,
}

fn main() {
    env_logger::init();
    info!("Updating merkle root authority...");

    let args: Args = Args::parse();
    let program_id = Pubkey::from_str(&*args.program_id).unwrap();
    let new_authority = Pubkey::from_str(&*args.new_authority).unwrap();
    let tip_distribution_account = Pubkey::from_str(&*args.tip_distribution_account).unwrap();

    let validator_vote_account = read_keypair_file(&args.validator_vote_account)
        .expect("Failed to read validator vote keypair file.");
    let validator_vote_account_pubkey = validator_vote_account.pubkey();
    let fee_payer_kp =
        read_keypair_file(&args.fee_payer).expect("Failed to read fee payer keypair file.");
    let fee_payer_pubkey = fee_payer_kp.pubkey();

    let rpc_client = RpcClient::new(args.rpc_url);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let recent_blockhash = runtime.block_on(rpc_client.get_latest_blockhash()).unwrap();

    let ix = set_merkle_root_upload_authority_ix(
        program_id,
        SetMerkleRootUploadAuthorityArgs {
            new_merkle_root_upload_authority: new_authority,
        },
        SetMerkleRootUploadAuthorityAccounts {
            tip_distribution_account,
            signer: validator_vote_account_pubkey,
        },
    );

    let tx = Transaction::new_signed_with_payer(
        &[ix],
        Some(&fee_payer_pubkey),
        &[&validator_vote_account, &fee_payer_kp],
        recent_blockhash,
    );
    execute_transactions(Arc::new(rpc_client), vec![tx]);
}
