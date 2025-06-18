use clap::{Parser, Subcommand};
use log::info;
use priority_fee_sharing::fee_records::{FeeRecordState, FeeRecords};
use priority_fee_sharing::{
    print_priority_fee_distribution_account_info, print_epoch_info, share_priority_fees_loop, Cluster
};
use solana_pubkey::Pubkey;
use solana_sdk::native_token::sol_to_lamports;
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// The command to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the fee sharing service
    Run {
        /// Cluster to use
        #[arg(long, env, default_value = "mainnet")]
        cluster: Cluster,

        /// RPC URL to use
        #[arg(long, env)]
        rpc_url: String,

        /// Fee Records DB Path
        #[arg(long, env)]
        fee_records_db_path: PathBuf,

        /// Where the priority fees are paid out from - usually the identity keypair
        #[arg(long, env)]
        priority_fee_payer_keypair_path: PathBuf,

        /// Needed to create the PriorityFeeDistribution Account - usually the identity keypair
        /// Can find by running `solana vote-account YOUR_VOTE_ACCOUNT`
        #[arg(long, env)]
        vote_authority_keypair_path: PathBuf,

        /// Validator vote account address
        #[arg(long, env)]
        validator_vote_pubkey: Pubkey,

        /// Merkle root upload authority
        #[arg(long, env)]
        merkle_root_upload_authority: Pubkey,

        /// Priority fee distribution program
        #[arg(long, env)]
        priority_fee_distribution_program: Pubkey,

        /// The minimum balance that should be left in the keypair to ensure it has
        /// enough to cover voting costs
        #[arg(long, env)]
        minimum_balance_sol: f64,

        /// The commission rate for validators in bips. A 100% commission (10_000 bips)
        /// would mean the validator takes all the fees for themselves. Default is 50%
        #[arg(long, env, default_value_t = 5_000)]
        commission_bps: u64,

        /// Priority Fee for sending share transactions
        #[arg(long, env, default_value_t = 0)]
        priority_fee_lamports: u64,

        /// How many TXs to send per epoch
        #[arg(long, env, default_value_t = 10)]
        transactions_per_epoch: u64,

        /// MS inbetween each loop ( Default 5min )
        #[arg(long, env, default_value_t = 1000 * 60 * 5)]
        loop_sleep_ms: u64,

        /// Verifies the command setup
        #[arg(long)]
        verify: bool,
    },

    /// Export records to CSV
    ExportCsv {
        /// Fee Records DB Path
        #[arg(long, env)]
        fee_records_db_path: PathBuf,

        /// Path to the output CSV file
        #[arg(long, env)]
        output_path: PathBuf,

        /// State of records to export (unprocessed, processed, skipped, antedup, complete, any)
        #[arg(long, env, default_value = "any")]
        state: String,
    },

    /// Print information about the Priority Fee Distribution Account
    PrintPriorityFeeDistributionAccountInfo {
        /// RPC URL to use
        #[arg(long, env)]
        rpc_url: String,

        /// Validator vote account address
        #[arg(long, env)]
        validator_vote_account: Pubkey,

        /// Priority fee distribution program
        #[arg(long, env)]
        priority_fee_distribution_program: Pubkey,

        /// Optional: Specific epoch to query (defaults to current epoch)
        #[arg(long)]
        epoch: Option<u64>,
    },

    /// Print information about the Epoch
    PrintEpochInfo {
        /// RPC URL to use
        #[arg(long, env)]
        rpc_url: String,

        /// Validator vote account address
        #[arg(long, env)]
        validator_vote_account: Pubkey,

        /// The epoch to query
        #[arg(long, env)]
        epoch: u64,

        /// Commission fee basis points
        #[arg(long, env, default_value_t = 5_000)]
        commission_fee_bps: u16,
    },
}

/// Parse state string to FeeRecordState enum
fn parse_state(state_str: &str) -> Result<FeeRecordState, anyhow::Error> {
    match state_str.to_lowercase().as_str() {
        "unprocessed" => Ok(FeeRecordState::Unprocessed),
        "processed" | "pending" => Ok(FeeRecordState::ProcessedAndPending),
        "skipped" => Ok(FeeRecordState::Skipped),
        "antedup" => Ok(FeeRecordState::AntedUp),
        "complete" | "completed" => Ok(FeeRecordState::Complete),
        "any" => Ok(FeeRecordState::Any),
        _ => Err(anyhow::anyhow!("Invalid state: {}", state_str)),
    }
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Load .env file if it exists
    dotenv::dotenv().ok();

    let args: Args = Args::parse();

    // Initialize logger with default INFO level
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match &args.command {
        Commands::Run {
            cluster,
            rpc_url,
            fee_records_db_path,
            priority_fee_payer_keypair_path,
            vote_authority_keypair_path,
            validator_vote_pubkey,
            merkle_root_upload_authority,
            priority_fee_distribution_program,
            minimum_balance_sol,
            commission_bps,
            priority_fee_lamports,
            transactions_per_epoch,
            loop_sleep_ms,
            verify,
        } => {
            let minimum_balance_lamports: u64 = sol_to_lamports(*minimum_balance_sol) as u64;

            info!("Running Transfer Loop");
            info!("Cluster: {:?}", cluster.clone());
            info!("Fee Records DB Path: {:?}", fee_records_db_path);
            info!(
                "Priority Fee Payer Keypair Path: {:?}",
                priority_fee_payer_keypair_path
            );
            info!(
                "Vote Authority Keypair Path: {:?}",
                vote_authority_keypair_path
            );
            info!("Validator Vote Pubkey: {}", validator_vote_pubkey);
            info!("Merkle Upload Authority: {}", merkle_root_upload_authority);
            info!(
                "Priority Fee Distribution Program: {}",
                priority_fee_distribution_program
            );
            info!("Minimum balance SOL: {}", minimum_balance_sol);
            info!("Commission bps: {}", commission_bps);
            info!("Priority fee lamports: {}", priority_fee_lamports);
            info!("Transactions per epoch: {}", transactions_per_epoch);
            info!("Loop timeout ms: {}", loop_sleep_ms);

            share_priority_fees_loop(
                cluster.clone(),
                rpc_url.clone(),
                fee_records_db_path.clone(),
                priority_fee_payer_keypair_path.clone(),
                vote_authority_keypair_path.clone(),
                *validator_vote_pubkey,
                *merkle_root_upload_authority,
                *priority_fee_distribution_program,
                minimum_balance_lamports,
                *commission_bps,
                *priority_fee_lamports,
                *transactions_per_epoch,
                *loop_sleep_ms,
                *verify,
            )
            .await?
        }

        Commands::ExportCsv {
            fee_records_db_path,
            output_path,
            state,
        } => {
            let state_enum = parse_state(state)?;
            info!(
                "Exporting records with state {:?} to CSV: {}",
                state_enum,
                output_path.display()
            );

            let fee_records = FeeRecords::new(fee_records_db_path)?;

            fee_records.export_to_csv(output_path, state_enum)?;
            info!("Export completed successfully");
        }

        Commands::PrintPriorityFeeDistributionAccountInfo {
            rpc_url,
            validator_vote_account,
            priority_fee_distribution_program,
            epoch,
        } => {
            print_priority_fee_distribution_account_info(
                rpc_url.clone(),
                *validator_vote_account,
                *priority_fee_distribution_program,
                *epoch,
            )
            .await?
        }

        Commands::PrintEpochInfo {
            rpc_url,
            validator_vote_account,
            epoch,
            commission_fee_bps,
        } => {
            print_epoch_info(
                rpc_url.clone(),
                *validator_vote_account,
                *epoch,
                *commission_fee_bps,
            )
            .await?
        }
    }

    Ok(())
}
