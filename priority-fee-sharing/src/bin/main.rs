use clap::{Parser, Subcommand};
use log::info;
use priority_fee_sharing::fee_records::{FeeRecordState, FeeRecords};
use priority_fee_sharing::share_priority_fees_loop;
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
        /// RPC URL to use
        #[arg(long, env)]
        rpc_url: String,

        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// Where the priority fees are paid out from
        #[arg(long, env)]
        priority_fee_payer_keypair_path: PathBuf,

        /// Needed to create the PriorityFeeDistribution Account
        /// Can find my running `solana vote-account YOU_VOTE_ACCOUNT`
        #[arg(long, env)]
        vote_authority_keypair_path: PathBuf,

        /// Validator vote account address
        #[arg(long, env)]
        validator_vote_account: Pubkey,

        /// Priority fee distribution program
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

        /// How many share IXs to bundle into a single transaction
        #[arg(long, env, default_value_t = 1)]
        chunk_size: usize,

        /// How many share TXs to send before timeout
        #[arg(long, env, default_value_t = 1)]
        call_limit: usize,
    },

    /// Export records to CSV
    ExportCsv {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// Path to the output CSV file
        #[arg(long, env)]
        output_path: PathBuf,

        /// State of records to export (unprocessed, processed, skipped, antedup, complete, any)
        #[arg(long, env, default_value = "any")]
        state: String,
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
    let args: Args = Args::parse();

    // Initialize logger with default INFO level
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    match &args.command {
        Commands::Run {
            rpc_url,
            fee_records_db_path,
            priority_fee_payer_keypair_path,
            vote_authority_keypair_path,
            validator_vote_account,
            merkle_root_upload_authority,
            priority_fee_distribution_program,
            minimum_balance_sol,
            commission_bps,
            chunk_size,
            call_limit,
        } => {
            info!("Running Transfer Loop");
            info!("For vote account: {}", validator_vote_account);

            let minimum_balance_lamports: u64 = sol_to_lamports(*minimum_balance_sol) as u64;

            share_priority_fees_loop(
                rpc_url.clone(),
                fee_records_db_path.clone(),
                priority_fee_payer_keypair_path.clone(),
                vote_authority_keypair_path.clone(),
                *validator_vote_account,
                *merkle_root_upload_authority,
                *priority_fee_distribution_program,
                minimum_balance_lamports,
                *commission_bps,
                *chunk_size,
                *call_limit,
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
    }

    Ok(())
}
