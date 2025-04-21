use clap::{Parser, Subcommand};
use log::info;
use priority_fee_sharing::fee_records::{
    FeeRecordCategory, FeeRecordEntry, FeeRecordState, FeeRecords,
};
use priority_fee_sharing::{share_priority_fees_loop, spam_priority_fees_loop};
use solana_clock::DEFAULT_SLOTS_PER_EPOCH;
use solana_pubkey::Pubkey;
use solana_sdk::native_token::{lamports_to_sol, sol_to_lamports};
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

        /// Path to payer keypair
        #[arg(long, env)]
        priority_fee_keypair_path: PathBuf,

        /// Validator vote account address
        #[arg(long, env)]
        validator_address: Pubkey,

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

        /// Priority fee distribution program
        #[arg(long, env, default_value_t = 1000)]
        go_live_epoch: u64,
    },

    /// Spam priority fees for testing
    TestSpamFees {
        /// RPC URL to use
        #[arg(long, env)]
        rpc_url: String,

        /// Path to payer keypair
        #[arg(long, env)]
        priority_fee_keypair_path: PathBuf,
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

    /// Get a specific record by slot
    GetRecord {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// Slot number to retrieve
        #[arg(long, env)]
        slot: u64,
    },

    /// Get records by state
    GetRecordsByState {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// State to filter by (unprocessed, processed, skipped, antedup, complete, any)
        #[arg(long, env)]
        state: String,
    },

    /// Get records by category
    GetRecordsByCategory {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// Category to filter by (priority-fee, ante, any)
        #[arg(long, env)]
        category: String,
    },

    /// Get total pending lamports
    GetPendingLamports {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,
    },

    /// Add a new ante record (for recovery purposes)
    AddAnteRecord {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,

        /// Slot number
        #[arg(long, env)]
        slot: u64,

        /// Fee amount in lamports
        #[arg(long, env)]
        fee_lamports: u64,

        /// Transaction signature
        #[arg(long, env)]
        signature: String,

        /// Slot the transaction landed
        #[arg(long, env)]
        slot_landed: u64,
    },

    /// Compact the database for performance
    CompactDb {
        /// Fee Records DB Path
        #[arg(long, env, default_value = "/var/lib/solana/fee_records")]
        fee_records_db_path: PathBuf,
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

/// Parse category string to FeeRecordCategory enum
fn parse_category(category_str: &str) -> Result<FeeRecordCategory, anyhow::Error> {
    match category_str.to_lowercase().as_str() {
        "priority-fee" | "priority" => Ok(FeeRecordCategory::PriorityFee),
        "ante" => Ok(FeeRecordCategory::Ante),
        "any" => Ok(FeeRecordCategory::Any),
        _ => Err(anyhow::anyhow!("Invalid category: {}", category_str)),
    }
}

/// Format record for display
fn format_record(record: &FeeRecordEntry) -> String {
    format!(
        "Slot: {}, State: {:?}, Category: {:?}, Fee: {} ({:.9} SOL), Time: {}, Landed: {}, Sig: {}",
        record.slot,
        record.state,
        record.category,
        record.priority_fee_lamports,
        record.priority_fee_lamports as f64 / 1_000_000_000.0,
        record.timestamp,
        record.slot_landed,
        record.signature
    )
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
            priority_fee_keypair_path,
            validator_address,
            priority_fee_distribution_program,
            minimum_balance_sol,
            commission_bps,
            chunk_size,
            call_limit,
            go_live_epoch,
        } => {
            info!("Running Transfer Loop");
            info!("Using validator address: {}", validator_address);

            let minimum_balance_lamports: u64 = sol_to_lamports(*minimum_balance_sol) as u64;

            share_priority_fees_loop(
                rpc_url.clone(),                    // RPC Client
                fee_records_db_path.clone(),        // Fee Records
                priority_fee_keypair_path.clone(),  // Payer keypair
                *validator_address,                 // Validator address (needs to be a reference)
                *priority_fee_distribution_program, // Priority Fee Distribution Address
                *commission_bps,                    // Commission BPS
                minimum_balance_lamports,           // Minimum balance
                *chunk_size,                        // Chunk size (as usize)
                *call_limit,                        // Call limit (as usize)
                *go_live_epoch,
            )
            .await?
        }

        Commands::TestSpamFees {
            rpc_url,
            priority_fee_keypair_path,
        } => {
            info!("Running fee spamming service");
            spam_priority_fees_loop(rpc_url.clone(), priority_fee_keypair_path.clone()).await?;
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

        Commands::GetRecord {
            fee_records_db_path,
            slot,
        } => {
            let epoch = slot / DEFAULT_SLOTS_PER_EPOCH;

            let fee_records = FeeRecords::new(fee_records_db_path)?;

            match fee_records.get_record(*slot, epoch)? {
                Some(record) => {
                    info!("Record for slot {}:", slot);
                    info!("{}", format_record(&record));
                }
                None => {
                    info!("No record found for slot {}", slot);
                }
            }
        }

        Commands::GetRecordsByState {
            fee_records_db_path,
            state,
        } => {
            let state_enum = parse_state(state)?;
            let fee_records = FeeRecords::new(fee_records_db_path)?;
            let records = fee_records.get_records_by_state(state_enum)?;

            info!(
                "Found {} records with state {:?}:",
                records.len(),
                state_enum
            );

            for record in records {
                info!("{}", format_record(&record));
            }
        }

        Commands::GetRecordsByCategory {
            fee_records_db_path,
            category,
        } => {
            let category_enum = parse_category(category)?;
            let fee_records = FeeRecords::new(fee_records_db_path)?;
            let records = fee_records.get_records_by_category(category_enum)?;

            info!(
                "Found {} records with category {:?}:",
                records.len(),
                category_enum
            );

            for record in records {
                info!("{}", format_record(&record));
            }
        }

        Commands::GetPendingLamports {
            fee_records_db_path,
        } => {
            let fee_records = FeeRecords::new(fee_records_db_path)?;
            let total = fee_records.get_total_pending_lamports()?;
            info!(
                "Total pending lamports: {} ({:.3})",
                total,
                lamports_to_sol(total)
            );
        }

        Commands::AddAnteRecord {
            fee_records_db_path,
            slot,
            fee_lamports,
            signature,
            slot_landed,
        } => {
            info!("{:?}", fee_records_db_path);
            info!("Slot: {}", slot);
            info!("Fee lamports: {}", fee_lamports);
            info!("Signature: {}", signature);
            info!("Slot landed: {}", slot_landed);

            todo!("Implement the logic to transfer lamports");
            //TODO actually transfer lamports

            // info!("Adding ante record for slot {}", slot);
            // fee_records.add_ante_record(*slot, *fee_lamports, signature, *slot_landed)?;
            // info!("Ante record added successfully");

            // // Display the record we just added
            // if let Some(record) = fee_records.get_record(*slot)? {
            //     info!("Record details:");
            //     info!("{}", format_record(&record));
            // }
        }

        Commands::CompactDb {
            fee_records_db_path,
        } => {
            let fee_records = FeeRecords::new(fee_records_db_path)?;

            info!("Compacting database...");
            fee_records.compact()?;
            info!("Database compaction completed");
        }
    }

    Ok(())
}
