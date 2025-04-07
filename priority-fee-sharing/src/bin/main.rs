use clap::{Parser, Subcommand};
use log::info;
use priority_fee_sharing::fee_records::{
    FeeRecordCategory, FeeRecordEntry, FeeRecordState, FeeRecords,
};
use priority_fee_sharing::share_priority_fees_loop;
use solana_clock::{Epoch, Slot, DEFAULT_MS_PER_SLOT, DEFAULT_SLOTS_PER_EPOCH};
use solana_pubkey::Pubkey;
use solana_rpc_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::reward_type::RewardType;
use solana_sdk::signature::{read_keypair_file, Keypair, Signer};
use std::cmp::min;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;
use tokio::time::{interval, sleep, Interval};

//TODO
// Add in File IO for tracking TXs as well as recovery
// Add in recovery look that looks at validator history
// Make it so it can be run as a CLI or a subroutine of the validator

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// RPC URL to use
    #[arg(long)]
    rpc_url: String,

    /// Priority fee distribution program
    #[arg(long)]
    priority_fee_distribution_program: Pubkey,

    /// Fee Records DB Path
    #[arg(long)]
    fee_records_db_path: PathBuf,

    /// The command to execute
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run the fee sharing service
    Run {
        /// Path to payer keypair
        #[arg(long)]
        payer_keypair: PathBuf,

        /// Validator vote account address
        #[arg(long)]
        validator_address: Pubkey,

        /// How frequently to check for new priority fees and distribute them
        #[arg(long)]
        period_seconds: u64,

        /// The minimum balance that should be left in the keypair to ensure it has
        /// enough to cover voting costs
        #[arg(long)]
        minimum_balance: u64,

        /// The commission rate for validators in bips. A 100% commission (10_000 bips)
        /// would mean the validator takes all the fees for themselves. That's the default
        /// value for safety reasons.
        #[arg(long, default_value_t = 10_000)]
        commission_bps: u64,
    },

    /// Export records to CSV
    ExportCsv {
        /// Path to the output CSV file
        #[arg(long)]
        output_path: PathBuf,

        /// State of records to export (unprocessed, processed, skipped, antedup, complete, any)
        #[arg(long, default_value = "any")]
        state: String,
    },

    /// Get a specific record by slot
    GetRecord {
        /// Slot number to retrieve
        #[arg(long)]
        slot: u64,
    },

    /// Get records by state
    GetRecordsByState {
        /// State to filter by (unprocessed, processed, skipped, antedup, complete, any)
        #[arg(long)]
        state: String,
    },

    /// Get records by category
    GetRecordsByCategory {
        /// Category to filter by (priority-fee, ante, any)
        #[arg(long)]
        category: String,
    },

    /// Get total pending lamports
    GetPendingLamports,

    /// Add a new ante record (for recovery purposes)
    AddAnteRecord {
        /// Slot number
        #[arg(long)]
        slot: u64,

        /// Fee amount in lamports
        #[arg(long)]
        fee_lamports: u64,

        /// Transaction signature
        #[arg(long)]
        signature: String,

        /// Slot the transaction landed
        #[arg(long)]
        slot_landed: u64,
    },

    /// Compact the database for performance
    CompactDb,
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
    let rpc = RpcClient::new(args.rpc_url.clone());

    // Initialize fee records database
    let fee_records = FeeRecords::new(&args.fee_records_db_path)?;

    match &args.command {
        Commands::Run {
            payer_keypair,
            validator_address,
            period_seconds,
            minimum_balance,
            commission_bps,
        } => {
            let keypair = read_keypair_file(payer_keypair)
                .unwrap_or_else(|err| panic!("Failed to read payer keypair file: {}", err));

            println!(
                "Running fee sharing service with period {} seconds",
                period_seconds
            );
            println!("Using validator address: {}", validator_address);

            let db_directory = "/tmp/pfs-test/";

            share_priority_fees_loop(
                &args.rpc_url,                          // RPC URL
                &keypair,                               // Payer keypair
                validator_address, // Validator address (needs to be a reference)
                *commission_bps,   // Commission BPS
                *minimum_balance,  // Minimum balance
                args.priority_fee_distribution_program, // Priority Fee Distribution Address
                db_directory.to_string(), // Database directory (as String, not reference)
                1,                 // Chunk size (as usize)
                1,                 // Call limit (as usize)
            )
            .await?
        }

        Commands::ExportCsv { output_path, state } => {
            let state_enum = parse_state(state)?;
            println!(
                "Exporting records with state {:?} to CSV: {}",
                state_enum,
                output_path.display()
            );

            fee_records.export_to_csv(output_path, state_enum)?;
            println!("Export completed successfully");
        }

        Commands::GetRecord { slot } => {
            let epoch = slot / DEFAULT_SLOTS_PER_EPOCH;
            match fee_records.get_record(*slot, epoch)? {
                Some(record) => {
                    println!("Record for slot {}:", slot);
                    println!("{}", format_record(&record));
                }
                None => {
                    println!("No record found for slot {}", slot);
                }
            }
        }

        Commands::GetRecordsByState { state } => {
            let state_enum = parse_state(state)?;
            let records = fee_records.get_records_by_state(state_enum)?;

            println!(
                "Found {} records with state {:?}:",
                records.len(),
                state_enum
            );

            for record in records {
                println!("{}", format_record(&record));
            }
        }

        Commands::GetRecordsByCategory { category } => {
            let category_enum = parse_category(category)?;
            let records = fee_records.get_records_by_category(category_enum)?;

            println!(
                "Found {} records with category {:?}:",
                records.len(),
                category_enum
            );

            for record in records {
                println!("{}", format_record(&record));
            }
        }

        Commands::GetPendingLamports => {
            let total = fee_records.get_total_pending_lamports()?;
            println!("Total pending lamports: {}", total);
            println!("SOL equivalent: {:.9}", total as f64 / 1_000_000_000.0);
        }

        Commands::AddAnteRecord {
            slot,
            fee_lamports,
            signature,
            slot_landed,
        } => {
            //TODO actually transfer lamports

            // println!("Adding ante record for slot {}", slot);
            // fee_records.add_ante_record(*slot, *fee_lamports, signature, *slot_landed)?;
            // println!("Ante record added successfully");

            // // Display the record we just added
            // if let Some(record) = fee_records.get_record(*slot)? {
            //     println!("Record details:");
            //     println!("{}", format_record(&record));
            // }
        }

        Commands::CompactDb => {
            println!("Compacting database...");
            fee_records.compact()?;
            println!("Database compaction completed");
        }
    }

    Ok(())
}
