pub mod cli;

use {solana_pubkey::Pubkey, std::path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TipRouterSnapshotConfig {
    pub output_dir: PathBuf,
    pub tip_distribution_program_id: Pubkey,
    pub priority_fee_distribution_program_id: Pubkey,
    pub tip_payment_program_id: Pubkey,
}
