pub mod cli;

use {solana_pubkey::Pubkey, std::path::PathBuf};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TipRouterSnapshotConfig {
    pub output_dir: PathBuf,
    pub ncn: Option<Pubkey>,
    pub tip_router_program_id: Option<Pubkey>,
    pub tip_distribution_program_id: Option<Pubkey>,
    pub priority_fee_distribution_program_id: Option<Pubkey>,
    pub tip_payment_program_id: Option<Pubkey>,
    pub max_candidates: Option<usize>,
}
