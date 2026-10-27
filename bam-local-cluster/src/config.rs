use {serde::Deserialize, std::path::PathBuf};

#[derive(Debug, Deserialize, Clone)]
pub struct LocalClusterConfig {
    pub bam_url: String,
    pub tip_payment_program_id: String,
    pub tip_distribution_program_id: String,
    pub faucet_address: String,
    pub ledger_base_directory: String,
    pub validator_build_path: String,
    pub ledger_tool_build_path: String,
    pub validators: Vec<CustomValidatorConfig>,
    pub dynamic_port_range_start: u16,
    /// Faucet mint balance in SOL.
    #[serde(default)]
    pub mint_sol: Option<u64>,
    pub bind_address: Option<String>,
    pub gossip_host: Option<String>,
    pub limit_blockstore_size: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct CustomValidatorConfig {
    pub geyser_config: Option<PathBuf>,
    pub node_keypair: PathBuf,
    pub node_pubkey: String,
    pub vote_keypair: PathBuf,
    pub vote_pubkey: String,
    pub ledger_path: PathBuf,
}

impl LocalClusterConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: LocalClusterConfig = toml::from_str(&config_str)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn example_config_parses() {
        toml::from_str::<LocalClusterConfig>(include_str!("../examples/example_config.toml"))
            .unwrap();
    }
}
