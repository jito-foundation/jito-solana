use {
    serde::{Deserialize, Serialize},
    std::path::PathBuf,
};

#[derive(Debug, Deserialize, Clone)]
pub struct LocalClusterConfig {
    pub bam_url: String,
    pub tip_payment_program_id: String,
    pub tip_distribution_program_id: String,
    pub faucet_address: String,
    pub ledger_base_directory: String,
    pub validator_build_path: String,
    pub validators: Vec<CustomValidatorConfig>,
    pub dynamic_port_range_start: u16,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CustomValidatorConfig {
    pub geyser_config: Option<PathBuf>,
    pub node_keypair: PathBuf,
    pub node_pubkey: String,
    pub vote_keypair: PathBuf,
    pub vote_pubkey: String,
    pub ledger_path: PathBuf,
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterInfo {
    pub rpc_endpoint: String,
    pub bootstrap_gossip: String,
}

impl LocalClusterConfig {
    pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let config_str = std::fs::read_to_string(path)?;
        let config: LocalClusterConfig = toml::from_str(&config_str)?;
        Ok(config)
    }

    pub fn get_bootstrap_node(&self) -> Option<&CustomValidatorConfig> {
        self.validators.first()
    }

    pub fn get_validator_nodes(&self) -> Vec<&CustomValidatorConfig> {
        self.validators.iter().skip(1).collect()
    }
}
