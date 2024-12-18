use {
    bam_local_cluster::{BamLocalCluster, LocalClusterConfig},
    clap::{App, Arg},
    log::{error, info},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    agave_logger::setup();
    let matches = App::new("BAM LocalCluster bootstrapper")
        .version("0.1")
        .about("Spins up a local Solana cluster from a TOML config for BAM testing")
        .arg(
            Arg::with_name("config")
                .long("config")
                .value_name("FILE")
                .help("TOML configuration file path")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("quiet")
                .long("quiet")
                .help("Quiet mode does not tail the validator log files to stdout")
                .takes_value(false)
                .required(false),
        )
        .arg(
            Arg::with_name("skip-last-validator")
                .long("skip-last-validator")
                .help("Skip starting the last validator in the configuration")
                .takes_value(false)
                .required(false),
        )
        .get_matches();

    let is_quiet = matches.is_present("quiet");
    let skip_last_validator = matches.is_present("skip-last-validator");
    let config_path = matches.value_of("config").unwrap();
    let config = LocalClusterConfig::from_file(config_path).expect("Failed to parse TOML");
    info!("Starting cluster with config: {config:?}");
    if skip_last_validator {
        if config.validators.len() <= 1 {
            error!("Cannot skip the last validator when only one validator is configured");
            return Err("Skipping the last validator requires at least two validators".into());
        }
        info!("Skipping startup of the last validator process");
    }

    let cluster = match BamLocalCluster::new(config.clone(), is_quiet, skip_last_validator) {
        Ok(cluster) => cluster,
        Err(e) => {
            error!("Failed to start cluster: {e}");
            return Err(e);
        }
    };

    // Run the cluster (this will block until shutdown is requested)
    if let Err(e) = cluster.run() {
        error!("Cluster error: {e}");
        return Err(e);
    }

    // Graceful shutdown
    cluster.shutdown();

    Ok(())
}
