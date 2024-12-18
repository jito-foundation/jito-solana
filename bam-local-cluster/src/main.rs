use {
    bam_local_cluster::{BamLocalCluster, LocalClusterConfig},
    clap::{App, Arg},
    log::{error, info},
    solana_logger::setup,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    setup();

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
        .get_matches();

    let is_quiet = matches.is_present("quiet");
    let config_path = matches.value_of("config").unwrap();
    let config = LocalClusterConfig::from_file(config_path).expect("Failed to parse TOML");

    info!("Starting cluster with config: {:?}", config);
    let cluster = match BamLocalCluster::new(config.clone(), is_quiet) {
        Ok(cluster) => cluster,
        Err(e) => {
            error!("Failed to start cluster: {}", e);
            return Err(e);
        }
    };

    // Run the cluster (this will block until shutdown is requested)
    if let Err(e) = cluster.run() {
        error!("Cluster error: {}", e);
        return Err(e);
    }

    // Graceful shutdown
    cluster.shutdown();

    Ok(())
}
