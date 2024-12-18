use {
    crate::{admin_rpc_service, cli::DefaultArgs},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::{path::Path, process::exit},
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-block-engine-config")
        .about("Set configuration for connection to a block engine")
        .arg(
            Arg::with_name("block_engine_url")
                .long("block-engine-url")
                .help("Block engine url.  Set to empty string to disable block engine connection.")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("trust_block_engine_packets")
                .long("trust-block-engine-packets")
                .takes_value(false)
                .help("Skip signature verification on block engine packets. Not recommended unless the block engine is trusted.")
        )
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) {
    let block_engine_url = value_t_or_exit!(subcommand_matches, "block_engine_url", String);
    let trust_packets = subcommand_matches.is_present("trust_block_engine_packets");
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move {
            admin_client
                .await?
                .set_block_engine_config(block_engine_url, trust_packets)
                .await
        })
        .unwrap_or_else(|err| {
            println!("set block engine config failed: {}", err);
            exit(1);
        });
}
