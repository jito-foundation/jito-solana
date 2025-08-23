use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-block-engine-config")
        .about("Set configuration for connection to a block engine")
        .arg(
            Arg::with_name("block_engine_url")
                .long("block-engine-url")
                .help("URL entrypoint to the Block Engine. Connected Block Engine will be autoconfigured unless `--disable-block-engine-autoconfig` is used. Set to empty string to disable block engine connection.")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("disable_block_engine_autoconfig")
                .long("disable-block-engine-autoconfig")
                .takes_value(false)
                .help("Disables Block Engine auto-configuration. This stops the validator client from using the most performant Block Engine region. Values provided to `--block-engine-url` will be used as-is."),
        )
        .arg(
            Arg::with_name("trust_block_engine_packets")
                .long("trust-block-engine-packets")
                .takes_value(false)
                .help("Skip signature verification on block engine packets. Not recommended unless the block engine is trusted.")
        )
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let block_engine_url = value_t_or_exit!(subcommand_matches, "block_engine_url", String);
    let disable_block_engine_autoconfig =
        subcommand_matches.is_present("disable_block_engine_autoconfig");
    let trust_packets = subcommand_matches.is_present("trust_block_engine_packets");
    let admin_client = admin_rpc_service::connect(ledger_path);

    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_block_engine_config(
                block_engine_url,
                disable_block_engine_autoconfig,
                trust_packets,
            )
            .await
    })?;
    Ok(())
}
