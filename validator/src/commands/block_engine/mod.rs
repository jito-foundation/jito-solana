use {
    crate::{
        admin_rpc_service,
        cli::DefaultArgs,
        commands::{Result, jito_args},
    },
    clap::{App, ArgMatches, SubCommand, value_t_or_exit},
    std::path::Path,
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-block-engine-config")
        .about("Set configuration for connection to a block engine")
        .arg(jito_args::block_engine_url().required(true))
        .arg(jito_args::disable_block_engine_autoconfig())
        .arg(jito_args::trust_block_engine_packets())
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
