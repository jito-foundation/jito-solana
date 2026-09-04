use {
    crate::{
        admin_rpc_service,
        cli::DefaultArgs,
        commands::{Result, jito_args},
    },
    clap::{App, ArgMatches, SubCommand, value_t_or_exit},
    solana_clap_utils::input_parsers::value_of,
    std::path::Path,
};

pub fn command(default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-relayer-config")
        .about("Set configuration for connection to a relayer")
        .arg(jito_args::relayer_url().required(true))
        .arg(jito_args::relayer_expected_heartbeat_interval_ms(
            &default_args.relayer_expected_heartbeat_interval_ms,
        ))
        .arg(jito_args::relayer_max_failed_heartbeats(
            &default_args.relayer_max_failed_heartbeats,
        ))
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let relayer_url = value_t_or_exit!(subcommand_matches, "relayer_url", String);
    let expected_heartbeat_interval_ms: u64 =
        value_of(subcommand_matches, "relayer_expected_heartbeat_interval_ms").unwrap();
    let max_failed_heartbeats: u64 =
        value_of(subcommand_matches, "relayer_max_failed_heartbeats").unwrap();
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_relayer_config(
                relayer_url,
                expected_heartbeat_interval_ms,
                max_failed_heartbeats,
            )
            .await
    })?;
    Ok(())
}
