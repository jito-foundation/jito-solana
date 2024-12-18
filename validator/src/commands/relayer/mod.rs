use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    solana_clap_utils::input_parsers::value_of,
    std::path::Path,
};

pub fn command(default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-relayer-config")
        .about("Set configuration for connection to a relayer")
        .arg(
            Arg::with_name("relayer_url")
                .long("relayer-url")
                .help("Relayer url. Set to empty string to disable relayer connection.")
                .takes_value(true)
                .required(true)
        )
        .arg(
            Arg::with_name("trust_relayer_packets")
                .long("trust-relayer-packets")
                .takes_value(false)
                .help("Skip signature verification on relayer packets. Not recommended unless the relayer is trusted.")
        )
        .arg(
            Arg::with_name("relayer_expected_heartbeat_interval_ms")
                .long("relayer-expected-heartbeat-interval-ms")
                .takes_value(true)
                .help("Interval at which the Relayer is expected to send heartbeat messages.")
                .required(false)
                .default_value(&default_args.relayer_expected_heartbeat_interval_ms)
        )
        .arg(
            Arg::with_name("relayer_max_failed_heartbeats")
                .long("relayer-max-failed-heartbeats")
                .takes_value(true)
                .help("Maximum number of heartbeats the Relayer can miss before falling back to the normal TPU pipeline.")
                .required(false)
                .default_value(&default_args.relayer_max_failed_heartbeats)
        )
}

pub fn execute(subcommand_matches: &ArgMatches, ledger_path: &Path) -> Result<()> {
    let relayer_url = value_t_or_exit!(subcommand_matches, "relayer_url", String);
    let trust_packets = subcommand_matches.is_present("trust_relayer_packets");
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
                trust_packets,
                expected_heartbeat_interval_ms,
                max_failed_heartbeats,
            )
            .await
    })?;
    Ok(())
}
