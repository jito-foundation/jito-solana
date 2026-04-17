use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{App, Arg, ArgMatches, SubCommand, value_t_or_exit},
    std::path::Path,
};

pub fn shred_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-receiver-address")
        .about("Set leader-broadcast shred receiver address(es)")
        .arg(
            Arg::with_name("shred_receiver_address")
                .long("shred-receiver-address")
                .value_name("SHRED_RECEIVER_ADDRESS")
                .takes_value(true)
                .help(
                    "Validator will mirror this validator's own broadcast shreds to these \
                     addresses in addition to normal turbine operation. Used for the direct \
                     leader path and replay-triggered rebroadcasts of this validator's slots. \
                     Accepts comma-separated ip:port or host:port entries. Hostnames resolve to \
                     IPv4 addresses only. Up to 32 unique addresses are allowed. Set to empty \
                     string to configure an empty explicit receiver list.",
                )
                .required(true),
        )
}

pub fn shred_retransmit_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-retransmit-receiver-address")
        .about("Set TVU retransmit-stage shred receiver address(es)")
        .arg(
            Arg::with_name("shred_retransmit_receiver_address")
                .long("shred-retransmit-receiver-address")
                .value_name("SHRED_RETRANSMIT_RECEIVER_ADDRESS")
                .takes_value(true)
                .help(
                    "Validator will mirror TVU retransmit-stage shreds to these addresses in \
                     addition to normal turbine operation. This applies only to shreds that enter \
                     retransmit; it does not mirror this validator's own leader broadcast path. \
                     Accepts comma-separated ip:port or host:port entries. Hostnames resolve to \
                     IPv4 addresses only. Up to 32 unique addresses are allowed. Set to empty \
                     string to disable.",
                )
                .required(true),
        )
}

pub fn set_shred_receiver_execute(
    subcommand_matches: &ArgMatches,
    ledger_path: &Path,
) -> Result<()> {
    let addr = value_t_or_exit!(subcommand_matches, "shred_receiver_address", String);
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_shred_receiver_address(addr).await })?;
    Ok(())
}

pub fn set_shred_retransmit_receiver_execute(
    subcommand_matches: &ArgMatches,
    ledger_path: &Path,
) -> Result<()> {
    let addr = value_t_or_exit!(
        subcommand_matches,
        "shred_retransmit_receiver_address",
        String
    );
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_shred_retransmit_receiver_address(addr)
            .await
    })?;
    Ok(())
}
