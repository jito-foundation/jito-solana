use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

pub fn shred_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-receiver-address")
        .about("Set shred receiver address(es)")
        .arg(
            Arg::with_name("shred_receiver_address")
                .long("shred-receiver-address")
                .value_name("SHRED_RECEIVER_ADDRESS")
                .takes_value(true)
                .help(
                    "Validator will forward all leader shreds to these addresses in addition to \
                     normal turbine operation. Accepts comma-separated ip:port or host:port \
                     entries. Hostnames resolve to IPv4 addresses only. Up to 32 unique \
                     addresses are allowed. Set to empty string to disable.",
                )
                .required(true),
        )
}

pub fn shred_retransmit_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-retransmit-receiver-address")
        .about("Set shred retransmit receiver address(es)")
        .arg(
            Arg::with_name("shred_retransmit_receiver_address")
                .long("shred-retransmit-receiver-address")
                .value_name("SHRED_RETRANSMIT_RECEIVER_ADDRESS")
                .takes_value(true)
                .help(
                    "Validator will forward all retransmit shreds to these addresses in addition \
                     to normal turbine operation. Accepts comma-separated ip:port or host:port \
                     entries. Hostnames resolve to IPv4 addresses only. Up to 32 unique \
                     addresses are allowed. Set to empty string to disable.",
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
