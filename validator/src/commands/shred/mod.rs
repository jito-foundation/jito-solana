use {
    crate::{admin_rpc_service, cli::DefaultArgs, commands::Result},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::path::Path,
};

pub fn shred_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-receiver-address")
        .about("Set shred receiver address")
        .arg(
            Arg::with_name("shred_receiver_address")
                .long("shred-receiver-address")
                .value_name("SHRED_RECEIVER_ADDRESS")
                .takes_value(true)
                .help("Validator will forward all leader shreds to this address in addition to normal turbine operation. Set to empty string to disable.")
                .required(true)
        )
}

pub fn shred_retransmit_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-retransmit-receiver-address")
        .about("Set shred retransmit receiver address")
        .arg(
            Arg::with_name("shred_retransmit_receiver_address")
                .long("shred-retransmit-receiver-address")
                .value_name("SHRED_RETRANSMIT_RECEIVER_ADDRESS")
                .takes_value(true)
                .help("Validator will forward all retransmit shreds to this address in addition to normal turbine operation. Set to empty string to disable.")
                .required(true)
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
