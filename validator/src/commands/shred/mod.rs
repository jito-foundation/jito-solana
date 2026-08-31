use {
    crate::{
        admin_rpc_service,
        cli::DefaultArgs,
        commands::{Result, jito_args},
    },
    clap::{App, ArgMatches, SubCommand, values_t_or_exit},
    std::path::Path,
};

pub fn shred_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-receiver-address")
        .about("Set leader-broadcast shred receiver address(es)")
        .arg(jito_args::shred_receiver_address().required(true))
}

pub fn shred_retransmit_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-retransmit-receiver-address")
        .about("Set TVU retransmit-stage shred receiver address(es)")
        .arg(jito_args::shred_retransmit_receiver_address().required(true))
}

pub fn set_shred_receiver_execute(
    subcommand_matches: &ArgMatches,
    ledger_path: &Path,
) -> Result<()> {
    let addr = values_t_or_exit!(subcommand_matches, "shred_receiver_address", String).join(",");
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_shred_receiver_address(addr).await })?;
    Ok(())
}

pub fn set_shred_retransmit_receiver_execute(
    subcommand_matches: &ArgMatches,
    ledger_path: &Path,
) -> Result<()> {
    let addr = values_t_or_exit!(
        subcommand_matches,
        "shred_retransmit_receiver_address",
        String
    )
    .join(",");
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime().block_on(async move {
        admin_client
            .await?
            .set_shred_retransmit_receiver_address(addr)
            .await
    })?;
    Ok(())
}
