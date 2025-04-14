use {
    crate::{admin_rpc_service, cli::DefaultArgs},
    clap::{value_t_or_exit, App, Arg, ArgMatches, SubCommand},
    std::{path::Path, process::exit},
};

pub fn shred_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-receiver-address")
        .about("Changes shred receiver address")
        .arg(
            Arg::with_name("shred_receiver_address")
                .long("shred-receiver-address")
                .value_name("SHRED_RECEIVER_ADDRESS")
                .takes_value(true)
                .help("Validator will forward all shreds to this address in addition to normal turbine operation. Set to empty string to disable.")
                .required(true)
        )
}

pub fn shred_retransmit_receiver_command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("set-shred-retransmit-receiver-address")
        .about("Changes shred retransmit receiver address")
        .arg(
            Arg::with_name("shred_receiver_address")
                .long("shred-receiver-address")
                .value_name("SHRED_RECEIVER_ADDRESS")
                .takes_value(true)
                .help("Validator will forward all retransmit shreds to this address in addition to normal turbine operation. Set to empty string to disable.")
                .required(true)
        )
}

pub fn set_shred_receiver_execute(subcommand_matches: &ArgMatches, ledger_path: &Path) {
    let addr = value_t_or_exit!(subcommand_matches, "shred_receiver_address", String);
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move { admin_client.await?.set_shred_receiver_address(addr).await })
        .unwrap_or_else(|err| {
            println!("set shred receiver address failed: {}", err);
            exit(1);
        });
}

pub fn set_shred_retransmit_receiver_execute(subcommand_matches: &ArgMatches, ledger_path: &Path) {
    let addr = value_t_or_exit!(subcommand_matches, "shred_receiver_address", String);
    let admin_client = admin_rpc_service::connect(ledger_path);
    admin_rpc_service::runtime()
        .block_on(async move {
            admin_client
                .await?
                .set_shred_retransmit_receiver_address(addr)
                .await
        })
        .unwrap_or_else(|err| {
            println!("set shred receiver address failed: {}", err);
            exit(1);
        });
}
