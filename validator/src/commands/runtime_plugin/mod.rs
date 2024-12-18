use {
    crate::cli::DefaultArgs,
    clap::{value_t, App, AppSettings, Arg, ArgMatches, SubCommand},
    solana_runtime_plugin::runtime_plugin_admin_rpc_service,
    std::{path::Path, process::exit},
    tokio::runtime::Runtime,
};

pub fn command(_default_args: &DefaultArgs) -> App<'_, '_> {
    SubCommand::with_name("runtime-plugin")
        .about("Manage and view runtime plugins")
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .setting(AppSettings::InferSubcommands)
        .subcommand(
            SubCommand::with_name("list")
                .about("List all current running runtime plugins")
        )
        .subcommand(
            SubCommand::with_name("unload")
                .about("Unload a particular runtime plugin. You must specify the runtime plugin name")
                .arg(
                    Arg::with_name("name")
                        .required(true)
                        .takes_value(true)
                )
        )
        .subcommand(
            SubCommand::with_name("reload")
                .about("Reload a particular runtime plugin. You must specify the runtime plugin name and the new config path")
                .arg(
                    Arg::with_name("name")
                        .required(true)
                        .takes_value(true)
                )
                .arg(
                    Arg::with_name("config")
                        .required(true)
                        .takes_value(true)
                )
        )
        .subcommand(
            SubCommand::with_name("load")
                .about("Load a new gesyer plugin. You must specify the config path. Fails if overwriting (use reload)")
                .arg(
                    Arg::with_name("config")
                        .required(true)
                        .takes_value(true)
                )
        )
}

pub fn execute(plugin_subcommand_matches: &ArgMatches, ledger_path: &Path) {
    let runtime_plugin_rpc_client = runtime_plugin_admin_rpc_service::connect(ledger_path);
    let runtime = Runtime::new().unwrap();
    match plugin_subcommand_matches.subcommand() {
        ("list", _) => {
            let plugins = runtime
                .block_on(async move { runtime_plugin_rpc_client.await?.list_plugins().await })
                .unwrap_or_else(|err| {
                    println!("Failed to list plugins: {err}");
                    exit(1);
                });
            if !plugins.is_empty() {
                println!("Currently the following plugins are loaded:");
                for (plugin, i) in plugins.into_iter().zip(1..) {
                    println!("  {i}) {plugin}");
                }
            } else {
                println!("There are currently no plugins loaded");
            }
        }
        ("unload", Some(subcommand_matches)) => {
            if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                runtime
                    .block_on(async {
                        runtime_plugin_rpc_client
                            .await?
                            .unload_plugin(name.clone())
                            .await
                    })
                    .unwrap_or_else(|err| {
                        println!("Failed to unload plugin {name}: {err:?}");
                        exit(1);
                    });
                println!("Successfully unloaded plugin: {name}");
            }
        }
        ("load", Some(subcommand_matches)) => {
            if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                let name = runtime
                    .block_on(async {
                        runtime_plugin_rpc_client
                            .await?
                            .load_plugin(config.clone())
                            .await
                    })
                    .unwrap_or_else(|err| {
                        println!("Failed to load plugin {config}: {err:?}");
                        exit(1);
                    });
                println!("Successfully loaded plugin: {name}");
            }
        }
        ("reload", Some(subcommand_matches)) => {
            if let Ok(name) = value_t!(subcommand_matches, "name", String) {
                if let Ok(config) = value_t!(subcommand_matches, "config", String) {
                    println!(
                        "This command does not work as intended on some systems.\
                        To correctly reload an existing plugin make sure to:\
                            1. Rename the new plugin binary file.\
                            2. Unload the previous version.\
                            3. Load the new, renamed binary using the 'Load' command."
                    );
                    runtime
                        .block_on(async {
                            runtime_plugin_rpc_client
                                .await?
                                .reload_plugin(name.clone(), config.clone())
                                .await
                        })
                        .unwrap_or_else(|err| {
                            println!("Failed to reload plugin {name}: {err:?}");
                            exit(1);
                        });
                    println!("Successfully reloaded plugin: {name}");
                }
            }
        }
        _ => unreachable!(),
    }
}
