//! The `bigtable` subcommand
use {
    crate::ledger_path::canonicalize_ledger_path,
    clap::{
        value_t, value_t_or_exit, values_t_or_exit, App, AppSettings, Arg, ArgMatches, SubCommand,
    },
    log::info,
    serde_json::json,
    solana_clap_utils::{
        input_parsers::pubkey_of,
        input_validators::{is_slot, is_valid_pubkey},
    },
    solana_cli_output::{
        display::println_transaction, CliBlock, CliTransaction, CliTransactionConfirmation,
        OutputFormat,
    },
    solana_ledger::{blockstore::Blockstore, blockstore_db::AccessType},
    solana_sdk::{clock::Slot, pubkey::Pubkey, signature::Signature},
    solana_storage_bigtable::CredentialType,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, EncodeError, TransactionDetails,
        UiTransactionEncoding,
    },
    std::{
        collections::HashSet,
        path::Path,
        process::exit,
        result::Result,
        sync::{atomic::AtomicBool, Arc},
    },
};

async fn upload(
    blockstore: Blockstore,
    starting_slot: Slot,
    ending_slot: Option<Slot>,
    force_reupload: bool,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

    solana_ledger::bigtable_upload::upload_confirmed_blocks(
        Arc::new(blockstore),
        bigtable,
        starting_slot,
        ending_slot,
        force_reupload,
        Arc::new(AtomicBool::new(false)),
    )
    .await
}

async fn delete_slots(
    slots: Vec<Slot>,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let dry_run = config.read_only;
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

    solana_ledger::bigtable_delete::delete_confirmed_blocks(bigtable, slots, dry_run).await
}

async fn first_available_block(
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config).await?;
    match bigtable.get_first_available_block().await? {
        Some(block) => println!("{}", block),
        None => println!("No blocks available"),
    }

    Ok(())
}

async fn block(
    slot: Slot,
    output_format: OutputFormat,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

    let confirmed_block = bigtable.get_confirmed_block(slot).await?;
    let encoded_block = confirmed_block
        .encode_with_options(
            UiTransactionEncoding::Base64,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::Full,
                show_rewards: true,
                max_supported_transaction_version: None,
            },
        )
        .map_err(|err| match err {
            EncodeError::UnsupportedTransactionVersion(version) => {
                format!(
                    "Failed to process unsupported transaction version ({}) in block",
                    version
                )
            }
        })?;

    let cli_block = CliBlock {
        encoded_confirmed_block: encoded_block.into(),
        slot,
    };
    println!("{}", output_format.formatted_string(&cli_block));
    Ok(())
}

async fn blocks(
    starting_slot: Slot,
    limit: usize,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

    let slots = bigtable.get_confirmed_blocks(starting_slot, limit).await?;
    println!("{:?}", slots);
    println!("{} blocks found", slots.len());

    Ok(())
}

async fn compare_blocks(
    starting_slot: Slot,
    limit: usize,
    config: solana_storage_bigtable::LedgerStorageConfig,
    ref_config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let owned_bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("failed to connect to owned bigtable: {:?}", err))?;
    let owned_bigtable_slots = owned_bigtable
        .get_confirmed_blocks(starting_slot, limit)
        .await?;
    info!(
        "owned bigtable {} blocks found ",
        owned_bigtable_slots.len()
    );
    let reference_bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(ref_config)
        .await
        .map_err(|err| format!("failed to connect to reference bigtable: {:?}", err))?;

    let reference_bigtable_slots = reference_bigtable
        .get_confirmed_blocks(starting_slot, limit)
        .await?;
    info!(
        "reference bigtable {} blocks found ",
        reference_bigtable_slots.len(),
    );

    println!(
        "{}",
        json!({
            "num_reference_slots": json!(reference_bigtable_slots.len()),
            "num_owned_slots": json!(owned_bigtable_slots.len()),
            "reference_last_block": json!(reference_bigtable_slots.len().checked_sub(1).map(|i| reference_bigtable_slots[i])),
            "missing_blocks":  json!(missing_blocks(&reference_bigtable_slots, &owned_bigtable_slots)),
        })
    );

    Ok(())
}

async fn confirm(
    signature: &Signature,
    verbose: bool,
    output_format: OutputFormat,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config)
        .await
        .map_err(|err| format!("Failed to connect to storage: {:?}", err))?;

    let transaction_status = bigtable.get_signature_status(signature).await?;

    let mut transaction = None;
    let mut get_transaction_error = None;
    if verbose {
        match bigtable.get_confirmed_transaction(signature).await {
            Ok(Some(confirmed_tx)) => {
                let decoded_tx = confirmed_tx.get_transaction();
                let encoded_tx_with_meta = confirmed_tx
                    .tx_with_meta
                    .encode(UiTransactionEncoding::Json, Some(0))
                    .map_err(|_| "Failed to encode transaction in block".to_string())?;
                transaction = Some(CliTransaction {
                    transaction: encoded_tx_with_meta.transaction,
                    meta: encoded_tx_with_meta.meta,
                    block_time: confirmed_tx.block_time,
                    slot: Some(confirmed_tx.slot),
                    decoded_transaction: decoded_tx,
                    prefix: "  ".to_string(),
                    sigverify_status: vec![],
                });
            }
            Ok(None) => {}
            Err(err) => {
                get_transaction_error = Some(format!("{:?}", err));
            }
        }
    }
    let cli_transaction = CliTransactionConfirmation {
        confirmation_status: Some(transaction_status.confirmation_status()),
        transaction,
        get_transaction_error,
        err: transaction_status.err.clone(),
    };
    println!("{}", output_format.formatted_string(&cli_transaction));
    Ok(())
}

pub async fn transaction_history(
    address: &Pubkey,
    mut limit: usize,
    mut before: Option<Signature>,
    until: Option<Signature>,
    verbose: bool,
    show_transactions: bool,
    query_chunk_size: usize,
    config: solana_storage_bigtable::LedgerStorageConfig,
) -> Result<(), Box<dyn std::error::Error>> {
    let bigtable = solana_storage_bigtable::LedgerStorage::new_with_config(config).await?;

    let mut loaded_block: Option<(Slot, ConfirmedBlock)> = None;
    while limit > 0 {
        let results = bigtable
            .get_confirmed_signatures_for_address(
                address,
                before.as_ref(),
                until.as_ref(),
                limit.min(query_chunk_size),
            )
            .await?;

        if results.is_empty() {
            break;
        }
        before = Some(results.last().unwrap().0.signature);
        assert!(limit >= results.len());
        limit = limit.saturating_sub(results.len());

        for (result, index) in results {
            if verbose {
                println!(
                    "{}, slot={}, memo=\"{}\", status={}",
                    result.signature,
                    result.slot,
                    result.memo.unwrap_or_else(|| "".to_string()),
                    match result.err {
                        None => "Confirmed".to_string(),
                        Some(err) => format!("Failed: {:?}", err),
                    }
                );
            } else {
                println!("{}", result.signature);
            }

            if show_transactions {
                // Instead of using `bigtable.get_confirmed_transaction()`, fetch the entire block
                // and keep it around.  This helps reduce BigTable query traffic and speeds up the
                // results for high-volume addresses
                loop {
                    if let Some((slot, block)) = &loaded_block {
                        if *slot == result.slot {
                            match block.transactions.get(index as usize).map(|tx_with_meta| {
                                (
                                    tx_with_meta.get_transaction(),
                                    tx_with_meta.get_status_meta(),
                                )
                            }) {
                                None => {
                                    println!(
                                        "  Transaction info for {} is corrupt",
                                        result.signature
                                    );
                                }
                                Some((transaction, meta)) => {
                                    println_transaction(
                                        &transaction,
                                        meta.map(|m| m.into()).as_ref(),
                                        "  ",
                                        None,
                                        None,
                                    );
                                }
                            }
                            break;
                        }
                    }
                    match bigtable.get_confirmed_block(result.slot).await {
                        Err(err) => {
                            println!("  Unable to get confirmed transaction details: {}", err);
                            break;
                        }
                        Ok(confirmed_block) => {
                            loaded_block = Some((result.slot, confirmed_block));
                        }
                    }
                }
                println!();
            }
        }
    }
    Ok(())
}

pub trait BigTableSubCommand {
    fn bigtable_subcommand(self) -> Self;
}

impl BigTableSubCommand for App<'_, '_> {
    fn bigtable_subcommand(self) -> Self {
        self.subcommand(
            SubCommand::with_name("bigtable")
                .about("Ledger data on a BigTable instance")
                .setting(AppSettings::InferSubcommands)
                .setting(AppSettings::SubcommandRequiredElseHelp)
                .arg(
                    Arg::with_name("rpc_bigtable_instance_name")
                        .global(true)
                        .long("rpc-bigtable-instance-name")
                        .takes_value(true)
                        .value_name("INSTANCE_NAME")
                        .default_value(solana_storage_bigtable::DEFAULT_INSTANCE_NAME)
                        .help("Name of the target Bigtable instance")
                )
                .subcommand(
                    SubCommand::with_name("upload")
                        .about("Upload the ledger to BigTable")
                        .arg(
                            Arg::with_name("starting_slot")
                                .long("starting-slot")
                                .validator(is_slot)
                                .value_name("START_SLOT")
                                .takes_value(true)
                                .index(1)
                                .help(
                                    "Start uploading at this slot [default: first available slot]",
                                ),
                        )
                        .arg(
                            Arg::with_name("ending_slot")
                                .long("ending-slot")
                                .validator(is_slot)
                                .value_name("END_SLOT")
                                .takes_value(true)
                                .index(2)
                                .help("Stop uploading at this slot [default: last available slot]"),
                        )
                        .arg(
                            Arg::with_name("force_reupload")
                                .long("force")
                                .takes_value(false)
                                .help(
                                    "Force reupload of any blocks already present in BigTable instance\
                                    Note: reupload will *not* delete any data from the tx-by-addr table;\
                                    Use with care.",
                                ),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("delete-slots")
                        .about("Delete ledger information from BigTable")
                        .arg(
                                Arg::with_name("slots")
                                    .index(1)
                                    .value_name("SLOTS")
                                    .takes_value(true)
                                    .multiple(true)
                                    .required(true)
                                    .help("Slots to delete"),
                                )
                            .arg(
                                Arg::with_name("force")
                                    .long("force")
                                    .takes_value(false)
                                    .help(
                                        "Deletions are only performed when the force flag is enabled. \
                                        If force is not enabled, show stats about what ledger data \
                                        will be deleted in a real deletion. "),
                            ),
                        )
                .subcommand(
                    SubCommand::with_name("first-available-block")
                        .about("Get the first available block in the storage"),
                )
                .subcommand(
                    SubCommand::with_name("blocks")
                        .about("Get a list of slots with confirmed blocks for the given range")
                        .arg(
                            Arg::with_name("starting_slot")
                                .long("starting-slot")
                                .validator(is_slot)
                                .value_name("SLOT")
                                .takes_value(true)
                                .index(1)
                                .required(true)
                                .default_value("0")
                                .help("Start listing at this slot"),
                        )
                        .arg(
                            Arg::with_name("limit")
                                .long("limit")
                                .validator(is_slot)
                                .value_name("LIMIT")
                                .takes_value(true)
                                .index(2)
                                .required(true)
                                .default_value("1000")
                                .help("Maximum number of slots to return"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("compare-blocks")
                        .about("Find the missing confirmed blocks of an owned bigtable for a given range \
                                by comparing to a reference bigtable")
                        .arg(
                            Arg::with_name("starting_slot")
                                .validator(is_slot)
                                .value_name("SLOT")
                                .takes_value(true)
                                .index(1)
                                .required(true)
                                .default_value("0")
                                .help("Start listing at this slot"),
                        )
                        .arg(
                            Arg::with_name("limit")
                                .validator(is_slot)
                                .value_name("LIMIT")
                                .takes_value(true)
                                .index(2)
                                .required(true)
                                .default_value("1000")
                                .help("Maximum number of slots to check"),
                        )
                        .arg(
                            Arg::with_name("reference_credential")
                                .long("reference-credential")
                                .short("c")
                                .value_name("REFERENCE_CREDENTIAL_FILEPATH")
                                .takes_value(true)
                                .required(true)
                                .help("File path for a credential to a reference bigtable"),
                        )
                        .arg(
                            Arg::with_name("reference_instance_name")
                                .long("reference-instance-name")
                                .takes_value(true)
                                .value_name("INSTANCE_NAME")
                                .default_value(solana_storage_bigtable::DEFAULT_INSTANCE_NAME)
                                .help("Name of the reference Bigtable instance to compare to")
                        ),
                )
                .subcommand(
                    SubCommand::with_name("block")
                        .about("Get a confirmed block")
                        .arg(
                            Arg::with_name("slot")
                                .long("slot")
                                .validator(is_slot)
                                .value_name("SLOT")
                                .takes_value(true)
                                .index(1)
                                .required(true),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("confirm")
                        .about("Confirm transaction by signature")
                        .arg(
                            Arg::with_name("signature")
                                .long("signature")
                                .value_name("TRANSACTION_SIGNATURE")
                                .takes_value(true)
                                .required(true)
                                .index(1)
                                .help("The transaction signature to confirm"),
                        ),
                )
                .subcommand(
                    SubCommand::with_name("transaction-history")
                        .about(
                            "Show historical transactions affecting the given address \
                             from newest to oldest",
                        )
                        .arg(
                            Arg::with_name("address")
                                .index(1)
                                .value_name("ADDRESS")
                                .required(true)
                                .validator(is_valid_pubkey)
                                .help("Account address"),
                        )
                        .arg(
                            Arg::with_name("limit")
                                .long("limit")
                                .takes_value(true)
                                .value_name("LIMIT")
                                .validator(is_slot)
                                .index(2)
                                .default_value("18446744073709551615")
                                .help("Maximum number of transaction signatures to return"),
                        )
                        .arg(
                            Arg::with_name("query_chunk_size")
                                .long("query-chunk-size")
                                .takes_value(true)
                                .value_name("AMOUNT")
                                .validator(is_slot)
                                .default_value("1000")
                                .help(
                                    "Number of transaction signatures to query at once. \
                                       Smaller: more responsive/lower throughput. \
                                       Larger: less responsive/higher throughput",
                                ),
                        )
                        .arg(
                            Arg::with_name("before")
                                .long("before")
                                .value_name("TRANSACTION_SIGNATURE")
                                .takes_value(true)
                                .help("Start with the first signature older than this one"),
                        )
                        .arg(
                            Arg::with_name("until")
                                .long("until")
                                .value_name("TRANSACTION_SIGNATURE")
                                .takes_value(true)
                                .help("End with the last signature newer than this one"),
                        )
                        .arg(
                            Arg::with_name("show_transactions")
                                .long("show-transactions")
                                .takes_value(false)
                                .help("Display the full transactions"),
                        ),
                ),
        )
    }
}

pub fn bigtable_process_command(ledger_path: &Path, matches: &ArgMatches<'_>) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let verbose = matches.is_present("verbose");
    let output_format = OutputFormat::from_matches(matches, "output_format", verbose);

    // this is kinda stupid, but there seems to be a bug in clap when a subcommand
    // arg is marked both `global(true)` and `default_value("default_value")`.
    // despite the "global", when the arg is specified on the subcommand, its value
    // is not propagated down to the (sub)subcommand args, resulting in the default
    // value when queried there. similarly, if the arg is specified on the
    // (sub)subcommand, the value is not propagated back up to the subcommand args,
    // again resulting in the default value. the arg having declared a
    // `default_value()` obviates `is_present(...)` tests since they will always
    // return true. so we consede and compare against the expected default. :/
    let (subcommand, sub_matches) = matches.subcommand();
    let on_command = matches
        .value_of("rpc_bigtable_instance_name")
        .map(|v| v != solana_storage_bigtable::DEFAULT_INSTANCE_NAME)
        .unwrap_or(false);
    let instance_name = if on_command {
        value_t_or_exit!(matches, "rpc_bigtable_instance_name", String)
    } else {
        let sub_matches = sub_matches.as_ref().unwrap();
        value_t_or_exit!(sub_matches, "rpc_bigtable_instance_name", String)
    };

    let future = match (subcommand, sub_matches) {
        ("upload", Some(arg_matches)) => {
            let starting_slot = value_t!(arg_matches, "starting_slot", Slot).unwrap_or(0);
            let ending_slot = value_t!(arg_matches, "ending_slot", Slot).ok();
            let force_reupload = arg_matches.is_present("force_reupload");
            let blockstore = crate::open_blockstore(
                &canonicalize_ledger_path(ledger_path),
                AccessType::Secondary,
                None,
            );
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };
            runtime.block_on(upload(
                blockstore,
                starting_slot,
                ending_slot,
                force_reupload,
                config,
            ))
        }
        ("delete-slots", Some(arg_matches)) => {
            let slots = values_t_or_exit!(arg_matches, "slots", Slot);
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: !arg_matches.is_present("force"),
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };
            runtime.block_on(delete_slots(slots, config))
        }
        ("first-available-block", Some(_arg_matches)) => {
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: true,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };
            runtime.block_on(first_available_block(config))
        }
        ("block", Some(arg_matches)) => {
            let slot = value_t_or_exit!(arg_matches, "slot", Slot);
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };
            runtime.block_on(block(slot, output_format, config))
        }
        ("blocks", Some(arg_matches)) => {
            let starting_slot = value_t_or_exit!(arg_matches, "starting_slot", Slot);
            let limit = value_t_or_exit!(arg_matches, "limit", usize);
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };

            runtime.block_on(blocks(starting_slot, limit, config))
        }
        ("compare-blocks", Some(arg_matches)) => {
            let starting_slot = value_t_or_exit!(arg_matches, "starting_slot", Slot);
            let limit = value_t_or_exit!(arg_matches, "limit", usize);
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };

            let credential_path = Some(value_t_or_exit!(
                arg_matches,
                "reference_credential",
                String
            ));

            let ref_instance_name =
                value_t_or_exit!(arg_matches, "reference_instance_name", String);
            let ref_config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                credential_type: CredentialType::Filepath(credential_path),
                instance_name: ref_instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };

            runtime.block_on(compare_blocks(starting_slot, limit, config, ref_config))
        }
        ("confirm", Some(arg_matches)) => {
            let signature = arg_matches
                .value_of("signature")
                .unwrap()
                .parse()
                .expect("Invalid signature");
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: false,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };

            runtime.block_on(confirm(&signature, verbose, output_format, config))
        }
        ("transaction-history", Some(arg_matches)) => {
            let address = pubkey_of(arg_matches, "address").unwrap();
            let limit = value_t_or_exit!(arg_matches, "limit", usize);
            let query_chunk_size = value_t_or_exit!(arg_matches, "query_chunk_size", usize);
            let before = arg_matches
                .value_of("before")
                .map(|signature| signature.parse().expect("Invalid signature"));
            let until = arg_matches
                .value_of("until")
                .map(|signature| signature.parse().expect("Invalid signature"));
            let show_transactions = arg_matches.is_present("show_transactions");
            let config = solana_storage_bigtable::LedgerStorageConfig {
                read_only: true,
                instance_name,
                ..solana_storage_bigtable::LedgerStorageConfig::default()
            };

            runtime.block_on(transaction_history(
                &address,
                limit,
                before,
                until,
                verbose,
                show_transactions,
                query_chunk_size,
                config,
            ))
        }
        _ => unreachable!(),
    };

    future.unwrap_or_else(|err| {
        eprintln!("{:?}", err);
        exit(1);
    });
}

fn missing_blocks(reference: &[Slot], owned: &[Slot]) -> Vec<Slot> {
    if owned.is_empty() && !reference.is_empty() {
        return reference.to_owned();
    } else if owned.is_empty() {
        return vec![];
    }

    let owned_hashset: HashSet<_> = owned.iter().collect();
    let mut missing_slots = vec![];
    for slot in reference {
        if !owned_hashset.contains(slot) {
            missing_slots.push(slot.to_owned());
        }
    }
    missing_slots
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_missing_blocks() {
        let reference_slots = vec![0, 37, 38, 39, 40, 41, 42, 43, 44, 45];
        let owned_slots = vec![0, 38, 39, 40, 43, 44, 45, 46, 47];
        let owned_slots_leftshift = vec![0, 25, 26, 27, 28, 29, 30, 31, 32];
        let owned_slots_rightshift = vec![0, 44, 46, 47, 48, 49, 50, 51, 52, 53, 54];
        let missing_slots = vec![37, 41, 42];
        let missing_slots_leftshift = vec![37, 38, 39, 40, 41, 42, 43, 44, 45];
        let missing_slots_rightshift = vec![37, 38, 39, 40, 41, 42, 43, 45];
        assert!(missing_blocks(&[], &[]).is_empty());
        assert!(missing_blocks(&[], &owned_slots).is_empty());
        assert_eq!(
            missing_blocks(&reference_slots, &[]),
            reference_slots.to_owned()
        );
        assert_eq!(
            missing_blocks(&reference_slots, &owned_slots),
            missing_slots
        );
        assert_eq!(
            missing_blocks(&reference_slots, &owned_slots_leftshift),
            missing_slots_leftshift
        );
        assert_eq!(
            missing_blocks(&reference_slots, &owned_slots_rightshift),
            missing_slots_rightshift
        );
    }
}
