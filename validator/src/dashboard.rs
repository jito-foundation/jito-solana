use {
    crate::{
        admin_rpc_service, format_name_value, new_spinner_progress_bar, println_name_value,
        ProgressBar,
    },
    console::style,
    solana_core::validator::ValidatorStartProgress,
    solana_rpc_client::rpc_client::RpcClient,
    solana_rpc_client_api::{client_error, request, response::RpcContactInfo},
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, exit::Exit, native_token::Sol,
        pubkey::Pubkey,
    },
    std::{
        io,
        net::SocketAddr,
        path::{Path, PathBuf},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread,
        time::{Duration, SystemTime},
    },
};

pub struct Dashboard {
    progress_bar: ProgressBar,
    ledger_path: PathBuf,
    exit: Arc<AtomicBool>,
}

impl Dashboard {
    pub fn new(
        ledger_path: &Path,
        log_path: Option<&Path>,
        validator_exit: Option<&mut Exit>,
    ) -> Result<Self, io::Error> {
        println_name_value("Ledger location:", &format!("{}", ledger_path.display()));
        if let Some(log_path) = log_path {
            println_name_value("Log:", &format!("{}", log_path.display()));
        }

        let progress_bar = new_spinner_progress_bar();
        progress_bar.set_message("Initializing...");

        let exit = Arc::new(AtomicBool::new(false));
        if let Some(validator_exit) = validator_exit {
            let exit = exit.clone();
            validator_exit.register_exit(Box::new(move || exit.store(true, Ordering::Relaxed)));
        }

        Ok(Self {
            exit,
            ledger_path: ledger_path.to_path_buf(),
            progress_bar,
        })
    }

    pub fn run(self, refresh_interval: Duration) {
        let Self {
            exit,
            ledger_path,
            progress_bar,
            ..
        } = self;
        drop(progress_bar);

        let runtime = admin_rpc_service::runtime();
        while !exit.load(Ordering::Relaxed) {
            let progress_bar = new_spinner_progress_bar();
            progress_bar.set_message("Connecting...");

            let Some((rpc_addr, start_time)) = runtime.block_on(wait_for_validator_startup(
                &ledger_path,
                &exit,
                progress_bar,
                refresh_interval,
            )) else {
                continue;
            };

            let rpc_client = RpcClient::new_socket(rpc_addr);
            let mut identity = match rpc_client.get_identity() {
                Ok(identity) => identity,
                Err(err) => {
                    println!("Failed to get validator identity over RPC: {err}");
                    continue;
                }
            };
            println_name_value("Identity:", &identity.to_string());

            if let Ok(genesis_hash) = rpc_client.get_genesis_hash() {
                println_name_value("Genesis Hash:", &genesis_hash.to_string());
            }

            if let Some(contact_info) = get_contact_info(&rpc_client, &identity) {
                println_name_value(
                    "Version:",
                    &contact_info.version.unwrap_or_else(|| "?".to_string()),
                );
                if let Some(shred_version) = contact_info.shred_version {
                    println_name_value("Shred Version:", &shred_version.to_string());
                }
                if let Some(gossip) = contact_info.gossip {
                    println_name_value("Gossip Address:", &gossip.to_string());
                }
                if let Some(tpu) = contact_info.tpu {
                    println_name_value("TPU Address:", &tpu.to_string());
                }
                if let Some(rpc) = contact_info.rpc {
                    println_name_value("JSON RPC URL:", &format!("http://{rpc}"));
                }
                if let Some(pubsub) = contact_info.pubsub {
                    println_name_value("WebSocket PubSub URL:", &format!("ws://{pubsub}"));
                }
            }

            let progress_bar = new_spinner_progress_bar();
            let mut snapshot_slot_info = None;
            for i in 0.. {
                if exit.load(Ordering::Relaxed) {
                    break;
                }
                if i % 10 == 0 {
                    snapshot_slot_info = rpc_client.get_highest_snapshot_slot().ok();
                }

                let new_identity = rpc_client.get_identity().unwrap_or(identity);
                if identity != new_identity {
                    identity = new_identity;
                    progress_bar.println(format_name_value("Identity:", &identity.to_string()));
                }

                match get_validator_stats(&rpc_client, &identity) {
                    Ok((
                        processed_slot,
                        confirmed_slot,
                        finalized_slot,
                        transaction_count,
                        identity_balance,
                        health,
                    )) => {
                        let uptime = {
                            let uptime =
                                chrono::Duration::from_std(start_time.elapsed().unwrap()).unwrap();

                            format!(
                                "{:02}:{:02}:{:02} ",
                                uptime.num_hours(),
                                uptime.num_minutes() % 60,
                                uptime.num_seconds() % 60
                            )
                        };

                        progress_bar.set_message(format!(
                            "{}{}| \
                                    Processed Slot: {} | Confirmed Slot: {} | Finalized Slot: {} | \
                                    Full Snapshot Slot: {} | Incremental Snapshot Slot: {} | \
                                    Transactions: {} | {}",
                            uptime,
                            if health == "ok" {
                                "".to_string()
                            } else {
                                format!("| {} ", style(health).bold().red())
                            },
                            processed_slot,
                            confirmed_slot,
                            finalized_slot,
                            snapshot_slot_info
                                .as_ref()
                                .map(|snapshot_slot_info| snapshot_slot_info.full.to_string())
                                .unwrap_or_else(|| '-'.to_string()),
                            snapshot_slot_info
                                .as_ref()
                                .and_then(|snapshot_slot_info| snapshot_slot_info
                                    .incremental
                                    .map(|incremental| incremental.to_string()))
                                .unwrap_or_else(|| '-'.to_string()),
                            transaction_count,
                            identity_balance
                        ));
                        thread::sleep(refresh_interval);
                    }
                    Err(err) => {
                        progress_bar.abandon_with_message(format!("RPC connection failure: {err}"));
                        break;
                    }
                }
            }
        }
    }
}

async fn wait_for_validator_startup(
    ledger_path: &Path,
    exit: &AtomicBool,
    progress_bar: ProgressBar,
    refresh_interval: Duration,
) -> Option<(SocketAddr, SystemTime)> {
    let mut admin_client = None;
    loop {
        if exit.load(Ordering::Relaxed) {
            return None;
        }

        if admin_client.is_none() {
            match admin_rpc_service::connect(ledger_path).await {
                Ok(new_admin_client) => admin_client = Some(new_admin_client),
                Err(err) => {
                    progress_bar.set_message(format!("Unable to connect to validator: {err}"));
                    thread::sleep(refresh_interval);
                    continue;
                }
            }
        }

        match admin_client.as_ref().unwrap().start_progress().await {
            Ok(start_progress) => {
                if start_progress == ValidatorStartProgress::Running {
                    let admin_client = admin_client.take().unwrap();

                    let validator_info = async move {
                        let rpc_addr = admin_client.rpc_addr().await?;
                        let start_time = admin_client.start_time().await?;
                        Ok::<_, jsonrpc_core_client::RpcError>((rpc_addr, start_time))
                    }
                    .await;
                    match validator_info {
                        Ok((None, _)) => progress_bar.set_message("RPC service not available"),
                        Ok((Some(rpc_addr), start_time)) => return Some((rpc_addr, start_time)),
                        Err(err) => {
                            progress_bar
                                .set_message(format!("Failed to get validator info: {err}"));
                        }
                    }
                } else {
                    progress_bar.set_message(format!("Validator startup: {start_progress:?}..."));
                }
            }
            Err(err) => {
                admin_client = None;
                progress_bar.set_message(format!("Failed to get validator start progress: {err}"));
            }
        }
        thread::sleep(refresh_interval);
    }
}

fn get_contact_info(rpc_client: &RpcClient, identity: &Pubkey) -> Option<RpcContactInfo> {
    rpc_client
        .get_cluster_nodes()
        .ok()
        .unwrap_or_default()
        .into_iter()
        .find(|node| node.pubkey == identity.to_string())
}

fn get_validator_stats(
    rpc_client: &RpcClient,
    identity: &Pubkey,
) -> client_error::Result<(Slot, Slot, Slot, u64, Sol, String)> {
    let finalized_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::finalized())?;
    let confirmed_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::confirmed())?;
    let processed_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::processed())?;
    let transaction_count =
        rpc_client.get_transaction_count_with_commitment(CommitmentConfig::processed())?;
    let identity_balance = rpc_client
        .get_balance_with_commitment(identity, CommitmentConfig::confirmed())?
        .value;

    let health = match rpc_client.get_health() {
        Ok(()) => "ok".to_string(),
        Err(err) => {
            if let client_error::ErrorKind::RpcError(request::RpcError::RpcResponseError {
                request_id: _,
                code: _,
                message: _,
                data:
                    request::RpcResponseErrorData::NodeUnhealthy {
                        num_slots_behind: Some(num_slots_behind),
                    },
            }) = &err.kind
            {
                format!("{num_slots_behind} slots behind")
            } else {
                "health unknown".to_string()
            }
        }
    };

    Ok((
        processed_slot,
        confirmed_slot,
        finalized_slot,
        transaction_count,
        Sol(identity_balance),
        health,
    ))
}
