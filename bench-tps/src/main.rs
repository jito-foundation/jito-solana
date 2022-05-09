#![allow(clippy::integer_arithmetic)]
use {
    clap::value_t,
    log::*,
    solana_bench_tps::{
        bench::{do_bench_tps, generate_keypairs},
        cli::{self, ExternalClientType},
        keypairs::get_keypairs,
    },
    solana_client::{
        connection_cache,
        rpc_client::RpcClient,
        tpu_client::{TpuClient, TpuClientConfig},
    },
    solana_genesis::Base64Account,
    solana_gossip::gossip_service::{discover_cluster, get_client, get_multi_client},
    solana_sdk::{
        commitment_config::CommitmentConfig, fee_calculator::FeeRateGovernor, system_program,
    },
    solana_streamer::socket::SocketAddrSpace,
    std::{collections::HashMap, fs::File, io::prelude::*, path::Path, process::exit, sync::Arc},
};

/// Number of signatures for all transactions in ~1 week at ~100K TPS
pub const NUM_SIGNATURES_FOR_TXS: u64 = 100_000 * 60 * 60 * 24 * 7;

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("bench-tps", /*version:*/ None);

    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        entrypoint_addr,
        json_rpc_url,
        websocket_url,
        id,
        num_nodes,
        tx_count,
        keypair_multiplier,
        client_ids_and_stake_file,
        write_to_client_file,
        read_from_client_file,
        target_lamports_per_signature,
        multi_client,
        num_lamports_per_account,
        target_node,
        external_client_type,
        use_quic,
        ..
    } = &cli_config;

    let keypair_count = *tx_count * keypair_multiplier;
    if *write_to_client_file {
        info!("Generating {} keypairs", keypair_count);
        let (keypairs, _) = generate_keypairs(id, keypair_count as u64);
        let num_accounts = keypairs.len() as u64;
        let max_fee =
            FeeRateGovernor::new(*target_lamports_per_signature, 0).max_lamports_per_signature;
        let num_lamports_per_account = (num_accounts - 1 + NUM_SIGNATURES_FOR_TXS * max_fee)
            / num_accounts
            + num_lamports_per_account;
        let mut accounts = HashMap::new();
        keypairs.iter().for_each(|keypair| {
            accounts.insert(
                serde_json::to_string(&keypair.to_bytes().to_vec()).unwrap(),
                Base64Account {
                    balance: num_lamports_per_account,
                    executable: false,
                    owner: system_program::id().to_string(),
                    data: String::new(),
                },
            );
        });

        info!("Writing {}", client_ids_and_stake_file);
        let serialized = serde_yaml::to_string(&accounts).unwrap();
        let path = Path::new(&client_ids_and_stake_file);
        let mut file = File::create(path).unwrap();
        file.write_all(&serialized.into_bytes()).unwrap();
        return;
    }

    info!("Connecting to the cluster");

    match external_client_type {
        ExternalClientType::RpcClient => {
            let client = Arc::new(RpcClient::new_with_commitment(
                json_rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            ));
            let keypairs = get_keypairs(
                client.clone(),
                id,
                keypair_count,
                *num_lamports_per_account,
                client_ids_and_stake_file,
                *read_from_client_file,
            );
            do_bench_tps(client, cli_config, keypairs);
        }
        ExternalClientType::ThinClient => {
            if *use_quic {
                connection_cache::set_use_quic(true);
            }
            let client = if let Ok(rpc_addr) = value_t!(matches, "rpc_addr", String) {
                let rpc = rpc_addr.parse().unwrap_or_else(|e| {
                    eprintln!("RPC address should parse as socketaddr {:?}", e);
                    exit(1);
                });
                let tpu = value_t!(matches, "tpu_addr", String)
                    .unwrap()
                    .parse()
                    .unwrap_or_else(|e| {
                        eprintln!("TPU address should parse to a socket: {:?}", e);
                        exit(1);
                    });

                solana_client::thin_client::create_client(rpc, tpu)
            } else {
                let nodes =
                    discover_cluster(entrypoint_addr, *num_nodes, SocketAddrSpace::Unspecified)
                        .unwrap_or_else(|err| {
                            eprintln!("Failed to discover {} nodes: {:?}", num_nodes, err);
                            exit(1);
                        });
                if *multi_client {
                    let (client, num_clients) =
                        get_multi_client(&nodes, &SocketAddrSpace::Unspecified);
                    if nodes.len() < num_clients {
                        eprintln!(
                            "Error: Insufficient nodes discovered.  Expecting {} or more",
                            num_nodes
                        );
                        exit(1);
                    }
                    client
                } else if let Some(target_node) = target_node {
                    info!("Searching for target_node: {:?}", target_node);
                    let mut target_client = None;
                    for node in nodes {
                        if node.id == *target_node {
                            target_client =
                                Some(get_client(&[node], &SocketAddrSpace::Unspecified));
                            break;
                        }
                    }
                    target_client.unwrap_or_else(|| {
                        eprintln!("Target node {} not found", target_node);
                        exit(1);
                    })
                } else {
                    get_client(&nodes, &SocketAddrSpace::Unspecified)
                }
            };
            let client = Arc::new(client);
            let keypairs = get_keypairs(
                client.clone(),
                id,
                keypair_count,
                *num_lamports_per_account,
                client_ids_and_stake_file,
                *read_from_client_file,
            );
            do_bench_tps(client, cli_config, keypairs);
        }
        ExternalClientType::TpuClient => {
            let rpc_client = Arc::new(RpcClient::new_with_commitment(
                json_rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            ));
            if *use_quic {
                connection_cache::set_use_quic(true);
            }
            let client = Arc::new(
                TpuClient::new(rpc_client, websocket_url, TpuClientConfig::default())
                    .unwrap_or_else(|err| {
                        eprintln!("Could not create TpuClient {:?}", err);
                        exit(1);
                    }),
            );
            let keypairs = get_keypairs(
                client.clone(),
                id,
                keypair_count,
                *num_lamports_per_account,
                client_ids_and_stake_file,
                *read_from_client_file,
            );
            do_bench_tps(client, cli_config, keypairs);
        }
    }
}
