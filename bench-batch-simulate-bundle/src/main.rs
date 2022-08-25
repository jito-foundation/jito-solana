mod simulator;

use {
    crate::simulator::{Simulator, Stats},
    clap::Parser,
    log::*,
    num_traits::abs_sub,
    solana_client::{
        pubsub_client::PubsubClient, rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    },
    solana_runtime::cost_model::CostModel,
    solana_sdk::{
        bundle::VersionedBundle,
        clock::Slot,
        commitment_config::{CommitmentConfig, CommitmentLevel},
        message::{
            v0::{LoadedAddresses, MessageAddressTableLookup},
            AddressLoaderError,
        },
        transaction::{AddressLoader, SanitizedTransaction, VersionedTransaction},
    },
    solana_transaction_status::{TransactionDetails, UiConfirmedBlock, UiTransactionEncoding},
    std::{
        cmp::Reverse,
        collections::BinaryHeap,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
        thread::{self, sleep, Builder, JoinHandle},
        time::Duration,
    },
};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// URL of the RPC server with no simulations running
    #[clap(long, env, default_value = "https://api.testnet.solana.com")]
    baseline_rpc_url: String,

    /// websocket URL of the RPC server with no simulations running
    #[clap(long, env, default_value = "ws://api.testnet.solana.com")]
    baseline_ws_url: String,

    /// URL of the RPC server running simulations against
    #[clap(long, env)]
    simulation_rpc_url: String,

    /// websocket URL of the RPC server running simulations against
    #[clap(long, env)]
    simulation_ws_url: String,

    /// duration to run the test for, must be >= [SIMULATION_REFRESH_SECS]
    #[clap(long, env, default_value_t = 60)]
    test_duration_secs: u64,

    /// size of the bundle batch being sent for simulation
    #[clap(long, env, default_value_t = 5)]
    bundle_batch_size: usize,

    /// number of threads sharing a single RPC connection
    #[clap(long, env, default_value_t = 16)]
    n_threads: usize,

    /// number of unique RPC connections
    #[clap(long, env, default_value_t = 32)]
    n_rpc_connections: u64,
}

const SIMULATION_REFRESH_SECS: u64 = 5;
const BUNDLE_SIZE: usize = 3;

pub struct BundleBatch {
    pub bundles: Vec<VersionedBundle>,
    pub simulation_slot: Slot,
}

fn main() {
    env_logger::init();

    println!("starting load test...");

    let args = Args::parse();
    assert!(args.test_duration_secs >= SIMULATION_REFRESH_SECS);

    let stats = Arc::new(Stats {
        total_rpc_errs: Arc::new(AtomicU64::new(0)),
        total_sim_errs: Arc::new(AtomicU64::new(0)),
        total_sim_success: Arc::new(AtomicU64::new(0)),
    });
    let simulation_refresh_interval = Duration::from_secs(SIMULATION_REFRESH_SECS);
    let exit = Arc::new(AtomicBool::new(false));

    // get the current finalized slots of each node and make sure they're not too far off
    const TOLERABLE_SLOT_DIFF: i64 = 3;
    let baseline_rpc_client = RpcClient::new(args.baseline_rpc_url.clone());
    let simulation_rpc_client = RpcClient::new(args.simulation_rpc_url.clone());
    let (baseline_node_slot, simulation_node_slot) = fetch_and_assert_slot_diff(
        &baseline_rpc_client,
        &simulation_rpc_client,
        Some(TOLERABLE_SLOT_DIFF),
    );
    println!(
        "[baseline_node_slot: {}, simulation_node_slot: {}, diff: {}]",
        baseline_node_slot,
        simulation_node_slot,
        abs_sub(baseline_node_slot, simulation_node_slot)
    );

    let t_hdls = vec![
        spawn_slots_subscribe_thread(
            args.simulation_ws_url,
            "simulation-node".into(),
            exit.clone(),
        ),
        spawn_slots_subscribe_thread(args.baseline_ws_url, "baseline-node".into(), exit.clone()),
    ];

    let rpc_client = RpcClient::new(args.baseline_rpc_url.clone());
    let (transactions, simulation_slot) =
        fetch_n_highest_cost_transactions(&rpc_client, BUNDLE_SIZE);

    let bundle = VersionedBundle { transactions };
    let bundles = (0..args.bundle_batch_size)
        .map(|_| bundle.clone())
        .collect::<Vec<VersionedBundle>>();
    drop(bundle);

    // This object is read-locked by all Simulator threads and write-locked by `spawn_highest_cost_bundle_scraper`
    // periodically to update.
    let bundle_batch = BundleBatch {
        bundles,
        simulation_slot,
    };
    let bundle_batch = Arc::new(RwLock::new(bundle_batch));

    spawn_highest_cost_bundle_scraper(
        bundle_batch.clone(),
        rpc_client,
        simulation_refresh_interval,
        args.bundle_batch_size,
        BUNDLE_SIZE,
    );

    let simulators: Vec<Arc<Simulator>> = (0..args.n_rpc_connections)
        .map(|_| {
            let stats = stats.clone();
            let rpc_client = RpcClient::new(args.simulation_rpc_url.clone());
            Arc::new(Simulator::new(
                rpc_client,
                stats,
                args.n_threads,
                exit.clone(),
            ))
        })
        .collect();
    for s in &simulators {
        let s = s.clone();
        let bundle_batch = bundle_batch.clone();
        thread::spawn(move || {
            s.start(bundle_batch);
        });
    }

    sleep(Duration::from_secs(args.test_duration_secs));
    exit.store(true, Ordering::Relaxed);

    for t in t_hdls {
        info!("joining...");
        t.join().unwrap();
    }

    {
        let t0 = stats.total_sim_success.load(Ordering::Acquire) as f64;
        let t1 = stats.total_sim_errs.load(Ordering::Acquire) as f64;
        let actual_rps = (t0 + t1) / args.test_duration_secs as f64;
        println!(
            "[successful simulations: {}, total_sim_errs: {}, total_rpc_errs: {}, actual_rps: {}]",
            stats.total_sim_success.load(Ordering::Acquire),
            stats.total_sim_errs.load(Ordering::Acquire),
            stats.total_rpc_errs.load(Ordering::Acquire),
            actual_rps,
        );

        let (baseline_node_slot, simulation_node_slot) =
            fetch_and_assert_slot_diff(&baseline_rpc_client, &simulation_rpc_client, None);
        println!(
            "[baseline_node_slot: {}, simulation_node_slot: {}, diff: {}]",
            baseline_node_slot,
            simulation_node_slot,
            abs_sub(baseline_node_slot, simulation_node_slot)
        );
    }

    println!("finished load test...");
}

fn spawn_highest_cost_bundle_scraper(
    bundle_batch: Arc<RwLock<BundleBatch>>,
    rpc_client: RpcClient,
    refresh: Duration,
    batch_size: usize,
    bundle_size: usize,
) -> JoinHandle<()> {
    Builder::new()
        .name("highest-cost-tx-scraper".into())
        .spawn(move || loop {
            let (transactions, simulation_slot) =
                fetch_n_highest_cost_transactions(&rpc_client, bundle_size);

            let bundle = VersionedBundle { transactions };
            let bundles = (0..batch_size)
                .map(|_| bundle.clone())
                .collect::<Vec<VersionedBundle>>();
            drop(bundle);

            let mut w_bundle_batch = bundle_batch.write().unwrap();
            *w_bundle_batch = BundleBatch {
                bundles,
                simulation_slot,
            };
            drop(w_bundle_batch);

            sleep(refresh);
        })
        .unwrap()
}

fn spawn_slots_subscribe_thread(
    pubsub_addr: String,
    node_name: String,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    let mut slots_sub = PubsubClient::slot_subscribe(&*pubsub_addr).unwrap();
    thread::spawn(move || loop {
        if exit.load(Ordering::Acquire) {
            let _ = slots_sub.0.shutdown();
            break;
        }

        match slots_sub.1.recv() {
            Ok(slot_info) => info!("[RPC={} slot={:?}]", node_name, slot_info.slot),
            Err(e) => {
                error!("error receiving on slots_sub channel: {}", e);
                slots_sub = PubsubClient::slot_subscribe(&*pubsub_addr).unwrap();
            }
        }
    })
}

/// Fetches the N highest cost transactions from the last confirmed block and returns said block's parent slot
fn fetch_n_highest_cost_transactions(
    rpc_client: &RpcClient,
    n: usize,
) -> (Vec<VersionedTransaction>, Slot) {
    let slot = rpc_client
        .get_slot_with_commitment(CommitmentConfig::confirmed())
        .unwrap();
    info!("fetched slot {}", slot);

    let config = RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Base64),
        transaction_details: Some(TransactionDetails::Full),
        rewards: None,
        commitment: Some(CommitmentConfig {
            commitment: CommitmentLevel::Confirmed,
        }),
        max_supported_transaction_version: None,
    };
    let block = rpc_client
        .get_block_with_config(slot, config)
        .expect(&*format!("failed to fetch block at slot: {}", slot));

    let parent_slot = block.parent_slot;
    (
        n_highest_cost_transactions_from_block(block, &CostModel::default(), n),
        parent_slot,
    )
}

#[derive(Eq)]
struct TransactionCost {
    transaction: VersionedTransaction,
    cost: u64,
}

impl PartialEq<Self> for TransactionCost {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost
    }
}

impl PartialOrd<Self> for TransactionCost {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.cost.partial_cmp(&other.cost)
    }
}

impl Ord for TransactionCost {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.cost.cmp(&other.cost)
    }
}

/// Returns the N highest cost transactions from a given block
fn n_highest_cost_transactions_from_block(
    block: UiConfirmedBlock,
    cost_model: &CostModel,
    n: usize,
) -> Vec<VersionedTransaction> {
    let txs: Vec<VersionedTransaction> = block
        .transactions
        .unwrap()
        .into_iter()
        .filter(|encoded_tx| encoded_tx.meta.as_ref().unwrap().err.is_none())
        .filter_map(|encoded_tx| encoded_tx.transaction.decode())
        .collect();
    let mut max_costs: BinaryHeap<Reverse<TransactionCost>> = BinaryHeap::with_capacity(n);

    for tx in txs {
        if let Ok(sanitized_tx) = SanitizedTransaction::try_create(
            tx.clone(),
            tx.message.hash(),
            None,
            MockAddressLoader {},
            false,
        ) {
            let cost = cost_model.calculate_cost(&sanitized_tx).sum();
            if let Some(min_cost) = max_costs.peek() {
                if cost > min_cost.0.cost {
                    if max_costs.len() == n {
                        let _ = max_costs.pop();
                    }
                    max_costs.push(Reverse(TransactionCost {
                        cost,
                        transaction: tx.clone(),
                    }));
                }
            } else {
                max_costs.push(Reverse(TransactionCost {
                    cost,
                    transaction: tx.clone(),
                }));
            }
        }
    }

    max_costs
        .into_iter()
        .map(|tx_cost| tx_cost.0.transaction)
        .collect::<Vec<VersionedTransaction>>()
}

fn fetch_and_assert_slot_diff(
    rpc_client_0: &RpcClient,
    rpc_client_1: &RpcClient,
    tolerable_diff: Option<i64>,
) -> (i64, i64) {
    let slot_0 = rpc_client_0
        .get_slot_with_commitment(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        })
        .unwrap() as i64;
    let slot_1 = rpc_client_1
        .get_slot_with_commitment(CommitmentConfig {
            commitment: CommitmentLevel::Finalized,
        })
        .unwrap() as i64;

    if let Some(tolerable_diff) = tolerable_diff {
        let actual_diff = abs_sub(slot_0, slot_1);
        assert!(
            actual_diff < tolerable_diff,
            "{}",
            format!(
                "actual_diff: {}, tolerable_diff: {}",
                actual_diff, tolerable_diff
            )
        );
    }

    (slot_0, slot_1)
}

#[derive(Clone)]
struct MockAddressLoader;

impl AddressLoader for MockAddressLoader {
    fn load_addresses(
        self,
        _lookups: &[MessageAddressTableLookup],
    ) -> Result<LoadedAddresses, AddressLoaderError> {
        Ok(LoadedAddresses::default())
    }
}
