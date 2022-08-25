use {
    crate::{BundleBatch, Slot},
    log::*,
    rayon::{ThreadPool, ThreadPoolBuilder},
    solana_client::{
        rpc_client::RpcClient,
        rpc_config::{RpcSimulateBundleConfig, SimulationSlotConfig},
        rpc_response::RpcBundleSimulationSummary,
    },
    solana_sdk::bundle::VersionedBundle,
    std::{
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, RwLock,
        },
        thread::sleep,
        time::Duration,
    },
};

pub struct Simulator {
    t_pool: ThreadPool,
    /// shared tcp socket amongst the thread pool
    rpc_client: Arc<RpcClient>,
    stats: Arc<Stats>,
    exit: Arc<AtomicBool>,
}

pub struct Stats {
    pub total_rpc_errs: Arc<AtomicU64>,
    pub total_sim_errs: Arc<AtomicU64>,
    pub total_sim_success: Arc<AtomicU64>,
}

impl Simulator {
    pub fn new(
        rpc_client: RpcClient,
        stats: Arc<Stats>,
        n_threads: usize,
        exit: Arc<AtomicBool>,
    ) -> Self {
        let t_pool = ThreadPoolBuilder::new()
            .num_threads(n_threads)
            .build()
            .unwrap();
        let rpc_client = Arc::new(rpc_client);

        Self {
            t_pool,
            rpc_client,
            stats,
            exit,
        }
    }

    pub fn start(&self, bundle_batch: Arc<RwLock<BundleBatch>>) {
        info!("starting bundle batch simulator...");

        loop {
            if self.exit.load(Ordering::Relaxed) {
                info!("simulator exiting...");
                break;
            }

            let (bundles, simulation_slot) = {
                let r_bundle_batch = bundle_batch.read().unwrap();
                (
                    r_bundle_batch.bundles.clone(),
                    r_bundle_batch.simulation_slot,
                )
            };
            let rpc_client = self.rpc_client.clone();
            let stats = self.stats.clone();

            self.t_pool.spawn(move || {
                // TODO: is this slow?
                if let Some((n_succeeded, n_failed)) =
                    Self::do_simulate(bundles, simulation_slot, &rpc_client)
                {
                    stats
                        .total_sim_success
                        .fetch_add(n_succeeded, Ordering::Relaxed);
                    stats.total_sim_errs.fetch_add(n_failed, Ordering::Relaxed);
                    info!(
                        "succeeded={}, failed={}, simulation_slot={}",
                        n_succeeded, n_failed, simulation_slot
                    );
                } else {
                    stats.total_rpc_errs.fetch_add(1, Ordering::Relaxed);
                }
            });

            sleep(Duration::from_millis(10));
        }
    }

    /// returns (num_succeeded, num_failed) simulations
    fn do_simulate(
        bundles: Vec<VersionedBundle>,
        simulation_slot: Slot,
        rpc_client: &Arc<RpcClient>,
    ) -> Option<(u64, u64)> {
        let configs = bundles
            .iter()
            .map(|b| RpcSimulateBundleConfig {
                // TODO: Let's set some accounts data for more realistic performance metrics.
                pre_execution_accounts_configs: vec![None; b.transactions.len()],
                post_execution_accounts_configs: vec![None; b.transactions.len()],
                replace_recent_blockhash: true,
                simulation_bank: Some(SimulationSlotConfig::Slot(simulation_slot)),
                skip_sig_verify: true,
                transaction_encoding: None,
            })
            .collect::<Vec<RpcSimulateBundleConfig>>();

        match rpc_client
            .batch_simulate_bundle_with_config(bundles.into_iter().zip(configs).collect())
        {
            Ok(response) => {
                let mut n_succeeded: u64 = 0;
                let mut n_failed: u64 = 0;

                for result in response {
                    match result.result.value.summary {
                        RpcBundleSimulationSummary::Failed {
                            error,
                            tx_signature,
                        } => {
                            error!(
                                "bundle simulation failed [error={:?}, tx_signature={}]",
                                error, tx_signature
                            );
                            n_failed = n_failed.checked_add(1).unwrap();
                        }
                        RpcBundleSimulationSummary::Succeeded => {
                            n_succeeded = n_succeeded.checked_add(1).unwrap()
                        }
                    }
                }

                Some((n_succeeded, n_failed))
            }
            Err(e) => {
                error!("error from rpc {}", e);
                None
            }
        }
    }
}
