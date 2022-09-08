use {
    rayon::{
        iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
        ThreadPool,
    },
    solana_rayon_threadlimit::get_thread_count,
    solana_sdk::transaction::{Result, TransactionAccountLocks},
    std::collections::{HashMap, HashSet},
};

lazy_static! {
    static ref PAR_THREAD_POOL: ThreadPool = rayon::ThreadPoolBuilder::new()
        .num_threads(get_thread_count())
        .thread_name(|ix| format!("blockstore_processor_{}", ix))
        .build()
        .unwrap();
}

const DEFAULT_CONFLICT_SET_SIZE: usize = 30;

// for each index, builds a transaction dependency graph of indices that need to execute before
// the current one.
pub fn build_dependency_graphs(
    tx_account_locks_results: &Vec<Result<TransactionAccountLocks>>,
) -> Result<Vec<HashSet<usize>>> {
    if let Some(err) = tx_account_locks_results.iter().find(|r| r.is_err()) {
        err.clone()?;
    }
    let transaction_locks: Vec<_> = tx_account_locks_results
        .iter()
        .map(|r| r.as_ref().unwrap())
        .collect();

    // build a map whose key is a pubkey + value is a sorted vector of all indices that
    // lock that account
    let mut indices_read_locking_account = HashMap::new();
    let mut indicies_write_locking_account = HashMap::new();
    transaction_locks
        .iter()
        .enumerate()
        .for_each(|(idx, tx_account_locks)| {
            for account in &tx_account_locks.readonly {
                indices_read_locking_account
                    .entry(**account)
                    .and_modify(|indices: &mut Vec<usize>| indices.push(idx))
                    .or_insert_with(|| vec![idx]);
            }
            for account in &tx_account_locks.writable {
                indicies_write_locking_account
                    .entry(**account)
                    .and_modify(|indices: &mut Vec<usize>| indices.push(idx))
                    .or_insert_with(|| vec![idx]);
            }
        });

    Ok(PAR_THREAD_POOL.install(|| {
        transaction_locks
            .par_iter()
            .enumerate()
            .map(|(idx, account_locks)| {
                // user measured value from mainnet; rarely see more than 30 conflicts or so
                let mut dep_graph = HashSet::with_capacity(DEFAULT_CONFLICT_SET_SIZE);
                let readlock_accs = account_locks.writable.iter();
                let writelock_accs = account_locks
                    .readonly
                    .iter()
                    .chain(account_locks.writable.iter());

                for acc in readlock_accs {
                    if let Some(indices) = indices_read_locking_account.get(acc) {
                        dep_graph.extend(indices.iter().take_while(|l_idx| **l_idx < idx));
                    }
                }

                for read_acc in writelock_accs {
                    if let Some(indices) = indicies_write_locking_account.get(read_acc) {
                        dep_graph.extend(indices.iter().take_while(|l_idx| **l_idx < idx));
                    }
                }
                dep_graph
            })
            .collect()
    }))
}
