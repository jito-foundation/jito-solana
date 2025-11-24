#![feature(test)]

// Necessary to run benchmarks in Rust
extern crate test;

// --- CONSTANTS ---
const NUM_PUBKEYS: usize = 10_000;
const NUM_FORKS: u64 = 16;
// The number of upsert operations to perform within the benchmark loop
const UPSERTS_PER_ITERATION: usize = NUM_PUBKEYS; 

use {
    rand::{thread_rng, Rng},
    solana_account::AccountSharedData,
    solana_accounts_db::{
        account_info::AccountInfo,
        accounts_index::{
            AccountSecondaryIndexes, AccountsIndex, ReclaimsSlotList, UpsertReclaim,
            ACCOUNTS_INDEX_CONFIG_FOR_BENCHMARKS,
        },
    },
    std::sync::Arc,
    test::Bencher,
};

#[cfg(not(any(target_env = "msvc", target_os = "freebsd")))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[bench]
fn bench_accounts_index_upsert(bencher: &mut Bencher) {
    // 1. SETUP: Prepare necessary data and structures outside the benchmark.
    
    // Generate unique random Pubkeys once.
    let pubkeys: Vec<_> = (0..NUM_PUBKEYS)
        .map(|_| solana_pubkey::new_rand())
        .collect();

    // Initialize the AccountsIndex structure.
    let index = AccountsIndex::<AccountInfo, AccountInfo>::new(
        &ACCOUNTS_INDEX_CONFIG_FOR_BENCHMARKS,
        Arc::default(),
    );

    // Create shared default objects to avoid re-creating them inside the loop.
    let default_account_data = AccountSharedData::default();
    let default_secondary_indexes = AccountSecondaryIndexes::default();
    let default_account_info = AccountInfo::default();

    // 2. PRE-POPULATE: Insert initial data to simulate a live Accounts Index.
    // This step is critical for accurate benchmark results in a realistic scenario.
    let mut pre_reclaims = ReclaimsSlotList::new();
    for f in 0..NUM_FORKS {
        for pubkey in pubkeys.iter().take(NUM_PUBKEYS) {
            index.upsert(
                f,
                f,
                pubkey,
                &default_account_data,
                &default_secondary_indexes,
                default_account_info,
                &mut pre_reclaims,
                UpsertReclaim::PopulateReclaims,
            );
        }
    }

    // 3. BENCHMARK EXECUTION:

    let mut rng = thread_rng();
    let mut fork = NUM_FORKS;
    let mut root = 0;
    
    // Pre-calculate the random indices to be used in the benchmark loop
    // to prevent RNG overhead from skewing the results.
    let random_indices: Vec<usize> = (0..UPSERTS_PER_ITERATION)
        .map(|_| rng.gen_range(0..NUM_PUBKEYS))
        .collect();

    // Reclaims list used inside the bench loop.
    let mut reclaims = ReclaimsSlotList::new();

    bencher.iter(|| {
        for idx in random_indices.iter() {
            // Perform the core operation (upserting random accounts).
            index.upsert(
                fork,
                fork,
                &pubkeys[*idx],
                &default_account_data,
                &default_secondary_indexes,
                default_account_info,
                &mut reclaims,
                UpsertReclaim::PopulateReclaims,
            );
            // NOTE: Clearing the reclaims list is included in the measurement 
            // as it simulates a common operation needed after upserting.
            reclaims.clear();
        }
        
        // Simulating the aging of the account index (adding root).
        index.add_root(root);
        root += 1;
        fork += 1;
    });
}
