use {
    rand::{thread_rng, Rng},
    solana_sdk::clock::Slot,
    std::time::Instant,
    tokio::task::JoinHandle,
};

fn main() {
    let num_blocks_to_fetch: Vec<u64> = vec![100, 250, 500];
    let num_tasks = 64;

    for limit in num_blocks_to_fetch {
        println!(
            "Benchmarking performance of get_confirmed_blocks_with_data for {:?} blocks",
            limit
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let highest_slot: Slot = 123506966; // recent slots are more uniform; genesis slots are tiny

        let start = Instant::now();
        let results: Vec<usize> = runtime.block_on(async {
            let tasks: Vec<JoinHandle<usize>> = (0..num_tasks)
                .map(|_| {
                    let mut rng = thread_rng();
                    let starting_slot: Slot = rng.gen_range(highest_slot - 1_000_000..highest_slot); // prevent caching by requesting random slot
                    runtime.spawn(async move {
                        let bigtable =
                            solana_storage_bigtable::LedgerStorage::new(true, None, None)
                                .await
                                .expect("connected to bigtable");
                        let slots: Vec<_> = (starting_slot..starting_slot + limit).collect();
                        let blocks: Vec<_> = bigtable
                            .get_confirmed_blocks_with_data(&slots.as_slice())
                            .await
                            .expect("got blocks")
                            .collect();

                        blocks.len()
                    })
                })
                .collect();
            let mut results = Vec::new();
            for t in tasks {
                let r = t.await.expect("results fetched");
                results.push(r);
            }
            results
        });
        let elapsed = start.elapsed();
        let num_blocks = results.iter().sum::<usize>();
        println!(
            "results [tasks={}, chunk={}, returned={}, elapsed={:.2}, blocks/s={:?}]",
            num_tasks,
            limit,
            num_blocks,
            elapsed.as_secs_f32(),
            num_blocks as f64 / elapsed.as_secs_f64()
        );
    }
}
