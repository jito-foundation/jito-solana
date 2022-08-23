use {
    log::info,
    solana_sdk::clock::Slot,
    solana_transaction_status::ConfirmedBlock,
    std::{
        sync::{Arc, Mutex},
        thread::{self, sleep},
        time::{Duration, Instant},
    },
    tokio::task::JoinHandle,
};

fn main() {
    env_logger::init();

    let num_blocks_to_fetch: Vec<u64> = vec![10];
    let num_tasks = 128;
    let lowest_slot: Slot = 1_000_000;
    let highest_slot: Slot = 135_000_000;
    let task_unit = (highest_slot.checked_sub(lowest_slot).unwrap())
        .checked_div(num_tasks)
        .unwrap();
    let test_duration_s = 4_u64.checked_mul(60).unwrap().checked_mul(60).unwrap();

    let log_duration = Duration::from_secs(1);

    let test_duration = Duration::from_secs(test_duration_s);

    for chunk_size in num_blocks_to_fetch {
        info!(
            "Benchmarking performance of get_confirmed_blocks_with_data for {:?} blocks",
            chunk_size
        );

        let total_blocks_read = Arc::new(Mutex::new(0_usize));

        let thread = {
            let total_blocks_read = total_blocks_read.clone();
            thread::spawn(move || {
                let test_start = Instant::now();

                let mut last_update_time = Instant::now();
                let mut last_update_count = 0;

                while test_start.elapsed() < test_duration {
                    let elapsed = last_update_time.elapsed();
                    if elapsed > log_duration {
                        let total_blocks_read = *total_blocks_read.lock().unwrap();
                        let blocks_received =
                            total_blocks_read.checked_sub(last_update_count).unwrap();
                        let recent_block_rate = blocks_received as f64 / elapsed.as_secs_f64();
                        let total_block_rate =
                            total_blocks_read as f64 / test_start.elapsed().as_secs_f64();
                        info!(
                            "tasks: {}, chunk_size: {}, recent_block_rate: {:.2}, total_blocks_read: {}, total_elapsed: {:.2}, total blocks/s: {:.2}",
                            num_tasks,
                            chunk_size,
                            recent_block_rate,
                            total_blocks_read,
                            test_start.elapsed().as_secs_f64(),
                            total_block_rate
                        );

                        last_update_time = Instant::now();
                        last_update_count = total_blocks_read;
                    }

                    sleep(Duration::from_millis(100));
                }
            })
        };

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let tasks: Vec<JoinHandle<()>> = (0..num_tasks)
                .map(|i| {
                    let total_blocks_read = total_blocks_read.clone();
                    runtime.spawn(async move {
                        let bigtable =
                            solana_storage_bigtable::LedgerStorage::new(true, None, None)
                                .await
                                .expect("connected to bigtable");

                        let start = Instant::now();
                        let mut starting_slot = (task_unit.checked_mul(i).unwrap())
                            .checked_add(lowest_slot)
                            .unwrap();
                        let stopping_slot = starting_slot.checked_add(task_unit).unwrap();

                        while start.elapsed() < test_duration {
                            let slot_requests: Vec<_> = (starting_slot
                                ..starting_slot.checked_add(chunk_size).unwrap_or(u64::MAX))
                                .collect();
                            let slots_blocks: Vec<(Slot, ConfirmedBlock)> = bigtable
                                .get_confirmed_blocks_with_data(slot_requests.as_slice())
                                .await
                                .expect("got blocks")
                                .collect();
                            starting_slot = slots_blocks.last().unwrap().0;
                            {
                                let mut total_blocks_read = total_blocks_read.lock().unwrap();
                                *total_blocks_read =
                                    total_blocks_read.checked_add(slots_blocks.len()).unwrap();
                            }
                            if starting_slot >= stopping_slot {
                                info!("work here is done!!");
                                break;
                            }
                        }
                    })
                })
                .collect();
            for t in tasks {
                t.await.expect("results fetched");
            }
        });

        thread.join().unwrap();
    }
}
