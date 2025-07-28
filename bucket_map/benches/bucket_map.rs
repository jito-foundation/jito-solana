use {
    bencher::{benchmark_main, Bencher, TDynBenchFn, TestDesc, TestDescAndFn, TestFn},
    rayon::prelude::*,
    solana_bucket_map::bucket_map::{BucketMap, BucketMapConfig},
    solana_pubkey::Pubkey,
    std::{borrow::Cow, collections::hash_map::HashMap, sync::RwLock, vec},
};

type IndexValue = u64;

/// Orphan rules workaround that allows for implementation of `TDynBenchFn`.
struct Bench<T>(T);

impl<T> TDynBenchFn for Bench<T>
where
    T: Fn(&mut Bencher) + Send,
{
    fn run(&self, harness: &mut Bencher) {
        (self.0)(harness)
    }
}

/// Benchmark insert with Hashmap as baseline for N threads inserting M keys each
fn do_bench_insert_baseline_hashmap(b: &mut Bencher, n: usize, m: usize) {
    let index = RwLock::new(HashMap::new());
    (0..n).into_par_iter().for_each(|i| {
        let key = Pubkey::new_unique();
        index
            .write()
            .unwrap()
            .insert(key, vec![(i, IndexValue::default())]);
    });
    b.iter(|| {
        (0..n).into_par_iter().for_each(|_| {
            for j in 0..m {
                let key = Pubkey::new_unique();
                index
                    .write()
                    .unwrap()
                    .insert(key, vec![(j, IndexValue::default())]);
            }
        })
    });
}

/// Benchmark insert with BucketMap with N buckets for N threads inserting M keys each
fn do_bench_insert_bucket_map(b: &mut Bencher, n: usize, m: usize) {
    let index = BucketMap::new(BucketMapConfig::new(n));
    (0..n).into_par_iter().for_each(|i| {
        let key = Pubkey::new_unique();
        index.update(&key, |_| Some((vec![(i, IndexValue::default())], 0)));
    });
    b.iter(|| {
        (0..n).into_par_iter().for_each(|_| {
            for j in 0..m {
                let key = Pubkey::new_unique();
                index.update(&key, |_| Some((vec![(j, IndexValue::default())], 0)));
            }
        })
    });
}

/// Benchmark cases represented as tuple (N, M), where N represents number of threads and M number of keys
const BENCH_CASES: &[(usize, usize)] = &[(1, 2), (2, 4), (4, 8), (8, 16), (16, 32), (32, 64)];

/// Logic in this function in big chunk comes from the expanded `bencher::benchmarks_group!` macro
/// This implementation brings clarity, without the need for separate functions per bench case
pub fn benches() -> Vec<TestDescAndFn> {
    let mut benches = vec![];

    BENCH_CASES.iter().enumerate().for_each(|(i, &(n, m))| {
        let name = format!("{:?}-bench_insert_baseline_hashmap[{:?}, {:?}]", i, n, m);
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(name),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(Bench(move |b: &mut Bencher| {
                do_bench_insert_baseline_hashmap(b, n, m);
            }))),
        });
    });

    BENCH_CASES.iter().enumerate().for_each(|(i, &(n, m))| {
        let name = format!("{:?}-bench_insert_bucket_map[{:?}, {:?}]", i, n, m);
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(name),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(Bench(move |b: &mut Bencher| {
                do_bench_insert_bucket_map(b, n, m);
            }))),
        });
    });

    benches
}

benchmark_main!(benches);
