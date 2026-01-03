#![allow(deprecated)]
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use solana_clock::Clock;
use solana_program_runtime::sysvar_cache::SysvarCache;
use solana_rent::Rent;
// use solana_sysvar_id::SysvarId;

fn bench_get_clock(c: &mut Criterion) {
    let mut cache = SysvarCache::default();
    let clock = Clock::default();
    cache.set_sysvar_for_tests(&clock);

    // Simulate 1000 calls per iteration to magnify the effect
    c.bench_function("SysvarCache::get_clock_1000_times", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(cache.get_clock()).unwrap();
            }
        })
    });
}

fn bench_get_rent(c: &mut Criterion) {
    let mut cache = SysvarCache::default();
    let rent = Rent::default();
    cache.set_sysvar_for_tests(&rent);

    c.bench_function("SysvarCache::get_rent_1000_times", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                black_box(cache.get_rent()).unwrap();
            }
        })
    });
}

criterion_group!(benches, bench_get_clock, bench_get_rent);
criterion_main!(benches);
