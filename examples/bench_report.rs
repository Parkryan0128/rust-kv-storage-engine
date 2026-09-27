//! Fixed-seed single-client latency measurements, including maintenance contention.
use rust_kv_storage_engine::{Engine, Options};
use std::time::Instant;
fn next(r: &mut u64) -> u64 {
    *r ^= *r << 13;
    *r ^= *r >> 7;
    *r ^= *r << 17;
    *r
}
fn main() {
    let n = std::env::var("KV_BENCH_OPS")
        .ok()
        .and_then(|x| x.parse::<usize>().ok())
        .unwrap_or(10000);
    assert!(n > 0);
    println!(
        "workload,operations,ops_per_sec,p50_us,p99_us,block_reads,cache_hits,bloom_negatives"
    );
    for name in [
        "sequential_sync_write",
        "random_sst_read_warm_cache",
        "random_sst_read_no_block_cache",
        "mixed_50_read_50_sync_write",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let opts = Options {
            memtable_size_limit: 64 * 1024,
            block_size: 4096,
            block_cache_capacity: if name == "random_sst_read_no_block_cache" {
                0
            } else {
                8 * 1024 * 1024
            },
            ..Options::default()
        };
        let e = Engine::open_with_options(dir.path(), opts).unwrap();
        let value = vec![42; 128];
        let keys = 5000u64;
        if name != "sequential_sync_write" {
            for k in 0..keys {
                e.put(&k.to_be_bytes(), &value).unwrap();
            }
            e.compact().unwrap();
        }
        if name == "random_sst_read_warm_cache" {
            for k in 0..keys {
                e.get(&k.to_be_bytes()).unwrap();
            }
        }
        let before = e.stats();
        let mut rng = 0xace123;
        let mut samples = Vec::with_capacity(n);
        let start = Instant::now();
        for i in 0..n {
            let key = if name == "sequential_sync_write" {
                i as u64
            } else {
                next(&mut rng) % keys
            };
            let k = key.to_be_bytes();
            let t = Instant::now();
            match name {
                "sequential_sync_write" => e.put(&k, &value).unwrap(),
                "mixed_50_read_50_sync_write" if i % 2 == 0 => e.put(&k, &value).unwrap(),
                _ => {
                    assert!(e.get(&k).unwrap().is_some());
                }
            }
            samples.push(t.elapsed().as_nanos() as u64);
        }
        let elapsed = start.elapsed().as_secs_f64();
        let after = e.stats();
        samples.sort_unstable();
        println!(
            "{name},{n},{:.0},{:.3},{:.3},{},{},{}",
            n as f64 / elapsed,
            samples[(n - 1) * 50 / 100] as f64 / 1000.0,
            samples[(n - 1) * 99 / 100] as f64 / 1000.0,
            after.block_reads - before.block_reads,
            after.cache_hits - before.cache_hits,
            after.bloom_negatives - before.bloom_negatives
        );
        e.flush().unwrap();
    }
}
