use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rust_kv_storage_engine::{Engine, Options};
use std::time::Duration;
fn benches(c: &mut Criterion) {
    let mut group = c.benchmark_group("durable_lsm");
    group
        .sample_size(20)
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1))
        .throughput(Throughput::Elements(1));
    let dir = tempfile::tempdir().unwrap();
    let e = Engine::open_with_options(
        dir.path(),
        Options {
            memtable_size_limit: 64 * 1024,
            ..Options::default()
        },
    )
    .unwrap();
    let value = vec![42; 128];
    let mut i = 0u64;
    group.bench_function("sequential_sync_write", |b| {
        b.iter(|| {
            i += 1;
            e.put(black_box(&i.to_be_bytes()), black_box(&value))
                .unwrap();
        })
    });
    e.compact().unwrap();
    let keys = i;
    let mut rng = 0xace123u64;
    group.bench_function("random_sst_read", |b| {
        b.iter(|| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            black_box(e.get(black_box(&(rng % keys + 1).to_be_bytes())).unwrap());
        })
    });
    let mut n = 0u64;
    group.bench_function("mixed_half_sync_write", |b| {
        b.iter(|| {
            n += 1;
            let key = (n % keys + 1).to_be_bytes();
            if n % 2 == 0 {
                e.put(black_box(&key), black_box(&value)).unwrap();
            } else {
                black_box(e.get(black_box(&key)).unwrap());
            }
        })
    });
    group.finish();
}
criterion_group!(storage, benches);
criterion_main!(storage);
