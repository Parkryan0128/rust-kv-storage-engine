# Benchmarks

Recorded 2026-09-27 with Rust 1.98.1, release build, Linux x86_64, kernel 6.18.44, and an AMD EPYC 9V74 host.

The workspace used container overlayfs. Its unusually short sync times do not represent physical-disk durability latency. Reads with the engine cache disabled can still hit the OS page cache.

## Latency report

```bash
cargo run --locked --release --example bench_report
```

Change the number of operations per workload:

```bash
KV_BENCH_OPS=100000 cargo run --locked --release --example bench_report
```

Settings: one client, seed `0xace123`, 8-byte keys, 128-byte values, 64 KiB memtable/WAL threshold, 4 KiB blocks, and compaction at four SSTs. Each workload starts with a fresh database.

- Sequential writes insert 10,000 keys.
- Read and mixed workloads preload 5,000 keys, then compact.
- Warm-cache reads visit every key before timing.
- Cache-disabled reads set the engine block cache capacity to zero.
- Mixed operations alternate random reads and synced overwrites.

Timing includes foreground lock waits and maintenance contention, but excludes setup and the final explicit flush. Each write calls `sync_all()`. Throughput includes sample collection; p50/p99 use sorted operation times at index `floor((N - 1) * percentile)`.

| Workload | Operations | ops/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|
| Sequential synced writes | 10,000 | 303,255 | 1.783 | 8.363 |
| Random SST reads, warm engine cache | 10,000 | 2,234,587 | 0.390 | 0.701 |
| Random SST reads, engine cache disabled | 10,000 | 266,285 | 3.605 | 4.967 |
| 50% reads / 50% synced writes | 10,000 | 343,027 | 2.033 | 15.924 |

[Raw CSV](benchmark-results.csv). Warm reads recorded zero data-block reads and 10,000 cache hits. Cache-disabled reads recorded 10,000 data-block reads. These counters measure engine calls, not hardware I/O.

## Criterion

```bash
cargo bench --locked --bench engine
```

Each workload uses 20 samples, 300 ms warmup, and a one-second measurement target. Sequential writes grow the database during calibration and sampling. Reads use that dataset; the mixed workload alternates reads and overwrites.

| Workload | Time interval µs/op | Throughput interval ops/s |
|---|---:|---:|
| Sequential synced writes | 30.231–37.007 | 27,022–33,079 |
| Random SST reads | 14.326–15.107 | 66,196–69,805 |
| Mixed half synced writes | 21.970–27.207 | 36,755–45,517 |

Reports are written to `target/criterion/`. These are Criterion timing intervals, not p50/p99. The dataset grows larger than in the latency report, so the two runs are not directly comparable. Full-run compaction also costs more as the database grows.
