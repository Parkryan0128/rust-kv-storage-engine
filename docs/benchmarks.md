# Benchmark results

Measured 2026-09-27 in an x86_64 Linux container, kernel 6.18.44, AMD EPYC 9V74 host CPU, overlayfs workspace, Rust 1.98.1, optimized release build. The container's CPU/storage allocation is not controlled. These are reproducible workload definitions with one recorded local run, not production hardware guarantees.

## Fixed workload latency report

Command:

```bash
cargo run --locked --release --example bench_report
# Optional sample count override:
KV_BENCH_OPS=100000 cargo run --locked --release --example bench_report
```

Single client; fixed RNG seed `0xace123`; 8-byte keys; 128-byte values; 64 KiB memtable/WAL rotation threshold; 4 KiB target blocks; compaction at four live SSTs. Each workload uses a fresh database. Read and mixed workloads preload 5,000 keys and compact before measurement. Warm-cache reads visit every key before timing; cache-disabled reads use zero engine cache capacity. Sequential writes add 10,000 keys; mixed operations alternate reads and synced overwrites of random preloaded keys.

The timer includes foreground lock/maintenance contention, but excludes setup and final explicit flush. Each persistent write still calls `sync_all()`. Percentiles use sorted per-operation elapsed times at floor((N−1)×percentile); throughput uses total timed loop wall time, including sample collection. Times below are microseconds.

| Workload | Operations | ops/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|
| Sequential synced writes | 10,000 | 303,255 | 1.783 | 8.363 |
| Random SST reads, warm engine cache | 10,000 | 2,234,587 | 0.390 | 0.701 |
| Random SST reads, engine cache disabled | 10,000 | 266,285 | 3.605 | 4.967 |
| 50% reads / 50% synced writes | 10,000 | 343,027 | 2.033 | 15.924 |

[Raw measured counters and CSV](benchmark-results.csv). Warm reads recorded zero data-block read calls and 10,000 cache hits. Cache-disabled reads recorded 10,000 data-block read calls. This demonstrates engine cache behavior, **not** physical disk I/O counts.

**Interpretation:** overlayfs sync times in this environment are unusually short and must not be presented as physical-media durability latency. Disabling the engine cache does not evict kernel page-cache data. Throughput will differ substantially on real storage and across machines/runs.

## Criterion growing-dataset benchmark

```bash
cargo bench --locked --bench engine
```

Twenty samples per workload, 300 ms warmup, approximately one second measurement. Sequential writes grow the database throughout Criterion's adaptive calibration/sampling. Random reads use that resulting dataset; the final workload alternates reads and overwrites. This is a separate, larger/growing workload, so the numbers are not directly comparable with the fixed 5,000-key latency report.

Recorded Criterion intervals:

| Workload | Time interval µs/op | Throughput interval ops/s |
|---|---:|---:|
| Sequential synced writes | 30.231–37.007 | 27,022–33,079 |
| Random SST reads | 14.326–15.107 | 66,196–69,805 |
| Mixed half synced writes | 21.970–27.207 | 36,755–45,517 |

Criterion writes detailed reports under `target/criterion/`. Its timing intervals are not per-operation p50/p99 values. The increasing cost as the dataset grows reflects, in part, full-run compaction's write amplification and a working set larger than the block cache. Multi-level compaction and group commit would be separate future designs, not features claimed here.
