# Benchmarks

These are small workloads on container storage (overlayfs). Its very short sync times do not represent physical-disk durability latency.

## Reads and writes

Recorded on 2026-09-27: Rust 1.98.1, release build, Linux x86_64 (kernel 6.18.44), AMD EPYC 9V74.

**ops/s** is operations per second; higher is faster. **p50** is the median time; **p99** is the time within which roughly 99% of operations finished. Times are in microseconds (µs). SSTs are sorted data files on disk.

| Workload | Operations | ops/s | p50 µs | p99 µs |
|---|---:|---:|---:|---:|
| Sequential synced writes | 10,000 | 303,255 | 1.783 | 8.363 |
| Random SST reads, warm engine cache | 10,000 | 2,234,587 | 0.390 | 0.701 |
| Random SST reads, engine cache disabled | 10,000 | 266,285 | 3.605 | 4.967 |
| 50% reads / 50% synced writes | 10,000 | 343,027 | 2.033 | 15.924 |

The cache makes repeated reads much faster in this run. Disabling the engine cache still allows reads from the OS page cache.

Run the report:

```bash
cargo run --locked --release --example bench_report
```

Use `KV_BENCH_OPS=100000` before the command to run 100,000 operations per workload.

Settings: one client, eight-byte keys, 128-byte values, 64 KiB memtable/WAL limit, 4 KiB blocks, and compaction at four files. Read and mixed tests preload and compact 5,000 keys. Warm-cache reads visit every key before timing.

Each write calls `sync_all()`. Timing includes lock waits and background-work contention, but excludes setup and the final flush. Mixed operations alternate reads and overwrites.

[Raw results](benchmark-results.csv) · [Benchmark code](../examples/bench_report.rs)

### Criterion runs

```bash
cargo bench --locked --bench engine
```

Criterion uses 20 samples, a 300 ms warmup, and a one-second measurement target. Its database grows during sampling, so these results differ from the fixed-size report above.

| Workload | Time interval µs/op | Throughput interval ops/s |
|---|---:|---:|
| Sequential synced writes | 30.231–37.007 | 27,022–33,079 |
| Random SST reads | 14.326–15.107 | 66,196–69,805 |
| Mixed half synced writes | 21.970–27.207 | 36,755–45,517 |

These intervals are Criterion estimates, not p50/p99. Reports are saved in `target/criterion/`.

## Compaction comparison

Compaction combines sorted disk files (SSTables):

- **Full** merges all live files once the threshold is reached.
- **SizeTiered** merges similarly sized files, leaving larger files alone until their size group is ready.

Both use the same engine, durability, and cache settings. Recorded on 2026-09-28 UTC: Rust 1.98.1 release, Linux x86_64/overlayfs, Intel Xeon Platinum 8272CL. Each workload ran three times per policy, alternating order.

```bash
cargo run --locked --release --example compaction_report -- 3 > compaction-results.csv
```

Each run starts with 8,192 keys, then applies 64 batches of 128 changes, flushing after each batch. Settings: eight-byte keys, 128-byte values, 8 MiB memtable/cache, 4 KiB blocks, and compaction at four files.

- **Append:** add 8,192 new keys.
- **Hot-set:** repeatedly update 256 keys.
- **Delete-heavy:** update existing keys, with roughly one-third of changes deleting a key.

### How much gets written?

**Write amplification** is SST bytes written divided by user-data bytes. For example, 19.36× means about 19 bytes of SST output for each byte of user data.

| Workload | Full SST write amplification | Size-tiered | SST bytes written reduction | Compaction output reduction |
|---|---:|---:|---:|---:|
| append | 19.36× | 2.86× | 85.2% | 90.6% |
| hot-set | 13.17× | 1.53× | 88.4% | 96.8% |
| delete-heavy | 13.64× | 2.40× | 82.4% | 90.2% |

SST output includes metadata. User-data bytes count the initial load and later changes: keys plus values for writes, keys only for deletes. WAL/manifest writes and filesystem/device overhead are excluded. File-byte results were identical across the three trials.

### The tradeoff

SizeTiered wrote 82.4–88.4% fewer SST bytes in these workloads. It can also keep old values and deletion markers around longer.

In the delete-heavy workload, SizeTiered used about **1.81× the live SST space** of Full. Median read p99 across the three runs was **19.90 µs**, compared with **11.17 µs** for Full. Reads used the existing engine and OS caches. A full compaction can remove the retained obsolete records.

The [raw CSV](compaction-results.csv) also includes throughput and write/flush latency. Update throughput includes batch flushes and test bookkeeping; write latency measures API calls separately. Flush p99 has only 64 samples per run.

Values are checked against an independent map before and after reopening. No final full merge is forced. Timing varies on the shared host; sustained disk throughput needs larger tests on physical storage.

[Benchmark code](../examples/compaction_report.rs) · [Storage and compaction details](storage-format.md)
