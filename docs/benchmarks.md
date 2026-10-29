# Benchmarks

The [2026-10-29 complete comparison](performance-results-2026-10-29.md) includes all 263 public conditions, process memory, SST sizes, slower results and independent controls. See also [cache-policy results](cache-policy-results.md) for tuned RocksDB caches. The older reports below retain their original dates and methodology.

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

## Additional audit workloads

The remaining-workload suite also checks variable-length keys and 128-byte common
prefixes in memory and on disk; narrow snapshots held during 32-operation batches;
and ten-row scans across twelve disjoint SST generations. A bounded mixed run
performs 2,000 rounds of synced 32-operation batches (puts/deletes), 32 random
reads and a retained narrow snapshot, with normal automatic compaction and a
512 KiB memtable target. It reports batch/read p99, 500-round latency windows,
process RSS, file sizes and available flush/compaction counters; final contents
are verified again after reopen. This is repeated six times per backend with
rotating execution order. It is not a sustained production soak or a larger-than-RAM
benchmark. The source audit also tests sparse volatile-page reclamation and
snapshot base/delta allocation accounting.

Engine `memtable_bytes` in older comparison rows is a logical rotation charge;
RocksDB reports its own memtable allocation estimate. These counters are not
interchangeable. Use process RSS/peak RSS for cross-engine memory comparisons.
The engine's new `*_memtable_allocated_bytes` API includes reachable page and
directory capacities but is still an estimate, excluding historical snapshot-only
pages and allocator overhead. Persistent format and WAL sync semantics are unchanged.


Read measurement v2
-------------------
The workload read harness separates throughput, warm latency sampling, and full seeded verification. Throughput consumes and drops owned results without constructing expected values, checking all bytes, or timing each operation. Point workers are ready before timing starts. The same query trace is fully checked outside the timed pass; range row counts must match. First/repeat throughput resource samples are captured before the extra passes. Latency samples come from a separate warm pass. Earlier v1 absolute throughput is not directly comparable: use an identical v2 harness against both revisions. Other diagnostic modes retain their explicitly documented validation and timing costs.

Remaining comparisons also distinguish sorted, shuffled, and repeated-key WAL histories, and check reachable memtable allocation after releasing a large-value snapshot. Allocation counters are engine-specific estimates; compare process RSS separately.

Production snapshot-write diagnostics

The snapshot mutation profile now builds `examples/snapshot_profile.rs` as a normal release example for both revisions. It no longer measures the test-only split-first-entry batch path. Frozen-memory and cache profiles remain release libtest diagnostics and are labeled separately in JSONL. Compare revisions with the same copied example; do not treat earlier libtest timings as production batch latency.

The disjoint-range suite reports first and immediate-repeat scan latency and includes an explicit reopen variant. These probes use warm OS cache and do not measure power-loss recovery or cold-device reads. Criterion random-read and mixed fixtures now have a fixed 20,000 keys and can run independently by name.


Warm range repeats now use the same fixed seeded trace multiple times (24 for
512-key spans, 96 for 32-key spans), identically for every backend. First-pass
and latency/verification sampling retain their original trace lengths. See
[workload methodology](../benchmarks/rocksdb-compare/WORKLOADS.md) for counts and
the longer production snapshot diagnostics. Criterion joins the write fixture's
worker before preparing read measurements.

## Selection and existing-key update comparison

The `[235-perf]` comparison step runs `focused.py` with six alternating baseline/current trials for picker-only and volatile existing-key updates, then six rotating baseline/current/RocksDB trials for durable overwrites. Combine the marker with `[paired-perf]` to run the general workload suite as well. It copies identical diagnostic sources into the explicit baseline checkout.

The picker diagnostic measures selection only at 32, 256, and 768 input tables (below threshold, full, and size-tiered selection). These release-libtest rates are not total compaction throughput and have no RocksDB equivalent. The volatile diagnostic uses the normal release library with 8-byte, 1-KiB, and 64-KiB keys, with and without a retained scan.

Durable overwrites use the normal comparison harness with WAL and a sync on every put for all three backends, 256 resident keys, 128-byte values, and an identical precomputed random trace. Key sizes are 8 bytes, 1 KiB, and 64 KiB; one additional 1-KiB case holds a scan across writes. Validation, initial population, and reopen are outside timing. The memtable budget prevents timed flushes in these cases; the existing flush-pressure suite tests that separate behavior. Internal CPU savings need not translate proportionally to durable throughput, because WAL encoding, I/O, and sync remain.

The directory-sync fault hooks cover errors and process termination immediately before SST publication sync and WAL directory sync. Recovery checks cover acknowledged writes/batches, an unchanged manifest on failed SST publication, blocked-writer notification, and startup cleanup ordering. They simulate software-visible errors and process crashes, not loss of device power.
