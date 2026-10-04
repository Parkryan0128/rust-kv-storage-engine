# Benchmarks

## Latency and throughput

Recorded 2026-09-27 with Rust 1.98.1, release build, Linux x86_64, kernel 6.18.44, and an AMD EPYC 9V74 host.

The workspace used container overlayfs. Its unusually short sync times do not represent physical-disk durability latency. Reads with the engine cache disabled can still hit the OS page cache.

### Latency report

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

### Criterion

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

## Compaction comparison

### Policy

The default policy groups SSTs by power-of-two file size and merges the oldest files in the smallest bucket that reaches the configured threshold. Each scheduled merge takes exactly that many inputs. Large cold files stay untouched while small files accumulate. Partial merges retain tombstones; explicit `compact()` performs a full merge and can reclaim them. Both policies use the same file formats.

`CompactionStyle::Full` merges all live SSTs once the file-count threshold is reached. The comparison uses the same engine with either policy; WAL durability, block format, cache, merge iterator, and synchronous flush behavior are shared.

### Reproduce

```bash
cargo run --locked --release --example compaction_report -- 3 > compaction-results.csv
```

Recorded 2026-09-28 UTC: Rust 1.98.1, x86_64 Linux container, Intel Xeon Platinum 8272CL, overlayfs. Three trials per policy/workload; policy order alternates between trials. [Raw CSV](compaction-results.csv).

Each run uses a fresh directory, 8,192 preloaded keys, eight-byte keys, 128-byte values, 64 batches of 128 mutations, and an explicit flush after each batch. Memtable threshold: 8 MiB; block size: 4 KiB; compaction threshold: four files; block cache: 8 MiB. The workload creates repeated small flushes alongside a larger existing SST.

- **Append:** 8,192 new distinct keys.
- **Hot-set:** repeatedly overwrite 256 existing keys.
- **Delete-heavy:** seeded random keys among the original 8,192, with roughly one-third deletes.

Every run compares 4,096 read results with an independent map, closes the database, reopens it, and checks every possible key against that map. No forced full compaction is performed at the end: retained versions and tombstones are part of the policy tradeoff.

### File writes

SST write amplification = `(flush_bytes + compaction_output_bytes) / logical_bytes`. The denominator includes preload and subsequent mutations: key plus value bytes for puts, key bytes for deletes. Numerator includes complete SST file lengths, including metadata. WAL, manifest, filesystem/device amplification, and failed writes are excluded. The selected-input counter sums file lengths, not measured physical reads.

File-byte results were identical across all three trials.

| Workload | Full SST write amplification | Size-tiered | SST bytes written reduction | Compaction output reduction |
|---|---:|---:|---:|---:|
| append | 19.36× | 2.86× | 85.2% | 90.6% |
| hot-set | 13.17× | 1.53× | 88.4% | 96.8% |
| delete-heavy | 13.64× | 2.40× | 82.4% | 90.2% |

### Latency and space tradeoffs

The table shows the median of three per-run measurements:

- **Update throughput** includes mutation calls, model bookkeeping, and batch flush/compaction time. It excludes preload, reads, and recovery checks.
- **Write p99** measures mutation API calls. **Flush p99** measures the explicit flush calls, including compaction.
- **Read p99** comes from a deterministic read pass with the engine and OS caches left intact.

Percentiles use the sorted sample at `floor((N-1)*p/100)`. Each run has 64 flush samples, so flush p99 is based on a small sample.

| Workload | Policy | Update ops/s | Write p99 µs | Flush p99 µs | Read p99 µs | Live SST bytes |
|---|---|---:|---:|---:|---:|---:|
| append | Full | 31,456 | 21.01 | 12,271.20 | 16.85 | 2,550,092 |
| append | SizeTiered | 85,034 | 22.83 | 5,246.25 | 13.52 | 2,550,128 |
| hot-set | Full | 42,486 | 23.92 | 6,897.55 | 12.31 | 1,295,540 |
| hot-set | SizeTiered | 97,471 | 22.63 | 2,498.13 | 12.27 | 1,315,944 |
| delete-heavy | Full | 42,304 | 22.66 | 6,194.32 | 11.17 | 1,028,897 |
| delete-heavy | SizeTiered | 81,879 | 23.01 | 3,932.66 | 19.90 | 1,859,632 |

In the delete-heavy workload, SizeTiered keeps about 1.81× as many live SST bytes as Full and has a higher read p99. An unselected cold SST still contains old values, so partial merges must keep the tombstones that hide them. An explicit full compaction removes those obsolete records.

These measurements come from small synthetic workloads on container storage. The file-write counts were consistent across the three trials; latency varies with the shared host. Testing larger datasets on physical storage would be needed to assess sustained disk throughput.
