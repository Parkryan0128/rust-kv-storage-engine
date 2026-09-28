# Compaction comparison

## Policy

The default policy groups SSTs by power-of-two file size and merges the oldest files in the smallest bucket that reaches the configured threshold. Each scheduled merge takes exactly that many inputs. Large cold files stay untouched while small files accumulate. Partial merges retain tombstones; explicit `compact()` performs a full merge and can reclaim them. File formats are unchanged.

`CompactionStyle::Full` retains the previous all-file selection policy as a comparison option. Both modes use the same WAL durability, block format, cache, merge iterator, and synchronous flush barrier in this benchmark. This compares policies in the current engine, not two independently optimized databases.

## Reproduce

```bash
cargo run --locked --release --example compaction_report -- 3 > compaction-results.csv
```

Recorded 2026-09-28 UTC: Rust 1.98.1, x86_64 Linux container, Intel Xeon Platinum 8272CL, overlayfs. Three trials per policy/workload; policy order alternates between trials. [Raw CSV](compaction-results.csv).

Each run uses a fresh directory, 8,192 preloaded keys, eight-byte keys, 128-byte values, 64 batches of 128 mutations, and an explicit flush after each batch. Memtable threshold: 8 MiB; block size: 4 KiB; compaction threshold: four files; block cache: 8 MiB. This deliberately exposes repeated small flushes beside a larger existing SST.

- **Append:** 8,192 new distinct keys.
- **Hot-set:** repeatedly overwrite 256 existing keys.
- **Delete-heavy:** seeded random keys among the original 8,192, with roughly one-third deletes.

Every run compares 4,096 read results with an independent map, closes the database, reopens it, and checks every possible key against that map. No forced full compaction is performed at the end: retained versions and tombstones are part of the policy tradeoff.

## File writes

SST write amplification = `(flush_bytes + compaction_output_bytes) / logical_bytes`. The denominator includes preload and subsequent mutations: key plus value bytes for puts, key bytes for deletes. Numerator includes complete SST file lengths, including metadata. WAL, manifest, filesystem/device amplification, and failed writes are excluded. The selected-input counter sums file lengths, not measured physical reads.

File-byte results were identical across all three trials.

| Workload | Full SST write amplification | Size-tiered | SST bytes written reduction | Compaction output reduction |
|---|---:|---:|---:|---:|
| append | 19.36× | 2.86× | 85.2% | 90.6% |
| hot-set | 13.17× | 1.53× | 88.4% | 96.8% |
| delete-heavy | 13.64× | 2.40× | 82.4% | 90.2% |

## Latency and space tradeoffs

Median of three per-run measurements below. Update throughput includes mutation calls, model bookkeeping, and batch flush/compaction time; preload, reads, and recovery checks are excluded. Write p99 measures API calls alone, so flush p99 is reported separately rather than hiding maintenance cost. Read p99 measures a deterministic read pass without clearing the engine or OS caches. Percentiles use the sorted sample at `floor((N-1)*p/100)`; there are only 64 flush samples per run.

| Workload | Policy | Update ops/s | Write p99 µs | Flush p99 µs | Read p99 µs | Live SST bytes |
|---|---|---:|---:|---:|---:|---:|
| append | Full | 31,456 | 21.01 | 12,271.20 | 16.85 | 2,550,092 |
| append | SizeTiered | 85,034 | 22.83 | 5,246.25 | 13.52 | 2,550,128 |
| hot-set | Full | 42,486 | 23.92 | 6,897.55 | 12.31 | 1,295,540 |
| hot-set | SizeTiered | 97,471 | 22.63 | 2,498.13 | 12.27 | 1,315,944 |
| delete-heavy | Full | 42,304 | 22.66 | 6,194.32 | 11.17 | 1,028,897 |
| delete-heavy | SizeTiered | 81,879 | 23.01 | 3,932.66 | 19.90 | 1,859,632 |

Delete-heavy tiering retains about 1.81× the live SST bytes of Full in this run and has a higher read p99. The unselected cold SST still contains old values; partial merges must preserve tombstones that hide them. Explicit full compaction reclaims this debt. Lower write amplification does not imply universally faster reads or less disk usage.

These are small synthetic workloads on container storage, not sustained physical-SSD durability throughput, a large-data scaling result, or a comparison against RocksDB. Latency varies with the shared host. The reproducible claim is the file-write reduction on these workloads; workload scale and durability environment must accompany throughput claims.
