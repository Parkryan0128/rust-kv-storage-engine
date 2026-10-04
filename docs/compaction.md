# Compaction comparison

## Policy

The default policy groups SSTs by power-of-two file size and merges the oldest files in the smallest bucket that reaches the configured threshold. Each scheduled merge takes exactly that many inputs. Large cold files stay untouched while small files accumulate. Partial merges retain tombstones; explicit `compact()` performs a full merge and can reclaim them. Both policies use the same file formats.

`CompactionStyle::Full` merges all live SSTs once the file-count threshold is reached. The comparison uses the same engine with either policy; WAL durability, block format, cache, merge iterator, and synchronous flush behavior are shared.

## Reproduce

```bash
cargo run --locked --release --example compaction_report -- 3 > compaction-results.csv
```

Recorded 2026-09-28 UTC: Rust 1.98.1, x86_64 Linux container, Intel Xeon Platinum 8272CL, overlayfs. Three trials per policy/workload; policy order alternates between trials. [Raw CSV](compaction-results.csv).

Each run uses a fresh directory, 8,192 preloaded keys, eight-byte keys, 128-byte values, 64 batches of 128 mutations, and an explicit flush after each batch. Memtable threshold: 8 MiB; block size: 4 KiB; compaction threshold: four files; block cache: 8 MiB. The workload creates repeated small flushes alongside a larger existing SST.

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
