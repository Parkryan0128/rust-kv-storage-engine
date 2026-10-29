# Performance comparison — 2026-10-29

**Historical checkpoint:** the [optimization continuation](performance-results-2026-10-29-continuation.md) supersedes this report for the later implementation. Measurements below remain unchanged and belong to the revisions listed here.

The corrected full run contains **263 public conditions, 5,283 public measurement records and 5,673 records including internal diagnostics**. Random reads are faster than RocksDB in this matrix and use less process memory; synced writes are close. Long common-prefix compactions and some retained-snapshot workloads remain weaker. This is not evidence that every workload is faster. Every public result, including losses and open times, is in the [complete CSV](performance-results-2026-10-29.csv).

## Revisions and measurement

- Measured library/harness revision: `543ecbe3a4311960abfc9cce58b80e6d0742dbc9`. The production Rust sources, root manifest and lockfile are identical to `f759f1329db0d1ead84b893e1a980bb398098fcc`; subsequent reporting/control commits do not change the library.
- Original comparison baseline: `c0299ec14e60a249fb9628f1888c5740c655432b`. It predates the start of this optimization task (`03bac47af9082618261bd48a526f86ea53884c4d`), so these are cumulative comparisons, not gains attributable solely to the latest edits.
- Full run: [40885275681](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40885275681), AMD EPYC 9V45, Linux x86_64, one shared CI host. RocksDB 11.8.1 through rust-rocksdb 0.25.0.
- Every participant uses exactly the same private DB pathname (`fixed-v1`), independently prepared/synced fixtures, warm OS file cache, uncompressed data and independently owned Get results. Applicable block sizes and total cache capacities match. Native SST representations may differ.
- Ratios are medians of trial-matched `other time / current time`; values above one favor this engine. Absolute columns are each backend’s median, so their quotient need not equal the paired ratio. CSV includes paired bootstrap 95% intervals (4,000 resamples, fixed seed), not family-wise corrected hypothesis tests. Small differences across this many exploratory conditions need independent repetition.
- Most full conditions use six trials; ordinary dedicated writes use three, batch32 twelve, and random reads twelve with an additional identical-executable control. Separate focused guards use twelve trials. Internal/instrumented profiles are not public API rankings.

## Random reads

Each row below is the repeated pass of 500,000 successful Gets. All returned bytes are checked in this runner. The CSV also retains every first pass, block size and identical-current control interval.

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| 1000000keys-128b-5sst-16384block | random_read_repeat | 1151.264 | 1154.428 | 1638.309 | 0.994x | 1.418x |
| 1000000keys-128b-5sst-4096block | random_read_repeat | 796.910 | 800.662 | 1194.634 | 0.989x | 1.493x |
| 1000000keys-128b-5sst-8192block | random_read_repeat | 888.355 | 902.876 | 1349.109 | 0.986x | 1.486x |
| 100000keys-1024b-5sst-16384block | random_read_repeat | 1091.479 | 1081.105 | 1503.173 | 1.009x | 1.394x |
| 100000keys-1024b-5sst-4096block | random_read_repeat | 716.867 | 713.174 | 1104.147 | 1.006x | 1.548x |
| 100000keys-1024b-5sst-8192block | random_read_repeat | 835.196 | 830.956 | 1246.769 | 1.004x | 1.499x |
| 100000keys-128b-1sst-16384block | random_read_repeat | 497.532 | 499.083 | 848.952 | 0.993x | 1.701x |
| 100000keys-128b-1sst-4096block | random_read_repeat | 394.728 | 397.970 | 725.549 | 0.992x | 1.827x |
| 100000keys-128b-1sst-8192block | random_read_repeat | 419.698 | 424.361 | 753.037 | 0.986x | 1.772x |

The original engine is already competitive on this fixed-layout workload: current/original differences are mostly small. The 1M-key/8 KiB repeated-read flag is examined with the independent controls below rather than presented as an improvement.

## Memory and storage

These rows fix the block size at 16 KiB. RSS describes the fresh read process after the repeated pass, excluding fixture creation and OS page cache. SST sizes describe database data files, not source-code size. Other block sizes and peak RSS remain in the CSV.

| Condition | Current RSS MiB | RocksDB RSS MiB | Current SST MiB | RocksDB SST MiB |
|---|---:|---:|---:|---:|
| 1000000keys-128b-5sst-16384block | 12.88 | 23.11 | 151.29 | 136.64 |
| 100000keys-1024b-5sst-16384block | 11.64 | 21.79 | 100.80 | 99.28 |
| 100000keys-128b-1sst-16384block | 11.04 | 21.74 | 15.13 | 13.66 |

For 1M 128-byte values at 16 KiB, the current process uses about 44% less RSS, while SST files are about 10.7% larger. Long common-prefix keys have a larger storage difference because native table representations differ. Engine logical memtable charges and RocksDB allocation counters are not interchangeable. Returned values retained by the application remain outside the cache budget.

## Synced writes and recovery

WAL writes retain `sync_all`; RocksDB uses synchronous writes with `use_fsync=true`. No acknowledgement, integrity check or batch atomicity was weakened.

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| batch256-16b | write | 259.661 | 252.265 | 251.223 | 0.993x | 0.998x |
| batch32-128b | write | 227.370 | 224.552 | 225.705 | 1.015x | 0.995x |
| flush-pressure | write | 255.531 | 253.805 | 242.921 | 1.013x | 0.957x |
| put-128b | write | 1605.411 | 1541.933 | 1551.669 | 1.020x | 1.006x |

Dedicated write differences are small and several intervals include equality. Do not turn these into a universal write-speed claim.

Recovery reaches the same endpoint on all backends: replayed data in memory with the durable WAL retained. RocksDB enables `avoid_flush_during_recovery`; every result asserts zero SST files after open.

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| wal-batch256 / recover | recover | 3.099 | 3.081 | 9.200 | 1.011x | 2.995x |
| wal-batch32-random100k / recover | recover | 36.139 | 34.103 | 65.559 | 1.060x | 1.909x |
| wal-overwrite / recover | recover | 1.565 | 1.347 | 9.313 | 1.158x | 6.959x |
| wal-put-random100k / recover | recover | 37.925 | 37.517 | 79.298 | 1.011x | 2.116x |
| wal-put-sorted / recover | recover | 1.904 | 1.885 | 7.485 | 1.006x | 3.973x |
| wal-random / recover | recover | 3.353 | 3.299 | 9.956 | 1.025x | 3.025x |
| wal-single / recover | recover | 2.329 | 1.978 | 7.752 | 1.177x | 3.917x |

## Memory lookups and ranges

These are the full matrix’s existing memory modes; timed validation remains part of the memory-get rows. The separate two-million-query guard excludes full payload validation from throughput and verifies the same seeded trace afterward. On its own EPYC 9V45 host it measured current/original speed ratios of 1.121x (20k keys) and 1.289x (100k), and current/RocksDB ratios of 6.547x and 5.856x. Do not combine absolute durations across those runs or measurement modes.

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| memory-early10 / memory-scan | memory-scan | 4.423 | 4.376 | 9.137 | 1.007x | 2.098x |
| memory-early10-100k / memory-scan | memory-scan | 5.048 | 4.820 | 8.099 | 1.048x | 1.676x |
| memory-early10-200k / memory-scan | memory-scan | 5.232 | 5.199 | 8.122 | 1.004x | 1.559x |
| memory-early10-2k / memory-scan | memory-scan | 4.294 | 4.229 | 8.024 | 1.015x | 1.899x |
| memory-full20k / memory-scan | memory-scan | 10.463 | 10.346 | 16.708 | 1.026x | 1.622x |
| memory-get100k / memory-get | memory-get | 49.733 | 41.562 | 100.205 | 1.217x | 2.451x |
| memory-get20k / memory-get | memory-get | 20.857 | 21.507 | 79.953 | 0.969x | 3.719x |
| memory-middle10-100k / memory-scan | memory-scan | 13.068 | 11.445 | 11.433 | 1.139x | 1.038x |
| memory-narrow10-100k / memory-scan | memory-scan | 9.801 | 9.900 | 11.886 | 0.986x | 1.201x |

## Remaining differences and controlled repeats

All full-matrix conditions with current/original paired median below 0.99 and upper interval below 1 are shown here. This selection identifies candidates for investigation, not proof of causality.

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| 16files-128b-above | bounds-repeat | 0.347 | 0.356 | 8.885 | 0.968x | 24.642x |
| n8-k8-distinct | compact-merge | 19.350 | 19.887 | 18.638 | 0.975x | 0.944x |
| n9-k8-distinct | compact-merge | 15.405 | 17.657 | 19.904 | 0.875x | 1.134x |
| p2-1024b-16384block-8388608cache-clean / uniform-t4-span0 | open | 0.260 | 0.277 | 3.769 | 0.941x | 13.606x |
| 1000000keys-128b-5sst-8192block | random_read_repeat | 888.355 | 902.876 | 1349.109 | 0.986x | 1.486x |
| phase-20000-2m / cache-phase | first | 12.640 | 12.801 | 47.977 | 0.987x | 3.742x |
| phase-20000-2m / cache-phase | repeat | 12.667 | 12.747 | 47.825 | 0.990x | 3.722x |
| phase-20000-4m / cache-phase | repeat | 12.594 | 12.751 | 48.034 | 0.988x | 3.788x |

The independent [24-trial read repeat](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40893254091) used an Intel Xeon Platinum 8573C for the original-baseline job. The flagged 1M-key/8 KiB repeat measured **0.9971x [0.9935, 1.0092]** original; the empty-range repeat was 0.9918x [0.9553, 1.0187], and the flagged open was 0.9941x [0.9523, 1.0265]. Both cache-phase sizes were near equality. None of the original-baseline repeat conditions had a median below 0.99 with an upper interval below one.

The separate identical-source job ran on EPYC 7763 and asserted identical library, manifest, lock and copied harness files. Its repeated million-key read nevertheless measured **1.0123x [1.0069, 1.0252]** against the separately built reference. A source-identical difference is not an optimization gain. This demonstrates why a roughly 1% result cannot by itself establish source causality; do not subtract this control from the other host’s result. The [40-condition control CSV](performance-controls-2026-10-29.csv) preserves both jobs and the merge controls with explicit host/reference labels.

These repeats did not reproduce the initial read flags. They do not rule out a CPU-specific effect on the original EPYC 9V45 host, and the original full-matrix values remain unmodified. The report makes no claim of universally regression-free or mathematically optimal performance.

Long common-prefix compaction remains a workload to disclose. These are all distinct-key 136-byte-key cases, rather than a selected best case:

| Condition | Stage | Original ms | Current ms | RocksDB ms | Current/original speed | Current/RocksDB speed |
|---|---|---:|---:|---:|---:|---:|
| n16-k136-distinct | compact-merge | 40.640 | 40.605 | 25.443 | 1.001x | 0.627x |
| n2-k136-distinct | compact-merge | 39.414 | 39.221 | 20.098 | 1.004x | 0.512x |
| n4-k136-distinct | compact-merge | 39.860 | 39.714 | 19.702 | 1.003x | 0.495x |
| n8-k136-distinct | compact-merge | 40.234 | 40.031 | 21.613 | 1.006x | 0.538x |
| n9-k136-distinct | compact-merge | 40.231 | 40.396 | 22.080 | 0.994x | 0.538x |

In the independent [24-trial merge controls](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40894507945), both jobs ran on EPYC 7763 hosts. The 9-input/8-byte case measured **0.9985x [0.9811, 1.0232]** original; its separately built identical-source control was 0.9945x [0.9511, 1.0166]. The five repeated original-baseline cases all included equality in their intervals. The 12.5% full-run flag was not reproduced. All 120 current/reference SST output pairs in each job (240 pairs total) also matched byte for byte.

The long-key RocksDB ranking changed across hosts even with fixed measurement paths. For the 4-input/136-byte distinct case, the full run measured 39.714 ms current versus 19.702 ms RocksDB; the independent repeat measured 16.995 ms versus 24.495 ms. The current/original ratios remained near one for this long-key case. Both runs wrote the same amounts: process `write_bytes` increased by **9,781,248 bytes** for this engine and **5,283,840 bytes** for RocksDB. Final SST sizes were 9,773,360 versus 5,266,643 bytes. RocksDB’s prefix-encoded table representation uses less space in this key shape even with compression disabled; its [11.8.1 block builder](https://github.com/facebook/rocksdb/blob/v11.8.1/table/block_based/block_builder.cc) stores shared-prefix lengths separately from unshared key suffixes.

A separate three-trial syscall diagnostic on the repeat host measured current-process median write wall time of 4.551 ms and sync wall time of 5.109 ms for that case, compared with 2.470 and 5.684 ms for RocksDB. These traces include open and verification/reopen, sum syscall observations across threads, and perturb timing; they are not exact fractions of normal compaction time. Greater write volume is a plausible contributor to storage-sensitive results, but this diagnostic on a different host does not prove the exact cause of the slower full-run host. Reducing that long-prefix file-size difference would require evaluating a different encoded representation and its read/CPU tradeoffs; the present optimization does not silently change the format or weaken sync.

Full-matrix losses are retained rather than replaced with the faster repeat. This is evidence of host/workload sensitivity, not a universal compaction win.

Full retained snapshots also cost more than narrow snapshots. In the full run the duplicated full-snapshot write probes are 0.974x and 0.952x RocksDB speed; the narrow one is 1.064x. A separate fixed-path six-trial diagnostic ([40885275605](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40885275605), EPYC 9V74) measured 0.910x [0.670, 0.972] for full snapshots and 1.078x [1.026, 1.180] for narrow ones. Instrumented builds attributed roughly 77–83% of measured phase time to WAL sync and 10–16% to applying mutations. Those instrumented fractions have clock overhead and must not be treated as exact percentages of the normal binary.

## Large owned values and cache policy

The separate [39-condition cache-policy report](cache-policy-results.md) keeps total capacity at 8 MiB and compares default LRU, a single LRU shard and HyperClock. With tuned RocksDB caches, mixed 128-byte/2–4 MiB reads that consume all returned bytes measured about 1.83–2.11x RocksDB speed. The much larger isolated Get/return/drop ratios measure ownership/copy behavior, not processing megabytes of application data.

A single validated value of at least 64 KiB can share an immutable block only when retained block/index allocation and ReadBlock metadata add at most 256 bytes. Arc/Bytes and allocator bookkeeping are additional. Multi-record blocks and excess capacity fall back to copying; caller-held slices can retain their full backing allocation. The owner retains no database or file handle.

## Changes retained and validation

Retained changes reduce unnecessary memtable searches and key/value reference copies; amortize page ownership during batches; cache established range boundaries; reuse WAL decode storage; and replay validated WAL frames in bounded, locality-sensitive chunks. Bloom metadata is smaller while persisted bits remain unchanged. Public reads retain modulo probes after direct API controls rejected a faster-looking arithmetic microbenchmark. Large-value sharing avoids copies under the explicit ownership bounds above. Experiments that regressed cached reads, scans or recovery were reverted.

The [pathname control](measurement-paths.md) isolated an apparent 15% 16 KiB range regression: the exact same executable changed speed under different DB pathnames. The fixed-path full run’s repeated 16 KiB hit-range ratio is 1.016x original [0.982, 1.032]. This supports the measurement correction; it does not establish an exact allocator or microarchitectural cause.

- [CI for the measured source](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40885275732) passed Linux/macOS debug and release/all-feature tests, Clippy, formatting, isolated Criterion filter checks and the demo.
- Rust 1.85 and the declared minimum bytes 1.10.1 passed compilation and owner-conversion tests. Tests cover bounds/prefix collisions, batch atomicity, corruption, crash recovery, replay ordering, retained large values, cache eviction and release of shared owners.
- [Extended stress](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40880357852), on the same production source, passed the 1M-distinct-key and 5M-overwrite tests: two passed, none failed, 998.20 seconds.
- Normal CI now checks Python runner syntax and fixture restoration. The full summarizer rejects wrong revisions, missing environment files, invalid durations and incomplete trial sets; it exports every public condition with its measurement-policy label.

These workloads fit host RAM and use warm OS cache on shared CI storage. They do not establish cold-device performance, prolonged production behavior or physical-power-loss safety. Different CPU/storage hosts must not have absolute times pooled or control ratios subtracted from one another.

## Reproduction and raw evidence

Use the [benchmark README](../benchmarks/rocksdb-compare/README.md) for full and targeted commands. The complete report uses the original full artifact, not a mixture of later focused runs.

| Run | Artifact | SHA-256 |
|---|---|---|
| Full 40885275681 | full-comparison / 12905307239 | `b7b57af3494224363f6d041435067bc653cf0c8687c855812ec2cee7951fd8ca` |
| Focused 40885275616 | recovery-comparison / 12903545888 | `81775ac4192e9763eb735d2273f282c1ade04cecff114398ec65c62b68ec7b3e` |
| Snapshot phases 40885275605 | write-diagnostics / 12902362510 | `08bdd04d2616e36465751cbf3e77739ec01f60e17308fadf157beb6e75d7fb5c` |
| Read controls 40893254091 | residual-read-original / 12907260569 | `23e493e66da9b266cf7d5aacaa86f6d7f3497bf9c19a7337c0fff9a80b18c79a` |
| Read controls 40893254091 | residual-read-identical-source / 12906382885 | `149c5bac57cbd993bfc8845bf99a2f11d987f75b3cbab41c3ae0eb50c1a87332` |
| Merge controls 40894507945 | merge-control-original / 12906323544 | `12afc74f3a7e3ee515096401de032af3f19782676f5d9aaca7bdc61934626f8b` |
| Merge controls 40894507945 | merge-control-identical-source / 12906866288 | `e1718727711d943b8e45e2d3c5c31b31a8b354fa2decff834b927b41dd7acd83` |
