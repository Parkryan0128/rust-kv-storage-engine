# Performance optimization continuation — 2026-10-29

**This report supersedes the earlier same-day report for the current implementation.** The complete comparison retains all 263 public conditions, including losses and open times. See the [complete CSV](performance-results-2026-10-29-continuation.csv).

## Revisions and interpretation

- Full measured revision: `7729109c9e46d7c6a9f49c0e6c5d405240c5703d`; baseline: `02ec395d02c6d9552ece476730e66879b8a709ec` (the start of this continuation).
- The full and final focused/CPU measurements use the same Git tree `ca27a7c7350bdc718513ec9dd7e8ba6cfae5d9e8`; their trigger commit IDs may differ.
- Full run: [40959958221](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959958221).
- Host: AMD EPYC 9V45 96-Core Processor, Linux x86_64. All full-run participants share that host. RocksDB uses the locked dependency versions.
- The native-prefix budget guard isolates an earlier revision of that change. Later cursor and snapshot improvements are measured separately; ratios from these runs are never multiplied.
- Ratios are medians of trial-matched `other time / current time`; above one favors current. Absolute columns are separate medians, so their quotient need not equal the paired ratio.
- Identical private measurement path, warm OS cache, owned Get values, synchronous WAL acknowledgements, and disabled general value compression. Native key representations may differ. This is not a cold-device or sustained production ranking.
- Most full conditions use six trials; dedicated writes use three, batch32 and large random reads twelve. Random reads also include an identical-executable control. The CSV retains paired bootstrap 95% intervals; exploratory flags are not corrected for multiple comparisons.

## Changes retained

- Bitmap-indexed snapshot deltas and spare capacity on copy-on-write clones remove redundant searches and reallocations while preserving old snapshot values.
- Optional SST version 05 shares long block-local key prefixes. Cache entries retain encoded bytes and a scan reconstructs only its current key. Common Bloom hash prefixes are reused without changing stored hashes.
- Published tables reuse the writer validation fingerprint. Every disk read still verifies CRC; a changed CRC triggers full validation. Reopen starts without the in-memory proof.
- Bloom rebuilding visits encoded keys directly. Native-prefix compaction charges its encoded buffers and bounded current-key allocation, removing the obsolete expanded-block allowance.
- Snapshot creation finds the ending group once and collects an exact-sized directory; broad scans avoid repeated group-boundary comparisons and directory growth.
- Cursor key buffers retain immutable prefixes across rows and reuse bounded storage across blocks. Proven-empty scans skip engine retention and merge initialization.
- Ordinary-format range cursors specialize the decoder without prefix reconstruction; version 05 keeps the mixed-block path and all validation checks.
- Memory pages retain a conservative common-prefix proof for long keys; suffix comparisons preserve exact byte ordering and divergent queries retain complete-key search.
- Memory scans skip runs of deleted base records on pages wholly inside the range, while boundary pages and snapshot delta pages retain their checked traversal.
- Prefix cursor capacity, mixed plain/prefix tables, bounds, snapshots, recomputed-checksum corruption, and crash/I/O publication boundaries have regression tests.

**File compatibility:** ordinary tables still use version 04. Existing versions 01–04 remain readable; older readers cannot open newly written version 05 tables. See the [storage format](storage-format.md#optional-prefix-blocks-version-05).

## Large random reads

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| 1000000keys-128b-5sst-16384block / random_read_repeat | 1347.226 | 1354.167 | 1798.764 | 1.001x | 1.337x |
| 1000000keys-128b-5sst-4096block / random_read_repeat | 986.403 | 988.662 | 1279.339 | 0.987x | 1.317x |
| 1000000keys-128b-5sst-8192block / random_read_repeat | 1074.708 | 1059.564 | 1442.639 | 1.005x | 1.353x |
| 100000keys-1024b-5sst-16384block / random_read_repeat | 1417.444 | 1389.499 | 1679.159 | 1.028x | 1.231x |
| 100000keys-1024b-5sst-4096block / random_read_repeat | 1138.557 | 1148.066 | 1260.104 | 0.972x | 1.111x |
| 100000keys-1024b-5sst-8192block / random_read_repeat | 1103.170 | 1147.943 | 1408.729 | 0.971x | 1.203x |
| 100000keys-128b-1sst-16384block / random_read_repeat | 569.934 | 550.053 | 918.776 | 1.031x | 1.676x |
| 100000keys-128b-1sst-4096block / random_read_repeat | 431.414 | 445.127 | 781.079 | 0.993x | 1.791x |
| 100000keys-128b-1sst-8192block / random_read_repeat | 476.784 | 485.335 | 807.853 | 0.983x | 1.622x |

## Memory and SST sizes

Fresh read-process RSS below excludes fixture creation and OS page cache. SST bytes are stored database files, not source code.

| Condition | Current RSS MiB | RocksDB RSS MiB | Before SST MiB | Current SST MiB | RocksDB SST MiB |
|---|---:|---:|---:|---:|---:|
| 1000000keys-128b-5sst-16384block | 12.90 | 23.10 | 151.29 | 151.29 | 136.64 |
| 100000keys-1024b-5sst-16384block | 11.67 | 21.84 | 100.80 | 100.80 | 99.28 |
| 100000keys-128b-1sst-16384block | 11.06 | 21.70 | 15.13 | 15.13 | 13.66 |

## Synchronous writes

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| batch256-16b / write | 281.785 | 285.717 | 295.466 | 0.986x | 0.992x |
| batch32-128b / write | 246.298 | 249.309 | 246.235 | 1.000x | 1.003x |
| flush-pressure / write | 317.583 | 280.081 | 258.757 | 1.138x | 0.927x |
| put-128b / write | 1715.724 | 1727.746 | 1822.365 | 0.994x | 1.006x |

## WAL recovery

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| wal-batch256 / recover | 3.260 | 3.312 | 10.004 | 0.976x | 2.975x |
| wal-batch32-random100k / recover | 37.173 | 37.678 | 73.466 | 0.984x | 1.989x |
| wal-overwrite / recover | 1.476 | 1.495 | 10.283 | 1.001x | 6.890x |
| wal-put-random100k / recover | 40.741 | 40.553 | 93.567 | 0.997x | 2.293x |
| wal-put-sorted / recover | 2.089 | 2.125 | 8.605 | 0.982x | 4.141x |
| wal-random / recover | 3.547 | 3.535 | 11.119 | 1.000x | 3.143x |
| wal-single / recover | 2.169 | 2.126 | 8.618 | 1.008x | 3.995x |

## Memory lookups, ranges and retained snapshots

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| memory-early10 / memory-scan | 4.632 | 4.588 | 9.651 | 1.008x | 2.116x |
| memory-early10-100k / memory-scan | 5.116 | 4.876 | 8.744 | 1.046x | 1.788x |
| memory-early10-200k / memory-scan | 5.534 | 5.086 | 8.767 | 1.088x | 1.722x |
| memory-full20k / memory-scan | 10.803 | 10.871 | 17.682 | 0.973x | 1.615x |
| memory-get100k / memory-get | 59.071 | 68.571 | 121.941 | 0.944x | 1.978x |
| memory-get20k / memory-get | 25.383 | 23.772 | 86.096 | 1.040x | 3.653x |
| snapshot-write32 / snapshot-write | 575.090 | 535.503 | 537.619 | 1.071x | 0.996x |

## Key shapes and mixed traffic

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| keys-common128-disk / shaped-keys | 103.065 | 75.549 | 250.184 | 1.416x | 3.371x |
| keys-common128-memory / shaped-keys | 95.090 | 89.735 | 122.133 | 1.100x | 1.421x |
| keys-variable-disk / shaped-keys | 56.593 | 57.823 | 208.794 | 0.980x | 3.627x |
| keys-variable-memory / shaped-keys | 23.027 | 24.677 | 93.540 | 1.036x | 3.863x |
| mixed-steady32 / mixed-steady | 646.856 | 680.652 | 672.783 | 0.973x | 1.000x |
| snapshot-narrow-write32 / snapshot-write | 525.805 | 514.995 | 548.101 | 0.999x | 1.054x |

Long-key SST sizes below use each backend's native representation after the same logical setup. Prefix sharing applies only when keys actually share enough bytes; it does not compress arbitrary values.

| Key shape | Before SST MiB | Current SST MiB | RocksDB SST MiB | Current reduction vs before |
|---|---:|---:|---:|---:|
| keys-common128-disk | 5.811 | 3.290 | 3.242 | 43.4% |
| keys-variable-disk | 3.316 | 3.316 | 3.020 | 0.0% |

## Compaction from shared baseline fixtures

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| n16-k136-distinct / compact-merge | 40.889 | 30.278 | 25.864 | 1.353x | 0.854x |
| n16-k8-distinct / compact-merge | 14.728 | 19.343 | 23.560 | 0.887x | 1.223x |
| n2-k136-distinct / compact-merge | 39.668 | 25.908 | 21.067 | 1.530x | 0.861x |
| n4-k136-distinct / compact-merge | 40.110 | 28.723 | 21.119 | 1.395x | 0.827x |
| n4-k8-distinct / compact-merge | 19.554 | 19.563 | 21.330 | 1.004x | 1.094x |
| n8-k136-distinct / compact-merge | 40.454 | 29.657 | 24.878 | 1.357x | 0.835x |
| n9-k136-distinct / compact-merge | 45.643 | 30.006 | 25.950 | 1.509x | 0.872x |

## Native long-key range queries

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| prefix-range-16384-10 / shaped-ranges | 14.662 | 13.386 | 26.893 | 1.093x | 2.000x |
| prefix-range-16384-20000 / shaped-ranges | 12.882 | 12.989 | 25.162 | 0.993x | 1.947x |
| prefix-range-16384-512 / shaped-ranges | 17.353 | 17.740 | 35.121 | 0.984x | 1.994x |
| prefix-range-4096-10 / shaped-ranges | 14.668 | 14.247 | 26.031 | 1.018x | 1.814x |
| prefix-range-4096-20000 / shaped-ranges | 21.332 | 21.264 | 33.886 | 1.003x | 1.575x |
| prefix-range-4096-512 / shaped-ranges | 27.888 | 28.402 | 46.223 | 0.979x | 1.597x |

These six additional conditions run on the full comparison host, with 12 paired trials each. They use 20,000 keys sharing a 128-byte prefix, varying suffix lengths, 128-byte values, disabled point-read cache, and 4 or 16 KiB logical blocks. Every sampled range is checked after the timed loop. They are separate from the historical 263-condition CSV.

| Condition | Paired speed vs before, 95% interval | Paired speed vs RocksDB, 95% interval |
|---|---:|---:|
| prefix-range-16384-10 | 1.093x [1.081, 1.108] | 2.000x [1.980, 2.027] |
| prefix-range-16384-20000 | 0.993x [0.982, 1.004] | 1.947x [1.865, 2.007] |
| prefix-range-16384-512 | 0.984x [0.968, 1.009] | 1.994x [1.949, 2.041] |
| prefix-range-4096-10 | 1.018x [0.998, 1.033] | 1.814x [1.772, 1.853] |
| prefix-range-4096-20000 | 1.003x [0.988, 1.017] | 1.575x [1.558, 1.607] |
| prefix-range-4096-512 | 0.979x [0.947, 1.002] | 1.597x [1.565, 1.656] |

The shared baseline fixtures in the full matrix are version 04. The native-prefix guard below separately tests compaction of version 05 inputs.

## Independent native-prefix budget confirmation

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| n16-k136-distinct-16k / compact-merge | 27.040 | 14.536 | 25.618 | 1.835x | 1.762x |
| n16-k8-distinct-16k / compact-merge | 12.923 | 12.708 | 22.678 | 1.020x | 1.784x |
| n8-k136-distinct-16k / compact-merge | 13.644 | 13.658 | 23.569 | 0.996x | 1.719x |
| n8-k8-distinct-16k / compact-merge | 11.445 | 11.188 | 20.663 | 1.037x | 1.861x |

This guard uses 24 paired trials, current `ebe1c44b85ebfc2ace5654665777a9cb55b2e818` against `be1e773e9a61a7b64d2135944ee49a824bd3c3d1`, on its own host. Do not multiply its ratios by full-run ratios. All output SST contents must match byte for byte; only filenames may differ when the number of merge rounds changes.

| Condition | Before/current merges | Before/current maximum inputs | Before/current process write_bytes MiB |
|---|---:|---:|---:|
| n16-k136-distinct-16k | 2.00 / 1.00 | 15.00 / 16.00 | 9.53 / 4.92 |
| n16-k8-distinct-16k | 1.00 / 1.00 | 16.00 / 16.00 | 4.96 / 4.96 |
| n8-k136-distinct-16k | 1.00 / 1.00 | 8.00 / 8.00 | 4.92 / 4.92 |
| n8-k8-distinct-16k | 1.00 / 1.00 | 8.00 / 8.00 | 4.96 / 4.96 |

## Independent large-memory confirmation

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| memory-early10-1m / memory-scan | 12.994 | 8.024 | 18.379 | 1.605x | 2.291x |
| memory-get1m-throughput / memory-get | 1141.901 | 1127.633 | 3618.693 | 1.015x | 3.187x |
| snapshot-write32-100k / snapshot-write | 834.161 | 790.686 | 815.694 | 1.076x | 1.018x |
| snapshot-write32-1m / snapshot-write | 865.691 | 812.949 | 894.438 | 1.010x | 1.109x |

These four conditions use 12 paired trials on the separate focused-run host ([40959454124](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959454124)). The focused revision `6a63f0bb3123dc8ca58134d2127212d329a79943` has the same source tree as the full run and uses the same original baseline. Do not pool its absolute times or multiply its ratios with those from another host.

## Isolated snapshot write CPU

CPU artifact/run: [40959454124](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959454124), EPYC 7763, 108 records. This auxiliary profile has its own recorded environment; do not pool its timing with another host.

Release library, volatile engine, identical example on both revisions; no WAL and no equivalent RocksDB measurement. Batch time isolates writes while the snapshot is retained. Total time includes snapshot creation/drop and diagnostic verification. Six paired trials per size; setup is outside timing and split within the public batch-size limit.

| Live keys | Before/current batch mean µs | Paired batch speed, 95% interval | Paired total speed |
|---|---:|---:|---:|
| 20000 | 22.408 / 17.965 | 1.225x [1.213, 1.284] | 1.167x |
| 100000 | 37.049 / 31.447 | 1.172x [1.125, 1.216] | 1.135x |
| 1000000 | 78.215 / 67.834 | 1.150x [1.094, 1.235] | 1.184x |

Other snapshot CPU controls are retained below; total time also includes snapshot work and verification.

| Requested keys / actual keys | Case / retained snapshot | Batch speed, 95% interval | Total speed, 95% interval |
|---|---|---:|---:|
| 20000 / 20000 | distributed / False | 1.020x [1.004, 1.137] | 1.023x [1.006, 1.136] |
| 20000 / 256 | repeated-fold / True | 1.052x [1.028, 1.064] | 1.121x [1.097, 1.127] |
| 100000 / 100000 | distributed / False | 0.961x [0.893, 1.008] | 0.967x [0.902, 1.005] |
| 100000 / 256 | repeated-fold / True | 1.036x [1.016, 1.088] | 1.093x [1.072, 1.128] |
| 1000000 / 1000000 | distributed / False | 1.050x [1.017, 1.159] | 1.041x [1.016, 1.139] |
| 1000000 / 256 | repeated-fold / True | 1.060x [1.043, 1.071] | 1.052x [1.037, 1.065] |

## Long-prefix memory lookup experiments

Each row below is an isolated candidate comparison against its own stated baseline. These ratios are not multiplied together or pooled across hosts. Twelve paired trials per read case; write, recovery and ordinary disk guards were also checked.

| Experiment / condition | Baseline → candidate | Speed vs baseline, 95% interval | Speed vs RocksDB |
|---|---|---:|---:|
| Retained page-prefix proof / keys-common128-memory | `829a3c8cc2cc` → `49aa4be98fa2` | 1.187x [1.117, 1.250] | 1.345x |
| Retained page-prefix proof / keys-variable-memory | `829a3c8cc2cc` → `49aa4be98fa2` | 1.009x [0.966, 1.049] | 3.497x |

Experiment Retained page-prefix proof: [run 40952583292](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40952583292); no public flags at the stated 3%/interval threshold.

| Rejected eager upper routing / keys-common128-memory | `49aa4be98fa2` → `edd01bb85b65` | 1.154x [1.135, 1.181] | 1.388x |
| Rejected eager upper routing / keys-variable-memory | `49aa4be98fa2` → `edd01bb85b65` | 1.004x [0.965, 1.012] | 3.084x |

Experiment Rejected eager upper routing: [run 40955044724](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40955044724); Public flags: memory-get20k-throughput/memory-get 0.952x

| Rejected collision-only upper routing / keys-common128-memory | `49aa4be98fa2` → `c2947c45b14a` | 1.015x [1.005, 1.030] | 1.313x |
| Rejected collision-only upper routing / keys-variable-memory | `49aa4be98fa2` → `c2947c45b14a` | 1.007x [0.978, 1.012] | 3.232x |

Experiment Rejected collision-only upper routing: [run 40957468087](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40957468087); Public flags: memory-get20k-throughput/memory-get 0.958x


## Unique-key insertion CPU and allocation guard

[Run 40959466720](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959466720), candidate `8c80c2fb2dbfb59ba2b46d142a802c251f422213`, baseline `02ec395d02c6d9552ece476730e66879b8a709ec`. This diagnostic uses the same Git tree as the full comparison. 100,000 new keys; retained-snapshot cases begin with 100,000 distinct existing keys. Key generation, setup, every-key checks and full ordered/snapshot model validation are outside timing. Six alternating paired trials use an identical executable pathname. **Volatile Engine::new: no WAL and no RocksDB comparison.** Allocated bytes estimate the current index only, excluding allocator headers and detached old snapshot versions; this is not process RSS.

The first run uses EPYC 9V45. Independent [run 40961841855](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40961841855) uses EPYC 7763 and the exact same Git tree. Both results are retained; they are not pooled.

| Key shape | Insertion order | Batch | Retained snapshot | First run speed, 95% interval | Independent repeat speed, 95% interval | Current allocation KiB / delta KiB |
|---|---|---:|---|---:|---:|---:|
| common128 | sorted | 1 | False | 0.968x [0.958, 1.019] | 0.993x [0.980, 1.012] | 35155.20 / +15.27 |
| common128 | sorted | 1 | True | 0.938x [0.919, 0.964] | 1.017x [1.006, 1.029] | 74354.64 / +45.78 |
| common128 | sorted | 32 | False | 0.965x [0.944, 0.981] | 0.971x [0.958, 0.974] | 35155.20 / +15.27 |
| common128 | sorted | 32 | True | 0.960x [0.939, 0.984] | 1.047x [1.043, 1.056] | 74354.64 / +45.78 |
| common128 | random | 1 | False | 1.076x [0.886, 1.229] | 1.093x [1.068, 1.138] | 37941.27 / +20.78 |
| common128 | random | 1 | True | 1.102x [1.034, 1.150] | 1.068x [0.999, 1.095] | 72592.03 / +39.14 |
| common128 | random | 32 | False | 1.137x [1.047, 1.193] | 1.091x [1.056, 1.138] | 37941.27 / +20.78 |
| common128 | random | 32 | True | 1.053x [0.946, 1.163] | 1.058x [1.040, 1.109] | 72592.03 / +39.14 |
| short | sorted | 1 | False | 1.011x [0.987, 1.045] | 1.004x [0.986, 1.015] | 21190.41 / +15.27 |
| short | sorted | 1 | True | 0.986x [0.940, 1.040] | 1.008x [0.992, 1.038] | 46425.06 / +45.78 |
| short | sorted | 32 | False | 0.963x [0.923, 1.015] | 0.984x [0.979, 0.998] | 21190.41 / +15.27 |
| short | sorted | 32 | True | 0.991x [0.959, 1.003] | 1.000x [0.995, 1.014] | 46425.06 / +45.78 |
| short | random | 1 | False | 0.988x [0.909, 1.001] | 1.056x [1.003, 1.092] | 23976.48 / +20.78 |
| short | random | 1 | True | 0.887x [0.763, 1.032] | 1.061x [1.003, 1.123] | 44662.45 / +39.14 |
| short | random | 32 | False | 0.980x [0.898, 1.016] | 1.007x [0.984, 1.044] | 23976.48 / +20.78 |
| short | random | 32 | True | 0.877x [0.825, 0.942] | 1.009x [0.965, 1.110] | 44662.45 / +39.14 |
| variable | sorted | 1 | False | 0.998x [0.969, 1.032] | 1.005x [0.994, 1.016] | 22655.20 / +15.27 |
| variable | sorted | 1 | True | 1.007x [0.993, 1.057] | 1.042x [1.002, 1.059] | 49354.64 / +45.78 |
| variable | sorted | 32 | False | 0.962x [0.954, 0.976] | 0.991x [0.979, 1.003] | 22655.20 / +15.27 |
| variable | sorted | 32 | True | 0.985x [0.966, 0.993] | 0.990x [0.984, 0.992] | 49354.64 / +45.78 |
| variable | random | 1 | False | 0.967x [0.899, 1.003] | 1.029x [0.951, 1.080] | 25441.27 / +20.78 |
| variable | random | 1 | True | 0.975x [0.786, 1.065] | 1.020x [0.967, 1.081] | 47592.03 / +39.14 |
| variable | random | 32 | False | 0.972x [0.919, 1.006] | 1.008x [0.978, 1.042] | 25441.27 / +20.78 |
| variable | random | 32 | True | 0.952x [0.871, 1.017] | 1.011x [0.984, 1.043] | 47592.03 / +39.14 |


## Rejected experiments and remaining tradeoffs

- Expanding a whole prefix block on read hurt uncached long-key reads; the retained implementation keeps it encoded.
- Reducing a snapshot group from 64 to 32 pages increased the 1M-key early-10 scan latency by about 39%, without a clear synchronous-write win. It was reverted. The 58 completed public conditions from run 40914373339 are valid; a subsequent auxiliary CPU profile failed because its setup batch exceeded 64 MiB. That setup is fixed in this report's separate 108-record CPU profile.
- Skipping a proved table-wide key prefix in merge comparisons had inconsistent small gains and an overlap regression. It was removed; output format prefix sharing remains.
- Outlining prefix reconstruction into a separate per-record call gave mixed small ordinary-range gains and regressed four native-prefix range cases by 1–2.3%; that experiment was reverted.
- Specializing borrowed merge dispatch did not establish a target range-query gain. Replacing its two key comparisons with a single comparison also did not improve the target churn workload reliably. Both were removed.
- Specializing ordinary compaction decoding showed small initial gains that did not reproduce consistently in the target short-key cases on another host. The simpler prior cursor remains.
- Disabling synchronous WAL durability, weakening validation, borrowing caller-visible Get values, or adding general value compression would change guarantees or introduce new costs. Those were not used to obtain these numbers.
- Snapshot acquisition remains proportional to overlapping groups. Retained versions require copy-on-write work; owned small values require copying; sync latency and cache misses depend on the host/storage. The measurements do not establish a universal optimum.

## Exploratory regression flags

The following table lists every full-run condition with a paired median below 0.97 and a bootstrap upper bound below one. These flags require interpretation and independent checks, not selective removal from the CSV.

| Suite / condition / scenario / stage | Speed vs before | Paired 95% interval |
|---|---:|---:|
| bounds / 16files-16384b-hit / - / bounds-open | 0.956x | 0.866–0.982 |
| paired / p0-128b-4096block-8388608cache-clean / scan-t1-span512 / first | 0.942x | 0.899–0.959 |
| paired / p1-128b-16384block-8388608cache-clean / scan-t1-span512 / first | 0.937x | 0.906–0.958 |
| paired / p1-128b-16384block-8388608cache-clean / scan-t1-span512 / repeat | 0.952x | 0.941–0.971 |
| paired / p3-128b-16384block-0cache-clean / scan-t1-span32 / repeat | 0.962x | 0.937–0.971 |
| paired / p3-128b-16384block-0cache-clean / scan-t1-span512 / first | 0.955x | 0.927–0.981 |
| paired / p3-128b-16384block-0cache-clean / scan-t1-span512 / repeat | 0.943x | 0.934–0.963 |
| remaining / phase-40000-2m / cache-phase / first | 0.948x | 0.794–0.977 |

## Independent deletion-heavy scan controls

An intermediate full run flagged a roughly 10% memory-scan loss. Two independent follow-ups did not reproduce it on the retained implementation. The stronger follow-up below uses 24 trials, one executable pathname for every participant, and separate original-baseline, same-source-build and byte-identical-executable jobs ([40934639674](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40934639674)). Each job has its own host; the two EPYC 7763 jobs are separate machines. Do not subtract control ratios from another job.

| Condition | Against original | Same source, separate builds | Byte-identical executable |
|---|---:|---:|---:|
| tombstones-memory-2048 | 1.034x [1.023, 1.048] | 1.029x [1.020, 1.038] | 0.995x [0.985, 1.010] |
| tombstones-memory-512 | 1.029x [1.018, 1.037] | 1.033x [1.020, 1.040] | 0.993x [0.973, 1.010] |
| tombstones-sst-2048 | 1.202x [1.196, 1.206] | 1.006x [1.002, 1.012] | 1.000x [0.996, 1.004] |
| tombstones-sst-512 | 1.206x [1.197, 1.209] | 1.005x [1.000, 1.010] | 0.998x [0.995, 1.001] |

All four byte-identical-executable intervals contain one. Separate builds of identical source still differ by about 3% in the memory case. Paired bootstrap intervals describe trial variation, not systematic build or code-layout effects. Small changes near that scale therefore are not treated as established optimization gains. The controls do not prove all architectures behave identically.

## Final deletion traversal confirmation

| Condition / stage | Before ms | Current ms | RocksDB ms | Speed vs before | Speed vs RocksDB |
|---|---:|---:|---:|---:|---:|
| tombstones-memory-2048 / tombstone-scan | 114.447 | 86.109 | 1954.545 | 1.321x | 22.720x |
| tombstones-memory-512 / tombstone-scan | 28.828 | 21.558 | 489.350 | 1.339x | 22.745x |
| tombstones-sst-2048 / tombstone-scan | 695.323 | 583.018 | 1558.525 | 1.194x | 2.674x |
| tombstones-sst-512 / tombstone-scan | 174.020 | 145.494 | 388.523 | 1.197x | 2.668x |

This independent 24-trial guard uses `6a63f0bb3123dc8ca58134d2127212d329a79943`, the same final source tree, in [run 40959454124](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959454124). Every backend uses the same executable pathname and DB pathname. The earlier isolated deletion-traversal run showed a large unrelated Get increase; these measurements report each workload separately. No Get speedup is attributed to the deletion traversal change.

| Condition | Speed vs original, 95% interval |
|---|---:|
| tombstones-memory-2048 | 1.321x [1.318, 1.341] |
| tombstones-memory-512 | 1.339x [1.331, 1.352] |
| tombstones-sst-2048 | 1.194x [1.187, 1.199] |
| tombstones-sst-512 | 1.197x [1.191, 1.200] |

## Independent final guard flags

These are every flagged condition in the final-source focused and memory guards, using the same below-0.97 median and below-one upper-interval rule. When both views come from one run, shared conditions are listed once. The focused run is separate from the full comparison and its timings are not pooled.

| Guard / condition / stage | Speed vs original | Paired 95% interval |
|---|---:|---:|
| None at this threshold | — | — |

## Additional expanded regression guard

[Run 40959439152](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959439152) is a separate 107-condition guard, not the 263-condition full matrix. It uses the same source tree and original baseline. The flags below are retained independently.

| Condition / scenario / stage | Speed vs original | Paired 95% interval |
|---|---:|---:|
| bounds / 16files-128b-hit / - / bounds-first | 0.949x | 0.871–0.985 |
| bounds / 16files-16384b-gap / - / bounds-open | 0.964x | 0.916–0.995 |
| bounds / 16files-16384b-hit / - / bounds-open | 0.956x | 0.923–0.979 |
| memory / memory-full20k / memory-scan / memory-scan | 0.954x | 0.922–0.987 |
| ranges / p2-1024b-16384block-8388608cache-clean / scan-t1-span32 / first | 0.967x | 0.939–0.983 |

## Validation and reproducibility

The [current engine validation run](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959439167) covers locked debug/release tests, fault injection, model checks, Rust 1.85, minimum bytes dependency, formatter/clippy, and CLI demo. Extended stress evidence: [40959439167](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40959439167), same Git tree; one million distinct durable keys and five million fsync overwrites both passed (1125.14 s). The full summary validates exact trial counts, all three backends, common CPU identity, fixed measurement paths, commit IDs, recovery endpoints, and random-read control binary identity.

Run `summarize_full.py <artifact-directory> <head> <baseline> --fixed-paths` and `export_results.py <summary.json> <output.csv>` from `benchmarks/rocksdb-compare` to reproduce the complete CSV. Do not combine measurements from different machines into a single speed ratio.


## Artifact provenance

| Run | ZIP SHA-256 |
|---|---|
| 40943086831 | `66f3e503cb0c4f26a2c5b09122b3b93117a00efbc13b9bb175aed60af234015c` |
| 40938907319 | `907a8de35f4819b61b63964d629f3e5a23175119449cb528db9c77ccce96a068` |
| 40940872632 | `069e2c9a6388eb80ccc162bc57bcdbdbbad3ac30d8d304025e4e957f3ced7c34` |
| 40943293645 | `2b1a9ec0cfe918cabb6f523d0b49769606fac9ea28cf5a90487d064097e67443` |
| 40943510432 | `2e8b5b152852a4ea667ce388672540cf0aa5a7cbecfea6aa96d5ffa50b6c80d8` |
| 40943834698 | `884dec959a2b70825482e57df7f1f6cde2d1b1bc47c34661d40b4c4b1ae9cf05` |
| 40945105868 | `58a8795394b08e0a6c3aac44f16cc0073a1ba7c2dcb807ca005003a6592c5a9d` |
| 40919298169 | `358eccc98132e38176e4ee197fcf6ed9b663afafff42ce6d139ff346c1b5737c` |
| 40934639674 | `b3ca33ee72d5927886abc27415cf069ebebcf521e4b018627fa043b93a59bb64` |
| 40934639674 | `f192c4aa93657d72514f86eb43066ad1be04827462129a45aa787fdbc2c2027a` |
| 40934639674 | `dfa60bb3826ec050631820d5b4f0e00083add0c282ae9d49aa8d2e66b4c1fddd` |
| 40959439152 | `ea2f69601f61bdd0159c7c5e7c4218a6fa03c1f5903231e3176b3647d11e2346` |
| 40959454124 | `aa5032243ceb3724a6c822ec10cc999c16782228d5899285e365b1f66ff61710` |
| 40959958221 | `b0d85250c07b2e7d5b8819410ee70e04470485fc845dfd0c538a0b4391b4a2f1` |
| 40963254915 | `1cb01d2e777a82e502c54102a1f85bf905b685b142163511bd0cd8e28e198578` |
| 40964214507 | `20901142f696f90319ede211b8e56733906ebd6490744e04dfa9425465dff659` |
| 40952583292 | `9732e6a3e8b1705030d91eef20ffd155325f200649b3c14e6f74f24a4a41cf80` |
| 40953435094 | `8c6208adeb5047a3ef1bc0751053c1b31ed00e9671b60a7c41940712668f45c3` |
| 40955044724 | `7a3fbf05cd37b67c7a79c20d8d7d1cad6efbd0847e40d5895013ddae57d620a3` |
| 40955665439 | `7c35b9264ed50794a9b94d2a41f679708586add39bd3d6df7746ca8e92f33df1` |
| 40955168605 | `0001d87d478e56ac257da9730f51cfe77e495ee584fd7f523b2452d099bc2d8c` |
| 40957545883 | `e7a9d91d6ef980bf889163e1b74d0e46fcbdea9c6185bfe99d7929973287614b` |
| 40957468087 | `fc54912b38f0d93d9c962e5bffa88019870a2faf32144e5abe6f873300693c49` |
| 40955077483 | `3a52dc6516d82f9afe2ec02147fe05563515cc42b05f6f53beb375cb12cf2906` |
| 40959466720 | `1a643565621e0554dca6a5cc951d98ba555bc7653db21ce5d197a8b5241d21a2` |
| 40955380247 | `b48615205077165b18bb3f58082cd7f60c92f699291d54cacfc8c82e2140735c` |
| 40957932745 | `c838eaabd7bc62946b9faccaafb30760b03d412a3fb0369e2def9f8011d30018` |
| 40959393623 | `2bdec9d1b1477cc4c280115f4ae2650332b918af37eef6f586db8b37d4ff12f1` |
| 40961356244 | `6a415412376c16d3d49dcf8b4ec5bd805f27117caa2ad892b0e2351d4daac0c4` |
| 40961841855 | `3bad79c47902aeee1f7d13b0e5582fa9bb951a6d8bd44d4d5126f5d3bcb3cdde` |
| 40965680775 | `100b1eaeaafa594a6d45ba4093bb3504a61c033d1c7d26e0dd303408fa2dcf21` |
| 40966631445 | `2bcf9213a8bdd61c621a680978cdaa64e3eaebf24d546079fa579855d3078bea` |

The retained page-prefix proof and zero-proof maintenance remain in the final page-only implementation. Upper-prefix routing, decoder rollback, cached cursor format flags, and deferred prefix-proof batching were rejected after their targeted comparisons.

The [single-SST allocation guard](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40965680775) checked 77 public conditions: ordinary repeat scans were 0.994–1.003x, and a four-table gap first query flagged 0.936x [0.879, 0.992]. The small transient capacity saving did not justify more constructor branches without a clear measured benefit. The [output-reservation guard](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40966631445) checked 51 public conditions and byte-identical compaction outputs. Native 16 KiB long-key n8/n16 merges were 1.002x [0.989, 1.023] and 1.001x [0.997, 1.017]; the isolated n9 gain did not establish a consistent improvement. That candidate was removed too.

All identified candidates from this review have a recorded decision. The published engine retains the exact tree measured by the full, focused, insertion and stress runs. This closes the investigated work; it is not a proof that no future design or hardware-specific optimization is possible.

The full run on EPYC 9V45 flags eight conditions, chiefly ordinary SST ranges with paired medians 0.938–0.962. The independent final 49-condition focused guard on EPYC 7763 has no flags at the stated threshold. The expanded 107-condition guard has five flags, including two large-value bounds-open cases. These differing results do not prove a universal regression-free implementation. No control ratio is subtracted and no loss is removed. New-key CPU insertion also differs by host: a 0.877x retained-snapshot short-key batch result on EPYC 9V45 repeats at 1.009x on EPYC 7763. Common-prefix sorted batch32 without snapshots is about 3% slower on both. The direct deferred-proof candidate did not recover that cost and was rejected. Synchronous flush-pressure writes remain slower than RocksDB in the full comparison.
