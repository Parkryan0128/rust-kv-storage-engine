# Random-read comparison

This isolated package compares the current engine and RocksDB 11.8.1
(rust-rocksdb 0.25.0). Both run the same Rust random-Get harness as separate
release processes on one Linux host. RocksDB is only a benchmark dependency.

Only random point reads are measured. The previous write, update, delete,
compaction and sequential verification phases have been removed. Ordinary
correctness tests for those operations remain in the library's test suite.

## Fixtures and conditions

Fixtures are created once per case, before measured processes start:

- The ignored library test `read_fixture::write_read_benchmark_fixture` uses
  the production SST writer and manifest, without millions of WAL/fsync calls.
- RocksDB's `SstFileWriter` and external-file ingestion create the same logical
  key/value dataset with the same number of disjoint, equally divided files.
- Each engine uses its production SST format: version 04 for this library (versions 01–03 remain readable).
- Every backend runs at 4, 8 and 16 KiB block sizes, with an 8 MiB block-cache
  budget, ten Bloom bits per key and no compression. Metadata is outside the
  block cache. The library's default block size remains 16 KiB; the benchmark
  sets block size explicitly when building fixtures.
- The engine compaction trigger is 64; RocksDB opens read-only. File counts are
  asserted at open and after each pass, so the prepared layout remains fixed.
- OS file cache is explicitly warmed before each process. This does not preload
  the engine block cache. Datasets fit the benchmark host's RAM.

Cases: 100,000 keys / 128-byte values / one SST; 1,000,000 keys / 128-byte values /
five SSTs; and 100,000 keys / 1,024-byte values / five SSTs. Keys are eight-byte
big-endian integers; values encode the key and a known fill pattern.

Each case/block-size combination runs six trials. Block-size order rotates so each
occupies every position twice; backend order alternates, three times each. Seeds change between trials
and match between backends and block sizes. Each
fresh process makes two identical seeded passes of 500,000 random successful
Get calls, validating the entire returned value every time. No sequential Get
pass is performed. The first pass starts with a fresh block cache; the second
repeats the same query sequence.

## Running

Use the `rocksdb-comparison` workflow for the complete reproducible run. It
builds both backends, runs a small smoke case and then
the full comparison. Native RocksDB compilation requires Rust 1.88+ and
libclang; the library still supports Rust 1.85.

Fixture reports verify their compiled source root and
requested block size. Stage tests also check actual SST frame sizes and report
the compiled decoder source checksum; the runner verifies that against the
intended checkout before accepting measurements.

After producing `comparison-bin/engine` and
`comparison-bin/rocksdb` as shown in the workflow:

```sh
python3 benchmarks/rocksdb-compare/run.py comparison-results
```

Use `KV_COMPARE_SMOKE=1` for a 100-key, 1,000-query-per-pass smoke case at all
three block sizes. `KV_READ_BLOCK_BYTES` configures standalone executable runs;
it must match the fixture. `KV_READ_FIXTURE_BLOCK_BYTES` configures the engine
fixture writer and stage microbenchmark.
`KV_BENCH_DIR` selects the temporary fixture filesystem. Fixture commands and
each read process have timeouts. Progress is printed for each fixture file,
process and read pass. The workflow saves raw JSONL, logs and environment data.

## Interpretation

Throughput includes random key generation, correctness checks and value
destruction. Sampled latency times Get only, at most 10,000 samples per pass.
RSS and peak RSS describe the fresh read process, excluding fixture creation.
They are not comparable to the previous full-workload lifetime peaks. Cgroup
memory is not reported because fixtures/page-cache pages were allocated outside
the measured child process.

This fixed-layout, OS-cache-warm, single-client hit workload differs from the
previous organically generated write/compaction workload. The old baseline was
removed because it cannot read version 03 fixtures; do not directly subtract old
benchmark numbers. Results do not establish cold-device, concurrent, missing-key,
write, range-scan or durability performance. Earlier runs used RocksDB pinned Get; the engine
returns independently owned values.

Before measured trials, a separate ignored test samples 10,000 random uncached
blocks for each library fixture. It times positional file reads,
CRC/validation, and lookup/value copying. Version 03 fully validates the first
touch of each block and reuses validation fingerprints on later matching CRCs;
the fingerprint starts empty in every measured process. This is not a full-record
parse on every miss, and the persisted directory is included in file/cache bytes.
Buffers are reused, files are OS-cache warm, and routing, cache management,
locking and buffer allocation are excluded. A separate warmed-buffer CRC-only
measurement overlaps validation and must not be added to it. Timer overhead
is included. These diagnostic means are not an end-to-end CPU profile or
percentages of total Get time.

Use equal-size rows to compare engines. A smaller block
can reduce miss processing but increases the index and may affect writes and
scans, which this benchmark does not measure. Reports retain every block size,
RSS and SST bytes rather than reporting only the fastest configuration.

## Paired workload and audit suite

The separate write-comparison workflow builds the same workloads harness
against the selected baseline, current engine and RocksDB. Unlike the older
random-read runner above, this suite returns owned values on all backends.
Paired durable batch cases now use at least 1,024 commits and an explicit 64 MiB
memtable budget; flush pressure remains a separate case. These larger cases
must not be compared directly to historical 32-commit / 4 MiB measurements.
Snapshot-write32 runs 2,000 iterations; total throughput includes snapshot
creation, input generation and verification, while batch_seconds and
batch_latency isolate the batch call.

With [audit-perf] alongside a performance trigger, the workflow also measures
128-byte-prefix tombstone scans (memory and SST), repeated warm reopen, and
separate instrumented baseline/current write phases. Phase builds perturb
timing and are for attribution, not throughput rankings. The ignored
merge_policy_profile experiment compares borrowed and heap merging at
2/4/8/9/16 SST sources, short/long keys, disjoint/overlapping histories and
deletions. It checks both outputs before timing. This is a warm release-libtest
policy experiment, not an end-to-end RocksDB comparison; its 80 ms samples
should be treated as supporting evidence rather than a universal threshold.

The scan-diagnostics workflow accepts a [scan-diag] push marker with exactly
one [baseline:<40-character SHA>] marker. It runs the identical active/frozen
memory, cache-hit and snapshot-mutation diagnostics against both revisions,
without rebuilding RocksDB. Use this shorter check to investigate a scan
regression before repeating the full paired suite. Normal release-libtest runs
exclude the copied-key test counter; all-features tests enable its assertions.

Reopen optimization reuses the WAL/SST directory inventories and skips an
unchanged SST directory sync. Root and WAL directory syncs remain mandatory:
a visible manifest rename or empty WAL may have survived a process interruption
before its parent directory was synced. Reopen must publish those entries
before deleting old files or accepting new writes. This is covered by an
interrupted-publication regression fixture and the existing crash tests.

Copied benchmark fixtures are file-synced and directory-synced, then read to
warm the OS cache before starting a measured child process. This prevents
dirty copytree output from charging fixture publication to one backend during
a later timed compaction merely because its open path performs fewer syncs.
The preparation is identical for baseline, current and RocksDB and excluded
from the reported child-process throughput.

## First post-open range bounds

`python3 benchmarks/rocksdb-compare/bounds.py bounds-results` compares 4/16
nonoverlapping SSTs, 128 B/16 KiB values, and gap/above-all/hit ranges. Each
backend prepares the same logical writes in its native format, because reusing
version 03 engine files would omit the version 04 metadata under test. Six
rotated trials report open, the first scan and 2,000 repeats separately, with
warm OS cache and the data-block cache disabled. Full data verification and
reopen happen outside timing. This is a first-query I/O diagnostic, not a
cold-device latency or universal throughput claim.

For compaction across a deliberate storage-format change, pass
`--allow-format-change` to `compactions.py`. The default still requires identical
baseline/current SST bytes; the explicit option keeps complete logical/reopen
verification and output digests while allowing new metadata bytes.


## Complete comparison

To reduce noise in a short first-range measurement, repeat the same first-pass
trace with a freshly opened database handle each time:

```sh
python3 benchmarks/rocksdb-compare/workloads.py reopened-results --baseline --reads-only --profile 0 --scenario scan-t1-span32 --trials 12 --reopens 32
```

Each trace starts with fresh engine block-cache and validation state. The timed
result sums the scan traces; opening, closing and full result verification stay
outside that timer. The OS page cache is warm, so this does not measure physical
cold-device reads. Per-trace and separate open times are retained in the raw output.
`--reopens` requires a scan scenario and accepts 1 through 128 traces.

The opt-in `full-comparison` workflow runs every public workload profile, large
random reads, native-format first ranges, durable writes, recovery, snapshots,
cache pressure, memory scans, compaction matrices and internal diagnostics on one
host. Use workflow_dispatch with an exact baseline SHA or a main commit carrying
`[full-comparison] [baseline:<40-character SHA>]`. Production library code is
not modified by this workflow. Internal profiles have no RocksDB equivalent.

`run.py --baseline` includes all three revisions with independently generated
native SST fixtures. The large random-read harness now uses **owned Get values
on both backends**, so old pinned-Get rates are not directly comparable. It
still includes validation in throughput; the general workloads suite measures
verification separately. Merge matrices require an explicit
`--allow-format-change` to relax file-byte equality across format versions;
complete logical checks after compact and reopen remain mandatory.

### Reproducing optimization regressions

`performance-regression-loop` compares an exact baseline SHA with the current engine and RocksDB, retaining the snapshot/range guards and using 12 trials for every durable-write case and seven WAL recovery patterns. Its random-read run adds `--engine-control`: the identical current executable and immutable SST fixture are read in another process in the rotated order. Compare this A/A variation before attributing a small difference to a source change. Twelve trials balance the four backend positions and three block sizes. WAL sync and integrity checks are unchanged.

The recovery guards include single-operation WriteBatch frames, random key order, overwrites, and 256-operation frames. Small validated WAL frames share bounded replay chunks, with the adaptive limits described below. Sorting is conditional on disorder and expected page reuse; larger batches sample locality before choosing streaming or chunked replay. This affects unpublished recovery state only; live writes and WAL commit order are unchanged.

Memory pages keep a shared immutable key array inside the shared value base, so copy-on-write value deltas do not acquire an extra key reference on each page clone. Production overwrites return only the prior payload charge; they do not clone an old shared value solely for accounting. The internal volatile-write profiles guard both snapshot-retained and ordinary writes.

Recovery coverage distinguishes a single operation in `write_batch` from the
public `put` WAL frame. The regression loop also replays 100,000 shuffled keys
written with individual puts and 32-operation batches. Fixtures assert that no
SST was flushed before the simulated process exit, and overwrite values carry
a generation byte so recovery must return the latest operation, not any version.

Random-read reports optionally include Linux main-thread CPU time, runqueue wait
and timeslices from [`/proc/thread-self/schedstat`](https://docs.kernel.org/scheduler/sched-stats.html).
These samples surround the wall-time loop and include a slightly wider diagnostic
window. They do not replace elapsed throughput or include background-thread CPU.
The environment records whether kernel schedstats are enabled; missing counters
remain null and disabled runqueue accounting must not be interpreted as no wait.

Recovery comparisons explicitly set `recover_to_memtable=true` on all seven WAL
cases. RocksDB uses `avoid_flush_during_recovery=true`, and each backend asserts
zero SST files after the measured open. This compares the same endpoint: replayed
data available in memory with its durable WAL retained. Sync commits, checksums
and memtable budgets are unchanged. Historical runs without this config used
RocksDB's default flush-on-recovery policy, so those RocksDB ratios also include
SST creation and must not be presented as a pure WAL replay speed difference.
See [RocksDB WAL recovery options](https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log).

Memory routing skips searches whose bounds are already known: a directory with
one entry needs no search, and an equal-prefix suffix ends at the last entry.
Complete key-byte comparisons still decide ordering when prefixes collide.
The variable/common-prefix memory workloads and boundary/model tests guard
these shortcuts without adding an index, cache, or retained allocation.

The full comparison also runs 12 large-random trials with the identical-binary
control. Keep cache-resident, long-key and cache-pressure cases alongside large
random reads: a fixed-step indexed lookup experiment preserved correctness but
regressed cached Get throughput, so production retains early equality exit.
The boundary/model regression tests remain in place.

`cargo test --locked --release --lib bloom::reduction_profile::profile_bloom_reduction -- --ignored --exact --nocapture --test-threads=1` compares reciprocal and original modulo probes in one process. A `[bloom-diag]` push marker runs it through `scan-diagnostics`. Six alternating trials use the same prehashed corpus, filter bytes and struct layout; this isolates reduction arithmetic, includes equal function-call overhead, and does not replace end-to-end Get measurements or test the old struct layout.

`[recovery-diag] [baseline:<40-hex commit>]` runs 12 alternating baseline/current release-test trials of `wal::profile::profile_recovery_stages`. Both builds receive the identical diagnostic. It separates warm-file recovery, in-memory frame checks, batch replay, owned decoding and predecoded memtable application for sorted/shuffled WALs, including 257-, 1,024- and 4,096-operation batches. These overlapping diagnostic paths have different allocation lifetimes; do not sum their times or call them production cost percentages. Helper replay excludes coalescing; `recover-file` exercises the actual coalescing path. Results include exact revisions, binary hashes, toolchain and CPU.

WAL replay retains initialized frame bytes and only zeroes a growing tail; complete reads and both frame checksums remain mandatory. Small atomic batches replay through bounded chunks after full validation. For larger frames, an eight-record sample selects chunk sorting when it finds disorder; otherwise the general streaming path remains in use. This is a performance hint, not an assumption that the rest of the frame is sorted. Ordered and concentrated replay chunks retain the 256-record bound. At that boundary, disordered chunks grow to at most 1,024 records / 256 KiB of logical record charge only when an eight-key sample spans more than 128 existing pages; an oversized record is applied immediately, and the reusable container itself is additional temporary memory. Retained WAL I/O buffers and the 256-record large-frame streaming threshold are unchanged. The staging vector and payload are dropped before open returns. Compare end-to-end recovery and RSS as well as phase diagnostics before retaining the larger-window policy. The full workflow preserves a read/write/recovery checkpoint before its later compaction and internal suites.

Add `[recovery-only]` to a `[performance-regression-loop] [baseline:<40-hex commit>]` push to run just the seven public recovery cases with 12 alternating trials and RocksDB, after the normal build and smoke checks. This narrower mode uses a separate concurrency group and publishes `recovery-comparison`; it omits unrelated scans, writes and random reads. It validates actual release library behavior separately from release-libtest phase diagnostics.

Replay chunk sorting also considers the current memtable page count: sparse chunks with fewer than two records per existing page on average retain WAL order. This heuristic limits sorting overhead when a growing index makes page reuse unlikely. Sampling only chooses a performance path: later scatter missed by the sample still uses the general small-chunk path, without skipping validation or assuming key order. Both paths preserve sequence order for duplicate keys, tombstones and accounting; no public API or durability setting changes.

Combine `[recovery-hotspot]` with the recovery-only comparison to add `wal-large-hotspot`: 200,000 durable operations first create 100,000 live keys, then update a permuted 1,000-key hotspot for 100 complete generations. Recovery verifies generation 100 for every hot key and generation 0 for every other key. The case is opt-in and saved under `recovery-hotspot`, preserving the historical default matrices. It guards against sorting heuristics that confuse a large index with a widely scattered write set.

`[cached-read-guard]` adds 12-trial public API hot-read comparisons with 4 KiB and 16 KiB blocks, common-prefix disk keys and a cache-capacity boundary case to the regression workflow. It can accompany `[recovery-only]` to isolate cache and replay changes without rerunning the full matrix. Bloom filter bitmaps have fixed length; boxed slices and the format-bounded 32-bit bit count remove unused capacity metadata while preserving the exact encoded filter and reciprocal reduction. The arithmetic diagnostic reports actual metadata size; measure total Get throughput separately.

`[recovery-value-guard]` adds 1 KiB and 16 KiB value recovery cases (random batches and sorted 16 KiB batches), each with 12 trials and the same memory-only recovery endpoint. All fixtures stay below the shared 64 MiB memtable target. `[read-codegen]` preserves complete symbol tables, selected point-read assembly and exact binary hashes; symbol spelling can vary with the Rust mangling version. Neither option changes the standard matrix.

`[sync-diag]` builds an uncommitted Linux-only append-sync experiment beside the unchanged production source. It changes only WAL append `sync_all` to `sync_data` in the runner checkout, preserving the exact patch, source hashes and binary hashes. Twelve alternating trials cover four write shapes with full value verification and reopen; this is not a RocksDB comparison, an adopted production change, or a power-loss test. Production continues to use `sync_all`, and RocksDB comparisons keep `use_fsync=true`. Linux `fdatasync` still synchronizes metadata required for subsequent data retrieval; see [fsync(2)](https://man7.org/linux/man-pages/man2/fsync.2.html).

`[bloom-api-diag]` isolates Bloom arithmetic and metadata layout in actual public `Get` calls. It builds the historical `c0299ec` reference, unchanged production, a same-layout candidate using the opposite probe arithmetic (modulo or reciprocal), and a candidate restoring only the historical Bloom module. Candidates exist only in the runner checkout, are tested before measurement, and are restored afterward; exact patches, source files and binary hashes are preserved. Twelve balanced process orders use identical reference-created SST bytes and query traces for 4/16 KiB hot reads, misses and 200,000-key uniform reads. This diagnostic is separate from RocksDB and never substitutes arithmetic microbenchmarks for API measurements.

The default public read probe uses modulo after the four-binary API diagnostic found reciprocal probes slower for cached positive lookups despite faster isolated arithmetic. Insertions still use exact reciprocal reduction. The compact metadata and persisted Bloom bytes are unchanged; report miss-heavy reads too, since their shorter negative probes can favor reciprocal arithmetic.
