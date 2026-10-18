# Bounded workload comparison

The `workload-comparison` workflow is opt-in: dispatch it manually or include
`[perf-check]` in a main-branch commit message. Ordinary pushes skip the job.
The measurement step has a ten-minute deadline; native compilation is separate.
This complements the larger, unchanged random-read suite in `run.py`.

Build `--bin workloads` from this package once without features and once with
`--features rocks`, saving executables as `comparison-bin/workloads-engine` and
`comparison-bin/workloads-rocksdb`. Run:

```sh
python3 benchmarks/rocksdb-compare/workloads.py workload-smoke --smoke
python3 benchmarks/rocksdb-compare/workloads.py workload-results
```

## Cases

- Uniform successful point reads; interleaved absent keys, not just keys beyond
  the largest stored key; 80% of hot-set reads target 1% of the keys.
- One reader and four concurrent readers, sharing one engine.
- Range scans over 32 and 512 key positions, including iterator creation and
  consumption. First-result latency is recorded separately.
- 128-byte and 1,024-byte values with 20,000 keys. The latter exceeds the 8 MiB
  cache budget. Block sizes 4/16 KiB and disabled-cache controls are included.
- Clean data and overlapping SSTs with overwritten values and 25% deleted keys.
- Synchronous individual puts and atomic batches of 32/256 operations; 8,192 keys
  per run. WAL commit time and final flush time are reported separately.
- Full compaction of copied churn fixtures, with before/after file allocation.

Four trials alternate backend-first order; scenarios rotate and seeds match.
Results include paired speed-ratio median/min/max, not a confidence interval.
Fixtures, copying, file-cache warming, statistics and reporting are outside operation
timing. Each measured read process opens a fresh copy of the same pristine fixture,
preventing RocksDB diagnostic-log accumulation or WAL rotation from changing
later trials' file sizes. It starts with a fresh DB
block cache, then repeats exactly the same trace. OS cache is warmed for both;
the first pass is **not** a cold-device measurement.

## Equal semantics, explicit differences

Both use enabled WAL and sync per put/batch, no compression, 4 MiB memtable targets,
the configured block/cache sizes and ten Bloom bits per key. Both return owned
values and range rows. The older random-read harness uses RocksDB's pinned Get;
its numbers cannot be directly subtracted from these owned-Get results.

Read fixtures use the same public-API write history and disable automatic
compaction. Different formats/rotation policies can produce different physical
file counts, which are reported rather than assumed equal. Write workloads use
each backend's normal compaction policy; they measure the complete implementation,
not identical algorithms. They are too short to establish sustained write rates.

Scans do not fill either point-read cache. RocksDB may still read already-cached
blocks; this engine bypasses the cache entirely. Both check data integrity.
RocksDB iterators provide their normal consistent view; engine scans share overlapping
immutable memory page groups and pin selected SST handles. Read fixtures are flushed before timing.

Each point pass issues 100,000 reads in total; range passes issue 512 short or
256 long scans. Point latency samples time only the API call. General read workloads use separate
throughput, warm latency and seeded full-validation passes. Throughput includes RNG,
allocations and value destruction, but excludes per-query timers and validation.
Workers are ready before the throughput timer, and finish before joining. Scan
latency includes traversal and row destruction, with validation in a separate pass. Do not
compare scan-query ops/s with point ops/s; scan row counts are reported separately.
Writes include native batch construction from the same prebuilt bounded trace.
Percentiles include sample counts; p95 is omitted below 20 samples and p99 below
100. These are sampled empirical percentiles, not a tail-latency guarantee.

RSS/peak RSS include the process/runtime, sample arrays and validation, but read
peaks exclude fixture creation. Write peaks include input traces. RSS excludes OS
page cache. File size, allocated regular-file blocks, process I/O counters and
backend statistics remain separate; none is a complete physical-device metric.

Raw JSONL, command logs, binary hashes, commit, CPU, filesystem and settings are
saved as artifacts. Every measured operation checks returned data; failures or
missing backend/stage results fail the runner. Shared CI and four short trials
are screening evidence. Cold-device latency, long steady-state writes, power-loss
durability and large-scale memory/compaction pressure still require dedicated runs.
# Focused durable-write comparison

The `write-comparison` workflow runs only on manual dispatch or a main commit
containing `[write-perf]`. It builds the same workload harness against the
pre-push baseline, current engine, and RocksDB, and rotates their execution order
over three trials on one host, with twelve trials for the 32-operation batch
case to expose intermittent storage stalls. Manual dispatch accepts a baseline commit.

It measures single puts, 32-operation batches, 256-operation batches with small
values, and writes with a 256 KiB memtable to exercise flushing and compaction.
The first three cases fit the engine's default active buffer; the batch cases
have at least 100 commit samples. Every key is checked before and after reopen.
The baseline must support the current harness's Engine/WriteBatch API.

Commit time and final flush time are reported separately. Resource snapshots
before the final flush expose write syscall counts and whether maintenance ran.
On the pressure case, background work can continue between the end of timing and
those snapshots. All three backends retain synchronous WAL durability and disable
compression. Shared CI timing includes scheduling and storage stalls; report the
trial spread and latency tails alongside throughput, not a universal winner.

## Write latency diagnostics

`write-diagnostics.yml` runs on manual dispatch or a main commit containing
`[write-diag]`. It builds separate normal, `write-profiling`, and RocksDB binaries.
Six rotating trials collect the usual batch32 score and per-commit phase timings.
The opt-in feature measures validation, locks, preparation, encoding, checksum,
allocation, WAL write/sync and memory publication. Default builds compile out
these clocks and counters. Instrumented scores are diagnostics, not performance
claims; both clock reads and sample collection perturb timing.

The workflow also probes synthetic files with growing, KEEP_SIZE-preallocated
and logically preallocated storage, using both fsync and fdatasync. These controls
do not establish WAL compatibility: extending a real WAL with zeros would break
strict recovery. Finally, strace records file writes, syncs and allocations for
both engines. Traces include setup/teardown and instrumentation overhead, so
their timings must not be used as benchmark scores. Artifacts retain every trial,
raw phase sample, probe and trace, including outliers. Shared storage can still
stall even after preallocation; syscall timing alone cannot identify host/device
contention or prove power-loss behavior.

## Compaction comparison

The same workflow additionally runs `compactions.py` when the commit contains
`[compact-perf]` or manual dispatch enables `compaction`. The focused write cases
also run, using the pre-push commit as baseline (or the selected manual baseline).

Three fixtures cover disjoint 128-byte values, four generations overwriting
128-byte values, and four generations overwriting 4096-byte values. Overwrite
fixtures delete one quarter of keys in the last generation. Preparation uses
64 MiB memtables and an explicit flush per generation with automatic compaction
disabled. Neither backend uses compression.

Baseline/current receive copies of the exact same baseline-generated engine
files; RocksDB receives the same logical history in its native format. Each of
six trials uses pristine copies, warms OS file cache outside timing, and rotates
backend order. Timing covers explicit compaction, including its sync/metadata
work. A fresh process records RSS/peak RSS before correctness reads. Every key is
checked after compaction and reopen. Baseline/current output SST names, lengths
and SHA-256 hashes must match. Report time spread as well as medians; differing
RocksDB layouts/policies and shared-host storage waits limit generalization.

## Cleanup comparison

Focused rechecks can select an existing workload without changing its query trace:
`writes.py OUT --case batch256-16b --trials 12` and
`workloads.py OUT --baseline --reads-only --profile 5 --scenario scan-t1-span512 --trials 12`.
Profile/scenario filters require `--reads-only` and are incompatible with `--smoke`.
Defaults preserve the full suite. A main commit with `[followup-perf]` and an
explicit `[baseline:<40-character SHA>]` runs these two rechecks, rotating backend
order over twelve trials. Retain the original full-run results alongside rechecks.

For a full same-run baseline/current/RocksDB comparison, a main commit can use
`[paired-perf] [baseline:<40-character commit SHA>]`. This runs the focused write
suite and `workloads.py --baseline`, including all read profiles, native-layout
churn compactions and bounded durable writes. The harness and query traces are
identical across baseline/current; engine read/compaction fixtures are copied
from the baseline. Six rotating trials balance backend order. This marker does
not change engine code. A baseline marker also overrides the usual push baseline
for the other comparison markers; a manual baseline input takes precedence.

`[cleanup-perf]` on a main commit runs the focused write and compaction suites,
then `workloads.py --baseline --reads-only` in the same job. The marker pins the
baseline to `9b50fb6`, immediately before the streaming SST/scan changes. Manual dispatch
can select another compatible baseline and enable `reads` and/or `compaction`.
The engine source is not changed by running this comparison.

Paired reads use six trials so baseline, current and RocksDB each occupy every
order position twice. Baseline/current receive copies of the same baseline-made
fixture; RocksDB has the same logical history in its native format. Summaries
include current/baseline as well as current/RocksDB ratios and trial ranges.
Read scenarios remain bounded to the cases above, with a ten-minute deadline.
All read fixtures are flushed: these scans check the disk iterator path, not
writer interference while constructing active/frozen memory snapshots. Table
preview performance is also outside this suite. No such speedup may be inferred
from these results.

The streaming comparison adds a bounded 4096-key, 4096-byte-value overwrite
fixture to exercise scan version selection without copying losing values. It
uses 96 short scans or 48 long scans per pass, with the same six paired trials
and baseline/current fixture copies. Other read profiles are unchanged.

## Remaining optimization checks

[paired-perf] [remaining-perf] [baseline:<SHA>] runs the existing suite and
remaining.py (six rotating paired trials). Added cases cover 2 MiB values,
mixed small/large values, WAL-only reopen, and early-stop active scans. Both
engine versions use byte-identical baseline fixtures; RocksDB uses matched writes.
Preparation exits after durable WAL commits without destructors. Reopen timing
excludes validation and the append/reopen correctness check. Warm file cache is
explicit; these are not cold-device or power-loss measurements.

Point latency collection now uses a shared budget of at most 4,000 samples across
all workers. The existing end-to-end mode remains. The additional throughput mode
uses ready workers and no per-query timers; timed work includes RNG,
allocations and owned-value destruction. Full validation runs in a separate pass. Do not merge its rates with
the older timed-query results. Internal frozen scans hold maintenance paused in
both versions and report creation/first-row time; they have no RocksDB comparison.
Internal profiles alternate baseline/current execution in six paired trials.
Memory snapshots share overlapping copy-on-write page groups (up to 64 pages,
256 records and 64 KiB of keys per page, except a single larger key). Small boundary
ranges retain only selected pages. Completed groups are released during traversal.
Memory merge candidates share key handles and borrow values; only emitted rows copy
keys and retain values. Shared updates use 16–32 row deltas before materialization;
structural edits can copy a page. Snapshotting can therefore still increase write cost.

Additional cases cover 2/4 MiB values followed by small-only phases with 20k,
40k, and 80k small keys, mixed cache pressure, 100k-key early scans, full memory
traversal, and batch writes while a snapshot is retained. The volatile snapshot-write
profile runs as a normal release example on baseline and current, without cfg(test)
batch hooks. It isolates copy-on-write costs from fsync, separately from durable
RocksDB comparisons. Frozen/cache diagnostics still run in release libtest builds. Cache hits/block reads are logical reads, not physical I/O.
The engine keeps at least a quarter of its cache budget for ordinary blocks;
large entries exceeding the remaining budget bypass caching. Idle oversized
entries can be reclaimed under sustained ordinary miss pressure.

### Focused memory scan scaling

Use `remaining.py OUTPUT --scan-focus` (or the `[scan-perf]` commit marker) to
compare the same harness against baseline, current, and RocksDB in six rotating
trials. Early-stop scans consume ten rows from 2k, 20k, 100k, and 200k key
memtables; 5,000 queries per case reduce timer noise. Random lower bounds, narrow
ranges, full scans, active-memory point reads, WAL recovery, and writes with a
retained snapshot guard against moving cost elsewhere. Creation, first-row, and
iterator-drop latency are reported separately. Memory-scan diagnostics retain
validation and all three phases in total throughput; general disk read passes do not. The 200k case uses a 128 MiB memtable on all backends to
avoid accidentally measuring disk scans. Internal profiles also isolate snapshot
copy-on-write cost. These measurements do not establish constant-time scaling.

The disjoint-range suite also reopens the database and reports first_scan_us and
repeat_scan_us outside the repeated-loop throughput timer. Reopen lazily learns SST
upper bounds in the engine; these are warm-OS-cache probes, not cold-device latency.
The standalone Criterion read/mixed benchmarks use a fixed 20,000-key fixture,
independent of selected filters and earlier write throughput.
