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
- Synchronous individual puts and atomic batches of 32/256 operations; 2,048 keys
  per run. WAL commit time and final flush time are reported separately.
- Full compaction of copied churn fixtures, with before/after file allocation.

Four trials alternate backend-first order; scenarios rotate and seeds match.
Results include paired speed-ratio median/min/max, not a confidence interval.
Fixtures, file-cache warming, statistics and reporting are outside operation
timing. Each measured read process reopens a fixture, starts with a fresh DB
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
RocksDB iterators provide their normal consistent view; engine scans copy matching
memory keys and pin SST handles. Read fixtures are flushed before timing.

Point latency samples time only the API call. Scan latency includes validation
and row destruction as well as the scan. Throughput includes RNG, checks and
value destruction; four-thread throughput also includes startup/join. Do not
compare scan-query ops/s with point ops/s; scan row counts are reported separately.
Writes include native batch construction from the same prebuilt bounded trace.

RSS/peak RSS include the process/runtime, sample arrays and validation, but read
peaks exclude fixture creation. Write peaks include input traces. RSS excludes OS
page cache. File size, allocated regular-file blocks, process I/O counters and
backend statistics remain separate; none is a complete physical-device metric.

Raw JSONL, command logs, binary hashes, commit, CPU, filesystem and settings are
saved as artifacts. Every measured operation checks returned data; failures or
missing backend/stage results fail the runner. Shared CI and four short trials
are screening evidence. Cold-device latency, long steady-state writes, power-loss
durability and large-scale memory/compaction pressure still require dedicated runs.
