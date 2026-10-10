# Random-read comparison

This isolated package compares the current engine, the pre-format-change engine
at `ef358614095922dccbcf36fdaddc1aa174b1cb9d`, and RocksDB 11.8.1
(rust-rocksdb 0.25.0). All run the same Rust random-Get harness as separate
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
- Each engine uses its own production SST writer and separate fixtures: version
  04 for the current library and version 03 for the fixed baseline. Only the
  benchmark package is copied into the baseline checkout; library code and its
  fixture writer are unchanged.
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
occupies every position twice; all six backend-order permutations run once. Seeds change between trials
and match between backends and block sizes. Each
fresh process makes two identical seeded passes of 500,000 random successful
Get calls, validating the entire returned value every time. No sequential Get
pass is performed. The first pass starts with a fresh block cache; the second
repeats the same query sequence.

## Running

Use the `rocksdb-comparison` workflow for the complete reproducible run. It
builds all three backends, runs a small smoke case and then
the full comparison. Native RocksDB compilation requires Rust 1.88+ and
libclang; the library still supports Rust 1.85.

Fixture reports verify their compiled source root and
requested block size. Stage tests also check actual SST frame sizes and report
the compiled decoder source checksum; the runner verifies that against the
intended checkout before accepting measurements.

After checking out the pinned baseline at `baseline-source`, running
`prepare_baseline.py`, and producing `comparison-bin/engine`, `comparison-bin/baseline` and
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
previous organically generated write/compaction workload. Compare current and
baseline measurements from the same run; do not directly subtract numbers from
different hosts or runs. Results do not establish cold-device, concurrent, missing-key,
write, range-scan or durability performance. RocksDB uses pinned Get; the engine
returns independently owned values.

Before measured trials, a separate ignored test samples 10,000 random uncached
blocks for each library fixture. It times positional file reads,
CRC/validation, and lookup/value copying. Versions 03/04 fully validate the first
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

Version 04 narrows record offsets from u32 to u16 when they fit, retaining u32
for larger offsets. It adds no decompression or expanded cache index. This saves
two bytes per record in narrow blocks, plus any savings from denser block packing.
The speed effect must be measured against the baseline: the encoding alone does
not guarantee identical timings across workloads or machines.
