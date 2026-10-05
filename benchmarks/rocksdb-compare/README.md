# Random-read comparison

This isolated package compares the current engine, the fixed pre-change engine
at `c5fe790d5352bdea9df921419ff855938fd13aa9`, and RocksDB 11.8.1
(rust-rocksdb 0.25.0). All three run the same Rust random-Get harness as separate
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
- The old and new engine executables read the exact same engine fixture.
- Both use 16 KiB blocks, an 8 MiB block-cache budget, ten Bloom bits per key
  and no compression. Metadata is outside the block cache.
- The engine compaction trigger is 64; RocksDB opens read-only. File counts are
  asserted at open and after each pass, so the prepared layout remains fixed.
- OS file cache is explicitly warmed before each process. This does not preload
  the engine block cache. Datasets fit the benchmark host's RAM.

Cases: 100,000 keys / 128-byte values / one SST; 1,000,000 keys / 128-byte values /
five SSTs; and 100,000 keys / 1,024-byte values / five SSTs. Keys are eight-byte
big-endian integers; values encode the key and a known fill pattern.

Each case runs six trials. Backend order rotates so each backend occupies every
position twice. Seeds change between trials and match between backends. Each
fresh process makes two identical seeded passes of 500,000 random successful
Get calls, validating the entire returned value every time. No sequential Get
pass is performed. The first pass starts with a fresh block cache; the second
repeats the same query sequence.

## Running

Use the `rocksdb-comparison` workflow for the complete reproducible run. It
builds the baseline with the current harness, runs a small smoke case and then
the full comparison. Native RocksDB compilation requires Rust 1.88+ and
libclang; the library still supports Rust 1.85.

After producing `comparison-bin/baseline`, `comparison-bin/engine`, and
`comparison-bin/rocksdb` as shown in the workflow:

```sh
python3 benchmarks/rocksdb-compare/run.py comparison-results
```

Use `KV_COMPARE_SMOKE=1` for a 100-key, 1,000-query-per-pass smoke case.
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
previous organically generated write/compaction workload. Use the included
same-host baseline to assess the optimization; do not directly subtract old
benchmark numbers. Results do not establish cold-device, concurrent, missing-key,
write, range-scan or durability performance. RocksDB uses pinned Get; the engine
returns independently owned values.
