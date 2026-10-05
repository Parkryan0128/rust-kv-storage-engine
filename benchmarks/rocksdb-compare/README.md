# Paired Linux resource comparison

This isolated package runs the same Rust workload against this engine and
RocksDB 11.8.1, through rust-rocksdb 0.25.0. Each backend is built into a separate
release executable. RocksDB is a benchmark dependency only; it does not change
the library's dependencies or Rust 1.85 support. The benchmark requires Rust
1.88 or newer and libclang for the native RocksDB build.

Both backends use a 4 MiB write buffer, at most three write buffers, an 8 MiB
block cache, 16 KiB blocks, ten Bloom filter bits per key, and no compression.
Every individual mutation writes a WAL and calls fsync before returning.
The engine retains its default size-tiered compaction and RocksDB its default
leveled compaction. These are comparable configuration budgets, not identical
algorithms or hard process memory limits. RocksDB's metadata remains outside
the block cache, as it does in this engine.

## Workload

One client inserts increasing eight-byte keys, flushes, reopens, performs two
identical random lookup passes, updates half the keys, deletes a quarter,
flushes and fully compacts. It then reopens and verifies every key and value.
The random read passes each contain at most 100,000 lookups. Values contain
the key and an update marker so verification detects incorrect values as well
as missing keys. No full in-memory expected-value map is kept.

The runner performs three paired trials of 100,000 keys with 128-byte values,
two paired trials of 1,000,000 keys with 128-byte values, and one paired trial
of 100,000 keys with 1,024-byte values. Each trial uses a fresh process and
database; backend order alternates. All trials run sequentially on one host.

## Run

The `rocksdb-comparison` GitHub Actions workflow builds and runs this benchmark
on a disposable Ubuntu runner. It uploads raw JSONL records, environment
metadata, per-process logs, and the dependency lockfile. The small verification
run is stored separately from full measurements.

To reproduce on a disposable Linux host with cgroup v2 and its memory
controller available:

```sh
export CARGO_BUILD_JOBS=3 CXXFLAGS=-g0
mkdir -p comparison-bin
cargo build --locked --release --manifest-path benchmarks/rocksdb-compare/Cargo.toml
cp benchmarks/rocksdb-compare/target/release/kv-compare comparison-bin/engine
cargo build --locked --release --manifest-path benchmarks/rocksdb-compare/Cargo.toml --features rocks
cp benchmarks/rocksdb-compare/target/release/kv-compare comparison-bin/rocksdb
sudo env KV_BENCH_COMMIT="$(git rev-parse HEAD)" KV_BENCH_RUST="$(rustc --version)" \
  python3 benchmarks/rocksdb-compare/run.py comparison-results
```

The runner creates and removes only its own unique cgroups and temporary DB
directory. It may enable the root cgroup memory controller; it does not set a
memory limit or drop the host page cache. `KV_COMPARE_SMOKE=1` runs a 100-key
verification workload instead. `KV_BENCH_DIR` selects the temporary filesystem.

## Interpreting the numbers

- Process RSS includes the engine, allocator retention and a bounded latency
  sample buffer. Peak RSS covers all phases, including verification.
- Cgroup `memory.current` and `memory.peak` include charged anonymous memory,
  file cache and kernel memory. Each child enters its group before exec and
  creating DB files. Already shared executable/library pages can be charged
  elsewhere. Do not add RSS to cgroup file cache because mapped pages overlap.
- Reopen occurs in the same process; allocator state and OS page cache remain.
  Neither random read pass measures cold-disk reads.
- Mutation throughput includes the operation loop and the final synchronous
  flush/background compaction wait. Per-operation p50/p99 is sampled at most
  10,000 times and excludes that final maintenance pause. Lookup throughput
  includes correctness checks.
- Disk bytes include regular DB files, including WAL and metadata. Allocated
  bytes use `st_blocks`, excluding directory metadata. Compare both after load
  and after compaction; allocation can differ from logical file length.
- Compression is disabled for a controlled comparison. The synthetic values
  are compressible, and RocksDB can support compression in other builds.
- Inspect the recorded filesystem mount options. Successful fsync calls on
  hosted CI storage do not validate power-loss recovery or physical durability.
- This is a small, single-client comparison. It does not establish concurrent
  throughput, long-running write amplification, production reliability, or
  performance with datasets larger than RAM. Two large trials and one larger
  value trial support only provisional conclusions about those workloads.
