# Testing and benchmarks

[Back to README](../README.md)

Run commands from the repository root. Requires Rust 1.85 or newer and Linux or macOS with a local filesystem.

## Tests

```bash
cargo test --locked --all-features
cargo test --locked --release --all-features
```

Run the extended tests: five million overwrites of 100 keys, and growth to one million distinct keys with reopen, update, delete, and compaction checks:

```bash
cargo test --locked --release --all-features --test stress -- --ignored --nocapture
```

To run the growing-database test on a chosen local disk, set `KV_STRESS_DIR` to an existing scratch directory on that disk:

```bash
KV_STRESS_DIR=/mnt/test-disk/kv-scratch cargo test --locked --release --test stress million_distinct_keys -- --ignored --nocapture
```

The test creates and cleans up its own temporary database there. This tests filesystem I/O and logical recovery, not physical power loss. Normal tests also check 20,000 distinct keys and compaction of 64 valid 1 MiB keys whose combined SST index exceeds 64 MiB. The dedicated million-key CI job runs on manual dispatch. The extended job runs both large acceptance tests on manual dispatch or an explicit `[extended-stress]` main-branch commit marker.

Check formatting and lints:

```bash
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
```

Check the local demo:

```bash
cargo build --locked --release --example demo
python3 scripts/test_demo.py
```

Tests cover crash recovery, corruption, concurrent operations, and compaction, including a merge that encounters corrupt input without publishing partial output or deleting its sources. They also check memory reclamation, cache eviction and accounting, interrupted/short I/O, empty keys and values, and directory locks through symlinks. CI checks all targets and features with Rust 1.85.0 in addition to stable Rust on Linux and macOS.

Process-kill tests leave the OS cache intact; physical power loss has not been tested. The `fault-injection` feature is for tests; leave it disabled in applications.

Batch tests cover repeated keys, validation and sequence limits, concurrent
writes/flushes, every truncation boundary in a small WAL batch, corrupt batch
payloads, and subprocess crashes or injected I/O errors before/after WAL sync
and during memory application. They also cover acknowledged batches across
flush/compaction publication failures, maintenance errors racing with writes,
and sequence preservation after compacting a delete-only batch to an empty SST.

Range tests cover binary and empty keys, inclusive/exclusive bounds, duplicate
versions and tombstones across memory and disk, snapshots during writes and
compaction, legacy SST formats, deferred corruption/I/O errors, cache isolation,
and comparison with a filtered `BTreeMap` through batches, flushes and reopens.

## Benchmarks

RocksDB comparisons and extended acceptance tests are opt-in. Ordinary pushes
run correctness checks; explicit commit markers or manual dispatch request the
longer jobs. The [comparison guide](../benchmarks/rocksdb-compare/README.md) describes
`[full-comparison]`, `[performance-regression-loop]`, and exact baseline markers.

A separate [bounded workload suite](../benchmarks/rocksdb-compare/WORKLOADS.md) covers
misses, hot keys, concurrent readers, ranges, durable batches and compaction, with
memory/disk metrics. Default runs use four repetitions against RocksDB, or six
when an explicit engine baseline is included. Its workflow is
opt-in through manual dispatch or a `[perf-check]` main-branch commit message.

Run the Criterion benchmarks:

```bash
cargo bench --locked --bench engine
```

Print throughput and p50/p99 latency as CSV:

```bash
cargo run --locked --release --example bench_report
```

Compare compaction policies on append, hot-set, and delete-heavy workloads:

```bash
cargo run --locked --release --example compaction_report -- 3
```
