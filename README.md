# Rust Key-Value Storage Engine

An embedded key-value storage engine written in Rust.

It writes changes to a log before updating memory, flushes sorted tables to disk, and merges them in the background. Reads use Bloom filters and a block cache.

[Benchmarks and compaction results](docs/benchmarks.md)

## How it works

The engine:

1. Appends each write or deletion to the write-ahead log and syncs it.
2. Updates the active `BTreeMap` memtable.
3. Rotates to a new memtable before the next write once the memory or WAL limit is reached.
4. Flushes frozen memtables to sorted SSTables in a background thread.
5. Checks memory first on reads, then uses Bloom filters, block indexes, and the LRU cache to find records in SSTables.
6. Merges similarly sized SSTables, keeping the newest record per key. Partial merges retain tombstones; full merges can remove them.
7. Recovers on open by loading the manifest and replaying newer WAL records.

Open a database directory:

```rust
use rust_kv_storage_engine::{Engine, Result};

fn main() -> Result<()> {
    let db = Engine::open("./data")?;

    db.put(b"name", b"Ryan")?;
    assert_eq!(db.get(b"name")?.as_deref(), Some(&b"Ryan"[..]));
    db.delete(b"name")?;

    Ok(())
}
```

`Engine::new()` creates an in-memory store. `Engine::clone()` shares the same database and can be used across threads. Only one engine can open a database directory at a time.

In-memory deletes remove keys immediately. Persistent deletes retain tombstones until compaction can safely discard them, so older on-disk values do not reappear.

Use it as a Rust library by adding the repository to your application's `Cargo.toml` (pin a reviewed commit for deployment):

```toml
[dependencies]
rust-kv-storage-engine = { git = "https://github.com/Parkryan0128/rust-kv-storage-engine" }
```

There is no general-purpose `put/get/delete` CLI. The executable targets are the browser demo, benchmark reports, and a test-only crash worker.

Flush pending records to SSTables and merge them explicitly:

```rust
db.flush()?;
db.compact()?;
```

Persistent writes are synced to the WAL before returning. After a write or maintenance I/O error, drop all handles and reopen the database. A failed write may still appear after recovery.

Background compaction uses size tiers to avoid rewriting large SSTs with every small flush. `compact()` still merges all tables and reclaims tombstones. `Options::compaction_style` can select `CompactionStyle::Full` to merge all tables. Size tiers trade lower write amplification for more retained versions and potentially slower reads. Transactions, range scans, and replication are not implemented.

[Storage format and configuration](docs/storage-format.md)

## Local demo

Use the browser demo to write keys, inspect memory and SST files, and compare compaction policies:

```bash
cargo run --locked --release --example demo
```

Open **http://127.0.0.1:8080**. The server binds to loopback and uses a temporary database for each launch. **Close & reopen database** keeps the current files; stopping the server ends the session.

## Project structure

```text
src/        Engine, memtable, WAL, SSTables, compaction, and cache
src/bin/    Crash-test subprocess
tests/      API, recovery, corruption, concurrency, and stress tests
benches/    Criterion benchmarks
examples/   Local browser demo, latency and compaction reports
docs/       Storage format and benchmark results
```

## Build

Requirements:

- Rust 1.85 or newer
- Linux or macOS with a local filesystem

```bash
cargo build --locked
```

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

The test creates and cleans up its own temporary database there. This tests filesystem I/O and logical recovery, not physical power loss. Normal tests also check 20,000 distinct keys and compaction of 64 valid 1 MiB keys whose combined SST index exceeds 64 MiB. CI runs the million-distinct-key test in a separate Linux job.

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

## Benchmarks

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

## Contact

Ryan Park · [Email](mailto:parkryan0128@gmail.com) · [LinkedIn](https://www.linkedin.com/in/parkryan0128) · [GitHub](https://github.com/Parkryan0128)
