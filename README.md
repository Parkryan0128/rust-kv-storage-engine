# Rust Key-Value Storage Engine

An embedded, persistent key-value store built in Rust, with crash recovery, atomic batches, and snapshot scans.

Writes go to a durable log and memory, then move into sorted files on disk. Background compaction merges those files. Reads check memory first, then use Bloom filters and a block cache to find data on disk.

## Quick start

Requires **Rust 1.85+** and **Linux or macOS** with a local filesystem.

Add the library to your application's `Cargo.toml` (pin a reviewed commit for deployment):

```toml
[dependencies]
rust-kv-storage-engine = { git = "https://github.com/Parkryan0128/rust-kv-storage-engine" }
```

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

Use `Engine::new()` for an in-memory store. Clone an engine to share it across threads; only one engine can open a database directory at a time.

## Features

- **Persistence and recovery** — writes are synced to the write-ahead log before returning.
- **Atomic batches** — commit several puts and deletes together.
- **Snapshot scans** — read a sorted range as it existed when the scan began.
- **Background compaction** — merge sorted files and safely reclaim obsolete records.
- **Read caching** — Bloom filters skip irrelevant files; a block cache speeds up repeated reads.

See the [usage guide](docs/usage.md) for batches, scans, and manual flushes.

## Try the demo

From a checkout of this repository:

```bash
cargo run --locked --release --example demo
```

Open [localhost:8080](http://127.0.0.1:8080) to write keys, inspect memory and disk files, and compare compaction policies. Each launch uses a temporary database. **Close & reopen database** keeps the current files; stopping the server ends the session.

## Build and test

```bash
cargo build --locked
cargo test --locked --all-features
```

Tests cover recovery, corruption, concurrency, batches, and snapshots. See the [testing guide](docs/testing.md) for release tests, stress tests, lints, and benchmark commands.

## Before using it

- After a write or maintenance I/O error, drop all handles and reopen the database. A failed write may still appear after recovery.
- Drop or exhaust scans promptly: they can retain memory, old files, and the database directory lock.
- Use a local filesystem. Network filesystems are unsupported, and physical power loss has not been tested.
- Transactions and replication are not implemented. Keep the test-only `fault-injection` feature disabled in applications.

## Documentation

- [Usage](docs/usage.md) — values, batches, scans, and maintenance
- [Storage design and format](docs/storage-format.md) — recovery, file compatibility, and configuration
- [Memory and snapshots](docs/memory.md) — ownership, retention, and statistics
- [Tests and benchmarks](docs/testing.md) — commands and CI coverage
- [Performance results](docs/benchmarks.md) — RocksDB comparisons, compaction results, and measurement limits

## Contact

Ryan Park · [Email](mailto:parkryan0128@gmail.com) · [LinkedIn](https://www.linkedin.com/in/parkryan0128) · [GitHub](https://github.com/Parkryan0128)
