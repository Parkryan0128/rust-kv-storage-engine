# Rust Key-Value Storage Engine

An embedded key-value storage engine written in Rust.

It writes changes to a log before updating memory, flushes sorted tables to disk, and merges them in the background. Reads use Bloom filters and a block cache.

[Benchmarks and compaction results](docs/benchmarks.md)

## How it works

The engine:

1. Appends each write or deletion to the write-ahead log and syncs it.
2. Updates the ordered, copy-on-write paged memtable.
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

Commit several puts and deletes atomically with `WriteBatch`:

```rust
use rust_kv_storage_engine::WriteBatch;

let mut batch = WriteBatch::new();
batch.put(b"name", b"Ryan");
batch.put(b"status", b"active");
batch.delete(b"old-name");
db.write_batch(batch)?;
```

A persistent batch uses one checksummed WAL frame and one `sync_all()`, then
publishes all operations together. Operations run in insertion order; the last
operation on a repeated key wins. Empty batches do nothing on a healthy engine.
Validation rejects the whole batch before writing. After an I/O failure or crash,
recovery includes the whole batch or none of it; a failed call may still have
committed. Separate `get()` calls are not a snapshot or a read/write transaction.

Each entry obeys the configured key/value limits, and the total encoded batch is
limited to 64 MiB (including headers). A batch is never split across memtables and
can exceed the memtable target. Older binaries cannot replay WALs containing
batches; keep a closed-database backup before upgrading if rollback is needed.

Read a range in ascending bytewise key order:

```rust
for row in db.scan(b"user:1".as_slice()..b"user:9".as_slice())?.take(100) {
    let (key, value) = row?;
    // Use this key/value pair.
}
```

`scan(start..end)` includes the start and excludes the end. Use `..=end` for an
inclusive end, `start..` or `..end` for an open end, and `scan(..)` for all keys.
A pair of `std::ops::Bound` values also supports an excluded start. Reversed
bounds return `InvalidConfig`; equal endpoints are empty unless both are included.
Ordering uses raw bytes, so `user:10` sorts before `user:2`.

Each owned `Scan` captures a snapshot when created, including whole atomic batches.
Later puts, deletes, flushes and compactions do not change its results. Duplicate
versions are merged and deleted keys are omitted. SST blocks are read lazily,
bypassing the point-read cache. Each item is a `Result`; an I/O or corruption error
ends the iterator after reporting the error, possibly after some successful rows.

Snapshot creation shares overlapping groups of active/frozen memory pages and
captures SST handles under the state lock. One group contains at most 64 pages;
creation clones group handles instead of touching every page or matching key.
Keys are copied when consumed; completed groups and sources are released. A
boundary group can retain pages outside the requested range. Later writes copy
only shared group metadata and changed pages, so retaining a snapshot can increase
write latency and memory use. Pages are bounded by 256 records and 64 KiB of keys,
except that one larger key occupies its own page.
Iteration buffers one block and candidate record per SST, rather than all disk
results. `.take(n)` limits consumption; `.collect()` allocates the full result.
Drop or exhaust scans promptly: they retain the database lock and old SST handles,
which can delay reclaiming disk space after compaction. They also keep selected
memory values alive. Transactions and replication are not implemented.

Background compaction uses size tiers to avoid rewriting large SSTs with every small flush. `compact()` still merges all tables and reclaims tombstones. `Options::compaction_style` can select `CompactionStyle::Full` to merge all tables. Size tiers trade lower write amplification for more retained versions and potentially slower reads.

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

The test creates and cleans up its own temporary database there. This tests filesystem I/O and logical recovery, not physical power loss. Normal tests also check 20,000 distinct keys and compaction of 64 valid 1 MiB keys whose combined SST index exceeds 64 MiB. The million-key and extended stress CI jobs run only when the workflow is manually dispatched.

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

The RocksDB comparison workflow is manual-only; ordinary pushes run correctness
checks without executing performance benchmarks or the extended stress tests.

A separate [bounded workload suite](benchmarks/rocksdb-compare/WORKLOADS.md) covers
misses, hot keys, concurrent readers, ranges, durable batches and compaction, with
memory/disk metrics and four paired repetitions against RocksDB. Its workflow is
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

## Contact

Ryan Park · [Email](mailto:parkryan0128@gmail.com) · [LinkedIn](https://www.linkedin.com/in/parkryan0128) · [GitHub](https://github.com/Parkryan0128)
