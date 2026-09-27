# rust-kv-storage-engine

An embedded LSM-tree key-value storage engine built from scratch in Rust, with synchronous write durability, background flushing and compaction, checksummed storage, Bloom filters, and a bounded block cache.

**Status:** Milestones 1–6 implemented. The repository includes model-based, concurrent, corruption, subprocess-crash, and extended stress tests, plus executable benchmarks. This is a small single-node storage engine; see the explicit guarantees and limits below.

## Quick start

```rust
use rust_kv_storage_engine::Engine;

fn main() -> rust_kv_storage_engine::Result<()> {
    let db = Engine::open("./data")?;
    db.put(b"name", b"Ryan")?; // WAL sync completes before success
    assert_eq!(db.get(b"name")?.as_deref(), Some(&b"Ryan"[..]));
    db.delete(b"name")?;
    db.flush()?;              // optional: wait for SST coverage of preceding writes
    db.compact()?;            // optional: flush, merge, deduplicate, collect tombstones
    Ok(())
}
```

`Engine::new()` remains available as a **volatile, in-memory** engine. Use `Engine::open(path)` or `Engine::open_with_options(path, options)` for persistence. Keys and values are arbitrary bytes, including empty bytes, within configured size limits. A missing key and a deleted key both return `Ok(None)` to callers; the internal read path distinguishes them.

`Engine` is cheaply cloneable. Clones share the writer, memory state, cache, worker, and directory lock. `KvEngine` also exposes the three data operations as a trait. The last clone's drop joins the worker and releases the lock; it need not flush because every acknowledged persistent mutation is already in a synced WAL.

## Guarantees

- **Durability:** every successful persistent `put` or `delete` has a checksummed WAL record synced with `File::sync_all()` before it becomes visible in memory. Newly created files and atomic renames also sync their parent directory.
- **Ordering:** a serialized writer assigns monotonically increasing sequence numbers. Concurrent point reads observe a consistent layer snapshot; newest sequence wins. SST compaction retains the newest version of each key.
- **Deletion:** tombstones shadow older records across all layers. They are removed only when merging **all** live disk tables; all active/frozen memory generations contain newer mutations.
- **Crash recovery:** the manifest determines the live SST set and durable WAL checkpoint. Recovery replays later WAL generations in order. An incomplete final WAL frame is truncated; an intact frame with a bad checksum is an error. Unacknowledged writes may or may not appear after recovery.
- **Exclusive open:** an OS advisory lock permits one engine per database directory across processes. Use clones for concurrent access within that engine.
- **Failure handling:** write/maintenance I/O failures halt the engine; reopen to recover. Read corruption is returned as an error. A failed write is not a promise that the record is absent after reopening.

These guarantees assume a local POSIX filesystem that implements file/directory sync and atomic same-directory rename correctly. The tests exercise process crashes and injected I/O failures, not physical power loss. Hardware that lies about sync, external deletion/editing of live files, network filesystems, and shared use of an inherited engine after `fork()` are outside the guarantee. Do not delete the `LOCK` file while the database is open.

## Architecture

Writes: serialized writer → synced per-generation WAL → active `BTreeMap` memtable. When its accounted size or WAL size reaches the threshold, the **next mutation** rotates to a new WAL and freezes the prior memtable. Explicit `flush()` also freezes it.

Reads: active memtable → frozen memtables newest first → live SSTables. SST lookups check the Bloom filter, binary-search the block index, then consult the shared LRU block cache. Disk-table candidates are resolved by sequence number. Disk I/O runs outside the memory-state and cache locks.

One maintenance thread flushes frozen tables and performs **single-tier full-run compaction** at the configured SST count. Compaction uses a heap merge with one decoded block per input, rather than loading the full database. SST publication and manifest replacement are serialized, while foreground operations continue; sustained overload applies writer backpressure.

Compaction policy deliberately favors simple, auditable deletion and recovery rules. Rewriting the entire base run has substantial write amplification as the database grows. This is not a multi-level RocksDB replacement or a sustained large-dataset throughput claim.

```text
data/
  LOCK
  MANIFEST
  wal/00000000000000000001.wal
  sst/00000000000000000003.sst
```

File IDs are shared across WAL/SST creation, so gaps are normal. Temporary files are never authoritative. See [the on-disk format and crash ordering](docs/storage-format.md).

## Options and observability

| Option | Default | Meaning |
|---|---:|---|
| `memtable_size_limit` | 4 MiB | Rotation threshold for accounted memory or WAL bytes |
| `max_immutable_memtables` | 2 | Writer backpressure when the frozen queue is full |
| `block_size` | 16 KiB | Target data-block size; a single larger record is allowed |
| `block_cache_capacity` | 8 MiB | Accounted resident block-cache budget; zero disables caching |
| `bloom_filter_bits_per_key` | 10 | Filter accuracy/space tradeoff; allowed range 1–30 |
| `compaction_file_threshold` | 4 | Full-run merge trigger; minimum two |
| `max_key_size` | 1 MiB | Maximum key length |
| `max_value_size` | 16 MiB | Maximum value length |

The combined configured key/value limits plus the 17-byte record header cannot exceed 32 MiB. Block targets must be 64 bytes to 32 MiB. Filters are capped at 8 MiB per table and metadata frames at 64 MiB; very large runs should use a more scalable compaction/index design.

The active/frozen memtable budget is approximate, allowing one oversized record per generation. SST indexes/filters, merge buffers, allocator overhead, and values retained by callers are additional memory; the memtable/cache limits are **not a process RSS cap**. Backpressure is necessary when storage cannot keep up with writers.

`stats()` reports sequence, active/frozen bytes, frozen count, live SST count, physical SST record count (including duplicates/tombstones before compaction), block-read calls, cache hits, Bloom negatives, and resident cache bytes. `block_reads` counts engine data-block reads; it does not distinguish OS page-cache hits from hardware I/O.

## Build and validation

```bash
cargo build --locked
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --all-features
cargo test --locked --release --all-features

# Extended acceptance: 100 keys × 50,000 overwrites, then delete/compact/reopen
cargo test --locked --release --all-features --test stress -- --ignored --nocapture

cargo bench --locked --bench engine
cargo run --locked --release --example bench_report
```

The `fault-injection` feature exists for subprocess testing only. It enables the `crash_worker` binary and named environment-controlled crash/I/O-error hooks. Leave it disabled in applications.

GitHub Actions runs formatting, warning-free Clippy, debug/release tests, and benchmark compilation on Linux and macOS. A manual workflow run additionally executes the five-million-write stress test. See [validation coverage and local results](docs/validation.md).

## Benchmarks

[Recorded results and methodology](docs/benchmarks.md) include throughput and per-operation p50/p99 for synced writes, warm-cache reads, reads with the engine block cache disabled, and a mixed workload. [Raw CSV](docs/benchmark-results.csv) is generated by `examples/bench_report.rs`. Criterion provides a separate, growing-dataset benchmark.

The recorded environment uses container overlayfs. Its sync latency is **not representative of durable physical media**, and cache-disabled reads can still hit the OS cache. Re-run on the intended filesystem before making performance claims.

## Milestones

| Milestone | Implemented deliverables |
|---|---|
| 1 — Memory and synchronization | Byte API, shared engine handles, sorted memtable, serialized writers/concurrent readers |
| 2 — Durability and recovery | Versioned, checksummed WAL generations; sequence recovery; torn-tail handling; exclusive open |
| 3 — Flush and SST creation | Bounded frozen queue, background flushing, sorted/indexed SSTs, tombstone persistence |
| 4 — Hierarchical reads | All-layer resolution, per-table Bloom filter, bounded thread-safe LRU block cache, read counters |
| 5 — Compaction | Streaming multi-way full-run merge, newest-wins deduplication, tombstone GC, atomic manifest publication |
| 6 — Validation and measurements | Differential testing, concurrent history checking, corruption/crash/I/O fault tests, stress, Criterion, p50/p99 report |

## Scope

Linux/macOS local POSIX filesystems; one database directory and point operations. No distributed replication, transactions, range scans, compression, column families, snapshots, or Windows support. The disk format is versioned `01`; no upgrade/migration mechanism is provided yet.
