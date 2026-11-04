# Usage

[Back to README](../README.md)

Examples below use an engine opened with `let db = Engine::open("./data")?;` and propagate errors from a function returning `Result<()>`.

## Values and engine handles

`get()` returns an owned, immutable `Bytes` value that remains valid after
writes, cache eviction, compaction, or dropping the engine. Large single-record
values may share validated storage; retained values are outside the block-cache
capacity. See [value ownership](storage-format.md) for memory details.

`Engine::new()` creates an in-memory store. `Engine::clone()` shares the same database and can be used across threads. Only one engine can open a database directory at a time.

In-memory deletes remove keys immediately. Persistent deletes retain tombstones until compaction can safely discard them, so older on-disk values do not reappear.

This is a Rust library; there is no general-purpose `put/get/delete` CLI. The executable targets are the browser demo, benchmark reports, and a test-only crash worker.

## Flush and compaction

Flush pending records to SSTables and merge them explicitly:

```rust
db.flush()?;
db.compact()?;
```


Explicit `compact()` flushes the writes before its writer barrier and requests one
full merge, without first draining redundant automatic merges. An unchanged
single SST already produced by a full merge during this open is reused, keeping
its file and cache entries. This proof is cleared on reopen; the first explicit
compaction then still rewrites the SST with the current options and format.
New SST publication invalidates the proof.

Persistent writes are synced to the WAL before returning. After a write or maintenance I/O error, drop all handles and reopen the database. A failed write may still appear after recovery.

Background compaction uses size tiers to avoid rewriting large SSTs with every small flush. `compact()` still merges all tables and reclaims tombstones. `Options::compaction_style` can select `CompactionStyle::Full` to merge all tables. Size tiers trade lower write amplification for more retained versions and potentially slower reads.

## Atomic batches

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

## Range scans

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
A range proven empty at creation retains no database handles and stays empty
even when later writes add keys in that range.
Later puts, deletes, flushes and compactions do not change its results. Duplicate
versions are merged and deleted keys are omitted. SST blocks are read lazily,
bypassing the point-read cache. Each item is a `Result`; an I/O or corruption error
ends the iterator after reporting the error, possibly after some successful rows.

Iteration buffers one block and candidate record per SST, rather than all disk results. `.take(n)` limits consumption; `.collect()` allocates the full result. Drop or exhaust scans promptly: they retain the database directory lock and old SST handles, which can delay reclaiming disk space after compaction. They also keep selected memory values alive. Transactions and replication are not implemented.

See [memory and snapshots](memory.md) for retention costs and [storage format](storage-format.md) for configuration and compatibility.
