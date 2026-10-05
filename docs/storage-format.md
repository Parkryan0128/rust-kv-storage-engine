# Storage format

The engine keeps recent changes in memory and saves them in three kinds of files:

| File | Purpose |
|---|---|
| WAL (write-ahead log) | Records each change before it is applied in memory, so it can be recovered after a crash |
| SSTable (sorted string table) | Stores keys and values in sorted order on disk |
| MANIFEST | Lists the SSTables in use and which WAL data has already been saved |

The in-memory write buffer is called a **memtable**. A **tombstone** is a record that marks a key as deleted.

## Writes and reads

Each write is logged and synced with `sync_all()` before updating memory.

When the memory or WAL limit is reached, the next write starts a new buffer. A background worker saves the old buffer as an SSTable. Writers pause if too many buffers are waiting.

Reads check memory first, then SSTables. Bloom filters skip files that cannot contain the key, indexes locate its block, and a cache holds recently read blocks.

Point reads cache validated encoded blocks with a compact record-offset index.
The frame CRC and every record's bounds, header, key order and sequence are
checked before caching. Only the requested value is copied out, so retaining a
small returned value does not retain the whole block. Cache accounting includes
the allocated frame/index capacities and an allowance for cache bookkeeping;
it is not a hard bound on process RSS. Iteration and compaction still decode
complete blocks. The on-disk format is unchanged, including legacy SST support.

Point reads share an immutable table-list snapshot. The block cache tracks LRU
order through indexed links rather than a tree. When a miss requires eviction,
an unshared victim's frame and offset buffers can be reused for the incoming
block; outstanding readers retain their original immutable block. Oversized
misses do not evict useful entries for recycling. Every reused frame and record
is validated again before being cached. Cache charges include allocated buffer
capacities and a per-entry allowance; no separate spare-buffer pool is retained.

## Combining files

Compaction merges SSTables and keeps the newest record for each key.

- **SizeTiered** (default) merges similarly sized files, avoiding repeated rewrites of large files.
- **Full** merges all live SSTables when the file-count threshold is reached.

Partial merges keep tombstones so old values in other files stay deleted. A full merge can remove both the old value and its tombstone.

`flush()` saves earlier writes and waits for eligible compactions. `compact()` also requests a full merge.

## Saving files and recovering

New files become active in this order:

1. Write the new SSTable, sync it, rename it, and sync its directory.
2. Save and sync the updated MANIFEST, including its directory entry.
3. Update the in-memory state, then remove obsolete files.

On open, the engine reads the MANIFEST, replays newer WAL records, and removes temporary or unused files.

Recovery accepts an incomplete tail only in the newest WAL. Corrupt records return an error; a missing manifest is not reconstructed.

After a write or maintenance I/O error, drop all engine handles and reopen the database. A write that returned an error may still appear after recovery.

## File encoding

Records store a sequence number, a value/deletion tag, and key/value lengths and bytes. Empty values and deleted keys have different tags. Numbers use little-endian encoding; CRC32 checksums detect damaged frames.

Frames are capped at 64 MiB and records at 32 MiB. New SSTables use format `RKVSST02`: a metadata summary frame followed by checksummed index pages targeting 1 MiB each. A larger individual index key may exceed that target, but each frame stays within the 64 MiB limit. This allows the total index to grow past one frame without stopping compaction.

The summary stores record count u64, maximum sequence u64, Bloom probe count u32, Bloom byte length u32, Bloom bytes, and index-entry count u64. Each index entry stores first-key length u32, first-key bytes, block offset u64, and framed block length u32. Entries are never split across pages. The 28-byte footer stores magic (8 bytes), metadata offset u64, total framed metadata length u64, and CRC32 of the preceding 24 bytes. Empty SSTables contain only the summary frame and no index pages.

Existing `RKVSST01` SSTables remain readable, including in the same database as version `02` files. Flush and compaction write version `02`; WAL and MANIFEST stay at version `01`. Older engine versions cannot read the new SSTables, so keep a backup made with all handles closed before upgrading if rollback is needed.

Exact layouts: [records](../src/codec.rs), [WAL](../src/wal.rs), [SSTables](../src/sstable.rs), [manifest](../src/manifest.rs).

## Configuration

Pass an `Options` value to `Engine::open_with_options()`.

| Option | Default | Meaning |
|---|---:|---|
| `memtable_size_limit` | 4 MiB | Memory or WAL size that triggers a new buffer |
| `max_immutable_memtables` | 2 | Maximum waiting buffers before writers pause |
| `block_size` | 16 KiB | Target size of an SST data block |
| `block_cache_capacity` | 8 MiB | Block-cache budget; zero disables it |
| `bloom_filter_bits_per_key` | 10 | Filter space per key; allowed range 1–30 |
| `compaction_style` | `SizeTiered` | Which files to merge |
| `compaction_file_threshold` | 4 | Files needed for a merge; minimum 2 |
| `max_key_size` | 1 MiB | Maximum key length |
| `max_value_size` | 16 MiB | Maximum value length |

The key, value, and 17-byte record header must fit within 32 MiB. A single record can exceed the memtable or block target. Cache and memtable budgets do not cover all process memory. Index pages bound the serialization buffer, not the in-memory index: all SST indexes and Bloom filters are still loaded at open.

SizeTiered applies the threshold per size group; Full counts all live files. The policy can change on reopen. [Option validation](../src/engine.rs).

## Limits and statistics

Use a local filesystem that supports file/directory sync and atomic rename. Network filesystems are unsupported, and physical power loss has not been tested. Keep `LOCK` in place while the database is open, and do not use inherited engine handles after `fork()`.

`stats()` reports memory, files, reads, cache hits, and compaction. Byte counters track engine files, not physical device I/O. Cumulative counters reset on reopen; `sst_bytes` shows current live SST bytes. Statistics are not an atomic snapshot.
