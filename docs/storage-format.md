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

`Engine::write_batch(WriteBatch)` validates all operations, writes one WAL frame,
syncs once, then applies the entire batch under the state write lock. The writer
barrier prevents other writes or rotation from splitting the batch. Each
operation receives a consecutive sequence number, including deletes and repeated
keys; the last operation on a key wins. Individual reads cannot observe a partially
applied batch, but separate reads do not form a snapshot. Empty batches only check
engine health. Validation errors leave the engine usable and apply nothing.

When the memory or WAL limit is reached, the next write starts a new buffer. A background worker saves the old buffer as an SSTable. Writers pause if too many buffers are waiting.

Reads check memory first, then SSTables. Bloom filters skip files that cannot contain the key, indexes locate its block, and a cache holds recently read blocks.

Table snapshots are ordered by descending maximum sequence. After finding a
record (including a tombstone), reads stop when the remaining files cannot
contain a newer version. A file's maximum sequence is only an upper bound:
finding a key in the first file does not by itself end the search. Keys below
a table's first indexed key skip that table before checking its Bloom filter.

Point reads cache encoded blocks. Version 03 stores the compact record-offset
index inside each checksummed data frame. On the first read of each block after
open, every record's bounds, header, key order, sequence and persisted offset are
validated. The table retains an eight-byte validation fingerprint per block.
After eviction, an identical payload CRC permits reuse of that validation;
every disk read still verifies the entire frame CRC. A changed CRC triggers full
validation again. This uses the existing CRC32 corruption model, not authentication
against deliberate checksum collisions. Search-path record decoding remains checked.
Versions 01/02 rebuild and validate their offset arrays on each cache miss.
Only the requested value is copied out, so retaining a
small returned value does not retain the whole block. Cache accounting includes
the allocated frame/index capacities and an allowance for cache bookkeeping;
it is not a hard bound on process RSS. Iteration and compaction still decode
complete blocks, without allocating a second full payload buffer.

Point reads share an immutable table-list snapshot. The block cache tracks LRU
order through indexed links rather than a tree. When a miss requires eviction,
an unshared victim's frame and offset buffers can be reused for the incoming
block; outstanding readers retain their original immutable block. Frames that
cannot fit the budget do not evict entries for recycling: legacy blocks use a
conservative offset-array bound and version 03 includes its index in the frame.
Reused frames follow the checksum and validation rules above. Cache charges include allocated buffer
capacities and a per-entry allowance; no separate spare-buffer pool is retained.

Reused frames grow to the requested length instead of doubling their capacity.
Sparse blocks shed oversized recycled offset arrays; retained spare capacity is
trimmed when it would prevent cache admission. Compaction evicts obsolete table
entries and prevents old reader snapshots from putting them back in the cache.
Outstanding readers still finish safely through their immutable blocks/open files.

Strict key ordering lets validation check the next-block upper bound once per
block. Block splitting includes encoded record lengths and the persisted directory,
separately from memtable memory charges.

## Range snapshots

`Engine::scan()` copies matching active/frozen memtable keys and shares immutable
value bytes while holding the state read lock. It captures the SST list under
that same lock, so it observes one consistent state, including all or none of a
batch. After creation, the owned iterator requires no engine state lock for I/O.
Later writes or maintenance failures do not change an already captured snapshot.

Each SST iterator seeks through the index to the lower-bound block, then searches
within that block. It streams forward up to the upper bound, retaining one encoded
block and a candidate record per source. A min-heap merges sources by key, chooses
the highest sequence number, and omits tombstones. Scans use the existing validated
block decoder for all supported SST versions and do not change any file format.
They bypass the point-read cache; disk reads still increment `stats().block_reads`.
Errors are iterator items and terminate the scan; earlier rows may already have
been returned. Exhausted or failed iterators always return `None` thereafter.

Creating a large memory snapshot can delay writers and allocates copied keys plus
map entries, while values share immutable bytes. Disk buffering grows with the
number of SSTs and their block sizes; it is not a fixed process-memory limit.
The iterator holds an engine handle and its open SSTs until drop, error or
exhaustion. Compaction can unlink old files, but those handles delay reclaiming
their space; `sst_bytes` counts current live tables, not unlinked pinned files.
Drop long-lived or partially consumed scans before reopening the database.

## Combining files

Compaction merges SSTables and keeps the newest record for each key.

- **SizeTiered** (default) merges similarly sized files, avoiding repeated rewrites of large files.
- **Full** merges all live SSTables when the file-count threshold is reached.

Partial merges keep tombstones so old values in other files stay deleted. A full merge can remove both the old value and its tombstone.

`flush()` saves earlier writes and waits for eligible compactions. `compact()` also requests a full merge.

Each merge opens at most four input iterators. Larger requests proceed through
manifest-published partial merges, retaining tombstones until it is safe to remove
them. Recovery can resume from any published round. This bounds input block/value
residency by four inputs rather than the total SST count; it does not cap RSS,
which also includes indexes, filters, output buffers and concurrent operations.
The tradeoff is additional intermediate write I/O for merges with more than four
inputs. `stats().max_compaction_inputs` reports the observed maximum since open.
Obsolete-cache cleanup runs after releasing the engine state write lock.

The background worker releases the maintenance lock fairly between jobs so a
manual flush does not wait for a continuously replenished background queue.
When compaction discards enough records to overallocate the Bloom filter by more
than roughly two times, the writer rebuilds it from output blocks with bounded
scratch space. Empty output always uses the minimum filter size. This adds an
output read pass only for such shrinking merges and does not change the file format.

## Saving files and recovering

New files become active in this order:

1. Write the new SSTable, sync it, rename it, and sync its directory.
2. Save and sync the updated MANIFEST, including its directory entry.
3. Update the in-memory state, then remove obsolete files.

On open, the engine reads the MANIFEST, replays newer WAL records, and removes temporary or unused files.

New or interrupted database initialization syncs the directory's ancestor chain
before publishing its first manifest, including entries created by an earlier
interrupted initialization. Reopening a database with a published manifest does
not repeat those ancestor reads; normal path traversal permission is sufficient
for ancestors of an initialized database.

Recovery accepts an incomplete tail only in the newest WAL. Corrupt records return an error; a missing manifest is not reconstructed.

After a write or maintenance I/O error, drop all engine handles and reopen the database. A write that returned an error may still appear after recovery.

A batch is recovered in full or not at all. Recovery validates the whole batch
before applying any operation. An incomplete batch frame at the newest WAL tail
is discarded and truncated; a complete frame with a bad checksum or invalid
batch payload returns a corruption error. A successful batch is synced before
returning, subject to the filesystem's sync guarantees.

## File encoding

Records store a sequence number, a value/deletion tag, and key/value lengths and bytes. Empty values and deleted keys have different tags. Numbers use little-endian encoding; CRC32 checksums detect damaged frames.

WAL files retain the `RKVWAL01` magic and the existing length/header-CRC/payload-CRC
frame layout. Single writes retain their original record payload. Batch payloads
start with a 16-byte envelope: zero u64 sentinel, version u32 (`1`), operation
count u32 (nonzero), then that many ordinary encoded records. Batch records must
have nonzero, consecutive sequence numbers newer than preceding WAL records.
Trailing payload bytes are rejected. The sentinel is an invalid legacy sequence,
so old readers reject a batch frame instead of replaying only part of it.

New readers accept both kinds of WAL frame. Older binaries cannot reopen an
active WAL containing a batch even though the file magic is unchanged. Keep a
closed-database backup before upgrading if rollback is needed. Batch support
does not change SSTable or MANIFEST formats.

Frames are capped at 64 MiB and records at 32 MiB. New SSTables use format `RKVSST03`. A data-frame payload contains encoded records, one little-endian u32 byte offset per record (relative to the payload start), and a final u32 record count. The directory and records share the frame CRC. This adds four bytes per record and four bytes per block. A metadata summary frame is followed by checksummed index pages targeting 1 MiB each, using the version 02 layout. A larger individual index key may exceed that target, but each frame stays within the 64 MiB limit.

The summary stores record count u64, maximum sequence u64, Bloom probe count u32, Bloom byte length u32, Bloom bytes, and index-entry count u64. Each index entry stores first-key length u32, first-key bytes, block offset u64, and framed block length u32. Entries are never split across pages. The 28-byte footer stores magic (8 bytes), metadata offset u64, total framed metadata length u64, and CRC32 of the preceding 24 bytes. Empty SSTables contain only the summary frame and no index pages.

Existing `RKVSST01` and `RKVSST02` SSTables remain readable alongside version `03` files. Flush and compaction write version `03`; WAL and MANIFEST stay at version `01`. Older engine versions cannot read version `03` SSTables, so keep a backup made with all handles closed before upgrading if rollback is needed.

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

A batch's 16-byte envelope plus all records must fit within the 64 MiB frame
payload limit. The batch owns copies of its inputs, and WAL serialization requires
an additional buffer; this limit is not a process-memory budget. A batch can
exceed the memtable target and is kept intact until the next write or explicit
flush rotates it. Each resulting SST block still follows the normal block target.

SizeTiered applies the threshold per size group; Full counts all live files. The policy can change on reopen. [Option validation](../src/engine.rs).

## Limits and statistics

Use a local filesystem that supports file/directory sync and atomic rename. Network filesystems are unsupported, and physical power loss has not been tested. Keep `LOCK` in place while the database is open, and do not use inherited engine handles after `fork()`.

`stats()` reports memory, files, reads, cache hits, and compaction. Byte counters track engine files, not physical device I/O. Cumulative counters reset on reopen; `sst_bytes` shows current live SST bytes. Statistics are not an atomic snapshot.
