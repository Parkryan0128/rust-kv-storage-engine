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

On Linux, WAL writes reserve physical disk space in 256 KiB chunks using
`fallocate(KEEP_SIZE)`. This reduces repeated allocation during appends while
preserving the exact file length, bytes and recovery format. Reservation is lazy,
so an empty WAL reserves no extra chunk. Unused reserved space is less than
256 KiB per WAL (apart from filesystem allocation granularity), and disappears
when the obsolete WAL is deleted after flush. This is disk space, not RAM.
If reservation fails or is unsupported, that WAL falls back to ordinary appends;
actual write and sync failures still fail the write. Other platforms use ordinary
appends. Every acknowledged commit still waits for `sync_all()`.

WAL recovery uses 64 KiB buffered sequential input and reuses a payload buffer.
Payload storage above 64 KiB is released after replay. Only a fully validated
frame advances the logical recovery offset; buffered read-ahead is discarded
before repairing a newest torn tail and positioning the next append.
Complete checksum errors and torn older WALs remain fatal.

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
Small returned values are copied out and cannot retain a whole block. A value
of at least 64 KiB may share a validated immutable block if it is the only
record and the allocated frame/index plus block metadata exceed its length by
at most 256 bytes (excluding allocator, Arc and Bytes bookkeeping). Other
values are copied. Shared values survive cache eviction and engine shutdown
without retaining files or the database lock. Retaining a short slice of a
large value can retain that large value, as with other shared `Bytes`; converting
a shared value into mutable storage requires a copy. Caller-held values are
outside the cache capacity. The bytes dependency requires at least 1.10.1,
including the [owned-to-Vec release fix](https://github.com/tokio-rs/bytes/pull/773);
CI checks that minimum in addition to the locked version. Cache accounting includes
the allocated frame/index capacities and an allowance for cache bookkeeping;
it is not a hard bound on process RSS. Table inspection reads one encoded block
at a time and copies only the preview's first 128 bytes of each key/value.

Point reads share an immutable table-list snapshot. The block cache tracks LRU
order through indexed links rather than a tree. Up to eight independently locked
shards share the configured entry budget; both table and block identity select a
shard. One mixed internal identity hash serves both routing and map lookup;
collisions still compare the full table/block identity. Cumulative hit counts
live under each shard's existing lock and are summed by `stats()`. Retained
hashes are included in the per-entry bookkeeping allowance. Small caches and
large configured block targets use fewer shards. The
default 8 MiB cache starts with eight 1 MiB shards. Ordinary blocks retain those
independent LRUs; skew can still reduce effective capacity. A frame exceeding
the original shard budget uses a shared overflow LRU. Space is reserved only
after successful reading and validation, using actual allocated entry charges.
At least one quarter of the total budget remains available to ordinary shards;
a large entry exceeding the remaining budget bypasses caching. No speculative
reservation is left behind by failed reads. Ordinary cache hits use precomputed
routing limits and do not perform reservation or retirement checks.
Every 32 ordinary admissions under reduced capacity, the pressured shard checks
whether overflow activity has changed since its previous check. If not, one
least-recently-used oversized entry is evicted and its budget returned. Retirement
also returns unused space. This trades small-block capacity and large-read
synchronization for oversized caching while allowing small-only phases to recover.
Pinned reader blocks can outlive eviction and are outside resident-cache charges.
A disabled cache bypasses cache locks and admission entirely.
When a miss requires eviction,
an unshared victim's frame and offset buffers can be reused for the incoming
block; outstanding readers retain their original immutable block. Frames that
cannot fit the budget do not evict entries for recycling: legacy blocks use a
conservative offset-array bound and versions 03 and 04 include their index in the frame.
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

`Engine::scan()` clones handles to overlapping active and frozen memory page groups
while holding the state read lock. It captures the SST list under the same lock,
so it observes one consistent state, including all or none of a batch. The ordered
memtable uses compact monotone key-prefix indexes with full-byte comparison
for equal prefixes, and an append path for increasing keys. It contains
copy-on-write pages, each limited to 256 records and 64 KiB of
key bytes, except a single larger key. At most 64 pages share a copy-on-write
directory group; a full group splits near its midpoint. Snapshot creation copies
group handles, without touching each page reference counter. Iteration copies
only the consumed key and shares immutable value bytes. Groups remain valid
across writes, flushes and compactions and are released as the cursor finishes
them. Nonoverlapping groups are not retained; boundary groups can retain pages
outside the requested range. Physical removals discard empty pages/groups and merge adjacent sparse pages
and groups when their combined occupancy fits the configured merge thresholds.
Logical deletes remain versioned tombstones until safe to discard.
After creation, the owned iterator requires no engine state lock for I/O.
Later writes or maintenance failures do not change an already captured snapshot.

Version 04 SST metadata persists the maximum key under the summary frame CRC.
Reopened scans and point reads can exclude files entirely below the requested
key without loading their final data blocks. This adds four length bytes plus
one maximum key per SST; the metadata key is held in memory after open. Empty
SSTs store a zero length and have no key. An actual empty key remains valid in a
nonempty SST. Versions 01–03 retain the lazy tail-read fallback. Metadata bounds
are checked on open; reading the last block also checks its actual maximum.

Each SST iterator seeks through the index to the lower-bound block, then searches
within that block. It streams forward up to the upper bound, retaining one encoded
block per source. A single source is consumed directly, skipping tombstones before
copying keys. Merges with at most eight sources and at least one SST compare
borrowed heads; memory-only and larger merges use a min-heap of candidate keys.
All paths choose the highest sequence number and omit tombstones. Scans use the existing validated
block decoder for all supported SST versions and do not change any file format.
They bypass the point-read cache; disk reads still increment `stats().block_reads`.
Errors are iterator items and terminate the scan; earlier rows may already have
been returned. Exhausted or failed iterators always return `None` thereafter.

Snapshot creation still takes time and space proportional to the number of
overlapping groups, not individual pages or keys. Group sizes are bounded, so
this is a smaller linear cost rather than a constant-time snapshot. A write to a
shared group copies at most 64 page handles and their prefix index, then a changed
shared page copies its key/value handles and prefix index. Immutable bytes remain
shared; no write clones the entire memtable. Retained snapshots can increase
write latency and keep old values alive. Disk buffering grows with SST count and
block sizes; it is not a fixed process-memory limit. Completed sources release
their SST handles early. The engine handle remains until drop, error or exhaustion. Compaction can unlink old files, but those handles delay reclaiming
their space; `sst_bytes` counts current live tables, not unlinked pinned files.
Drop long-lived or partially consumed scans before reopening the database.

## Combining files

Compaction merges SSTables and keeps the newest record for each key.

Flush writes borrow keys and records directly from the frozen memtable. The SST
writer accepts both borrowed flush input and owned compaction output, preserving
the same ordering checks, encoding and checksums without temporary flush copies.

- **SizeTiered** (default) merges similarly sized files, avoiding repeated rewrites of large files.
- **Full** merges all live SSTables when the file-count threshold is reached.

Partial merges keep tombstones so old values in other files stay deleted. A full merge can remove both the old value and its tombstone.

`flush()` saves earlier writes and waits for eligible compactions. `compact()` also requests a full merge.

Explicit `compact()` flushes the writes before its writer barrier and requests one
full merge, without first draining redundant automatic merges. An unchanged
single SST already produced by a full merge during this open is reused, keeping
its file and cache entries. This proof is cleared on reopen; the first explicit
compaction then still rewrites the SST with the current options and format.
New SST publication invalidates the proof.


Each merge retains one validated encoded block per input and compares borrowed
keys and sequence numbers. Only the winning record is copied for output;
superseded versions and safely discarded tombstones need no owned copies.
Input frame buffers are reused, with oversized buffers released on smaller blocks.
Checksums and record validation still cover discarded versions.

Each merge selects up to sixteen input cursors under a 512 KiB input-buffer
estimate, accounting for the largest encoded block in each file, buffer reuse
slack and legacy offset arrays. At least two inputs are allowed even when their
records exceed that budget, so every merge can make progress. This is not a hard
RSS bound: indexes, filters, output buffers and concurrent operations are separate.
Requests that exceed either limit proceed through
manifest-published partial merges, retaining tombstones until it is safe to remove
them. Recovery can resume from any published round. Small-block inputs can merge
together without an intermediate file; large-record inputs use fewer cursors.
`stats().max_compaction_inputs` reports the observed maximum since open.
Obsolete-cache cleanup runs after releasing the engine state write lock.

SST publication coalesces sequential block and metadata writes through a fixed
256 KiB output buffer without changing the file format or block target. The
buffer is explicitly flushed before Bloom-filter readback and before file sync
and rename; a flush error prevents publication. Oversized writes can bypass the
buffer rather than growing it.

The background worker releases the maintenance lock fairly between jobs so a
manual flush does not wait for a continuously replenished background queue.
When compaction discards enough records to overallocate the Bloom filter by more
than roughly two times, the writer rebuilds it at the final record count.
Compactions retain at most 64 KiB of base key hashes to rebuild the identical
filter without rereading values. Above that cap the hash buffer is released and
the writer falls back to reading output blocks with bounded scratch space.
Ordinary memtable flushes do not retain hashes because their count is exact.
Empty output always uses the minimum filter size. Neither path changes the file
format, Bloom probes, or publication ordering.

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

Frames are capped at 64 MiB and records at 32 MiB. New SSTables use format `RKVSST04`. A data-frame payload contains encoded records, one little-endian u32 byte offset per record (relative to the payload start), and a final u32 record count. The directory and records share the frame CRC. This adds four bytes per record and four bytes per block. A metadata summary frame is followed by checksummed index pages targeting 1 MiB each, using the version 02 layout. A larger individual index key may exceed that target, but each frame stays within the 64 MiB limit.

The summary stores record count u64, maximum sequence u64, Bloom probe count u32, Bloom byte length u32, Bloom bytes, and index-entry count u64, followed in version 04 by maximum-key length u32 and maximum-key bytes (zero length for an empty SST). Versions 02/03 end their summary after the index-entry count. Each index entry stores first-key length u32, first-key bytes, block offset u64, and framed block length u32. Entries are never split across pages. The 28-byte footer stores magic (8 bytes), metadata offset u64, total framed metadata length u64, and CRC32 of the preceding 24 bytes. Empty SSTables contain only the summary frame and no index pages.

Existing `RKVSST01`, `RKVSST02`, and `RKVSST03` SSTables remain readable alongside version `04` files. Flush and compaction write version `04`; WAL and MANIFEST stay at version `01`. Older engine versions cannot read version `04` SSTables, so keep a backup made with all handles closed before upgrading if rollback is needed.

Exact layouts: [records](../src/codec.rs), [WAL](../src/wal.rs), [SSTables](../src/sstable.rs), [manifest](../src/manifest.rs).

## Configuration

Pass an `Options` value to `Engine::open_with_options()`.

| Option | Default | Meaning |
|---|---:|---|
| `memtable_size_limit` | 4 MiB | Memory or WAL size that triggers a new buffer |
| `max_immutable_memtables` | 2 | Maximum waiting buffers; a writer pauses only when rotation needs another slot |
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


## Optional prefix blocks (version 05)

Tables containing a prefix-encoded block use `RKVSST05`; tables without one
retain the exact version 04 layout. Versions 01–04 remain readable. Older
engines reject version 05 by its magic, so opening a newly rewritten prefix
table requires a reader supporting version 05.

The version 05 summary extends version 04 with a u32 maximum expanded framed
block length, bounded by 64 MiB plus the 12-byte frame header. This also bounds
compaction input memory. Version 05 may mix ordinary indexed blocks and prefix
blocks. A set high bit in the final u32 record count identifies a prefix block;
ordinary blocks retain their existing count and bytes.

A prefix block payload contains expanded payload length u32, common-prefix
length u32, prefix bytes, records with suffix-only keys using the existing
17-byte record header, one u32 offset per record relative to the suffix-record
region, and the tagged record count. The entire encoded payload is checksummed.
The reader validates expansion length, directory, full record limits and CRC,
expands once, then applies normal ordering, sequence and table-bound checks.
Cached reads use the existing full-key block representation.

The writer uses this representation only with at least two records, a first
key of at least 32 bytes, a common prefix of at least 16 bytes, and at least
12.5% payload savings. Prefix sharing reduces storage and write traffic; it
does add reconstruction work on a cache miss. It is not general value
compression and does not change WAL durability or logical block boundaries.
