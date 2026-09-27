# Storage format and recovery protocol

All numeric fields use little-endian encoding. Keys are sorted lexicographically by unsigned byte value. File names are canonical 20-digit nonzero decimal IDs; one monotonic allocator serves both WALs and SSTables.

## Shared frame

| Field | Bytes |
|---|---:|
| Payload length | 4 |
| CRC32 of the four length bytes | 4 |
| CRC32 of the payload | 4 |
| Payload | declared length |

Length checksums prevent a corrupt length from being mistaken for an incomplete record. Frames are capped at 64 MiB before allocation. Only the **last WAL** may have a physically incomplete final frame. Complete checksum failures, malformed records, and incomplete frames in earlier generations are errors. No later frames are skipped after an error.

## Record payload

| Field | Bytes |
|---|---:|
| Sequence (nonzero) | 8 |
| Tag: 0 tombstone, 1 value | 1 |
| Key length | 4 |
| Value length (zero for tombstones) | 4 |
| Key | key length |
| Value | value length |

A present empty value has tag 1 and length 0. A tombstone has tag 0. Encoded records are capped at 32 MiB. Sequence numbers must strictly increase during WAL recovery, beginning after the manifest's durable sequence high-water mark.

## WAL

Eight-byte magic `RKVWAL01`, then one frame per mutation. `sync_all()` follows every append; there is no async/group-commit mode. A new generation's header and its directory entry are synced before it is used. A highest-generation header shorter than eight bytes is an interrupted empty generation, since no write could have been acknowledged in it; it is repaired before use.

Rotation creates and syncs the next generation before publishing the old memtable as immutable. Replay handles each retained generation separately, preserving the oldest-first flush order. The active generation is the newest WAL. IDs at or below the manifest checkpoint are obsolete even if cleanup was interrupted.

## SSTable

1. Magic `RKVSST01` (8 bytes).
2. Data-block frames, each containing concatenated sorted records.
3. One metadata frame.
4. Footer (24 bytes).

Metadata: record count u64, maximum sequence u64, Bloom probe count u32, Bloom byte length u32, Bloom bytes, index-entry count u32, then index entries. Each entry is first-key length u32, first-key bytes, block offset u64, framed block length u32. Offsets must be contiguous; index keys and decoded block keys strictly increase. An empty run has no data blocks, count 0, max sequence 0, and a valid empty filter/index.

Footer: magic (8), metadata offset u64, metadata framed length u32, CRC32 of the preceding 20 footer bytes. The exact file length is checked. Metadata checksums are checked on open; data-block checksums and record ordering are checked on reads/merge.

Bloom hashing is fixed in `bloom.rs`, independent of Rust's randomized map hashing. It starts with FNV-1a, applies a fixed mixing step, and uses double hashing. Changing its algorithm requires a format version change. Tombstone keys are included in the filter.

## Manifest

Magic `RKVMAN01`, then a single frame containing WAL checkpoint u64, maximum durable sequence u64, live-SST count u32, and that many SST IDs u64. Duplicate/zero IDs and trailing bytes are rejected. The high-water sequence survives a compaction that removes every record.

## Publication ordering

Flush:

1. Keep the frozen memtable readable and retain its WAL.
2. Write `sst/ID.tmp`, sync its file, rename to `ID.sst`, sync `sst/`.
3. Write `MANIFEST.tmp` including the new SST, updated WAL checkpoint, and sequence high-water mark.
4. Sync the manifest file, rename it over `MANIFEST`, sync the database directory.
5. Under the state lock, publish the SST and remove the frozen memtable together.
6. Delete the obsolete WAL and sync `wal/`.

Compaction:

1. Snapshot all live SSTs under the maintenance lock; reads keep file handles alive.
2. Stream-merge records; retain the highest sequence per key and drop tombstones.
3. Sync/rename the new SST and publish a manifest replacing the input list.
4. Publish the new table set in memory.
5. Unlink old SSTs and sync `sst/`. POSIX open file handles keep old read snapshots valid.

If interrupted before manifest publication, old files and WALs remain sufficient. If interrupted after publication, the new SSTs were already synced. Startup removes unreferenced SSTs, checkpointed WALs, and temporary SST/WAL files. It never rebuilds a missing manifest from guessed file contents.

Manifest writes, flushes, and compactions are serialized. Foreground writes use a separate mutex; they do not hold the state write lock across WAL I/O. Full-run compaction is prioritized at its threshold so a continuous flush workload cannot starve merging. The frozen-table limit applies backpressure when the worker lags.

## Failure semantics

An error around sync or manifest publication can occur after the operation is durable. The engine therefore halts after write/maintenance I/O failure and requires recovery, rather than attempting to roll back an uncertain filesystem outcome. Automatic recovery does not repair checksum-corrupt committed data; keep backups for media failure. Tests distinguish process crashes from power-loss guarantees of the underlying filesystem/hardware.
