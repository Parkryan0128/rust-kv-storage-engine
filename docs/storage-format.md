# Storage format

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

1. Select input SSTs under the maintenance lock; reads keep file handles alive.
2. Stream-merge records, retaining the highest sequence per key. Drop tombstones only when every live SST is included.
3. Sync/rename the new SST and publish a manifest replacing only the selected inputs; unselected SSTs remain live.
4. Publish the new table set in memory.
5. Unlink old SSTs and sync `sst/`. POSIX open file handles keep old read snapshots valid.

If interrupted before manifest publication, old files and WALs remain sufficient. If interrupted after publication, the new SSTs were already synced. Startup removes unreferenced SSTs, checkpointed WALs, and temporary SST/WAL files. It never rebuilds a missing manifest from guessed file contents.

Manifest writes, flushes, and compactions are serialized. Foreground writes use a separate mutex; they do not hold the state write lock across WAL I/O. Eligible compaction jobs run before the next flush. Size-tiered jobs select the oldest IDs in the smallest ready power-of-two file-size bucket; each job takes exactly `compaction_file_threshold` files. Full mode selects all live SSTs at that threshold. The frozen-table limit applies backpressure when the worker lags.

## Recovery notes

- An error during sync or manifest publication can leave the operation on disk. Failed writes may appear after recovery.
- Write or maintenance I/O errors halt the engine. Drop all handles and reopen it to recover.
- Read corruption returns an error. Recovery does not repair checksum-corrupt data.
- File and directory sync, and atomic rename, must be supported by the local filesystem. Network filesystems are unsupported; physical power loss has not been tested.
- Keep `LOCK` in place while the database is open. Do not use inherited engine handles after `fork()`.
- Format version `01` has no migration path yet.

## Runtime options

Pass an `Options` value to `Engine::open_with_options()`.

| Option | Default | Use |
|---|---:|---|
| `memtable_size_limit` | 4 MiB | Rotate when accounted memory or WAL bytes reach this size |
| `max_immutable_memtables` | 2 | Pause writers when the frozen queue is full |
| `block_size` | 16 KiB | Target block size; one larger record is allowed |
| `block_cache_capacity` | 8 MiB | Cache budget; zero disables it |
| `bloom_filter_bits_per_key` | 10 | Filter bits per key; range 1–30 |
| `compaction_style` | `SizeTiered` | Similar-size merges; `Full` selects the original all-file policy |
| `compaction_file_threshold` | 4 | Files per bucket before merging (total files for Full); minimum 2 |
| `max_key_size` | 1 MiB | Maximum key length |
| `max_value_size` | 16 MiB | Maximum value length |

Key/value limits plus the 17-byte record header must fit within 32 MiB. Block targets range from 64 bytes to 32 MiB. Bloom filters are capped at 8 MiB per table; metadata frames at 64 MiB.

Memory accounting allows one oversized record per memtable. Indexes, filters, merge buffers, allocator overhead, and values held by callers are separate from the memtable/cache budgets.

`stats()` reports sequence, memory usage, frozen-table count, SST files/records, block reads, cache hits, and Bloom negatives. SST record counts include duplicates and tombstones before compaction. Block reads count engine calls, including reads served by the OS cache.

## Compaction policy and metrics

Size tiers use `floor(log2(file_bytes))`: files in one bucket differ in size by less than a factor of two. After scheduled work settles, every bucket contains fewer than the threshold number of files. This bounds file count per bucket, not the total database size or individual job bytes. Adjacent bucket boundaries can separate nearly equal files. The policy favors small ready buckets and does not implement leveled key-range partitioning or an age-based cleanup policy.

`flush()` persists writes before its writer barrier and completes currently eligible compactions before returning. It may therefore take longer than an SST write alone. `compact()` additionally requests one full merge, including when a single SST still contains tombstones. Partial compaction conservatively keeps tombstones even when an individual key happens to have no older version elsewhere. Use a full merge to reclaim cold obsolete versions that do not reach a size-tier threshold.

The manifest/SST/WAL formats are unchanged. Selection uses existing file lengths and does not require persistent level metadata. The policy can change on reopen. Existing Rust callers that enumerate every `Options` field must add `compaction_style` or use `..Options::default()`.

Additional stats: `sst_bytes` is the sum of live SST file lengths. `flush_bytes`, `compaction_input_bytes`, `compaction_output_bytes`, and `compactions` count successfully published operations since open. Input bytes sum selected file lengths; output bytes include framing, indexes, and Bloom filters. These are logical file accounting, not device I/O measurements; WAL, manifest, filesystem amplification, and failed/orphan writes are excluded. Counters reset on reopen and are not an atomic multi-field snapshot.
