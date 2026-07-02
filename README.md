# rust-kv-storage-engine

An embedded LSM-tree key-value storage engine built from scratch in Rust.

**Stack:** Rust · POSIX filesystem · single- or multi-threaded I/O  
**Status:** Pre-Milestone 1 (not yet initialized — `cargo init` pending)

---

## Overview

This project implements a durable, concurrent KV store using the log-structured merge-tree (LSM-tree) pattern. Writes are buffered in memory, persisted to a write-ahead log (WAL), flushed to immutable on-disk tables (SSTables), and periodically compacted to reclaim space and control read amplification.

### High-Level Data Flow

```
put / delete
    │
    ▼
┌─────────┐     fsync      ┌─────────┐
│   WAL   │ ─────────────► │  disk   │
└─────────┘                └─────────┘
    │
    ▼
┌─────────────┐  threshold   ┌──────────────────┐  background   ┌─────────┐
│  MemTable   │ ───────────► │ ImmutableMemTable │ ────────────► │  .sst   │
│  (active)   │              │   (frozen)        │               │  files  │
└─────────────┘              └──────────────────┘               └─────────┘
    ▲                                                                  │
    │                              compaction                          │
    └──────────────── merge / dedup / tombstone GC ◄───────────────────┘

get: MemTable → ImmutableMemTable(s) → SSTables (newest → oldest) → Bloom filter → block cache
```

### Milestone Dependency Order

| # | Milestone | Depends On |
|---|-----------|------------|
| 1 | In-Memory Engine & Synchronization | — |
| 2 | Durability & Crash Recovery (WAL) | 1 |
| 3 | Memory Flushing & SSTable Creation | 1, 2 |
| 4 | Hierarchical Reading & Optimizations | 3 |
| 5 | Background Merging & Compaction | 3, 4 |
| 6 | Validation, Benchmarks & Auditing | 1–5 |

---

## Public API

The engine exposes exactly three operations over arbitrary byte-slice keys and values:

| Method | Signature | Behavior |
|--------|-----------|----------|
| `put` | `(key: &[u8], value: &[u8]) -> Result<()>` | Upsert a key-value pair |
| `get` | `(key: &[u8]) -> Result<Option<Bytes>>` | Return the newest value, or `None` if absent or deleted |
| `delete` | `(key: &[u8]) -> Result<()>` | Logically remove a key (tombstone) |

**Planned crate dependencies:** `bytes`, `parking_lot`, `crossbeam`  
**Later milestones add:** `crc32fast` (or equivalent), `criterion` (dev/bench)

---

## Cross-Cutting Design Decisions

These conventions apply across all milestones and should be decided early:

| Decision | Choice |
|----------|--------|
| Key ordering | Lexicographic byte order (`[u8]` comparison) |
| Conflict resolution | **Newest wins** — each mutation carries a monotonically increasing **sequence number**; higher sequence overrides lower across all layers |
| Tombstone semantics | Deletes write a tombstone marker; tombstones shadow older values until compaction drops them at the lowest tier |
| Concurrency model | `Arc<RwLock<MemTable>>` — multiple concurrent readers; writers block readers during `put`/`delete` |
| Durability guarantee | WAL record is `fsync`'d **before** MemTable is updated (write-ahead, not write-behind) |
| Value type | `bytes::Bytes` internally to avoid unnecessary cloning |

---

## On-Disk Layout

### Directory Structure (planned)

```
<data_dir>/
├── current.wal          # active write-ahead log
├── MANIFEST             # metadata: list of SST files, sequence counter, compaction state
└── sst/
    ├── 000001.sst
    ├── 000002.sst
    └── ...
```

### WAL Record Format

```
[CRC32: 4B][Key Len: 4B][Val Len: 4B][Key Bytes][Value Bytes]
```

- Deletion: special value-length sentinel (`-1` as `i32`, or explicit enum tag — pick one and document)
- Recovery: scan from byte 0, verify CRC32 per record, replay valid entries into MemTable

### SSTable File Format

```
┌──────────────────────────────────────┐
│  Data Blocks (sorted KV records)     │
├──────────────────────────────────────┤
│  Bloom Filter                        │
├──────────────────────────────────────┤
│  Index Block (key → block offset)    │
└──────────────────────────────────────┘
```

- Keys sorted lexicographically within blocks and across the file
- Tombstones persisted as first-class records inside data blocks
- Bloom filter checked before loading data blocks on `get`

---

## Configuration & Tunables

| Parameter | Default (suggested) | Used In |
|-----------|-------------------|---------|
| `memtable_size_limit` | 4 MB | Milestone 3 — triggers freeze + flush |
| `block_cache_capacity` | TBD | Milestone 4 — LRU cache size |
| `bloom_filter_bits_per_key` | TBD | Milestone 4 — false-positive rate trade-off |
| `compaction_file_threshold` | TBD | Milestone 5 — files per level before merge |
| `compaction_strategy` | Size-tiered or leveled | Milestone 5 — pick one |

---

## Planned Module Structure

```
src/
├── lib.rs              # public API surface
├── engine.rs           # orchestrates all subsystems
├── memtable.rs         # active + immutable in-memory tables
├── wal/
│   ├── mod.rs
│   ├── writer.rs
│   └── reader.rs
├── sstable/
│   ├── mod.rs
│   ├── writer.rs
│   ├── reader.rs
│   └── bloom.rs
├── cache/
│   └── block_cache.rs  # LRU block cache
├── compaction/
│   ├── mod.rs
│   └── merge.rs        # multi-way merge iterator
└── manifest.rs         # on-disk metadata tracking
```

---

## Glossary

| Term | Definition |
|------|------------|
| **MemTable** | Mutable in-memory sorted buffer for recent writes |
| **ImmutableMemTable** | Frozen MemTable awaiting flush to disk |
| **WAL** | Append-only log ensuring durability before memory update |
| **SSTable** | Immutable, sorted on-disk key-value file |
| **Tombstone** | Delete marker that logically removes a key |
| **Bloom filter** | Probabilistic structure to skip SSTables/blocks on negative lookups |
| **Compaction** | Background merge of SSTables to deduplicate and garbage-collect tombstones |
| **Manifest** | Authoritative list of live SST files and engine metadata |

---

## Non-Goals (Out of Scope)

- Distributed replication / consensus
- Multi-key transactions or ACID isolation levels
- Range scans / iterators (may be a future stretch goal)
- Compression (Snappy/LZ4) — not in initial milestones
- Column families or multiple namespaces

---

## Development

```bash
cargo build
cargo test
cargo bench          # after Milestone 6 (criterion integration)
```

---

## Milestones

Each milestone follows the same template: **Objective → Steps → Definition of Done → Tests**.

---

### Milestone 1 — In-Memory Engine & Synchronization

**Objective:** Buffer writes in memory, define the public API, and handle concurrent access safely.

| Step | Task |
|------|------|
| 1.1 | `cargo init --lib`; add deps (`bytes`, `parking_lot`, `crossbeam`); define `put` / `get` / `delete` |
| 1.2 | Implement `MemTable` with ordered concurrent skip list or synchronized `BTreeMap`; store entries as `bytes::Bytes` |
| 1.3 | Wrap state in `Arc<RwLock<MemTable>>`; readers concurrent, writers exclusive |

**Definition of Done**
- [ ] `cargo build` succeeds
- [ ] Concurrent `put` / `get` without deadlocks or data races
- [ ] `get` returns the value from the most recent `put` for a key

**Tests**
- Unit: put-then-get, get on missing key, delete-then-get → `None`
- Stress: 10 writer threads + 10 reader threads on unique keys; no corruption under `cargo test`

---

### Milestone 2 — Durability & Crash Recovery (WAL)

**Objective:** Persist every mutation to disk before updating memory so the store survives crashes.

| Step | Task |
|------|------|
| 2.1 | Define WAL binary format (see [On-Disk Layout](#on-disk-layout)) |
| 2.2 | `WalWriter` — sequential append + `sync_all()` before MemTable update |
| 2.3 | `WalReader` — auto-replay on engine init; CRC32 validation per record |

**Definition of Done**
- [ ] Every mutation is WAL-persisted before returning success
- [ ] Engine fully reconstructs state from `.wal` after process crash

**Tests**
- Crash recovery: put 1,000 keys → kill instance → reopen same path → all 1,000 keys readable
- Corruption: inject bad bytes mid-log → CRC failure → controlled error, no panic or polluted state

---

### Milestone 3 — Memory Flushing & SSTable Creation

**Objective:** Flush saturated MemTables to immutable on-disk SSTables to bound memory growth.

| Step | Task |
|------|------|
| 3.1 | Define SSTable binary spec (data blocks + index block at tail) |
| 3.2 | On size threshold, freeze MemTable → spawn new active MemTable; background worker writes `.sst` |
| 3.3 | Persist tombstones in flushed SSTables |

**Definition of Done**
- [ ] MemTable → immutable transition without rejecting or stalling writes
- [ ] Flushed `.sst` files contain fully sorted keys with valid index

**Tests**
- Flush threshold: set limit to 64 KB, trigger multiple flushes, verify `.sst` files on disk and RAM plateaus
- Logical delete: insert → flush → delete → flush → `get` returns `None`

---

### Milestone 4 — Hierarchical Reading & Performance Optimizations

**Objective:** Efficient `get` path spanning memory and disk with minimal I/O.

| Step | Task |
|------|------|
| 4.1 | Search router: active MemTable → immutable MemTable(s) → SSTables newest-first via index blocks |
| 4.2 | Per-SSTable Bloom filter; skip data-block reads on negative lookup |
| 4.3 | Thread-safe LRU `BlockCache` for parsed SSTable blocks |

**Definition of Done**
- [ ] `get` resolves newest record or tombstone across all layers
- [ ] Non-existent key lookups avoid physical reads in most cases (Bloom filter)

**Tests**
- Multi-generation: Key-A in SST → updated in newer SST → updated in MemTable → `get` returns MemTable value; delete in MemTable → `get` returns `None` despite older SST values
- I/O elimination: 5,000 random non-existent keys; instrument fd reads; verify Bloom filter minimizes disk I/O

---

### Milestone 5 — Background Merging & Compaction

**Objective:** Merge fragmented SSTables, deduplicate, and garbage-collect tombstones in the background.

| Step | Task |
|------|------|
| 5.1 | Compaction manager — monitor SST count per layer; trigger when threshold exceeded |
| 5.2 | Multi-way merge iterator over sorted SSTables; keep newest sequence; drop tombstones at lowest tier |
| 5.3 | Atomic manifest swap to new merged SST; delete deprecated files |

**Definition of Done**
- [ ] Compaction runs in background without blocking foreground reads/writes
- [ ] Stale duplicates and redundant tombstones purged after compaction

**Tests**
- Continuous overwrite: 100 keys × 50,000 overwrites → many small SSTs → compaction reduces file count to one record per key

---

### Milestone 6 — Validation, Benchmarks & Performance Auditing

**Objective:** Prove correctness under extreme load and measure throughput/latency.

| Step | Task |
|------|------|
| 6.1 | Differential fuzz harness — random `put`/`get`/`delete` interleaves vs `HashMap` reference |
| 6.2 | `criterion` benchmarks — sequential writes, random reads, mixed workloads; report ops/sec and p50/p99 latency |

**Definition of Done**
- [ ] Extended concurrent fuzz tests pass with zero mismatches
- [ ] Reproducible performance matrix (p50, p99) across load profiles

**Tests**
- Final acceptance: 100,000 fuzz iterations with active compaction worker; zero inconsistencies
