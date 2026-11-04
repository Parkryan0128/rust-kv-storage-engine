# Memory and snapshots

[Back to README](../README.md) · [Scan usage](usage.md#range-scans)

## Snapshot storage

Snapshot creation shares overlapping groups of active/frozen memory pages and
captures SST handles under the state lock. One group contains at most 64 pages;
creation clones group handles instead of touching every page or matching key.
Keys are copied when consumed; completed groups and sources are released. A
boundary slice of at most four pages retains only those pages; larger boundary
groups can still retain pages outside the requested range. Shared-page updates
copy a bounded delta (16–32 records, depending on page occupancy), sharing unchanged keys and values.
Once a base is exclusively owned again, the next update folds pending deltas in place and releases superseded payloads. Sorted appends fill pages before allocating the next page; middle inserts retain balanced splitting.
Shared delta folds copy value handles while keeping the ordered keys and routing
prefixes shared. Structural edits also detach the key arrays, so retaining a snapshot
can still increase write latency and memory use. Pages are bounded by 256 records and 64 KiB of keys,
except that one larger key occupies its own page.
The memory cursor checks whether each whole page is within the range. On such
pages, live-only traversal skips tombstones without loading their keys or
repeating the bound comparison. Partial boundary pages keep per-row checks so
short scans do not pay for an extra upper-bound search.
Iteration buffers one block and candidate record per SST, rather than all disk
results. `.take(n)` limits consumption; `.collect()` allocates the full result.
Drop or exhaust scans promptly: they retain the database lock and old SST handles,
which can delay reclaiming disk space after compaction. They also keep selected
memory values alive. Transactions and replication are not implemented.

## Statistics

`Stats::*_memtable_bytes` are logical rotation charges (key/value lengths plus
48 bytes per entry), not RAM usage. `*_memtable_allocated_bytes` estimate reachable
page/directory capacities and base/delta payload lengths. They exclude allocator
headers, spare `Bytes` backing capacity, and old pages held only by scans; shared
payload can also be counted more than once. Use process RSS/peak RSS for total
memory comparisons. Use `Engine::activity_stats()` for frequent counter polling; it skips page allocation estimates. `stats()` retains the detailed estimates for compatibility. Both release the state lock before sampling cache shards, so cache and state counters are not one atomic snapshot. Allocation estimates still visit pages. The rotation policy continues to use logical charges.

See [storage design](storage-format.md) for cache accounting and value ownership.
