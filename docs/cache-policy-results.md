# Owned-value reads and RocksDB cache policies

This report uses the corrected `fixed-v1` measurement paths: every participant opens the same database pathname. The earlier native-path report remains in Git history. See [pathname control](measurement-paths.md).

Measured on 2026-10-29 in [run 40885275616](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40885275616), with engine revision `543ecbe3a4311960abfc9cce58b80e6d0742dbc9` and original comparison baseline `c0299ec14e60a249fb9628f1888c5740c655432b` (which predates this optimization round). This is a targeted diagnostic, not a claim that every database workload is faster.

The same host (AMD EPYC 9V45, Linux x86_64), 8 MiB total cache capacity, uncompressed data, independently owned Get results and rotated 12-trial order apply to every backend. RocksDB is 11.8.1 through rust-rocksdb 0.25.0. The default LRU is compared with a single-shard LRU and HyperClock with automatic entry-size estimation. Only the RocksDB cache policy changes between these cases. Engine repetitions across policies serve as additional controls.

## Repeated reads

Ratios below are the median of per-trial RocksDB time / engine time; values above one favor this engine. They are not ratios of separately pooled medians. Every first/open/repeat stage, absolute duration and paired bootstrap 95% interval is in the [39-condition CSV](cache-policy-results.csv).

| Workload | Default LRU | Single-shard LRU | HyperClock |
|---|---:|---:|---:|
| 128 B values plus one 2 MiB value; full bytes checked | 4.88x | 2.11x | 1.94x |
| 128 B values plus one 4 MiB value; full bytes checked | 3.96x | 1.85x | 1.83x |
| 20,000 small keys, 4 KiB blocks | 3.91x | 3.88x | 3.76x |
| 20,000 small keys, 16 KiB blocks | 4.05x | 4.08x | 3.93x |
| 2 MiB Get/return/drop only; payload consumption excluded | 980.30x | 288.61x | 283.04x |

The mixed-size cases issue 4,096 reads, every sixteenth read retrieving the large value. All returned bytes are compared with expected contents inside timing. These are read-only traces, not concurrent read/write workloads. With tuned RocksDB caching, the engine is about 1.83–2.11x faster in these two traces; the default-policy 4–5x ratios also reflect cache admission.

The isolated 2 MiB row times returning and dropping independently owned values. The engine can share a validated, immutable single-record block; RocksDB’s owned Get copies the value. The roughly 283–289x tuned ratios therefore describe this narrow API/lifetime operation, not processing 2 MiB of application data or general database throughput. Full seeded payload validation is performed outside that timed pass. A DB-borrowed pinned value has a different lifetime contract and is not substituted for the independently owned API.

## Why cache policy matters

For repeated isolated 2 MiB reads, RocksDB reported 87 cache bytes with default LRU, compared with 4,194,833 bytes for single-shard LRU and 4,198,668 for HyperClock. The latter two actually admitted the large blocks. All three had the same 8 MiB configured capacity. Reporting only the default policy would overstate the implementation advantage. The engine reported 4,194,730 cache bytes.

Sharing applies only to one validated value of at least 64 KiB whose retained block/index allocation and ReadBlock metadata exceed the value by at most 256 bytes. Arc/Bytes ownership and allocator bookkeeping are additional. Caller-held values are outside the cache budget; retaining a small slice can retain its large backing allocation. Multi-record blocks and excessive spare capacity use copies. Returned values retain no engine or file handle. See [storage ownership](storage-format.md) and the lifetime/eviction tests.

These measurements use warm OS cache and data that fits host RAM. They do not establish cold-device throughput, sustained concurrent performance, or behavior during physical power loss. WAL sync and CRC validation were not weakened. The downloadable artifact is `recovery-comparison`, SHA-256 `81775ac4192e9763eb735d2273f282c1ade04cecff114398ec65c62b68ec7b3e`; it contains 1,404 cache-policy records plus 1,512 focused read/recovery guard records. The 39 cache-policy conditions are reported separately from the canonical full comparison.
