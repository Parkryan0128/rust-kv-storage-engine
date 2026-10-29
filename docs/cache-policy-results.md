# Owned-value reads and RocksDB cache policies

Measured on 2026-10-29 in [run 40875797178](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40875797178), with engine revision `3b1296e9a5473e4bdfa0f2e314233ccc13c17df0` and pre-sharing baseline `1c9a3383a9e35bc7719344e85da22cbf0d384341`. This is a targeted diagnostic, not a claim that every database workload is faster.

The same host (AMD EPYC 7763, Linux x86_64), 8 MiB total cache capacity, uncompressed data, independently owned Get results and rotated 12-trial order apply to every backend. RocksDB is 11.8.1 through rust-rocksdb 0.25.0. The default LRU is compared with a single-shard LRU and HyperClock with automatic entry-size estimation. Only the RocksDB cache policy changes between these cases. Engine repetitions across policies serve as additional controls.

## Repeated reads

Ratios below are the median of per-trial RocksDB time / engine time; values above one favor this engine. They are not ratios of separately pooled medians. Every first/open/repeat stage, absolute duration and paired bootstrap 95% interval is in the [39-condition CSV](cache-policy-results.csv).

| Workload | Default LRU | Single-shard LRU | HyperClock |
|---|---:|---:|---:|
| 128 B values plus one 2 MiB value; full bytes checked | 4.96x | 2.17x | 2.20x |
| 128 B values plus one 4 MiB value; full bytes checked | 4.11x | 2.03x | 1.99x |
| 20,000 small keys, 4 KiB blocks | 4.12x | 4.10x | 4.00x |
| 20,000 small keys, 16 KiB blocks | 4.40x | 4.41x | 4.23x |
| 2 MiB Get/return/drop only; payload consumption excluded | 1418.48x | 410.03x | 407.13x |

The mixed-size cases issue 4,096 reads, every sixteenth read retrieving the large value. All returned bytes are compared with expected contents inside timing. These are read-only traces, not concurrent read/write workloads. With tuned RocksDB caching, the engine is about 2.0–2.2x faster in these two traces; the default-policy 4–5x ratios also reflect cache admission.

The isolated 2 MiB row times returning and dropping independently owned values. The engine can share a validated, immutable single-record block; RocksDB’s owned Get copies the value. The roughly 407–410x tuned ratios therefore describe this narrow API/lifetime operation, not processing 2 MiB of application data or general database throughput. Full seeded payload validation is performed outside that timed pass. A DB-borrowed pinned value has a different lifetime contract and is not substituted for the independently owned API.

## Why cache policy matters

For repeated isolated 2 MiB reads, RocksDB reported 87 cache bytes with default LRU, compared with 4,194,833 bytes for single-shard LRU and 4,198,668 for HyperClock. The latter two actually admitted the large blocks. All three had the same 8 MiB configured capacity. Reporting only the default policy would overstate the implementation advantage. The engine reported 4,194,730 cache bytes.

Sharing applies only to one validated value of at least 64 KiB whose retained block/index allocation and ReadBlock metadata exceed the value by at most 256 bytes. Arc/Bytes ownership and allocator bookkeeping are additional. Caller-held values are outside the cache budget; retaining a small slice can retain its large backing allocation. Multi-record blocks and excessive spare capacity use copies. Returned values retain no engine or file handle. See [storage ownership](storage-format.md) and the lifetime/eviction tests.

These measurements use warm OS cache and data that fits host RAM. They do not establish cold-device throughput, sustained concurrent performance, or behavior during physical power loss. WAL sync and CRC validation were not weakened. The downloadable artifact is `recovery-comparison`, SHA-256 `e4f1aa9dbb36d82a5fb9b7150fa214ffbc30e76d5b1b3e98f9f8561b7b1bc43e`; it contains 1,404 cache-policy records plus 252 recovery guard records.
