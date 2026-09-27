# Tests

Run the regular suite:

```bash
cargo test --locked --all-features
cargo test --locked --release --all-features
```

Run the five-million-write test:

```bash
cargo test --locked --release --all-features --test stress -- --ignored --nocapture
```

## Coverage

| File | Checks |
|---|---|
| `src/memtable.rs`, `src/bloom.rs` | Memory accounting, tombstones, Bloom filter membership and false positives |
| `tests/engine.rs` | API, reopen, limits, shared handles, cache, background flush, compaction, directory locks |
| `tests/corruption.rs` | Byte flips, truncation, malformed records, missing files, torn WAL recovery |
| `tests/crash.rs` | Process kills, crash points during publication, I/O errors, cross-process locking |
| `tests/linearizability.rs` | Concurrent histories checked against a sequential model |
| `tests/stress.rs` | Five million synced writes, compaction, reopen, deletion, and another reopen |

The model tests run 100,000 mixed operations plus five 10,000-operation seeds against a `BTreeMap`. They compare values after compaction and reopening.

Concurrency tests use ten writers and ten readers, contended keys, and concurrent flush/compact calls. The history checker tries valid sequential orders for 60 histories of 12 operations each, preserving call/return ordering.

Corruption tests flip every byte in small WAL, manifest, and SST fixtures; truncate the SST at every offset; and try 1,000 checksum-valid malformed WAL records. Torn-tail tests cut the final WAL record at every offset, recover, write again, and reopen.

## Crash tests

`fault-injection` enables the `crash_worker` binary. Tests set these variables in child processes:

| Variable | Effect |
|---|---|
| `KV_FAILPOINT` | Select a named hook in the write or maintenance path |
| `KV_FAIL_ACTION=error` | Return an I/O error instead of exiting |

The default action exits with code 86 without running destructors. The suite covers 12 WAL/flush crash points, eight compaction crash points, and seven I/O errors.

The SIGKILL test kills a writer during writes, flushes, and compaction in eight trials. Acknowledgements are printed only after `put()` succeeds. Recovery must retain every acknowledged write; unacknowledged writes may also be present.

## Recorded runs

2026-09-27, Rust 1.98.1:

- Linux container: 34 regular tests/doctests passed; 35 with the stress test included.
- Crash and history suites: 25 consecutive runs passed, covering 1,500 histories, 200 SIGKILL trials, 500 crash points, and 175 injected I/O errors.
- [GitHub CI](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/36337265086): Linux x86_64 and macOS ARM64 passed formatting, Clippy, debug/release tests, and benchmark compilation.

The repeat run followed a lock-release fix: a concurrently forked child could briefly retain the database file lock. `Disk::drop` now unlocks it explicitly.

SIGKILL leaves the OS cache intact. Power loss, torn sectors, and device write reordering have not been tested. I/O injection covers named hooks, not every possible filesystem failure.
