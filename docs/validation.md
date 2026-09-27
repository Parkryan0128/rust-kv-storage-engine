# Validation record

Local validation date: 2026-09-27. Rust 1.98.1, Linux x86_64, container overlayfs. Tests use actual filesystem operations and real subprocess termination. Source forbids unsafe Rust; dependencies may contain their own unsafe implementations.

## Commands and observed results

| Check | Result |
|---|---|
| `cargo fmt --check` | Passed |
| `cargo clippy --locked --all-targets --all-features -- -D warnings` | Passed without warnings |
| `cargo test --locked --all-features` | 34 tests/doctests passed; one extended stress test intentionally excluded |
| `cargo test --locked --release --all-features -- --include-ignored` | 35 tests/doctests, including five-million-write stress |
| Crash and linearizability integration suites repeated 25 times | All repetitions passed |
| `cargo run --locked --release --example bench_report` | Executed; recorded CSV and p50/p99 |
| `cargo bench --locked --bench engine` | All three workloads executed |

GitHub Actions defines independent Linux/macOS checks. A workflow definition is not evidence of a passing remote run; consult the linked PR/Actions results for the current commit. The recorded local run covers Linux only.

## Coverage

| Area | Independent behavior exercised |
|---|---|
| API | Missing keys; empty/binary keys and values; overwrites; repeated deletion; reinsert after deletion; volatile mode; shared clones |
| Limits | Invalid options, maximum key/value boundaries, a 1 MiB value spanning ordinary block/memory thresholds |
| Persistence | Repeated reopen, WAL-only recovery, multiple flushed generations, value/tombstone resolution, sequence continuity after empty compaction |
| Model comparison | 100,000 mixed operations against an independent ordered map, with full comparisons and compact/reopen every 2,000 operations; five more fixed seeds × 10,000 operations |
| Concurrency | Ten writers + ten readers; overlapping hot-key writes/deletes; concurrent manual flush/compact; recovery comparison after join |
| Linearizability | 60 histories per run, three threads × four overlapping operations on one key; exhaustive model search respects call/return real-time precedence |
| Memory/backpressure | Bounded active/frozen accounting under 5,000 writes and WAL reclamation after a flush barrier |
| Compaction | Deduplication, deletion without resurrection, empty run reopen, continuous overwrite acceptance |
| Bloom/cache | No Bloom false negatives in deterministic set; bounded false positives; 5,000 absent probes skip most reads; warm block reuse; cache budget and disabled cache |
| Corruption | Every byte flipped in a small complete WAL, manifest, and SST fixture; every truncation of a small SST; missing referenced SST and manifest; 1,000 checksum-valid malformed WAL payloads; oversized frame length |
| Torn writes | Every truncation offset in the final WAL record; recovery followed by further writes/reopen; incomplete new-generation headers; nonfinal torn WAL rejected |
| Process crashes | Twelve WAL/flush publication boundaries; eight compaction boundaries; eight actual SIGKILL trials while writes/flush/compaction run |
| I/O faults | Seven injected write/sync/publication/cleanup failures; engine halts; acknowledged prior data survives reopen |
| Ownership/lifecycle | Same-process clone lifetime lock; cross-process exclusive open; lock cleanup after crashes and last drop |
| Extended acceptance | 100 keys × 50,000 synced overwrites = 5,000,000 writes; compact to exactly 100 records; reopen and verify final generation; delete all, compact to zero, reopen |

The 25-run repeat batch exercised 1,500 concurrent histories, 200 SIGKILL trials, 500 named crash-boundary cases, and 175 injected I/O-error cases. It was added to investigate an intermittent immediate-reopen lock failure during parallel child-process creation. Explicitly unlocking at final engine teardown fixed the observed issue; the repeated batch did not reproduce it.

## Fault injection

Compile with `--features fault-injection` to enable named hooks in `fault.rs`. `KV_FAILPOINT` selects the stage; default action exits the process with code 86 without Rust destructors. `KV_FAIL_ACTION=error` instead injects an I/O error. Integration tests set these variables only inside isolated subprocesses after opening the database, so parallel tests do not share global injection state.

The SIGKILL test reads acknowledgements from the child only after `put` returns. Every acknowledged key must survive; extra unacknowledged keys are permitted by the durability contract. No graceful drop/close is used to simulate a crash.

## Limits of the evidence

Passing tests are evidence for the exercised cases, not a proof of arbitrary executions or production readiness. SIGKILL retains the OS cache and does not emulate power failure or every torn-sector/write-reordering behavior. Actual durability depends on the filesystem and hardware honoring sync/rename ordering. Injected errors do not emulate all possible ENOSPC/device behaviors. Checksums detect corruption but do not repair it; backups remain necessary for media loss. Physical-media power-cut testing, long-duration soak tests on deployment hardware, and scalable multi-level compaction are not included.
