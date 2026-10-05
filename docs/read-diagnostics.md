# Read-path diagnosis

Run the explicitly ignored diagnostic in release mode:

```sh
cargo test --locked --release --lib read_cost_breakdown -- --ignored --nocapture --test-threads=1
```

The `read-diagnostics` workflow runs this command and uploads JSON records
prefixed with `READ_DIAGNOSTIC`, plus machine metadata. This is an experiment,
not a performance threshold enforced by ordinary tests.

Fixtures contain one million eight-byte keys and 128-byte values. They are
created directly with the production SST writer and manifest before timing,
avoiding the unrelated cost of synchronous WAL insertion. One fixture has
one SST and another has five disjoint SSTs. A high compaction trigger keeps
this layout fixed during the read-only experiment. This differs from the
earlier end-to-end benchmark's organically produced SST layout.

Each configuration is measured three times with deterministic random queries.
All timed query variants validate returned values. OS file cache is retained.

- **Public Get cache sweep:** zero and 8 MiB block caches are warmed with the
  query sequence. A 256 MiB cache is populated with every block before timing.
  The larger cache is a diagnostic counterfactual, not a recommended setting
  or a low-memory configuration. Counters report cache hits and block reads.
- **Block phase timings:** forced cache misses separate raw buffer allocation
  and pread; frame allocation/copy and CRC; full record decoding/validation;
  binary search/value cloning; and buffer/record destruction. Phase timers
  add overhead and alter temporary-buffer lifetimes, so proportions are
  approximate. Full decoding duplicates the production validation logic for
  instrumentation; the separate baseline below calls production `Table::block`.
- **Materialization control:** on the same query sequence and file, compare
  production `Table::block` plus lookup against a diagnostic implementation
  that traverses borrowed record slices and copies only the requested value.
  Both read and checksum the entire frame and validate all record headers,
  order, sequence bounds and block boundaries. Queries force cache misses;
  this is not an optimized replacement for the complete public Get path.
  Control order alternates across trials.

These measurements can identify expensive read-path work. They do not predict
an exact end-to-end speedup, prove equivalence on every malformed file, measure
cold physical-disk reads, or benchmark concurrent clients. A production
decoder/cache change still requires compatibility and corruption tests.
