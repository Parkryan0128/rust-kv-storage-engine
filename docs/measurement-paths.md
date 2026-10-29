# Measurement path control

A reproducible benchmark bias was isolated on 2026-10-29. The EPYC 7763
[control run](https://github.com/Parkryan0128/rust-kv-storage-engine/actions/runs/40883350894)
used the exact same current executable and fixture under two DB pathnames.

For 2,000 ten-row scans of 16 KiB values, the native engine path took a median
63.835 ms; the same executable under the baseline-length path took 52.683 ms.
The paired control/engine ratio was 0.8291, with bootstrap 95% interval
[0.8196, 0.8436]. This cannot be attributed to a source-code difference.

With one identical pathname reused sequentially, baseline/current/control
medians were 53.913 / 53.048 / 52.976 ms. The paired baseline/current ratio was
1.0085 [0.9767, 1.0280], and identical-binary control/current was 0.9962
[0.9672, 1.0063]. Full validation and the timed scan loop were unchanged.
The benchmark had reported roughly a 15% source regression on this CPU; the
fixed-path control does not support that attribution.

Earlier controls on Intel 6973P-C and EPYC 9V74 did not reproduce the large
difference. Path-dependent heap allocation layout is a plausible mechanism;
these measurements establish the pathname effect, not its exact allocator
or microarchitectural cause. Do not subtract a control ratio from an unrelated
run or pool absolute times across CPUs.

All normal comparison runners now use exactly one private measurement pathname
for baseline, engine and RocksDB, with rotated backend order. Each process gets
a fresh copied fixture or fresh database. Large read-only fixtures are renamed
into that pathname, directory-synced and warmed before timing, then restored
after the child exits, including on failure. Existing destinations are rejected.
The explicit layout diagnostic retains native paths for reproduction.

Environment files label corrected runs with `measurement_path_policy: fixed-v1`.
Historical results remain available, but native-path absolute timings and small
relative differences must not be combined with fixed-path results as if they
used one methodology. The correction changes benchmark setup, not the library
API, storage format, durability or record verification.

The standard-runner artifact is `read-layout-ubuntu-latest`, SHA-256
`664e71308a16480fbd017aa1bf29a1cda200122edc369cf700af9c608ff01573`.
It contains 432 records: two value sizes, native/fixed paths, three participants,
three stages and twelve trials. The current/control executable hashes match.
