"""Bounded missing workloads, paired on one host with identical harness binaries."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
from workloads import capture, warm, summarize

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--trials", type=int, choices=range(1, 31), default=6)
    parser.add_argument("--scan-focus", action="store_true", help="Scan scaling plus memory lookup, writes and recovery guards")
    parser.add_argument("--case", action="append", dest="cases", help="Run only these named cases (repeatable)")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    common = {"keys": 20000, "value": 128, "block": 16384, "cache": 8388608,
              "memtable": 67108864, "span": 10, "batch": 256, "fixed": True}
    cases = [
        ("throughput-t1", "read", dict(common, queries=200000, throughput=True), "prepare"),
        ("throughput-t4", "read", dict(common, queries=50000, threads=4, throughput=True), "prepare"),
        ("oversized-2m", "read", dict(common, keys=2, value=2097152, span=1, queries=512), "prepare"),
        ("wal-single", "recover", dict(common, keys=10000, batch=1), "prepare-wal"),
        ("wal-random", "recover", dict(common, keys=10000, batch=1, wal_pattern="random"), "prepare-wal"),
        ("wal-overwrite", "recover", dict(common, keys=10000, batch=1, wal_pattern="overwrite"), "prepare-wal"),
        ("wal-put-sorted", "recover", dict(common, keys=10000, batch=1, wal_individual=True), "prepare-wal"),
        ("wal-put-random100k", "recover", dict(common, keys=100000, batch=1, wal_individual=True, wal_pattern="random"), "prepare-wal"),
        ("wal-batch32-random100k", "recover", dict(common, keys=100000, batch=32, wal_pattern="random"), "prepare-wal"),
        ("snapshot-release", "snapshot-release", dict(common, keys=2, span=1, queries=16), None),
        ("wal-batch256", "recover", dict(common, batch=256), "prepare-wal"),
        ("memory-early10", "memory-scan", dict(common, queries=100), None),
        ("mixed-small-large", "mixed-large", dict(common, keys=4096, value=2097152, queries=4096), None),
    ]
    cases.extend([
        (f"phase-{keys}-{size // 1048576}m", "cache-phase", dict(common, keys=keys, value=size, queries=50000), None)
        for keys in [20000, 40000, 80000] for size in [2097152, 4194304]
    ])
    cases.extend([
        ("mixed-pressure-4m", "mixed-large", dict(common, keys=40000, value=4194304, queries=4096), None),
        ("memory-early10-100k", "memory-scan", dict(common, keys=100000, queries=100), None),
        ("memory-full20k", "memory-scan", dict(common, span=20000, queries=10), None),
        ("snapshot-write32", "snapshot-write", dict(common, batch=32, queries=2000), None),
        ("snapshot-narrow-write32", "snapshot-write", dict(common, batch=32, queries=2000, narrow=True), None),
        ("disjoint12-range10", "disjoint-range", dict(common, keys=24000, generations=12, queries=2000), None),
        ("disjoint12-reopened-range10", "disjoint-range", dict(common, keys=24000, generations=12, queries=2000, reopen=True), None),
        ("mixed-steady32", "mixed-steady", dict(common, batch=32, queries=2000, fixed=False, memtable=524288), None),
    ])
    cases.extend([
        (f"keys-{'common128' if common_prefix else 'variable'}-{'disk' if disk else 'memory'}",
         "shaped-keys", dict(common, queries=200000, common_prefix=common_prefix, disk=disk), None)
        for common_prefix in [False, True] for disk in [False, True]
    ])
    if args.scan_focus:
        cases = [case for case in cases if case[0] in {"memory-early10", "memory-early10-100k", "memory-full20k", "snapshot-write32", "wal-single", "wal-batch256"}]
        cases = [(label, mode, dict(config, queries=5000) if label.startswith("memory-early") else config, preparation)
                 for label, mode, config, preparation in cases]
        cases.extend([
            ("memory-early10-2k", "memory-scan", dict(common, keys=2000, queries=5000), None),
            ("memory-early10-200k", "memory-scan", dict(common, keys=200000, queries=5000, memtable=134217728), None),
            ("memory-middle10-100k", "memory-scan", dict(common, keys=100000, queries=5000, vary_start=True), None),
            ("memory-narrow10-100k", "memory-scan", dict(common, keys=100000, queries=5000, vary_start=True, narrow=True), None),
            ("memory-get20k", "memory-get", dict(common, queries=200000), None),
            ("memory-get100k", "memory-get", dict(common, keys=100000, queries=200000), None),
        ])
    # Opt-in guard: 100k live keys followed by 100k writes to a shuffled 1k-key hotspot.
    # Keep historical default matrices unchanged.
    if args.cases and "wal-large-hotspot" in args.cases:
        cases.append(("wal-large-hotspot", "recover",
                      dict(common, keys=200000, batch=32, wal_pattern="hotspot"), "prepare-wal"))
    # Both engines return with the replayed data in memory, before an SST flush.
    cases = [(label, mode, dict(config, recover_to_memtable=True) if mode == "recover" else config, preparation)
             for label, mode, config, preparation in cases]
    if args.cases:
        selected = set(args.cases)
        unknown = selected - {case[0] for case in cases}
        if unknown:
            parser.error("Unknown cases: " + ", ".join(sorted(unknown)))
        cases = [case for case in cases if case[0] in selected]
    records = []
    with tempfile.TemporaryDirectory(prefix="kv-remaining-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment = {"commit": capture("git", "rev-parse", "HEAD"),
            "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
            "cpu": capture("lscpu"), "platform": platform.platform(), "trials": args.trials,
            "filesystem": capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS"),
            "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name,p in binaries.items()},
            "semantics": "Same total cache/owned values/sync WAL. Baseline/current reuse identical SST and WAL bytes. OS file cache warmed. Recovery fixture exits without destructors after durable writes; recover_to_memtable=true enables RocksDB avoid_flush_during_recovery and asserts zero SST files after open on all backends. Memory/mixed setup outside timers; RSS includes setup. Read-mode throughput excludes worker startup/join, per-query timers and full validation; validation and warm latency are separate seeded passes. Other diagnostic modes retain validation/timers as reported. All include RNG/drop. Frozen internal microbenchmark is separate, not RocksDB-equivalent."}
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("REMAINING_ENV " + json.dumps(environment), flush=True)
        def run(name, mode, directory, config, label, trial, order):
            completed = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)],
                capture_output=True, text=True, timeout=120)
            (args.output / f"{label}-{trial}-{name}-{mode}.log").write_text(completed.stdout + completed.stderr)
            if completed.returncode:
                print(completed.stdout + completed.stderr, flush=True)
            completed.check_returncode()
            assert "WORKLOAD_VERIFIED" in completed.stdout
            rows = []
            for line in completed.stdout.splitlines():
                if line.startswith("WORKLOAD_RESULT "):
                    row = json.loads(line.removeprefix("WORKLOAD_RESULT "))
                    assert row["config"] == config
                    assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine")
                    row.update(backend=name, case=label, scenario=mode, trial=trial, order=order)
                    rows.append(row)
            expected = {"prepare": [], "prepare-wal": [], "read": ["open", "first", "repeat"],
                "recover": ["recover"], "memory-scan": ["memory-scan"], "memory-get": ["memory-get"], "mixed-large": ["first", "repeat"], "cache-phase": ["first", "repeat"], "snapshot-write": ["snapshot-write"]}
            expected.update({mode: [mode] for mode in ["shaped-keys", "disjoint-range", "mixed-steady", "snapshot-release"]})
            assert [r["stage"] for r in rows] == expected[mode]
            return rows
        fixtures = {}
        for label, mode, config, preparation in cases:
            if preparation is None:
                continue
            for name in ["baseline", "rocksdb"]:
                directory = root / f"fixture-{label}-{name}"
                run(name, preparation, directory, config, label, 0, 0)
                fixtures[label, name] = directory
            fixtures[label, "engine"] = fixtures[label, "baseline"]
        with (args.output / "results.jsonl").open("w") as output:
            for trial in range(1, args.trials + 1):
                shift = (trial - 1) % len(cases)
                for label, mode, config, preparation in cases[shift:] + cases[:shift]:
                    config = dict(config, seed=7919 + trial * 104729)
                    order_shift = (trial - 1) % len(names)
                    for order, name in enumerate(names[order_shift:] + names[:order_shift], 1):
                        print(f"REMAINING_CASE {label} trial={trial} backend={name}", flush=True)
                        directory = root / f"run-{label}-{trial}-{name}"
                        if preparation:
                            shutil.copytree(fixtures[label, name], directory)
                            warm(directory)
                        rows = run(name, mode, directory, config, label, trial, order)
                        for row in rows:
                            records.append(row)
                            output.write(json.dumps(row) + "\n")
                            output.flush()
                            print("REMAINING_RESULT " + json.dumps(row), flush=True)
                        shutil.rmtree(directory)
    summary = summarize(records, True)
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    for row in summary:
        print("REMAINING_SUMMARY " + json.dumps(row), flush=True)
    print(f"REMAINING_COMPLETE records={len(records)}", flush=True)

if __name__ == "__main__":
    main()
