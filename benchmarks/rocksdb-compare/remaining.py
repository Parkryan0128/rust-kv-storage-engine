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
        ("snapshot-write32", "snapshot-write", dict(common, batch=32, queries=200), None),
    ])
    records = []
    with tempfile.TemporaryDirectory(prefix="kv-remaining-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment = {"commit": capture("git", "rev-parse", "HEAD"),
            "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
            "cpu": capture("lscpu"), "platform": platform.platform(), "trials": args.trials,
            "filesystem": capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS"),
            "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name,p in binaries.items()},
            "semantics": "Same total cache/owned values/sync WAL. Baseline/current reuse identical SST and WAL bytes. OS file cache warmed. Recovery fixture exits without destructors after durable writes. Memory/mixed setup outside timers; RSS includes setup. Throughput excludes worker startup/join and per-query timers, includes validation/RNG/drop. Frozen internal microbenchmark is separate, not RocksDB-equivalent."}
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
                "recover": ["recover"], "memory-scan": ["memory-scan"], "mixed-large": ["first", "repeat"], "cache-phase": ["first", "repeat"], "snapshot-write": ["snapshot-write"]}
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
