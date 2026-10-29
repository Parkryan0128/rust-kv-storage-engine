"""Bounded same-host baseline/current/RocksDB durable-write comparison."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import statistics
import shutil
import subprocess
import tempfile


def capture(*command):
    return subprocess.check_output(command, text=True).strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--case", choices=["put-128b", "batch32-128b", "batch256-16b", "flush-pressure"])
    parser.add_argument("--trials", type=int, choices=range(1, 31))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
        "platform": platform.platform(), "cpu": capture("lscpu"),
        "rust": capture("rustc", "--version"), "trials": args.trials or 3, "batch32_trials": args.trials or 12,
        "selected_case": args.case,
        "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name, p in binaries.items()},
        "semantics": "Identical harness for baseline/current; WAL enabled, sync per commit for all backends; no compression. Batch construction included. Validate all keys before and after reopen.",
        "limits": "Shared CI host; bounded workloads, not long-duration steady state or cold-device results. Wall time includes OS scheduling and sync tails. No per-fsync timing. Resource snapshots occur outside timing; pressure-case worker may continue between snapshots.",
    }
    cases = [
        ("put-128b", {"keys": 8192, "value": 128, "batch": 1, "workload": "put"}),
        ("batch32-128b", {"keys": 32768, "value": 128, "batch": 32, "memtable": 67108864, "workload": "batch"}),
        # 1024 commits with an explicit 64 MiB budget; pressure remains separate.
        ("batch256-16b", {"keys": 262144, "value": 16, "batch": 256, "memtable": 67108864, "workload": "batch"}),
        ("flush-pressure", {"keys": 32768, "value": 128, "batch": 32, "workload": "batch", "memtable": 262144}),
    ]
    records = []
    if args.case:
        cases = [(name, config) for name, config in cases if name == args.case]
    with tempfile.TemporaryDirectory(prefix="kv-writes-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        environment["measurement_path_policy"] = "fixed-v1"
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("WRITE_ENV " + json.dumps(environment), flush=True)
        with (args.output / "results.jsonl").open("w") as output:
            for trial in range(args.trials or 12):
                # Each backend occupies every order position; rotate cases too.
                case_order = trial % len(cases)
                backend_order = trial % len(names)
                for case, settings in cases[case_order:] + cases[:case_order]:
                    if trial >= (args.trials or (12 if case == "batch32-128b" else 3)):
                        continue
                    config = dict(settings, fixed=False)
                    for order, name in enumerate(names[backend_order:] + names[:backend_order], 1):
                        label = f"{case}-{trial+1}-{name}"
                        print("WRITE_CASE " + label, flush=True)
                        done = subprocess.run([str(binaries[name]), "write", str(root / "probe"), json.dumps(config)], text=True, capture_output=True, timeout=90)
                        (args.output / (label + ".log")).write_text(done.stdout + done.stderr)
                        if done.returncode:
                            print(done.stdout + done.stderr, flush=True)
                        done.check_returncode()
                        assert "WORKLOAD_VERIFIED" in done.stdout
                        rows = [json.loads(line.removeprefix("WORKLOAD_RESULT ")) for line in done.stdout.splitlines() if line.startswith("WORKLOAD_RESULT ")]
                        assert len(rows) == 1 and rows[0]["stage"] == "write"
                        row = rows[0]
                        assert row["config"] == config
                        assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine")
                        row.update(backend=name, case=case, trial=trial+1, order=order)
                        records.append(row)
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                        print("WRITE_RESULT " + json.dumps(row), flush=True)
                        shutil.rmtree(root / "probe")
    summary = []
    for case, _ in cases:
        for name in names:
            rows = [r for r in records if r["case"] == case and r["backend"] == name]
            rates = [r["metrics"]["ops_per_second"] for r in rows]
            ratios = []
            for row in rows:
                base = next(r for r in records if r["case"] == case and r["backend"] == "baseline" and r["trial"] == row["trial"])
                ratios.append(row["metrics"]["ops_per_second"] / base["metrics"]["ops_per_second"])
            summary.append({"case": case, "backend": name, "median_ops_per_second": statistics.median(rates), "min_ops_per_second": min(rates), "max_ops_per_second": max(rates), "paired_vs_baseline": ratios, "median_including_flush_ops_per_second": statistics.median(r["metrics"]["including_flush_ops_per_second"] for r in rows)})
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    for row in summary:
        print("WRITE_SUMMARY " + json.dumps(row), flush=True)
    print(f"WRITE_COMPLETE records={len(records)}", flush=True)


if __name__ == "__main__":
    main()
