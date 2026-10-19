"""Bounded, paired workload comparison. Keep this separate from the large read suite."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def warm(path):
    for file in sorted(path.rglob("*")):
        if file.is_file():
            with file.open("rb") as source:
                while source.read(1024 * 1024):
                    pass


def summarize(records, baseline=False):
    groups = {}
    for row in records:
        if row["stage"] == "open":
            continue
        key = (row["case"], row["scenario"], row["stage"])
        groups.setdefault(key, {}).setdefault(row["backend"], []).append(row)
    summary = []
    for (case, scenario, stage), backends in sorted(groups.items()):
        if set(backends) != ({"baseline", "engine", "rocksdb"} if baseline else {"engine", "rocksdb"}):
            raise AssertionError("unpaired backend result")
        unit = "seconds" if stage == "compact" else "ops_per_second"
        a = {r["trial"]: r["metrics"][unit] for r in backends["engine"]}
        b = {r["trial"]: r["metrics"][unit] for r in backends["rocksdb"]}
        assert a.keys() == b.keys()
        ratios = [b[t] / a[t] if stage == "compact" else a[t] / b[t] for t in a]
        row = {"case": case, "scenario": scenario, "stage": stage,
                        "unit": unit, "engine_median": statistics.median(a.values()),
                        "rocksdb_median": statistics.median(b.values()),
                        "engine_speed_ratio_median": statistics.median(ratios),
                        "engine_speed_ratio_min": min(ratios), "engine_speed_ratio_max": max(ratios),
                        "trials": len(ratios),
                        "engine_peak_rss_kib_median": statistics.median(r["metrics"]["after"]["peak_rss_kib"] for r in backends["engine"]),
                        "rocksdb_peak_rss_kib_median": statistics.median(r["metrics"]["after"]["peak_rss_kib"] for r in backends["rocksdb"])}
        if baseline:
            old = {r["trial"]: r["metrics"][unit] for r in backends["baseline"]}
            assert old.keys() == a.keys()
            change = [old[t] / a[t] if stage == "compact" else a[t] / old[t] for t in a]
            row.update(baseline_median=statistics.median(old.values()),
                       baseline_peak_rss_kib_median=statistics.median(r["metrics"]["after"]["peak_rss_kib"] for r in backends["baseline"]),
                       vs_baseline_speed_ratio_median=statistics.median(change),
                       vs_baseline_speed_ratio_min=min(change), vs_baseline_speed_ratio_max=max(change))
        summary.append(row)
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--smoke", action="store_true")
    parser.add_argument("--baseline", action="store_true", help="Include workloads-baseline, using identical engine read fixture bytes")
    parser.add_argument("--reads-only", action="store_true", help="Skip writes and compaction when separate suites already cover them")
    parser.add_argument("--profile", type=int, choices=range(6), help="Run one numbered read profile")
    parser.add_argument("--scenario", choices=["uniform-t1-span0", "uniform-t4-span0", "hot-t1-span0", "miss-t1-span0", "scan-t1-span32", "scan-t1-span512"])
    parser.add_argument("--trials", type=int, choices=range(1, 31))
    args = parser.parse_args()
    if (args.profile is not None or args.scenario) and (args.smoke or not args.reads_only):
        parser.error("Profile/scenario selection requires --reads-only and cannot use --smoke")
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"] if args.baseline else ["engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    trials = args.trials or (1 if args.smoke else (6 if args.baseline else 4))
    profiles = [{"keys": 128, "value": 32, "block": 4096, "cache": 8192, "churn": True}] if args.smoke else [
        {"keys": 20000, "value": 128, "block": 4096, "cache": 8388608, "churn": False},
        {"keys": 20000, "value": 128, "block": 16384, "cache": 8388608, "churn": False},
        {"keys": 20000, "value": 1024, "block": 16384, "cache": 8388608, "churn": False},
        {"keys": 20000, "value": 128, "block": 16384, "cache": 0, "churn": False},
        {"keys": 20000, "value": 128, "block": 16384, "cache": 8388608, "churn": True},
        {"keys": 4096, "value": 4096, "block": 16384, "cache": 8388608, "churn": True},
    ]
    query_count = 64 if args.smoke else 100000
    scenarios = [
        {"workload": "uniform", "queries": query_count},
        {"workload": "hot", "queries": query_count},
        {"workload": "miss", "queries": query_count},
        {"workload": "uniform", "queries": query_count // 4, "threads": 4},
        {"workload": "scan", "span": 16 if args.smoke else 32, "queries": 8 if args.smoke else 512},
        {"workload": "scan", "span": 64 if args.smoke else 512, "queries": 4 if args.smoke else 256},
    ]
    records = []
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"), "platform": platform.platform(),
        "rust": capture("rustc", "--version"), "cpu": capture("lscpu"),
        "memory": Path("/proc/meminfo").read_text(), "trials": trials, "profiles": profiles,
        "selected_profile": args.profile, "selected_scenario": args.scenario,
        "binaries": {name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for name, p in binaries.items()},
        "semantics": {
            "durability": "WAL enabled; sync_all/fsync per put or whole batch",
            "ownership": "Both return owned values; scans return owned keys and values",
            "read_layout": "Identical logical public-API write history; automatic compaction disabled; physical SST counts may differ",
            "write_layout": "Each engine's normal background compaction; no compression; 4 MiB memtable target",
            "scan_cache": "No fill on either backend; existing cached blocks may still benefit RocksDB",
            "cache_state": "Pristine fixture copy and warm OS file cache before every read process; fresh engine cache on first pass; identical seeded scan trace repeated 24/96 times for warm throughput only",
            "latency": "Separate warm pass: point API call only; scan creation/consumption/drop, no expected-value construction or verification",
            "throughput": "v2: includes RNG and owned result/drop; excludes full verification, per-query timers, worker startup/join. Identical seeded full verification runs after timing. Not directly comparable to earlier v1 absolute rates.",
            "memory": "Read RSS/HWM excludes fixture creation but includes harness/runtime; OS page cache excluded",
            "disk": "Directory size and allocated file blocks exclude directory metadata and unlinked pinned files",
            "limits": "Small bounded workloads on shared CI; no cold-device, power-loss or long-duration steady-state claims",
        },
    }
    if args.baseline:
        environment["baseline"] = capture("git", "-C", "baseline", "rev-parse", "HEAD")
        environment["semantics"]["baseline_layout"] = "Baseline and current use exact copies of the baseline-generated engine fixture, with the identical harness and query trace"
    with tempfile.TemporaryDirectory(prefix="kv-workloads-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("WORKLOAD_ENV " + json.dumps(environment), flush=True)
        with (args.output / "results.jsonl").open("w") as output:
            def run(name, mode, directory, config, case, scenario, trial, order):
                label = f"{case}-{scenario}-{trial}-{name}"
                print("WORKLOAD_CASE " + label, flush=True)
                completed = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)], text=True, capture_output=True, timeout=120)
                (args.output / (label + ".log")).write_text(completed.stdout + completed.stderr)
                if completed.returncode:
                    print(completed.stdout + completed.stderr, flush=True)
                completed.check_returncode()
                assert "WORKLOAD_VERIFIED" in completed.stdout
                stages = []
                for line in completed.stdout.splitlines():
                    if line.startswith("WORKLOAD_RESULT "):
                        row = json.loads(line.removeprefix("WORKLOAD_RESULT "))
                        assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine") and row["config"] == config
                        row.update(backend=name, case=case, scenario=scenario, trial=trial, order=order)
                        stages.append(row["stage"])
                        records.append(row)
                        output.write(json.dumps(row) + "\n"); output.flush()
                        print("WORKLOAD_RESULT " + json.dumps(row), flush=True)
                    elif line.startswith("WORKLOAD_PREPARED "):
                        print(line, flush=True)
                assert stages == {"prepare": [], "read": ["open", "first", "repeat"], "write": ["write"], "compact": ["compact"]}[mode]

            for profile_index, profile in enumerate(profiles):
                if args.profile is not None and profile_index != args.profile:
                    continue
                case = f"p{profile_index}-{profile['value']}b-{profile['block']}block-{profile['cache']}cache-{'churn' if profile['churn'] else 'clean'}"
                fixtures = {name: root / (case + name) for name in binaries}
                for name, directory in fixtures.items():
                    if args.baseline and name == "engine":
                        continue
                    run(name, "prepare", directory, profile, case, "prepare", 0, 0)
                if args.baseline:
                    fixtures["engine"] = fixtures["baseline"]
                for trial in range(1, trials + 1):
                    # Each backend occupies every position equally across the full run.
                    offset = (trial - 1) % len(scenarios)
                    for scenario in scenarios[offset:] + scenarios[:offset]:
                        label = scenario["workload"] + f"-t{scenario.get('threads',1)}-span{scenario.get('span',0)}"
                        if args.scenario and label != args.scenario:
                            continue
                        config = dict(profile, **scenario, seed=7919 + trial * 104729)
                        if profile["value"] == 4096 and scenario["workload"] == "scan":
                            config["queries"] = 96 if scenario["span"] == 32 else 48
                        if scenario["workload"] == "scan":
                            config["scan_repeat_passes"] = 2 if args.smoke else (96 if scenario["span"] == 32 else 24)
                        offset_backend = (trial - 1) % len(binaries)
                        order_names = list(binaries)[offset_backend:] + list(binaries)[:offset_backend]
                        for order, name in enumerate(order_names, 1):
                            # Writable opens can rotate WALs/accumulate diagnostic
                            # logs. Keep every measured process at identical history.
                            copy = root / f"read-{profile_index}-{trial}-{name}"
                            shutil.copytree(fixtures[name], copy)
                            warm(copy)
                            run(name, "read", copy, config, case, label, trial, order)
                            shutil.rmtree(copy)
                    if profile["churn"] and not args.reads_only:
                        for order, name in enumerate(order_names, 1):
                            copy = root / f"compact-{profile_index}-{trial}-{name}"
                            shutil.copytree(fixtures[name], copy)
                            warm(copy)
                            run(name, "compact", copy, dict(profile, queries=query_count, seed=trial*7919), case, "compact", trial, order)
                            shutil.rmtree(copy)
            for batch in ([] if args.reads_only else [1, 32, 256]):
                for trial in range(1, trials + 1):
                    offset_backend = (trial - 1) % len(binaries)
                    order_names = list(binaries)[offset_backend:] + list(binaries)[:offset_backend]
                    for order, name in enumerate(order_names, 1):
                        directory = root / f"write-{batch}-{trial}-{name}"
                        config = {"keys": 64 if args.smoke else 8192, "value": 128, "batch": batch,
                                  "fixed": False, "workload": "put" if batch == 1 else "batch"}
                        run(name, "write", directory, config, "durable-write", f"batch-{batch}", trial, order)
    result = summarize(records, args.baseline)
    (args.output / "summary.json").write_text(json.dumps(result, indent=2))
    for row in result:
        print("WORKLOAD_SUMMARY " + json.dumps(row), flush=True)
    print(f"WORKLOAD_COMPLETE records={len(records)}", flush=True)


if __name__ == "__main__":
    main()
