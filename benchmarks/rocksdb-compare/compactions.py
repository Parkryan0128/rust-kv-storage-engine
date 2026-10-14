"""Compare compaction on identical engine SSTs and matched RocksDB history."""
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


def capture(*command):
    return subprocess.check_output(command, text=True).strip()


def sst_digest(directory):
    return [(p.name, p.stat().st_size, hashlib.sha256(p.read_bytes()).hexdigest())
            for p in sorted((directory / "sst").glob("*.sst"))]


def warm(directory):
    for p in directory.rglob("*"):
        if p.is_file():
            with p.open("rb") as f:
                while f.read(1024 * 1024):
                    pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    cases = [
        ("disjoint-128b", {"keys": 32768, "value": 128, "churn": False}),
        ("overwrite-128b", {"keys": 32768, "value": 128, "churn": True}),
        ("overwrite-4096b", {"keys": 8192, "value": 4096, "churn": True}),
    ]
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
        "platform": platform.platform(), "cpu": capture("lscpu"),
        "rust": capture("rustc", "--version"), "trials": 6,
        "semantics": "Each trial gets a pristine fixture copy and warm OS cache. Baseline/current use identical SST bytes prepared by baseline; RocksDB uses the same logical write history. Four flushed generations, automatic compaction disabled, no compression. All keys verified after compact and reopen outside timing. Final baseline/current SST files must match byte for byte.",
        "limits": "Shared CI, warm OS cache, bounded single compactions. Different native table layouts and compaction policies across engine/RocksDB. No claim about sustained writes, cold devices or fsync-tail causality.",
        "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name, p in binaries.items()},
    }
    records = []
    with tempfile.TemporaryDirectory(prefix="kv-compactions-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("COMPACTION_ENV " + json.dumps(environment), flush=True)

        def run(name, mode, directory, config, label):
            done = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)], text=True, capture_output=True, timeout=90)
            (args.output / (label + ".log")).write_text(done.stdout + done.stderr)
            if done.returncode:
                print(done.stdout + done.stderr, flush=True)
            done.check_returncode()
            assert "WORKLOAD_VERIFIED" in done.stdout
            return done.stdout

        fixtures = {}
        for case, settings in cases:
            config = dict(settings, fixed=True, generations=4, memtable=64 * 1024 * 1024, cache=0)
            fixtures[case] = {}
            for name in ["baseline", "rocksdb"]:
                directory = root / ("fixture-" + case + name)
                output = run(name, "prepare-merge", directory, config, "prepare-" + case + name)
                prepared = [json.loads(line.removeprefix("WORKLOAD_PREPARED ")) for line in output.splitlines() if line.startswith("WORKLOAD_PREPARED ")]
                assert len(prepared) == 1
                print("COMPACTION_FIXTURE " + json.dumps({"case": case, "backend": name, "config": config, **prepared[0]}), flush=True)
                fixtures[case][name] = directory
            fixtures[case]["engine"] = fixtures[case]["baseline"]

        with (args.output / "results.jsonl").open("w") as output:
            for trial in range(6):
                offset = trial % len(cases)
                for case, settings in cases[offset:] + cases[:offset]:
                    config = dict(settings, fixed=True, generations=4, memtable=64 * 1024 * 1024, cache=0)
                    digests = {}
                    order_offset = trial % len(names)
                    for order, name in enumerate(names[order_offset:] + names[:order_offset], 1):
                        label = f"{case}-{trial+1}-{name}"
                        print("COMPACTION_CASE " + label, flush=True)
                        directory = root / label
                        shutil.copytree(fixtures[case][name], directory)
                        warm(directory)
                        stdout = run(name, "compact-merge", directory, config, label)
                        rows = [json.loads(line.removeprefix("WORKLOAD_RESULT ")) for line in stdout.splitlines() if line.startswith("WORKLOAD_RESULT ")]
                        assert len(rows) == 1 and rows[0]["stage"] == "compact-merge"
                        row = rows[0]
                        assert row["config"] == config
                        assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine")
                        row.update(backend=name, case=case, trial=trial+1, order=order)
                        if name != "rocksdb":
                            digests[name] = sst_digest(directory)
                            row["output_sst_digest"] = digests[name]
                        records.append(row)
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                        print("COMPACTION_RESULT " + json.dumps(row), flush=True)
                        shutil.rmtree(directory)
                    assert digests["baseline"] == digests["engine"], "SST output differs from baseline"
    summary = []
    for case, _ in cases:
        for name in names:
            rows = [r for r in records if r["case"] == case and r["backend"] == name]
            times = [r["metrics"]["seconds"] for r in rows]
            ratios = []
            for row in rows:
                base = next(r for r in records if r["case"] == case and r["backend"] == "baseline" and r["trial"] == row["trial"])
                ratios.append(base["metrics"]["seconds"] / row["metrics"]["seconds"])
            summary.append({"case": case, "backend": name, "median_seconds": statistics.median(times), "min_seconds": min(times), "max_seconds": max(times), "paired_speed_vs_baseline": ratios,
                            "median_peak_rss_kib": statistics.median(r["metrics"]["after"]["peak_rss_kib"] for r in rows)})
    (args.output / "summary.json").write_text(json.dumps(summary, indent=2))
    for row in summary:
        print("COMPACTION_SUMMARY " + json.dumps(row), flush=True)
    print(f"COMPACTION_COMPLETE records={len(records)} identical_engine_outputs=true", flush=True)


if __name__ == "__main__":
    main()
