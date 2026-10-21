"""First post-open bounded scans, with each backend's native table format."""
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import sys
import tempfile
from workloads import capture, warm


def main():
    output = Path(sys.argv[1])
    output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    common = dict(keys=4096, value=128, block=4096, cache=0, memtable=67108864,
                  span=10, batch=256, fixed=True, queries=2000)
    cases = [(f"{files}files-{value}b-{probe}", dict(common, generations=files, value=value, range=probe))
             for files, value in [(4, 128), (16, 128), (16, 16384)]
             for probe in ["gap", "above", "hit"]]
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
        "platform": platform.platform(), "cpu": capture("lscpu"), "trials": 6,
        "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name, p in binaries.items()},
        "semantics": "Same logical disjoint ranges and durable write history, native SST format per backend; automatic compaction disabled, no compression, no data-block cache. Fresh fixture copy for each trial; warm OS cache before open. Open, first post-open scan and 2000 repeats are separate. First scan includes collection of owned results; validation/drop are outside that first timer. Repeats include result drop, not full validation. All data validated after timing and reopen.",
        "limits": "Shared CI, six first-query samples, warm OS cache; not cold-device p99. Extra engine data block reads are measured directly; RocksDB internal counters are not equivalent. Native fixtures differ by design to exercise persisted maximum-key metadata."
    }
    records = []
    with tempfile.TemporaryDirectory(prefix="kv-bounds-", dir=os.environ.get("KV_BENCH_DIR")) as temp:
        root = Path(temp)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("BOUNDS_ENV " + json.dumps(environment), flush=True)

        def run(name, mode, directory, config, label):
            done = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)],
                                  text=True, capture_output=True, timeout=90)
            (output / (label + ".log")).write_text(done.stdout + done.stderr)
            if done.returncode:
                print(done.stdout + done.stderr, flush=True)
            done.check_returncode()
            assert "WORKLOAD_VERIFIED" in done.stdout
            return done.stdout

        fixtures = {}
        for label, config in cases:
            fixture_key = (config["generations"], config["value"])
            for name in names:
                if (*fixture_key, name) in fixtures:
                    continue
                directory = root / f"fixture-{fixture_key[0]}-{fixture_key[1]}-{name}"
                run(name, "prepare-bounds", directory, config, directory.name)
                fixtures[*fixture_key, name] = directory
        with (output / "results.jsonl").open("w") as sink:
            for trial in range(1, 7):
                shift = (trial - 1) % len(cases)
                for label, config in cases[shift:] + cases[:shift]:
                    order = (trial - 1) % 3
                    for rank, name in enumerate(names[order:] + names[:order], 1):
                        tag = f"{label}-{trial}-{name}"
                        print("BOUNDS_CASE " + tag, flush=True)
                        directory = root / tag
                        shutil.copytree(fixtures[config["generations"], config["value"], name], directory)
                        warm(directory)
                        stdout = run(name, "bounds-read", directory, config, tag)
                        rows = [json.loads(line.removeprefix("WORKLOAD_RESULT ")) for line in stdout.splitlines() if line.startswith("WORKLOAD_RESULT ")]
                        assert [r["stage"] for r in rows] == ["bounds-open", "bounds-first", "bounds-repeat"]
                        for row in rows:
                            assert row["config"] == config
                            assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine")
                            row.update(backend=name, case=label, trial=trial, order=rank)
                            records.append(row)
                            sink.write(json.dumps(row) + "\n")
                            sink.flush()
                            print("BOUNDS_RESULT " + json.dumps(row), flush=True)
                        shutil.rmtree(directory)
    summary = []
    for label, _ in cases:
        for stage in ["bounds-open", "bounds-first", "bounds-repeat"]:
            for name in names:
                rows = [r for r in records if r["case"] == label and r["stage"] == stage and r["backend"] == name]
                times = [r["metrics"]["seconds"] for r in rows]
                summary.append(dict(case=label, stage=stage, backend=name,
                                    median_seconds=statistics.median(times), min_seconds=min(times), max_seconds=max(times)))
    (output / "summary.json").write_text(json.dumps(summary, indent=2))
    print(f"BOUNDS_COMPLETE records={len(records)}", flush=True)


if __name__ == "__main__":
    main()
