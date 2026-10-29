"""Measure the actual SST publisher across fan-in, key length and overlap."""
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


from workloads import warm


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    parser.add_argument("--allow-format-change", action="store_true", help="Verify logical outputs while retaining digests across SST format versions")
    parser.add_argument("--trials", type=int, choices=range(1, 31), default=6)
    parser.add_argument("--case", action="append", dest="selected_cases", help="Select existing named cases (repeatable)")
    parser.add_argument("--trace-syscalls", action="store_true", help="Separate diagnostic: trace write/sync wall time, including setup/reopen")
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["baseline", "engine", "rocksdb"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    cases = [(f"n{n}-k{key_bytes}-{shape}", dict(keys=32768 if shape == "distinct" else 8192, value=128, generations=n,
              block=4096, merge_key_bytes=key_bytes, churn=shape != "distinct",
              merge_deletes=shape == "deletes"))
             for n in [2, 4, 8, 9, 16] for key_bytes in [8, 136]
             for shape in ["distinct", "overlap", "deletes"]]
    # Opt-in outputs larger than the bounded retained-hash buffer force the
    # publisher's Bloom readback path, absent from the original 8k overlap cases.
    for n in [4, 16]:
        for key_bytes in [8, 136]:
            label = f"n{n}-k{key_bytes}-overlap-wide"
            if args.selected_cases and label in args.selected_cases:
                cases.append((label, dict(keys=16384, value=128, generations=n,
                    block=4096, merge_key_bytes=key_bytes, churn=True, merge_deletes=False)))
    # Native prefix cursors budget encoded frames and one current key.
    for n in [8, 16]:
        for key_bytes in [8, 136]:
            label = f"n{n}-k{key_bytes}-distinct-16k"
            if args.selected_cases and label in args.selected_cases:
                cases.append((label, dict(keys=32768, value=128, generations=n,
                    block=16384, merge_key_bytes=key_bytes, churn=False, merge_deletes=False)))
    if args.selected_cases:
        selected = set(args.selected_cases)
        available = {name for name, _ in cases}
        if selected - available:
            parser.error(f"Unknown cases: {sorted(selected - available)}")
        cases = [case for case in cases if case[0] in selected]
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
        "platform": platform.platform(), "cpu": capture("lscpu"),
        "rust": capture("rustc", "--version"), "trials": args.trials,
        "selected_cases": [name for name, _ in cases],
        "syscall_profile": args.trace_syscalls,
        "semantics": "Each trial gets a pristine fixture copy and warm OS cache. Baseline/current use identical SST bytes prepared by baseline; RocksDB uses the same logical write history. 2/4/8/9/16 flushed generations, 8/136-byte common-prefix keys, distinct/overlap/deletion histories, 32768 distinct keys or 8192 keys per overlapping generation, automatic compaction disabled, no compression. All keys verified after compact and reopen outside timing. Final baseline/current SST files must match byte for byte.",
        "limits": "Shared CI, warm OS cache, bounded single compactions. Different native table layouts and compaction policies across engine/RocksDB. No claim about sustained writes, cold devices or fsync-tail causality.",
        "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest() for name, p in binaries.items()},
    }
    if args.trace_syscalls:
        environment["trace_version"] = capture("strace", "--version")
        environment["limits"] += " Instrumented write/sync syscall wall times include open, compaction and verification/reopen. They are not exact API phase percentages or normal throughput rankings."
    environment["allow_format_change"] = args.allow_format_change
    if args.allow_format_change:
        environment["semantics"] = environment["semantics"].replace("Final baseline/current SST files must match byte for byte.", "SST formats may differ; all logical values are verified after compaction and reopen. File digests are retained.")
    records = []
    with tempfile.TemporaryDirectory(prefix="kv-merge-matrix-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        environment["measurement_path_policy"] = "fixed-v1"
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
        print("MERGE_MATRIX_ENV " + json.dumps(environment), flush=True)

        def run(name, mode, directory, config, label):
            command = [str(binaries[name]), mode, str(directory), json.dumps(config)]
            if args.trace_syscalls and mode == "compact-merge":
                command = ["strace", "-f", "-qq", "-w", "-c", "-e",
                           "trace=write,writev,pwrite64,fsync,fdatasync", "-o",
                           str(args.output / (label + "-syscalls.txt"))] + command
            done = subprocess.run(command, text=True, capture_output=True, timeout=90)
            (args.output / (label + ".log")).write_text(done.stdout + done.stderr)
            if done.returncode:
                print(done.stdout + done.stderr, flush=True)
            done.check_returncode()
            assert "WORKLOAD_VERIFIED" in done.stdout
            return done.stdout

        fixtures = {}
        for case, settings in cases:
            config = dict(settings, fixed=True, memtable=64 * 1024 * 1024, cache=0)
            fixtures[case] = {}
            for name in ["baseline", "rocksdb"]:
                directory = root / ("fixture-" + case + name)
                output = run(name, "prepare-merge", directory, config, "prepare-" + case + name)
                prepared = [json.loads(line.removeprefix("WORKLOAD_PREPARED ")) for line in output.splitlines() if line.startswith("WORKLOAD_PREPARED ")]
                assert len(prepared) == 1
                if name == "baseline":
                    assert prepared[0]["stats"]["sst_files"] == config["generations"]
                print("MERGE_MATRIX_FIXTURE " + json.dumps({"case": case, "backend": name, "config": config, **prepared[0]}), flush=True)
                fixtures[case][name] = directory
            fixtures[case]["engine"] = fixtures[case]["baseline"]

        with (args.output / "results.jsonl").open("w") as output:
            for trial in range(args.trials):
                offset = trial % len(cases)
                for case, settings in cases[offset:] + cases[:offset]:
                    config = dict(settings, fixed=True, memtable=64 * 1024 * 1024, cache=0)
                    digests = {}
                    order_offset = trial % len(names)
                    for order, name in enumerate(names[order_offset:] + names[:order_offset], 1):
                        label = f"{case}-{trial+1}-{name}"
                        print("MERGE_MATRIX_CASE " + label, flush=True)
                        directory = root / "probe"
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
                            maximum = row["metrics"]["stats_after"]["max_compaction_inputs"]
                            if case.endswith("-16k"):
                                # These opt-in cases measure whether the input memory budget
                                # requires extra merge rounds; do not assume one round.
                                assert 2 <= maximum <= config["generations"]
                                assert row["metrics"]["stats_after"]["sst_files"] == 1
                            else:
                                assert maximum == config["generations"]
                            digests[name] = sst_digest(directory)
                            row["output_sst_digest"] = digests[name]
                        records.append(row)
                        output.write(json.dumps(row) + "\n")
                        output.flush()
                        print("MERGE_MATRIX_RESULT " + json.dumps(row), flush=True)
                        shutil.rmtree(directory)
                    if not args.allow_format_change:
                        # Extra merge rounds allocate a different final file ID.
                        # Compare every content byte while retaining names in the raw result.
                        normalized = {name: sorted((size, digest) for _, size, digest in files)
                                      for name, files in digests.items()} if case.endswith("-16k") else digests
                        assert normalized["baseline"] == normalized["engine"], "SST output differs from baseline"
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
        print("MERGE_MATRIX_SUMMARY " + json.dumps(row), flush=True)
    print(f"MERGE_MATRIX_COMPLETE records={len(records)} identical_engine_outputs={str(not args.allow_format_change).lower()}", flush=True)


if __name__ == "__main__":
    main()
