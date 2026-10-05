"""Random-read-only comparison; fixture creation is outside measured processes."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile

BASELINE = "c5fe790d5352bdea9df921419ff855938fd13aa9"


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def warm_files(path):
    # Explicitly warm only the OS file cache, outside the measured child process.
    for file in path.rglob("*"):
        if file.is_file():
            with file.open("rb") as source:
                while source.read(1024 * 1024):
                    pass


def main():
    output = Path(sys.argv[1] if len(sys.argv) > 1 else "comparison-results")
    output.mkdir(parents=True, exist_ok=True)
    names = ("baseline", "engine", "rocksdb")
    binaries = {name: Path("comparison-bin", name).resolve() for name in names}
    smoke = os.environ.get("KV_COMPARE_SMOKE") == "1"
    queries = 1000 if smoke else 500_000
    trials = 1 if smoke else 6
    cases = [(100, 128, 1)] if smoke else [
        (100_000, 128, 1), (1_000_000, 128, 5), (100_000, 1024, 5)
    ]
    metadata = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline_commit": BASELINE,
        "platform": platform.platform(),
        "rust": capture("rustc", "--version"),
        "cpu": capture("lscpu"),
        "host_memory": Path("/proc/meminfo").read_text(),
        "binaries": {
            name: {"bytes": p.stat().st_size, "sha256": hashlib.sha256(p.read_bytes()).hexdigest()}
            for name, p in binaries.items()
        },
        "settings": {
            "rocksdb": "11.8.1 (rust-rocksdb 0.25.0)",
            "block_cache_bytes": 8 * 1024 * 1024,
            "block_bytes": 16 * 1024,
            "bloom_bits_per_key": 10,
            "compression": "none",
            "queries_per_pass": queries,
            "trials": trials,
            "cases": cases,
        },
        "notes": [
            "Only random Get is measured; every returned value is checked.",
            "No write/update/delete/compaction or sequential verification phases.",
            "Fixtures: production engine SST writer and RocksDB SstFileWriter/ingestion.",
            "Both use the specified number of disjoint equally divided SST files.",
            "Fixture construction and OS-cache warming are outside measured processes.",
            "Each trial is a fresh process/block cache; two identical seeded random passes.",
            "Read-only fixed layouts: engine compaction trigger 64; RocksDB read-only open.",
            "Backend order rotates; six trials balance each backend's position twice.",
            "RSS belongs to the read process only; setup peaks and charged file cache are excluded.",
            "Results are not directly comparable with earlier organically generated layouts.",
            "Baseline and current engine read the same fixture bytes with the same harness.",
            "Latency samples time Get only; throughput includes RNG, validation and value release.",
            "This is a single-client OS-cache-warm hit workload, not a physical-disk or write test.",
        ],
    }
    with tempfile.TemporaryDirectory(prefix="kvread-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        metadata["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (output / "environment.json").write_text(json.dumps(metadata, indent=2))
        print("READ_ENV " + json.dumps(metadata), flush=True)
        verified_count = 0
        with (output / "results.jsonl").open("w") as result_file:
            for keys, size, tables in cases:
                case = f"{keys}-{size}-{tables}"
                engine_dir = root / (case + "-engine")
                rocks_dir = root / (case + "-rocksdb")
                print(f"READ_PREPARE {case}", flush=True)
                env = dict(os.environ, KV_READ_FIXTURE_DIR=str(engine_dir),
                           KV_READ_FIXTURE_KEYS=str(keys), KV_READ_FIXTURE_VALUE_BYTES=str(size),
                           KV_READ_FIXTURE_TABLES=str(tables))
                subprocess.run(
                    ["cargo", "test", "--locked", "--release", "--lib",
                     "read_fixture::write_read_benchmark_fixture", "--",
                     "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                    env=env, check=True, timeout=180,
                )
                subprocess.run(
                    [str(binaries["rocksdb"]), "prepare", str(rocks_dir),
                     str(keys), str(size), str(tables)], check=True, timeout=180,
                )
                for trial in range(1, trials + 1):
                    rotation = (trial - 1) % len(names)
                    order = names[rotation:] + names[:rotation]
                    seed = 0xACE123 + trial * 104729
                    for position, name in enumerate(order, 1):
                        directory = rocks_dir if name == "rocksdb" else engine_dir
                        warm_files(directory)
                        label = f"{case}-{trial}-{name}"
                        print(f"READ_CASE {label}", flush=True)
                        command = [str(binaries[name]), "read", str(directory), str(keys),
                                   str(size), str(tables), str(queries), str(seed)]
                        completed = subprocess.run(command, text=True, capture_output=True,
                                                   check=True, timeout=120)
                        (output / f"{label}.log").write_text(completed.stdout + completed.stderr)
                        seen = []
                        for line in completed.stdout.splitlines():
                            if line.startswith("READ_REPORT "):
                                record = json.loads(line.removeprefix("READ_REPORT "))
                                assert record["keys"] == keys and record["value_bytes"] == size
                                assert record["tables"] == tables and record["operations"] == queries
                                record.update(engine=name, trial=trial, order=position)
                                seen.append(record["stage"])
                                result_file.write(json.dumps(record) + "\n")
                                result_file.flush()
                                print("READ_RESULT " + json.dumps(record), flush=True)
                            else:
                                print(line, flush=True)
                        assert seen == ["random_read_first_pass", "random_read_repeat"]
                        assert f"READ_VERIFIED queries={queries * 2} keys={keys}" in completed.stdout
                        verified_count += 1
        assert verified_count == len(cases) * trials * len(names)
        print(f"READ_COMPLETE verified_processes={verified_count}", flush=True)


if __name__ == "__main__":
    main()
