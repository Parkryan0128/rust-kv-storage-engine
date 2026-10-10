"""Random-read-only comparison across equal 4/8/16 KiB block sizes."""
import hashlib
from itertools import permutations
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import zlib


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def warm_files(path):
    for file in path.rglob("*"):
        if file.is_file():
            with file.open("rb") as source:
                while source.read(1024 * 1024):
                    pass


def logged(command, env, path, timeout, cpu=None):
    completed = subprocess.run(command, env=env, text=True, capture_output=True, timeout=timeout,
                               preexec_fn=None if cpu is None else lambda: os.sched_setaffinity(0, {cpu}))
    path.write_text(completed.stdout + completed.stderr)
    if completed.returncode:
        print(completed.stdout + completed.stderr, flush=True)
    completed.check_returncode()
    return completed.stdout


def main():
    output = Path(sys.argv[1] if len(sys.argv) > 1 else "comparison-results")
    output.mkdir(parents=True, exist_ok=True)
    names = ("engine", "baseline", "rocksdb")
    orders = list(permutations(names))
    cpu = min(os.sched_getaffinity(0))
    sources = {"engine": Path.cwd(), "baseline": Path("baseline-source").resolve()}
    binaries = {name: Path("comparison-bin", name).resolve() for name in names}
    smoke = os.environ.get("KV_COMPARE_SMOKE") == "1"
    queries = 1000 if smoke else 500_000
    trials = 1 if smoke else 6
    block_sizes = (4096, 8192, 16384)
    cases = [(100, 128, 1)] if smoke else [
        (100_000, 128, 1), (1_000_000, 128, 5), (100_000, 1024, 5)
    ]
    metadata = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline_commit": capture("git", "-C", str(sources["baseline"]), "rev-parse", "HEAD"),
        "platform": platform.platform(),
        "benchmark_cpu": cpu,
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
            "block_sizes": block_sizes,
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
            "Every backend is compared at all three equal block-size settings.",
            "All block-size fixtures are prepared before a case's measured trials.",
            "Fixture creation and OS-cache warming are outside measured processes.",
            "Fresh process/block cache per trial; two identical seeded random passes.",
            "Read-only fixed layouts: engine compaction trigger 64; RocksDB read-only open.",
            "Block-size order rotates twice; all six backend-order permutations run once.",
            "RSS excludes fixture creation and OS page cache.",
            "Current and baseline use their own production V5/V3 SST writers and separate fixtures; RocksDB uses its production SST writer.",
            "V3/V5 full record validation is cached per block within each fresh engine process; every disk read still checks the full frame CRC.",
            "Latency samples time Get only; throughput includes RNG, validation and value release.",
            "Single-client OS-cache-warm hit workload, not physical-disk or write performance.",
            "All measured read processes and diagnostic profiles are pinned to the same allowed CPU core.",
            "READ_PROFILE is a separate forced-miss microbenchmark, not an end-to-end CPU profile.",
            "Profile CRC-only time overlaps decode; routing/cache/locks/allocation are excluded.",
        ],
    }
    with tempfile.TemporaryDirectory(prefix="kvread-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        metadata["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (output / "environment.json").write_text(json.dumps(metadata, indent=2))
        print("READ_ENV " + json.dumps(metadata), flush=True)
        verified_count = 0
        with (output / "results.jsonl").open("w") as result_file, \
                (output / "profiles.jsonl").open("w") as profile_file:
            for keys, size, tables in cases:
                case = f"{keys}-{size}-{tables}"
                fixtures = {}
                for block_bytes in block_sizes:
                    label = f"{case}-{block_bytes}"
                    directories = {name: root / (label + "-" + name) for name in names}
                    engine_dir = directories["engine"]
                    rocks_dir = directories["rocksdb"]
                    env = dict(os.environ, KV_READ_FIXTURE_DIR=str(engine_dir),
                               KV_READ_FIXTURE_KEYS=str(keys), KV_READ_FIXTURE_VALUE_BYTES=str(size),
                               KV_READ_FIXTURE_TABLES=str(tables),
                               KV_READ_FIXTURE_BLOCK_BYTES=str(block_bytes),
                               KV_READ_BLOCK_BYTES=str(block_bytes))
                    fixtures[block_bytes] = (directories, env)
                    print(f"READ_PREPARE {label}", flush=True)
                    for name, source in sources.items():
                        fixture_env = dict(env, KV_READ_FIXTURE_DIR=str(directories[name]))
                        text = logged(
                            ["cargo", "test", "--locked", "--release", "--lib",
                             "--manifest-path", str(source / "Cargo.toml"),
                             "read_fixture::write_read_benchmark_fixture", "--",
                             "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                            fixture_env, output / f"{label}-{name}-fixture.log", 180,
                        )
                        configs = [json.loads(line.split("READ_FIXTURE_CONFIG ", 1)[1])
                                   for line in text.splitlines() if "READ_FIXTURE_CONFIG " in line]
                        assert len(configs) == 1
                        config = configs[0]
                        assert config["block_bytes"] == block_bytes and config["keys"] == keys
                        assert Path(config["source_root"]).resolve() == source
                        config["engine"] = name
                        print("READ_FIXTURE_CONFIG " + json.dumps(config), flush=True)
                    subprocess.run(
                        [str(binaries["rocksdb"]), "prepare", str(rocks_dir),
                         str(keys), str(size), str(tables)], env=env, check=True, timeout=180,
                    )
                    if not smoke:
                        for name, source in sources.items():
                            warm_files(directories[name])
                            manifest = str(source / "Cargo.toml")
                            profile_env = dict(env, KV_READ_FIXTURE_DIR=str(directories[name]))
                            target = str(source / "target")
                            text = logged(
                                ["cargo", "test", "--locked", "--release", "--lib",
                                 "--manifest-path", manifest, "--target-dir", target,
                                 "sstable::read_profile::profile_random_read_stages", "--",
                                 "--ignored", "--exact", "--nocapture", "--test-threads=1"],
                                profile_env, output / f"{label}-{name}-profile.log", 180, cpu=cpu,
                            )
                            reports = [json.loads(line.split("READ_PROFILE ", 1)[1])
                                       for line in text.splitlines() if "READ_PROFILE " in line]
                            assert len(reports) == 1
                            report = reports[0]
                            assert report["keys"] == keys and report["block_bytes"] == block_bytes
                            assert Path(report["source_root"]).resolve() == source
                            assert report["decoder_source_crc32"] == zlib.crc32(
                                (source / "src/codec.rs").read_bytes())
                            report["engine"] = name
                            profile_file.write(json.dumps(report) + "\n")
                            profile_file.flush()
                            print("READ_PROFILE " + json.dumps(report), flush=True)
                for trial in range(1, trials + 1):
                    block_rotation = (trial - 1) % len(block_sizes)
                    blocks = block_sizes[block_rotation:] + block_sizes[:block_rotation]
                    order = orders[(trial - 1) % len(orders)]
                    seed = 0xACE123 + trial * 104729
                    for block_bytes in blocks:
                        directories, env = fixtures[block_bytes]
                        for position, name in enumerate(order, 1):
                            directory = directories[name]
                            warm_files(directory)
                            label = f"{case}-{block_bytes}-{trial}-{name}"
                            print(f"READ_CASE {label}", flush=True)
                            text = logged(
                                [str(binaries[name]), "read", str(directory), str(keys),
                                 str(size), str(tables), str(queries), str(seed)],
                                env, output / f"{label}.log", 120, cpu=cpu,
                            )
                            seen = []
                            for line in text.splitlines():
                                if line.startswith("READ_REPORT "):
                                    record = json.loads(line.removeprefix("READ_REPORT "))
                                    assert record["keys"] == keys and record["value_bytes"] == size
                                    assert record["tables"] == tables and record["operations"] == queries
                                    assert record["block_bytes"] == block_bytes
                                    record.update(engine=name, trial=trial, order=position,
                                                  block_order=blocks.index(block_bytes) + 1)
                                    seen.append(record["stage"])
                                    result_file.write(json.dumps(record) + "\n")
                                    result_file.flush()
                                    print("READ_RESULT " + json.dumps(record), flush=True)
                                else:
                                    print(line, flush=True)
                            assert seen == ["random_read_first_pass", "random_read_repeat"]
                            assert f"READ_VERIFIED queries={queries * 2} keys={keys}" in text
                            verified_count += 1
        assert verified_count == len(cases) * len(block_sizes) * trials * len(names)
        print(f"READ_COMPLETE verified_processes={verified_count}", flush=True)


if __name__ == "__main__":
    main()
