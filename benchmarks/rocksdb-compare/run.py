"""Linux paired comparison; run with cgroup v2 write access on a disposable runner."""
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile
import uuid


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def create_group():
    root = Path("/sys/fs/cgroup")
    if "memory" not in (root / "cgroup.subtree_control").read_text().split():
        (root / "cgroup.subtree_control").write_text("+memory")
    group = root / ("kvcompare-" + uuid.uuid4().hex)
    group.mkdir()
    # Fail before benchmarking if the requested accounting is unavailable.
    for name in ("memory.current", "memory.peak", "memory.stat", "memory.swap.current"):
        (group / name).read_text()
    return group


def main():
    if sys.argv[1:] == ["--check-cgroup"]:
        group = create_group()
        group.rmdir()
        print("COMPARE_CGROUP_READY", flush=True)
        return
    output = Path(sys.argv[1] if len(sys.argv) > 1 else "comparison-results")
    output.mkdir(parents=True, exist_ok=True)
    binaries = {name: Path("comparison-bin", name).resolve() for name in ("engine", "rocksdb")}
    metadata = {
        "commit": capture("git", "rev-parse", "HEAD"),
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
            "write_buffer_bytes": 4 * 1024 * 1024,
            "max_write_buffers": 3,
            "block_cache_bytes": 8 * 1024 * 1024,
            "block_size": 16 * 1024,
            "bloom_bits_per_key": 10,
            "compression": "none",
            "writes": "WAL enabled; sync per operation; fsync",
            "compaction": "engine default size-tiered; RocksDB default leveled",
        },
        "notes": [
            "One client thread. Identical workload source, separate release executables.",
            "Fresh process and database per trial. Engine order alternates between trials.",
            "RSS includes harness/allocator. Cgroup memory includes charged anon/file/kernel pages.",
            "Shared pages can be charged elsewhere; unique DB files are created after entering cgroup.",
            "Do not add RSS and cgroup file bytes: mapped pages can overlap.",
            "Memory budgets are configuration targets, not hard process limits.",
            "Reopen keeps OS cache and allocator state; neither read pass is a cold-disk test.",
            "Values are deterministic/compressible; compression is disabled for both engines.",
            "Throughput includes verification and final flush/background compaction wait.",
            "Operation latency sampled at most 10000 times per phase; excludes end-of-phase flush.",
            "Disk allocation includes regular DB files, excluding directory metadata.",
            "fsync calls on a hosted CI filesystem are not a power-loss durability test.",
        ],
    }
    cases = [(100_000, 128, trial) for trial in range(1, 4)]
    cases += [(1_000_000, 128, trial) for trial in range(1, 3)]
    cases += [(100_000, 1024, 1)]
    if os.environ.get("KV_COMPARE_SMOKE") == "1":
        cases = [(100, 128, 1)]
    with tempfile.TemporaryDirectory(prefix="kvcompare-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        metadata["filesystem"] = capture("findmnt", "-T", root, "-o", "SOURCE,FSTYPE,OPTIONS")
        (output / "environment.json").write_text(json.dumps(metadata, indent=2))
        print("COMPARE_ENV " + json.dumps(metadata), flush=True)
        with (output / "results.jsonl").open("w") as result_file:
            for keys, size, trial in cases:
                order = ("engine", "rocksdb") if trial % 2 else ("rocksdb", "engine")
                for position, name in enumerate(order, 1):
                    case = f"{keys}-{size}-{trial}-{name}"
                    group = create_group()
                    print(f"COMPARE_CASE {case}", flush=True)
                    env = dict(os.environ, KV_BENCH_CGROUP=str(group))

                    def enter_group():
                        # Parent has no threads; move before exec/DB allocation.
                        (group / "cgroup.procs").write_text(str(os.getpid()))

                    verified = False
                    try:
                        command = [str(binaries[name]), str(Path(root) / case), str(keys), str(size)]
                        with (output / f"{case}.log").open("w") as raw:
                            with subprocess.Popen(command, stdout=subprocess.PIPE, text=True,
                                                  env=env, preexec_fn=enter_group) as process:
                                for line in process.stdout:
                                    raw.write(line)
                                    raw.flush()
                                    if line.startswith("COMPARE_REPORT "):
                                        record = json.loads(line.removeprefix("COMPARE_REPORT "))
                                        record.update(engine=name, trial=trial, order=position)
                                        result_file.write(json.dumps(record) + "\n")
                                        result_file.flush()
                                        print("COMPARE_RESULT " + json.dumps(record), flush=True)
                                    else:
                                        verified |= line.startswith("COMPARE_VERIFIED ")
                                        print(line.rstrip(), flush=True)
                                if process.wait() != 0 or not verified:
                                    raise RuntimeError(f"workload {case} failed")
                    finally:
                        group.rmdir()
    print("COMPARE_COMPLETE", flush=True)


if __name__ == "__main__":
    main()
