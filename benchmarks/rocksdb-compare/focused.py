"""Paired F2/F3 diagnostics and normal durable overwrites against RocksDB."""
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

output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)


def capture(*cmd):
    return subprocess.check_output(cmd, text=True).strip()


def build(manifest, example=None):
    command = ["cargo", "build" if example else "test", "--locked", "--release",
               "--manifest-path", manifest, "--message-format=json"]
    command += ["--example", example] if example else ["--lib", "--no-run"]
    done = subprocess.run(command, text=True, capture_output=True)
    if done.returncode:
        print(done.stdout + done.stderr, flush=True)
    done.check_returncode()
    artifacts = [json.loads(line) for line in done.stdout.splitlines() if line.startswith("{")]
    matches = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact"
               and a.get("executable") and (a.get("target", {}).get("name") == example
               if example else a.get("profile", {}).get("test"))]
    assert len(matches) == 1, matches
    return matches[0]


internal = {}
for name, manifest in [("baseline", "baseline/Cargo.toml"), ("engine", "Cargo.toml")]:
    internal[name] = {"picker": build(manifest), "upsert": build(manifest, "upsert_profile")}
names = ["baseline", "engine", "rocksdb"]
binaries = {n: Path("comparison-bin", "workloads-" + n).resolve() for n in names}
environment = {
    "commit": capture("git", "rev-parse", "HEAD"),
    "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
    "platform": platform.platform(), "cpu": capture("lscpu"),
    "rust": capture("rustc", "--version"), "trials": 6,
    "binaries": {n: hashlib.sha256(p.read_bytes()).hexdigest() for n, p in binaries.items()},
    "semantics": "Durable overwrites: sync WAL for every put, no compression, same trace. Internal picker and volatile upsert diagnostics have no RocksDB equivalent.",
    "limits": "Shared CI host; short warm-cache workloads. Internal libtest picker results describe selection only, not total compaction."
}
records = []
with (output / "internal.jsonl").open("w") as sink:
    for trial in range(1, 7):
        for profile in ["picker", "upsert"]:
            for name in (["baseline", "engine"] if trial % 2 else ["engine", "baseline"]):
                command = [internal[name][profile]]
                if profile == "picker":
                    command += ["picker_profile", "--ignored", "--nocapture", "--test-threads=1"]
                print(f"FOCUSED_CASE internal {profile} {trial} {name}", flush=True)
                done = subprocess.run(command, text=True, capture_output=True, timeout=60)
                (output / f"{profile}-{trial}-{name}.log").write_text(done.stdout + done.stderr)
                if done.returncode:
                    print(done.stdout + done.stderr, flush=True)
                done.check_returncode()
                prefix = "PICKER_PROFILE " if profile == "picker" else "UPSERT_PROFILE "
                rows = [json.loads(line.split(prefix, 1)[1]) for line in done.stdout.splitlines() if prefix in line]
                assert len(rows) == (9 if profile == "picker" else 6), rows
                for row in rows:
                    row.update(backend=name, profile=profile, trial=trial)
                    records.append(row)
                    sink.write(json.dumps(row) + "\n")
                    print("FOCUSED_INTERNAL_RESULT " + json.dumps(row), flush=True)

cases = [
    ("key8", 8, 8192, False), ("key1024", 1024, 4096, False),
    ("key65536", 65536, 1024, False), ("key1024-snapshot", 1024, 4096, True)
]
with tempfile.TemporaryDirectory(prefix="kv-focused-", dir=os.environ.get("KV_BENCH_DIR")) as tmp:
    root = Path(tmp)
    environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
    (output / "environment.json").write_text(json.dumps(environment, indent=2))
    print("FOCUSED_ENV " + json.dumps(environment), flush=True)
    with (output / "durable.jsonl").open("w") as sink:
        for trial in range(1, 7):
            for case, key_bytes, queries, snapshot in cases:
                config = dict(keys=256, value=128, queries=queries, key_bytes=key_bytes,
                              snapshot=snapshot, fixed=True, memtable=268435456, seed=12345)
                offset = (trial - 1) % 3
                for order, name in enumerate(names[offset:] + names[:offset], 1):
                    label = f"{case}-{trial}-{name}"
                    print("FOCUSED_CASE durable " + label, flush=True)
                    done = subprocess.run([str(binaries[name]), "overwrite-keys", str(root / label),
                                           json.dumps(config)], text=True, capture_output=True, timeout=120)
                    (output / (label + ".log")).write_text(done.stdout + done.stderr)
                    if done.returncode:
                        print(done.stdout + done.stderr, flush=True)
                    done.check_returncode()
                    assert "WORKLOAD_VERIFIED" in done.stdout
                    rows = [json.loads(line.removeprefix("WORKLOAD_RESULT ")) for line in done.stdout.splitlines()
                            if line.startswith("WORKLOAD_RESULT ")]
                    assert len(rows) == 1 and rows[0]["stage"] == "overwrite-keys"
                    row = rows[0]
                    assert row["config"] == config
                    assert row["backend"] == ("rocksdb" if name == "rocksdb" else "engine")
                    row.update(backend=name, case=case, trial=trial, order=order, profile="durable")
                    records.append(row)
                    sink.write(json.dumps(row) + "\n")
                    sink.flush()
                    print("FOCUSED_RESULT " + json.dumps(row), flush=True)
                    shutil.rmtree(root / label)


def identity(row):
    return (row["profile"], row.get("case"), row.get("tables"), row.get("key_bytes"), row.get("retain_snapshot"))


def rate(row):
    return row["metrics"]["ops_per_second"] if row["profile"] == "durable" else row["ops_per_second"]


summary = []
for current in [r for r in records if r["backend"] == "engine" and r["trial"] == 1]:
    group = [r for r in records if identity(r) == identity(current)]
    result = {"profile": current["profile"], "condition": identity(current)[1:]}
    for name in names:
        rows = [r for r in group if r["backend"] == name]
        if rows:
            result[name + "_median_ops_per_second"] = statistics.median(map(rate, rows))
    for other in ["baseline", "rocksdb"]:
        ratios = [rate(r) / rate(next(b for b in group if b["backend"] == other and b["trial"] == r["trial"]))
                  for r in group if r["backend"] == "engine"] if any(r["backend"] == other for r in group) else []
        if ratios:
            result["paired_vs_" + other] = ratios
            result["paired_median_vs_" + other] = statistics.median(ratios)
    summary.append(result)
    print("FOCUSED_SUMMARY " + json.dumps(result), flush=True)
(output / "summary.json").write_text(json.dumps(summary, indent=2))
print(f"FOCUSED_COMPLETE records={len(records)}", flush=True)
