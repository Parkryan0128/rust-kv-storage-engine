"""Compare one experimental Linux WAL sync call; production source stays unchanged."""
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
capture = lambda *args: subprocess.check_output(args, text=True).strip()
binaries = {name: Path("comparison-bin", "workloads-" + name).resolve()
            for name in ("fsync", "fdatasync")}
patch = json.loads((output / "candidate.json").read_text())
assert hashlib.sha256(Path("src/wal.rs").read_bytes()).hexdigest() == patch["candidate_wal_sha256"]
assert hashlib.sha256(Path("baseline/src/wal.rs").read_bytes()).hexdigest() == patch["baseline_wal_sha256"]
environment = dict(commit=capture("git", "rev-parse", "HEAD"),
                   baseline=capture("git", "-C", "baseline", "rev-parse", "HEAD"),
                   cpu=capture("lscpu"), platform=platform.platform(),
                   rust=capture("rustc", "--version"), candidate=patch, trials=12,
                   binaries={n:hashlib.sha256(p.read_bytes()).hexdigest() for n,p in binaries.items()},
                   notes="Uncommitted experimental Linux build only: WAL append sync_all vs sync_data. Creation, truncation, manifests and directory sync remain unchanged. Not a RocksDB comparison or power-loss test.")
assert environment["commit"] == environment["baseline"]
cases = [
    ("put-128b", dict(keys=8192, value=128, batch=1, workload="put")),
    ("batch32-128b", dict(keys=32768, value=128, batch=32, workload="batch", memtable=67108864)),
    ("batch256-16b", dict(keys=262144, value=16, batch=256, workload="batch", memtable=67108864)),
    ("flush-pressure", dict(keys=32768, value=128, batch=32, workload="batch", memtable=262144)),
]
rows = []
with tempfile.TemporaryDirectory(prefix="kv-sync-policy-", dir=os.environ.get("KV_BENCH_DIR")) as tmp:
    root = Path(tmp)
    environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
    environment["measurement_path_policy"] = "fixed-v1"
    (output / "environment.json").write_text(json.dumps(environment, indent=2))
    with (output / "results.jsonl").open("w") as sink:
        for trial in range(1, 13):
            order = ["fsync", "fdatasync"] if trial % 2 else ["fdatasync", "fsync"]
            offset = (trial - 1) % len(cases)
            for case, settings in cases[offset:] + cases[:offset]:
                config = dict(settings, fixed=False)
                for position, name in enumerate(order, 1):
                    label = f"{case}-{trial}-{name}"
                    print("SYNC_CASE " + label, flush=True)
                    run = subprocess.run([str(binaries[name]), "write", str(root / "probe"), json.dumps(config)],
                                         text=True, capture_output=True, timeout=90)
                    (output / (label + ".log")).write_text(run.stdout + run.stderr)
                    if run.returncode:
                        print(run.stdout + run.stderr, flush=True)
                    run.check_returncode()
                    assert "WORKLOAD_VERIFIED" in run.stdout
                    found = [json.loads(line.removeprefix("WORKLOAD_RESULT ")) for line in run.stdout.splitlines()
                             if line.startswith("WORKLOAD_RESULT ")]
                    assert len(found) == 1
                    row = found[0]
                    assert row["stage"] == "write" and row["backend"] == "engine" and row["config"] == config
                    row.update(backend=name, case=case, trial=trial, order=position)
                    rows.append(row)
                    sink.write(json.dumps(row) + "\n")
                    sink.flush()
                    shutil.rmtree(root / "probe")
assert len(rows) == 96
for case, _ in cases:
    paired = []
    for trial in range(1, 13):
        sample = {r["backend"]:r["metrics"]["commit_seconds"] for r in rows if r["case"] == case and r["trial"] == trial}
        paired.append(sample["fsync"] / sample["fdatasync"])
    print("SYNC_SUMMARY " + json.dumps(dict(case=case, median_speed_ratio=statistics.median(paired), paired=paired)), flush=True)
print("SYNC_COMPLETE records=96", flush=True)
