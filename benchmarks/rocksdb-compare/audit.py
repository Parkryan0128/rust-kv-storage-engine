"""Paired reopen/tombstone checks plus separately instrumented write attribution."""
import json
import os
from pathlib import Path
import statistics
import shutil
import subprocess
import sys
import tempfile
from workloads import capture

output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
names = ["baseline", "engine", "rocksdb"]
common = dict(keys=8192, value=128, batch=256, span=10, memtable=67108864, fixed=True)
print("AUDIT_ENV " + json.dumps(dict(commit=capture("git", "rev-parse", "HEAD"),
    baseline=capture("git", "-C", "baseline", "rev-parse", "HEAD"), cpu=capture("lscpu"),
    measurement_path_policy="fixed-v1", semantics="Normal binaries for throughput. Profiling builds for phase attribution only; no cross-comparison with normal rates. Warm OS cache; same harness and logical history.")), flush=True)

def run(binary, mode, directory, config, label):
    result = subprocess.run([str(Path("comparison-bin", "workloads-" + binary).resolve()),
        mode, str(directory), json.dumps(config)], capture_output=True, text=True, timeout=180)
    (output / (label + ".log")).write_text(result.stdout + result.stderr)
    if result.returncode:
        print(result.stdout + result.stderr, flush=True)
    result.check_returncode()
    assert "WORKLOAD_VERIFIED" in result.stdout
    row = next(json.loads(line.split(" ", 1)[1]) for line in result.stdout.splitlines()
        if line.startswith("WORKLOAD_RESULT "))
    assert row["config"] == config
    timing = next((json.loads(line.split(" ", 1)[1]) for line in result.stdout.splitlines()
        if line.startswith("WRITE_TIMINGS ")), None)
    shutil.rmtree(directory)
    return row, timing

rows = []
phases = []
with tempfile.TemporaryDirectory(prefix="kv-audit-", dir=os.environ.get("KV_BENCH_DIR")) as temporary:
    root = Path(temporary)
    for trial in range(1, 7):
        for case, mode, config in [
            ("tombstones-memory", "tombstone-scan", dict(common, queries=512, disk=False)),
            ("tombstones-sst", "tombstone-scan", dict(common, queries=512, disk=True)),
            ("clean-reopen", "reopen-loop", dict(common, queries=300)),
        ]:
            shift = (trial-1) % 3
            for order, name in enumerate(names[shift:] + names[:shift], 1):
                label = f"{case}-{trial}-{name}"
                row, _ = run(name, mode, root / "probe", config, label)
                row.update(backend=name, case=case, trial=trial, order=order)
                rows.append(row)
                print("AUDIT_RESULT " + json.dumps(row), flush=True)
        for case, mode, config in [
            ("batch32", "write", dict(common, keys=32768, batch=32, workload="batch")),
            ("batch256", "write", dict(common, keys=262144, batch=256, workload="batch")),
            ("snapshot-write32", "snapshot-write", dict(common, keys=20000, batch=32, queries=2000)),
        ]:
            for name in (["baseline", "engine"] if trial % 2 else ["engine", "baseline"]):
                label = f"phases-{case}-{trial}-{name}"
                _, timing = run(name + "-profile", mode, root / "probe", config, label)
                assert timing is not None
                samples = timing["samples"]
                assert len(samples) == (2000 if mode == "snapshot-write" else 1024)
                totals = {phase: sum(sample["phase_ns"][i] for sample in samples) / 1e6
                    for i, phase in enumerate(timing["phases"])}
                row = dict(case=case, backend=name, trial=trial, commits=len(samples),
                    total_ms=sum(sample["total_ns"] for sample in samples)/1e6, phases_ms=totals)
                phases.append(row)
                print("AUDIT_PHASE " + json.dumps(row), flush=True)
(output / "results.json").write_text(json.dumps(rows, indent=2))
(output / "phases.json").write_text(json.dumps(phases, indent=2))
for case in sorted({r["case"] for r in rows}):
    rates = {(r["trial"], r["backend"]): r["metrics"]["ops_per_second"] for r in rows if r["case"] == case}
    print("AUDIT_SUMMARY " + json.dumps(dict(case=case,
        current_vs_baseline=statistics.median(rates[t,"engine"]/rates[t,"baseline"] for t in range(1,7)),
        current_vs_rocksdb=statistics.median(rates[t,"engine"]/rates[t,"rocksdb"] for t in range(1,7)))), flush=True)
print(f"AUDIT_COMPLETE records={len(rows)} phases={len(phases)}", flush=True)
