"""Matched production-build snapshot CPU diagnostics at several index sizes."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
from workloads import capture

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("output", type=Path)
parser.add_argument("--trials", type=int, choices=range(1, 31), default=6)
args = parser.parse_args()
output = args.output
output.mkdir(parents=True, exist_ok=True)
example = Path("examples/snapshot_profile.rs")
shutil.copyfile(example, Path("baseline") / example)
binaries = {}
for name, manifest in [("baseline", "baseline/Cargo.toml"), ("engine", "Cargo.toml")]:
    build = subprocess.run(["cargo", "build", "--locked", "--release", "--manifest-path", manifest,
                            "--example", "snapshot_profile", "--message-format=json"],
                           text=True, capture_output=True)
    if build.returncode:
        print(build.stdout + build.stderr, flush=True)
    build.check_returncode()
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    binaries[name] = next(Path(a["executable"]) for a in artifacts
                          if a.get("reason") == "compiler-artifact" and a.get("executable")
                          and a.get("target", {}).get("name") == "snapshot_profile")

environment = {"commit": capture("git", "rev-parse", "HEAD"),
               "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
               "cpu": capture("lscpu"), "trials": args.trials, "measurement_path_policy": "fixed-executable-v1",
               "harness_sha256": hashlib.sha256(example.read_bytes()).hexdigest(),
               "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest()
                            for name, p in binaries.items()},
               "semantics": "Identical production example on both revisions; volatile engine; "
                            "batch timer isolates writes, total includes snapshot creation/drop. "
                            "Sequential interleaved processes, not a RocksDB comparison."}
(output / "environment.json").write_text(json.dumps(environment, indent=2))
with (output / "results.jsonl").open("w") as sink:
    for trial in range(1, args.trials + 1):
        names = ["baseline", "engine"] if trial % 2 else ["engine", "baseline"]
        for keys in [20000, 100000, 1000000]:
            for name in names:
                # Both backends execute from the same argv[0] and environment.
                # Keep separately built originals for provenance and copying.
                probe = (output / "snapshot-profile-probe").resolve()
                shutil.copy2(binaries[name], probe)
                run = subprocess.run([str(probe)], text=True, capture_output=True,
                    env=dict(os.environ, KV_PROFILE_TRIAL=str(trial), KV_PROFILE_KEYS=str(keys),
                             KV_PROFILE_QUERIES="10000"), timeout=120)
                (output / f"{keys}-{trial}-{name}.log").write_text(run.stdout + run.stderr)
                if run.returncode:
                    print(run.stdout + run.stderr, flush=True)
                run.check_returncode()
                rows = [json.loads(line.removeprefix("MEMORY_WRITE_PROFILE "))
                        for line in run.stdout.splitlines() if line.startswith("MEMORY_WRITE_PROFILE ")]
                assert len(rows) == 3
                for row in rows:
                    assert row["trial"] == trial and row["queries"] == 10000
                    assert row["keys"] == (256 if row["case"] == "repeated-fold" else keys)
                    row.update(backend=name, requested_keys=keys)
                    sink.write(json.dumps(row) + "\n")
                    sink.flush()
                    print("SNAPSHOT_SCALING_RESULT " + json.dumps(row), flush=True)
print("SNAPSHOT_SCALING_COMPLETE", flush=True)

probe.unlink()
