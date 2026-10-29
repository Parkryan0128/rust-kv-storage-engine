"""Matched production-build insertion diagnostics with varied keys and snapshots."""
import argparse
import hashlib
import json
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
example = Path("examples/insertion_profile.rs")
shutil.copyfile(example, Path("baseline") / example)
binaries = {}
for name, manifest in [("baseline", "baseline/Cargo.toml"), ("engine", "Cargo.toml")]:
    build = subprocess.run(["cargo", "build", "--locked", "--release", "--manifest-path", manifest,
                            "--example", "insertion_profile", "--message-format=json"],
                           text=True, capture_output=True)
    if build.returncode:
        print(build.stdout + build.stderr, flush=True)
    build.check_returncode()
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    binaries[name] = next(Path(a["executable"]) for a in artifacts
                          if a.get("reason") == "compiler-artifact" and a.get("executable")
                          and a.get("target", {}).get("name") == "insertion_profile")

environment = {"commit": capture("git", "rev-parse", "HEAD"),
               "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
               "cpu": capture("lscpu"), "trials": args.trials, "measurement_path_policy": "fixed-executable-v1",
               "harness_sha256": hashlib.sha256(example.read_bytes()).hexdigest(),
               "binaries": {name: hashlib.sha256(p.read_bytes()).hexdigest()
                            for name, p in binaries.items()},
               "semantics": "Identical production example on both revisions; volatile engine; "
                            "new-key insertion timer excludes setup and model checks. "
                            "Sequential interleaved processes, not a RocksDB comparison."}
(output / "environment.json").write_text(json.dumps(environment, indent=2))
with (output / "results.jsonl").open("w") as sink:
    for trial in range(1, args.trials + 1):
        names = ["baseline", "engine"] if trial % 2 else ["engine", "baseline"]
        for name in names:
            probe = (output / "insertion-profile-probe").resolve()
            shutil.copy2(binaries[name], probe)
            run = subprocess.run([str(probe)], text=True, capture_output=True, timeout=180)
            (output / f"{trial}-{name}.log").write_text(run.stdout + run.stderr)
            if run.returncode:
                print(run.stdout + run.stderr, flush=True)
            run.check_returncode()
            rows = [json.loads(line.removeprefix("INSERTION_PROFILE "))
                    for line in run.stdout.splitlines() if line.startswith("INSERTION_PROFILE ")]
            assert len(rows) == 24
            identities = {(r["kind"], r["shuffled"], r["batch"], r["retained_snapshot"]) for r in rows}
            assert len(identities) == 24
            for row in rows:
                assert row["keys"] == 100000
                row.update(backend=name, trial=trial)
                sink.write(json.dumps(row) + "\n")
                sink.flush()
                print("INSERTION_RESULT " + json.dumps(row), flush=True)
print("INSERTION_COMPLETE", flush=True)

probe.unlink()
