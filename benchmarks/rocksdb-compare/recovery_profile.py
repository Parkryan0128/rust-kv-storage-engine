"""Alternate identical bounded WAL diagnostics on baseline/current release test binaries."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
binaries = {}
for name, manifest in [("baseline", "baseline/Cargo.toml"), ("engine", "Cargo.toml")]:
    build = subprocess.run(["cargo", "test", "--locked", "--release", "--lib", "--manifest-path", manifest,
                            "--no-run", "--message-format=json"], text=True, capture_output=True)
    if build.returncode:
        print(build.stdout + build.stderr, flush=True)
    build.check_returncode()
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    tests = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact"
             and a.get("executable") and a.get("profile", {}).get("test")]
    assert len(tests) == 1, tests
    binaries[name] = tests[0]
metadata = {
    "revisions": {name: subprocess.check_output(["git", "-C", root, "rev-parse", "HEAD"], text=True).strip()
                  for name, root in [("baseline", "baseline"), ("engine", ".")]},
    "binaries": {name: hashlib.sha256(Path(binary).read_bytes()).hexdigest() for name, binary in binaries.items()},
    "rust": subprocess.check_output(["rustc", "--version"], text=True),
    "cpu": subprocess.check_output(["lscpu"], text=True),
    "profile_sha256": hashlib.sha256(Path("src/wal_profile.rs").read_bytes()).hexdigest(),
    "notes": "Both test binaries compile the exact same diagnostic. Production phases overlap and must not be summed."
}
(output / "environment.json").write_text(json.dumps(metadata, indent=2))
with (output / "results.jsonl").open("w") as sink:
    for trial in range(1, 13):
        names = ["baseline", "engine"] if trial % 2 else ["engine", "baseline"]
        for order, name in enumerate(names, 1):
            print(f"RECOVERY_CASE trial={trial} backend={name}", flush=True)
            run = subprocess.run([binaries[name], "wal::profile::profile_recovery_stages", "--ignored", "--exact",
                                  "--nocapture", "--test-threads=1"], text=True, capture_output=True, timeout=90)
            (output / f"{trial}-{name}.log").write_text(run.stdout + run.stderr)
            if run.returncode:
                print(run.stdout + run.stderr, flush=True)
            run.check_returncode()
            count = 0
            for line in run.stdout.splitlines():
                if "RECOVERY_PROFILE " in line:
                    row = json.loads(line.split("RECOVERY_PROFILE ", 1)[1])
                    row.update(backend=name, trial=trial, order=order)
                    sink.write(json.dumps(row) + "\n")
                    sink.flush()
                    count += 1
            assert count == 50, (name, trial, count)
print("RECOVERY_COMPLETE", flush=True)
