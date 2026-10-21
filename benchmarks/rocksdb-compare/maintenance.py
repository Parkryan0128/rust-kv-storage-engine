"""Paired production scheduling diagnostic; no comparable RocksDB internals."""
import json
from pathlib import Path
import subprocess
import sys

profile = sys.argv[2] if len(sys.argv) > 2 else "maintenance_yield_profile"
prefix, expected_rows = {"maintenance_yield_profile": ("MAINTENANCE", 2),
                         "compact_request_profile": ("COMPACT_REQUEST", 3)}[profile]
output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
binaries = {}
for name, manifest in [("baseline", "baseline/Cargo.toml"), ("engine", "Cargo.toml")]:
    build = subprocess.run(["cargo", "test", "--locked", "--release", "--lib",
                            "--manifest-path", manifest, "--no-run", "--message-format=json"],
                           text=True, capture_output=True, timeout=120)
    if build.returncode:
        print(build.stdout + build.stderr, flush=True)
    build.check_returncode()
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    tests = [a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact"
             and a.get("executable") and a.get("profile", {}).get("test")]
    assert len(tests) == 1, tests
    binaries[name] = tests[0]
with (output / "results.jsonl").open("w") as sink:
    for trial in range(1, 7):
        names = ["baseline", "engine"] if trial % 2 else ["engine", "baseline"]
        for name in names:
            run = subprocess.run([binaries[name], profile,
                                  "--ignored", "--nocapture", "--test-threads=1"],
                                 text=True, capture_output=True, timeout=90)
            (output / f"{trial}-{name}.log").write_text(run.stdout + run.stderr)
            if run.returncode:
                print(run.stdout + run.stderr, flush=True)
            run.check_returncode()
            count = 0
            for line in run.stdout.splitlines():
                if prefix + "_PROFILE " in line:
                    row = json.loads(line.split(prefix + "_PROFILE ", 1)[1])
                    row.update(backend=name, trial=trial, build_mode="release-libtest")
                    sink.write(json.dumps(row) + "\n")
                    sink.flush()
                    print(prefix + "_RESULT " + json.dumps(row), flush=True)
                    count += 1
            assert count == expected_rows, count
print(f"{prefix}_COMPLETE records={6 * 2 * expected_rows}", flush=True)
