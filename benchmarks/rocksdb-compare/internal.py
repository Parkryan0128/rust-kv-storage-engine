"""Interleave baseline/current internal profiles; never overlap measured processes."""
import json
import os
from pathlib import Path
import subprocess
import sys

output = Path(sys.argv[1])
output.mkdir(parents=True, exist_ok=True)
binaries = {}
write_binaries = {}
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
    build = subprocess.run(["cargo", "build", "--locked", "--release", "--manifest-path", manifest,
                            "--example", "snapshot_profile", "--message-format=json"], text=True, capture_output=True)
    if build.returncode:
        print(build.stdout + build.stderr, flush=True)
    build.check_returncode()
    artifacts = [json.loads(line) for line in build.stdout.splitlines() if line.startswith("{")]
    write_binaries[name] = next(a["executable"] for a in artifacts if a.get("reason") == "compiler-artifact"
                                and a.get("executable") and a.get("target", {}).get("name") == "snapshot_profile")

prefixes = ("MEMORY_SCAN_PROFILE ", "CACHE_HIT_PROFILE ", "MEMORY_WRITE_PROFILE ")
with (output / "results.jsonl").open("w") as sink:
    for trial in range(1, 7):
        names = ["baseline", "engine"] if trial % 2 else ["engine", "baseline"]
        for profile in ["remaining_memory_scan_profile", "current_cache_hit_profile", "snapshot_mutation_profile"]:
            for name in names:
                print(f"INTERNAL_CASE trial={trial} profile={profile} backend={name}", flush=True)
                command = ([write_binaries[name]] if profile == "snapshot_mutation_profile" else
                           [binaries[name], profile, "--ignored", "--nocapture", "--test-threads=1"])
                run = subprocess.run(command,
                    env=dict(os.environ, KV_PROFILE_TRIAL=str(trial)), text=True, capture_output=True, timeout=90)
                (output / f"{profile}-{trial}-{name}.log").write_text(run.stdout + run.stderr)
                if run.returncode:
                    print(run.stdout + run.stderr, flush=True)
                run.check_returncode()
                count = 0
                for line in run.stdout.splitlines():
                    # libtest can prefix the first emitted line with the test name.
                    for prefix in prefixes:
                        if prefix in line:
                            row = json.loads(line.split(prefix, 1)[1])
                            row.update(backend=name, profile=profile, trial=trial,
                                       build_mode="release-library" if profile == "snapshot_mutation_profile" else "release-libtest")
                            sink.write(json.dumps(row) + "\n")
                            sink.flush()
                            print("INTERNAL_RESULT " + json.dumps(row), flush=True)
                            count += 1
                            break
                assert count == {"remaining_memory_scan_profile": 4, "current_cache_hit_profile": 1,
                                 "snapshot_mutation_profile": 3}[profile], (profile, count)
print("INTERNAL_COMPLETE", flush=True)
