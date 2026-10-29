"""Separate source changes from pathname effects using identical-binary controls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
from workloads import capture, warm


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve()
                for name in ["baseline", "engine"]}
    binaries["engine-control"] = binaries["engine"]
    environment = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "baseline": capture("git", "-C", "baseline", "rev-parse", "HEAD"),
        "cpu": capture("lscpu"), "rustc": capture("rustc", "--version", "--verbose"),
        "binaries": {name: hashlib.sha256(binary.read_bytes()).hexdigest()
                     for name, binary in binaries.items()},
        "trials": 12,
        "semantics": "Engine/control use the same binary and fixture. Native control uses baseline-length pathname. Fixed policy uses exactly the same DB pathname for all participants. Fixtures are copied, synced and OS-warmed before reads. Bounds timers and validation are unchanged."
    }
    assert environment["binaries"]["engine"] == environment["binaries"]["engine-control"]
    configs = [dict(keys=4096, value=value, block=4096, cache=0, memtable=67108864,
                    span=10, batch=256, fixed=True, queries=2000, generations=16, range="hit")
               for value in [128, 16384]]
    with tempfile.TemporaryDirectory(prefix="kv-bounds-", dir=os.environ.get("KV_BENCH_DIR")) as temp:
        root = Path(temp)
        environment["filesystem"] = capture("findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS")
        (args.output / "environment.json").write_text(json.dumps(environment, indent=2))

        def execute(name, mode, directory, config, label):
            done = subprocess.run([str(binaries[name]), mode, str(directory), json.dumps(config)],
                                  text=True, capture_output=True, timeout=90)
            (args.output / (label + ".log")).write_text(done.stdout + done.stderr)
            if done.returncode:
                print(done.stdout + done.stderr, flush=True)
            done.check_returncode()
            assert "WORKLOAD_VERIFIED" in done.stdout
            return done.stdout

        fixtures = {}
        for config in configs:
            for name in ["baseline", "engine"]:
                label = f"fixture-{config['value']}-{name}"
                directory = root / label
                execute(name, "prepare-bounds", directory, config, label)
                fixtures[config["value"], name] = directory
        cases = [(config, policy, name) for config in configs for policy in ["native", "fixed"]
                 for name in ["baseline", "engine", "engine-control"]]
        with (args.output / "results.jsonl").open("w") as sink:
            for trial in range(1, 13):
                shift = (trial - 1) % len(cases)
                for rank, (config, policy, name) in enumerate(cases[shift:] + cases[:shift], 1):
                    value = config["value"]
                    label = f"{value}-{policy}-{trial}-{name}"
                    suffix = "engine" if name == "engine" else "baseline"
                    directory = root / (f"16files-{value}b-hit-{trial}-{suffix}"
                                        if policy == "native" else "probe")
                    fixture_name = "engine" if name == "engine-control" else name
                    shutil.copytree(fixtures[value, fixture_name], directory)
                    warm(directory)
                    stdout = execute(name, "bounds-read", directory, config, label)
                    rows = [json.loads(line.removeprefix("WORKLOAD_RESULT "))
                            for line in stdout.splitlines() if line.startswith("WORKLOAD_RESULT ")]
                    assert [row["stage"] for row in rows] == ["bounds-open", "bounds-first", "bounds-repeat"]
                    for row in rows:
                        assert row["config"] == config and row["backend"] == "engine"
                        row.update(backend=name, case=f"{value}-{policy}", trial=trial, order=rank,
                                   pathname=directory.name, pathname_bytes=len(os.fsencode(directory)))
                        sink.write(json.dumps(row) + "\n")
                        sink.flush()
                        print("BOUNDS_LAYOUT " + json.dumps(row), flush=True)
                    shutil.rmtree(directory)


if __name__ == "__main__":
    main()
