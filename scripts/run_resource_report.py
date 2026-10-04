"""Run each workload in a fresh process and preserve raw stage measurements."""
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import tempfile


def capture(*args):
    return subprocess.check_output(args, text=True).strip()


def main():
    output = Path(sys.argv[1] if len(sys.argv) > 1 else "resource-results")
    output.mkdir(parents=True, exist_ok=True)
    scratch = os.environ.get("KV_BENCH_DIR")
    metadata = {
        "commit": capture("git", "rev-parse", "HEAD"),
        "platform": platform.platform(),
        "rust": capture("rustc", "--version"),
        "cpu": capture("lscpu"),
        "memory": Path("/proc/meminfo").read_text(),
        "defaults": "Options::default(); release; default features; sync every write",
        "notes": [
            "Process RSS includes the engine, allocator retention and a bounded latency sample buffer.",
            "OS filesystem cache is not counted in RSS and is not cleared between phases.",
            "API latency is systematically sampled at most 10000 times per phase.",
            "Throughput includes loop/verification overhead and final flush for mutation phases.",
            "Reopen occurs in the same process, so allocator memory may remain resident.",
            "Filesystem allocation sums regular-file st_blocks, excluding directory metadata.",
        ],
    }
    with tempfile.TemporaryDirectory(prefix="kv-resources-", dir=scratch) as root:
        metadata["filesystem"] = capture("findmnt", "-T", root, "-o", "SOURCE,FSTYPE,OPTIONS")
        (output / "environment.json").write_text(json.dumps(metadata, indent=2))
        print("RESOURCE_ENV " + json.dumps(metadata), flush=True)
        cases = [(100_000, 128, trial) for trial in range(1, 4)]
        cases += [(1_000_000, 128, 1), (100_000, 1024, 1)]
        with (output / "results.jsonl").open("w") as result_file:
            for keys, size, trial in cases:
                case = f"{keys}-{size}-{trial}"
                command = ["target/release/examples/resource_report", str(Path(root) / case), str(keys), str(size)]
                print(f"RESOURCE_CASE {case}", flush=True)
                with (output / f"{case}.log").open("w") as raw:
                    process = subprocess.Popen(command, stdout=subprocess.PIPE, text=True)
                    for line in process.stdout:
                        raw.write(line)
                        raw.flush()
                        if line.startswith("RESOURCE_REPORT "):
                            record = json.loads(line.removeprefix("RESOURCE_REPORT "))
                            record["trial"] = trial
                            result_file.write(json.dumps(record) + "\n")
                            result_file.flush()
                            print("RESOURCE_RESULT " + json.dumps(record), flush=True)
                        else:
                            print(line.rstrip(), flush=True)
                    if process.wait() != 0:
                        raise RuntimeError(f"workload {case} failed")
    print("RESOURCE_COMPLETE", flush=True)


if __name__ == "__main__":
    main()
