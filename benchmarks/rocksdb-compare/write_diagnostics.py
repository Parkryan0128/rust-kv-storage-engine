"""Stage and syscall attribution; profiling/strace results are not speed scores."""
import argparse
import ctypes
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time


def distribution(ns):
    ns = sorted(ns)
    return {"count": len(ns), "total_ms": sum(ns) / 1e6,
            "p50_us": ns[(len(ns)-1)//2] / 1e3,
            "p99_us": ns[(len(ns)-1)*99//100] / 1e3, "max_us": ns[-1] / 1e3}


def run(binary, directory, config, output, trace=None, mode="write"):
    command = [str(binary), mode, str(directory), json.dumps(config)]
    if trace:
        command = ["strace", "-f", "-yy", "-qq", "-T", "-s", "32", "-o", str(trace),
                   "-e", "trace=write,pwrite64,fsync,fdatasync,fallocate,ftruncate"] + command
    done = subprocess.run(command, capture_output=True, text=True, timeout=120)
    output.write_text(done.stdout + done.stderr)
    if done.returncode:
        print(done.stdout[-4000:] + done.stderr[-4000:], flush=True)
    done.check_returncode()
    assert "WORKLOAD_VERIFIED" in done.stdout
    row = next(json.loads(s.removeprefix("WORKLOAD_RESULT ")) for s in done.stdout.splitlines()
               if s.startswith("WORKLOAD_RESULT "))
    timing = next((json.loads(s.removeprefix("WRITE_TIMINGS ")) for s in done.stdout.splitlines()
                   if s.startswith("WRITE_TIMINGS ")), None)
    shutil.rmtree(directory)
    return row, timing


def file_probe(path, allocation, sync):
    # Same frame size as 32 x (8-byte key + 128-byte value + 17-byte record)
    # plus a 16-byte batch envelope and 12-byte checksum header. Synthetic file,
    # NOT an engine WAL: logical preallocation would require recovery support.
    payload = bytes((i % 251 for i in range(4924)))
    phases = {"write": [], "sync": []}
    start = time.perf_counter_ns()
    with path.open("x+b", buffering=0) as file:
        os.write(file.fileno(), b"PROBE001")
        if allocation == "keep-size":
            libc = ctypes.CDLL(None, use_errno=True)
            libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong]
            libc.fallocate.restype = ctypes.c_int
            if libc.fallocate(file.fileno(), 1, 0, 4 * 1024 * 1024):
                raise OSError(ctypes.get_errno(), "fallocate KEEP_SIZE failed")
        elif allocation == "logical":
            os.posix_fallocate(file.fileno(), 0, 4 * 1024 * 1024)
        os.fsync(file.fileno())
        setup_ns = time.perf_counter_ns() - start
        for _ in range(512):
            start = time.perf_counter_ns()
            view = memoryview(payload)
            while view:
                n = os.write(file.fileno(), view)
                assert n > 0
                view = view[n:]
            phases["write"].append(time.perf_counter_ns() - start)
            start = time.perf_counter_ns()
            (os.fsync if sync == "fsync" else os.fdatasync)(file.fileno())
            phases["sync"].append(time.perf_counter_ns() - start)
        size = file.tell()
        file.seek(8)
        assert file.read(512 * len(payload)) == payload * 512
    path.unlink()
    return {"allocation": allocation, "sync": sync, "setup_ms": setup_ns / 1e6,
            "written_bytes": size, "phases": {k: distribution(v) for k, v in phases.items()}}, phases


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["engine", "rocksdb", "profile"]
    binaries = {n: Path("comparison-bin", "workloads-" + n).resolve() for n in names}
    config = {"keys": 16384, "value": 128, "batch": 32, "workload": "batch", "fixed": False}
    results = []

    def emit(kind, row):
        results.append({"kind": kind, **row})
        print("DIAG_RESULT " + json.dumps(results[-1]), flush=True)

    with tempfile.TemporaryDirectory(prefix="kv-write-diag-", dir=os.environ.get("KV_BENCH_DIR")) as root:
        root = Path(root)
        env = {"commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
               "filesystem": subprocess.check_output(["findmnt", "-T", str(root), "-o", "SOURCE,FSTYPE,OPTIONS"], text=True),
               "cpu": subprocess.check_output(["lscpu"], text=True),
               "limits": "Shared runner, 6 trials. Profiling and strace perturb timing. Synthetic allocation controls do not establish an engine fix or WAL compatibility."}
        env["measurement_path_policy"] = "fixed-v1"
        (args.output / "environment.json").write_text(json.dumps(env, indent=2))
        print("DIAG_ENV " + json.dumps(env), flush=True)
        for trial in range(6):
            order = trial % 3
            for name in names[order:] + names[:order]:
                label = f"batch32-{trial+1}-{name}"
                row, timing = run(binaries[name], root / "probe", config, args.output / (label + ".log"))
                emit("batch", {"backend": name, "trial": trial+1, "metrics": row["metrics"]})
                if timing:
                    samples, phases = timing["samples"], timing["phases"]
                    assert len(samples) == 512
                    summaries = {p: distribution([r["phase_ns"][i] for r in samples]) for i, p in enumerate(phases)}
                    slowest = sorted(samples, key=lambda r: r["total_ns"], reverse=True)[:10]
                    emit("stages", {"trial": trial+1, "phases": summaries, "total": distribution([r["total_ns"] for r in samples]),
                                    "slowest": [{"total_us": r["total_ns"] / 1000, **{p: r["phase_ns"][i]/1000 for i, p in enumerate(phases)}} for r in slowest]})
        modes = [(a, s) for a in ["grow", "keep-size", "logical"] for s in ["fsync", "fdatasync"]]
        for trial in range(6):
            for allocation, sync in modes[trial:] + modes[:trial]:
                label = f"probe-{trial+1}-{allocation}-{sync}"
                row, raw = file_probe(root / "probe", allocation, sync)
                (args.output / (label + ".json")).write_text(json.dumps(raw))
                emit("file-probe", {"trial": trial+1, **row})
        for name in ["engine", "rocksdb"]:
            trace = args.output / (name + ".strace")
            run(binaries[name], root / "probe", config, args.output / (name + "-traced.log"), trace)
            calls = []
            allocation = []
            for line in trace.read_text().splitlines():
                if "fallocate(" in line or "ftruncate(" in line:
                    allocation.append(line)
                m = re.search(r"\b(write|pwrite64|fsync|fdatasync)\(.*<([0-9.]+)>$", line)
                if m and re.search(r"\d+\.(wal|log)(?:>| )", line):
                    calls.append({"call": m[1], "us": float(m[2])*1e6, "line": line})
            emit("syscalls", {"backend": name, "counts": {c: sum(r["call"]==c for r in calls) for c in ["write", "pwrite64", "fsync", "fdatasync"]},
                              "slowest": sorted(calls, key=lambda r: r["us"], reverse=True)[:10], "allocation": allocation[:20]})
    (args.output / "summary.json").write_text(json.dumps(results, indent=2))
    print(f"DIAG_COMPLETE records={len(results)}", flush=True)


if __name__ == "__main__":
    main()
