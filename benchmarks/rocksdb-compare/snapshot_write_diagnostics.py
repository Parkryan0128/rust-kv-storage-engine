"""Attribute durable snapshot-write costs; instrumented results are not speed scores."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from workloads import capture
from write_diagnostics import distribution, run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    names = ["engine", "rocksdb", "profile"]
    binaries = {name: Path("comparison-bin", "workloads-" + name).resolve() for name in names}
    environment = {"commit": capture("git", "rev-parse", "HEAD"), "cpu": capture("lscpu"),
                   "binaries": {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in binaries.items()},
                   "trials": 6, "limits": "Phase clocks perturb timing. Compare normal engine/RocksDB separately; phase percentages describe the instrumented engine only."}
    environment["measurement_path_policy"] = "fixed-v1"
    (args.output / "environment.json").write_text(json.dumps(environment, indent=2))
    common = dict(keys=20000, value=128, block=16384, cache=8388608, memtable=67108864,
                  span=10, batch=32, fixed=True, queries=2000)
    with tempfile.TemporaryDirectory(prefix="kv-snapshot-diag-", dir=os.environ.get("KV_BENCH_DIR")) as temp:
        root = Path(temp)
        with (args.output / "results.jsonl").open("w") as sink:
            for trial in range(1, 7):
                for narrow in [False, True] if trial % 2 else [True, False]:
                    config = dict(common, narrow=narrow)
                    order = (trial - 1) % len(names)
                    for rank, name in enumerate(names[order:] + names[:order], 1):
                        label = f"snapshot-{'narrow' if narrow else 'full'}-{trial}-{name}"
                        row, timing = run(binaries[name], root / "probe", config,
                                          args.output / (label + ".log"), mode="snapshot-write")
                        result = dict(case="narrow" if narrow else "full", trial=trial, backend=name,
                                      order=rank, config=config, metrics=row["metrics"])
                        if timing:
                            samples, phases = timing["samples"], timing["phases"]
                            assert len(samples) == config["queries"]
                            result["phases"] = {phase: distribution([sample["phase_ns"][i] for sample in samples])
                                                for i, phase in enumerate(phases)}
                            result["batch_total"] = distribution([sample["total_ns"] for sample in samples])
                        assert (timing is not None) == (name == "profile")
                        sink.write(json.dumps(result) + "\n")
                        sink.flush()
                        print("SNAPSHOT_DIAGNOSTIC " + json.dumps(result), flush=True)


if __name__ == "__main__":
    main()
