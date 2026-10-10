"""Use identical benchmark harnesses without changing baseline library code."""
from pathlib import Path
import shutil

benchmark = Path("benchmarks/rocksdb-compare")
baseline = Path("baseline-source") / benchmark
for name in ("Cargo.toml", "Cargo.lock"):
    shutil.copy2(benchmark / name, baseline / name)
for source in (benchmark / "src").glob("*.rs"):
    shutil.copy2(source, baseline / "src" / source.name)
