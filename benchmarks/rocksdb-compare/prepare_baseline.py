"""Copy only benchmark/test harnesses into the fixed baseline checkout."""
from pathlib import Path
import shutil

baseline = Path("baseline-source")
benchmark = Path("benchmarks/rocksdb-compare")
for name in ("Cargo.toml", "Cargo.lock"):
    shutil.copy2(benchmark / name, baseline / benchmark / name)
for source in (benchmark / "src").glob("*.rs"):
    shutil.copy2(source, baseline / benchmark / "src" / source.name)
shutil.copy2("src/read_profile.rs", baseline / "src/read_profile.rs")
table = baseline / "src/sstable.rs"
source = table.read_text()
assert "mod read_profile;" not in source
table.write_text(source + '\n#[cfg(test)]\n#[path = "read_profile.rs"]\nmod read_profile;\n')
