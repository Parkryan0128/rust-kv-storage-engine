"""Use one pathname without copying large read-only fixtures for every trial."""
from contextlib import contextmanager
import os
from pathlib import Path


@contextmanager
def fixed_fixture_path(fixture, root):
    root = Path(root).resolve()
    fixture = Path(fixture).resolve()
    target = root / "probe"
    if fixture.parent != root or fixture == target:
        raise ValueError("Fixture must be a separate direct child of the private run directory")
    if target.exists():
        raise FileExistsError(target)
    fixture.rename(target)
    try:
        # Keep fixture publication I/O outside measured opens on Linux.
        if hasattr(os, "O_DIRECTORY"):
            descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        yield target
    finally:
        target.rename(fixture)
