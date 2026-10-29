"""Fixtures must survive child failures and must never replace an existing path."""
from pathlib import Path
import tempfile
import unittest
from measurement_paths import fixed_fixture_path


class MeasurementPaths(unittest.TestCase):
    def test_success_and_exception_restore_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = root / "fixture"
            fixture.mkdir()
            (fixture / "data").write_bytes(b"unchanged fixture")
            for fail in [False, True]:
                try:
                    with fixed_fixture_path(fixture, root) as path:
                        self.assertEqual(path, root / "probe")
                        self.assertFalse(fixture.exists())
                        self.assertEqual((path / "data").read_bytes(), b"unchanged fixture")
                        if fail:
                            raise RuntimeError("child failure")
                except RuntimeError:
                    self.assertTrue(fail)
                self.assertEqual((fixture / "data").read_bytes(), b"unchanged fixture")
                self.assertFalse((root / "probe").exists())

    def test_existing_target_is_preserved(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = root / "fixture"
            fixture.mkdir()
            target = root / "probe"
            target.mkdir()
            (target / "keep").write_bytes(b"existing")
            with self.assertRaises(FileExistsError):
                with fixed_fixture_path(fixture, root):
                    self.fail("must not yield")
            self.assertTrue(fixture.is_dir())
            self.assertEqual((target / "keep").read_bytes(), b"existing")

    def test_nested_fixture_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            fixture = root / "parent" / "fixture"
            fixture.mkdir(parents=True)
            with self.assertRaises(ValueError):
                with fixed_fixture_path(fixture, root):
                    self.fail("must not yield")
            self.assertTrue(fixture.is_dir())


if __name__ == "__main__":
    unittest.main()
