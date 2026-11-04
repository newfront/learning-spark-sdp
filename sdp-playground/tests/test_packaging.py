"""The CLI must be able to import helpers without pytest's import path."""

from pathlib import Path
import subprocess
import sys


def test_transforms_import_outside_project(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            "from lib.transforms import add_doubled, filter_even, filter_large_doubled",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
