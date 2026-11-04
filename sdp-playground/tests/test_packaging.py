"""The CLI must be able to import helpers without pytest's import path."""

import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "symbol", ["add_doubled", "filter_even", "filter_large_doubled"]
)
def test_transforms_import_outside_project(tmp_path: Path, symbol: str) -> None:
    # A fresh interpreter catches packaging errors that pytest's imports can hide.
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-c",
            f"from lib.transforms import {symbol}",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
