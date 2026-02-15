"""Tests for main CLI (parse_args)."""

import sys

import pytest

from zerobus_ingest.main import parse_args


def test_parse_args_default_env():
    """Default --env is dev."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py"])
        args = parse_args()
    assert args.env == "dev"
    assert args.generate is False
    assert args.count == 100


def test_parse_args_env_prod():
    """--env prod is accepted."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--env", "prod"])
        args = parse_args()
    assert args.env == "prod"


def test_parse_args_generate_and_count():
    """--generate and --count are parsed."""
    with pytest.MonkeyPatch.context() as m:
        m.setattr(sys, "argv", ["main.py", "--generate", "--count", "50"])
        args = parse_args()
    assert args.generate is True
    assert args.count == 50
