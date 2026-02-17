"""Tests for config.Config."""

from zerobus_ingest.config import Config


def test_databricks_returns_dict_with_expected_keys():
    """Config.databricks() returns a dict with all expected keys."""
    result = Config.databricks()
    assert isinstance(result, dict)
    expected = {
        "host",
        "workspace_id",
        "workspace_url",
        "zerobus_client_id",
        "zerobus_client_secret",
        "token",
        "region",
        "catalog",
        "schema",
        "table",
    }
    assert set(result.keys()) == expected


def test_databricks_reads_env_vars(monkeypatch):
    """Config.databricks() reads values from environment."""
    monkeypatch.setenv("DATABRICKS_HOST", "test-host")
    monkeypatch.setenv("UC_CATALOG", "main")
    result = Config.databricks()
    assert result["host"] == "test-host"
    assert result["catalog"] == "main"


def test_databricks_missing_env_returns_none(monkeypatch):
    """Missing env vars appear as None."""
    for key in (
        "DATABRICKS_HOST",
        "UC_CATALOG",
        "UC_SCHEMA",
        "UC_TABLE",
    ):
        monkeypatch.delenv(key, raising=False)
    result = Config.databricks()
    assert result["host"] is None
    assert result["catalog"] is None
