"""Shared pytest fixtures.

Provides a session-scoped local SparkSession so tests don't pay the
~5-10 second startup cost per test.
"""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local SparkSession suitable for unit tests."""
    session = (
        SparkSession.builder
        .appName("sdp-playground-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
