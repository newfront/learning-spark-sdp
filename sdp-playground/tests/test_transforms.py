"""Tests for the pure transformation functions in ``lib.transforms``.

These tests exercise the DataFrame logic directly without needing an
active SDP runtime, since the SDP decorators are only applied in
``transformations/example_python_materialized_view.py``.
"""

import pytest
from pyspark.sql import SparkSession

from lib.transforms import add_doubled, filter_even, filter_large_doubled


class TestFilterEven:
    def test_filters_to_even_ids(self, spark: SparkSession) -> None:
        df = spark.range(10)
        result = sorted(row["id"] for row in filter_even(df).collect())
        assert result == [0, 2, 4, 6, 8]

    def test_empty_input_returns_empty(self, spark: SparkSession) -> None:
        df = spark.range(0)
        assert filter_even(df).count() == 0

    def test_only_odd_input_returns_empty(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(1,), (3,), (5,)], ["id"])
        assert filter_even(df).count() == 0

    def test_only_even_input_passes_through(self, spark: SparkSession) -> None:
        df = spark.createDataFrame([(0,), (2,), (4,)], ["id"])
        assert filter_even(df).count() == 3

    def test_preserves_schema(self, spark: SparkSession) -> None:
        df = spark.range(4)
        assert filter_even(df).schema == df.schema


class TestAddDoubled:
    def test_adds_doubled_column(self, spark: SparkSession) -> None:
        df = spark.range(3)
        result = {row["id"]: row["doubled"] for row in add_doubled(df).collect()}
        assert result == {0: 0, 1: 2, 2: 4}

    def test_doubled_column_is_added(self, spark: SparkSession) -> None:
        df = spark.range(1)
        out = add_doubled(df)
        assert "doubled" in out.columns
        assert "id" in out.columns

    def test_handles_empty_input(self, spark: SparkSession) -> None:
        df = spark.range(0)
        out = add_doubled(df)
        assert out.count() == 0
        assert "doubled" in out.columns


class TestFilterLargeDoubled:
    @pytest.mark.parametrize(
        ("threshold", "expected_ids"),
        [
            (0, [0, 5, 10, 15]),
            (10, [5, 10, 15]),
            (20, [10, 15]),
            (100, []),
        ],
    )
    def test_threshold_variations(
        self,
        spark: SparkSession,
        threshold: int,
        expected_ids: list[int],
    ) -> None:
        df = spark.createDataFrame(
            [(0, 0), (5, 10), (10, 20), (15, 30)],
            ["id", "doubled"],
        )
        result = sorted(row["id"] for row in filter_large_doubled(df, threshold).collect())
        assert result == expected_ids

    def test_default_threshold_is_20(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(0, 0), (10, 20), (20, 40)],
            ["id", "doubled"],
        )
        result = sorted(row["id"] for row in filter_large_doubled(df).collect())
        assert result == [10, 20]


class TestPipelineComposition:
    """Integration-style test composing the full transformation chain."""

    def test_full_chain_matches_sql_view(self, spark: SparkSession) -> None:
        source = spark.range(20)
        out = filter_large_doubled(add_doubled(filter_even(source)))
        result = sorted((row["id"], row["doubled"]) for row in out.collect())
        assert result == [
            (10, 20),
            (12, 24),
            (14, 28),
            (16, 32),
            (18, 36),
        ]
