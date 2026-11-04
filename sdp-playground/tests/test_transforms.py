"""Exercise pure transforms with real Spark DataFrames, outside the SDP runtime."""

import pytest
from lib.transforms import add_doubled, filter_even, filter_large_doubled
from pyspark.sql import SparkSession


class TestFilterEven:
    @pytest.mark.parametrize(
        ("ids", "expected"),
        [
            pytest.param(list(range(10)), [0, 2, 4, 6, 8], id="mixed"),
            pytest.param([], [], id="empty"),
            pytest.param([1, 3, 5], [], id="all-odd"),
            pytest.param([0, 2, 4], [0, 2, 4], id="all-even"),
            pytest.param([-2, -2, -1, 0], [-2, -2, 0], id="negative-duplicates"),
        ],
    )
    def test_filters_rows_and_preserves_schema(
        self, spark: SparkSession, ids: list[int], expected: list[int]
    ) -> None:
        source = spark.createDataFrame([(value,) for value in ids], "id long")

        result = filter_even(source)

        assert result.schema == source.schema
        assert sorted(row.id for row in result.collect()) == expected


class TestAddDoubled:
    @pytest.mark.parametrize(
        ("ids", "expected"),
        [
            pytest.param([0, 1, 2], [(0, 0), (1, 2), (2, 4)], id="multiple-rows"),
            pytest.param([0], [(0, 0)], id="zero"),
            pytest.param([], [], id="empty"),
            pytest.param([-2, -2], [(-2, -4), (-2, -4)], id="negative-duplicates"),
        ],
    )
    def test_adds_doubled_column(
        self, spark: SparkSession, ids: list[int], expected: list[tuple[int, int]]
    ) -> None:
        source = spark.createDataFrame([(value,) for value in ids], "id long")

        result = add_doubled(source)

        assert result.columns == ["id", "doubled"]
        assert sorted((row.id, row.doubled) for row in result.collect()) == expected


class TestFilterLargeDoubled:
    @pytest.mark.parametrize(
        ("threshold", "expected_ids"),
        [
            pytest.param(0, [0, 5, 10, 15], id="keep-all"),
            pytest.param(10, [5, 10, 15], id="inclusive-10"),
            pytest.param(20, [10, 15], id="inclusive-20"),
            pytest.param(100, [], id="keep-none"),
        ],
    )
    def test_threshold_variations(
        self,
        spark: SparkSession,
        threshold: int,
        expected_ids: list[int],
    ) -> None:
        source = spark.createDataFrame(
            [(0, 0), (5, 10), (10, 20), (15, 30)],
            ["id", "doubled"],
        )

        result = filter_large_doubled(source, threshold)

        assert result.schema == source.schema
        assert sorted(row.id for row in result.collect()) == expected_ids

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
