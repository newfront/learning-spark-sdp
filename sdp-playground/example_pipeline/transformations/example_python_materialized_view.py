"""Example Python materialized views for the SDP demo pipeline.

The pure transformation logic lives in ``example_pipeline.lib.transforms``
so it can be unit-tested independently of the SDP runtime.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from lib.transforms import add_doubled, filter_even


@dp.materialized_view
def source_numbers() -> DataFrame:
    """Source materialized view: integers 0..19."""
    return SparkSession.active().range(20)


@dp.materialized_view
def even_numbers() -> DataFrame:
    """Even integers from ``source_numbers``."""
    return filter_even(SparkSession.active().table("source_numbers"))


@dp.materialized_view
def doubled_even_numbers() -> DataFrame:
    """Even integers with a ``doubled`` column."""
    return add_doubled(SparkSession.active().table("even_numbers"))
