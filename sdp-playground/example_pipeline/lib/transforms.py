"""DataFrame transformations shared by the pipeline and its unit tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_even(df: DataFrame) -> DataFrame:
    """Keep rows whose id is even."""
    return df.filter(F.col("id") % 2 == 0)


def add_doubled(df: DataFrame) -> DataFrame:
    """Add a column containing twice the id."""
    return df.withColumn("doubled", F.col("id") * 2)


def filter_large_doubled(df: DataFrame, threshold: int = 20) -> DataFrame:
    """Keep rows whose doubled value meets the threshold."""
    return df.filter(F.col("doubled") >= threshold)
