---
name: pyspark-testing
description: Write fast, deterministic PySpark unit tests with pytest. Use when adding or editing tests for PySpark DataFrame transformations, setting up a shared SparkSession fixture, or scaffolding a new test module under `tests/`.
---

# PySpark Testing with pytest

Test patterns for PySpark transformation functions in this repo. Built around a shared `SparkSession` fixture in `conftest.py` and class-organized tests under `tests/`.

## When to Use

- Adding tests for a pure DataFrame-in / DataFrame-out function.
- Scaffolding a new `tests/test_<module>.py` file.
- Setting up the SparkSession fixture in a fresh project.
- Debugging slow test suites caused by per-test Spark startup.

## Fixture: Shared SparkSession in `conftest.py`

Use a **session-scoped** `SparkSession` so the ~5–10s startup cost is paid once per test run, not per test. Keep it small and quiet.

```python
"""Shared pytest fixtures."""

from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    """Session-scoped local SparkSession suitable for unit tests."""
    session = (
        SparkSession.builder
        .appName("<project>-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()
```

Key choices:

- `scope="session"` — single session for the whole `pytest` invocation.
- `local[2]` — 2 cores: enough to catch parallelism bugs without slowing CI.
- `spark.sql.shuffle.partitions=2` — avoid the 200-partition default that kills small-test speed.
- `spark.ui.enabled=false` — no port binding, no UI thread overhead.
- `try/finally` with `session.stop()` — clean shutdown even on test failure.

## Test File Layout

- One test module per production module: `lib/transforms.py` → `tests/test_transforms.py`.
- One **test class per function** under test: `TestFilterEven`, `TestAddDoubled`.
- Inject the fixture by name and type-annotate it:

```python
class TestFilterEven:
    def test_filters_to_even_ids(self, spark: SparkSession) -> None:
        df = spark.range(10)
        result = sorted(row["id"] for row in filter_even(df).collect())
        assert result == [0, 2, 4, 6, 8]
```

## Test Data: Two Building Blocks

**`spark.range(n)`** — sequential `id` column. Use for size/threshold tests.

```python
df = spark.range(10)  # id: 0..9
```

**`spark.createDataFrame(rows, schema)`** — explicit data. Use for shape, mixed values, or specific column names.

```python
df = spark.createDataFrame([(0, 0), (5, 10), (10, 20)], ["id", "doubled"])
```

## Assertion Patterns

Always make assertions **deterministic** — Spark does not guarantee row order.

| Goal | Pattern |
|------|---------|
| Compare row values | `sorted(row["col"] for row in df.collect())` |
| Compare row tuples | `sorted((row["a"], row["b"]) for row in df.collect())` |
| Build a lookup | `{row["id"]: row["other"] for row in df.collect()}` |
| Row count | `df.count() == N` |
| Column presence | `"col" in df.columns` |
| Schema preserved | `result.schema == input.schema` |

Avoid `df.collect() == [...]` without sorting — it will flake.

## Parametrize for Multi-Input Cases

For functions with a parameter sweep (thresholds, flags, modes), use `@pytest.mark.parametrize`:

```python
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
```

## Edge Case Checklist

For every public transform, write tests covering:

- [ ] Happy path with representative data.
- [ ] **Empty input** (`spark.range(0)` or `createDataFrame([], schema)`).
- [ ] All rows filtered **out** (no matches).
- [ ] All rows filtered **in** (everything passes through).
- [ ] **Schema preservation** if the function shouldn't change shape.
- [ ] **Column added** check when the function projects a new column.
- [ ] Default-argument behavior when applicable.

## Pipeline Composition Test

Add one integration-style test that composes the full chain to catch ordering/contract regressions:

```python
class TestPipelineComposition:
    def test_full_chain_matches_sql_view(self, spark: SparkSession) -> None:
        source = spark.range(20)
        out = filter_large_doubled(add_doubled(filter_even(source)))
        result = sorted((row["id"], row["doubled"]) for row in out.collect())
        assert result == [(10, 20), (12, 24), (14, 28), (16, 32), (18, 36)]
```

## Running

From the project root (uv-managed):

```bash
uv run pytest                                 # all tests
uv run pytest tests/test_transforms.py -v     # one file, verbose
uv run pytest -k "filter_even"                # by keyword
uv run pytest --cov=lib --cov-branch --cov-report=term-missing
```

## Anti-Patterns

- **Per-test SparkSession.** Use the session-scoped fixture; never call `SparkSession.builder.getOrCreate()` inside a test.
- **Order-sensitive equality.** Always sort before comparing collected rows.
- **Asserting on non-deterministic columns.** Don't compare on `current_timestamp()`, random UUIDs, or input-file paths without pinning them.
- **Decorated SDP functions in tests.** Test the **pure** underlying transform (e.g. `lib.transforms.filter_even`), not the `@sdp.materialized_view` wrapper. SDP decorators require an active runtime.
- **Hidden state between tests.** No global temp views, no caching that survives a test. If you must, drop/uncache in a `yield`-style fixture.
