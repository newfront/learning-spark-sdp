---
name: delta-lake
description: >-
  Work with Delta Lake tables in PySpark — SparkSession configuration, reading
  and writing Delta tables, schema evolution, MERGE/upsert, time travel, OPTIMIZE,
  VACUUM, partitioning, liquid clustering, batched writes, and testing patterns.
  Use when creating, reading, or writing Delta tables, configuring Delta in Spark,
  performing upserts/merges, managing table maintenance, or asking about Delta Lake
  best practices in PySpark.
---

# Delta Lake with PySpark

Patterns and best practices for using Delta Lake with PySpark, drawn from
production usage and the official Delta Lake APIs.

## 1. SparkSession configuration

Delta requires three Spark configs: the JAR, the SQL extension, and the catalog.

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("my-app")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.1.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
```

Key points:
- Match `delta-spark_2.13:X.Y.Z` to your Spark version (e.g. `4.1.0` for Spark 4.x).
- `DeltaSparkSessionExtension` enables Delta SQL commands (MERGE, OPTIMIZE, etc.).
- `DeltaCatalog` lets `spark.catalog` and SQL DDL work with Delta tables.
- On Databricks clusters, Delta is pre-installed — skip `spark.jars.packages`.

### Adding spark-protobuf (optional)

When deserializing Protobuf into DataFrames before writing Delta:

```python
.config(
    "spark.jars.packages",
    "io.delta:delta-spark_2.13:4.1.0,"
    "org.apache.spark:spark-protobuf_2.13:4.1.1",
)
```

### Python dependency

```toml
# pyproject.toml
dependencies = [
    "pyspark==4.1.1",
    "delta-spark==4.1.0",
]
```

## 2. Writing Delta tables

### Basic write

```python
df.write.format("delta").mode("overwrite").save("output/my_table")
```

### Write modes

| Mode | Behavior |
|------|----------|
| `overwrite` | Replace all data (and optionally schema) |
| `append` | Add rows; schema must be compatible |
| `ignore` | No-op if table exists |
| `error` / `errorifexists` | Fail if table exists (default) |

### Write with partitioning

```python
df.write.format("delta").mode("overwrite").partitionBy("date").save(path)
```

Use `partitionBy` when queries almost always filter on that column and cardinality
is low-to-moderate (e.g. date, region). For high-cardinality or evolving access
patterns, prefer liquid clustering (see section 8).

### Helper function pattern

```python
from pathlib import Path
from pyspark.sql import DataFrame

def write_df_to_delta(
    df: DataFrame, delta_path: str | Path, mode: str = "overwrite"
) -> None:
    Path(delta_path).parent.mkdir(parents=True, exist_ok=True)
    df.write.format("delta").mode(mode).save(str(delta_path))
```

### Batched writes for large datasets

When materializing millions of rows, batch to avoid driver OOM:

```python
batch_size = 100_000
total = len(all_records)
for start in range(0, total, batch_size):
    end = min(start + batch_size, total)
    batch = all_records[start:end]
    df = create_dataframe(batch, spark)
    mode = "overwrite" if start == 0 else "append"
    write_df_to_delta(df, output_path, mode=mode)
```

First batch overwrites (creates the table), subsequent batches append.

## 3. Reading Delta tables

```python
df = spark.read.format("delta").load("output/my_table")
```

### SQL access

```python
spark.sql("CREATE TABLE my_table USING DELTA LOCATION 'output/my_table'")
df = spark.sql("SELECT * FROM my_table WHERE date = '2026-04-01'")
```

## 4. Time travel

Read historical versions of a Delta table.

```python
# By version number
df = spark.read.format("delta").option("versionAsOf", 0).load(path)

# By timestamp
df = (
    spark.read.format("delta")
    .option("timestampAsOf", "2026-04-01 12:00:00")
    .load(path)
)
```

View version history:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, path)
dt.history().show(truncate=False)
```

## 5. Schema evolution

### Auto-merge on write

```python
df.write.format("delta").mode("append") \
    .option("mergeSchema", "true") \
    .save(path)
```

Or enable globally:

```python
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

### Overwrite with new schema

```python
df.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save(path)
```

### Column mapping (rename/drop support)

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);
ALTER TABLE my_table RENAME COLUMN old_name TO new_name;
ALTER TABLE my_table DROP COLUMN col_to_remove;
```

## 6. MERGE (upsert)

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, path)

target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

### Merge with conditions

```python
target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id AND t.date = s.date"
).whenMatchedUpdate(
    condition="s.updated_at > t.updated_at",
    set={"value": "s.value", "updated_at": "s.updated_at"},
).whenNotMatchedInsertAll() \
 .execute()
```

### Merge with schema evolution

```python
target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .withSchemaEvolution() \
 .execute()
```

### Merge with delete

```python
target.alias("t").merge(
    source_df.alias("s"),
    "t.id = s.id"
).whenMatchedDelete(condition="s.is_deleted = true") \
 .whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

## 7. Table maintenance

### OPTIMIZE (compact small files)

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, path)
dt.optimize().executeCompaction()
```

With Z-ORDER (improves predicate pushdown on chosen columns):

```python
dt.optimize().executeZOrderBy("date", "user_id")
```

### VACUUM (remove old files)

```python
dt.vacuum(retentionHours=168)  # default 7 days
```

**Run VACUUM after OPTIMIZE** — OPTIMIZE compacts files but does not delete the
old ones; VACUUM cleans them up.

Safety: Delta prevents vacuuming files newer than the retention threshold. To
override (dangerous — breaks time travel):

```python
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
dt.vacuum(retentionHours=0)
```

### Recommended maintenance cadence

| Operation | Frequency | Notes |
|-----------|-----------|-------|
| OPTIMIZE | Daily or after large ingestion | Compacts small files |
| VACUUM | Weekly | Cleans up after OPTIMIZE |

On Databricks with Unity Catalog, **predictive optimization** runs both
automatically for managed tables.

## 8. Liquid clustering

An alternative to `partitionBy` + Z-ORDER. Cannot be combined with either.

```sql
CREATE TABLE events (
  id BIGINT, event_type STRING, ts TIMESTAMP, region STRING
) USING DELTA
CLUSTER BY (region, event_type);
```

Change clustering columns without rewriting data:

```sql
ALTER TABLE events CLUSTER BY (ts, region);
```

Then run `OPTIMIZE` to apply clustering incrementally.

Best for: high-cardinality columns, skewed data, or when access patterns change
over time.

## 9. Streaming reads and writes

### Read a Delta table as a stream

```python
stream_df = spark.readStream.format("delta").load(path)
```

### Append to a Delta table from a stream

```python
query = (
    stream_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/tmp/checkpoints/my_stream")
    .start(output_path)
)
```

## 10. Testing patterns

### Session-scoped Spark fixture (pytest)

```python
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("test")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.13:4.1.0",
        )
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
```

Use `scope="session"` — Spark startup is expensive; reuse across all tests.

### Write test artifacts for inspection

```python
def test_write_users_delta(spark):
    df = create_test_dataframe(spark)
    delta_path = Path("tests/resources/delta/users")
    delta_path.parent.mkdir(parents=True, exist_ok=True)
    df.coalesce(1).write.format("delta").mode("overwrite").save(str(delta_path))
    # Verify round-trip
    loaded = spark.read.format("delta").load(str(delta_path))
    assert loaded.count() == df.count()
```

`coalesce(1)` produces a single data file — useful for test fixtures; avoid in
production (defeats parallelism).

### Assert schema and data

```python
def test_schema(spark):
    df = spark.read.format("delta").load("tests/resources/delta/users")
    assert "uuid" in df.columns
    assert "email_address" in df.columns
    assert df.count() > 0
    row = df.first()
    assert row["uuid"] is not None
```

## Common mistakes

| Mistake | Symptom | Fix |
|---------|---------|-----|
| Missing `DeltaSparkSessionExtension` | `AnalysisException` on Delta ops | Add the three required configs |
| Wrong `delta-spark` version | `ClassNotFoundException` | Match `delta-spark_2.13:X.Y.Z` to Spark |
| `coalesce(1)` in production | Slow writes, one file per batch | Remove; let Spark partition naturally |
| VACUUM without retention buffer | Breaks concurrent readers / time travel | Keep default 7-day retention |
| OPTIMIZE without VACUUM | Disk usage grows unbounded | Run VACUUM after OPTIMIZE |
| `partitionBy` on high-cardinality column | Millions of tiny directories | Use liquid clustering or Z-ORDER instead |
| Schema mismatch on append | `AnalysisException` | Use `mergeSchema` or `overwriteSchema` |

## Additional resources

- [Delta Lake docs](https://docs.delta.io/latest/)
- [Delta Lake PySpark API](https://docs.delta.io/latest/api/python/)
- [Spark + Delta quickstart](https://docs.delta.io/latest/quick-start.html)
- For protobuf-to-Delta ingestion, see the **protobuf-with-spark** skill