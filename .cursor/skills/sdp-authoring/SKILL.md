---
name: sdp-authoring
description: Author Spark Declarative Pipelines (SDP) the way this repo expects — pure logic in `lib/`, decorated entry points in `transformations/`, reads inside the flow body, no actions in dataset code. Use this skill whenever adding or editing a `@dp.table` / `@dp.materialized_view` / `@dp.temporary_view` / `@dp.append_flow`, wiring a new dataset into `example_pipeline/`, debugging "graph element defined outside of declarative pipeline" or "session mutation" errors, or whenever the user mentions SDP, declarative pipelines, dataflow graphs, streaming tables, materialized views, `spark-pipelines`, or `just sdp-run` / `just dry-run`.
---

# Authoring Spark Declarative Pipelines (OSS Spark 4.1)

How to write SDP datasets and flows that resolve, validate, and run in this repo's
local Spark Connect cluster. The rules here are not style preferences — each one
maps to something the SDP engine enforces in code (see the "Why" notes). Follow
them and `just dry-run` passes the first time; ignore them and the failure mode is
usually silent (a missing DAG edge) rather than a clean error.

## When to Use

- Adding a new dataset (`@dp.table`, `@dp.materialized_view`, `@dp.temporary_view`).
- Adding a flow that appends to an existing table (`@dp.append_flow`).
- Wiring a multi-stage pipeline (bronze ingest → typed views → gold aggregate).
- Debugging `GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE`,
  `SESSION_MUTATION_IN_DECLARATIVE_PIPELINE`, `PIPELINE_DATASET_WITHOUT_FLOW`, or
  `INVALID_FLOW_QUERY_TYPE.*`.
- Deciding whether something belongs in `lib/` or `transformations/`.

## The One-Paragraph Model

A decorated function declares a **dataset + a flow** that writes it. At
registration the engine calls your function to capture a *plan* (never to run it)
and **infers the dependency graph from the tables your function reads**. Then it
resolves schemas, validates (cycles, streaming-ness, every-dataset-has-a-flow), and
executes flows in topological order. You declare *what* each dataset is; the engine
derives *how* and *when*.

## The Four Hard Rules

### 1. Reads go INSIDE the flow body

The DAG is inferred from the `spark.table(...)` / `spark.readStream...` calls the
engine observes while calling your function. Put them in the body.

```python
# CORRECT — engine sees the read, infers bronze_events as a parent
@dp.materialized_view
def purchase():
    df = spark.table("bronze_events")
    return df.filter(F.col("event_name") == "purchase")
```

```python
# WRONG — passing the upstream as an argument hides the read.
# The edge silently disappears; the view may run before its parent exists.
def purchase(bronze):                 # ← not a flow the engine can resolve
    return bronze.filter(...)
```

> **Why:** `CoreDataflowNodeProcessor` + `FlowResolver` build the graph by invoking
> the flow function and recording what it loaded. No read observed → no edge.

### 2. Pure logic in `lib/`, decorators in `transformations/`

Decorators only work while a pipeline run is active (the registry lives in a
thread-local `ContextVar`). Importing a `transformations/` module **anywhere else —
including a unit test — raises** `GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE`.

- `lib/transforms.py` — plain `DataFrame -> DataFrame` (or `Column`) functions. No
  decorators, no module-level session capture. Import-safe and unit-testable.
- `transformations/*.py` — thin `@dp.*` entry points that read upstreams and call
  `lib/` functions.

```python
# lib/transforms.py  (testable)
def select_event_type(df, event_name):
    return df.filter(F.col("event_name") == F.lit(event_name))

# transformations/silver.py  (entry point)
from lib.transforms import select_event_type

@dp.materialized_view
def purchase():
    return select_event_type(spark.table("bronze_events"), "purchase")
```

> **Why:** `get_active_graph_element_registry()` raises when no run is active.
> This is the root cause of the repo's "never import `transformations/` in tests"
> rule — it's a consequence of the engine design, not a convention.

### 3. No actions or side effects inside a flow function

A flow function must **build** a DataFrame, never **execute** one. The engine blocks
analysis/execution RPCs during registration, so these fail immediately:

`collect()`, `count()`, `toPandas()`, `show()`, `save()`, `saveAsTable()`,
`start()`, `toTable()`, `write...`, and `spark.sql(...)` commands.

Also forbidden (session mutations): `spark.conf.set`, `setCurrentCatalog/Database`,
`create*TempView`, `dropTempView`, UDF registration. Set Spark conf via the
`spec.configuration` block or the decorator's `spark_conf=` instead.

> **Why:** `block_connect_access` and `block_session_mutations` enforce this at the
> transport layer so definitions stay deterministic and side-effect-free.

### 4. Get the streaming-ness right

| You want | Use | Reads must be |
|----------|-----|---------------|
| Incremental, append-only ingest | `@dp.table` (streaming table) | `spark.readStream...` |
| Recomputed-from-scratch derived table | `@dp.materialized_view` | `spark.read` / `spark.table(...)` |
| Pipeline-scoped intermediate, not persisted | `@dp.temporary_view` | either |
| Extra flow into an existing streaming table | `@dp.append_flow(target=...)` | `spark.readStream...` |

A batch read into a streaming table (or a streaming read into an MV) fails
validation with `INVALID_FLOW_QUERY_TYPE.*`.

## Decorator Reference (OSS Spark 4.1 supported args)

```python
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.active()   # OSS: active session. NOT a Databricks-injected `spark`.

@dp.materialized_view(
    name="session_facts",              # default: function name
    comment="One row per session.",
    spark_conf={"spark.sql.shuffle.partitions": "8"},
    table_properties={"quality": "gold"},
    partition_cols=["session_date"],
    schema="session_id STRING, ...",   # StructType or DDL string; optional
    format="delta",                    # delta, parquet, ...
)
def session_facts():
    ...
```

`@dp.table` takes the same args plus `cluster_by`. `@dp.temporary_view` takes
`name`, `comment`, `spark_conf`. `@dp.append_flow` takes `target` (required),
`name`, `spark_conf`.

Supported today (per `docs/links.md`): `name`, `comment`, `spark_conf`,
`table_properties`, `partition_cols`, `cluster_by`, `schema`, `format`. Don't reach
for Databricks/Lakeflow-only params (expectations, `@dlt.expect`, etc.) — they
aren't in OSS 4.1.

## Standard Pipeline Shape

A reliable layering that the engine's topological executor handles cleanly:

```
@dp.table            bronze_<x>     ← streaming ingest (readStream), partitioned
@dp.materialized_view silver_<type> ← filter/typed projection of bronze
@dp.materialized_view gold_<agg>    ← join/aggregate of silver tables
```

Each layer reads the previous one *inside the body*; the engine infers the chain.

## Authoring Workflow (incremental — let the gates decide correctness)

1. Write/extend the pure transform in `lib/transforms.py`.
2. Add parametrized tests in `tests/test_transforms.py`; `just test` until green.
3. Add **one** `@dp.*` entry point in `transformations/` that reads its upstream in
   the body and calls the `lib/` function.
4. `just dry-run sdp-playground/example_pipeline` — confirm it resolves and the
   intended parent appears in the graph. Fix before adding the next dataset.
5. Repeat 3–4 one dataset at a time.
6. `just sdp-run sdp-playground/example_pipeline` end-to-end (full refresh).

Run on a feature branch (`feat/...`), never `main`.

## Self-Check Before Committing

For every flow function you added:

- [ ] Every upstream is a `spark.table(...)` / `readStream` **inside the body**, not
      an argument.
- [ ] No `collect`/`count`/`show`/`save`/`saveAsTable`/`start`/`toTable`/`toPandas`/
      `write`/`spark.sql`.
- [ ] No session mutation (`conf.set`, `setCurrent*`, temp-view create/drop, UDF reg).
- [ ] Streaming table reads stream; MV reads batch.
- [ ] Reused logic lives in `lib/` and is unit-tested; nothing imports a decorator
      into a test.
- [ ] `just dry-run` is clean.

## Anti-Patterns

- **Upstream as a function parameter.** Breaks DAG inference silently. Read inside.
- **Importing `transformations/` in a test.** Raises at import. Test `lib/` instead.
- **An action "just to debug" inside a flow.** Blocked by the engine. Debug in a
  scratch script or a test, not in the dataset definition.
- **`spark.conf.set` inside a flow.** Use `spark_conf=` on the decorator or
  `configuration:` in `spark-pipeline.yml`.
- **Assuming a global `spark`.** In OSS, capture `SparkSession.active()`; there is no
  Databricks-injected session.
- **Big-bang pipelines.** Adding ten datasets then running `dry-run` once buries the
  one broken edge. Add one, validate, repeat.

## Related

- `pyspark-testing` skill — how to test the `lib/` functions this skill produces.
- `proto-to-sdp` skill — column paths for protobuf-sourced events (the `oneof`
  payload → nullable-struct projection pattern).
- `knowledge/pipelines/` (if present) — the engine internals these rules derive from.

