# sdp-playground

A [uv](https://docs.astral.sh/uv/)-managed Python project for learning and
experimenting with [Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
on PySpark **4.1.1**.

## Layout

```text
sdp-playground/
├── pyproject.toml          # uv project + pytest config
├── uv.lock
├── example_pipeline/       # an SDP pipeline project
│   ├── spark-pipeline.yml  # pipeline spec (name, storage, libraries)
│   ├── lib/                # pure transform functions (testable, NOT loaded by SDP)
│   │   └── transforms.py
│   └── transformations/    # SDP-loaded sources (Python + SQL)
│       ├── example_python_materialized_view.py
│       └── example_sql_materialized_view.sql
└── tests/                  # pytest tests for lib/
    ├── conftest.py         # session-scoped SparkSession fixture
    └── test_transforms.py
```

## Requirements

- Python 3.10+
- Java 17 (PySpark 4.1.1 requirement)
- [uv](https://docs.astral.sh/uv/)

## Setup

From the repo root:

```bash
cd sdp-playground
uv sync
```

This installs `pyspark[pipelines]==4.1.1` along with `pytest` and `pytest-cov`
into a local `.venv/`.

## Running the example pipeline

The `example_pipeline/` directory is a self-contained SDP pipeline project.
It defines four datasets:

| Dataset | Kind | Source |
|---|---|---|
| `source_numbers` | materialized view (Python) | `spark.range(20)` |
| `even_numbers` | materialized view (Python) | `filter_even(source_numbers)` |
| `doubled_even_numbers` | materialized view (Python) | `add_doubled(even_numbers)` |
| `large_doubled_even_numbers` | materialized view (SQL) | filters `doubled >= 20` |

### Validate the graph (no data written)

```bash
cd example_pipeline
uv run spark-pipelines dry-run
```

### Execute the pipeline

```bash
cd example_pipeline
uv run spark-pipelines run
```

> **Note:** The `storage` field in `spark-pipeline.yml` must be an absolute URI
> (`file://`, `s3a://`, `hdfs://`, …). Update it if you clone this repo to a
> different path.

## Running the tests

From `sdp-playground/`:

```bash
uv run pytest --cov=lib --cov-branch --cov-report=term-missing
```

For an HTML coverage report:

```bash
uv run pytest --cov=lib --cov-branch --cov-report=html
open htmlcov/index.html
```

### Why are tests in `lib/` and not `transformations/`?

`@dp.materialized_view` (and friends) raise
`GRAPH_ELEMENT_DEFINED_OUTSIDE_OF_DECLARATIVE_PIPELINE` if invoked outside an
active SDP runtime — which means the modules under `transformations/` cannot be
imported from a unit test.

The pattern this project uses:

- **Pure transforms** (e.g. `filter_even`, `add_doubled`) live in
  `example_pipeline/lib/transforms.py` and are unit-tested directly.
- **SDP entry points** in `example_pipeline/transformations/*.py` simply wire
  those pure transforms into `@dp.materialized_view`-decorated functions and
  are validated end-to-end via `spark-pipelines dry-run` / `run`.

This keeps the SDP boundary thin and makes the bulk of the logic testable
without spinning up an SDP runtime.

## Adding a new dataset

1. Add (or extend) a pure function in `example_pipeline/lib/transforms.py`.
2. Add unit tests for it in `tests/test_transforms.py`.
3. Wire it into a new `@dp.materialized_view` (or `@dp.table`) function in
   `example_pipeline/transformations/`.
4. Run `uv run spark-pipelines dry-run` from `example_pipeline/` to confirm the
   graph still compiles.
