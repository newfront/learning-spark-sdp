# sdp-playground

A [uv](https://docs.astral.sh/uv/)-managed Python project for learning and
experimenting with [Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
on PySpark **4.2.1**.

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

The root [Nix + direnv development shell](../README.md#development-shell-nix--direnv)
provides these tools automatically. For a manual setup:

- Python 3.14+
- Java 17 or later
- [uv](https://docs.astral.sh/uv/)

## Setup

From the repo root:

```bash
cd sdp-playground
uv sync
```

This installs `pyspark[pipelines]==4.2.1` along with `pytest` and `pytest-cov`
into a local `.venv/`. It also installs the project's `lib` package in editable
mode, so both the SDP CLI and tests can import the shared transforms regardless
of their working directory. No `PYTHONPATH` override is needed.

The pandas dependency is constrained to `>=2.3.3,<3`: this provides Python 3.14
wheels while staying within Spark's supported pandas versions.

### VS Code

Open the repository root (`learning-spark-sdp`) in VS Code. The checked-in
workspace settings point Python and the recommended `astral-sh.ty` extension at
`sdp-playground/.venv/bin/python`; run `just sync` first to create that environment.

Install the recommended **ty** VS Code extension (`astral-sh.ty`) from the
Extensions panel. The extension uses ty from this project's dev dependencies.
To check from the repository root, run `uv run --project sdp-playground ty check --project sdp-playground .`.

If VS Code already remembers another interpreter, press **Ctrl+Shift+P**, run
**Python: Select Interpreter**, choose **Enter interpreter path...**, and select
`sdp-playground/.venv/bin/python`. This is a one-time step because VS Code keeps
existing selections instead of overriding them with the workspace default.
Run **Developer: Reload Window** to restart the extensions with these settings.

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

## SQL linting and formatting

`just sync` installs SQLFluff and sqlfmt (`shandy-sqlfmt`) as project dev
dependencies. Both are configured in `pyproject.toml`:

- SQLFluff uses the `sparksql` dialect and the raw templater for SDP SQL files.
- sqlfmt uses its Spark-compatible `polyglot` dialect and an 88-character line
  length. SQLFluff's capitalization and layout rules are aligned with sqlfmt.

SQL keywords, unquoted identifiers, function names, types, and boolean/null
literals use **lowercase**. String contents and quoted identifiers retain their
case.

From the repository root:

```bash
just fix         # Ruff fixes/formatting, SQLFluff fixes, sqlfmt, then SQL lint
just sql-format  # format all SQL under example_pipeline in place
just sql-lint    # lint SQL without changing files
just sql-check   # lint and check formatting (suitable for CI)
```

Use `sql-format` for formatting; `sql-check` reports any remaining lint issues.

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
