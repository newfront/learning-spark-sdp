# AGENTS.md — Project Guide for AI Agents

This file gives AI coding agents the orientation they need to work effectively
in this repository. Read it before making any changes.

---

## Agent rules and skills

Project-specific guidance for agents lives under [`.agents/`](./.agents). Read
the applicable files before working in the relevant area.

### Rules — always apply

These are non-negotiable working agreements. Follow them on every task.

| Rule | What it covers |
|---|---|
| [`.agents/rules/branching-rule.mdc`](./.agents/rules/branching-rule.mdc) | Never commit to `main`; create a `feat/`-style branch before the first change. |
| [`.agents/rules/testing-discipline.mdc`](./.agents/rules/testing-discipline.mdc) | Every change ships with tests; never delete passing tests; target ~80% branch coverage. |

### Skills — apply when relevant

Situational playbooks. Read the matching skill before doing that kind of work.

| Skill | Use when |
|---|---|
| [`.agents/skills/sdp-authoring/SKILL.md`](./.agents/skills/sdp-authoring/SKILL.md) | Adding or editing an SDP dataset/flow (`@dp.table`, `@dp.materialized_view`, etc.) or debugging dataflow-graph errors. |
| [`.agents/skills/proto-to-sdp/SKILL.md`](./.agents/skills/proto-to-sdp/SKILL.md) | Decoding protobuf clickstream data (`from_protobuf`, GA4 events, `commerce.v1.Amount`, exploding `items[]`) into Spark columns. |
| [`.agents/skills/pyspark-testing/SKILL.md`](./.agents/skills/pyspark-testing/SKILL.md) | Writing or editing PySpark unit tests with pytest and the shared `SparkSession` fixture. |
| [`.agents/skills/delta-lake/SKILL.md`](./.agents/skills/delta-lake/SKILL.md) | Reading or writing Delta tables in PySpark — SparkSession config, MERGE/upsert, schema evolution, time travel, OPTIMIZE/VACUUM, partitioning, liquid clustering. |

---

## What this project is

`learning-spark-sdp` is a hands-on learning environment for
[Spark Declarative Pipelines (SDP)](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html) —
the high-level, dataset-oriented pipeline API introduced in Apache Spark 4.x.

The primary sub-project is [`sdp-playground/`](./sdp-playground), a
[uv](https://docs.astral.sh/uv/)-managed Python project running
**PySpark 4.1.1** that contains an example SDP pipeline and a pytest test suite.

---

## Repository layout

```text
learning-spark-sdp/
├── AGENTS.md                   ← you are here
├── .agents/                    ← agent rules & skills (referenced from AGENTS.md)
│   ├── rules/                  ← always-apply working agreements
│   └── skills/                 ← situational playbooks (SDP, protobuf, testing, Delta Lake)
├── Justfile                    ← all runnable commands (see below)
├── docker-compose.yaml         ← standalone Spark 4.1.1 cluster (1 master + 3 workers)
├── README.md                   ← human-facing setup guide
└── sdp-playground/
    ├── pyproject.toml          ← uv project (pyspark[pipelines]==4.1.1, pytest)
    ├── uv.lock
    ├── example_pipeline/       ← a self-contained SDP pipeline project
    │   ├── spark-pipeline.yml  ← pipeline spec (name, storage root, libraries glob)
    │   ├── lib/
    │   │   └── transforms.py   ← pure transform functions (unit-testable)
    │   └── transformations/    ← SDP entry points (@dp.materialized_view decorated)
    │       ├── example_python_materialized_view.py
    │       └── example_sql_materialized_view.sql
    └── tests/
        ├── conftest.py         ← session-scoped SparkSession fixture
        └── test_transforms.py  ← pytest tests for lib/transforms.py
```

---

## Key concepts

### Spark Declarative Pipelines (SDP)

SDP lets you declare datasets (materialized views, tables, streaming tables) and
Spark resolves the dependency graph and executes them in order. Think of it as
dbt-style pipelines but running natively on Spark with Python and SQL.

- Python entry points use `@dp.materialized_view`, `@dp.table`, etc.
- SQL entry points are plain `.sql` files picked up by the libraries glob in
  `spark-pipeline.yml`.
- **SDP decorators raise an error if imported outside an active SDP runtime.**
  Never import from `transformations/` in unit tests. Put testable logic in
  `lib/` instead.

### Spark Connect

The Docker cluster exposes a
[Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html)
server on port **15002**. The `spark-pipelines` CLI connects to it by passing
`--remote` to the underlying `spark-submit`:

```
spark-pipelines --remote sc://localhost:15002 run …
```

This is wired up automatically by the `sdp-run`, `sdp-update`, and `dry-run`
Just recipes via the `connect_url` variable (default `sc://localhost:15002`,
overridable via `CONNECT_URL`).

> **Why not `SPARK_REMOTE=...`?** The `spark-pipelines` script is a bash
> wrapper around `spark-submit`. `spark-submit` spawns a JVM with a local
> `SparkContext` before the Python CLI starts, so `SparkSession.builder
> .getOrCreate()` returns the classic JVM session and silently ignores the
> `SPARK_REMOTE` env var. The `--remote` flag is consumed by `spark-submit`
> itself and short-circuits the JVM path, routing the run through Spark
> Connect to the Docker cluster.

---

## Running things — use `just`

All common tasks are encoded in the [`Justfile`](./Justfile). Run `just` (or
`just --list`) to see every available recipe.

| Recipe | What it does |
|---|---|
| `just cluster-up` | Start the Docker Spark cluster (detached) |
| `just cluster-down` | Stop and remove the cluster |
| `just cluster-status` | `docker compose ps` |
| `just cluster-logs` | Stream all container logs |
| `just sdp-run <path>` | Full-refresh run — drops and recreates all tables (safe for re-runs) |
| `just sdp-update <path>` | Incremental run — only recomputes stale datasets |
| `just dry-run <path>` | Validate a pipeline graph (no writes) |
| `just clean-pipeline <path>` | Delete `spark-warehouse/` and `pipeline-storage/` for a hard reset |
| `just test` | Run pytest with branch coverage |
| `just sync` | `uv sync` in `sdp-playground/` |

### Typical workflow

```bash
just cluster-up
just sdp-run sdp-playground/example_pipeline   # safe to run repeatedly
just cluster-down
```

### Re-run behaviour

`sdp-run` passes `--full-refresh-all` to `spark-pipelines`. This drops and
recreates all pipeline tables on every run, avoiding the
`LOCATION_ALREADY_EXISTS` error that occurs when the local Spark warehouse
directory persists across sessions but the in-memory metastore is fresh.

Use `sdp-update` when you want the default SDP incremental behaviour (only
recomputes stale datasets). If both the warehouse and the catalog are out of
sync, `just clean-pipeline <path>` removes the artefacts entirely.

### Override the Spark Connect URL

```bash
CONNECT_URL=sc://remote-host:15002 just sdp-run sdp-playground/example_pipeline
```

---

## Testing

Tests live in `sdp-playground/tests/` and cover the pure transform functions in
`sdp-playground/example_pipeline/lib/`.

```bash
just test
# or directly:
cd sdp-playground && uv run pytest --cov=lib --cov-branch --cov-report=term-missing
```

- Use **pytest** with `@pytest.mark.parametrize` for multi-scenario functions.
- Target ~80% branch coverage. The CI gate enforces this.
- Never delete a passing test — fix the code or update the assertion.
- Do **not** import from `transformations/` in tests (SDP runtime error).

---

## Adding a new dataset to the example pipeline

1. Add the pure transform function to `example_pipeline/lib/transforms.py`.
2. Add parameterized tests in `tests/test_transforms.py`.
3. Wire it into a new `@dp.materialized_view` (or `@dp.table`) file under
   `example_pipeline/transformations/`.
4. Validate the graph: `just dry-run sdp-playground/example_pipeline`.
5. Run end-to-end: `just sdp-run sdp-playground/example_pipeline`.

> **Authoring SDP pipelines beyond toy examples?** Read [Lisa Cao's
> `create-sdp-pipeline` Cursor skill][create-sdp-pipeline-skill] first. It
> codifies the SDP authoring workflow (workload analysis → design → implement
> → register → test) and — more importantly — calls out OSS-vs-Databricks
> gotchas that are easy to get wrong, including:
>
> - `SparkSession.active()` vs. the Databricks-provided `spark`
> - Referencing upstream tables **inside** the decorated function body so the
>   framework can infer the DAG (passing them as arguments silently breaks
>   dependency resolution)
> - Which primitives are OSS Spark 4.1 vs. Databricks-managed Lakeflow only
> - Why you should never mock `@dp.table` in unit tests — put pure logic in
>   `lib/` and integration-test the decorated function end-to-end (which is
>   exactly the pattern this repo follows)

[create-sdp-pipeline-skill]: https://github.com/lisancao/pyspark-sdp/tree/main/.cursor/skills/create-sdp-pipeline

---

## Branching conventions

| Prefix | Use when |
|---|---|
| `feat/` | New functionality |
| `fix/` | Bug fix |
| `chore/` | Build / CI / dependency upkeep |
| `refactor/` | Restructuring without behavior change |
| `test/` | Tests only |

- **Never commit directly to `main`.**
- Create a branch before the first file change.

---

## Key links

| Resource | URL |
|---|---|
| SDP Programming Guide | https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html |
| PySpark 4.1.1 Release Notes | https://spark.apache.org/releases/spark-release-4-1-1.html |
| Spark Connect Overview | https://spark.apache.org/docs/latest/spark-connect-overview.html |
| `create-sdp-pipeline` Cursor skill (Lisa Cao) | https://github.com/lisancao/pyspark-sdp/tree/main/.cursor/skills/create-sdp-pipeline |
| uv documentation | https://docs.astral.sh/uv/ |
| just documentation | https://just.systems/man/en/ |
| Docker Compose reference | https://docs.docker.com/compose/ |
