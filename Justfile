# Spark Connect URL for the Docker cluster.
# Override at call-time: CONNECT_URL=sc://remote-host:15002 just sdp-run <path>
connect_url := env_var_or_default("CONNECT_URL", "sc://localhost:15002")

# List all available recipes
default:
    @just --list

# ── Cluster management ──────────────────────────────────────────────────────

# Start the Spark cluster in the background
cluster-up:
    docker compose up -d
    @echo ""
    @echo "  Spark master UI  → http://localhost:8080"
    @echo "  Worker 1 UI      → http://localhost:8081"
    @echo "  Worker 2 UI      → http://localhost:8082"
    @echo "  Worker 3 UI      → http://localhost:8083"
    @echo "  Spark Connect    → {{connect_url}}"
    @echo ""

# Stop and remove the Spark cluster
cluster-down:
    docker compose down

# Show cluster container status
cluster-status:
    docker compose ps

# Tail all cluster logs (Ctrl-C to stop)
cluster-logs:
    docker compose logs -f

# ── Pipeline commands ────────────────────────────────────────────────────────

# NOTE: `spark-pipelines` is a bash wrapper that ultimately invokes
# `spark-submit`. The `SPARK_REMOTE` env var is IGNORED in that code path
# (spark-submit spawns a local JVM SparkContext first, which makes PySpark's
# `SparkSession.builder.getOrCreate()` return the classic JVM session instead
# of a Spark Connect client). We therefore pass `--remote` as a spark-submit
# argument *before* the SDP subcommand — that flag is mutually exclusive with
# `--master`/`--deploy-mode` and routes the run through Spark Connect to the
# Docker cluster.

# Validate a pipeline graph without writing any data.
# Usage: just dry-run sdp-playground/example_pipeline
dry-run path:
    cd "{{path}}" && uv run spark-pipelines --remote {{connect_url}} dry-run

# Run a pipeline against the Docker Spark cluster (full refresh — drops and
# recreates all tables so re-runs never hit LOCATION_ALREADY_EXISTS).
# Usage: just sdp-run sdp-playground/example_pipeline
sdp-run path:
    cd "{{path}}" && uv run spark-pipelines --remote {{connect_url}} run --full-refresh-all

# Incremental update — only recomputes datasets that are stale.
# Usage: just sdp-update sdp-playground/example_pipeline
sdp-update path:
    cd "{{path}}" && uv run spark-pipelines --remote {{connect_url}} run

# Remove pipeline warehouse and storage artefacts for a hard reset. Cleans
# both the in-spec local artefacts (left over from prior local-mode runs) and
# the bind-mounted `pipeline-storage/` at the repo root used by the cluster.
# Usage: just clean-pipeline sdp-playground/example_pipeline
clean-pipeline path:
    rm -rf "{{path}}/spark-warehouse" "{{path}}/pipeline-storage" "{{path}}/metastore_db" "{{path}}/derby.log"
    find pipeline-storage -mindepth 1 ! -name .gitkeep -exec rm -rf {} +
    @echo "Cleaned pipeline artefacts in {{path}} and ./pipeline-storage/"

# ── Dev helpers ──────────────────────────────────────────────────────────────

# Sync uv dependencies for sdp-playground
sync:
    cd sdp-playground && uv sync

# Run the test suite with branch coverage
test:
    cd sdp-playground && uv run pytest --cov=lib --cov-branch --cov-report=term-missing
