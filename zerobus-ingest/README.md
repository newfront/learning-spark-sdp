# zerobus-ingest

Ingest application using the Databricks ZeroBus ingest SDK.

## Environment

Copy `.env` (or `.env-prod` for production) and fill in your Databricks workspace and auth details. Loaded automatically based on `--env dev` (default) or `--env prod`.

### Required variables in `.env`

```env
# Workspace
DATABRICKS_HOST=
DATABRICKS_WORKSPACE_ID=
DATABRICKS_WORKSPACE_URL=
DATABRICKS_REGION=

# Auth (use one of: token, or client_id + client_secret)
DATABRICKS_TOKEN=
DATABRICKS_CLIENT_ID=
DATABRICKS_CLIENT_SECRET=
```

- **DATABRICKS_HOST** — Host only (e.g. `dbc-xxxx-xxxx.cloud.databricks.com` or short form).
- **DATABRICKS_WORKSPACE_ID** — Numeric workspace ID from the workspace URL.
- **DATABRICKS_WORKSPACE_URL** — Full workspace URL (e.g. `https://dbc-xxxx-xxxx.cloud.databricks.com/`).
- **DATABRICKS_REGION** — Cloud region (e.g. `us-west-2`).
- **DATABRICKS_TOKEN** — Personal access token (if using token auth).
- **DATABRICKS_CLIENT_ID** / **DATABRICKS_CLIENT_SECRET** — OAuth client credentials (if using OAuth).

## Run

### Default (no generation)

Connects to the workspace and prints the workspace ID. Environment is chosen with `--env`.

```bash
uv run main.py              # dev (loads .env)
uv run main.py --env prod   # prod (loads .env-prod)
```

### Generate mode

Use `--generate` to create sample order records (Order protobufs) instead of only connecting to the workspace. The number of records is controlled with `--count` (default: 100).

```bash
uv run main.py --generate                    # generate 100 orders (default count)
uv run main.py --generate --count 50         # generate 50 orders
uv run main.py --generate --count 500        # generate 500 orders
uv run main.py --env prod --generate --count 200   # prod env, 200 orders
```

| Option       | Default | Description |
|-------------|--------|-------------|
| `--generate` | off    | Turn on generate mode; creates sample orders via `datagen.Orders.generate_orders()`. |
| `--count`   | 100    | Number of records to generate when `--generate` is set. |
