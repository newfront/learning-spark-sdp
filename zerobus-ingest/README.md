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

# Zerobus (for ZerobusWriter / --publish)
ZEROBUS_CLIENT_ID=
ZEROBUS_CLIENT_SECRET=

# Unity Catalog target (for ZerobusWriter / --publish)
UC_CATALOG=
UC_SCHEMA=
UC_TABLE=
```

- **DATABRICKS_HOST** — Host only (e.g. `dbc-xxxx-xxxx.cloud.databricks.com` or short form).
- **DATABRICKS_WORKSPACE_ID** — Numeric workspace ID from the workspace URL.
- **DATABRICKS_WORKSPACE_URL** — Full workspace URL (e.g. `https://dbc-xxxx-xxxx.cloud.databricks.com/`).
- **DATABRICKS_REGION** — Cloud region (e.g. `us-west-2`).
- **DATABRICKS_TOKEN** — Personal access token (if using token auth).
- **DATABRICKS_CLIENT_ID** / **DATABRICKS_CLIENT_SECRET** — OAuth client credentials (if using OAuth).
- **ZEROBUS_CLIENT_ID** / **ZEROBUS_CLIENT_SECRET** — Zerobus ingest client credentials (required for `ZerobusWriter` and `--publish`).
- **UC_CATALOG**, **UC_SCHEMA**, **UC_TABLE** — Unity Catalog catalog, schema, and table for Zerobus ingestion target.

## Run

### Default (no generation)

Connects to the workspace and prints the workspace ID. Environment is chosen with `--env`.

```bash
uv run main.py              # dev (loads .env)
uv run main.py --env prod   # prod (loads .env-prod)
```

### Generate mode

Use `--generate` to create sample order records (Order protobufs) and print them to stdout. The number of records is controlled with `--count` (default: 100).

```bash
uv run main.py --generate                    # generate 100 orders (default count)
uv run main.py --generate --count 50         # generate 50 orders
uv run main.py --generate --count 500        # generate 500 orders
uv run main.py --env prod --generate --count 200   # prod env, 200 orders
```

### Publish mode

Use `--publish` to generate orders and publish each one to Zerobus (same `--count` and env as generate). Requires `ZEROBUS_*` and `UC_*` env vars.

```bash
uv run main.py --publish                      # generate and publish 100 orders
uv run main.py --publish --count 20           # generate and publish 20 orders
uv run main.py --env prod --publish --count 50
```

| Option       | Default | Description |
|-------------|--------|-------------|
| `--env`     | dev    | Environment: `dev` (loads `.env`) or `prod` (loads `.env-prod`). |
| `--generate` | off   | Generate sample orders via `datagen.Orders.generate_orders()` and print to stdout. |
| `--publish` | off    | Generate orders and publish each to Zerobus via `ZerobusWriter`. |
| `--count`   | 100    | Number of records to generate when `--generate` or `--publish` is set. |

---

## ZerobusWriter

`ZerobusWriter` wraps the Zerobus ingest SDK to write protobuf (or dict) records to a Zerobus stream. The stream is created lazily on the first `write()`; when the record is a protobuf message, the message’s `DESCRIPTOR` is used for `TableProperties` so the stream schema matches the message type.

### Config

Build a writer from the same config dict used elsewhere (e.g. from `Config.databricks()`). The config must include:

- **host**, **workspace_url** — Databricks workspace
- **zerobus_client_id**, **zerobus_client_secret** — Zerobus client credentials
- **catalog**, **schema**, **table** — Unity Catalog target (e.g. from `UC_CATALOG`, `UC_SCHEMA`, `UC_TABLE`)

Load env (e.g. `python-dotenv`) before calling `Config.databricks()` so these are set.

### Basic usage

Use `from_config()` and the context manager to ensure the stream is closed after writing. Call `write(record)` for each record and `flush()` before closing.

```python
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.utils import ZerobusWriter

load_dotenv()
config = Config.databricks()

with ZerobusWriter.from_config(config) as writer:
    for record in my_records:
        writer.write(record)
    writer.flush()
# writer.close() called automatically
```

### Example: Config + datagen

Generate orders with `Orders.generate_orders()` and publish them through `ZerobusWriter`:

```python
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ZerobusWriter

load_dotenv()
config = Config.databricks()

orders = Orders.generate_orders(count=50, seed=42)

with ZerobusWriter.from_config(config) as writer:
    for order in orders:
        writer.write(order)
    writer.flush()
print(f"Published {len(orders)} orders to Zerobus.")
```

This matches what the CLI does with `uv run main.py --publish --count 50`.

### API summary

| Method / API | Description |
|--------------|-------------|
| `ZerobusWriter.from_config(config)` | Build a writer from a config dict (e.g. `Config.databricks()`). |
| `writer.write(record)` | Ingest one record (protobuf message or dict). Returns `RecordAcknowledgment`. Lazy-creates the stream on first write; for protobuf messages, the record’s `DESCRIPTOR` is used for `TableProperties`. |
| `writer.flush()` | Flush the stream. |
| `writer.close()` | Close the stream and release resources. Use `with ZerobusWriter.from_config(config) as writer:` to close automatically. |
| `ZerobusWriter.get_descriptor(record)` | Static: return the record’s `DESCRIPTOR` if it is a protobuf message, else `None`. Useful in tests to assert the descriptor used for `TableProperties` (e.g. `assert ZerobusWriter.get_descriptor(order).full_name == "orders.v1.Order"`). |
