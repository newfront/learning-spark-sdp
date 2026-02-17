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

## TableUtils

`TableUtils` provides helpers for Unity Catalog table operations using the Databricks SDK `WorkspaceClient` (same credentials as your workspace). Use `table_exists` to check if a table exists before publishing, and `create_table` to create a managed Delta table (e.g. for Zerobus ingest) without passing a storage path—the location is derived from the schema’s storage root.

### Check table exists and create from a protobuf descriptor

A common workflow is: **check if the table exists in Databricks; if not, derive the table schema from your protobuf message and create the table.** That way you can ensure the Unity Catalog table exists (and matches your proto) before running `--publish` or writing with `ZerobusWriter`.

- **`table_exists(workspace_client, catalog, schema, table)`** — Returns `True` if the table exists in the metastore.
- **`descriptor_to_columns(descriptor: Descriptor) -> list[ColumnInfo]`** — Converts a protobuf message `DESCRIPTOR` (e.g. `Order.DESCRIPTOR` from your generated `orders_pb2`) into a list of Unity Catalog `ColumnInfo`. Uses the inverse of the [zerobus generate_proto type mappings](https://github.com/databricks/zerobus-sdk-py/blob/main/zerobus/tools/generate_proto.py): scalars (int32→INT, int64→LONG, string→STRING, etc.), enums→INT, nested messages→`STRUCT<...>`, repeated→`ARRAY<...>`. The result is suitable for `create_table(..., columns=...)`.
- **`pretty_print_columns(columns: list[ColumnInfo]) -> str`** — Formats a list of `ColumnInfo` as a readable table string (one column per line with name and type). Use this to preview the schema before creating the table.

**Example: ensure table exists, creating it from the Order protobuf if missing**

```python
from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import TableUtils

load_dotenv()
config = Config.databricks()
client = WorkspaceClient(host=config["host"], token=config["token"])

catalog, schema, table = config["catalog"], config["schema"], config["table"]

if not TableUtils.table_exists(client, catalog, schema, table):
    # Get ColumnInfo from the Order message DESCRIPTOR
    descriptor = Orders.generate_orders(1, seed=42)[0].DESCRIPTOR
    columns = TableUtils.descriptor_to_columns(descriptor)

    # Optional: preview the table schema
    print("Table schema (from proto):")
    print(TableUtils.pretty_print_columns(columns))

    TableUtils.create_table(client, catalog, schema, table, columns=columns)
    print(f"Created table {catalog}.{schema}.{table}")
else:
    print("Table already exists.")
```

### create_table

Creates a table in the metastore. Defaults to a **managed** Delta table; if you omit `storage_location`, it is derived from the schema’s `storage_root`. For external tables, pass `storage_location` and `table_type=TableType.EXTERNAL`.

You can pass **columns** as a list of `ColumnInfo` from `databricks.sdk.service.catalog` (use `name` and `type_name` with `ColumnTypeName`), and **properties** for Delta table properties (e.g. change data feed, column mapping).

**Example: create a managed Delta table with columns and Delta properties**

```python
from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnTypeName,
    TableType,
)

from zerobus_ingest.config import Config
from zerobus_ingest.utils import TableUtils

load_dotenv()
config = Config.databricks()

client = WorkspaceClient(host=config["host"], token=config["token"])

columns = [
    ColumnInfo(name="order_id", type_name=ColumnTypeName.STRING),
    ColumnInfo(name="customer_id", type_name=ColumnTypeName.STRING),
    ColumnInfo(name="total_units", type_name=ColumnTypeName.LONG),
]

delta_properties = {
    "delta.enableChangeDataFeed": "true",
    "delta.columnMapping.mode": "name",
}

table_info = TableUtils.create_table(
    client,
    catalog=config["catalog"],
    schema=config["schema"],
    table=config["table"],
    columns=columns,
    properties=delta_properties,
)
print(f"Created table {table_info.full_name}")
```

- **ColumnInfo** (`databricks.sdk.service.catalog`): use `name` and `type_name` (e.g. `ColumnTypeName.STRING`, `ColumnTypeName.LONG`, `ColumnTypeName.DOUBLE`, `ColumnTypeName.BOOLEAN`, `ColumnTypeName.TIMESTAMP`). Optional: `comment`, `nullable`, etc.
- **Delta properties** (optional): e.g. `delta.enableChangeDataFeed` for change data feed, `delta.columnMapping.mode` for column mapping. See [Delta table properties](https://docs.delta.io/table-properties).

To create an **external** Delta table with your own path, pass `storage_location` and `table_type=TableType.EXTERNAL`.

---

## VolumeUtils

`VolumeUtils` helps upload local files to [Unity Catalog volumes](https://docs.databricks.com/data-governance/unity-catalog/volumes.html) using the Databricks SDK `WorkspaceClient`. You need `USE CATALOG`, `USE SCHEMA`, and `WRITE VOLUME` on the target volume.

### Upload a file to a UC volume

- **`upload_file(workspace_client, file, destination, *, overwrite=True) -> bool`** — Uploads a local file to a Unity Catalog volume path. `destination` is the full volume path (e.g. a directory). If it doesn’t end with the file’s name, the file is written to `destination/<file.name>`.

Volume paths use the form:

```
/Volumes/<catalog_name>/<schema_name>/<volume_name>/<optional_path>
```

**Example: upload descriptor.bin to a volume**

```python
from pathlib import Path

from dotenv import load_dotenv

from databricks.sdk import WorkspaceClient

from zerobus_ingest.config import Config
from zerobus_ingest.utils import VolumeUtils

load_dotenv()
config = Config.databricks()
client = WorkspaceClient(host=config["host"], token=config["token"])

# Upload the generated descriptor to a volume (e.g. for Spark or other consumers)
VolumeUtils.upload_file(
    client,
    Path("gen/python/orders/v1/descriptor.bin"),
    "/Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1/",
)
# File is at: .../v1/descriptor.bin
```

You can pass a full file path as `destination` (including the filename) to control the name in the volume; otherwise the local filename is used under the given directory.

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
