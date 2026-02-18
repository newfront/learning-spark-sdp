"""CLI and main entry logic for zerobus-ingest."""

import argparse
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from google.protobuf import descriptor_pb2
from google.protobuf.descriptor_pool import DescriptorPool
from zerobus.sdk.shared.definitions import RecordType, StreamConfigurationOptions

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import TableUtils, ZerobusWriter


def _load_descriptor_from_binary(path: str | Path, message_name: str):
    """Load a message Descriptor from a binary FileDescriptorSet file.

    The file should be a serialized FileDescriptorSet (e.g. from
    protoc --descriptor_set_out=file.pb). message_name is the full protobuf
    message name (e.g. 'orders.v1.Order').
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Descriptor file not found: {path}")
    with path.open("rb") as f:
        fs = descriptor_pb2.FileDescriptorSet()
        fs.ParseFromString(f.read())
    pool = DescriptorPool()
    for fp in fs.file:
        pool.Add(fp)
    desc = pool.FindMessageTypeByName(message_name)
    if desc is None:
        raise ValueError(
            f"Message {message_name!r} not found in descriptor file. "
            "Use the full protobuf message name (e.g. orders.v1.Order)."
        )
    return desc


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--env",
        choices=["dev", "prod"],
        default="dev",
        help="Environment: dev (uses .env) or prod (uses .env-prod)",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Turn on generate mode (print orders to stdout).",
    )
    parser.add_argument(
        "--publish",
        action="store_true",
        help="Generate orders and publish each to Zerobus via ZerobusWriter.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        metavar="N",
        help="Number of records to generate (default: 100).",
    )
    parser.add_argument(
        "--create-table",
        action="store_true",
        help="If the table does not exist, create it using a binary "
        + "protobuf descriptor.",
    )
    parser.add_argument(
        "--descriptor-path",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to a binary FileDescriptorSet (required with --create-table).",
    )
    parser.add_argument(
        "--message-name",
        type=str,
        default=None,
        metavar="NAME",
        help="Full protobuf message name, e.g. orders.v1.Order (required with "
        + "--create-table).",
    )
    return parser.parse_args()


def main(
    workspace_client: WorkspaceClient,
    generate: bool | None = None,
    publish: bool | None = None,
    count: int = 100,
    config: dict[str, Any] | None = None,
    create_table: bool = False,
    descriptor_path: str | None = None,
    message_name: str | None = None,
) -> None:
    # use workspace_client for Databricks API calls
    if create_table:
        if not config:
            raise ValueError("config is required when --create-table is set")
        if not descriptor_path or not message_name:
            raise ValueError(
                "--descriptor-path and --message-name are required when "
                + " --create-table is set"
            )
        catalog, schema, table = config["catalog"], config["schema"], config["table"]
        if TableUtils.table_exists(workspace_client, catalog, schema, table):
            print(f"Table {catalog}.{schema}.{table} already exists.")
            return
        descriptor = _load_descriptor_from_binary(descriptor_path, message_name)
        columns = TableUtils.descriptor_to_columns(descriptor)
        TableUtils.create_table(
            workspace_client, catalog, schema, table, columns=columns
        )
        print(f"Created table {catalog}.{schema}.{table}")

    if generate:
        orders = Orders.generate_orders(count, seed=42)
        print(orders)

    if publish:
        if not config:
            raise ValueError("config is required when publish=True")
        table_name = f"{config['catalog']}.{config['schema']}.{config['table']}"
        if not TableUtils.table_exists(
            workspace_client,
            catalog=config["catalog"],
            schema=config["schema"],
            table=config["table"],
        ):
            raise ValueError(
                f"Table {table_name} does not exist in the workspace. "
                "Create the table before publishing."
            )
        orders = Orders.generate_orders(count, seed=42)
        stream_options = StreamConfigurationOptions(record_type=RecordType.PROTO)
        with ZerobusWriter.from_config(config).with_stream_options(
            stream_options
        ) as writer:
            for order in orders:
                # we can use wait_for_ack()
                ack = writer.write(order)
                ack.wait_for_ack()
            writer.flush()
        print(f"Published {len(orders)} orders to Zerobus.")
