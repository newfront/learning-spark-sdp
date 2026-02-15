"""CLI and main entry logic for zerobus-ingest."""

import argparse
from typing import Any

from databricks.sdk import WorkspaceClient
from zerobus.sdk.shared.definitions import RecordType, StreamConfigurationOptions

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import TableUtils, ZerobusWriter


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
    return parser.parse_args()


def main(
    workspace_client: WorkspaceClient,
    generate: bool | None = None,
    publish: bool | None = None,
    count: int = 100,
    config: dict[str, Any] | None = None,
) -> None:
    # use workspace_client for Databricks API calls
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
        with ZerobusWriter.from_config(config).with_stream_options(stream_options) as writer:
            for order in orders:
                # we can use wait_for_ack()
                ack = writer.write(order)
                ack.wait_for_ack()
                logger.info(f"Published order {order.order_id} to Zerobus.")
            writer.flush()
        print(f"Published {len(orders)} orders to Zerobus.")
