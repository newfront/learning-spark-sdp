"""CLI and main entry logic for zerobus-ingest."""

import argparse
from typing import Any

from databricks.sdk import WorkspaceClient

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ZerobusWriter


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
        help="Number of records to generate when --generate or --publish (default: 100).",
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
        orders = Orders.generate_orders(count, seed=42)
        with ZerobusWriter.from_config(config) as writer:
            for order in orders:
                writer.write(order)
            writer.flush()
        print(f"Published {len(orders)} orders to Zerobus.")
