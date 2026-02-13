"""CLI and main entry logic for zerobus-ingest."""

import argparse

from databricks.sdk import WorkspaceClient

from zerobus_ingest.datagen import Orders


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
        help="Turn on generate mode.",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=100,
        metavar="N",
        help="Number of records to generate when --generate is set (default: 100).",
    )
    return parser.parse_args()


def main(
    workspace_client: WorkspaceClient,
    generate: bool | None = None,
    count: int = 100,
) -> None:
    # use workspace_client for Databricks API calls
    if generate:
        orders = Orders.generate_orders(count, seed=42)
        print(orders)
