import argparse
import os
from typing import Optional

from datagen import Orders
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

from config import Config


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
    generate: Optional[bool] = None,
    count: int = 100,
) -> None:
    # use workspace_client for Databricks API calls
    print("Workspace ID:", workspace_client.get_workspace_id())
    if generate:
        orders = Orders.generate_orders(count, seed=42)
        print(orders)

if __name__ == "__main__":
    args = parse_args()
    if args.env == "prod":
        load_dotenv(".env-prod")
    else:
        load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])
    main(client, generate=args.generate, count=args.count)
