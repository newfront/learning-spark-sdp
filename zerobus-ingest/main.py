"""Entry point script: loads env, creates WorkspaceClient, runs zerobus_ingest.main."""

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv

from zerobus_ingest.config import Config
from zerobus_ingest.main import main, parse_args

if __name__ == "__main__":
    args = parse_args()
    if args.env == "prod":
        load_dotenv(".env-prod")
    else:
        load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])
    main(
        client,
        generate=args.generate,
        publish=args.publish,
        count=args.count,
        config=config if (args.publish or args.create_table) else None,
        create_table=args.create_table,
        descriptor_path=args.descriptor_path,
        message_name=args.message_name,
    )
