"""Configuration for zerobus-ingest."""

import os


class Config:
    @staticmethod
    def databricks() -> dict:
        """Read Databricks-related env vars into a dictionary."""
        return {
            "host": os.environ.get("DATABRICKS_HOST"),
            "workspace_id": os.environ.get("DATABRICKS_WORKSPACE_ID"),
            "workspace_url": os.environ.get("DATABRICKS_WORKSPACE_URL"),
            "zerobus_client_id": os.environ.get("ZEROBUS_CLIENT_ID"),
            "zerobus_client_secret": os.environ.get("ZEROBUS_CLIENT_SECRET"),
            "token": os.environ.get("DATABRICKS_TOKEN"),
            "region": os.environ.get("DATABRICKS_REGION"),            
            "catalog": os.environ.get("UC_CATALOG"),
            "schema": os.environ.get("UC_SCHEMA"),
            "table": os.environ.get("UC_TABLE"),
        }
