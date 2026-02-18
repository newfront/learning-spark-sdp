"""
Tests for VolumeUtils.
upload_file test is end-to-end and requires Databricks credentials.
"""

from pathlib import Path

import pytest

from zerobus_ingest.utils import VolumeUtils

# Path to descriptor.bin from repo root (zerobus-ingest)
_REPO_ROOT = Path(__file__).resolve().parent.parent
DESCRIPTOR_BIN = _REPO_ROOT / "gen" / "python" / "orders" / "v1" / "descriptor.bin"
VOLUME_DESTINATION = "/Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1/"


def _has_databricks_env() -> bool:
    """True if we have minimal env to create a WorkspaceClient and call the API."""
    import os

    return bool(
        os.environ.get("DATABRICKS_HOST") and os.environ.get("DATABRICKS_TOKEN")
    )


@pytest.mark.skipif(
    not DESCRIPTOR_BIN.exists(),
    reason="descriptor.bin not found; run 'make descriptor' first",
)
@pytest.mark.skipif(
    not _has_databricks_env(),
    reason="DATABRICKS_HOST and DATABRICKS_TOKEN not set; load .env or set env for E2E",
)
def test_upload_descriptor_bin_to_volume():
    """End-to-end: upload descriptor.bin to Unity Catalog volume.

    Requires .env (or env) with DATABRICKS_HOST, DATABRICKS_TOKEN, and WRITE VOLUME
    on the target volume. Run with:

        uv run pytest \
          tests/test_volume_utils.py::test_upload_descriptor_bin_to_volume \
          -v -s 

    File ends up at:
        /Volumes/scotts_playground/demos/apps/schemas/protos/orders/v1/descriptor.bin
    """
    from databricks.sdk import WorkspaceClient
    from dotenv import load_dotenv

    from zerobus_ingest.config import Config

    load_dotenv()
    config = Config.databricks()
    client = WorkspaceClient(host=config["host"], token=config["token"])

    result = VolumeUtils.upload_file(
        client,
        DESCRIPTOR_BIN,
        VOLUME_DESTINATION,
        overwrite=True,
    )

    assert result is True

    # Confirm the file appears in the volume (full E2E)
    entries = list(client.files.list_directory_contents(VOLUME_DESTINATION))
    names = [e.name for e in entries]
    assert "descriptor.bin" in names, f"expected descriptor.bin in volume, got: {names}"
