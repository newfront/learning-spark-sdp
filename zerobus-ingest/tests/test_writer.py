"""Tests for ZerobusWriter (get_descriptor only; rest requires Databricks)."""

import logging
from pathlib import Path

import pytest
from dotenv import load_dotenv
from zerobus.sdk.shared.definitions import RecordType, StreamConfigurationOptions, TableProperties

from zerobus_ingest.config import Config
from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ZerobusWriter

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ENV_FILE = _REPO_ROOT / ".env"

_REQUIRED_KEYS = (
    "host",
    "workspace_id",
    "workspace_url",
    "region",
    "zerobus_client_id",
    "zerobus_client_secret",
    "catalog",
    "schema",
    "table",
)


def _config_ready() -> bool:
    """True if .env exists and required config keys are set."""
    if not _ENV_FILE.exists():
        return False
    load_dotenv(_ENV_FILE)
    config = Config.databricks()
    return all(config.get(k) for k in _REQUIRED_KEYS)


def test_get_descriptor_returns_none_for_non_protobuf():
    """get_descriptor returns None for objects without DESCRIPTOR."""
    assert ZerobusWriter.get_descriptor({}) is None
    assert ZerobusWriter.get_descriptor("not a message") is None
    assert ZerobusWriter.get_descriptor(None) is None


def test_get_descriptor_from_generated_order():
    """
    get_descriptor(order)
    returns the Order message DESCRIPTOR with expected details."""
    orders = Orders.generate_orders(1, seed=42)
    assert len(orders) == 1
    order = orders[0]

    descriptor = ZerobusWriter.get_descriptor(order)

    assert descriptor is not None
    assert descriptor is order.DESCRIPTOR
    assert descriptor.name == "Order"
    assert descriptor.full_name == "orders.v1.Order"


@pytest.mark.skipif(
    not _config_ready(),
    reason=".env missing or incomplete; set DATABRICKS_*, ZEROBUS_*, UC_* for create_stream test",
)
def test_create_stream_with_config_and_table_properties(caplog):
    """
    Create ZerobusSdk from .env config, build TableProperties from Config + Order descriptor,
    and call create_stream to debug stream creation (requires Databricks + Zerobus connectivity).
    """
    load_dotenv(_ENV_FILE)
    config = Config.databricks()
    writer = ZerobusWriter.from_config(config).with_stream_options(
        StreamConfigurationOptions(record_type=RecordType.PROTO)
    )

    table_name = f"{config['catalog']}.{config['schema']}.{config['table']}"
    orders = Orders.generate_orders(1, seed=42)
    descriptor = orders[0].DESCRIPTOR
    table_properties = TableProperties(table_name, descriptor_proto=descriptor)

    with caplog.at_level(logging.INFO):
        sdk = writer.generate_sdk()
    assert "Server endpoint:" in caplog.text or "zerobus" in caplog.text.lower()
    assert "Unity catalog" in caplog.text or "unity_catalog" in caplog.text.lower()

    stream = sdk.create_stream(
        client_id=config["zerobus_client_id"],
        client_secret=config["zerobus_client_secret"],
        table_properties=table_properties,
        options=writer._stream_options,
    )

    assert stream is not None
    stream.close()
