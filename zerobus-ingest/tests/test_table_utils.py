"""Tests for TableUtils (descriptor_to_columns; table_exists/create_table require Databricks)."""

from pathlib import Path

import pytest

from databricks.sdk.service.catalog import ColumnTypeName

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ProtobufUtils, TableUtils, read_binary

_REPO_ROOT = Path(__file__).resolve().parent.parent
DESCRIPTOR_BIN = _REPO_ROOT / "gen" / "python" / "orders" / "v1" / "descriptor.bin"


def test_descriptor_to_columns_from_order():
    """descriptor_to_columns(Order.DESCRIPTOR) returns correct ColumnInfo list."""
    orders = Orders.generate_orders(1, seed=42)
    assert len(orders) == 1
    descriptor = orders[0].DESCRIPTOR

    columns = TableUtils.descriptor_to_columns(descriptor)

    assert len(columns) == 14

    names = [c.name for c in columns]
    assert names == [
        "order_id",
        "customer_id",
        "status",
        "line_items",
        "subtotal",
        "tax",
        "shipping_cost",
        "total",
        "shipping_address",
        "billing_address",
        "payment_method",
        "payment_id",
        "created_at",
        "updated_at",
    ]

    by_name = {c.name: c for c in columns}

    assert by_name["order_id"].type_name == ColumnTypeName.STRING
    assert by_name["customer_id"].type_name == ColumnTypeName.STRING
    assert by_name["status"].type_name == ColumnTypeName.INT
    assert by_name["payment_method"].type_name == ColumnTypeName.INT
    assert by_name["payment_id"].type_name == ColumnTypeName.STRING
    assert by_name["created_at"].type_name == ColumnTypeName.LONG
    assert by_name["updated_at"].type_name == ColumnTypeName.LONG

    assert by_name["line_items"].type_name == ColumnTypeName.ARRAY
    assert by_name["line_items"].type_text is not None
    assert "ARRAY<STRUCT<" in by_name["line_items"].type_text
    assert "product_id:STRING" in by_name["line_items"].type_text

    assert by_name["subtotal"].type_name == ColumnTypeName.STRUCT
    assert by_name["subtotal"].type_text is not None
    assert "currency_code:STRING" in by_name["subtotal"].type_text
    assert "units:LONG" in by_name["subtotal"].type_text
    assert "nanos:INT" in by_name["subtotal"].type_text

    assert by_name["shipping_address"].type_name == ColumnTypeName.STRUCT
    assert "line_1:STRING" in by_name["shipping_address"].type_text
    assert "country_code:STRING" in by_name["shipping_address"].type_text


@pytest.mark.skipif(
    not DESCRIPTOR_BIN.exists(),
    reason="descriptor.bin not found; run 'make descriptor' first",
)
def test_descriptor_to_columns_from_binary_matches_order_descriptor():
    """ProtobufUtils.descriptor_from_binary(descriptor.bin) yields same Descriptor as Order.DESCRIPTOR."""
    orders = Orders.generate_orders(1, seed=42)
    from_descriptor_attr = orders[0].DESCRIPTOR

    raw = read_binary(DESCRIPTOR_BIN)
    from_binary = ProtobufUtils.descriptor_from_binary(raw, "orders.v1.Order")

    columns_from_attr = TableUtils.descriptor_to_columns(from_descriptor_attr)
    columns_from_binary = TableUtils.descriptor_to_columns(from_binary)

    assert [c.name for c in columns_from_attr] == [c.name for c in columns_from_binary]
    assert len(columns_from_binary) == 14
    assert from_binary.full_name == from_descriptor_attr.full_name == "orders.v1.Order"
