"""Tests for ZerobusWriter (get_descriptor only; rest requires Databricks)."""

from zerobus_ingest.datagen import Orders
from zerobus_ingest.utils import ZerobusWriter


def test_get_descriptor_returns_none_for_non_protobuf():
    """get_descriptor returns None for objects without DESCRIPTOR."""
    assert ZerobusWriter.get_descriptor({}) is None
    assert ZerobusWriter.get_descriptor("not a message") is None
    assert ZerobusWriter.get_descriptor(None) is None


def test_get_descriptor_from_generated_order():
    """get_descriptor(order) returns the Order message DESCRIPTOR with expected details."""
    orders = Orders.generate_orders(1, seed=42)
    assert len(orders) == 1
    order = orders[0]

    descriptor = ZerobusWriter.get_descriptor(order)

    assert descriptor is not None
    assert descriptor is order.DESCRIPTOR
    assert descriptor.name == "Order"
    assert descriptor.full_name == "orders.v1.Order"
