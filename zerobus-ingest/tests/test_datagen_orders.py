"""Tests for datagen.Orders."""

from zerobus_ingest.datagen import Orders


def test_generate_orders_returns_list():
    """Orders.generate_orders returns a list."""
    result = Orders.generate_orders(0, seed=42)
    assert result == []


def test_generate_orders_count():
    """generate_orders(count, seed) returns exactly count orders."""
    for n in (1, 3, 10):
        orders = Orders.generate_orders(n, seed=42)
        assert len(orders) == n


def test_generate_orders_deterministic_with_seed():
    """Same seed produces same customer_ids and totals (order_id uses uuid,
    so not deterministic)."""
    a = Orders.generate_orders(2, seed=123)
    b = Orders.generate_orders(2, seed=123)
    assert [o.customer_id for o in a] == [o.customer_id for o in b]
    assert [o.total.units for o in a] == [o.total.units for o in b]
    assert [len(o.line_items) for o in a] == [len(o.line_items) for o in b]


def test_generate_orders_message_has_required_fields():
    """Each generated order has order_id, customer_id, status, line_items, total."""
    orders = Orders.generate_orders(1, seed=42)
    assert len(orders) == 1
    o = orders[0]
    assert o.order_id
    assert o.customer_id
    assert o.status is not None
    assert len(o.line_items) >= 1
    assert o.total.currency_code == "USD"
    assert o.total.units >= 0
