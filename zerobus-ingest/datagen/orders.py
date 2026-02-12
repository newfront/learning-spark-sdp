"""Generate Order protobuf messages using gen/python/orders/v1."""

import random
import sys
import time
from pathlib import Path

import protovalidate

# Make gen/python importable (orders.v1.orders_pb2)
_root = Path(__file__).resolve().parent.parent
_gen = _root / "gen" / "python"
if str(_gen) not in sys.path:
    sys.path.insert(0, str(_gen))

from orders.v1 import orders_pb2  # noqa: E402

OrderStatus = orders_pb2.OrderStatus
PaymentMethod = orders_pb2.PaymentMethod
Money = orders_pb2.Money
Address = orders_pb2.Address
OrderLineItem = orders_pb2.OrderLineItem
Order = orders_pb2.Order


def _money(currency_code: str, units: int, nanos: int = 0) -> Money:
    m = Money()
    m.currency_code = currency_code
    m.units = units
    m.nanos = nanos
    return m


def _address(
    line_1: str,
    city: str,
    state_or_province: str,
    postal_code: str,
    country_code: str,
    line_2: str = "",
) -> Address:
    a = Address()
    a.line_1 = line_1
    a.line_2 = line_2
    a.city = city
    a.state_or_province = state_or_province
    a.postal_code = postal_code
    a.country_code = country_code
    return a


def _line_item(product_id: str, sku: str, name: str, quantity: int, unit_cents: int) -> OrderLineItem:
    total_cents = quantity * unit_cents
    item = OrderLineItem()
    item.product_id = product_id
    item.sku = sku
    item.name = name
    item.quantity = quantity
    item.unit_price.CopyFrom(_money("USD", unit_cents // 100, (unit_cents % 100) * 10_000_000))
    item.total_price.CopyFrom(_money("USD", total_cents // 100, (total_cents % 100) * 10_000_000))
    return item


class Orders:
    """Generate Order protobuf messages."""

    _PRODUCTS = [
        ("prod-1", "SKU-001", "Widget A", 1999),
        ("prod-2", "SKU-002", "Widget B", 2999),
        ("prod-3", "SKU-003", "Gadget X", 4999),
        ("prod-4", "SKU-004", "Gadget Y", 7999),
    ]

    _CUSTOMER_IDS = [f"cust-{i:04d}" for i in range(1, 101)]

    _SHIPPING_LINE_1 = [
        "123 Main St",
        "456 Oak Ave",
        "789 Pine Rd",
    ]

    _CITIES = [
        ("San Francisco", "CA", "94102"),
        ("Seattle", "WA", "98101"),
        ("Portland", "OR", "97201"),
    ]

    @staticmethod
    def generate_orders(count: int = 1, seed: int | None = None) -> list[Order]:
        """Generate `count` Order messages using the orders.v1 protobuf classes."""
        if seed is not None:
            random.seed(seed)
        now = int(time.time())
        orders: list[Order] = []
        for i in range(count):
            order_id = f"ord-{now}-{i:04d}"
            customer_id = random.choice(Orders._CUSTOMER_IDS)
            num_items = random.randint(1, 4)
            line_items: list[OrderLineItem] = []
            subtotal_cents = 0
            for _ in range(num_items):
                prod_id, sku, name, unit_cents = random.choice(Orders._PRODUCTS)
                qty = random.randint(1, 3)
                item = _line_item(prod_id, sku, name, qty, unit_cents)
                line_items.append(item)
                # total_price is USD: units=dollars, nanos=fraction; recover cents
                subtotal_cents += item.total_price.units * 100 + (
                    item.total_price.nanos // 10_000_000
                )
            tax_cents = int(subtotal_cents * 0.08)
            shipping_cents = 599 if subtotal_cents < 5000 else 0
            total_cents = subtotal_cents + tax_cents + shipping_cents

            city, state, zip_code = random.choice(Orders._CITIES)
            line_1 = random.choice(Orders._SHIPPING_LINE_1)

            o = Order()
            o.order_id = order_id
            o.customer_id = customer_id
            o.status = OrderStatus.ORDER_STATUS_CONFIRMED
            o.line_items.extend(line_items)
            o.subtotal.CopyFrom(_money("USD", subtotal_cents // 100, (subtotal_cents % 100) * 10_000_000))
            o.tax.CopyFrom(_money("USD", tax_cents // 100, (tax_cents % 100) * 10_000_000))
            o.shipping_cost.CopyFrom(_money("USD", shipping_cents // 100, (shipping_cents % 100) * 10_000_000))
            o.total.CopyFrom(_money("USD", total_cents // 100, (total_cents % 100) * 10_000_000))
            o.shipping_address.CopyFrom(_address(line_1, city, state, zip_code, "US"))
            o.billing_address.CopyFrom(_address(line_1, city, state, zip_code, "US"))
            o.payment_method = PaymentMethod.PAYMENT_METHOD_CARD
            o.payment_id = f"pay-{order_id}"
            o.created_at = now
            o.updated_at = now

            try:
                protovalidate.validate(o)
            except Exception as e:
                print(f"Validation: {e}", file=sys.stderr)
            orders.append(o)
        
        
        return orders
