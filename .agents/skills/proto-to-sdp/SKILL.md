---
name: proto-to-sdp
description: Map protobuf-sourced clickstream data (the Tidewell GA4 event/user/product schema from pyspark-datagen) to the correct Spark column paths in an SDP pipeline. Use this skill whenever decoding `event.v1.Event` with `from_protobuf`, projecting fields out of a protobuf `oneof` payload, reconstructing money from a `commerce.v1.Amount` (units + nanos), exploding `items[]` line items, or whenever the user mentions protobuf, descriptors, GA4 events, sessionization, the buy-flow funnel, or the events/users/products datasets. Reach for this before writing any `F.col(...)` against decoded protobuf data — the field paths are non-obvious and easy to hallucinate as flat columns.
---

# Protobuf → SDP Column Paths (Tidewell Buy-Flow)

The clickstream data comes from `pyspark-datagen` as protobuf messages. After
decoding, the schema has shapes that don't map to flat column names — a `oneof`
becomes nullable structs, money is split into `units`/`nanos`, and line items are a
repeated struct. This skill gives the exact paths so transforms address real
columns instead of guessed ones.

## When to Use

- Writing `from_protobuf(...)` against `event.v1.Event`.
- Projecting any per-event-type field (cart_id, transaction_id, shipping_tier, ...).
- Converting a `commerce.v1.Amount` to a decimal dollar value.
- Exploding `Event.items[]`.
- Joining events to users (`user_id` → `User.uuid`) or products
  (`items[].item_id` → `Product.product_id`).

## The Three Entities (keys and references)

| Entity | Proto | Key | References |
|--------|-------|-----|------------|
| User | `user.v1.User` | `uuid` | — |
| Product | `product.v1.Product` | `product_id` | `price` is an `Amount` |
| Event | `event.v1.Event` | `session_id` (+ `event_id`) | `user_id`→`User.uuid`; `items[].item_id`→`Product.product_id` |

> `order.v1.Order` exists in the repo but is leftover from early datagen
> exploration. It uses its **own** nested `Amount`/`Product` and uint64 timestamps,
> shares no logic with the others, and is **not part of the buy-flow pipeline**.
> Ignore it; derive "orders" from `purchase` events instead (they carry
> `transaction_id`).

## Decoding the Event

Against a Kafka/binary source, decode with the descriptor file:

```python
from pyspark.sql.protobuf.functions import from_protobuf

decoded = raw.select(
    from_protobuf(F.col("value"), "event.v1.Event",
                  descFilePath="gen/descriptors/descriptor.bin").alias("e")
).select("e.*")
```

Against the datagen **Delta** output the proto is already exploded into columns, so
`spark.read.format("delta").load(...)` gives the same shape directly — the field
paths below are identical either way.

## `Amount` — money is units + nanos

`commerce.v1.Amount` = `{ currency, units, nanos }` where `nanos` is 1e-9 of a unit.
`$12.99` → `units=12, nanos=990000000`. **Never** treat an amount as a single number;
reconstruct it (put this in `lib/transforms.py`):

```python
def amount_to_decimal(units, nanos):
    return (units.cast("decimal(18,2)")
            + (nanos.cast("decimal(18,9)") / F.lit(1_000_000_000))
            ).cast("decimal(18,2)")
```

Amount appears at: `Event.value.{units,nanos}`, `Event.items[].price.{units,nanos}`,
`Product.price.{units,nanos}`, and inside `Purchase.tax`, `Purchase.shipping`,
`Refund.refund_amount`, `AddShippingInfo.shipping_cost`.

## The Envelope (shared fields, present on every event)

Flat columns, addressable directly:

```
event_id, event_name, user_id, session_id, event_timestamp_ms,
device_category, operating_system, country, region, city,
source, medium, campaign, page_location, page_referrer,
currency, value (Amount), items[] (repeated Item)
```

`event_timestamp_ms` is epoch **milliseconds**, monotonic within a session:

```python
.withColumn("event_ts", (F.col("event_timestamp_ms") / 1000).cast("timestamp"))
```

## The `oneof payload` — exactly one branch populated per row

This is the crux. `from_protobuf` decodes `oneof payload` into a struct with **11
nullable sub-structs; exactly one is non-null per row**, selected by `event_name`.
Address a field as `payload.<branch>.<field>` and filter on `event_name` first.

| `event_name` | `payload.<branch>` | Branch fields |
|--------------|--------------------|---------------|
| `session_start` | `payload.session_start` | `is_first_session`, `landing_page` |
| `view_item_list` | `payload.view_item_list` | `item_list_id`, `item_list_name` |
| `view_item` | `payload.view_item` | `item_list_id`, `item_list_name` |
| `add_to_cart` | `payload.add_to_cart` | `cart_id` |
| `view_cart` | `payload.view_cart` | `cart_id`, `num_items` |
| `remove_from_cart` | `payload.remove_from_cart` | `cart_id`, `reason` |
| `begin_checkout` | `payload.begin_checkout` | `cart_id`, `coupon` |
| `add_shipping_info` | `payload.add_shipping_info` | `cart_id`, `shipping_tier`, `shipping_cost` (Amount) |
| `add_payment_info` | `payload.add_payment_info` | `cart_id`, `payment_method` |
| `purchase` | `payload.purchase` | `transaction_id`, `coupon`, `tax` (Amount), `shipping` (Amount), `payment_method`, `shipping_tier` |
| `refund` | `payload.refund` | `transaction_id`, `refund_amount` (Amount), `reason` |

**Standard typed-view pattern** — filter on `event_name`, then project the matching
branch (the other 10 are null and must not be referenced):

```python
@dp.materialized_view
def purchase():
    df = spark.table("bronze_events").filter(F.col("event_name") == "purchase")
    return df.select(
        "session_id", "user_id", "event_ts",
        F.col("payload.purchase.transaction_id").alias("transaction_id"),
        F.col("payload.purchase.payment_method").alias("payment_method"),
        amount_to_decimal(F.col("value.units"), F.col("value.nanos")).alias("order_total"),
    )
```

## `items[]` — repeated line-item struct

`Event.items` is `repeated Item`. Each `Item`:
`item_id` (→`Product.product_id`), `sku`, `item_name`, `item_brand`,
`item_category`, `item_category2`, `item_variant`, `size`,
`price` (Amount), `quantity`, `percent_discount`.

Empty for `session_start`; populated for most ecommerce events. Explode to one row
per item:

```python
exploded = df.select(
    "session_id", "user_id", "event_ts",
    F.explode("items").alias("item"),
).select(
    "session_id", "user_id", "event_ts",
    F.col("item.item_id").alias("product_id"),
    F.col("item.item_name"),
    F.col("item.quantity"),
    amount_to_decimal(F.col("item.price.units"), F.col("item.price.nanos")).alias("price"),
)
```

## The Funnel (for sessionization)

Each `session_id` is an ordered walk (by `event_timestamp_ms`):

```
session_start → view_item_list → view_item → add_to_cart → view_cart
  → (remove_from_cart) → begin_checkout → add_shipping_info
  → add_payment_info → purchase → (refund)
```

Sessionize with `groupBy("session_id")`: `min`/`max` of `event_ts` for
start/end/duration, `max((event_name == "<stage>").cast("int"))` for reached-stage
flags, and left-join the `purchase` view for revenue.

## Field-Path Cheat Sheet

| Want | Path |
|------|------|
| Who | `user_id` (= `User.uuid`) |
| Which session | `session_id` |
| When | `event_timestamp_ms` (ms) → cast to `event_ts` |
| What happened | `event_name` (string discriminator) |
| Event-specific field | `payload.<branch>.<field>` (filter on `event_name` first) |
| Order total | `amount_to_decimal(value.units, value.nanos)` |
| Transaction id | `payload.purchase.transaction_id` |
| Product on a line item | `items[].item_id` (= `Product.product_id`) |
| Line-item price | `amount_to_decimal(items[].price.units, .nanos)` |

## Anti-Patterns

- **Flat payload columns.** There is no top-level `cart_id` or `transaction_id`;
  they live under `payload.<branch>`. Referencing a flat name returns null or errors.
- **Reading a `oneof` branch without filtering `event_name`.** On non-matching rows
  that branch is null; you'll get nulls or NPE-like analysis surprises.
- **Treating `Amount` as a number.** Always reconstruct from `units` + `nanos`.
- **Forgetting `items` is empty on `session_start`.** Explode only where items exist.
- **Reintroducing `order.v1.Order`.** Not part of the buy-flow; derive orders from
  `purchase` events.

## Related

- `sdp-authoring` skill — how to wrap these projections in `@dp.*` entry points.
- `pyspark-testing` skill — unit-testing `amount_to_decimal` and the projections.
- `pyspark-datagen` repo `AGENTS.md` — how to regenerate users/products/sessions and
  the `descriptor.bin` prerequisite.

