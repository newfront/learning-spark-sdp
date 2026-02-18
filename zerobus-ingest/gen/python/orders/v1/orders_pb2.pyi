from buf.validate import validate_pb2 as _validate_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Order(_message.Message):
    __slots__ = ("order_id", "customer_id", "status", "line_items", "subtotal", "tax", "shipping_cost", "total", "shipping_address", "billing_address", "payment_method", "payment_id", "created_at", "updated_at")
    class OrderStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        ORDER_STATUS_UNSPECIFIED: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_PENDING: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_CONFIRMED: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_PROCESSING: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_SHIPPED: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_DELIVERED: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_CANCELLED: _ClassVar[Order.OrderStatus]
        ORDER_STATUS_REFUNDED: _ClassVar[Order.OrderStatus]
    ORDER_STATUS_UNSPECIFIED: Order.OrderStatus
    ORDER_STATUS_PENDING: Order.OrderStatus
    ORDER_STATUS_CONFIRMED: Order.OrderStatus
    ORDER_STATUS_PROCESSING: Order.OrderStatus
    ORDER_STATUS_SHIPPED: Order.OrderStatus
    ORDER_STATUS_DELIVERED: Order.OrderStatus
    ORDER_STATUS_CANCELLED: Order.OrderStatus
    ORDER_STATUS_REFUNDED: Order.OrderStatus
    class PaymentMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        PAYMENT_METHOD_UNSPECIFIED: _ClassVar[Order.PaymentMethod]
        PAYMENT_METHOD_CARD: _ClassVar[Order.PaymentMethod]
        PAYMENT_METHOD_APPLEPAY: _ClassVar[Order.PaymentMethod]
        PAYMENT_METHOD_BANK_TRANSFER: _ClassVar[Order.PaymentMethod]
        PAYMENT_METHOD_OTHER: _ClassVar[Order.PaymentMethod]
    PAYMENT_METHOD_UNSPECIFIED: Order.PaymentMethod
    PAYMENT_METHOD_CARD: Order.PaymentMethod
    PAYMENT_METHOD_APPLEPAY: Order.PaymentMethod
    PAYMENT_METHOD_BANK_TRANSFER: Order.PaymentMethod
    PAYMENT_METHOD_OTHER: Order.PaymentMethod
    class Money(_message.Message):
        __slots__ = ("currency_code", "units", "nanos")
        CURRENCY_CODE_FIELD_NUMBER: _ClassVar[int]
        UNITS_FIELD_NUMBER: _ClassVar[int]
        NANOS_FIELD_NUMBER: _ClassVar[int]
        currency_code: str
        units: int
        nanos: int
        def __init__(self, currency_code: _Optional[str] = ..., units: _Optional[int] = ..., nanos: _Optional[int] = ...) -> None: ...
    class Address(_message.Message):
        __slots__ = ("line_1", "line_2", "city", "state_or_province", "postal_code", "country_code")
        LINE_1_FIELD_NUMBER: _ClassVar[int]
        LINE_2_FIELD_NUMBER: _ClassVar[int]
        CITY_FIELD_NUMBER: _ClassVar[int]
        STATE_OR_PROVINCE_FIELD_NUMBER: _ClassVar[int]
        POSTAL_CODE_FIELD_NUMBER: _ClassVar[int]
        COUNTRY_CODE_FIELD_NUMBER: _ClassVar[int]
        line_1: str
        line_2: str
        city: str
        state_or_province: str
        postal_code: str
        country_code: str
        def __init__(self, line_1: _Optional[str] = ..., line_2: _Optional[str] = ..., city: _Optional[str] = ..., state_or_province: _Optional[str] = ..., postal_code: _Optional[str] = ..., country_code: _Optional[str] = ...) -> None: ...
    class OrderLineItem(_message.Message):
        __slots__ = ("product_id", "sku", "name", "quantity", "unit_price", "total_price")
        PRODUCT_ID_FIELD_NUMBER: _ClassVar[int]
        SKU_FIELD_NUMBER: _ClassVar[int]
        NAME_FIELD_NUMBER: _ClassVar[int]
        QUANTITY_FIELD_NUMBER: _ClassVar[int]
        UNIT_PRICE_FIELD_NUMBER: _ClassVar[int]
        TOTAL_PRICE_FIELD_NUMBER: _ClassVar[int]
        product_id: str
        sku: str
        name: str
        quantity: int
        unit_price: Order.Money
        total_price: Order.Money
        def __init__(self, product_id: _Optional[str] = ..., sku: _Optional[str] = ..., name: _Optional[str] = ..., quantity: _Optional[int] = ..., unit_price: _Optional[_Union[Order.Money, _Mapping]] = ..., total_price: _Optional[_Union[Order.Money, _Mapping]] = ...) -> None: ...
    ORDER_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    LINE_ITEMS_FIELD_NUMBER: _ClassVar[int]
    SUBTOTAL_FIELD_NUMBER: _ClassVar[int]
    TAX_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_COST_FIELD_NUMBER: _ClassVar[int]
    TOTAL_FIELD_NUMBER: _ClassVar[int]
    SHIPPING_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    BILLING_ADDRESS_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_METHOD_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_FIELD_NUMBER: _ClassVar[int]
    order_id: str
    customer_id: str
    status: str
    line_items: _containers.RepeatedCompositeFieldContainer[Order.OrderLineItem]
    subtotal: Order.Money
    tax: Order.Money
    shipping_cost: Order.Money
    total: Order.Money
    shipping_address: Order.Address
    billing_address: Order.Address
    payment_method: str
    payment_id: str
    created_at: int
    updated_at: int
    def __init__(self, order_id: _Optional[str] = ..., customer_id: _Optional[str] = ..., status: _Optional[str] = ..., line_items: _Optional[_Iterable[_Union[Order.OrderLineItem, _Mapping]]] = ..., subtotal: _Optional[_Union[Order.Money, _Mapping]] = ..., tax: _Optional[_Union[Order.Money, _Mapping]] = ..., shipping_cost: _Optional[_Union[Order.Money, _Mapping]] = ..., total: _Optional[_Union[Order.Money, _Mapping]] = ..., shipping_address: _Optional[_Union[Order.Address, _Mapping]] = ..., billing_address: _Optional[_Union[Order.Address, _Mapping]] = ..., payment_method: _Optional[str] = ..., payment_id: _Optional[str] = ..., created_at: _Optional[int] = ..., updated_at: _Optional[int] = ...) -> None: ...
