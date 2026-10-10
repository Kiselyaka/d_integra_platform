"""Self-contained demo contracts seeded by M8.

They describe the already published demo wire contracts; they do not redefine
Mini Shop, CRM, or Warehouse behaviour.
"""

from __future__ import annotations

from typing import Any


ORDER_CREATED_SOURCE: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["order_id", "customer", "shipping_address", "items", "currency", "total_amount"],
    "properties": {
        "order_id": {"type": "string", "minLength": 1},
        "customer": {
            "type": "object", "additionalProperties": False,
            "required": ["customer_id", "full_name", "email"],
            "properties": {
                "customer_id": {"type": "string", "minLength": 1},
                "full_name": {"type": "string", "minLength": 1},
                "email": {"type": "string", "minLength": 1},
                "phone": {"type": ["string", "null"]},
            },
        },
        "shipping_address": {
            "type": "object", "additionalProperties": False,
            "required": ["country", "city", "postal_code", "street", "building"],
            "properties": {
                "country": {"type": "string", "minLength": 1}, "city": {"type": "string", "minLength": 1},
                "postal_code": {"type": "string", "minLength": 1}, "street": {"type": "string", "minLength": 1},
                "building": {"type": "string", "minLength": 1}, "apartment": {"type": ["string", "null"]},
            },
        },
        "items": {"type": "array", "minItems": 1, "items": {
            "type": "object", "additionalProperties": False,
            "required": ["product_code", "name", "quantity", "unit_price"],
            "properties": {
                "product_code": {"type": "string", "minLength": 1}, "name": {"type": "string", "minLength": 1},
                "quantity": {"type": "integer", "minimum": 1}, "unit_price": {"type": ["number", "string"]},
            },
        }},
        "currency": {"type": "string", "minLength": 3, "maxLength": 3},
        "total_amount": {"type": ["number", "string"]},
    },
}

CRM_TARGET: dict[str, Any] = {
    "type": "object", "additionalProperties": False,
    "required": ["external_order_id", "client", "deal", "delivery_address", "positions"],
    "properties": {
        "external_order_id": {"type": "string", "minLength": 1},
        "client": {"type": "object", "required": ["external_customer_id", "name", "email", "phone"], "properties": {
            "external_customer_id": {"type": "string"}, "name": {"type": "string"}, "email": {"type": "string"}, "phone": {"type": ["string", "null"]},
        }},
        "deal": {"type": "object", "required": ["amount", "currency"], "properties": {"amount": {"type": "string"}, "currency": {"type": "string"}}},
        "delivery_address": {"type": "object"},
        "positions": {"type": "array", "minItems": 1, "items": {"type": "object", "required": ["external_product_code", "title", "quantity", "price"]}},
    },
}

MINI_SHOP_RESULT_TARGET: dict[str, Any] = {
    "oneOf": [
        {"type": "object", "additionalProperties": False, "required": ["order_id", "reservation_result", "warehouse_event_id"], "properties": {
            "order_id": {"type": "string", "minLength": 1}, "reservation_result": {"const": "succeeded"}, "warehouse_event_id": {"type": "string", "minLength": 1},
        }},
        {"type": "object", "additionalProperties": False, "required": ["order_id", "reservation_result", "warehouse_event_id", "reason_code", "message"], "properties": {
            "order_id": {"type": "string", "minLength": 1}, "reservation_result": {"const": "failed"}, "warehouse_event_id": {"type": "string", "minLength": 1},
            "reason_code": {"type": "string", "minLength": 1}, "message": {"type": "string", "minLength": 1},
        }},
    ]
}

WAREHOUSE_REQUEST_XSD = """<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="ReservationRequest"><xs:complexType><xs:sequence>
    <xs:element name="OrderNumber" type="xs:string"/>
    <xs:element name="Positions"><xs:complexType><xs:sequence>
      <xs:element name="Position" minOccurs="1" maxOccurs="unbounded"><xs:complexType><xs:sequence>
        <xs:element name="ItemCode" type="xs:string"/>
        <xs:element name="Quantity" type="xs:positiveInteger"/>
      </xs:sequence></xs:complexType></xs:element>
    </xs:sequence></xs:complexType></xs:element>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"""


def warehouse_callback_xsd(event_name: str, result_name: str, extra: str = "") -> str:
    """Return a strict self-contained XSD for one Warehouse callback kind."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<xs:schema xmlns:xs="http://www.w3.org/2001/XMLSchema">
  <xs:element name="WarehouseEvent"><xs:complexType><xs:sequence>
    <xs:element name="EventId" type="xs:string"/>
    <xs:element name="EventType"><xs:simpleType><xs:restriction base="xs:string"><xs:enumeration value="{event_name}"/></xs:restriction></xs:simpleType></xs:element>
    <xs:element name="OccurredAt" type="xs:dateTime"/>
    <xs:element name="CorrelationId" type="xs:string" minOccurs="0"/>
    <xs:element name="SchemaVersion" type="xs:positiveInteger"/>
    <xs:element name="Payload"><xs:complexType><xs:sequence>
      <xs:element name="{result_name}"><xs:complexType><xs:sequence>
        <xs:element name="OrderNumber" type="xs:string"/>
        <xs:element name="Status" type="xs:string"/>{extra}
      </xs:sequence></xs:complexType></xs:element>
    </xs:sequence></xs:complexType></xs:element>
  </xs:sequence></xs:complexType></xs:element>
</xs:schema>"""


WAREHOUSE_SUCCESS_SOURCE_XSD = warehouse_callback_xsd("reservation.succeeded", "ReservationSucceeded")
WAREHOUSE_FAILURE_SOURCE_XSD = warehouse_callback_xsd(
    "reservation.failed", "ReservationFailed", '<xs:element name="Code" type="xs:string"/><xs:element name="Message" type="xs:string"/>'
)


DEMO_SCHEMA_SPECS = {
    "mini-shop-order-created-source": ("Mini Shop order.created source", "JSON", "JSON_SCHEMA", ORDER_CREATED_SOURCE),
    "crm-order-target": ("CRM order target", "JSON", "JSON_SCHEMA", CRM_TARGET),
    "mini-shop-reservation-result-target": ("Mini Shop reservation result target", "JSON", "JSON_SCHEMA", MINI_SHOP_RESULT_TARGET),
    "warehouse-reservation-request-target": ("Warehouse ReservationRequest target", "XML", "XSD", WAREHOUSE_REQUEST_XSD),
    "warehouse-reservation-succeeded-source": ("Warehouse succeeded callback source", "XML", "XSD", WAREHOUSE_SUCCESS_SOURCE_XSD),
    "warehouse-reservation-failed-source": ("Warehouse failed callback source", "XML", "XSD", WAREHOUSE_FAILURE_SOURCE_XSD),
}
