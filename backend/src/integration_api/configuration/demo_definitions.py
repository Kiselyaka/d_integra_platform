"""Initial FIELD_MAPPING_V1 definitions for the local demonstration routes."""

from __future__ import annotations

from typing import Any


def _from(path: str) -> dict[str, Any]:
    return {"op": "from", "path": path}


def _literal(value: Any) -> dict[str, Any]:
    return {"op": "literal", "value": value}


def _string(path: str) -> dict[str, Any]:
    return {"op": "to_string", "value": _from(path)}


INITIAL_FIELD_MAPPING_V1_WAREHOUSE_DEFINITION: dict[str, Any] = {
    "target": {
        "ReservationRequest": {
            "OrderNumber": _from("$.order_id"),
            "Positions": {
                "Position": {
                    "op": "map",
                    "path": "$.items",
                    "as": "item",
                    "target": {"ItemCode": _from("$item.product_code"), "Quantity": _string("$item.quantity")},
                }
            },
        }
    }
}


FIELD_MAPPING_V1_DEFINITIONS: dict[str, dict[str, Any]] = {
    "order-created-to-crm": {
        "target": {
            "external_order_id": _from("$.order_id"),
            "client": {
                "external_customer_id": _from("$.customer.customer_id"),
                "name": _from("$.customer.full_name"),
                "email": _from("$.customer.email"),
                "phone": _from("$.customer.phone"),
            },
            "deal": {"amount": _string("$.total_amount"), "currency": _from("$.currency")},
            "delivery_address": {
                "country": _from("$.shipping_address.country"),
                "city": _from("$.shipping_address.city"),
                "postal_code": _from("$.shipping_address.postal_code"),
                "street": _from("$.shipping_address.street"),
                "building": _from("$.shipping_address.building"),
                "apartment": _from("$.shipping_address.apartment"),
            },
            "positions": {
                "op": "map",
                "path": "$.items",
                "as": "item",
                "target": {
                    "external_product_code": _from("$item.product_code"),
                    "title": _from("$item.name"),
                    "quantity": _from("$item.quantity"),
                    "price": _string("$item.unit_price"),
                },
            },
        }
    },
    "order-created-to-warehouse-reservation": {
        "target": {
            "xml": {
                "name": _literal("ReservationRequest"),
                "children": [
                    {"name": _literal("OrderNumber"), "text": _from("$.order_id")},
                    {
                        "name": _literal("Positions"),
                        "children": {
                            "op": "map",
                            "path": "$.items",
                            "as": "item",
                            "target": {
                                "name": _literal("Position"),
                                "children": [
                                    {"name": _literal("ItemCode"), "text": _from("$item.product_code")},
                                    {"name": _literal("Quantity"), "text": _string("$item.quantity")},
                                ],
                            }
                        },
                    },
                ],
            }
        }
    },
    "warehouse-reservation-succeeded-to-mini-shop": {
        "target": {
            "order_id": _from("$.external_order_id"),
            "reservation_result": _literal("succeeded"),
            "warehouse_event_id": _from("$event.event_id"),
        }
    },
    "warehouse-reservation-failed-to-mini-shop": {
        "target": {
            "order_id": _from("$.external_order_id"),
            "reservation_result": _literal("failed"),
            "warehouse_event_id": _from("$event.event_id"),
            "reason_code": _from("$.reason_code"),
            "message": _from("$.message"),
        }
    },
}
