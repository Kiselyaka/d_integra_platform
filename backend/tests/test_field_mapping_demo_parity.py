from datetime import UTC, datetime
from xml.etree import ElementTree

import pytest

from integration_api.configuration.demo_definitions import FIELD_MAPPING_V1_DEFINITIONS
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.ports import IntegrationRoute
from integration_api.message_processing.transformation import (
    OrderCreatedToCrmTransformation,
    OrderCreatedToWarehouseReservationTransformation,
    RouteTransformationProvider,
    WarehouseReservationFailedToMiniShopTransformation,
    WarehouseReservationSucceededToMiniShopTransformation,
)


def _order() -> ExternalEventEnvelope:
    return ExternalEventEnvelope(
        event_id="order-event", source_system="mini-shop", event_type="order.created", occurred_at=datetime(2026, 10, 10, tzinfo=UTC), schema_version=1,
        payload={
            "order_id": "order-1", "customer": {"customer_id": "customer-1", "full_name": "Ada", "email": "ada@example.test", "phone": None},
            "shipping_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1", "apartment": None},
            "items": [{"product_code": "SKU-001", "name": "Product", "quantity": 2, "unit_price": "10.50"}], "currency": "RUB", "total_amount": "21.00",
        },
    )


def _field_route(key: str, target_format: str) -> IntegrationRoute:
    return IntegrationRoute(
        key, 1, "http://target", target_format=target_format, transformation_key=key,
        transformation_engine_type="FIELD_MAPPING_V1", transformation_definition=FIELD_MAPPING_V1_DEFINITIONS[key],
    )


def test_crm_field_mapping_matches_legacy_contract() -> None:
    event = _order()
    actual = RouteTransformationProvider().transform(_field_route("order-created-to-crm", "JSON"), event)
    assert actual.body == OrderCreatedToCrmTransformation().transform(event)


def test_warehouse_field_mapping_plus_generic_xml_serializer_matches_contract() -> None:
    event = _order()
    actual = RouteTransformationProvider().transform(_field_route("order-created-to-warehouse-reservation", "XML"), event)
    expected = OrderCreatedToWarehouseReservationTransformation().transform(event)
    assert actual.content_type == "application/xml; charset=utf-8"
    assert ElementTree.tostring(ElementTree.fromstring(actual.body)) == ElementTree.tostring(ElementTree.fromstring(expected))


@pytest.mark.parametrize(
    ("event_type", "key", "legacy"),
    [
        ("warehouse.reservation.succeeded", "warehouse-reservation-succeeded-to-mini-shop", WarehouseReservationSucceededToMiniShopTransformation()),
        ("warehouse.reservation.failed", "warehouse-reservation-failed-to-mini-shop", WarehouseReservationFailedToMiniShopTransformation()),
    ],
)
def test_callback_field_mappings_match_legacy_contract(event_type: str, key: str, legacy: object) -> None:
    payload = {"external_order_id": "order-1", "status": "SUCCEEDED"}
    if event_type.endswith("failed"):
        payload = {**payload, "status": "FAILED", "reason_code": "OUT_OF_STOCK", "message": "No stock"}
    event = ExternalEventEnvelope(
        event_id="warehouse-event", source_system="1c-warehouse", event_type=event_type, occurred_at=datetime(2026, 10, 10, tzinfo=UTC), schema_version=1, payload=payload,
    )
    actual = RouteTransformationProvider().transform(_field_route(key, "JSON"), event)
    assert actual.body == legacy.transform(event)  # type: ignore[union-attr]
