from datetime import UTC, datetime
from xml.etree import ElementTree

import httpx
import pytest

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.http_delivery import (
    CrmHttpTargetDeliveryAdapter,
    WarehouseDeliveryError,
    WarehouseHttpTargetDeliveryAdapter,
)
from integration_api.message_processing.ports import IntegrationRoute, TargetPayload
from integration_api.message_processing.transformation import (
    OrderCreatedToWarehouseReservationTransformation,
    RouteTransformationProvider,
)


def _event() -> ExternalEventEnvelope:
    return ExternalEventEnvelope.model_validate(
        {
            "event_id": "event-451",
            "source_system": "mini-shop",
            "event_type": "order.created",
            "occurred_at": datetime(2026, 10, 9, tzinfo=UTC),
            "schema_version": 1,
            "payload": {
                "order_id": "order<&451",
                "customer": {"customer_id": "customer-001", "full_name": "Ada", "email": "ada@example.test"},
                "shipping_address": {
                    "country": "RU",
                    "city": "Moscow",
                    "postal_code": "101000",
                    "street": "Tverskaya",
                    "building": "1",
                },
                "items": [
                    {"product_code": "SKU<&001", "name": "First", "quantity": 2, "unit_price": "10.50"},
                    {"product_code": "SKU-002", "name": "Second", "quantity": 1, "unit_price": "4.00"},
                ],
                "currency": "EUR",
                "total_amount": "25.00",
            },
        }
    )


def test_transforms_order_created_to_the_published_warehouse_xml_contract() -> None:
    xml = OrderCreatedToWarehouseReservationTransformation().transform(_event())

    root = ElementTree.fromstring(xml)
    assert root.tag == "ReservationRequest"
    assert root.findtext("OrderNumber") == "order<&451"
    assert [
        (position.findtext("ItemCode"), position.findtext("Quantity"))
        for position in root.findall("./Positions/Position")
    ] == [("SKU<&001", "2"), ("SKU-002", "1")]
    assert "&lt;" in xml and "&amp;" in xml


def test_route_transformation_marks_warehouse_body_as_xml() -> None:
    route = IntegrationRoute("mini-shop-order-created-to-warehouse", 1, "http://host.docker.internal:8081/WarehouseDemo_Base", "warehouse")

    payload = RouteTransformationProvider().transform(route, _event())

    assert payload.content_type == "application/xml; charset=utf-8"
    assert isinstance(payload.body, str)


def test_warehouse_http_adapter_posts_xml_to_the_published_1c_endpoint(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class AcceptedResponse:
        status_code = 202

    def fake_post(url: str, **kwargs: object) -> AcceptedResponse:
        captured["url"] = url
        captured.update(kwargs)
        return AcceptedResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    route = IntegrationRoute("mini-shop-order-created-to-warehouse", 1, "http://host.docker.internal:8081/WarehouseDemo_Base", "warehouse")
    payload = TargetPayload("application/xml; charset=utf-8", "<ReservationRequest />")

    WarehouseHttpTargetDeliveryAdapter(2.5).deliver(route, payload, "event-451")

    assert captured == {
        "url": "http://host.docker.internal:8081/WarehouseDemo_Base/hs/reservations/",
        "content": b"<ReservationRequest />",
        "headers": {"Content-Type": "application/xml; charset=utf-8"},
        "timeout": 2.5,
    }


def test_crm_http_adapter_posts_the_json_body_not_the_target_payload(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class CreatedResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: object) -> CreatedResponse:
        captured["url"] = url
        captured.update(kwargs)
        return CreatedResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    route = IntegrationRoute("mini-shop-order-created-to-crm", 1, "http://crm:8000", "crm")

    CrmHttpTargetDeliveryAdapter(2.5).deliver(route, TargetPayload("application/json", {"external_order_id": "order-451"}), "event-451")

    assert captured == {
        "url": "http://crm:8000/api/integration/orders",
        "json": {"external_order_id": "order-451"},
        "headers": {"Idempotency-Key": "event-451"},
        "timeout": 2.5,
    }


def test_warehouse_http_adapter_rejects_non_202_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class RejectedResponse:
        status_code = 409

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: RejectedResponse())
    route = IntegrationRoute("mini-shop-order-created-to-warehouse", 1, "http://host.docker.internal:8081/WarehouseDemo_Base", "warehouse")

    with pytest.raises(WarehouseDeliveryError, match="HTTP 409"):
        WarehouseHttpTargetDeliveryAdapter(2.5).deliver(route, TargetPayload("application/xml", "<ReservationRequest />"), "event-451")


def test_warehouse_http_adapter_wraps_transport_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def raise_connection_error(*args: object, **kwargs: object) -> None:
        raise httpx.ConnectError("connection refused")

    monkeypatch.setattr(httpx, "post", raise_connection_error)
    route = IntegrationRoute("mini-shop-order-created-to-warehouse", 1, "http://host.docker.internal:8081/WarehouseDemo_Base", "warehouse")

    with pytest.raises(WarehouseDeliveryError, match="connection refused"):
        WarehouseHttpTargetDeliveryAdapter(2.5).deliver(route, TargetPayload("application/xml", "<ReservationRequest />"), "event-451")
