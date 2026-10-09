from datetime import UTC, datetime

import httpx
import pytest

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.http_delivery import MiniShopDeliveryError, MiniShopHttpTargetDeliveryAdapter
from integration_api.message_processing.ports import IntegrationRoute, TargetPayload
from integration_api.message_processing.transformation import RouteTransformationProvider


def warehouse_event(event_type: str) -> ExternalEventEnvelope:
    payload = (
        {"external_order_id": "order-451", "status": "SUCCEEDED"}
        if event_type == "warehouse.reservation.succeeded"
        else {
            "external_order_id": "order-451",
            "status": "FAILED",
            "reason_code": "OUT_OF_STOCK",
            "message": "No stock for SKU-001",
        }
    )
    return ExternalEventEnvelope(
        event_id="warehouse-event-451",
        source_system="warehouse-1c",
        event_type=event_type,
        occurred_at=datetime(2026, 10, 9, tzinfo=UTC),
        correlation_id="order-order-451",
        schema_version=1,
        payload=payload,
    )


@pytest.mark.parametrize(
    ("event_type", "expected_body"),
    [
        (
            "warehouse.reservation.succeeded",
            {
                "order_id": "order-451",
                "reservation_result": "succeeded",
                "warehouse_event_id": "warehouse-event-451",
            },
        ),
        (
            "warehouse.reservation.failed",
            {
                "order_id": "order-451",
                "reservation_result": "failed",
                "warehouse_event_id": "warehouse-event-451",
                "reason_code": "OUT_OF_STOCK",
                "message": "No stock for SKU-001",
            },
        ),
    ],
)
def test_transforms_warehouse_callback_to_mini_shop_result_contract(
    event_type: str, expected_body: dict[str, str]
) -> None:
    route = IntegrationRoute("warehouse-to-mini-shop", 1, "http://mini-shop:8002", "mini-shop")

    payload = RouteTransformationProvider().transform(route, warehouse_event(event_type))

    assert payload == TargetPayload("application/json", expected_body)


def test_transforms_normalized_m6_warehouse_source_code() -> None:
    route = IntegrationRoute("warehouse-to-mini-shop", 1, "http://mini-shop:8002", "mini-shop")
    event = warehouse_event("warehouse.reservation.succeeded").model_copy(update={"source_system": "1c-warehouse"})

    payload = RouteTransformationProvider().transform(route, event)

    assert payload.body["reservation_result"] == "succeeded"


def test_mini_shop_http_adapter_posts_json_result(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class AcceptedResponse:
        def raise_for_status(self) -> None:
            return None

    def fake_post(url: str, **kwargs: object) -> AcceptedResponse:
        captured["url"] = url
        captured.update(kwargs)
        return AcceptedResponse()

    monkeypatch.setattr(httpx, "post", fake_post)
    route = IntegrationRoute("warehouse-reservation-succeeded-to-mini-shop", 1, "http://mini-shop:8002", "mini-shop")
    payload = TargetPayload(
        "application/json",
        {"order_id": "order-451", "reservation_result": "succeeded", "warehouse_event_id": "warehouse-event-451"},
    )

    MiniShopHttpTargetDeliveryAdapter(2.5).deliver(route, payload, "warehouse-event-451")

    assert captured == {
        "url": "http://mini-shop:8002/api/integration/reservation-results",
        "json": payload.body,
        "headers": {"Idempotency-Key": "warehouse-event-451"},
        "timeout": 2.5,
    }


@pytest.mark.parametrize("exception", [httpx.ConnectError("connection refused"), httpx.ReadTimeout("timed out")])
def test_mini_shop_http_adapter_wraps_transport_errors(monkeypatch: pytest.MonkeyPatch, exception: httpx.HTTPError) -> None:
    def raise_transport_error(*args: object, **kwargs: object) -> None:
        raise exception

    monkeypatch.setattr(httpx, "post", raise_transport_error)
    route = IntegrationRoute("warehouse-reservation-succeeded-to-mini-shop", 1, "http://mini-shop:8002", "mini-shop")

    with pytest.raises(MiniShopDeliveryError, match=str(exception)):
        MiniShopHttpTargetDeliveryAdapter(2.5).deliver(
            route,
            TargetPayload("application/json", {"order_id": "order-451"}),
            "warehouse-event-451",
        )


def test_mini_shop_http_adapter_wraps_non_success_response(monkeypatch: pytest.MonkeyPatch) -> None:
    class ConflictResponse:
        def raise_for_status(self) -> None:
            request = httpx.Request("POST", "http://mini-shop:8002/api/integration/reservation-results")
            response = httpx.Response(409, request=request)
            raise httpx.HTTPStatusError("409 conflict", request=request, response=response)

    monkeypatch.setattr(httpx, "post", lambda *args, **kwargs: ConflictResponse())
    route = IntegrationRoute("warehouse-reservation-succeeded-to-mini-shop", 1, "http://mini-shop:8002", "mini-shop")

    with pytest.raises(MiniShopDeliveryError, match="409 conflict"):
        MiniShopHttpTargetDeliveryAdapter(2.5).deliver(
            route,
            TargetPayload("application/json", {"order_id": "order-451"}),
            "warehouse-event-451",
        )
