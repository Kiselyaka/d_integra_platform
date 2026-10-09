from sqlalchemy import func, select
from fastapi.testclient import TestClient

from integration_api.configuration.settings import Settings
from integration_api.main import create_app
from integration_api.message_processing.models import Base, Message, RouteProcessingResult
from integration_api.message_processing.ports import IntegrationRoute
from integration_api.message_processing.transformation import OrderCreatedToCrmTransformation
from integration_api.message_processing.contracts import ExternalEventEnvelope
from shared.database import create_database_engine, create_session_factory


def _event(event_id: str = "event-001") -> dict[str, object]:
    return {
        "event_id": event_id,
        "event_type": "order.created",
        "source_system": "mini-shop",
        "occurred_at": "2026-10-04T12:00:00Z",
        "correlation_id": "order-001",
        "schema_version": 1,
        "payload": {
            "order_id": "order-001",
            "customer": {
                "customer_id": "customer-001",
                "full_name": "Ada Lovelace",
                "email": "ada@example.test",
            },
            "shipping_address": {
                "country": "RU",
                "city": "Moscow",
                "postal_code": "101000",
                "street": "Tverskaya",
                "building": "1",
            },
            "items": [
                {"product_code": "SKU-001", "name": "Example Product", "quantity": 2, "unit_price": "10.50"}
            ],
            "currency": "EUR",
            "total_amount": "21.00",
        },
    }


class RecordingDelivery:
    def __init__(self) -> None:
        self.calls: list[tuple[IntegrationRoute, dict[str, object], str]] = []

    def deliver(self, route: IntegrationRoute, payload: dict[str, object], idempotency_key: str) -> None:
        self.calls.append((route, payload, idempotency_key))


class FailingDelivery:
    def deliver(self, route: IntegrationRoute, payload: dict[str, object], idempotency_key: str) -> None:
        raise RuntimeError("CRM unavailable")


class NoRouteProvider:
    def find_matching_routes(self, event: ExternalEventEnvelope) -> list[object]:
        return []


def _client_and_session_factory(delivery: object | None = None, route_provider: object | None = None) -> tuple[TestClient, object]:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    app = create_app(
        Settings(database_url="sqlite+pysqlite:///:memory:"),
        session_factory=session_factory,
        target_delivery=delivery or RecordingDelivery(),  # type: ignore[arg-type]
        route_provider=route_provider,  # type: ignore[arg-type]
    )
    return TestClient(app), session_factory


def test_accepts_and_persists_a_valid_mini_shop_event() -> None:
    client, session_factory = _client_and_session_factory()

    response = client.post("/api/v1/events", json=_event())

    assert response.status_code == 202
    assert response.json()["status"] == "accepted"
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
    assert message is not None
    assert response.json()["message_id"] == message.id
    assert message.status == "COMPLETED"
    assert message.payload["order_id"] == "order-001"
    with session_factory() as session:  # type: ignore[operator]
        result = session.scalar(select(RouteProcessingResult))
    assert result is not None and result.status == "DELIVERY_REQUESTED"


def test_accepting_the_same_source_event_twice_is_idempotent() -> None:
    delivery = RecordingDelivery()
    client, session_factory = _client_and_session_factory(delivery)

    first = client.post("/api/v1/events", json=_event())
    second = client.post("/api/v1/events", json=_event())

    assert first.status_code == second.status_code == 202
    assert first.json()["message_id"] == second.json()["message_id"]
    assert second.json()["status"] == "already_accepted"
    with session_factory() as session:  # type: ignore[operator]
        assert session.scalar(select(func.count()).select_from(Message)) == 1
    assert len(delivery.calls) == 1


def test_stores_source_payload_validation_errors_without_crashing() -> None:
    client, session_factory = _client_and_session_factory()
    invalid_event = _event()
    invalid_event["payload"] = {"order_id": "order-001", "items": []}

    response = client.post("/api/v1/events", json=invalid_event)

    assert response.status_code == 202
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
    assert message is not None
    assert message.status == "VALIDATION_FAILED"
    assert message.validation_error is not None


def test_marks_message_no_route_when_the_route_provider_has_no_match() -> None:
    client, session_factory = _client_and_session_factory(route_provider=NoRouteProvider())

    response = client.post("/api/v1/events", json=_event())

    assert response.status_code == 202
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
    assert message is not None and message.status == "NO_ROUTE"


def test_marks_message_routing_failed_when_crm_delivery_fails() -> None:
    client, session_factory = _client_and_session_factory(delivery=FailingDelivery())

    response = client.post("/api/v1/events", json=_event())

    assert response.status_code == 202
    with session_factory() as session:  # type: ignore[operator]
        message = session.scalar(select(Message))
        result = session.scalar(select(RouteProcessingResult))
    assert message is not None and message.status == "ROUTING_FAILED"
    assert result is not None and result.status == "DELIVERY_REQUESTED"
    assert result.error == "CRM unavailable"


def test_order_created_transformation_returns_the_crm_contract() -> None:
    target = OrderCreatedToCrmTransformation().transform(ExternalEventEnvelope.model_validate(_event()))

    assert target == {
        "external_order_id": "order-001",
        "client": {
            "external_customer_id": "customer-001",
            "name": "Ada Lovelace",
            "email": "ada@example.test",
            "phone": None,
        },
        "deal": {"amount": "21.00", "currency": "EUR"},
        "delivery_address": {
            "country": "RU",
            "city": "Moscow",
            "postal_code": "101000",
            "street": "Tverskaya",
            "building": "1",
            "apartment": None,
        },
        "positions": [
            {"external_product_code": "SKU-001", "title": "Example Product", "quantity": 2, "price": "10.50"}
        ],
    }
