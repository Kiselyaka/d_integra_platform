from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Base, Message, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.transformation import RouteTransformationProvider
from shared.database import create_database_engine, create_session_factory


def _event() -> ExternalEventEnvelope:
    return ExternalEventEnvelope.model_validate(
        {
            "event_id": "event-001",
            "source_system": "mini-shop",
            "event_type": "order.created",
            "occurred_at": "2026-10-09T12:00:00Z",
            "schema_version": 1,
            "payload": {
                "order_id": "order-001",
                "customer": {"customer_id": "customer-001", "full_name": "Ada Lovelace", "email": "ada@example.test"},
                "shipping_address": {
                    "country": "RU",
                    "city": "Moscow",
                    "postal_code": "101000",
                    "street": "Tverskaya",
                    "building": "1",
                },
                "items": [{"product_code": "SKU-001", "name": "Example", "quantity": 2, "unit_price": "10.50"}],
                "currency": "EUR",
                "total_amount": "21.00",
            },
        }
    )


class ConfigurableDelivery:
    def __init__(self, failing_targets: set[str]) -> None:
        self._failing_targets = failing_targets
        self.calls: list[str] = []

    def deliver(self, route: object, payload: object, idempotency_key: str) -> None:
        target_system = getattr(route, "target_system")
        self.calls.append(target_system)
        if target_system in self._failing_targets:
            raise RuntimeError(f"{target_system} unavailable")


@pytest.mark.parametrize(
    ("failing_targets", "message_status", "result_statuses"),
    [
        (set(), "COMPLETED", {"crm": "DELIVERY_REQUESTED", "warehouse": "DELIVERY_REQUESTED"}),
        ({"warehouse"}, "ROUTING_FAILED", {"crm": "DELIVERY_REQUESTED", "warehouse": "DELIVERY_FAILED"}),
        ({"crm"}, "ROUTING_FAILED", {"crm": "DELIVERY_FAILED", "warehouse": "DELIVERY_REQUESTED"}),
        ({"crm", "warehouse"}, "ROUTING_FAILED", {"crm": "DELIVERY_FAILED", "warehouse": "DELIVERY_FAILED"}),
    ],
)
def test_fanout_isolates_route_failures(
    failing_targets: set[str], message_status: str, result_statuses: dict[str, str]
) -> None:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    event = _event()
    with session_factory() as session:
        message = Message(
            id="message-001",
            external_event_id=event.event_id,
            source_system=event.source_system,
            event_type=event.event_type,
            payload=event.payload,
            status="VALIDATED",
            correlation_id=event.correlation_id,
            schema_version=event.schema_version,
            occurred_at=event.occurred_at,
        )
        session.add(message)
        session.commit()

    delivery = ConfigurableDelivery(failing_targets)
    service = EventProcessingService(
        session_factory,
        StaticRouteProvider("http://crm:8000", "http://host.docker.internal:8081/WarehouseDemo_Base"),
        RouteTransformationProvider(),
        delivery,  # type: ignore[arg-type]
    )

    service.process("message-001")

    assert delivery.calls == ["crm", "warehouse"]
    with session_factory() as session:
        message = session.get(Message, "message-001")
        results = list(session.scalars(select(RouteProcessingResult).order_by(RouteProcessingResult.route_key)))
    assert message is not None and message.status == message_status
    assert len(results) == 2
    assert {result.route_key.split("-to-")[1]: result.status for result in results} == result_statuses
