from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Base, Message, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.transformation import RouteTransformationProvider
from shared.database import create_database_engine, create_session_factory


class RecordingDelivery:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object, str]] = []

    def deliver(self, route: object, payload: object, idempotency_key: str) -> None:
        self.calls.append((getattr(route, "key"), payload, idempotency_key))


@pytest.mark.parametrize(
    ("event_type", "payload", "route_key", "expected_target_payload"),
    [
        (
            "warehouse.reservation.succeeded",
            {"external_order_id": "order-451", "status": "SUCCEEDED"},
            "warehouse-reservation-succeeded-to-mini-shop",
            {
                "order_id": "order-451",
                "reservation_result": "succeeded",
                "warehouse_event_id": "warehouse-event-451",
            },
        ),
        (
            "warehouse.reservation.failed",
            {
                "external_order_id": "order-451",
                "status": "FAILED",
                "reason_code": "OUT_OF_STOCK",
                "message": "No stock",
            },
            "warehouse-reservation-failed-to-mini-shop",
            {
                "order_id": "order-451",
                "reservation_result": "failed",
                "warehouse_event_id": "warehouse-event-451",
                "reason_code": "OUT_OF_STOCK",
                "message": "No stock",
            },
        ),
    ],
)
def test_warehouse_callback_completes_its_own_message_and_delivers_to_mini_shop(
    event_type: str, payload: dict[str, str], route_key: str, expected_target_payload: dict[str, str]
) -> None:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    event = ExternalEventEnvelope(
        event_id="warehouse-event-451",
        source_system="warehouse-1c",
        event_type=event_type,
        occurred_at=datetime(2026, 10, 9, tzinfo=UTC),
        correlation_id="order-order-451",
        schema_version=1,
        payload=payload,
    )
    with session_factory() as session:
        session.add(
            Message(
                id="callback-message-451",
                external_event_id=event.event_id,
                source_system=event.source_system,
                event_type=event.event_type,
                payload=event.payload,
                status="VALIDATED",
                correlation_id=event.correlation_id,
                schema_version=event.schema_version,
                occurred_at=event.occurred_at,
            )
        )
        session.commit()

    delivery = RecordingDelivery()
    EventProcessingService(
        session_factory,
        StaticRouteProvider("http://crm:8000", "http://warehouse:8081", "http://mini-shop:8002"),
        RouteTransformationProvider(),
        delivery,  # type: ignore[arg-type]
    ).process("callback-message-451")

    assert len(delivery.calls) == 1
    delivered_route, delivered_payload, idempotency_key = delivery.calls[0]
    assert delivered_route == route_key
    assert idempotency_key == "warehouse-event-451"
    assert getattr(delivered_payload, "body") == expected_target_payload
    with session_factory() as session:
        message = session.get(Message, "callback-message-451")
        results = list(session.scalars(select(RouteProcessingResult)))
    assert message is not None and message.status == "COMPLETED"
    assert [(result.route_key, result.status) for result in results] == [(route_key, "DELIVERY_REQUESTED")]
