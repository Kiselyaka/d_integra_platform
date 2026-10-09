from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from delivery_service.delivery.application import DeliveryProcessor, RetryPolicySnapshot
from delivery_service.delivery.models import Delivery, DeliveryBase, DeliveryOutboxEvent
from delivery_service.delivery.requested_consumer import DeliveryRequestedConsumer
from delivery_service.delivery.ports import DeliveryExecution
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.completion_consumer import MessageCompletionConsumer
from integration_api.message_processing.models import Base, Message, ProcessingOutboxEvent, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.transformation import RouteTransformationProvider
from shared.database import create_database_engine, create_session_factory
from shared.messaging.contracts import DeliveryCompletedPayload, DeliveryRequestedPayload, InternalEventEnvelope


class RecordingExecutor:
    def __init__(self) -> None:
        self.deliveries: list[Delivery] = []

    def execute(self, delivery: Delivery) -> DeliveryExecution:
        self.deliveries.append(delivery)
        return DeliveryExecution(http_status=200)


@pytest.mark.parametrize(
    ("event_type", "payload", "route_key", "expected_target_payload"),
    [
        (
            "warehouse.reservation.succeeded", {"external_order_id": "order-451", "status": "SUCCEEDED"},
            "warehouse-reservation-succeeded-to-mini-shop",
            '{"order_id":"order-451","reservation_result":"succeeded","warehouse_event_id":"warehouse-event-451"}',
        ),
        (
            "warehouse.reservation.failed",
            {"external_order_id": "order-451", "status": "FAILED", "reason_code": "OUT_OF_STOCK", "message": "No stock"},
            "warehouse-reservation-failed-to-mini-shop",
            '{"message":"No stock","order_id":"order-451","reason_code":"OUT_OF_STOCK","reservation_result":"failed","warehouse_event_id":"warehouse-event-451"}',
        ),
    ],
)
def test_warehouse_callback_creates_then_delivers_its_own_delivery(
    event_type: str, payload: dict[str, str], route_key: str, expected_target_payload: str
) -> None:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    event = ExternalEventEnvelope(
        event_id="warehouse-event-451", source_system="warehouse-1c", event_type=event_type,
        occurred_at=datetime(2026, 10, 9, tzinfo=UTC), correlation_id="order-order-451", schema_version=1, payload=payload,
    )
    with session_factory() as session:
        session.add(Message(
            id="callback-message-451", external_event_id=event.event_id, source_system=event.source_system,
            event_type=event.event_type, payload=event.payload, status="VALIDATED", correlation_id=event.correlation_id,
            schema_version=event.schema_version, occurred_at=event.occurred_at,
        ))
        session.commit()

    EventProcessingService(
        session_factory, StaticRouteProvider("http://crm:8000", "http://warehouse:8081", "http://mini-shop:8002"),
        RouteTransformationProvider(),
        RetryPolicySnapshot(3, "exponential", 5, 300, 2, "TIMEOUT"),
    ).process("callback-message-451")
    with session_factory() as session:
        requested = list(session.scalars(select(ProcessingOutboxEvent)))
    requested_consumer = DeliveryRequestedConsumer(session_factory)
    for item in requested:
        envelope = InternalEventEnvelope.model_validate(item.envelope)
        requested_consumer.handle(envelope, DeliveryRequestedPayload.model_validate(envelope.payload))
    executor = RecordingExecutor()
    DeliveryProcessor(session_factory, executor, None, 60).process_available(
        datetime(2026, 10, 9, tzinfo=UTC)
    )
    with session_factory() as session:
        completed = list(session.scalars(select(DeliveryOutboxEvent).where(DeliveryOutboxEvent.event_type == "delivery.completed")))
    completion_consumer = MessageCompletionConsumer(session_factory)
    for item in completed:
        envelope = InternalEventEnvelope.model_validate(item.envelope)
        completion_consumer.handle(envelope, DeliveryCompletedPayload.model_validate(envelope.payload))

    assert len(executor.deliveries) == 1
    delivery = executor.deliveries[0]
    assert delivery.target_system == "mini-shop"
    assert delivery.transformed_payload == expected_target_payload
    assert delivery.idempotency_key == delivery.id
    with session_factory() as session:
        message = session.get(Message, "callback-message-451")
        results = list(session.scalars(select(RouteProcessingResult)))
        persisted = session.scalar(select(Delivery))
    assert message is not None and message.status == "COMPLETED"
    assert [(result.route_key, result.status) for result in results] == [(route_key, "DELIVERY_REQUESTED")]
    assert persisted is not None and persisted.status == "DELIVERED"
