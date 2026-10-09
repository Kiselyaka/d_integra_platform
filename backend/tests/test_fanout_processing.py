from datetime import UTC, datetime

import pytest
from sqlalchemy import select

from delivery_service.delivery.application import DeliveryProcessor, DeliveryRequestService, RetryPolicySnapshot
from delivery_service.delivery.models import Delivery, DeliveryBase, DeliveryFailure
from delivery_service.delivery.ports import DeliveryExecution
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.delivery_completion import MessageDeliveryCompletionService
from integration_api.message_processing.models import Base, Message, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.transformation import RouteTransformationProvider
from shared.database import create_database_engine, create_session_factory


def _event() -> ExternalEventEnvelope:
    return ExternalEventEnvelope.model_validate(
        {
            "event_id": "event-001", "source_system": "mini-shop", "event_type": "order.created",
            "occurred_at": "2026-10-09T12:00:00Z", "schema_version": 1,
            "payload": {
                "order_id": "order-001",
                "customer": {"customer_id": "customer-001", "full_name": "Ada Lovelace", "email": "ada@example.test"},
                "shipping_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1"},
                "items": [{"product_code": "SKU-001", "name": "Example", "quantity": 2, "unit_price": "10.50"}],
                "currency": "EUR", "total_amount": "21.00",
            },
        }
    )


class ConfigurableExecutor:
    def __init__(self, failing_targets: set[str]) -> None:
        self._failing_targets = failing_targets
        self.calls: list[str] = []

    def execute(self, delivery: Delivery) -> DeliveryExecution:
        self.calls.append(delivery.target_system)
        if delivery.target_system in self._failing_targets:
            raise _NonRetryableError(delivery.target_system)
        return DeliveryExecution(http_status=202)


class _NonRetryableError(RuntimeError):
    def __init__(self, target_system: str) -> None:
        self.delivery_failure = DeliveryFailure("CONTRACT_ERROR", "HTTP_409", f"{target_system} conflict", retryable=False, http_status=409)
        super().__init__(self.delivery_failure.message)


@pytest.mark.parametrize(
    ("failing_targets", "message_status", "delivery_statuses"),
    [
        (set(), "COMPLETED", {"crm": "DELIVERED", "warehouse": "DELIVERED"}),
        ({"warehouse"}, "ROUTING_FAILED", {"crm": "DELIVERED", "warehouse": "DEAD_LETTER"}),
        ({"crm"}, "ROUTING_FAILED", {"crm": "DEAD_LETTER", "warehouse": "DELIVERED"}),
        ({"crm", "warehouse"}, "ROUTING_FAILED", {"crm": "DEAD_LETTER", "warehouse": "DEAD_LETTER"}),
    ],
)
def test_fanout_keeps_delivery_lifecycles_independent(
    failing_targets: set[str], message_status: str, delivery_statuses: dict[str, str]
) -> None:
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    event = _event()
    with session_factory() as session:
        session.add(Message(
            id="message-001", external_event_id=event.event_id, source_system=event.source_system,
            event_type=event.event_type, payload=event.payload, status="VALIDATED", correlation_id=event.correlation_id,
            schema_version=event.schema_version, occurred_at=event.occurred_at,
        ))
        session.commit()

    requests = DeliveryRequestService(
        session_factory, RetryPolicySnapshot(3, "exponential", 5, 300, 2, "REMOTE_SERVER_ERROR")
    )
    EventProcessingService(
        session_factory,
        StaticRouteProvider("http://crm:8000", "http://host.docker.internal:8081/WarehouseDemo_Base"),
        RouteTransformationProvider(), requests,
    ).process("message-001")
    with session_factory() as session:
        pending_message = session.get(Message, "message-001")
        pending_deliveries = list(session.scalars(select(Delivery)))
    assert pending_message is not None and pending_message.status == "VALIDATED"
    assert {delivery.status for delivery in pending_deliveries} == {"QUEUED"}

    executor = ConfigurableExecutor(failing_targets)
    DeliveryProcessor(session_factory, executor, MessageDeliveryCompletionService(session_factory), 60).process_available(
        datetime(2026, 10, 9, tzinfo=UTC)
    )

    assert set(executor.calls) == {"crm", "warehouse"}
    with session_factory() as session:
        message = session.get(Message, "message-001")
        results = list(session.scalars(select(RouteProcessingResult)))
        deliveries = list(session.scalars(select(Delivery)))
    assert message is not None and message.status == message_status
    assert {result.status for result in results} == {"DELIVERY_REQUESTED"}
    assert {delivery.target_system: delivery.status for delivery in deliveries} == delivery_statuses
