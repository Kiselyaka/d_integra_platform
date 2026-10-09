from datetime import UTC, datetime

from sqlalchemy import select

from delivery_service.delivery.application import DeliveryProcessor, DeliveryRequest, DeliveryRequestService, RetryPolicySnapshot
from delivery_service.delivery.models import Delivery, DeliveryBase, DeliveryFailure, DeliveryInboxMessage, DeliveryOutboxEvent
from delivery_service.delivery.ports import DeliveryExecution
from delivery_service.delivery.requested_consumer import DeliveryRequestedConsumer
from integration_api.message_processing.completion_consumer import MessageCompletionConsumer
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import (
    Base,
    Message,
    ProcessingDeliveryCompletion,
    ProcessingInboxMessage,
    ProcessingOutboxEvent,
    RouteProcessingResult,
)
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.transformation import RouteTransformationProvider
from shared.database import create_database_engine, create_session_factory
from shared.messaging.contracts import DeliveryCompletedPayload, DeliveryRequestedPayload, InternalEventEnvelope, envelope_json
from shared.messaging.outbox import OutboxPublisher


NOW = datetime(2026, 10, 9, 12, tzinfo=UTC)
POLICY = RetryPolicySnapshot(3, "exponential", 5, 60, 2, "TIMEOUT,REMOTE_SERVER_ERROR")


def _sessions():
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    return create_session_factory(engine)


def _message(session_factory, message_id: str = "message-m5") -> ExternalEventEnvelope:
    event = ExternalEventEnvelope.model_validate(
        {
            "event_id": "source-event-m5", "source_system": "mini-shop", "event_type": "order.created",
            "occurred_at": "2026-10-09T12:00:00Z", "correlation_id": "order-m5", "schema_version": 1,
            "payload": {
                "order_id": "order-m5",
                "customer": {"customer_id": "customer-m5", "full_name": "Ada", "email": "ada@example.test"},
                "shipping_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1"},
                "items": [{"product_code": "SKU-001", "name": "Example", "quantity": 1, "unit_price": "10.00"}],
                "currency": "RUB", "total_amount": "10.00",
            },
        }
    )
    with session_factory() as session:
        session.add(Message(
            id=message_id, external_event_id=event.event_id, source_system=event.source_system, event_type=event.event_type,
            payload=event.payload, status="VALIDATED", correlation_id=event.correlation_id,
            schema_version=event.schema_version, occurred_at=event.occurred_at,
        ))
        session.commit()
    return event


def _requested_events(session_factory):
    with session_factory() as session:
        return [InternalEventEnvelope.model_validate(row.envelope) for row in session.scalars(select(ProcessingOutboxEvent))]


def test_processing_fanout_writes_two_transactional_outbox_events_before_kafka() -> None:
    session_factory = _sessions()
    _message(session_factory)
    EventProcessingService(
        session_factory,
        StaticRouteProvider("http://crm:8000", "http://warehouse:8081"),
        RouteTransformationProvider(),
        POLICY,
    ).process("message-m5")

    with session_factory() as session:
        results = list(session.scalars(select(RouteProcessingResult)))
        outbox = list(session.scalars(select(ProcessingOutboxEvent)))
        deliveries = list(session.scalars(select(Delivery)))
    assert len(results) == len(outbox) == 2
    assert deliveries == []
    assert {row.status for row in results} == {"DELIVERY_REQUESTED"}
    payloads = [DeliveryRequestedPayload.model_validate(row.envelope["payload"]) for row in outbox]
    assert len({payload.delivery_id for payload in payloads}) == 2
    assert {row.topic for row in outbox} == {"integration.delivery.requested"}
    assert all(row.published_at is None for row in outbox)


def test_delivery_inbox_deduplicates_same_kafka_event_and_preserves_delivery_identity() -> None:
    session_factory = _sessions()
    _message(session_factory)
    EventProcessingService(session_factory, StaticRouteProvider("http://crm:8000", "http://warehouse:8081"), RouteTransformationProvider(), POLICY).process("message-m5")
    event = _requested_events(session_factory)[0]
    payload = DeliveryRequestedPayload.model_validate(event.payload)
    consumer = DeliveryRequestedConsumer(session_factory)

    assert consumer.handle(event, payload) is True
    assert consumer.handle(event, payload) is False
    with session_factory() as session:
        deliveries = list(session.scalars(select(Delivery)))
        inbox = list(session.scalars(select(DeliveryInboxMessage)))
    assert len(deliveries) == len(inbox) == 1
    assert deliveries[0].id == deliveries[0].idempotency_key == payload.delivery_id


class _SuccessExecutor:
    def execute(self, delivery: Delivery) -> DeliveryExecution:
        return DeliveryExecution(http_status=200)


class _NonRetryableExecutor:
    def execute(self, delivery: Delivery) -> DeliveryExecution:
        error = RuntimeError("conflict")
        error.delivery_failure = DeliveryFailure("CONTRACT_ERROR", "HTTP_409", "conflict", False, 409)  # type: ignore[attr-defined]
        raise error


class _MixedExecutor:
    def execute(self, delivery: Delivery) -> DeliveryExecution:
        if delivery.route_processing_result_id == "result-failure":
            return _NonRetryableExecutor().execute(delivery)
        return DeliveryExecution(http_status=200)


def test_terminal_delivery_writes_completed_and_dead_letter_outbox_once() -> None:
    session_factory = _sessions()
    service = DeliveryRequestService(session_factory, POLICY)
    success = service.request(_request("result-success"), now=NOW)
    failure = service.request(_request("result-failure"), now=NOW)
    DeliveryProcessor(session_factory, _MixedExecutor(), None, 60).process_available(NOW, limit=2)
    with session_factory() as session:
        outbox = list(session.scalars(select(DeliveryOutboxEvent).order_by(DeliveryOutboxEvent.delivery_id, DeliveryOutboxEvent.event_type)))
    assert [(row.delivery_id, row.event_type) for row in outbox].count((success.id, "delivery.completed")) == 1
    assert [(row.delivery_id, row.event_type) for row in outbox].count((failure.id, "delivery.completed")) == 1
    assert [(row.delivery_id, row.event_type) for row in outbox].count((failure.id, "delivery.dead_lettered")) == 1


def test_completion_inbox_waits_for_all_routes_then_handles_duplicate_without_cross_context_select() -> None:
    session_factory = _sessions()
    _message(session_factory)
    EventProcessingService(session_factory, StaticRouteProvider("http://crm:8000", "http://warehouse:8081"), RouteTransformationProvider(), POLICY).process("message-m5")
    requested = _requested_events(session_factory)
    consumer = MessageCompletionConsumer(session_factory)
    first = _completed_from_requested(requested[0], "DELIVERED")
    second = _completed_from_requested(requested[1], "DEAD_LETTER")

    assert consumer.handle(first, DeliveryCompletedPayload.model_validate(first.payload)) is True
    with session_factory() as session:
        assert session.get(Message, "message-m5").status == "VALIDATED"
    assert consumer.handle(first, DeliveryCompletedPayload.model_validate(first.payload)) is False
    assert consumer.handle(second, DeliveryCompletedPayload.model_validate(second.payload)) is True
    with session_factory() as session:
        message = session.get(Message, "message-m5")
        inbox = list(session.scalars(select(ProcessingInboxMessage)))
        projection = list(session.scalars(select(ProcessingDeliveryCompletion)))
    assert message is not None and message.status == "ROUTING_FAILED"
    assert len(inbox) == len(projection) == 2


def test_completion_marks_message_completed_only_when_every_route_is_delivered() -> None:
    session_factory = _sessions()
    _message(session_factory)
    EventProcessingService(session_factory, StaticRouteProvider("http://crm:8000", "http://warehouse:8081"), RouteTransformationProvider(), POLICY).process("message-m5")
    consumer = MessageCompletionConsumer(session_factory)
    for requested in _requested_events(session_factory):
        completed = _completed_from_requested(requested, "DELIVERED")
        consumer.handle(completed, DeliveryCompletedPayload.model_validate(completed.payload))
    with session_factory() as session:
        assert session.get(Message, "message-m5").status == "COMPLETED"


def test_completion_consumer_projects_dead_letter_topic_and_deduplicates_it() -> None:
    session_factory = _sessions()
    _message(session_factory)
    EventProcessingService(session_factory, StaticRouteProvider("http://crm:8000", "http://warehouse:8081"), RouteTransformationProvider(), POLICY).process("message-m5")
    requested = _requested_events(session_factory)[0]
    payload = DeliveryRequestedPayload.model_validate(requested.payload)
    dead_letter = InternalEventEnvelope(
        event_id=f"dead-letter-{payload.delivery_id}", event_type="delivery.dead_lettered",
        aggregate_id=payload.delivery_id, occurred_at=NOW, correlation_id=requested.correlation_id,
        payload={
            "delivery_id": payload.delivery_id, "message_id": payload.message_id,
            "route_processing_result_id": payload.route_processing_result_id,
            "target_system": payload.target.system_code, "reason": "HTTP_409", "attempt_count": 1,
            "last_error": {"category": "CONTRACT_ERROR", "code": "HTTP_409", "message": "conflict"},
        },
    )
    consumer = MessageCompletionConsumer(session_factory)

    envelope, terminal = consumer._parse_terminal(dead_letter.as_json_bytes())
    assert terminal.status == "DEAD_LETTER"
    assert consumer.handle(envelope, terminal) is True
    assert consumer.handle(envelope, terminal) is False

    with session_factory() as session:
        message = session.get(Message, "message-m5")
        inbox = list(session.scalars(select(ProcessingInboxMessage)))
        projections = list(session.scalars(select(ProcessingDeliveryCompletion)))
    assert message is not None and message.status == "VALIDATED"
    assert len(inbox) == len(projections) == 1
    assert projections[0].terminal_status == "DEAD_LETTER"


def test_outbox_reclaims_the_same_event_id_after_ack_before_mark_crash() -> None:
    session_factory = _sessions()
    event = InternalEventEnvelope(
        event_id="outbox-event-m5", event_type="delivery.requested", aggregate_id="result-m5",
        occurred_at=NOW, correlation_id="order-m5", payload={
            "delivery_id": "delivery-m5", "message_id": "message-m5", "route_processing_result_id": "result-m5",
            "route": {"id": "route", "version": 1},
            "target": {"system_code": "crm", "url": "http://crm", "http_method": "POST"},
            "content": {"format": "JSON", "content_type": "application/json", "body": {}},
            "retry": {"max_attempts": 3, "strategy": "exponential", "initial_delay_seconds": 5, "max_delay_seconds": 60, "multiplier": 2, "retryable_errors": []},
        },
    )
    with session_factory() as session:
        session.add(ProcessingOutboxEvent(
            id="outbox-row-m5", event_id=event.event_id, event_type=event.event_type, aggregate_id=event.aggregate_id,
            topic="integration.delivery.requested", message_key="delivery-m5", envelope=envelope_json(event), publish_attempt_count=0,
        ))
        session.commit()
    publisher = OutboxPublisher(session_factory, ProcessingOutboxEvent, producer=None)  # type: ignore[arg-type]
    first = publisher._claim_one()
    assert first is not None
    # The broker has accepted first.envelope, then the publisher process dies
    # before _mark_published. A real restart eventually releases this lease.
    with session_factory() as session:
        row = session.get(ProcessingOutboxEvent, "outbox-row-m5")
        row.publish_lease_until = NOW
        session.commit()
    second = publisher._claim_one()
    assert second is not None
    assert first.envelope["event_id"] == second.envelope["event_id"] == event.event_id
    publisher._mark_published(second)
    with session_factory() as session:
        assert session.get(ProcessingOutboxEvent, "outbox-row-m5").published_at is not None


def test_outbox_publish_failure_leaves_event_unpublished_and_retryable() -> None:
    session_factory = _sessions()
    event = InternalEventEnvelope(
        event_id="outbox-failure-m5", event_type="delivery.requested", aggregate_id="result-failure-m5",
        occurred_at=NOW, payload={},
    )
    with session_factory() as session:
        session.add(ProcessingOutboxEvent(
            id="outbox-failure-row", event_id=event.event_id, event_type=event.event_type, aggregate_id=event.aggregate_id,
            topic="integration.delivery.requested", message_key="delivery-failure-m5", envelope=envelope_json(event), publish_attempt_count=0,
        ))
        session.commit()
    publisher = OutboxPublisher(session_factory, ProcessingOutboxEvent, producer=None)  # type: ignore[arg-type]
    claimed = publisher._claim_one()
    assert claimed is not None
    publisher._record_failure(claimed, "broker unavailable")
    with session_factory() as session:
        row = session.get(ProcessingOutboxEvent, "outbox-failure-row")
        assert row.published_at is None and row.last_publish_error == "broker unavailable"
        row.publish_lease_until = NOW
        session.commit()
    retried = publisher._claim_one()
    assert retried is not None and retried.envelope["event_id"] == event.event_id


def _request(result_id: str) -> DeliveryRequest:
    return DeliveryRequest(
        message_id="message-direct", route_processing_result_id=result_id, route_key="route", route_version=1,
        target_system="crm", payload_format="application/json", transformed_payload="{}",
        target_url="http://crm", http_method="POST",
    )


def _completed_from_requested(requested: InternalEventEnvelope, status: str) -> InternalEventEnvelope:
    payload = DeliveryRequestedPayload.model_validate(requested.payload)
    return InternalEventEnvelope(
        event_id=f"completed-{payload.delivery_id}", event_type="delivery.completed", aggregate_id=payload.delivery_id,
        occurred_at=NOW, correlation_id=requested.correlation_id,
        payload={
            "delivery_id": payload.delivery_id, "message_id": payload.message_id,
            "route_processing_result_id": payload.route_processing_result_id, "target_system": payload.target.system_code,
            "status": status, "attempt_count": 1, "completed_at": NOW.isoformat(),
        },
    )
