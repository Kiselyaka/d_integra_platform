from sqlalchemy import func, select

from delivery_service.delivery.application import DeliveryProcessor, RetryPolicySnapshot
from delivery_service.delivery.models import DeliveryBase, DeliveryFailure, DeliveryOutboxEvent
from delivery_service.delivery.requested_consumer import DeliveryRequestedConsumer
from delivery_service.delivery.ports import DeliveryExecution
from integration_api.message_processing.completion_consumer import MessageCompletionConsumer
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Base, Message, ProcessingOutboxEvent, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.service import EventIngestionService
from integration_api.message_processing.transformation import OrderCreatedToCrmTransformation, RouteTransformationProvider
from integration_api.message_processing.validation import PydanticSourceValidator
from shared.database import create_database_engine, create_session_factory
from shared.messaging.contracts import DeliveryCompletedPayload, DeliveryRequestedPayload, InternalEventEnvelope


def _event(event_id: str = "event-001") -> dict[str, object]:
    return {
        "event_id": event_id, "event_type": "order.created", "source_system": "mini-shop",
        "occurred_at": "2026-10-04T12:00:00Z", "correlation_id": "order-001", "schema_version": 1,
        "payload": {
            "order_id": "order-001",
            "customer": {"customer_id": "customer-001", "full_name": "Ada Lovelace", "email": "ada@example.test"},
            "shipping_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1"},
            "items": [{"product_code": "SKU-001", "name": "Example Product", "quantity": 2, "unit_price": "10.50"}],
            "currency": "EUR", "total_amount": "21.00",
        },
    }


class RecordingExecutor:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []

    def execute(self, delivery: object) -> DeliveryExecution:
        self.calls.append((getattr(delivery, "target_system"), getattr(delivery, "idempotency_key")))
        return DeliveryExecution(http_status=202)


class FailingExecutor:
    def execute(self, delivery: object) -> DeliveryExecution:
        error = RuntimeError("CRM unavailable")
        error.delivery_failure = DeliveryFailure("CONTRACT_ERROR", "HTTP_409", "CRM unavailable", False, 409)  # type: ignore[attr-defined]
        raise error


class NoRouteProvider:
    def find_matching_routes(self, event: ExternalEventEnvelope) -> list[object]:
        return []


def _services(executor: object | None = None, route_provider: object | None = None):
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    session_factory = create_session_factory(engine)
    processor = DeliveryProcessor(
        session_factory, executor or RecordingExecutor(), None, stale_processing_timeout_seconds=60
    )
    processing = EventProcessingService(
        session_factory,
        route_provider or StaticRouteProvider("http://crm:8000", "http://warehouse:8081"),
        RouteTransformationProvider(),
        RetryPolicySnapshot(3, "exponential", 5, 300, 2, "TIMEOUT"),
    )
    return session_factory, EventIngestionService(session_factory, PydanticSourceValidator()), processing, processor


def _ingest_and_process(ingestion: EventIngestionService, processing: EventProcessingService, processor: DeliveryProcessor, data: dict[str, object]):
    result = ingestion.ingest(ExternalEventEnvelope.model_validate(data))
    if not result.duplicate:
        processing.process(result.message_id)
        requested_consumer = DeliveryRequestedConsumer(processor._session_factory)
        with processor._session_factory() as session:
            requested = list(session.scalars(select(ProcessingOutboxEvent)))
        for item in requested:
            envelope = InternalEventEnvelope.model_validate(item.envelope)
            requested_consumer.handle(envelope, DeliveryRequestedPayload.model_validate(envelope.payload))
        processor.process_available()
        completion_consumer = MessageCompletionConsumer(processor._session_factory)
        with processor._session_factory() as session:
            completed = list(session.scalars(select(DeliveryOutboxEvent).where(DeliveryOutboxEvent.event_type == "delivery.completed")))
        for item in completed:
            envelope = InternalEventEnvelope.model_validate(item.envelope)
            completion_consumer.handle(envelope, DeliveryCompletedPayload.model_validate(envelope.payload))
    return result


def test_accepts_and_processes_a_valid_mini_shop_event() -> None:
    session_factory, ingestion, processing, processor = _services()
    result = _ingest_and_process(ingestion, processing, processor, _event())
    with session_factory() as session:
        message = session.scalar(select(Message))
        results = list(session.scalars(select(RouteProcessingResult)))
    assert result.duplicate is False
    assert message is not None and message.id == result.message_id and message.status == "COMPLETED"
    assert message.payload["order_id"] == "order-001"
    assert {item.status for item in results} == {"DELIVERY_REQUESTED"}


def test_accepting_the_same_source_event_twice_is_idempotent() -> None:
    executor = RecordingExecutor()
    session_factory, ingestion, processing, processor = _services(executor)
    first = _ingest_and_process(ingestion, processing, processor, _event())
    second = _ingest_and_process(ingestion, processing, processor, _event())
    assert first.message_id == second.message_id and second.duplicate is True
    with session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Message)) == 1
    assert len(executor.calls) == 2


def test_stores_source_payload_validation_errors_without_crashing() -> None:
    session_factory, ingestion, processing, processor = _services()
    invalid_event = _event()
    invalid_event["payload"] = {"order_id": "order-001", "items": []}
    result = _ingest_and_process(ingestion, processing, processor, invalid_event)
    with session_factory() as session:
        message = session.get(Message, result.message_id)
    assert message is not None and message.status == "VALIDATION_FAILED" and message.validation_error is not None


def test_marks_message_no_route_when_the_route_provider_has_no_match() -> None:
    session_factory, ingestion, processing, processor = _services(route_provider=NoRouteProvider())
    result = _ingest_and_process(ingestion, processing, processor, _event())
    with session_factory() as session:
        message = session.get(Message, result.message_id)
    assert message is not None and message.status == "NO_ROUTE"


def test_marks_message_routing_failed_after_a_non_retryable_delivery_error() -> None:
    session_factory, ingestion, processing, processor = _services(executor=FailingExecutor())
    result = _ingest_and_process(ingestion, processing, processor, _event())
    with session_factory() as session:
        message = session.get(Message, result.message_id)
        results = list(session.scalars(select(RouteProcessingResult)))
    assert message is not None and message.status == "ROUTING_FAILED"
    assert {item.status for item in results} == {"DELIVERY_REQUESTED"}


def test_order_created_transformation_returns_the_crm_contract() -> None:
    target = OrderCreatedToCrmTransformation().transform(ExternalEventEnvelope.model_validate(_event()))
    assert target == {
        "external_order_id": "order-001",
        "client": {"external_customer_id": "customer-001", "name": "Ada Lovelace", "email": "ada@example.test", "phone": None},
        "deal": {"amount": "21.00", "currency": "EUR"},
        "delivery_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1", "apartment": None},
        "positions": [{"external_product_code": "SKU-001", "title": "Example Product", "quantity": 2, "price": "10.50"}],
    }
