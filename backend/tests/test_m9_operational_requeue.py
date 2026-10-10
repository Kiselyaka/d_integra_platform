from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from delivery_service.delivery.application import DeliveryProcessor
from delivery_service.delivery.models import Delivery, DeliveryAttempt, DeliveryBase, DeliveryOutboxEvent
from delivery_service.delivery.ports import DeliveryExecution
from delivery_service.delivery.requeue import (
    DeliveryNotDeadLetterError,
    DeliveryNotFoundError,
    RequeueDeliveryService,
)
from integration_api.message_processing.models import Base, Message, RouteProcessingResult
from integration_api.operational_query.outcome import derive_outcome
from integration_api.operational_query.pagination import decode_cursor
from integration_api.operational_query.service import OperationalQueryService


NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)


def _factory() -> sessionmaker:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    return sessionmaker(engine)


def _delivery(identifier: str, message_id: str, result_id: str, *, parent: str | None = None) -> Delivery:
    return Delivery.create(
        delivery_id=identifier, requeued_from_delivery_id=parent, idempotency_key="original-idempotency",
        message_id=message_id, route_processing_result_id=result_id, route_key="route", route_version=1,
        target_system="crm", payload_format="application/json", transformed_payload='{"id":"order"}',
        target_url="http://crm.test/orders", http_method="POST", max_attempts=2, retry_strategy="exponential",
        retry_initial_delay_seconds=1, retry_max_delay_seconds=2, retry_multiplier=2, retryable_errors="HTTP_503",
        correlation_id="correlation-m9",
    )


def _message(identifier: str, status: str) -> Message:
    return Message(
        id=identifier, external_event_id=f"event-{identifier}", source_system="mini-shop", event_type="order.created",
        payload={"order_id": identifier}, status=status, correlation_id="correlation-m9", schema_version=1,
        occurred_at=NOW, received_at=NOW,
    )


def _route(identifier: str, message_id: str, status: str) -> RouteProcessingResult:
    return RouteProcessingResult(
        id=identifier, message_id=message_id, route_key=f"route-{identifier}", route_version=1,
        status=status, started_at=NOW, completed_at=NOW,
        transformation_id="transformation-v1", transformation_version=1,
        source_validation_schema_key="source-schema", source_validation_schema_version=1,
        target_validation_schema_key="target-schema", target_validation_schema_version=1,
    )


def test_requeue_creates_queued_snapshot_child_and_keeps_dead_letter_immutable() -> None:
    factory = _factory()
    message = _message("message-original", "ROUTING_FAILED")
    route = _route("rpr-original", message.id, "DELIVERY_REQUESTED")
    source = _delivery("delivery-original", "message-original", "rpr-original")
    source.queue(NOW)
    source.start_processing(NOW)
    source.dead_letter(NOW, "non_retryable_transport_error")
    source.attempt_count = 2
    source_id = source.id
    with factory.begin() as session:
        session.add_all([message, route, source])

    service = RequeueDeliveryService(factory)
    first = service.requeue(source_id)
    second = service.requeue(source_id)

    assert first.created is True
    assert second.created is False and second.delivery.id == first.delivery.id
    assert first.delivery.id != source_id
    assert first.delivery.status == "QUEUED"
    assert first.delivery.requeued_from_delivery_id == source_id
    assert first.delivery.idempotency_key == "original-idempotency"
    assert first.delivery.transformed_payload == '{"id":"order"}'
    assert first.delivery.target_url == "http://crm.test/orders"
    with factory() as session:
        original = session.get(Delivery, source_id)
        assert original is not None and original.status == "DEAD_LETTER" and original.attempt_count == 2

    processor = DeliveryProcessor(factory, _SuccessfulExecutor(), None, stale_processing_timeout_seconds=60)
    assert processor.process_available(NOW) == 1
    with factory() as session:
        recovery = session.get(Delivery, first.delivery.id)
        events = list(session.query(DeliveryOutboxEvent).where(DeliveryOutboxEvent.delivery_id == first.delivery.id))
    assert recovery is not None and recovery.status == "DELIVERED"
    assert [(event.event_type, event.topic) for event in events] == [
        ("delivery.recovery.completed", "integration.delivery.recovery.completed")
    ]
    historical = OperationalQueryService(factory).message_details("message-original")
    assert historical["derived_outcome"] == "FAILED"
    assert historical["initial_delivery_count"] == 1
    assert historical["recovery_delivery_count"] == 1


def test_operational_query_projects_outcome_cursor_timeline_and_historical_payload() -> None:
    factory = _factory()
    success = _message("message-success", "COMPLETED")
    partial = _message("message-partial", "ROUTING_FAILED")
    success_route = _route("rpr-success", success.id, "DELIVERY_REQUESTED")
    partial_ok = _route("rpr-partial-ok", partial.id, "DELIVERY_REQUESTED")
    partial_failed = _route("rpr-partial-failed", partial.id, "TARGET_VALIDATION_FAILED")
    delivered = _delivery("delivery-success", success.id, success_route.id)
    delivered.status, delivered.completed_at = "DELIVERED", NOW
    partial_delivery = _delivery("delivery-partial", partial.id, partial_ok.id)
    partial_delivery.status, partial_delivery.completed_at = "DELIVERED", NOW
    attempt = DeliveryAttempt.started(delivered.id, 1, NOW)
    attempt.mark_succeeded(NOW, 202)
    success_id, partial_id, delivered_id = success.id, partial.id, delivered.id
    with factory.begin() as session:
        session.add_all([success, partial, success_route, partial_ok, partial_failed, delivered, partial_delivery, attempt])

    service = OperationalQueryService(factory)
    page = service.list_messages(limit=1, cursor=None)
    assert len(page["items"]) == 1 and page["next_cursor"] is not None
    assert decode_cursor(page["next_cursor"]) is not None
    details = service.message_details(success_id)
    assert details["derived_outcome"] == "SUCCESS"
    assert details["raw_payload_available"] is False and details["canonical_payload"] == {"order_id": success_id}
    delivery_details = service.delivery_details(delivered_id)
    assert delivery_details["historical_route_processing"]["transformation_id"] == "transformation-v1"
    assert delivery_details["auth_profile"] is None
    partial_details = service.message_details(partial_id)
    assert partial_details["derived_outcome"] == "PARTIAL_SUCCESS"
    timeline = service.message_timeline(success_id)
    assert {item["kind"] for item in timeline["items"]} == {
        "MESSAGE_RECEIVED", "ROUTE_PROCESSING_STARTED", "DELIVERY_REQUESTED", "DELIVERY_CREATED",
        "DELIVERY_ATTEMPT_STARTED", "DELIVERY_ATTEMPT_SUCCEEDED", "DELIVERY_DELIVERED",
    }
    assert timeline == service.message_timeline(success_id)
    correlation = service.correlation_trace("correlation-m9")
    assert {item["message_id"] for item in correlation["messages"]} == {success_id, partial_id}


def test_outcome_cursor_scans_past_non_matches_without_losing_messages() -> None:
    factory = _factory()
    active_top = _message("message-z-active", "VALIDATED")
    success_top = _message("message-y-success", "COMPLETED")
    active_middle = _message("message-x-active", "VALIDATED")
    success_bottom = _message("message-w-success", "COMPLETED")
    rows = [active_top, success_top, active_middle, success_bottom]
    routes = [
        _route("rpr-y", success_top.id, "DELIVERY_REQUESTED"),
        _route("rpr-w", success_bottom.id, "DELIVERY_REQUESTED"),
    ]
    deliveries = [
        _delivery("delivery-y", success_top.id, routes[0].id),
        _delivery("delivery-w", success_bottom.id, routes[1].id),
    ]
    for delivery in deliveries:
        delivery.status = "DELIVERED"
        delivery.completed_at = NOW
    with factory.begin() as session:
        session.add_all([*rows, *routes, *deliveries])

    service = OperationalQueryService(factory)
    first = service.list_messages(limit=1, cursor=None, outcome="SUCCESS")
    assert [item["message_id"] for item in first["items"]] == ["message-y-success"]
    assert first["next_cursor"] is not None
    second = service.list_messages(limit=1, cursor=decode_cursor(first["next_cursor"]), outcome="SUCCESS")
    assert [item["message_id"] for item in second["items"]] == ["message-w-success"]
    assert second["next_cursor"] is None


def test_requeue_rejects_non_dead_letter_and_allows_a_lineage_chain() -> None:
    factory = _factory()
    queued = _delivery("delivery-queued", "message-queued", "rpr-queued")
    queued.queue(NOW)
    failed = _delivery("delivery-dead", "message-dead", "rpr-dead")
    failed.queue(NOW)
    failed.start_processing(NOW)
    failed.dead_letter(NOW, "http_404")
    queued_id, failed_id = queued.id, failed.id
    with factory.begin() as session:
        session.add_all([queued, failed])

    service = RequeueDeliveryService(factory)
    try:
        service.requeue("unknown")
    except DeliveryNotFoundError:
        pass
    else:
        raise AssertionError("unknown Delivery must not be requeued")
    try:
        service.requeue(queued_id)
    except DeliveryNotDeadLetterError:
        pass
    else:
        raise AssertionError("non-terminal Delivery must not be requeued")

    first = service.requeue(failed_id).delivery
    with factory.begin() as session:
        persisted = session.get(Delivery, first.id)
        assert persisted is not None
        persisted.start_processing(NOW)
        persisted.dead_letter(NOW, "http_404_again")
    second = service.requeue(first.id).delivery
    assert second.requeued_from_delivery_id == first.id
    assert second.idempotency_key == "original-idempotency"
    details = OperationalQueryService(factory).delivery_details(second.id)
    assert [item["delivery_id"] for item in details["lineage"]] == [failed_id, first.id, second.id]


def test_derived_outcomes_cover_terminal_and_non_terminal_states() -> None:
    assert derive_outcome("COMPLETED", ["DELIVERY_REQUESTED"], ["DELIVERED"]) == "SUCCESS"
    assert derive_outcome("ROUTING_FAILED", ["DELIVERY_REQUESTED", "TARGET_VALIDATION_FAILED"], ["DELIVERED"]) == "PARTIAL_SUCCESS"
    assert derive_outcome("ROUTING_FAILED", ["DELIVERY_REQUESTED"], ["DEAD_LETTER"]) == "FAILED"
    assert derive_outcome("NO_ROUTE", [], []) == "FAILED"
    assert derive_outcome("VALIDATED", ["DELIVERY_REQUESTED"], ["QUEUED"]) == "IN_PROGRESS"


class _SuccessfulExecutor:
    def execute(self, _: Delivery) -> DeliveryExecution:
        return DeliveryExecution(http_status=202)
