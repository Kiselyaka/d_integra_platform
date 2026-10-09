from datetime import UTC, datetime, timedelta

import pytest
import httpx
from sqlalchemy import select
from sqlalchemy.orm.exc import StaleDataError

from delivery_service.delivery.application import DeliveryProcessor, DeliveryRequest, DeliveryRequestService, RetryPolicySnapshot
from delivery_service.delivery.models import (
    Delivery,
    DeliveryAttempt,
    DeliveryBase,
    DeliveryFailure,
    DeliveryStatus,
    DeliveryTransitionError,
)
from delivery_service.delivery.ports import DeliveryExecution
from integration_api.message_processing.http_delivery import DeliveryTransportError, HttpDeliveryExecutor
from shared.database import create_database_engine, create_session_factory


NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
POLICY = RetryPolicySnapshot(3, "exponential", 5, 60, 2, "TIMEOUT,REMOTE_SERVER_ERROR")


class NoopCompletion:
    def __init__(self) -> None:
        self.message_ids: list[str] = []

    def refresh(self, message_id: str) -> None:
        self.message_ids.append(message_id)


class SequenceExecutor:
    def __init__(self, results: list[DeliveryExecution | Exception]) -> None:
        self._results = iter(results)
        self.keys: list[str] = []
        self.snapshots: list[tuple[str, str, str]] = []

    def execute(self, delivery: Delivery) -> DeliveryExecution:
        self.keys.append(delivery.idempotency_key)
        self.snapshots.append((delivery.transformed_payload, delivery.target_url, delivery.http_method))
        result = next(self._results)
        if isinstance(result, Exception):
            raise result
        return result


class Retryable503(RuntimeError):
    delivery_failure = DeliveryFailure("REMOTE_SERVER_ERROR", "HTTP_503", "Target returned HTTP 503", True, 503)


class NonRetryable409(RuntimeError):
    delivery_failure = DeliveryFailure("CONTRACT_ERROR", "HTTP_409", "Target returned HTTP 409", False, 409)


def _session_factory():
    engine = create_database_engine("sqlite+pysqlite:///:memory:")
    DeliveryBase.metadata.create_all(engine)
    return create_session_factory(engine)


def _request(
    service: DeliveryRequestService,
    result_id: str = "route-result-001",
    target_system: str = "crm",
    target_url: str = "http://crm:8000/api/integration/orders",
) -> Delivery:
    return service.request(
        DeliveryRequest(
            message_id="message-001", route_processing_result_id=result_id,
            route_key="mini-shop-order-created-to-crm", route_version=1, target_system=target_system,
            payload_format="application/json", transformed_payload='{"external_order_id":"order-001"}',
            target_url=target_url, http_method="POST",
        ),
        now=NOW,
    )


def test_delivery_domain_state_machine_rejects_invalid_and_terminal_transitions() -> None:
    delivery = Delivery.create(
        message_id="message-001", route_processing_result_id="result-001", route_key="route", route_version=1,
        target_system="crm", payload_format="application/json", transformed_payload="{}", target_url="http://crm", http_method="POST",
        max_attempts=3, retry_strategy="exponential", retry_initial_delay_seconds=5, retry_max_delay_seconds=60,
        retry_multiplier=2, retryable_errors="TIMEOUT",
    )
    with pytest.raises(DeliveryTransitionError):
        delivery.start_processing(NOW)
    delivery.queue(NOW)
    delivery.start_processing(NOW)
    assert delivery.attempt_count == 1
    delivery.mark_delivered(NOW)
    assert delivery.status == DeliveryStatus.DELIVERED
    with pytest.raises(DeliveryTransitionError):
        delivery.queue(NOW)
    with pytest.raises(DeliveryTransitionError):
        delivery.dead_letter(NOW, "must not mutate terminal delivery")


def test_retry_uses_one_delivery_two_attempts_and_an_immutable_idempotency_key() -> None:
    session_factory = _session_factory()
    delivery = _request(DeliveryRequestService(session_factory, POLICY))
    executor = SequenceExecutor([Retryable503(), DeliveryExecution(http_status=201)])
    completion = NoopCompletion()
    processor = DeliveryProcessor(session_factory, executor, completion, stale_processing_timeout_seconds=60)

    assert processor.process_available(NOW) == 1
    with session_factory() as session:
        persisted = session.get(Delivery, delivery.id)
        attempts = list(session.scalars(select(DeliveryAttempt).order_by(DeliveryAttempt.attempt_number)))
    assert persisted is not None and persisted.status == DeliveryStatus.RETRY_WAIT
    assert persisted.next_retry_at is not None
    assert persisted.next_retry_at.replace(tzinfo=UTC) == NOW + timedelta(seconds=5)
    assert [(attempt.attempt_number, attempt.outcome, attempt.http_status) for attempt in attempts] == [(1, "FAILED", 503)]
    assert processor.process_available(NOW + timedelta(seconds=4)) == 0
    assert processor.process_available(NOW + timedelta(seconds=5)) == 1

    with session_factory() as session:
        persisted = session.get(Delivery, delivery.id)
        attempts = list(session.scalars(select(DeliveryAttempt).order_by(DeliveryAttempt.attempt_number)))
    assert persisted is not None
    assert persisted.status == DeliveryStatus.DELIVERED
    assert persisted.attempt_count == 2
    assert persisted.idempotency_key == persisted.id
    assert executor.keys == [delivery.id, delivery.id]
    assert executor.snapshots[0] == executor.snapshots[1]
    assert [(attempt.attempt_number, attempt.outcome) for attempt in attempts] == [(1, "FAILED"), (2, "SUCCEEDED")]


def test_non_retryable_error_and_retry_exhaustion_end_in_dead_letter() -> None:
    session_factory = _session_factory()
    completion = NoopCompletion()
    non_retryable = _request(DeliveryRequestService(session_factory, POLICY), "route-result-409")
    DeliveryProcessor(
        session_factory, SequenceExecutor([NonRetryable409()]), completion, stale_processing_timeout_seconds=60
    ).process_available(NOW)
    with session_factory() as session:
        persisted = session.get(Delivery, non_retryable.id)
    assert persisted is not None and persisted.status == DeliveryStatus.DEAD_LETTER
    assert persisted.dead_letter_reason == "non_retryable_transport_error"

    exhausted = _request(
        DeliveryRequestService(session_factory, RetryPolicySnapshot(1, "exponential", 5, 60, 2, "REMOTE_SERVER_ERROR")),
        "route-result-503",
    )
    processor = DeliveryProcessor(session_factory, SequenceExecutor([Retryable503()]), completion, stale_processing_timeout_seconds=60)
    processor.process_available(NOW)
    with session_factory() as session:
        persisted = session.get(Delivery, exhausted.id)
        attempts = list(session.scalars(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == exhausted.id)))
    assert persisted is not None and persisted.status == DeliveryStatus.DEAD_LETTER
    assert persisted.dead_letter_reason == "retry_limit_exhausted"
    assert len(attempts) == 1


def test_delivery_request_is_deduplicated_by_route_processing_result() -> None:
    session_factory = _session_factory()
    service = DeliveryRequestService(session_factory, POLICY)
    first = _request(service)
    second = _request(service)
    assert first.id == second.id
    with session_factory() as session:
        assert len(list(session.scalars(select(Delivery)))) == 1


def test_optimistic_lock_allows_only_one_claim() -> None:
    session_factory = _session_factory()
    delivery = _request(DeliveryRequestService(session_factory, POLICY))
    first = session_factory()
    second = session_factory()
    try:
        first_delivery = first.get(Delivery, delivery.id)
        second_delivery = second.get(Delivery, delivery.id)
        assert first_delivery is not None and second_delivery is not None
        first_delivery.start_processing(NOW)
        first.commit()
        second_delivery.start_processing(NOW)
        with pytest.raises(StaleDataError):
            second.commit()
    finally:
        first.close()
        second.close()


def test_stale_processing_keeps_attempt_history_and_schedules_retry() -> None:
    session_factory = _session_factory()
    delivery = _request(DeliveryRequestService(session_factory, POLICY))
    completion = NoopCompletion()
    processor = DeliveryProcessor(session_factory, SequenceExecutor([]), completion, stale_processing_timeout_seconds=60)
    assert processor._claim_next(NOW) == delivery.id
    assert processor.recover_stale(NOW + timedelta(seconds=61)) == 1
    with session_factory() as session:
        persisted = session.get(Delivery, delivery.id)
        attempt = session.scalar(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id))
    assert persisted is not None and persisted.status == DeliveryStatus.RETRY_WAIT
    assert persisted.last_error_category == "STALE_PROCESSING"
    assert attempt is not None and attempt.outcome == "INTERRUPTED"
    assert attempt.error_category == "STALE_PROCESSING"


def test_http_executor_uses_delivery_idempotency_key_and_classifies_503_and_409(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = _session_factory()
    delivery = _request(DeliveryRequestService(session_factory, POLICY))
    captured: dict[str, object] = {}

    class Response:
        status_code = 201

    def success(method: str, url: str, **kwargs: object) -> Response:
        captured["method"] = method
        captured["url"] = url
        captured.update(kwargs)
        return Response()

    monkeypatch.setattr(httpx, "request", success)
    assert HttpDeliveryExecutor(2.5).execute(delivery).http_status == 201
    assert captured["headers"] == {
        "Content-Type": "application/json",
        "Idempotency-Key": delivery.id,
    }

    class Unavailable:
        status_code = 503

    monkeypatch.setattr(httpx, "request", lambda *args, **kwargs: Unavailable())
    with pytest.raises(DeliveryTransportError) as unavailable:
        HttpDeliveryExecutor(2.5).execute(delivery)
    assert unavailable.value.delivery_failure.category == "REMOTE_SERVER_ERROR"
    assert unavailable.value.delivery_failure.retryable is True

    class Conflict:
        status_code = 409

    monkeypatch.setattr(httpx, "request", lambda *args, **kwargs: Conflict())
    with pytest.raises(DeliveryTransportError) as conflict:
        HttpDeliveryExecutor(2.5).execute(delivery)
    assert conflict.value.delivery_failure.category == "CONTRACT_ERROR"
    assert conflict.value.delivery_failure.retryable is False


def test_http_timeout_moves_delivery_to_retry_wait(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = _session_factory()
    delivery = _request(DeliveryRequestService(session_factory, POLICY), "route-result-timeout")

    def timeout(*args: object, **kwargs: object) -> None:
        raise httpx.ReadTimeout("target did not respond")

    monkeypatch.setattr(httpx, "request", timeout)
    processor = DeliveryProcessor(session_factory, HttpDeliveryExecutor(2.5), NoopCompletion(), stale_processing_timeout_seconds=60)
    assert processor.process_available(NOW) == 1
    with session_factory() as session:
        persisted = session.get(Delivery, delivery.id)
        attempt = session.scalar(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id))
    assert persisted is not None and persisted.status == DeliveryStatus.RETRY_WAIT
    assert persisted.last_error_category == "TIMEOUT"
    assert attempt is not None and attempt.outcome == "FAILED"
    assert attempt.error_category == "TIMEOUT"


def test_mini_shop_conflict_moves_delivery_to_dead_letter(monkeypatch: pytest.MonkeyPatch) -> None:
    session_factory = _session_factory()
    delivery = _request(
        DeliveryRequestService(session_factory, POLICY),
        "route-result-mini-shop-conflict",
        target_system="mini-shop",
        target_url="http://mini-shop:8002/api/integration/reservation-results",
    )

    class Conflict:
        status_code = 409

    monkeypatch.setattr(httpx, "request", lambda *args, **kwargs: Conflict())
    processor = DeliveryProcessor(session_factory, HttpDeliveryExecutor(2.5), NoopCompletion(), stale_processing_timeout_seconds=60)
    assert processor.process_available(NOW) == 1
    with session_factory() as session:
        persisted = session.get(Delivery, delivery.id)
        attempt = session.scalar(select(DeliveryAttempt).where(DeliveryAttempt.delivery_id == delivery.id))
    assert persisted is not None and persisted.status == DeliveryStatus.DEAD_LETTER
    assert persisted.dead_letter_reason == "non_retryable_transport_error"
    assert persisted.next_retry_at is None
    assert attempt is not None and attempt.http_status == 409
