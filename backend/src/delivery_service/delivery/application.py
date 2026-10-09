from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from delivery_service.delivery.models import (
    Delivery,
    DeliveryAttempt,
    DeliveryAttemptOutcome,
    DeliveryFailure,
    DeliveryOutboxEvent,
    DeliveryStatus,
)
from delivery_service.delivery.ports import DeliveryExecution, DeliveryExecutorPort, MessageCompletionPort
from shared.messaging.contracts import InternalEventEnvelope, envelope_json, utc_now


@dataclass(frozen=True)
class RetryPolicySnapshot:
    max_attempts: int
    strategy: str
    initial_delay_seconds: int
    max_delay_seconds: int
    multiplier: int
    retryable_errors: str


@dataclass(frozen=True)
class DeliveryRequest:
    message_id: str
    route_processing_result_id: str
    route_key: str
    route_version: int
    target_system: str
    payload_format: str
    transformed_payload: str
    target_url: str
    http_method: str


class DeliveryRequestService:
    """M4 compatibility fixture for isolated Delivery aggregate tests.

    Production M5 code never invokes this service: requested Deliveries are
    created exclusively by ``DeliveryRequestedConsumer`` after Kafka Inbox
    deduplication.
    """

    def __init__(self, session_factory: sessionmaker[Session], retry_policy: RetryPolicySnapshot) -> None:
        self._session_factory = session_factory
        self._retry_policy = retry_policy

    def request(self, request: DeliveryRequest, now: datetime | None = None) -> Delivery:
        now = now or datetime.now(timezone.utc)
        with self._session_factory() as session:
            existing = session.scalar(
                select(Delivery).where(Delivery.route_processing_result_id == request.route_processing_result_id)
            )
            if existing is not None:
                return existing
            delivery = Delivery.create(
                message_id=request.message_id,
                route_processing_result_id=request.route_processing_result_id,
                route_key=request.route_key,
                route_version=request.route_version,
                target_system=request.target_system,
                payload_format=request.payload_format,
                transformed_payload=request.transformed_payload,
                target_url=request.target_url,
                http_method=request.http_method,
                max_attempts=self._retry_policy.max_attempts,
                retry_strategy=self._retry_policy.strategy,
                retry_initial_delay_seconds=self._retry_policy.initial_delay_seconds,
                retry_max_delay_seconds=self._retry_policy.max_delay_seconds,
                retry_multiplier=self._retry_policy.multiplier,
                retryable_errors=self._retry_policy.retryable_errors,
            )
            delivery.queue(now)
            session.add(delivery)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(Delivery).where(Delivery.route_processing_result_id == request.route_processing_result_id)
                )
                if existing is None:
                    raise
                return existing
            return delivery


class DeliveryProcessor:
    """Claims queued Deliveries and records the outcome of each HTTP attempt."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        executor: DeliveryExecutorPort,
        completion: MessageCompletionPort | None,
        stale_processing_timeout_seconds: int,
    ) -> None:
        self._session_factory = session_factory
        self._executor = executor
        # Only M4 tests may inject this compatibility seam. M5 production
        # completion is emitted through Delivery Outbox and Kafka.
        self._completion = completion
        self._stale_processing_timeout = timedelta(seconds=stale_processing_timeout_seconds)

    def process_available(self, now: datetime | None = None, limit: int = 100) -> int:
        now = now or datetime.now(timezone.utc)
        processed = 0
        for _ in range(limit):
            delivery_id = self._claim_next(now)
            if delivery_id is None:
                break
            processed += 1
            self._execute_claimed(delivery_id, now)
        return processed

    def recover_stale(self, now: datetime | None = None) -> int:
        now = now or datetime.now(timezone.utc)
        cutoff = now - self._stale_processing_timeout
        with self._session_factory() as session:
            stale = list(
                session.scalars(
                    select(Delivery).where(
                        Delivery.status == DeliveryStatus.PROCESSING,
                        Delivery.processing_started_at < cutoff,
                    )
                )
            )
            recovered_message_ids: list[str] = []
            for delivery in stale:
                failure = DeliveryFailure(
                    category="STALE_PROCESSING",
                    code="STALE_PROCESSING",
                    message="Delivery processing lease expired before completion",
                    retryable=True,
                )
                attempt = session.scalar(
                    select(DeliveryAttempt).where(
                        DeliveryAttempt.delivery_id == delivery.id,
                        DeliveryAttempt.attempt_number == delivery.attempt_count,
                    )
                )
                if attempt is not None and attempt.outcome == DeliveryAttemptOutcome.PROCESSING:
                    attempt.mark_interrupted(now, failure.message)
                delivery.register_failure(failure)
                if delivery.attempt_count < delivery.max_attempts:
                    delivery.schedule_retry(now)
                else:
                    delivery.dead_letter(now, "retry_limit_exhausted_after_stale_processing")
                    self._append_terminal_outbox(session, delivery, now)
                recovered_message_ids.append(delivery.message_id)
            try:
                session.commit()
            except StaleDataError:
                session.rollback()
                return 0
        if self._completion is not None:
            for message_id in recovered_message_ids:
                self._completion.refresh(message_id)
        return len(recovered_message_ids)

    def _claim_next(self, now: datetime) -> str | None:
        with self._session_factory() as session:
            delivery = session.scalar(
                select(Delivery)
                .where(
                    or_(
                        Delivery.status == DeliveryStatus.QUEUED,
                        and_(
                            Delivery.status == DeliveryStatus.RETRY_WAIT,
                            Delivery.next_retry_at <= now,
                        ),
                    )
                )
                .order_by(Delivery.created_at, Delivery.id)
                .limit(1)
            )
            if delivery is None:
                return None
            if delivery.status == DeliveryStatus.RETRY_WAIT:
                delivery.queue(now)
            delivery.start_processing(now)
            session.add(DeliveryAttempt.started(delivery.id, delivery.attempt_count, now))
            try:
                session.commit()
            except StaleDataError:
                session.rollback()
                return None
            return delivery.id

    def _execute_claimed(self, delivery_id: str, now: datetime) -> None:
        with self._session_factory() as session:
            delivery = session.get(Delivery, delivery_id)
            if delivery is None or delivery.status != DeliveryStatus.PROCESSING:
                return
            try:
                execution = self._executor.execute(delivery)
            except Exception as error:  # Converted to DeliveryFailure at this local transport boundary.
                failure = _failure_from_exception(error)
                self._finish_failure(session, delivery, failure, now)
            else:
                self._finish_success(session, delivery, execution, now)
            message_id = delivery.message_id
        if self._completion is not None:
            self._completion.refresh(message_id)

    @staticmethod
    def _active_attempt(session: Session, delivery: Delivery) -> DeliveryAttempt:
        attempt = session.scalar(
            select(DeliveryAttempt).where(
                DeliveryAttempt.delivery_id == delivery.id,
                DeliveryAttempt.attempt_number == delivery.attempt_count,
            )
        )
        if attempt is None:
            raise RuntimeError(f"Missing attempt {delivery.attempt_count} for Delivery {delivery.id}")
        return attempt

    def _finish_success(self, session: Session, delivery: Delivery, execution: DeliveryExecution, now: datetime) -> None:
        self._active_attempt(session, delivery).mark_succeeded(now, execution.http_status)
        delivery.mark_delivered(now)
        self._append_terminal_outbox(session, delivery, now)
        session.commit()

    def _finish_failure(self, session: Session, delivery: Delivery, failure: DeliveryFailure, now: datetime) -> None:
        self._active_attempt(session, delivery).mark_failed(now, failure)
        delivery.register_failure(failure)
        if failure.retryable and delivery.attempt_count < delivery.max_attempts:
            delivery.schedule_retry(now)
        else:
            reason = "retry_limit_exhausted" if failure.retryable else "non_retryable_transport_error"
            delivery.dead_letter(now, reason)
            self._append_terminal_outbox(session, delivery, now)
        session.commit()

    @staticmethod
    def _append_terminal_outbox(session: Session, delivery: Delivery, now: datetime) -> None:
        """Persist terminal Delivery state and its Kafka events atomically."""

        status = DeliveryStatus(delivery.status)
        if status not in {DeliveryStatus.DELIVERED, DeliveryStatus.DEAD_LETTER}:
            return
        existing = session.scalar(
            select(DeliveryOutboxEvent).where(
                DeliveryOutboxEvent.delivery_id == delivery.id,
                DeliveryOutboxEvent.event_type == "delivery.completed",
            )
        )
        if existing is not None:
            return
        completed_at = delivery.completed_at or now
        completed = InternalEventEnvelope(
            event_id=str(uuid4()),
            event_type="delivery.completed",
            aggregate_id=delivery.id,
            occurred_at=completed_at,
            correlation_id=delivery.correlation_id,
            payload={
                "delivery_id": delivery.id,
                "message_id": delivery.message_id,
                "route_processing_result_id": delivery.route_processing_result_id,
                "target_system": delivery.target_system,
                "status": status.value,
                "attempt_count": delivery.attempt_count,
                "completed_at": completed_at.isoformat(),
            },
        )
        session.add(
            DeliveryOutboxEvent(
                id=str(uuid4()), event_id=completed.event_id, event_type=completed.event_type,
                aggregate_id=completed.aggregate_id, delivery_id=delivery.id,
                topic="integration.delivery.completed", message_key=delivery.id,
                envelope=envelope_json(completed), publish_attempt_count=0,
            )
        )
        if status is not DeliveryStatus.DEAD_LETTER:
            return
        dead_letter = InternalEventEnvelope(
            event_id=str(uuid4()),
            event_type="delivery.dead_lettered",
            aggregate_id=delivery.id,
            occurred_at=completed_at,
            correlation_id=delivery.correlation_id,
            payload={
                "delivery_id": delivery.id,
                "message_id": delivery.message_id,
                "route_processing_result_id": delivery.route_processing_result_id,
                "target_system": delivery.target_system,
                "reason": delivery.dead_letter_reason or "unspecified",
                "attempt_count": delivery.attempt_count,
                "last_error": {
                    "category": delivery.last_error_category,
                    "code": delivery.last_error_code,
                    "message": delivery.last_error_message,
                },
            },
        )
        session.add(
            DeliveryOutboxEvent(
                id=str(uuid4()), event_id=dead_letter.event_id, event_type=dead_letter.event_type,
                aggregate_id=dead_letter.aggregate_id, delivery_id=delivery.id,
                topic="integration.delivery.dead_lettered", message_key=delivery.id,
                envelope=envelope_json(dead_letter), publish_attempt_count=0,
            )
        )


def _failure_from_exception(error: Exception) -> DeliveryFailure:
    failure = getattr(error, "delivery_failure", None)
    if isinstance(failure, DeliveryFailure):
        return failure
    return DeliveryFailure(category="UNEXPECTED_ERROR", code=type(error).__name__, message=str(error), retryable=False)
