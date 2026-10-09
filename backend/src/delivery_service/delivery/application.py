from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.exc import StaleDataError

from delivery_service.delivery.models import (
    Delivery,
    DeliveryAttempt,
    DeliveryAttemptOutcome,
    DeliveryFailure,
    DeliveryStatus,
)
from delivery_service.delivery.ports import DeliveryExecution, DeliveryExecutorPort, MessageCompletionPort


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
    """Local M4 boundary used by Message Processing to request a Delivery."""

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
        completion: MessageCompletionPort,
        stale_processing_timeout_seconds: int,
    ) -> None:
        self._session_factory = session_factory
        self._executor = executor
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
                recovered_message_ids.append(delivery.message_id)
            try:
                session.commit()
            except StaleDataError:
                session.rollback()
                return 0
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
        session.commit()

    def _finish_failure(self, session: Session, delivery: Delivery, failure: DeliveryFailure, now: datetime) -> None:
        self._active_attempt(session, delivery).mark_failed(now, failure)
        delivery.register_failure(failure)
        if failure.retryable and delivery.attempt_count < delivery.max_attempts:
            delivery.schedule_retry(now)
        else:
            reason = "retry_limit_exhausted" if failure.retryable else "non_retryable_transport_error"
            delivery.dead_letter(now, reason)
        session.commit()


def _failure_from_exception(error: Exception) -> DeliveryFailure:
    failure = getattr(error, "delivery_failure", None)
    if isinstance(failure, DeliveryFailure):
        return failure
    return DeliveryFailure(category="UNEXPECTED_ERROR", code=type(error).__name__, message=str(error), retryable=False)
