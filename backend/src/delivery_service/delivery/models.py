from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class DeliveryBase(DeclarativeBase):
    """Persistence base owned by the Delivery context.

    References to Message Processing are deliberately logical references: the
    two bounded contexts do not have physical cross-context foreign keys.
    """


class DeliveryStatus(StrEnum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    PROCESSING = "PROCESSING"
    RETRY_WAIT = "RETRY_WAIT"
    DELIVERED = "DELIVERED"
    DEAD_LETTER = "DEAD_LETTER"


class DeliveryAttemptOutcome(StrEnum):
    PROCESSING = "PROCESSING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    INTERRUPTED = "INTERRUPTED"


TERMINAL_DELIVERY_STATUSES = {DeliveryStatus.DELIVERED, DeliveryStatus.DEAD_LETTER}
ACTIVE_DELIVERY_STATUSES = {
    DeliveryStatus.PENDING,
    DeliveryStatus.QUEUED,
    DeliveryStatus.PROCESSING,
    DeliveryStatus.RETRY_WAIT,
}


class DeliveryTransitionError(ValueError):
    pass


@dataclass(frozen=True)
class DeliveryFailure:
    category: str
    code: str | None
    message: str
    retryable: bool
    http_status: int | None = None


class Delivery(DeliveryBase):
    __tablename__ = "deliveries"
    __table_args__ = (
        UniqueConstraint("route_processing_result_id", name="uq_deliveries_route_processing_result"),
        CheckConstraint("attempt_count >= 0", name="ck_deliveries_attempt_count_nonnegative"),
        CheckConstraint("max_attempts >= 1", name="ck_deliveries_max_attempts_positive"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    message_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    route_processing_result_id: Mapped[str] = mapped_column(String(36), nullable=False)
    route_key: Mapped[str] = mapped_column(String(128), nullable=False)
    route_version: Mapped[int] = mapped_column(Integer, nullable=False)
    target_system: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    payload_format: Mapped[str] = mapped_column(String(128), nullable=False)
    transformed_payload: Mapped[str] = mapped_column(Text, nullable=False)
    target_url: Mapped[str] = mapped_column(Text, nullable=False)
    http_method: Mapped[str] = mapped_column(String(16), nullable=False)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_strategy: Mapped[str] = mapped_column(String(32), nullable=False)
    retry_initial_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_max_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    retry_multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    retryable_errors: Mapped[str] = mapped_column(Text, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)
    last_error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    dead_letter_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    lock_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    idempotency_key: Mapped[str] = mapped_column(String(36), nullable=False)

    __mapper_args__ = {"version_id_col": lock_version, "version_id_generator": False}

    @classmethod
    def create(
        cls,
        *,
        message_id: str,
        route_processing_result_id: str,
        route_key: str,
        route_version: int,
        target_system: str,
        payload_format: str,
        transformed_payload: str,
        target_url: str,
        http_method: str,
        max_attempts: int,
        retry_strategy: str,
        retry_initial_delay_seconds: int,
        retry_max_delay_seconds: int,
        retry_multiplier: int,
        retryable_errors: str,
    ) -> "Delivery":
        delivery_id = str(uuid4())
        return cls(
            id=delivery_id,
            message_id=message_id,
            route_processing_result_id=route_processing_result_id,
            route_key=route_key,
            route_version=route_version,
            target_system=target_system,
            status=DeliveryStatus.PENDING,
            payload_format=payload_format,
            transformed_payload=transformed_payload,
            target_url=target_url,
            http_method=http_method,
            attempt_count=0,
            max_attempts=max_attempts,
            retry_strategy=retry_strategy,
            retry_initial_delay_seconds=retry_initial_delay_seconds,
            retry_max_delay_seconds=retry_max_delay_seconds,
            retry_multiplier=retry_multiplier,
            retryable_errors=retryable_errors,
            lock_version=1,
            idempotency_key=delivery_id,
        )

    @property
    def is_terminal(self) -> bool:
        return DeliveryStatus(self.status) in TERMINAL_DELIVERY_STATUSES

    def queue(self, now: datetime) -> None:
        status = DeliveryStatus(self.status)
        if status is DeliveryStatus.PENDING:
            pass
        elif (
            status is DeliveryStatus.RETRY_WAIT
            and self.next_retry_at is not None
            and _normalized_datetime(self.next_retry_at) <= _normalized_datetime(now)
        ):
            pass
        else:
            raise DeliveryTransitionError(f"Cannot queue Delivery in {status}")
        self.status = DeliveryStatus.QUEUED
        self.next_retry_at = None
        self._bump_version()

    def start_processing(self, now: datetime) -> None:
        if DeliveryStatus(self.status) is not DeliveryStatus.QUEUED:
            raise DeliveryTransitionError(f"Cannot start Delivery in {self.status}")
        self.status = DeliveryStatus.PROCESSING
        self.processing_started_at = now
        self.attempt_count += 1
        self._bump_version()

    def mark_delivered(self, now: datetime) -> None:
        self._require_processing("mark delivered")
        self.status = DeliveryStatus.DELIVERED
        self.completed_at = now
        self.next_retry_at = None
        self.processing_started_at = None
        self.last_error_category = None
        self.last_error_code = None
        self.last_error_message = None
        self._bump_version()

    def register_failure(self, failure: DeliveryFailure) -> None:
        self._require_processing("register failure")
        self.last_error_category = failure.category
        self.last_error_code = failure.code
        self.last_error_message = failure.message
        self._bump_version()

    def schedule_retry(self, now: datetime) -> None:
        self._require_processing("schedule retry")
        if self.attempt_count >= self.max_attempts:
            raise DeliveryTransitionError("Retry limit is exhausted")
        self.status = DeliveryStatus.RETRY_WAIT
        self.next_retry_at = now + timedelta(seconds=self.retry_delay_seconds())
        self.processing_started_at = None
        self._bump_version()

    def dead_letter(self, now: datetime, reason: str) -> None:
        self._require_processing("dead letter")
        self.status = DeliveryStatus.DEAD_LETTER
        self.completed_at = now
        self.next_retry_at = None
        self.processing_started_at = None
        self.dead_letter_reason = reason
        self._bump_version()

    def retry_delay_seconds(self) -> int:
        if self.retry_strategy != "exponential":
            raise DeliveryTransitionError(f"Unsupported retry strategy: {self.retry_strategy}")
        delay = self.retry_initial_delay_seconds * (self.retry_multiplier ** (self.attempt_count - 1))
        return min(delay, self.retry_max_delay_seconds)

    def _require_processing(self, action: str) -> None:
        if DeliveryStatus(self.status) is not DeliveryStatus.PROCESSING:
            raise DeliveryTransitionError(f"Cannot {action} for Delivery in {self.status}")

    def _bump_version(self) -> None:
        self.lock_version += 1


def _normalized_datetime(value: datetime) -> datetime:
    """SQLite returns naive values even for timezone-aware DateTime columns."""
    return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


class DeliveryAttempt(DeliveryBase):
    __tablename__ = "delivery_attempts"
    __table_args__ = (UniqueConstraint("delivery_id", "attempt_number", name="uq_delivery_attempts_delivery_number"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    # This FK remains inside the Delivery bounded context.
    delivery_id: Mapped[str] = mapped_column(ForeignKey("deliveries.id"), nullable=False, index=True)
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_category: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    @classmethod
    def started(cls, delivery_id: str, attempt_number: int, now: datetime) -> "DeliveryAttempt":
        return cls(
            delivery_id=delivery_id,
            attempt_number=attempt_number,
            started_at=now,
            outcome=DeliveryAttemptOutcome.PROCESSING,
        )

    def mark_succeeded(self, now: datetime, http_status: int | None) -> None:
        self.outcome = DeliveryAttemptOutcome.SUCCEEDED
        self.completed_at = now
        self.http_status = http_status

    def mark_failed(self, now: datetime, failure: DeliveryFailure) -> None:
        self.outcome = DeliveryAttemptOutcome.FAILED
        self.completed_at = now
        self.http_status = failure.http_status
        self.error_category = failure.category
        self.error_code = failure.code
        self.error_message = failure.message

    def mark_interrupted(self, now: datetime, message: str) -> None:
        self.outcome = DeliveryAttemptOutcome.INTERRUPTED
        self.completed_at = now
        self.error_category = "STALE_PROCESSING"
        self.error_code = "STALE_PROCESSING"
        self.error_message = message
