"""M9 manual recovery command owned by the Delivery Context."""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.models import Delivery, DeliveryStatus
from shared.messaging.contracts import utc_now


class RequeueDeliveryError(ValueError):
    pass


class DeliveryNotFoundError(RequeueDeliveryError):
    pass


class DeliveryNotDeadLetterError(RequeueDeliveryError):
    pass


@dataclass(frozen=True)
class RequeueResult:
    delivery: Delivery
    created: bool


class RequeueDeliveryService:
    """Create one immutable recovery execution for a DEAD_LETTER Delivery.

    This deliberately does not re-enter ``delivery.requested``: that Kafka
    command denotes initial creation and is deduplicated by RPR identity.  A
    queued recovery Delivery is picked up by the existing Delivery worker.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def requeue(self, delivery_id: str) -> RequeueResult:
        try:
            with self._session_factory.begin() as session:
                source = session.get(Delivery, delivery_id)
                if source is None:
                    raise DeliveryNotFoundError("Delivery was not found")
                existing = session.scalar(
                    select(Delivery).where(Delivery.requeued_from_delivery_id == source.id)
                )
                if existing is not None:
                    session.expunge(existing)
                    return RequeueResult(existing, created=False)
                if source.status != DeliveryStatus.DEAD_LETTER:
                    raise DeliveryNotDeadLetterError("Only DEAD_LETTER Delivery can be requeued")

                recovery = Delivery.create(
                    correlation_id=source.correlation_id,
                    idempotency_key=source.idempotency_key,
                    requeued_from_delivery_id=source.id,
                    message_id=source.message_id,
                    route_processing_result_id=source.route_processing_result_id,
                    route_key=source.route_key,
                    route_version=source.route_version,
                    target_system=source.target_system,
                    payload_format=source.payload_format,
                    transformed_payload=source.transformed_payload,
                    target_url=source.target_url,
                    http_method=source.http_method,
                    connect_timeout_ms=source.connect_timeout_ms,
                    read_timeout_ms=source.read_timeout_ms,
                    max_attempts=source.max_attempts,
                    retry_strategy=source.retry_strategy,
                    retry_initial_delay_seconds=source.retry_initial_delay_seconds,
                    retry_max_delay_seconds=source.retry_max_delay_seconds,
                    retry_multiplier=source.retry_multiplier,
                    retryable_errors=source.retryable_errors,
                )
                recovery.queue(utc_now())
                session.add(recovery)
                session.flush()
                session.expunge(recovery)
                return RequeueResult(recovery, created=True)
        except IntegrityError:
            # The unique direct-parent constraint makes a retried operator
            # command return its already-created recovery instead of creating
            # another outbound execution.
            with self._session_factory() as session:
                recovery = session.scalar(
                    select(Delivery).where(Delivery.requeued_from_delivery_id == delivery_id)
                )
                if recovery is None:
                    raise
                session.expunge(recovery)
                return RequeueResult(recovery, created=False)
