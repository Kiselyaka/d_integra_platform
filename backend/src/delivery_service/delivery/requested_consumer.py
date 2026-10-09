from __future__ import annotations

import json
import logging
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.models import Delivery, DeliveryInboxMessage
from shared.messaging.contracts import DeliveryRequestedPayload, InternalEventEnvelope, utc_now


logger = logging.getLogger(__name__)
DELIVERY_WORKERS_CONSUMER = "delivery-workers"


class DeliveryRequestedConsumer:
    """Creates one queued Delivery from one Kafka delivery.requested event."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    async def handle_raw(self, value: bytes) -> None:
        try:
            envelope = InternalEventEnvelope.from_json_bytes(value)
            if envelope.event_type != "delivery.requested":
                raise ValueError(f"Unexpected event type {envelope.event_type}")
            payload = DeliveryRequestedPayload.model_validate(envelope.payload)
        except (ValidationError, ValueError) as error:
            # There is no safe domain mutation for a malformed internal event.
            # Returning lets the worker commit its offset and prevents a poison
            # record from blocking the whole partition forever.
            logger.error("invalid_delivery_requested_event", extra={"error": str(error)})
            return
        self.handle(envelope, payload)

    def handle(self, envelope: InternalEventEnvelope, payload: DeliveryRequestedPayload) -> bool:
        """Returns true only when this call created the logical Delivery."""

        with self._session_factory() as session:
            inbox = session.scalar(
                select(DeliveryInboxMessage).where(
                    DeliveryInboxMessage.consumer_name == DELIVERY_WORKERS_CONSUMER,
                    DeliveryInboxMessage.event_id == envelope.event_id,
                )
            )
            if inbox is not None:
                logger.info("delivery_inbox_duplicate", extra={"event_id": envelope.event_id})
                return False

            # These checks additionally protect the domain from malformed
            # duplicate logical requests with a different Kafka event_id.
            existing = session.get(Delivery, payload.delivery_id) or session.scalar(
                select(Delivery).where(Delivery.route_processing_result_id == payload.route_processing_result_id)
            )
            inbox = DeliveryInboxMessage(
                id=str(uuid4()), event_id=envelope.event_id,
                consumer_name=DELIVERY_WORKERS_CONSUMER, status="RECEIVED",
            )
            session.add(inbox)
            if existing is not None:
                inbox.status = "PROCESSED"
                inbox.processed_at = utc_now()
                session.commit()
                logger.warning("delivery_requested_logical_duplicate", extra={"event_id": envelope.event_id})
                return False

            delivery = _delivery_from_event(envelope, payload)
            delivery.queue(utc_now())
            session.add(delivery)
            inbox.status = "PROCESSED"
            inbox.processed_at = utc_now()
            try:
                # Inbox insert and queued Delivery creation are one transaction.
                session.commit()
            except IntegrityError:
                session.rollback()
                self._record_logical_duplicate_after_race(envelope.event_id)
                logger.info("delivery_inbox_duplicate_after_race", extra={"event_id": envelope.event_id})
                return False
            logger.info("delivery_created_from_event", extra={"delivery_id": delivery.id, "event_id": envelope.event_id})
            return True

    def _record_logical_duplicate_after_race(self, event_id: str) -> None:
        """Persist the Inbox evidence when a Delivery uniqueness race was won elsewhere."""

        with self._session_factory() as session:
            existing = session.scalar(
                select(DeliveryInboxMessage).where(
                    DeliveryInboxMessage.consumer_name == DELIVERY_WORKERS_CONSUMER,
                    DeliveryInboxMessage.event_id == event_id,
                )
            )
            if existing is not None:
                return
            session.add(
                DeliveryInboxMessage(
                    id=str(uuid4()), event_id=event_id,
                    consumer_name=DELIVERY_WORKERS_CONSUMER, status="PROCESSED", processed_at=utc_now(),
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()


def _delivery_from_event(envelope: InternalEventEnvelope, payload: DeliveryRequestedPayload) -> Delivery:
    route = payload.route
    target = payload.target
    content = payload.content
    retry = payload.retry
    body = content.body
    format_name = content.format
    if format_name == "JSON":
        transformed_payload = json.dumps(body, separators=(",", ":"), sort_keys=True)
        content_type = content.content_type
    elif format_name == "XML" and isinstance(body, str):
        transformed_payload = body
        content_type = content.content_type
    else:
        raise ValueError("delivery.requested content must be JSON or XML")
    retryable = retry.retryable_errors
    return Delivery.create(
        delivery_id=payload.delivery_id,
        correlation_id=envelope.correlation_id,
        message_id=payload.message_id,
        route_processing_result_id=payload.route_processing_result_id,
        route_key=route.id,
        route_version=route.version,
        target_system=target.system_code,
        payload_format=content_type,
        transformed_payload=transformed_payload,
        target_url=target.url,
        http_method=target.http_method,
        connect_timeout_ms=target.connect_timeout_ms,
        read_timeout_ms=target.read_timeout_ms,
        max_attempts=retry.max_attempts,
        retry_strategy=retry.strategy,
        retry_initial_delay_seconds=retry.initial_delay_seconds,
        retry_max_delay_seconds=retry.max_delay_seconds,
        retry_multiplier=retry.multiplier,
        retryable_errors=",".join(str(value) for value in retryable),
    )
