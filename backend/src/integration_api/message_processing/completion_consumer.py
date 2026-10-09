from __future__ import annotations

import logging
from uuid import uuid4

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from integration_api.message_processing.models import (
    Message,
    ProcessingDeliveryCompletion,
    ProcessingInboxMessage,
    RouteProcessingResult,
)
from shared.messaging.contracts import (
    DeliveryCompletedPayload,
    DeliveryDeadLetteredPayload,
    InternalEventEnvelope,
    utc_now,
)


logger = logging.getLogger(__name__)
MESSAGE_COMPLETION_CONSUMER = "message-completion-handler"


class MessageCompletionConsumer:
    """Projects terminal Delivery events without reading Delivery tables."""

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    async def handle_raw(self, value: bytes) -> None:
        try:
            envelope, payload = self._parse_terminal(value)
        except (ValidationError, ValueError) as error:
            logger.error("invalid_delivery_terminal_event", extra={"error": str(error)})
            return
        self.handle(envelope, payload)

    @staticmethod
    def _parse_terminal(value: bytes) -> tuple[InternalEventEnvelope, DeliveryCompletedPayload]:
        envelope = InternalEventEnvelope.from_json_bytes(value)
        if envelope.event_type == "delivery.completed":
            return envelope, DeliveryCompletedPayload.model_validate(envelope.payload)
        if envelope.event_type == "delivery.dead_lettered":
            dead_letter = DeliveryDeadLetteredPayload.model_validate(envelope.payload)
            # A dead-letter event is a terminal completion too.  Its compact
            # payload carries the reason rather than a second completed_at.
            return envelope, DeliveryCompletedPayload(
                delivery_id=dead_letter.delivery_id,
                message_id=dead_letter.message_id,
                route_processing_result_id=dead_letter.route_processing_result_id,
                target_system=dead_letter.target_system,
                status="DEAD_LETTER",
                attempt_count=dead_letter.attempt_count,
                completed_at=envelope.occurred_at,
            )
        raise ValueError(f"Unexpected event type {envelope.event_type}")

    def handle(self, envelope: InternalEventEnvelope, payload: DeliveryCompletedPayload) -> bool:
        with self._session_factory() as session:
            inbox = session.scalar(
                select(ProcessingInboxMessage).where(
                    ProcessingInboxMessage.consumer_name == MESSAGE_COMPLETION_CONSUMER,
                    ProcessingInboxMessage.event_id == envelope.event_id,
                )
            )
            if inbox is not None:
                logger.info("processing_inbox_duplicate", extra={"event_id": envelope.event_id})
                return False

            message = session.get(Message, payload.message_id)
            if message is None:
                raise ValueError(f"Unknown Message {payload.message_id} for delivery completion")
            route_result = session.get(RouteProcessingResult, payload.route_processing_result_id)
            if route_result is None or route_result.message_id != message.id:
                raise ValueError("Delivery completion does not belong to its Message")

            inbox = ProcessingInboxMessage(
                id=str(uuid4()), event_id=envelope.event_id,
                consumer_name=MESSAGE_COMPLETION_CONSUMER, status="RECEIVED",
            )
            session.add(inbox)
            projection = session.scalar(
                select(ProcessingDeliveryCompletion).where(
                    ProcessingDeliveryCompletion.route_processing_result_id == payload.route_processing_result_id
                )
            )
            if projection is None:
                projection = ProcessingDeliveryCompletion(
                    id=str(uuid4()),
                    message_id=payload.message_id,
                    route_processing_result_id=payload.route_processing_result_id,
                    delivery_id=payload.delivery_id,
                    terminal_status=payload.status,
                    completed_at=payload.completed_at,
                )
                session.add(projection)
            elif projection.delivery_id != payload.delivery_id or projection.terminal_status != payload.status:
                raise ValueError("Conflicting terminal Delivery completion")

            _refresh_message_from_processing_projection(session, message)
            inbox.status = "PROCESSED"
            inbox.processed_at = utc_now()
            try:
                # Inbox, projection and Message transition commit atomically.
                session.commit()
            except IntegrityError:
                session.rollback()
                self._record_duplicate_after_race(envelope.event_id)
                logger.info("processing_inbox_duplicate_after_race", extra={"event_id": envelope.event_id})
                return False
            logger.info("message_completion_processed", extra={"message_id": message.id, "event_id": envelope.event_id})
            return True

    def _record_duplicate_after_race(self, event_id: str) -> None:
        with self._session_factory() as session:
            existing = session.scalar(
                select(ProcessingInboxMessage).where(
                    ProcessingInboxMessage.consumer_name == MESSAGE_COMPLETION_CONSUMER,
                    ProcessingInboxMessage.event_id == event_id,
                )
            )
            if existing is not None:
                return
            session.add(
                ProcessingInboxMessage(
                    id=str(uuid4()), event_id=event_id,
                    consumer_name=MESSAGE_COMPLETION_CONSUMER, status="PROCESSED", processed_at=utc_now(),
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()


def _refresh_message_from_processing_projection(session: Session, message: Message) -> None:
    if message.status in {"NO_ROUTE", "VALIDATION_FAILED"}:
        return
    results = list(session.scalars(select(RouteProcessingResult).where(RouteProcessingResult.message_id == message.id)))
    requested = [result for result in results if result.status == "DELIVERY_REQUESTED"]
    completions = list(
        session.scalars(select(ProcessingDeliveryCompletion).where(ProcessingDeliveryCompletion.message_id == message.id))
    )
    requested_ids = {result.id for result in requested}
    terminal = [completion for completion in completions if completion.route_processing_result_id in requested_ids]
    if requested and len(terminal) < len(requested):
        message.status = "VALIDATED"
    elif results and len(requested) == len(results) and all(item.terminal_status == "DELIVERED" for item in terminal):
        message.status = "COMPLETED"
    else:
        message.status = "ROUTING_FAILED"
