import logging
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Message
from integration_api.message_processing.ports import SourceValidationPort

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EventIngestionResult:
    message_id: str
    duplicate: bool


class EventIngestionService:
    def __init__(self, session_factory: sessionmaker[Session], source_validator: SourceValidationPort) -> None:
        self._session_factory = session_factory
        self._source_validator = source_validator

    def ingest(self, event: ExternalEventEnvelope) -> EventIngestionResult:
        with self._session_factory() as session:
            existing = self._find_existing(session, event)
            if existing is not None:
                logger.info("duplicate_event_detected", extra={"event_id": event.event_id, "message_id": existing.id})
                return EventIngestionResult(message_id=existing.id, duplicate=True)

            validation_error = self._source_validator.validate(event)
            message = Message(
                id=str(uuid4()),
                external_event_id=event.event_id,
                source_system=event.source_system,
                event_type=event.event_type,
                payload=event.payload,
                status="VALIDATION_FAILED" if validation_error else "VALIDATED",
                correlation_id=event.correlation_id,
                schema_version=event.schema_version,
                occurred_at=event.occurred_at,
                validation_error=validation_error,
            )
            session.add(message)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = self._find_existing(session, event)
                if existing is None:
                    raise
                logger.info("duplicate_event_detected", extra={"event_id": event.event_id, "message_id": existing.id})
                return EventIngestionResult(message_id=existing.id, duplicate=True)

            logger.info("message_received", extra={"event_id": event.event_id, "message_id": message.id})
            return EventIngestionResult(message_id=message.id, duplicate=False)

    @staticmethod
    def _find_existing(session: Session, event: ExternalEventEnvelope) -> Message | None:
        return session.scalar(
            select(Message).where(
                Message.source_system == event.source_system,
                Message.external_event_id == event.event_id,
            )
        )
