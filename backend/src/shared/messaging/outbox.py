from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session, sessionmaker

from shared.messaging.kafka import KafkaProducerPort
from shared.messaging.contracts import utc_now


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ClaimedOutboxEvent:
    id: str
    token: str
    topic: str
    message_key: str
    envelope: dict[str, Any]


class OutboxPublisher:
    """Lease one persisted event, publish it, then record broker acknowledgement.

    A crash after send and before ``published_at`` intentionally leaves the row
    available for another send. Consumers must deduplicate by ``event_id``.
    """

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        outbox_model: type[Any],
        producer: KafkaProducerPort,
        lease_seconds: int = 30,
    ) -> None:
        self._session_factory = session_factory
        self._outbox_model = outbox_model
        self._producer = producer
        self._lease_seconds = lease_seconds

    async def publish_available(self, limit: int = 100) -> int:
        published = 0
        for _ in range(limit):
            claimed = self._claim_one()
            if claimed is None:
                break
            try:
                from shared.messaging.contracts import InternalEventEnvelope

                envelope = InternalEventEnvelope.model_validate(claimed.envelope)
                await self._producer.send(claimed.topic, claimed.message_key, envelope.as_json_bytes())
            except Exception as error:
                self._record_failure(claimed, str(error))
                logger.warning("outbox_publish_failed", extra={"outbox_id": claimed.id, "error": str(error)})
            else:
                self._mark_published(claimed)
                published += 1
                logger.info("outbox_published", extra={"outbox_id": claimed.id, "topic": claimed.topic})
        return published

    def _claim_one(self) -> ClaimedOutboxEvent | None:
        now = utc_now()
        token = str(uuid4())
        with self._session_factory() as session:
            row = session.scalar(
                select(self._outbox_model)
                .where(
                    self._outbox_model.published_at.is_(None),
                    or_(
                        self._outbox_model.publish_lease_until.is_(None),
                        self._outbox_model.publish_lease_until < now,
                    ),
                )
                .order_by(self._outbox_model.created_at, self._outbox_model.id)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            if row is None:
                return None
            row.publish_lease_token = token
            row.publish_lease_until = now + timedelta(seconds=self._lease_seconds)
            row.publish_attempt_count += 1
            session.commit()
            return ClaimedOutboxEvent(row.id, token, row.topic, row.message_key, row.envelope)

    def _mark_published(self, claimed: ClaimedOutboxEvent) -> None:
        with self._session_factory() as session:
            row = session.get(self._outbox_model, claimed.id)
            if row is None or row.publish_lease_token != claimed.token:
                return
            row.published_at = utc_now()
            row.publish_lease_token = None
            row.publish_lease_until = None
            row.last_publish_error = None
            session.commit()

    def _record_failure(self, claimed: ClaimedOutboxEvent, error: str) -> None:
        with self._session_factory() as session:
            row = session.get(self._outbox_model, claimed.id)
            if row is None or row.publish_lease_token != claimed.token:
                return
            row.last_publish_error = error[:4000]
            # Keep the short lease after a failure. Otherwise one poll cycle
            # would re-claim the same unreachable broker event up to ``limit``
            # times and busy-loop. It becomes eligible after lease expiry.
            session.commit()
