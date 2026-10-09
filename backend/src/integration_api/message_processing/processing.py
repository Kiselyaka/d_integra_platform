from __future__ import annotations

import logging
from datetime import datetime
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.http_delivery import target_http_method_for_route, target_url_for_route
from integration_api.message_processing.models import Message, ProcessingOutboxEvent, RouteProcessingResult
from integration_api.message_processing.ports import IntegrationRoute, RouteProvider, RouteTransformationPort, TargetPayload
from shared.messaging.contracts import InternalEventEnvelope, envelope_json, utc_now


logger = logging.getLogger(__name__)
DELIVERY_REQUESTED_TOPIC = "integration.delivery.requested"


class EventProcessingService:
    """Records delivery requests transactionally; it never creates Delivery rows."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        route_provider: RouteProvider,
        transformer: RouteTransformationPort,
        retry_policy: object,
    ) -> None:
        self._session_factory = session_factory
        self._route_provider = route_provider
        self._transformer = transformer
        self._retry_policy = retry_policy

    def process(self, message_id: str) -> None:
        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None or message.status != "VALIDATED":
                return
            event = ExternalEventEnvelope(
                event_id=message.external_event_id,
                event_type=message.event_type,
                source_system=message.source_system,
                occurred_at=message.occurred_at,
                correlation_id=message.correlation_id,
                schema_version=message.schema_version,
                payload=message.payload,
            )
            routes = self._route_provider.find_matching_routes(event)
            if not routes:
                message.status = "NO_ROUTE"
                session.commit()
                return

        requested_count = 0
        for route in routes:
            logger.info("route_matched", extra={"message_id": message_id, "route_key": route.key})
            try:
                target_payload = self._transformer.transform(route, event)
            except Exception as exc:
                self._record_failure(message_id, route, "TRANSFORMATION_FAILED", str(exc))
                continue
            if self._record_requested(message_id, event, route, target_payload):
                requested_count += 1

        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is not None:
                # A requested Delivery is asynchronous. Its terminal event is
                # the only thing that can complete this Message after M5.
                message.status = "VALIDATED" if requested_count else "ROUTING_FAILED"
                session.commit()

    def _record_requested(
        self,
        message_id: str,
        event: ExternalEventEnvelope,
        route: IntegrationRoute,
        target_payload: TargetPayload,
    ) -> bool:
        """Persist RPR + Outbox in exactly one local SQL transaction."""

        with self._session_factory() as session:
            existing = session.scalar(
                select(RouteProcessingResult).where(
                    RouteProcessingResult.message_id == message_id,
                    RouteProcessingResult.route_key == route.key,
                    RouteProcessingResult.route_version == route.version,
                )
            )
            if existing is not None:
                return existing.status == "DELIVERY_REQUESTED"

            result_id = str(uuid4())
            delivery_id = str(uuid4())
            now = utc_now()
            result = RouteProcessingResult(
                id=result_id,
                message_id=message_id,
                route_key=route.key,
                route_version=route.version,
                status="DELIVERY_REQUESTED",
                completed_at=now,
            )
            envelope = self._requested_envelope(
                event=event,
                delivery_id=delivery_id,
                message_id=message_id,
                result_id=result_id,
                route=route,
                target_payload=target_payload,
                now=now,
            )
            session.add(result)
            session.add(
                ProcessingOutboxEvent(
                    id=str(uuid4()),
                    event_id=envelope.event_id,
                    event_type=envelope.event_type,
                    aggregate_id=envelope.aggregate_id,
                    topic=DELIVERY_REQUESTED_TOPIC,
                    message_key=delivery_id,
                    envelope=envelope_json(envelope),
                    publish_attempt_count=0,
                )
            )
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing = session.scalar(
                    select(RouteProcessingResult).where(
                        RouteProcessingResult.message_id == message_id,
                        RouteProcessingResult.route_key == route.key,
                        RouteProcessingResult.route_version == route.version,
                    )
                )
                return existing is not None and existing.status == "DELIVERY_REQUESTED"
            logger.info("delivery_requested_recorded", extra={"message_id": message_id, "delivery_id": delivery_id})
            return True

    def _record_failure(self, message_id: str, route: IntegrationRoute, status: str, error: str) -> None:
        with self._session_factory() as session:
            existing = session.scalar(
                select(RouteProcessingResult).where(
                    RouteProcessingResult.message_id == message_id,
                    RouteProcessingResult.route_key == route.key,
                    RouteProcessingResult.route_version == route.version,
                )
            )
            if existing is not None:
                return
            session.add(
                RouteProcessingResult(
                    id=str(uuid4()),
                    message_id=message_id,
                    route_key=route.key,
                    route_version=route.version,
                    status=status,
                    error=error,
                    completed_at=utc_now(),
                )
            )
            session.commit()

    def _requested_envelope(
        self,
        *,
        event: ExternalEventEnvelope,
        delivery_id: str,
        message_id: str,
        result_id: str,
        route: IntegrationRoute,
        target_payload: TargetPayload,
        now: datetime,
    ) -> InternalEventEnvelope:
        policy = self._retry_policy
        content_format = "JSON" if target_payload.content_type.startswith("application/json") else "XML"
        retryable = getattr(policy, "retryable_errors")
        return InternalEventEnvelope(
            event_id=str(uuid4()),
            event_type="delivery.requested",
            aggregate_id=result_id,
            occurred_at=now,
            correlation_id=event.correlation_id,
            payload={
                "delivery_id": delivery_id,
                "message_id": message_id,
                "route_processing_result_id": result_id,
                "route": {"id": route.key, "version": route.version},
                "target": {
                    "system_code": route.target_system,
                    "url": target_url_for_route(route),
                    "http_method": target_http_method_for_route(route),
                },
                "content": {
                    "format": content_format,
                    "content_type": target_payload.content_type,
                    "body": target_payload.body,
                },
                "retry": {
                    "max_attempts": getattr(policy, "max_attempts"),
                    "strategy": getattr(policy, "strategy"),
                    "initial_delay_seconds": getattr(policy, "initial_delay_seconds"),
                    "max_delay_seconds": getattr(policy, "max_delay_seconds"),
                    "multiplier": getattr(policy, "multiplier"),
                    "retryable_errors": retryable.split(",") if isinstance(retryable, str) else retryable,
                },
            },
        )
