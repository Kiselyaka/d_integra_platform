import logging
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Message, RouteProcessingResult
from integration_api.message_processing.ports import IntegrationRoute, RouteProvider, RouteTransformationPort, TargetDeliveryPort

logger = logging.getLogger(__name__)


class EventProcessingService:
    """Runs all M2 matched routes; no Delivery aggregate or retry exists yet."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        route_provider: RouteProvider,
        transformer: RouteTransformationPort,
        delivery_port: TargetDeliveryPort,
    ) -> None:
        self._session_factory = session_factory
        self._route_provider = route_provider
        self._transformer = transformer
        self._delivery_port = delivery_port

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
        all_succeeded = True
        for route in routes:
            result_id = self._start_result(message_id, route)
            logger.info("route_matched", extra={"message_id": message_id, "route_key": route.key})
            try:
                target_payload = self._transformer.transform(route, event)
                logger.info("transformation_completed", extra={"message_id": message_id, "route_key": route.key})
            except Exception as exc:
                all_succeeded = False
                self._finish_failure(result_id, "TRANSFORMATION_FAILED", str(exc))
                continue

            try:
                self._delivery_port.deliver(route, target_payload, event.event_id)
            except Exception as exc:
                all_succeeded = False
                self._finish_failure(result_id, "DELIVERY_FAILED", str(exc))
                continue

            self._finish_success(result_id)

        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None:
                return
            message.status = "COMPLETED" if all_succeeded else "ROUTING_FAILED"
            session.commit()

    def _start_result(self, message_id: str, route: IntegrationRoute) -> str:
        with self._session_factory() as session:
            result = RouteProcessingResult(
                id=str(uuid4()),
                message_id=message_id,
                route_key=route.key,
                route_version=route.version,
                status="PROCESSING",
            )
            session.add(result)
            session.commit()
            return result.id

    def _finish_success(self, result_id: str) -> None:
        with self._session_factory() as session:
            result = session.get(RouteProcessingResult, result_id)
            if result is None:
                return
            result.status = "DELIVERY_REQUESTED"
            result.completed_at = datetime.now(timezone.utc)
            session.commit()

    def _finish_failure(self, result_id: str, route_status: str, error: str) -> None:
        with self._session_factory() as session:
            result = session.get(RouteProcessingResult, result_id)
            if result is None:
                return
            result.status = route_status
            result.error = error
            result.completed_at = datetime.now(timezone.utc)
            session.commit()
