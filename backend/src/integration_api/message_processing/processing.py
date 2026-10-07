import logging
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Message, RouteProcessingResult
from integration_api.message_processing.ports import RouteProvider, TargetDeliveryPort, TransformationPort

logger = logging.getLogger(__name__)


class EventProcessingService:
    """Runs the single M1 route after ingress; no Delivery aggregate or retry exists yet."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        route_provider: RouteProvider,
        transformer: TransformationPort,
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
            route = self._route_provider.find_route(event)
            if route is None:
                message.status = "NO_ROUTE"
                session.commit()
                return
            result = RouteProcessingResult(
                id=str(uuid4()),
                message_id=message.id,
                route_key=route.key,
                route_version=route.version,
                status="PROCESSING",
            )
            session.add(result)
            session.commit()
            result_id = result.id

        logger.info("route_matched", extra={"message_id": message_id, "route_key": route.key})
        try:
            target_payload = self._transformer.transform(event)
            logger.info("transformation_completed", extra={"message_id": message_id, "route_key": route.key})
        except Exception as exc:
            self._finish_failure(message_id, result_id, "TRANSFORMATION_FAILED", str(exc))
            return

        try:
            self._delivery_port.deliver(route, target_payload, event.event_id)
        except Exception as exc:
            self._finish_failure(message_id, result_id, "DELIVERY_REQUESTED", str(exc))
            return

        with self._session_factory() as session:
            message = session.get(Message, message_id)
            result = session.get(RouteProcessingResult, result_id)
            if message is None or result is None:
                return
            # M1 shortcut: DELIVERY_REQUESTED means the one synchronous HTTP
            # request received a 2xx response. A delivery aggregate arrives later.
            result.status = "DELIVERY_REQUESTED"
            result.completed_at = datetime.now(timezone.utc)
            message.status = "COMPLETED"
            session.commit()

    def _finish_failure(self, message_id: str, result_id: str, route_status: str, error: str) -> None:
        with self._session_factory() as session:
            message = session.get(Message, message_id)
            result = session.get(RouteProcessingResult, result_id)
            if message is None or result is None:
                return
            result.status = route_status
            result.error = error
            result.completed_at = datetime.now(timezone.utc)
            message.status = "ROUTING_FAILED"
            session.commit()
