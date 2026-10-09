import logging
import json
from datetime import datetime, timezone
from uuid import uuid4

from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.application import DeliveryRequest, DeliveryRequestService
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.http_delivery import target_http_method_for_route, target_url_for_route
from integration_api.message_processing.models import Message, RouteProcessingResult
from integration_api.message_processing.ports import IntegrationRoute, RouteProvider, RouteTransformationPort

logger = logging.getLogger(__name__)


class EventProcessingService:
    """Creates one immutable Delivery request for every transformed route."""

    def __init__(
        self,
        session_factory: sessionmaker[Session],
        route_provider: RouteProvider,
        transformer: RouteTransformationPort,
        delivery_requests: DeliveryRequestService,
    ) -> None:
        self._session_factory = session_factory
        self._route_provider = route_provider
        self._transformer = transformer
        self._delivery_requests = delivery_requests

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
        all_requested = True
        created_delivery = False
        for route in routes:
            result_id = self._start_result(message_id, route)
            logger.info("route_matched", extra={"message_id": message_id, "route_key": route.key})
            try:
                target_payload = self._transformer.transform(route, event)
                logger.info("transformation_completed", extra={"message_id": message_id, "route_key": route.key})
            except Exception as exc:
                all_requested = False
                self._finish_failure(result_id, "TRANSFORMATION_FAILED", str(exc))
                continue

            try:
                self._delivery_requests.request(
                    DeliveryRequest(
                        message_id=message_id,
                        route_processing_result_id=result_id,
                        route_key=route.key,
                        route_version=route.version,
                        target_system=route.target_system,
                        payload_format=target_payload.content_type,
                        transformed_payload=_serialize_payload(target_payload.body),
                        target_url=target_url_for_route(route),
                        http_method=target_http_method_for_route(route),
                    )
                )
            except Exception as exc:
                all_requested = False
                self._finish_failure(result_id, "DELIVERY_REQUEST_FAILED", str(exc))
                continue

            self._finish_success(result_id)
            created_delivery = True

        with self._session_factory() as session:
            message = session.get(Message, message_id)
            if message is None:
                return
            # Delivery is now asynchronous: VALIDATED is the existing
            # non-terminal state until all requested Deliveries finish.
            message.status = "VALIDATED" if created_delivery else "ROUTING_FAILED"
            if not all_requested and not created_delivery:
                message.status = "ROUTING_FAILED"
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


def _serialize_payload(payload: dict[str, object] | str) -> str:
    if isinstance(payload, str):
        return payload
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)
