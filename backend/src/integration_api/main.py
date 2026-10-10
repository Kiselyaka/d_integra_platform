import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from integration_api.configuration.lifecycle import ConfigurationConflictError, ConfigurationLifecycleError, ConfigurationLifecycleService
from integration_api.configuration.preview import ConfigurationPreviewService, PreviewError
from integration_api.configuration.settings import Settings
from integration_api.configuration.routes import PostgresRouteConfigurationProvider
from integration_api.message_processing.completion_consumer import MessageCompletionConsumer
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import ProcessingOutboxEvent
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.ports import RouteProvider, RouteTransformationPort
from integration_api.message_processing.service import EventIngestionService
from integration_api.message_processing.transformation import RouteTransformationProvider
from integration_api.message_processing.preprocessing import SourceMaterial
from integration_api.message_processing.validation import TechnicalEnvelopeValidator
from integration_api.message_processing.warehouse_xml import WarehouseXmlParseError, parse_warehouse_event
from integration_api.operational_query.pagination import InvalidCursorError, decode_cursor
from integration_api.operational_query.service import OperationalNotFoundError, OperationalQueryService
from delivery_service.delivery.requeue import DeliveryNotDeadLetterError, DeliveryNotFoundError, RequeueDeliveryService
from shared.database import create_database_engine, create_session_factory
from shared.health import build_health_router
from shared.messaging.kafka import AioKafkaProducerAdapter, KafkaConsumerWorker
from shared.messaging.outbox import OutboxPublisher


logger = logging.getLogger(__name__)


class EventAcceptedResponse(BaseModel):
    message_id: str
    status: str


class PreviewRequest(BaseModel):
    sample_payload: dict[str, Any]
    event_id: str = "preview-event"
    source_system: str = "preview"
    event_type: str = "preview"
    schema_version: int = 1
    raw_xml: str | None = None


class ConfigurationUpdateRequest(BaseModel):
    changes: dict[str, Any]


def create_app(
    settings: Settings | None = None,
    session_factory: sessionmaker[Session] | None = None,
    route_provider: RouteProvider | None = None,
    transformer: RouteTransformationPort | None = None,
) -> FastAPI:
    settings = settings or Settings()
    engine = create_database_engine(settings.database_url)
    session_factory = session_factory or create_session_factory(engine)
    ingestion_service = EventIngestionService(session_factory, TechnicalEnvelopeValidator())
    processing_service = EventProcessingService(
        session_factory,
        route_provider or PostgresRouteConfigurationProvider(session_factory),
        transformer or RouteTransformationProvider(),
    )
    preview_service = ConfigurationPreviewService(session_factory, transformer if isinstance(transformer, RouteTransformationProvider) else None)
    lifecycle_service = ConfigurationLifecycleService(session_factory)
    operational_query_service = OperationalQueryService(session_factory)
    requeue_delivery_service = RequeueDeliveryService(session_factory)
    producer = AioKafkaProducerAdapter(settings.kafka_bootstrap_servers)
    outbox_publisher = OutboxPublisher(session_factory, ProcessingOutboxEvent, producer)
    completion_consumer = MessageCompletionConsumer(session_factory)
    @asynccontextmanager
    async def lifespan(_: FastAPI):
        stopped = asyncio.Event()
        async def publish_loop() -> None:
            while not stopped.is_set():
                try:
                    await outbox_publisher.publish_available()
                except Exception:
                    logger.exception("processing_outbox_poll_failed")
                await asyncio.sleep(settings.kafka_outbox_poll_interval_seconds)

        completed_consumer = KafkaConsumerWorker(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic="integration.delivery.completed",
            group_id="message-completion-handler",
            handle=completion_consumer.handle_raw,
        )
        dead_letter_consumer = KafkaConsumerWorker(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic="integration.delivery.dead_lettered",
            group_id="message-completion-handler",
            handle=completion_consumer.handle_raw,
        )
        tasks = [
            asyncio.create_task(publish_loop()),
            asyncio.create_task(completed_consumer.run(stopped)),
            asyncio.create_task(dead_letter_consumer.run(stopped)),
        ]
        try:
            yield
        finally:
            stopped.set()
            for task in tasks:
                task.cancel()
            for task in tasks:
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            await producer.stop()

    app = FastAPI(title="Integration API", version="0.1.0", lifespan=lifespan)
    app.include_router(build_health_router(engine))

    def ingest_event(event: ExternalEventEnvelope, source_material: SourceMaterial | None = None) -> EventAcceptedResponse:
        result = ingestion_service.ingest(event)
        if not result.duplicate:
            # Kafka availability is deliberately not part of the HTTP request.
            # The committed Processing Outbox makes this safe while unavailable.
            processing_service.process(result.message_id, source_material)
        return EventAcceptedResponse(message_id=result.message_id, status="already_accepted" if result.duplicate else "accepted")

    @app.post("/api/v1/events", response_model=EventAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["events"])
    def post_event(event: ExternalEventEnvelope) -> EventAcceptedResponse:
        return ingest_event(event)

    @app.post("/api/v1/events/warehouse", response_model=EventAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["events"])
    async def post_warehouse_event(request: Request) -> EventAcceptedResponse:
        try:
            raw_xml = await request.body()
            event = parse_warehouse_event(raw_xml)
        except WarehouseXmlParseError as error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
        return ingest_event(event, SourceMaterial(raw_format="XML", raw_body=raw_xml))

    def preview_response(result: Any) -> dict[str, Any]:
        return {
            "success": result.success,
            "status": result.status,
            "stages": list(result.stages),
            "canonical_target": result.canonical_target,
            "serialized_payload": result.payload.body if result.payload else None,
            "content_type": result.payload.content_type if result.payload else None,
            "error": result.diagnostic.as_dict() if result.diagnostic else None,
        }

    @app.post("/api/v1/routes/{route_id}/preview", tags=["configuration"])
    def preview_route(route_id: str, request: PreviewRequest) -> dict[str, Any]:
        try:
            return preview_response(preview_service.preview_route(
                route_id, request.sample_payload, event_id=request.event_id, source_system=request.source_system,
                event_type=request.event_type, schema_version=request.schema_version, raw_xml=request.raw_xml,
            ))
        except PreviewError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.post("/api/v1/transformations/{transformation_id}/preview", tags=["configuration"])
    def preview_transformation(transformation_id: str, request: PreviewRequest) -> dict[str, Any]:
        try:
            return preview_response(preview_service.preview_transformation(
                transformation_id, request.sample_payload, event_id=request.event_id, source_system=request.source_system,
                event_type=request.event_type, schema_version=request.schema_version,
            ))
        except PreviewError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc

    @app.post("/api/v1/configuration/{kind}/{entity_id}/new-version", tags=["configuration"])
    def create_configuration_version(kind: Literal["schema", "transformation", "route"], entity_id: str) -> dict[str, Any]:
        try:
            entity = lifecycle_service.create_new_version(kind, entity_id)
            return {"id": entity.id, "version": entity.version, "status": entity.status}
        except ConfigurationLifecycleError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @app.patch("/api/v1/configuration/{kind}/{entity_id}/draft", tags=["configuration"])
    def update_configuration_draft(kind: Literal["schema", "transformation", "route"], entity_id: str, request: ConfigurationUpdateRequest) -> dict[str, Any]:
        try:
            entity = lifecycle_service.update_draft(kind, entity_id, request.changes)
            return {"id": entity.id, "version": entity.version, "status": entity.status}
        except ConfigurationLifecycleError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    @app.post("/api/v1/configuration/{kind}/{entity_id}/activate", tags=["configuration"])
    def activate_configuration(kind: Literal["schema", "transformation", "route"], entity_id: str) -> dict[str, Any]:
        try:
            entity = lifecycle_service.activate(kind, entity_id)
            return {"id": entity.id, "version": entity.version, "status": entity.status}
        except ConfigurationConflictError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
        except ConfigurationLifecycleError as exc:
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc)) from exc

    @app.post("/api/v1/configuration/{kind}/{entity_id}/retire", tags=["configuration"])
    def retire_configuration(kind: Literal["schema", "transformation", "route"], entity_id: str) -> dict[str, Any]:
        try:
            entity = lifecycle_service.retire(kind, entity_id)
            return {"id": entity.id, "version": entity.version, "status": entity.status}
        except ConfigurationLifecycleError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    def operational_error(exc: Exception) -> HTTPException:
        if isinstance(exc, OperationalNotFoundError):
            return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
        if isinstance(exc, InvalidCursorError):
            return HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=str(exc))
        raise exc

    @app.get("/api/v1/admin/messages", tags=["operational"])
    def list_messages(
        limit: int = Query(default=50, ge=1, le=200), cursor: str | None = None,
        source_system: str | None = None, event_type: str | None = None, status_filter: str | None = Query(default=None, alias="status"),
        outcome: str | None = None, correlation_id: str | None = None, external_event_id: str | None = None, message_id: str | None = None,
        received_from: datetime | None = None, received_to: datetime | None = None,
    ) -> dict[str, Any]:
        try:
            return operational_query_service.list_messages(
                limit=limit, cursor=decode_cursor(cursor), source_system=source_system, event_type=event_type,
                status=status_filter, outcome=outcome, correlation_id=correlation_id, external_event_id=external_event_id,
                message_id=message_id, received_from=received_from, received_to=received_to,
            )
        except (OperationalNotFoundError, InvalidCursorError) as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/messages/{message_id}/timeline", tags=["operational"])
    def message_timeline(message_id: str) -> dict[str, Any]:
        try:
            return operational_query_service.message_timeline(message_id)
        except OperationalNotFoundError as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/messages/{message_id}", tags=["operational"])
    def message_details(message_id: str) -> dict[str, Any]:
        try:
            return operational_query_service.message_details(message_id)
        except OperationalNotFoundError as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/correlations/{correlation_id}", tags=["operational"])
    def correlation_trace(correlation_id: str) -> dict[str, Any]:
        try:
            return operational_query_service.correlation_trace(correlation_id)
        except OperationalNotFoundError as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/deliveries", tags=["operational"])
    def list_deliveries(
        limit: int = Query(default=50, ge=1, le=200), cursor: str | None = None,
        target_system: str | None = None, route: str | None = None, status_filter: str | None = Query(default=None, alias="status"),
        error_category: str | None = None, correlation_id: str | None = None, message_id: str | None = None,
        created_from: datetime | None = None, created_to: datetime | None = None,
    ) -> dict[str, Any]:
        try:
            return operational_query_service.list_deliveries(
                limit=limit, cursor=decode_cursor(cursor), target_system=target_system, route=route, status=status_filter,
                error_category=error_category, correlation_id=correlation_id, message_id=message_id,
                created_from=created_from, created_to=created_to,
            )
        except InvalidCursorError as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/deliveries/{delivery_id}", tags=["operational"])
    def delivery_details(delivery_id: str) -> dict[str, Any]:
        try:
            return operational_query_service.delivery_details(delivery_id)
        except OperationalNotFoundError as exc:
            raise operational_error(exc) from exc

    @app.get("/api/v1/admin/dead-letters", tags=["operational"])
    def dead_letters(
        limit: int = Query(default=50, ge=1, le=200), cursor: str | None = None,
        target_system: str | None = None, route: str | None = None, error_category: str | None = None,
        correlation_id: str | None = None, created_from: datetime | None = None, created_to: datetime | None = None,
    ) -> dict[str, Any]:
        try:
            return operational_query_service.list_deliveries(
                limit=limit, cursor=decode_cursor(cursor), target_system=target_system, route=route,
                error_category=error_category, correlation_id=correlation_id, created_from=created_from,
                created_to=created_to, dead_letters_only=True,
            )
        except InvalidCursorError as exc:
            raise operational_error(exc) from exc

    @app.post("/api/v1/admin/dead-letters/{delivery_id}/requeue", tags=["operational"])
    def requeue_dead_letter(delivery_id: str) -> dict[str, Any]:
        try:
            result = requeue_delivery_service.requeue(delivery_id)
            return {"delivery_id": result.delivery.id, "status": result.delivery.status, "created": result.created,
                    "requeued_from_delivery_id": result.delivery.requeued_from_delivery_id}
        except DeliveryNotFoundError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
        except DeliveryNotDeadLetterError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    return app


app = create_app()
