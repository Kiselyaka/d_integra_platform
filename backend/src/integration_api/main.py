import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Request, status
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

    return app


app = create_app()
