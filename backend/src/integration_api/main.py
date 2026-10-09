import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from integration_api.configuration.settings import Settings
from integration_api.configuration.routes import PostgresRouteConfigurationProvider
from integration_api.message_processing.completion_consumer import MessageCompletionConsumer
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import ProcessingOutboxEvent
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.ports import RouteProvider, RouteTransformationPort
from integration_api.message_processing.service import EventIngestionService
from integration_api.message_processing.transformation import RouteTransformationProvider
from integration_api.message_processing.validation import PydanticSourceValidator
from integration_api.message_processing.warehouse_xml import WarehouseXmlParseError, parse_warehouse_event
from shared.database import create_database_engine, create_session_factory
from shared.health import build_health_router
from shared.messaging.kafka import AioKafkaProducerAdapter, KafkaConsumerWorker
from shared.messaging.outbox import OutboxPublisher


logger = logging.getLogger(__name__)


class EventAcceptedResponse(BaseModel):
    message_id: str
    status: str


def create_app(
    settings: Settings | None = None,
    session_factory: sessionmaker[Session] | None = None,
    route_provider: RouteProvider | None = None,
    transformer: RouteTransformationPort | None = None,
) -> FastAPI:
    settings = settings or Settings()
    engine = create_database_engine(settings.database_url)
    session_factory = session_factory or create_session_factory(engine)
    ingestion_service = EventIngestionService(session_factory, PydanticSourceValidator())
    processing_service = EventProcessingService(
        session_factory,
        route_provider or PostgresRouteConfigurationProvider(session_factory),
        transformer or RouteTransformationProvider(),
    )
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

    def ingest_event(event: ExternalEventEnvelope) -> EventAcceptedResponse:
        result = ingestion_service.ingest(event)
        if not result.duplicate:
            # Kafka availability is deliberately not part of the HTTP request.
            # The committed Processing Outbox makes this safe while unavailable.
            processing_service.process(result.message_id)
        return EventAcceptedResponse(message_id=result.message_id, status="already_accepted" if result.duplicate else "accepted")

    @app.post("/api/v1/events", response_model=EventAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["events"])
    def post_event(event: ExternalEventEnvelope) -> EventAcceptedResponse:
        return ingest_event(event)

    @app.post("/api/v1/events/warehouse", response_model=EventAcceptedResponse, status_code=status.HTTP_202_ACCEPTED, tags=["events"])
    async def post_warehouse_event(request: Request) -> EventAcceptedResponse:
        try:
            event = parse_warehouse_event(await request.body())
        except WarehouseXmlParseError as error:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(error)) from error
        return ingest_event(event)

    return app


app = create_app()
