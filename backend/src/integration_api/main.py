from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from delivery_service.delivery.application import DeliveryProcessor, DeliveryRequestService, RetryPolicySnapshot
from integration_api.configuration.settings import Settings
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.delivery_completion import MessageDeliveryCompletionService
from integration_api.message_processing.http_delivery import (
    HttpDeliveryExecutor,
)
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.ports import RouteProvider, RouteTransformationPort
from integration_api.message_processing.routing import StaticRouteProvider
from integration_api.message_processing.service import EventIngestionService
from integration_api.message_processing.transformation import RouteTransformationProvider
from integration_api.message_processing.validation import PydanticSourceValidator
from integration_api.message_processing.warehouse_xml import WarehouseXmlParseError, parse_warehouse_event
from shared.database import create_database_engine, create_session_factory
from shared.health import build_health_router


class EventAcceptedResponse(BaseModel):
    message_id: str
    status: str


def create_app(
    settings: Settings | None = None,
    session_factory: sessionmaker[Session] | None = None,
    route_provider: RouteProvider | None = None,
    transformer: RouteTransformationPort | None = None,
    delivery_processor: DeliveryProcessor | None = None,
) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Integration API", version="0.1.0")
    engine = create_database_engine(settings.database_url)
    app.include_router(build_health_router(engine))
    session_factory = session_factory or create_session_factory(engine)
    ingestion_service = EventIngestionService(session_factory, PydanticSourceValidator())
    retry_policy = RetryPolicySnapshot(
        max_attempts=settings.delivery_max_attempts,
        strategy=settings.delivery_retry_strategy,
        initial_delay_seconds=settings.delivery_retry_initial_delay_seconds,
        max_delay_seconds=settings.delivery_retry_max_delay_seconds,
        multiplier=settings.delivery_retry_multiplier,
        retryable_errors="TIMEOUT,CONNECTION_ERROR,REMOTE_SERVER_ERROR,RATE_LIMITED",
    )
    completion_service = MessageDeliveryCompletionService(session_factory)
    delivery_processor = delivery_processor or DeliveryProcessor(
        session_factory,
        HttpDeliveryExecutor(max(settings.crm_delivery_timeout_seconds, settings.warehouse_delivery_timeout_seconds, settings.mini_shop_delivery_timeout_seconds)),
        completion_service,
        settings.delivery_stale_processing_timeout_seconds,
    )
    processing_service = EventProcessingService(
        session_factory,
        route_provider or StaticRouteProvider(settings.crm_base_url, settings.warehouse_base_url, settings.mini_shop_base_url),
        transformer or RouteTransformationProvider(),
        DeliveryRequestService(session_factory, retry_policy),
    )

    def ingest_event(event: ExternalEventEnvelope) -> EventAcceptedResponse:
        result = ingestion_service.ingest(event)
        if not result.duplicate:
            processing_service.process(result.message_id)
            delivery_processor.process_available()
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
