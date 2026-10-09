from fastapi import FastAPI, HTTPException, Request, status
from pydantic import BaseModel
from sqlalchemy.orm import Session, sessionmaker

from integration_api.configuration.settings import Settings
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.http_delivery import (
    CrmHttpTargetDeliveryAdapter,
    RouteTargetDeliveryAdapter,
    WarehouseHttpTargetDeliveryAdapter,
)
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.ports import RouteProvider, RouteTransformationPort, TargetDeliveryPort
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
    target_delivery: TargetDeliveryPort | None = None,
) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(title="Integration API", version="0.1.0")
    engine = create_database_engine(settings.database_url)
    app.include_router(build_health_router(engine))
    session_factory = session_factory or create_session_factory(engine)
    ingestion_service = EventIngestionService(session_factory, PydanticSourceValidator())
    processing_service = EventProcessingService(
        session_factory,
        route_provider or StaticRouteProvider(settings.crm_base_url, settings.warehouse_base_url),
        transformer or RouteTransformationProvider(),
        target_delivery
        or RouteTargetDeliveryAdapter(
            CrmHttpTargetDeliveryAdapter(settings.crm_delivery_timeout_seconds),
            WarehouseHttpTargetDeliveryAdapter(settings.warehouse_delivery_timeout_seconds),
        ),
    )

    def ingest_event(event: ExternalEventEnvelope) -> EventAcceptedResponse:
        result = ingestion_service.ingest(event)
        if not result.duplicate:
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
