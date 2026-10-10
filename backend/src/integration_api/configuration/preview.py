"""Side-effect-free M8 configuration previews."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy.orm import Session, sessionmaker

from integration_api.configuration.models import TransformationDefinition
from integration_api.configuration.routes import PostgresRouteConfigurationProvider
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.preprocessing import PreprocessingPipeline, PreprocessingResult, SourceMaterial, StructuredDiagnostic
from integration_api.message_processing.ports import IntegrationRoute
from integration_api.message_processing.serialization import serialize_target_document
from integration_api.message_processing.transformation import RouteTransformationProvider


class PreviewError(ValueError):
    pass


class ConfigurationPreviewService:
    def __init__(self, session_factory: sessionmaker[Session], transformer: RouteTransformationProvider | None = None) -> None:
        self._session_factory = session_factory
        self._transformer = transformer or RouteTransformationProvider()
        self._pipeline = PreprocessingPipeline(self._transformer)
        self._routes = PostgresRouteConfigurationProvider(session_factory)

    def preview_route(
        self, route_id: str, sample_payload: dict[str, Any], *, event_id: str = "preview-event", source_system: str = "preview",
        event_type: str = "preview", schema_version: int = 1, raw_xml: str | None = None,
    ) -> PreprocessingResult:
        route = self._routes.get_route_version(route_id)
        if route is None:
            raise PreviewError("Route was not found")
        event = ExternalEventEnvelope(
            event_id=event_id, source_system=source_system, event_type=event_type,
            occurred_at=datetime.now(UTC), schema_version=schema_version, payload=sample_payload,
        )
        source = SourceMaterial("XML", raw_xml.encode("utf-8")) if raw_xml is not None else None
        return self._pipeline.execute(route, event, source)

    def preview_transformation(
        self, transformation_id: str, sample_payload: dict[str, Any], *, event_id: str = "preview-event",
        source_system: str = "preview", event_type: str = "preview", schema_version: int = 1,
    ) -> PreprocessingResult:
        with self._session_factory() as session:
            definition = session.get(TransformationDefinition, transformation_id)
            if definition is None:
                raise PreviewError("Transformation was not found")
            route = IntegrationRoute(
                id="preview", key="preview", version=definition.version, target_base_url="http://preview.invalid",
                source_format=definition.source_format, target_format=definition.target_format,
                transformation_id=definition.id, transformation_key=definition.transformation_key,
                transformation_version=definition.version, transformation_engine_type=definition.engine_type,
                transformation_definition=definition.definition,
            )
        event = ExternalEventEnvelope(
            event_id=event_id, source_system=source_system, event_type=event_type,
            occurred_at=datetime.now(UTC), schema_version=schema_version, payload=sample_payload,
        )
        try:
            document = self._transformer.transform_document(route, event)
            payload = serialize_target_document(route.target_format, document)
        except Exception as exc:
            return PreprocessingResult(
                "CONFIGURATION_ERROR", ("definition_validation",),
                diagnostic=StructuredDiagnostic("definition_validation", "PREVIEW_FAILED", str(exc)),
            )
        return PreprocessingResult("DELIVERY_REQUESTED", ("definition_validation", "transformation", "serialization"), document, payload)
