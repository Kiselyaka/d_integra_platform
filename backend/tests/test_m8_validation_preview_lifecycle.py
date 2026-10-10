from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.orm import sessionmaker

from integration_api.configuration.lifecycle import ConfigurationLifecycleError, ConfigurationLifecycleService
from integration_api.configuration.models import ConfigurationBase, IntegrationRouteConfiguration, TransformationDefinition, ValidationSchema
from integration_api.configuration.preview import ConfigurationPreviewService
from integration_api.configuration.routes import PostgresRouteConfigurationProvider
from integration_api.configuration.seed import seed_demo_configuration
from integration_api.configuration.settings import Settings
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Base, Message, ProcessingOutboxEvent, RouteProcessingResult
from integration_api.message_processing.preprocessing import PreprocessingPipeline, SourceMaterial
from integration_api.message_processing.transformation import RouteTransformationProvider
from delivery_service.delivery.models import Delivery, DeliveryAttempt, DeliveryBase, DeliveryInboxMessage, DeliveryOutboxEvent


def _factory() -> sessionmaker:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(schema_translate_map={"configuration": None})
    Base.metadata.create_all(engine)
    ConfigurationBase.metadata.create_all(engine)
    DeliveryBase.metadata.create_all(engine)
    factory = sessionmaker(engine)
    with factory() as session:
        seed_demo_configuration(session, Settings())
    return factory


def _event(payload: dict | None = None) -> ExternalEventEnvelope:
    return ExternalEventEnvelope(
        event_id="m8-event", event_type="order.created", source_system="mini-shop", occurred_at=datetime(2026, 10, 10, tzinfo=UTC),
        schema_version=1, payload=payload or {
            "order_id": "order-m8", "customer": {"customer_id": "customer", "full_name": "Ada", "email": "ada@example.test", "phone": None},
            "shipping_address": {"country": "RU", "city": "Moscow", "postal_code": "101000", "street": "Tverskaya", "building": "1", "apartment": None},
            "items": [{"product_code": "SKU-001", "name": "Example", "quantity": 1, "unit_price": "10.00"}],
            "currency": "RUB", "total_amount": "10.00",
        },
    )


def _route(factory: sessionmaker, key: str):
    route = next(route for route in PostgresRouteConfigurationProvider(factory).find_matching_routes(_event()) if route.key == key)
    return route


def test_pipeline_distinguishes_source_target_and_wire_xsd_failures() -> None:
    factory = _factory()
    pipeline = PreprocessingPipeline(RouteTransformationProvider())
    crm_route = _route(factory, "mini-shop-order-created-to-crm")
    assert pipeline.execute(crm_route, _event({"order_id": "missing-everything"})).status == "SOURCE_VALIDATION_FAILED"

    target_bad = replace(crm_route, transformation_definition={"target": {"external_order_id": {"op": "from", "path": "$.order_id"}}})
    assert pipeline.execute(target_bad, _event()).status == "TARGET_VALIDATION_FAILED"

    mapping_bad = replace(crm_route, transformation_definition={"target": {"missing": {"op": "from", "path": "$.unknown"}}})
    assert pipeline.execute(mapping_bad, _event()).status == "TRANSFORMATION_FAILED"

    warehouse_route = _route(factory, "mini-shop-order-created-to-warehouse")
    wrong_order = replace(warehouse_route, transformation_definition={
        "target": {"xml": {"name": {"op": "literal", "value": "ReservationRequest"}, "children": [
            {"name": {"op": "literal", "value": "Positions"}, "children": []},
            {"name": {"op": "literal", "value": "OrderNumber"}, "text": {"op": "from", "path": "$.order_id"}},
        ]}}
    })
    wrong_order_result = pipeline.execute(wrong_order, _event())
    assert wrong_order_result.status == "TARGET_VALIDATION_FAILED"
    assert wrong_order_result.diagnostic is not None
    assert wrong_order_result.diagnostic.category == "XSD_VALIDATION_FAILED"


def test_warehouse_raw_xml_xsd_is_checked_before_callback_mapping() -> None:
    factory = _factory()
    route = PostgresRouteConfigurationProvider(factory).get_route_version(
        next(row.id for row in _route_rows(factory) if row.route_key == "warehouse-reservation-succeeded-to-mini-shop")
    )
    assert route is not None
    event = ExternalEventEnvelope(
        event_id="raw-xsd", source_system="1c-warehouse", event_type="warehouse.reservation.succeeded",
        occurred_at=datetime(2026, 10, 10, tzinfo=UTC), schema_version=1,
        payload={"external_order_id": "order", "status": None},
    )
    raw = b"<WarehouseEvent><EventId>raw-xsd</EventId><EventType>reservation.succeeded</EventType><OccurredAt>2026-10-10T00:00:00Z</OccurredAt><SchemaVersion>1</SchemaVersion><Payload><ReservationSucceeded><OrderNumber>order</OrderNumber></ReservationSucceeded></Payload></WarehouseEvent>"
    result = PreprocessingPipeline(RouteTransformationProvider()).execute(route, event, SourceMaterial("XML", raw))
    assert result.status == "SOURCE_VALIDATION_FAILED"
    assert result.diagnostic is not None and result.diagnostic.category == "XSD_VALIDATION_FAILED"


def test_preview_creates_no_runtime_rows_and_lifecycle_retires_previous_version() -> None:
    factory = _factory()
    route_row = next(row for row in _route_rows(factory) if row.route_key == "mini-shop-order-created-to-warehouse")
    with factory() as session:
        runtime_models = (
            Message, RouteProcessingResult, ProcessingOutboxEvent,
            Delivery, DeliveryAttempt, DeliveryInboxMessage, DeliveryOutboxEvent,
        )
        before = tuple(session.scalar(select(func.count()).select_from(model)) for model in runtime_models)
    preview = ConfigurationPreviewService(factory).preview_route(route_row.id, _event().payload, source_system="mini-shop", event_type="order.created")
    assert preview.success and isinstance(preview.payload.body, str)
    with factory() as session:
        after = tuple(session.scalar(select(func.count()).select_from(model)) for model in runtime_models)
        transformation = session.scalar(select(TransformationDefinition).where(TransformationDefinition.transformation_key == "order-created-to-crm"))
        assert transformation is not None
        transformation_id = transformation.id
    assert before == after

    lifecycle = ConfigurationLifecycleService(factory)
    v2 = lifecycle.create_new_version("transformation", transformation_id)
    assert v2.status == "DRAFT" and v2.version == 2
    lifecycle.update_draft("transformation", v2.id, {"name": "Order created to CRM v2"})
    lifecycle.activate("transformation", v2.id)
    with factory() as session:
        versions = list(session.scalars(select(TransformationDefinition).where(TransformationDefinition.transformation_key == "order-created-to-crm").order_by(TransformationDefinition.version)))
    assert [item.status for item in versions] == ["RETIRED", "ACTIVE"]
    with pytest.raises(ConfigurationLifecycleError):
        lifecycle.update_draft("transformation", versions[1].id, {"name": "forbidden"})


def test_route_and_schema_lifecycle_keep_one_active_version_and_validate_drafts() -> None:
    factory = _factory()
    lifecycle = ConfigurationLifecycleService(factory)
    with factory() as session:
        route = session.scalar(select(IntegrationRouteConfiguration).where(
            IntegrationRouteConfiguration.route_key == "mini-shop-order-created-to-crm"
        ))
        schema = session.scalar(select(ValidationSchema).where(
            ValidationSchema.schema_key == "mini-shop-order-created-source"
        ))
        assert route is not None and schema is not None
        route_id, schema_id = route.id, schema.id

    route_v2 = lifecycle.create_new_version("route", route_id)
    lifecycle.activate("route", route_v2.id)
    schema_v2 = lifecycle.create_new_version("schema", schema_id)
    lifecycle.update_draft("schema", schema_v2.id, {"definition_json": {"type": "not-a-real-json-schema"}})
    with pytest.raises(ConfigurationLifecycleError):
        lifecycle.activate("schema", schema_v2.id)

    with factory() as session:
        routes = list(session.scalars(select(IntegrationRouteConfiguration).where(
            IntegrationRouteConfiguration.route_key == "mini-shop-order-created-to-crm"
        ).order_by(IntegrationRouteConfiguration.version)))
        schema_v2_row = session.get(ValidationSchema, schema_v2.id)
    assert [item.status for item in routes] == ["RETIRED", "ACTIVE"]
    assert schema_v2_row is not None and schema_v2_row.status == "DRAFT"
    matched = [route for route in PostgresRouteConfigurationProvider(factory).find_matching_routes(_event()) if route.key == "mini-shop-order-created-to-crm"]
    assert [route.version for route in matched] == [2]


def _route_rows(factory: sessionmaker) -> list[IntegrationRouteConfiguration]:
    with factory() as session:
        return list(session.scalars(select(IntegrationRouteConfiguration)))
