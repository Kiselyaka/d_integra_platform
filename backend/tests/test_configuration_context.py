from __future__ import annotations

from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from integration_api.configuration.models import (
    ConfigurationBase,
    ExternalSystem,
    IntegrationRouteConfiguration,
    RetryPolicy,
    TransformationDefinition,
    ValidationSchema,
)
from integration_api.configuration.routes import ConfigurationError, PostgresRouteConfigurationProvider
from integration_api.configuration.seed import seed_demo_configuration
from integration_api.configuration.settings import Settings
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.models import Base, Message, ProcessingOutboxEvent, RouteProcessingResult
from integration_api.message_processing.processing import EventProcessingService
from integration_api.message_processing.transformation import RouteTransformationProvider


def _factory() -> sessionmaker:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True).execution_options(
        schema_translate_map={"configuration": None}
    )
    Base.metadata.create_all(engine)
    ConfigurationBase.metadata.create_all(engine)
    factory = sessionmaker(engine)
    with factory() as session:
        seed_demo_configuration(session, Settings())
    return factory


def _event(event_id: str, order_id: str = "order-m6") -> ExternalEventEnvelope:
    return ExternalEventEnvelope(
        event_id=event_id,
        event_type="order.created",
        source_system="mini-shop",
        occurred_at=datetime(2026, 10, 10, tzinfo=UTC),
        correlation_id=order_id,
        schema_version=1,
        payload={
            "order_id": order_id,
            "customer": {"customer_id": "customer-m6", "full_name": "M6 Test", "email": "m6@example.test", "phone": "+70000000000"},
            "shipping_address": {"country": "RU", "city": "Moscow", "street": "Test", "building": "1", "apartment": "2", "postal_code": "101000"},
            "items": [{"product_code": "SKU-001", "name": "Example Product", "quantity": 1, "unit_price": "100.00"}],
            "total_amount": "100.00",
            "currency": "RUB",
        },
    )


def _persist_message(factory: sessionmaker, event: ExternalEventEnvelope, message_id: str) -> None:
    with factory() as session:
        session.add(
            Message(
                id=message_id,
                external_event_id=event.event_id,
                event_type=event.event_type,
                source_system=event.source_system,
                payload=event.payload,
                status="VALIDATED",
                correlation_id=event.correlation_id,
                schema_version=event.schema_version,
                occurred_at=event.occurred_at,
            )
        )
        session.commit()


def test_seed_is_idempotent_and_exposes_two_database_driven_order_routes() -> None:
    factory = _factory()
    with factory() as session:
        seed_demo_configuration(session, Settings())
        assert session.scalar(select(func.count()).select_from(ExternalSystem)) == 3
        assert session.scalar(select(func.count()).select_from(IntegrationRouteConfiguration)) == 4
        assert session.scalar(select(func.count()).select_from(TransformationDefinition)) == 4

    routes = PostgresRouteConfigurationProvider(factory).find_matching_routes(_event("event-routes"))
    assert [(route.key, route.target_system, route.target_endpoint) for route in routes] == [
        ("mini-shop-order-created-to-crm", "crm", "/api/integration/orders"),
        ("mini-shop-order-created-to-warehouse", "1c-warehouse", "/hs/reservations/"),
    ]
    assert {route.transformation_key for route in routes} == {
        "order-created-to-crm",
        "order-created-to-warehouse-reservation",
    }
    assert all(route.transformation_engine_type == "FIELD_MAPPING_V1" for route in routes)
    assert all(route.transformation_definition is not None for route in routes)
    assert all(route.retry_policy is not None for route in routes)


def test_callback_routes_are_resolved_from_configuration() -> None:
    factory = _factory()
    provider = PostgresRouteConfigurationProvider(factory)
    for event_type, key in [
        ("warehouse.reservation.succeeded", "warehouse-reservation-succeeded-to-mini-shop"),
        ("warehouse.reservation.failed", "warehouse-reservation-failed-to-mini-shop"),
    ]:
        callback = ExternalEventEnvelope(
            event_id=f"event-{event_type}", event_type=event_type, source_system="1c-warehouse",
            occurred_at=datetime(2026, 10, 10, tzinfo=UTC), schema_version=1, payload={},
        )
        routes = provider.find_matching_routes(callback)
        assert [(route.key, route.target_system) for route in routes] == [(key, "mini-shop")]


def test_legacy_warehouse_source_code_remains_routable_after_m6_upgrade() -> None:
    factory = _factory()
    legacy_callback = ExternalEventEnvelope(
        event_id="legacy-warehouse-callback", event_type="warehouse.reservation.succeeded", source_system="warehouse-1c",
        occurred_at=datetime(2026, 10, 10, tzinfo=UTC), schema_version=1, payload={},
    )

    routes = PostgresRouteConfigurationProvider(factory).find_matching_routes(legacy_callback)

    assert [(route.key, route.target_system) for route in routes] == [
        ("warehouse-reservation-succeeded-to-mini-shop", "mini-shop")
    ]


def test_disabling_a_route_changes_fanout_without_application_code_change() -> None:
    factory = _factory()
    provider = PostgresRouteConfigurationProvider(factory)
    with factory() as session:
        crm_route = session.scalar(select(IntegrationRouteConfiguration).where(
            IntegrationRouteConfiguration.route_key == "mini-shop-order-created-to-crm"
        ))
        assert crm_route is not None
        crm_route.enabled = False
        session.commit()
    assert [route.target_system for route in provider.find_matching_routes(_event("event-route-disabled"))] == ["1c-warehouse"]


def test_processing_snapshots_configuration_and_keeps_existing_work_immutable() -> None:
    factory = _factory()
    provider = PostgresRouteConfigurationProvider(factory)
    processor = EventProcessingService(factory, provider, RouteTransformationProvider())
    first = _event("event-snapshot-a", "order-snapshot-a")
    _persist_message(factory, first, "message-snapshot-a")
    processor.process("message-snapshot-a")

    with factory() as session:
        first_crm = session.scalar(select(ProcessingOutboxEvent).where(
            ProcessingOutboxEvent.aggregate_id == session.scalar(select(RouteProcessingResult.id).where(
                RouteProcessingResult.message_id == "message-snapshot-a",
                RouteProcessingResult.route_key == "mini-shop-order-created-to-crm",
            ))
        ))
        assert first_crm is not None
        assert first_crm.envelope["payload"]["target"]["url"] == "http://crm:8000/api/integration/orders"
        assert first_crm.envelope["payload"]["retry"]["max_attempts"] == 3
        assert first_crm.envelope["payload"]["target"]["read_timeout_ms"] == 5000
        crm = session.scalar(select(ExternalSystem).where(ExternalSystem.code == "crm"))
        policy = session.scalar(select(RetryPolicy).where(RetryPolicy.name == "demo-exponential-v1"))
        assert crm is not None and policy is not None
        crm.base_url = "http://crm-new:8000"
        policy.max_attempts = 7
        session.commit()

    second = _event("event-snapshot-b", "order-snapshot-b")
    _persist_message(factory, second, "message-snapshot-b")
    processor.process("message-snapshot-b")
    with factory() as session:
        envelopes = list(session.scalars(select(ProcessingOutboxEvent).order_by(ProcessingOutboxEvent.created_at)))
        crm_envelopes = [item.envelope for item in envelopes if item.envelope["payload"]["target"]["system_code"] == "crm"]
        assert [item["payload"]["target"]["url"] for item in crm_envelopes] == [
            "http://crm:8000/api/integration/orders", "http://crm-new:8000/api/integration/orders"
        ]
        assert [item["payload"]["retry"]["max_attempts"] for item in crm_envelopes] == [3, 7]


def test_disabled_target_system_records_configuration_error_without_delivery_request() -> None:
    factory = _factory()
    with factory() as session:
        crm = session.scalar(select(ExternalSystem).where(ExternalSystem.code == "crm"))
        assert crm is not None
        crm.enabled = False
        session.commit()
    event = _event("event-disabled-system")
    _persist_message(factory, event, "message-disabled-system")
    EventProcessingService(factory, PostgresRouteConfigurationProvider(factory), RouteTransformationProvider()).process("message-disabled-system")
    with factory() as session:
        results = list(session.scalars(select(RouteProcessingResult).order_by(RouteProcessingResult.route_key)))
        assert [(result.route_key, result.status) for result in results] == [
            ("mini-shop-order-created-to-crm", "CONFIGURATION_ERROR"),
            ("mini-shop-order-created-to-warehouse", "DELIVERY_REQUESTED"),
        ]
        assert session.scalar(select(func.count()).select_from(ProcessingOutboxEvent)) == 1


def test_transformation_identity_is_selected_by_route_data_not_target_branching() -> None:
    factory = _factory()
    with factory() as session:
        route = session.scalar(select(IntegrationRouteConfiguration).where(
            IntegrationRouteConfiguration.route_key == "mini-shop-order-created-to-warehouse"
        ))
        crm_transformation = session.scalar(select(TransformationDefinition).where(
            TransformationDefinition.transformation_key == "order-created-to-crm"
        ))
        crm_schema = session.scalar(select(ValidationSchema).where(ValidationSchema.schema_key == "crm-order-target"))
        assert route is not None and crm_transformation is not None and crm_schema is not None
        # This deliberately odd test route is not sent over HTTP. It proves
        # that Message Processing executes the configured transformation key,
        # not a hardcoded target_system == warehouse branch.
        route.transformation_id = crm_transformation.id
        route.target_format = "JSON"
        route.target_validation_schema_id = crm_schema.id
        session.commit()
    event = _event("event-transform-selection")
    _persist_message(factory, event, "message-transform-selection")
    EventProcessingService(factory, PostgresRouteConfigurationProvider(factory), RouteTransformationProvider()).process(
        "message-transform-selection"
    )
    with factory() as session:
        outbox = session.scalar(select(ProcessingOutboxEvent).where(
            ProcessingOutboxEvent.aggregate_id == select(RouteProcessingResult.id).where(
                RouteProcessingResult.message_id == "message-transform-selection",
                RouteProcessingResult.route_key == "mini-shop-order-created-to-warehouse",
            ).scalar_subquery()
        ))
        assert outbox is not None
        assert outbox.envelope["payload"]["content"]["format"] == "JSON"
        assert outbox.envelope["payload"]["content"]["body"]["external_order_id"] == "order-m6"


def test_definition_mutation_changes_only_new_transformation_snapshots() -> None:
    factory = _factory()
    processor = EventProcessingService(factory, PostgresRouteConfigurationProvider(factory), RouteTransformationProvider())
    first = _event("event-definition-a", "order-definition-a")
    _persist_message(factory, first, "message-definition-a")
    processor.process("message-definition-a")
    with factory() as session:
        transform = session.scalar(select(TransformationDefinition).where(
            TransformationDefinition.transformation_key == "order-created-to-crm"
        ))
        assert transform is not None and transform.definition is not None
        updated = {**transform.definition, "target": {**transform.definition["target"], "client": {
            **transform.definition["target"]["client"], "name": {"op": "literal", "value": "Configured name"}
        }}}
        transform.definition = updated
        session.commit()
    second = _event("event-definition-b", "order-definition-b")
    _persist_message(factory, second, "message-definition-b")
    processor.process("message-definition-b")
    with factory() as session:
        envelopes = list(session.scalars(select(ProcessingOutboxEvent).order_by(ProcessingOutboxEvent.created_at)))
        crm_bodies = [item.envelope["payload"]["content"]["body"] for item in envelopes if item.envelope["payload"]["target"]["system_code"] == "crm"]
        assert [body["client"]["name"] for body in crm_bodies] == ["M6 Test", "Configured name"]


def test_invalid_field_mapping_is_configuration_error_but_other_fanout_route_continues() -> None:
    factory = _factory()
    with factory() as session:
        transform = session.scalar(select(TransformationDefinition).where(
            TransformationDefinition.transformation_key == "order-created-to-crm"
        ))
        assert transform is not None
        transform.definition = {"target": {"unsafe": {"op": "eval", "value": "anything"}}}
        session.commit()
    event = _event("event-invalid-definition")
    _persist_message(factory, event, "message-invalid-definition")
    EventProcessingService(factory, PostgresRouteConfigurationProvider(factory), RouteTransformationProvider()).process(
        "message-invalid-definition"
    )
    with factory() as session:
        results = list(session.scalars(select(RouteProcessingResult).order_by(RouteProcessingResult.route_key)))
        assert [(result.route_key, result.status) for result in results] == [
            ("mini-shop-order-created-to-crm", "CONFIGURATION_ERROR"),
            ("mini-shop-order-created-to-warehouse", "DELIVERY_REQUESTED"),
        ]
        assert '"category": "CONFIGURATION_INVALID"' in results[0].error
        assert session.scalar(select(func.count()).select_from(ProcessingOutboxEvent)) == 1


def test_runtime_mapping_error_creates_no_delivery_for_that_route() -> None:
    factory = _factory()
    with factory() as session:
        transform = session.scalar(select(TransformationDefinition).where(
            TransformationDefinition.transformation_key == "order-created-to-crm"
        ))
        assert transform is not None
        transform.definition = {"target": {"missing": {"op": "from", "path": "$.unknown"}}}
        session.commit()
    event = _event("event-runtime-definition")
    _persist_message(factory, event, "message-runtime-definition")
    EventProcessingService(factory, PostgresRouteConfigurationProvider(factory), RouteTransformationProvider()).process(
        "message-runtime-definition"
    )
    with factory() as session:
        results = list(session.scalars(select(RouteProcessingResult).order_by(RouteProcessingResult.route_key)))
        assert [(result.route_key, result.status) for result in results] == [
            ("mini-shop-order-created-to-crm", "TRANSFORMATION_FAILED"),
            ("mini-shop-order-created-to-warehouse", "DELIVERY_REQUESTED"),
        ]
        assert '"category": "SOURCE_PATH_NOT_FOUND"' in results[0].error
        assert session.scalar(select(func.count()).select_from(ProcessingOutboxEvent)) == 1


def test_repository_constraints_reject_duplicate_transform_and_invalid_retry_policy() -> None:
    factory = _factory()
    with factory() as session:
        duplicate = TransformationDefinition(
            id="duplicate-transform", transformation_key="order-created-to-crm", version=1, name="duplicate",
            source_format="JSON", target_format="JSON", engine_type="BUILTIN", definition=None, enabled=True,
        )
        session.add(duplicate)
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()
        session.add(RetryPolicy(
            id="invalid-policy", name="invalid-policy", max_attempts=0, strategy="exponential",
            initial_delay_seconds=1, max_delay_seconds=1, multiplier=1, retryable_errors=[], enabled=True,
        ))
        with pytest.raises(IntegrityError):
            session.commit()
        session.rollback()


def test_unknown_or_disabled_source_is_a_configuration_error() -> None:
    factory = _factory()
    provider = PostgresRouteConfigurationProvider(factory)
    unknown = _event("event-unknown")
    unknown = unknown.model_copy(update={"source_system": "unknown-system"})
    with pytest.raises(ConfigurationError, match="Unknown source"):
        provider.find_matching_routes(unknown)
    with factory() as session:
        source = session.scalar(select(ExternalSystem).where(ExternalSystem.code == "mini-shop"))
        assert source is not None
        source.enabled = False
        session.commit()
    with pytest.raises(ConfigurationError, match="disabled"):
        provider.find_matching_routes(_event("event-source-disabled"))
