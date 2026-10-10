"""Idempotent M6 demonstration configuration bootstrap."""

from __future__ import annotations

from uuid import NAMESPACE_URL, uuid5

from sqlalchemy import select
from sqlalchemy.orm import Session

from integration_api.configuration.models import (
    AuthProfile,
    ExternalSystem,
    IntegrationRouteConfiguration,
    RetryPolicy,
    TransformationDefinition,
)
from integration_api.configuration.demo_definitions import FIELD_MAPPING_V1_DEFINITIONS
from integration_api.configuration.settings import Settings
from shared.database import create_database_engine, create_session_factory


def _id(name: str) -> str:
    return str(uuid5(NAMESPACE_URL, f"integration-platform/demo-configuration/{name}"))


def seed_demo_configuration(session: Session, settings: Settings) -> None:
    """Insert missing demo definitions without changing operator-managed rows."""

    auth = _get_or_add(session, AuthProfile, AuthProfile.name == "demo-none", lambda: AuthProfile(
        id=_id("auth/demo-none"), name="demo-none", auth_type="NONE", enabled=True
    ))
    systems = {
        "mini-shop": _get_or_add(session, ExternalSystem, ExternalSystem.code == "mini-shop", lambda: ExternalSystem(
            id=_id("system/mini-shop"), code="mini-shop", name="Mini Shop", system_type="HTTP_JSON",
            base_url=settings.mini_shop_base_url, enabled=True, auth_profile_id=auth.id,
            connect_timeout_ms=5000, read_timeout_ms=5000, health_endpoint="/health",
        )),
        "crm": _get_or_add(session, ExternalSystem, ExternalSystem.code == "crm", lambda: ExternalSystem(
            id=_id("system/crm"), code="crm", name="Demo CRM", system_type="HTTP_JSON",
            base_url=settings.crm_base_url, enabled=True, auth_profile_id=auth.id,
            connect_timeout_ms=5000, read_timeout_ms=5000, health_endpoint="/health",
        )),
        "1c-warehouse": _get_or_add(session, ExternalSystem, ExternalSystem.code == "1c-warehouse", lambda: ExternalSystem(
            id=_id("system/1c-warehouse"), code="1c-warehouse", name="Warehouse 1C", system_type="HTTP_XML",
            base_url=settings.warehouse_base_url, enabled=True, auth_profile_id=auth.id,
            connect_timeout_ms=5000, read_timeout_ms=5000, health_endpoint=None,
        )),
    }
    retry = _get_or_add(session, RetryPolicy, RetryPolicy.name == "demo-exponential-v1", lambda: RetryPolicy(
        id=_id("retry/demo-exponential-v1"), name="demo-exponential-v1", max_attempts=settings.delivery_max_attempts,
        strategy=settings.delivery_retry_strategy, initial_delay_seconds=settings.delivery_retry_initial_delay_seconds,
        max_delay_seconds=settings.delivery_retry_max_delay_seconds, multiplier=settings.delivery_retry_multiplier,
        retryable_errors=["TIMEOUT", "CONNECTION_ERROR", "REMOTE_SERVER_ERROR", "RATE_LIMITED"], enabled=True,
    ))
    transformations = {
        key: _get_or_add(session, TransformationDefinition,
            (TransformationDefinition.transformation_key == key) & (TransformationDefinition.version == 1),
            lambda key=key, source=source, target=target, name=name: TransformationDefinition(
                id=_id(f"transformation/{key}/1"), transformation_key=key, version=1, name=name,
                source_format=source, target_format=target, engine_type="FIELD_MAPPING_V1",
                definition=FIELD_MAPPING_V1_DEFINITIONS[key], enabled=True,
            ),
        )
        for key, source, target, name in [
            ("order-created-to-crm", "JSON", "JSON", "Order created to CRM"),
            ("order-created-to-warehouse-reservation", "JSON", "XML", "Order created to Warehouse reservation"),
            ("warehouse-reservation-succeeded-to-mini-shop", "JSON", "JSON", "Warehouse success to Mini Shop"),
            ("warehouse-reservation-failed-to-mini-shop", "JSON", "JSON", "Warehouse failure to Mini Shop"),
        ]
    }
    route_specs = [
        ("mini-shop-order-created-to-crm", "Mini Shop order created to CRM", "mini-shop", "order.created", "crm", "/api/integration/orders", "JSON", "JSON", "order-created-to-crm"),
        ("mini-shop-order-created-to-warehouse", "Mini Shop order created to Warehouse", "mini-shop", "order.created", "1c-warehouse", "/hs/reservations/", "JSON", "XML", "order-created-to-warehouse-reservation"),
        ("warehouse-reservation-succeeded-to-mini-shop", "Warehouse reservation success to Mini Shop", "1c-warehouse", "warehouse.reservation.succeeded", "mini-shop", "/api/integration/reservation-results", "JSON", "JSON", "warehouse-reservation-succeeded-to-mini-shop"),
        ("warehouse-reservation-failed-to-mini-shop", "Warehouse reservation failure to Mini Shop", "1c-warehouse", "warehouse.reservation.failed", "mini-shop", "/api/integration/reservation-results", "JSON", "JSON", "warehouse-reservation-failed-to-mini-shop"),
    ]
    for key, name, source, event_type, target, endpoint, source_format, target_format, transformation in route_specs:
        _get_or_add(session, IntegrationRouteConfiguration,
            (IntegrationRouteConfiguration.route_key == key) & (IntegrationRouteConfiguration.version == 1),
            lambda key=key, name=name, source=source, event_type=event_type, target=target, endpoint=endpoint, source_format=source_format, target_format=target_format, transformation=transformation: IntegrationRouteConfiguration(
                id=_id(f"route/{key}/1"), route_key=key, version=1, name=name, enabled=True,
                source_system_id=systems[source].id, event_type=event_type, target_system_id=systems[target].id,
                http_method="POST", target_endpoint=endpoint, source_format=source_format, target_format=target_format,
                validation_schema_id=None, transformation_id=transformations[transformation].id,
                retry_policy_id=retry.id, delivery_mode="ASYNC",
            ),
        )
    session.commit()


def _get_or_add(session: Session, model: type, criterion: object, factory):  # type: ignore[no-untyped-def]
    existing = session.scalar(select(model).where(criterion))
    if existing is not None:
        return existing
    value = factory()
    session.add(value)
    session.flush()
    return value


def main() -> None:
    settings = Settings()
    engine = create_database_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    with session_factory() as session:
        seed_demo_configuration(session, settings)


if __name__ == "__main__":
    main()
