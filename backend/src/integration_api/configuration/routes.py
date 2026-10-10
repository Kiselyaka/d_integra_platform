"""PostgreSQL-backed runtime route lookup for Message Processing."""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session, aliased, sessionmaker

from integration_api.configuration.models import (
    ExternalSystem,
    IntegrationRouteConfiguration,
    RetryPolicy,
    TransformationDefinition,
    ValidationSchema,
)
from integration_api.message_processing.contracts import ExternalEventEnvelope
from integration_api.message_processing.ports import (
    IntegrationRoute,
    RetryPolicyConfiguration,
    RouteProvider,
    ValidationSchemaConfiguration,
)


class ConfigurationError(RuntimeError):
    """A matching route cannot safely create a new Delivery."""


# M3--M5 callback Messages used this source identity. M6 normalizes new XML
# intake to ``1c-warehouse`` but an unprocessed historic Message must still be
# routable after the upgrade.
LEGACY_SOURCE_SYSTEM_CODES = {"warehouse-1c": "1c-warehouse"}


class PostgresRouteConfigurationProvider(RouteProvider):
    """Reads enabled route definitions directly from Configuration storage.

    The returned objects are plain immutable snapshots. Message Processing
    never receives an ORM relationship graph or a Session from this provider.
    """

    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._session_factory = session_factory

    def find_matching_routes(self, event: ExternalEventEnvelope) -> list[IntegrationRoute]:
        with self._session_factory() as session:
            resolved_source_code = LEGACY_SOURCE_SYSTEM_CODES.get(event.source_system, event.source_system)
            source = session.scalar(select(ExternalSystem).where(ExternalSystem.code == resolved_source_code))
            if source is None:
                raise ConfigurationError(f"Unknown source system: {event.source_system}")
            if not source.enabled:
                raise ConfigurationError(f"Source system is disabled: {source.code}")

            target = aliased(ExternalSystem)
            source_schema = aliased(ValidationSchema)
            target_schema = aliased(ValidationSchema)
            statement = (
                select(IntegrationRouteConfiguration, target, RetryPolicy, TransformationDefinition, source_schema, target_schema)
                .join(target, IntegrationRouteConfiguration.target_system_id == target.id)
                .join(RetryPolicy, IntegrationRouteConfiguration.retry_policy_id == RetryPolicy.id)
                .outerjoin(
                    TransformationDefinition,
                    IntegrationRouteConfiguration.transformation_id == TransformationDefinition.id,
                )
                .outerjoin(source_schema, IntegrationRouteConfiguration.source_validation_schema_id == source_schema.id)
                .outerjoin(target_schema, IntegrationRouteConfiguration.target_validation_schema_id == target_schema.id)
                .where(
                    IntegrationRouteConfiguration.source_system_id == source.id,
                    IntegrationRouteConfiguration.event_type == event.event_type,
                    IntegrationRouteConfiguration.enabled.is_(True),
                    IntegrationRouteConfiguration.status == "ACTIVE",
                )
                .order_by(IntegrationRouteConfiguration.route_key, IntegrationRouteConfiguration.version)
            )
            return [
                self._to_snapshot(route, target_system, retry_policy, transformation, source_contract, target_contract)
                for route, target_system, retry_policy, transformation, source_contract, target_contract in session.execute(statement).all()
            ]

    def get_route_version(self, route_id: str) -> IntegrationRoute | None:
        """Resolve an exact DRAFT/ACTIVE route for side-effect-free preview."""
        with self._session_factory() as session:
            target = aliased(ExternalSystem)
            source_schema = aliased(ValidationSchema)
            target_schema = aliased(ValidationSchema)
            row = session.execute(
                select(IntegrationRouteConfiguration, target, RetryPolicy, TransformationDefinition, source_schema, target_schema)
                .join(target, IntegrationRouteConfiguration.target_system_id == target.id)
                .join(RetryPolicy, IntegrationRouteConfiguration.retry_policy_id == RetryPolicy.id)
                .outerjoin(TransformationDefinition, IntegrationRouteConfiguration.transformation_id == TransformationDefinition.id)
                .outerjoin(source_schema, IntegrationRouteConfiguration.source_validation_schema_id == source_schema.id)
                .outerjoin(target_schema, IntegrationRouteConfiguration.target_validation_schema_id == target_schema.id)
                .where(IntegrationRouteConfiguration.id == route_id)
            ).one_or_none()
            if row is None:
                return None
            return self._to_snapshot(*row)

    @staticmethod
    def _to_snapshot(
        route: IntegrationRouteConfiguration,
        target: ExternalSystem,
        retry: RetryPolicy,
        transformation: TransformationDefinition | None,
        source_schema: ValidationSchema | None,
        target_schema: ValidationSchema | None,
    ) -> IntegrationRoute:
        errors: list[str] = []
        if not target.enabled:
            errors.append(f"Target system is disabled: {target.code}")
        if not retry.enabled:
            errors.append(f"Retry policy is disabled: {retry.name}")
        if transformation is None:
            errors.append("Route has no transformation definition")
        elif not transformation.enabled:
            errors.append(f"Transformation is disabled: {transformation.transformation_key}")
        elif transformation.status != "ACTIVE":
            errors.append(f"Transformation is not ACTIVE: {transformation.transformation_key}")
        for role, validation_schema in (("Source", source_schema), ("Target", target_schema)):
            if validation_schema is not None and not validation_schema.enabled:
                errors.append(f"{role} validation schema is disabled: {validation_schema.schema_key}")
            if validation_schema is not None and validation_schema.status != "ACTIVE":
                errors.append(f"{role} validation schema is not ACTIVE: {validation_schema.schema_key}")
        if transformation is not None and (
            route.source_format != transformation.source_format or route.target_format != transformation.target_format
        ):
            errors.append("Route formats do not match transformation formats")

        return IntegrationRoute(
            id=route.id,
            key=route.route_key,
            version=route.version,
            target_base_url=target.base_url.rstrip("/"),
            target_system=target.code,
            target_endpoint=route.target_endpoint,
            http_method=route.http_method,
            source_format=route.source_format,
            target_format=route.target_format,
            transformation_id=transformation.id if transformation is not None else None,
            transformation_key=transformation.transformation_key if transformation is not None else None,
            transformation_version=transformation.version if transformation is not None else None,
            transformation_engine_type=transformation.engine_type if transformation is not None else None,
            transformation_definition=transformation.definition if transformation is not None else None,
            validation_schema_key=source_schema.schema_key if source_schema is not None else None,
            validation_schema_version=source_schema.version if source_schema is not None else None,
            source_validation_schema=_schema_snapshot(source_schema),
            target_validation_schema=_schema_snapshot(target_schema),
            retry_policy=RetryPolicyConfiguration(
                id=retry.id,
                max_attempts=retry.max_attempts,
                strategy=retry.strategy,
                initial_delay_seconds=retry.initial_delay_seconds,
                max_delay_seconds=retry.max_delay_seconds,
                multiplier=retry.multiplier,
                retryable_errors=tuple(retry.retryable_errors),
            ),
            connect_timeout_ms=target.connect_timeout_ms,
            read_timeout_ms=target.read_timeout_ms,
            configuration_error="; ".join(errors) if errors else None,
        )


def _schema_snapshot(schema: ValidationSchema | None) -> ValidationSchemaConfiguration | None:
    if schema is None:
        return None
    return ValidationSchemaConfiguration(
        id=schema.id,
        key=schema.schema_key,
        version=schema.version,
        payload_format=schema.payload_format,
        schema_type=schema.schema_type,
        definition_json=schema.definition_json,
        definition_text=schema.definition_text,
    )
