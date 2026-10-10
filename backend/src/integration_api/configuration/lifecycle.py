"""M8 application commands for immutable versioned configuration."""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Literal, TypeAlias
from uuid import uuid4

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from integration_api.configuration.models import (
    ExternalSystem,
    IntegrationRouteConfiguration,
    RetryPolicy,
    TransformationDefinition,
    ValidationSchema,
)
from integration_api.message_processing.field_mapping import FieldMappingV1DefinitionParser, TransformationConfigurationError
from integration_api.message_processing.preprocessing import ContractValidator, ValidationConfigurationError
from integration_api.message_processing.ports import ValidationSchemaConfiguration


ConfigurationEntity: TypeAlias = ValidationSchema | TransformationDefinition | IntegrationRouteConfiguration
EntityKind = Literal["schema", "transformation", "route"]


class ConfigurationLifecycleError(ValueError):
    pass


class ConfigurationConflictError(ConfigurationLifecycleError):
    pass


_MODELS: dict[str, type[ConfigurationEntity]] = {
    "schema": ValidationSchema,
    "transformation": TransformationDefinition,
    "route": IntegrationRouteConfiguration,
}
_KEY_FIELDS = {"schema": "schema_key", "transformation": "transformation_key", "route": "route_key"}


class ConfigurationValidationService:
    """Cross-entity checks used by activation, never by delivery runtime."""

    def __init__(self, validator: ContractValidator | None = None) -> None:
        self._validator = validator or ContractValidator()

    def validate_schema(self, schema: ValidationSchema) -> None:
        try:
            self._validator.validate_definition(_schema_snapshot(schema))
        except ValidationConfigurationError as exc:
            raise ConfigurationLifecycleError(str(exc)) from exc

    def validate_transformation(self, definition: TransformationDefinition) -> None:
        if definition.engine_type != "FIELD_MAPPING_V1":
            raise ConfigurationLifecycleError(f"Unsupported transformation engine: {definition.engine_type}")
        if definition.source_format not in {"JSON", "XML"} or definition.target_format not in {"JSON", "XML"}:
            raise ConfigurationLifecycleError("Transformation has unsupported format metadata")
        try:
            FieldMappingV1DefinitionParser().parse(definition.definition)
        except TransformationConfigurationError as exc:
            raise ConfigurationLifecycleError(str(exc)) from exc

    def validate_route(self, session: Session, route: IntegrationRouteConfiguration) -> None:
        source = session.get(ExternalSystem, route.source_system_id)
        target = session.get(ExternalSystem, route.target_system_id)
        retry = session.get(RetryPolicy, route.retry_policy_id)
        transformation = session.get(TransformationDefinition, route.transformation_id)
        source_schema = session.get(ValidationSchema, route.source_validation_schema_id)
        target_schema = session.get(ValidationSchema, route.target_validation_schema_id)
        if source is None or not source.enabled:
            raise ConfigurationLifecycleError("Route source system is missing or disabled")
        if target is None or not target.enabled:
            raise ConfigurationLifecycleError("Route target system is missing or disabled")
        if retry is None or not retry.enabled:
            raise ConfigurationLifecycleError("Route retry policy is missing or disabled")
        if transformation is None or transformation.status != "ACTIVE" or not transformation.enabled:
            raise ConfigurationLifecycleError("Route transformation must be ACTIVE and enabled")
        for role, schema, expected in (("source", source_schema, route.source_format), ("target", target_schema, route.target_format)):
            if schema is None or schema.status != "ACTIVE" or not schema.enabled:
                raise ConfigurationLifecycleError(f"Route {role} schema must be ACTIVE and enabled")
            if schema.payload_format != expected:
                raise ConfigurationLifecycleError(f"Route {role} schema format does not match route format")
        if transformation.source_format != route.source_format or transformation.target_format != route.target_format:
            raise ConfigurationLifecycleError("Route formats do not match transformation formats")


class ConfigurationLifecycleService:
    def __init__(self, session_factory: sessionmaker[Session], validation: ConfigurationValidationService | None = None) -> None:
        self._session_factory = session_factory
        self._validation = validation or ConfigurationValidationService()

    def create_new_version(self, kind: EntityKind, entity_id: str) -> ConfigurationEntity:
        model = _MODELS[kind]
        key_field = _KEY_FIELDS[kind]
        with self._session_factory.begin() as session:
            original = session.get(model, entity_id)
            if original is None:
                raise ConfigurationLifecycleError("Configuration object was not found")
            key = getattr(original, key_field)
            versions = list(session.scalars(select(model).where(getattr(model, key_field) == key).with_for_update()))
            next_version = max(item.version for item in versions) + 1
            clone = _clone(kind, original, next_version)
            session.add(clone)
            session.flush()
            session.expunge(clone)
            return clone

    def update_draft(self, kind: EntityKind, entity_id: str, changes: dict[str, Any]) -> ConfigurationEntity:
        model = _MODELS[kind]
        with self._session_factory.begin() as session:
            entity = session.get(model, entity_id)
            if entity is None:
                raise ConfigurationLifecycleError("Configuration object was not found")
            _require_draft(entity)
            forbidden = {"id", "version", "status", "created_at", "activated_at", "retired_at"}
            if forbidden & set(changes):
                raise ConfigurationLifecycleError("Lifecycle identity cannot be changed in place")
            for field, value in changes.items():
                if not hasattr(entity, field):
                    raise ConfigurationLifecycleError(f"Unknown configuration field: {field}")
                setattr(entity, field, deepcopy(value))
            session.flush()
            session.expunge(entity)
            return entity

    def activate(self, kind: EntityKind, entity_id: str) -> ConfigurationEntity:
        model = _MODELS[kind]
        key_field = _KEY_FIELDS[kind]
        try:
            with self._session_factory.begin() as session:
                candidate = session.get(model, entity_id)
                if candidate is None:
                    raise ConfigurationLifecycleError("Configuration object was not found")
                _require_draft(candidate)
                key = getattr(candidate, key_field)
                siblings = list(session.scalars(select(model).where(getattr(model, key_field) == key).with_for_update()))
                if kind == "schema":
                    self._validation.validate_schema(candidate)  # type: ignore[arg-type]
                elif kind == "transformation":
                    self._validation.validate_transformation(candidate)  # type: ignore[arg-type]
                else:
                    self._validation.validate_route(session, candidate)  # type: ignore[arg-type]
                now = datetime.now(timezone.utc)
                for sibling in siblings:
                    if sibling.status == "ACTIVE":
                        sibling.status = "RETIRED"
                        sibling.retired_at = now
                # A partial unique index allows only one ACTIVE version. Flush
                # retirement first: SQLAlchemy does not guarantee the update
                # ordering otherwise, and activating the candidate first
                # would transiently violate that invariant.
                session.flush()
                candidate.status = "ACTIVE"
                candidate.activated_at = now
                session.flush()
                session.expunge(candidate)
                return candidate
        except IntegrityError as exc:
            raise ConfigurationConflictError("Another version was activated concurrently") from exc

    def retire(self, kind: EntityKind, entity_id: str) -> ConfigurationEntity:
        model = _MODELS[kind]
        with self._session_factory.begin() as session:
            entity = session.get(model, entity_id)
            if entity is None:
                raise ConfigurationLifecycleError("Configuration object was not found")
            if entity.status != "ACTIVE":
                raise ConfigurationLifecycleError("Only ACTIVE configuration can be retired")
            entity.status = "RETIRED"
            entity.retired_at = datetime.now(timezone.utc)
            session.flush()
            session.expunge(entity)
            return entity


def _require_draft(entity: ConfigurationEntity) -> None:
    if entity.status != "DRAFT":
        raise ConfigurationLifecycleError("Only DRAFT configuration is editable or activatable")


def _clone(kind: EntityKind, source: ConfigurationEntity, version: int) -> ConfigurationEntity:
    payload = {column.name: deepcopy(getattr(source, column.name)) for column in source.__table__.columns}
    payload.pop("created_at", None)
    payload.pop("updated_at", None)
    payload.update({"id": str(uuid4()), "version": version, "status": "DRAFT", "activated_at": None, "retired_at": None})
    return _MODELS[kind](**payload)  # type: ignore[call-arg]


def _schema_snapshot(schema: ValidationSchema) -> ValidationSchemaConfiguration:
    return ValidationSchemaConfiguration(
        id=schema.id, key=schema.schema_key, version=schema.version, payload_format=schema.payload_format,
        schema_type=schema.schema_type, definition_json=schema.definition_json, definition_text=schema.definition_text,
    )
