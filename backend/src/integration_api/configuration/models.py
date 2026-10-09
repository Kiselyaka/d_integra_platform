"""Persistence owned exclusively by the Configuration bounded context."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, UniqueConstraint, func, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON

CONFIGURATION_SCHEMA = "configuration"


class ConfigurationBase(DeclarativeBase):
    """Metadata is intentionally separate from Message Processing tables."""


class AuthProfile(ConfigurationBase):
    __tablename__ = "auth_profiles"
    __table_args__ = ({"schema": CONFIGURATION_SCHEMA},)

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    auth_type: Mapped[str] = mapped_column(String(50), nullable=False)
    secret_reference: Mapped[str | None] = mapped_column(String(500), nullable=True)
    username_reference: Mapped[str | None] = mapped_column(String(500), nullable=True)
    header_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ExternalSystem(ConfigurationBase):
    __tablename__ = "external_systems"
    __table_args__ = (UniqueConstraint("code", name="uq_configuration_external_systems_code"), {"schema": CONFIGURATION_SCHEMA})

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    code: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    system_type: Mapped[str] = mapped_column(String(50), nullable=False)
    base_url: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    auth_profile_id: Mapped[str | None] = mapped_column(
        ForeignKey(f"{CONFIGURATION_SCHEMA}.auth_profiles.id"), nullable=True
    )
    connect_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    read_timeout_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    health_endpoint: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class ValidationSchema(ConfigurationBase):
    __tablename__ = "validation_schemas"
    __table_args__ = (
        UniqueConstraint("schema_key", "version", name="uq_configuration_validation_schemas_key_version"),
        {"schema": CONFIGURATION_SCHEMA},
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    schema_key: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_format: Mapped[str] = mapped_column(String(32), nullable=False)
    schema_type: Mapped[str] = mapped_column(String(50), nullable=False)
    definition_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    definition_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class TransformationDefinition(ConfigurationBase):
    __tablename__ = "transformation_definitions"
    __table_args__ = (
        UniqueConstraint("transformation_key", "version", name="uq_configuration_transformations_key_version"),
        {"schema": CONFIGURATION_SCHEMA},
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    transformation_key: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_format: Mapped[str] = mapped_column(String(32), nullable=False)
    target_format: Mapped[str] = mapped_column(String(32), nullable=False)
    engine_type: Mapped[str] = mapped_column(String(50), nullable=False)
    definition: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())


class RetryPolicy(ConfigurationBase):
    __tablename__ = "retry_policies"
    __table_args__ = (
        CheckConstraint("max_attempts >= 1", name="ck_configuration_retry_max_attempts"),
        CheckConstraint("initial_delay_seconds >= 0", name="ck_configuration_retry_initial_delay"),
        CheckConstraint("max_delay_seconds >= initial_delay_seconds", name="ck_configuration_retry_max_delay"),
        CheckConstraint("multiplier >= 1", name="ck_configuration_retry_multiplier"),
        {"schema": CONFIGURATION_SCHEMA},
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    strategy: Mapped[str] = mapped_column(String(50), nullable=False)
    initial_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    max_delay_seconds: Mapped[int] = mapped_column(Integer, nullable=False)
    multiplier: Mapped[int] = mapped_column(Integer, nullable=False)
    retryable_errors: Mapped[list[str]] = mapped_column(JSON, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class IntegrationRouteConfiguration(ConfigurationBase):
    __tablename__ = "integration_routes"
    __table_args__ = (
        UniqueConstraint("route_key", "version", name="uq_configuration_routes_key_version"),
        Index(
            "ix_configuration_routes_enabled_source_event",
            "source_system_id",
            "event_type",
            postgresql_where=text("enabled"),
        ),
        {"schema": CONFIGURATION_SCHEMA},
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    route_key: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    source_system_id: Mapped[str] = mapped_column(ForeignKey(f"{CONFIGURATION_SCHEMA}.external_systems.id"), nullable=False)
    event_type: Mapped[str] = mapped_column(String(128), nullable=False)
    target_system_id: Mapped[str] = mapped_column(ForeignKey(f"{CONFIGURATION_SCHEMA}.external_systems.id"), nullable=False)
    http_method: Mapped[str] = mapped_column(String(16), nullable=False)
    target_endpoint: Mapped[str] = mapped_column(String(500), nullable=False)
    source_format: Mapped[str] = mapped_column(String(32), nullable=False)
    target_format: Mapped[str] = mapped_column(String(32), nullable=False)
    validation_schema_id: Mapped[str | None] = mapped_column(
        ForeignKey(f"{CONFIGURATION_SCHEMA}.validation_schemas.id"), nullable=True
    )
    transformation_id: Mapped[str | None] = mapped_column(
        ForeignKey(f"{CONFIGURATION_SCHEMA}.transformation_definitions.id"), nullable=True
    )
    retry_policy_id: Mapped[str] = mapped_column(ForeignKey(f"{CONFIGURATION_SCHEMA}.retry_policies.id"), nullable=False)
    delivery_mode: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )
