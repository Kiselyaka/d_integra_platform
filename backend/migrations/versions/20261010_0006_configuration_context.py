"""Add the M6 persistent Configuration Context."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20261010_0006"
down_revision = "20261009_0005"
branch_labels = None
depends_on = None


SCHEMA = "configuration"


def _schema_name() -> str | None:
    return SCHEMA if op.get_bind().dialect.name == "postgresql" else None


def _reference(schema: str | None, table: str) -> str:
    return f"{schema}.{table}.id" if schema is not None else f"{table}.id"


def upgrade() -> None:
    schema = _schema_name()
    json_type = postgresql.JSONB(astext_type=sa.Text()) if schema is not None else sa.JSON()
    if schema is not None:
        op.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    op.create_table(
        "auth_profiles",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("auth_type", sa.String(length=50), nullable=False),
        sa.Column("secret_reference", sa.String(length=500), nullable=True),
        sa.Column("username_reference", sa.String(length=500), nullable=True),
        sa.Column("header_name", sa.String(length=255), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("name", name="uq_configuration_auth_profiles_name"),
        schema=schema,
    )
    op.create_table(
        "external_systems",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("code", sa.String(length=100), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("system_type", sa.String(length=50), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("auth_profile_id", sa.String(length=36), nullable=True),
        sa.Column("connect_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("read_timeout_ms", sa.Integer(), nullable=False),
        sa.Column("health_endpoint", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["auth_profile_id"], [_reference(schema, "auth_profiles")]),
        sa.UniqueConstraint("code", name="uq_configuration_external_systems_code"),
        schema=schema,
    )
    op.create_table(
        "validation_schemas",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("schema_key", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("payload_format", sa.String(length=32), nullable=False),
        sa.Column("schema_type", sa.String(length=50), nullable=False),
        sa.Column("definition_json", json_type, nullable=True),
        sa.Column("definition_text", sa.Text(), nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("schema_key", "version", name="uq_configuration_validation_schemas_key_version"),
        schema=schema,
    )
    op.create_table(
        "transformation_definitions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("transformation_key", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("source_format", sa.String(length=32), nullable=False),
        sa.Column("target_format", sa.String(length=32), nullable=False),
        sa.Column("engine_type", sa.String(length=50), nullable=False),
        sa.Column("definition", json_type, nullable=True),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.UniqueConstraint("transformation_key", "version", name="uq_configuration_transformations_key_version"),
        schema=schema,
    )
    op.create_table(
        "retry_policies",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("strategy", sa.String(length=50), nullable=False),
        sa.Column("initial_delay_seconds", sa.Integer(), nullable=False),
        sa.Column("max_delay_seconds", sa.Integer(), nullable=False),
        sa.Column("multiplier", sa.Integer(), nullable=False),
        sa.Column("retryable_errors", json_type, nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.CheckConstraint("max_attempts >= 1", name="ck_configuration_retry_max_attempts"),
        sa.CheckConstraint("initial_delay_seconds >= 0", name="ck_configuration_retry_initial_delay"),
        sa.CheckConstraint("max_delay_seconds >= initial_delay_seconds", name="ck_configuration_retry_max_delay"),
        sa.CheckConstraint("multiplier >= 1", name="ck_configuration_retry_multiplier"),
        sa.UniqueConstraint("name", name="uq_configuration_retry_policies_name"),
        schema=schema,
    )
    op.create_table(
        "integration_routes",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("route_key", sa.String(length=128), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("source_system_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("target_system_id", sa.String(length=36), nullable=False),
        sa.Column("http_method", sa.String(length=16), nullable=False),
        sa.Column("target_endpoint", sa.String(length=500), nullable=False),
        sa.Column("source_format", sa.String(length=32), nullable=False),
        sa.Column("target_format", sa.String(length=32), nullable=False),
        sa.Column("validation_schema_id", sa.String(length=36), nullable=True),
        sa.Column("transformation_id", sa.String(length=36), nullable=True),
        sa.Column("retry_policy_id", sa.String(length=36), nullable=False),
        sa.Column("delivery_mode", sa.String(length=50), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["source_system_id"], [_reference(schema, "external_systems")]),
        sa.ForeignKeyConstraint(["target_system_id"], [_reference(schema, "external_systems")]),
        sa.ForeignKeyConstraint(["validation_schema_id"], [_reference(schema, "validation_schemas")]),
        sa.ForeignKeyConstraint(["transformation_id"], [_reference(schema, "transformation_definitions")]),
        sa.ForeignKeyConstraint(["retry_policy_id"], [_reference(schema, "retry_policies")]),
        sa.UniqueConstraint("route_key", "version", name="uq_configuration_routes_key_version"),
        schema=schema,
    )
    op.create_index("ix_configuration_routes_enabled_source_event", "integration_routes", ["source_system_id", "event_type"], unique=False, schema=schema, **({"postgresql_where": sa.text("enabled")} if schema is not None else {}))

    # New fields are nullable to preserve M1--M5 historical route records.
    op.add_column("route_processing_results", sa.Column("route_id", sa.String(length=36), nullable=True))
    op.add_column("route_processing_results", sa.Column("transformation_id", sa.String(length=36), nullable=True))
    op.add_column("route_processing_results", sa.Column("transformation_version", sa.Integer(), nullable=True))
    op.add_column("route_processing_results", sa.Column("validation_schema_key", sa.String(length=128), nullable=True))
    op.add_column("route_processing_results", sa.Column("validation_schema_version", sa.Integer(), nullable=True))

    # Existing deliveries retain a concrete transport snapshot. New M6 rows
    # fill these values from ExternalSystem instead of service environment.
    op.add_column("deliveries", sa.Column("connect_timeout_ms", sa.Integer(), nullable=False, server_default="5000"))
    op.add_column("deliveries", sa.Column("read_timeout_ms", sa.Integer(), nullable=False, server_default="5000"))


def downgrade() -> None:
    schema = _schema_name()
    op.drop_column("deliveries", "read_timeout_ms")
    op.drop_column("deliveries", "connect_timeout_ms")
    op.drop_column("route_processing_results", "validation_schema_version")
    op.drop_column("route_processing_results", "validation_schema_key")
    op.drop_column("route_processing_results", "transformation_version")
    op.drop_column("route_processing_results", "transformation_id")
    op.drop_column("route_processing_results", "route_id")
    op.drop_index("ix_configuration_routes_enabled_source_event", table_name="integration_routes", schema=schema)
    op.drop_table("integration_routes", schema=schema)
    op.drop_table("retry_policies", schema=schema)
    op.drop_table("transformation_definitions", schema=schema)
    op.drop_table("validation_schemas", schema=schema)
    op.drop_table("external_systems", schema=schema)
    op.drop_table("auth_profiles", schema=schema)
    if schema is not None:
        op.execute(f"DROP SCHEMA IF EXISTS {schema}")
