"""Add M8 configuration lifecycle and validation history."""

from alembic import op
import sqlalchemy as sa


revision = "20261010_0010"
down_revision = "20261010_0009"
branch_labels = None
depends_on = None


SCHEMA = "configuration"
KNOWN_TRANSFORMATIONS = (
    "order-created-to-crm",
    "order-created-to-warehouse-reservation",
    "warehouse-reservation-succeeded-to-mini-shop",
    "warehouse-reservation-failed-to-mini-shop",
)
KNOWN_ROUTES = (
    "mini-shop-order-created-to-crm",
    "mini-shop-order-created-to-warehouse",
    "warehouse-reservation-succeeded-to-mini-shop",
    "warehouse-reservation-failed-to-mini-shop",
)


def _schema() -> str | None:
    return SCHEMA if op.get_bind().dialect.name == "postgresql" else None


def _table(name: str) -> str:
    return f"{SCHEMA}.{name}" if _schema() else name


def _where(expression: str) -> dict[str, object]:
    dialect = op.get_bind().dialect.name
    if dialect == "postgresql":
        return {"postgresql_where": sa.text(expression)}
    if dialect == "sqlite":
        return {"sqlite_where": sa.text(expression)}
    return {}


def _lifecycle_columns(table: str) -> None:
    op.add_column(table, sa.Column("status", sa.String(length=16), nullable=False, server_default="DRAFT"), schema=_schema())
    op.add_column(table, sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True), schema=_schema())
    op.add_column(table, sa.Column("retired_at", sa.DateTime(timezone=True), nullable=True), schema=_schema())


def upgrade() -> None:
    schema = _schema()
    _lifecycle_columns("validation_schemas")
    _lifecycle_columns("transformation_definitions")
    _lifecycle_columns("integration_routes")

    op.add_column("integration_routes", sa.Column("source_validation_schema_id", sa.String(length=36), nullable=True), schema=schema)
    op.add_column("integration_routes", sa.Column("target_validation_schema_id", sa.String(length=36), nullable=True), schema=schema)
    if schema is not None:
        op.create_foreign_key("fk_routes_source_validation_schema", "integration_routes", "validation_schemas", ["source_validation_schema_id"], ["id"], source_schema=schema, referent_schema=schema)
        op.create_foreign_key("fk_routes_target_validation_schema", "integration_routes", "validation_schemas", ["target_validation_schema_id"], ["id"], source_schema=schema, referent_schema=schema)

    # A legacy singular reference has source semantics if a deployment used it.
    op.execute(f"UPDATE {_table('integration_routes')} SET source_validation_schema_id = validation_schema_id WHERE validation_schema_id IS NOT NULL")

    for column, type_ in (
        ("source_validation_schema_id", sa.String(length=36)),
        ("source_validation_schema_key", sa.String(length=128)),
        ("source_validation_schema_version", sa.Integer()),
        ("target_validation_schema_id", sa.String(length=36)),
        ("target_validation_schema_key", sa.String(length=128)),
        ("target_validation_schema_version", sa.Integer()),
        ("error_stage", sa.String(length=64)),
        ("error_category", sa.String(length=128)),
        ("error_details", sa.JSON()),
    ):
        op.add_column("route_processing_results", sa.Column(column, type_, nullable=True))

    quoted_transformations = ", ".join(f"'{key}'" for key in KNOWN_TRANSFORMATIONS)
    quoted_routes = ", ".join(f"'{key}'" for key in KNOWN_ROUTES)
    # Only the stable M6/M7 bootstrap identities become ACTIVE automatically.
    op.execute(
        f"UPDATE {_table('transformation_definitions')} SET status = 'ACTIVE', activated_at = CURRENT_TIMESTAMP "
        f"WHERE version = 1 AND enabled = true AND transformation_key IN ({quoted_transformations})"
    )
    op.execute(
        f"UPDATE {_table('integration_routes')} SET status = 'ACTIVE', activated_at = CURRENT_TIMESTAMP "
        f"WHERE version = 1 AND enabled = true AND route_key IN ({quoted_routes})"
    )

    op.drop_index("ix_configuration_routes_enabled_source_event", table_name="integration_routes", schema=schema)
    op.create_index(
        "ix_configuration_routes_active_source_event", "integration_routes", ["source_system_id", "event_type"],
        schema=schema, **_where("status = 'ACTIVE' AND enabled"),
    )
    op.create_index(
        "uq_configuration_routes_one_active_version", "integration_routes", ["route_key"], unique=True,
        schema=schema, **_where("status = 'ACTIVE'"),
    )
    op.create_index(
        "uq_configuration_transformations_one_active_version", "transformation_definitions", ["transformation_key"], unique=True,
        schema=schema, **_where("status = 'ACTIVE'"),
    )
    op.create_index(
        "uq_configuration_schemas_one_active_version", "validation_schemas", ["schema_key"], unique=True,
        schema=schema, **_where("status = 'ACTIVE'"),
    )


def downgrade() -> None:
    schema = _schema()
    op.drop_index("uq_configuration_schemas_one_active_version", table_name="validation_schemas", schema=schema)
    op.drop_index("uq_configuration_transformations_one_active_version", table_name="transformation_definitions", schema=schema)
    op.drop_index("uq_configuration_routes_one_active_version", table_name="integration_routes", schema=schema)
    op.drop_index("ix_configuration_routes_active_source_event", table_name="integration_routes", schema=schema)
    op.create_index("ix_configuration_routes_enabled_source_event", "integration_routes", ["source_system_id", "event_type"], schema=schema, **_where("enabled"))
    for column in ("error_details", "error_category", "error_stage", "target_validation_schema_version", "target_validation_schema_key", "target_validation_schema_id", "source_validation_schema_version", "source_validation_schema_key", "source_validation_schema_id"):
        op.drop_column("route_processing_results", column)
    if schema is not None:
        op.drop_constraint("fk_routes_target_validation_schema", "integration_routes", schema=schema, type_="foreignkey")
        op.drop_constraint("fk_routes_source_validation_schema", "integration_routes", schema=schema, type_="foreignkey")
    op.drop_column("integration_routes", "target_validation_schema_id", schema=schema)
    op.drop_column("integration_routes", "source_validation_schema_id", schema=schema)
    for table in ("integration_routes", "transformation_definitions", "validation_schemas"):
        for column in ("retired_at", "activated_at", "status"):
            op.drop_column(table, column, schema=schema)
