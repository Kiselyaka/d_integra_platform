"""Repair the short-lived M7 migration guard for JSON `null` M6 definitions."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from integration_api.configuration.demo_definitions import FIELD_MAPPING_V1_DEFINITIONS


revision = "20261010_0008"
down_revision = "20261010_0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    schema = "configuration" if op.get_bind().dialect.name == "postgresql" else None
    definition_type = postgresql.JSONB(astext_type=sa.Text()) if schema else sa.JSON()
    table = sa.table(
        "transformation_definitions",
        sa.column("transformation_key", sa.String()),
        sa.column("engine_type", sa.String()),
        sa.column("definition", definition_type),
        schema=schema,
    )
    empty_definition = sa.or_(table.c.definition.is_(None), sa.cast(table.c.definition, sa.Text) == "null")
    for key, definition in FIELD_MAPPING_V1_DEFINITIONS.items():
        op.get_bind().execute(
            table.update()
            .where(table.c.transformation_key == key)
            .where(table.c.engine_type == "BUILTIN")
            .where(empty_definition)
            .values(engine_type="FIELD_MAPPING_V1", definition=definition)
        )


def downgrade() -> None:
    # 0008 only repairs a guard in 0007; it has no independent reversible state.
    pass
