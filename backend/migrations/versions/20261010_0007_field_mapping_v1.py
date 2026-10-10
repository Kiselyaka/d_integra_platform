"""Migrate the original M6 demo definitions to FIELD_MAPPING_V1."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from integration_api.configuration.demo_definitions import FIELD_MAPPING_V1_DEFINITIONS


revision = "20261010_0007"
down_revision = "20261010_0006"
branch_labels = None
depends_on = None


_DEMO_KEYS = tuple(FIELD_MAPPING_V1_DEFINITIONS)


def _table() -> sa.TableClause:
    schema = "configuration" if op.get_bind().dialect.name == "postgresql" else None
    definition_type = postgresql.JSONB(astext_type=sa.Text()) if schema else sa.JSON()
    return sa.table(
        "transformation_definitions",
        sa.column("transformation_key", sa.String()),
        sa.column("engine_type", sa.String()),
        sa.column("definition", definition_type),
        schema=schema,
    )


def upgrade() -> None:
    """Keep stable ids/keys/versions and upgrade only untouched M6 demo rows."""

    table = _table()
    connection = op.get_bind()
    for key, definition in FIELD_MAPPING_V1_DEFINITIONS.items():
        connection.execute(
            table.update()
            .where(table.c.transformation_key == key)
            .where(table.c.engine_type == "BUILTIN")
            .where(_is_empty_legacy_definition(table))
            .values(engine_type="FIELD_MAPPING_V1", definition=definition)
        )


def _is_empty_legacy_definition(table: sa.TableClause) -> sa.ColumnElement[bool]:
    """M6 SQLAlchemy JSON persisted Python None as JSON `null`, not SQL NULL."""

    return sa.or_(table.c.definition.is_(None), sa.cast(table.c.definition, sa.Text) == "null")


def downgrade() -> None:
    table = _table()
    op.get_bind().execute(
        table.update()
        .where(table.c.transformation_key.in_(_DEMO_KEYS))
        .where(table.c.engine_type == "FIELD_MAPPING_V1")
        .values(engine_type="BUILTIN", definition=None)
    )
