"""Preserve ReservationRequest element order after JSONB persistence."""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from integration_api.configuration.demo_definitions import (
    FIELD_MAPPING_V1_DEFINITIONS,
    INITIAL_FIELD_MAPPING_V1_WAREHOUSE_DEFINITION,
)


revision = "20261010_0009"
down_revision = "20261010_0008"
branch_labels = None
depends_on = None


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
    """Upgrade only the exact early-M7 demo definition, not operator edits."""

    table = _table()
    op.get_bind().execute(
        table.update()
        .where(table.c.transformation_key == "order-created-to-warehouse-reservation")
        .where(table.c.engine_type == "FIELD_MAPPING_V1")
        .where(table.c.definition == INITIAL_FIELD_MAPPING_V1_WAREHOUSE_DEFINITION)
        .values(definition=FIELD_MAPPING_V1_DEFINITIONS["order-created-to-warehouse-reservation"])
    )


def downgrade() -> None:
    table = _table()
    op.get_bind().execute(
        table.update()
        .where(table.c.transformation_key == "order-created-to-warehouse-reservation")
        .where(table.c.engine_type == "FIELD_MAPPING_V1")
        .where(table.c.definition == FIELD_MAPPING_V1_DEFINITIONS["order-created-to-warehouse-reservation"])
        .values(definition=INITIAL_FIELD_MAPPING_V1_WAREHOUSE_DEFINITION)
    )
