"""Create minimal Message and RouteProcessingResult runtime tables for M1."""

from alembic import op
import sqlalchemy as sa

revision = "20261004_0002"
down_revision = "20260919_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "messages",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("external_event_id", sa.String(length=255), nullable=False),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("correlation_id", sa.String(length=255), nullable=True),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("validation_error", sa.Text(), nullable=True),
        sa.UniqueConstraint("source_system", "external_event_id", name="uq_messages_source_external_event"),
    )
    op.create_table(
        "route_processing_results",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("message_id", sa.String(length=36), nullable=False),
        sa.Column("route_key", sa.String(length=128), nullable=False),
        sa.Column("route_version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_route_processing_results_message_id", "route_processing_results", ["message_id"])


def downgrade() -> None:
    op.drop_index("ix_route_processing_results_message_id", table_name="route_processing_results")
    op.drop_table("route_processing_results")
    op.drop_table("messages")
