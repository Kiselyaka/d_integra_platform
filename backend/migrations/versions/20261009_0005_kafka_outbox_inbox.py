"""Add M5 transactional Outbox/Inbox persistence."""

from alembic import op
import sqlalchemy as sa


revision = "20261009_0005"
down_revision = "20261009_0004"
branch_labels = None
depends_on = None


def _create_outbox(name: str, unique_prefix: str, include_delivery_id: bool = False) -> None:
    columns = [
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("aggregate_id", sa.String(length=36), nullable=False),
    ]
    if include_delivery_id:
        columns.append(sa.Column("delivery_id", sa.String(length=36), nullable=False))
    columns.extend(
        [
            sa.Column("topic", sa.String(length=255), nullable=False),
            sa.Column("message_key", sa.String(length=36), nullable=False),
            sa.Column("envelope", sa.JSON(), nullable=False),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
            sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("publish_attempt_count", sa.Integer(), nullable=False, server_default="0"),
            sa.Column("last_publish_error", sa.Text(), nullable=True),
            sa.Column("publish_lease_token", sa.String(length=36), nullable=True),
            sa.Column("publish_lease_until", sa.DateTime(timezone=True), nullable=True),
            sa.UniqueConstraint("event_id", name=f"uq_{unique_prefix}_event_id"),
        ]
    )
    if include_delivery_id:
        columns.append(sa.UniqueConstraint("delivery_id", "event_type", name="uq_delivery_outbox_delivery_event_type"))
    op.create_table(name, *columns)
    op.create_index(
        f"ix_{unique_prefix}_unpublished",
        name,
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )


def _create_inbox(name: str, unique_prefix: str) -> None:
    op.create_table(
        name,
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("consumer_name", sa.String(length=128), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.UniqueConstraint("consumer_name", "event_id", name=f"uq_{unique_prefix}_consumer_event"),
    )
    op.create_index(f"ix_{unique_prefix}_consumer_name", name, ["consumer_name"])


def upgrade() -> None:
    # Existing M1--M4 tables are in PostgreSQL public. Context-prefixed table
    # names preserve ownership without moving historical tables across schemas.
    _create_outbox("processing_outbox_events", "processing_outbox_events")
    _create_inbox("processing_inbox_messages", "processing_inbox")
    op.create_table(
        "processing_delivery_completions",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("message_id", sa.String(length=36), nullable=False),
        sa.Column("route_processing_result_id", sa.String(length=36), nullable=False),
        sa.Column("delivery_id", sa.String(length=36), nullable=False),
        sa.Column("terminal_status", sa.String(length=32), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("route_processing_result_id", name="uq_processing_completion_route_result"),
        sa.UniqueConstraint("delivery_id", name="uq_processing_completion_delivery"),
    )
    op.create_index("ix_processing_completion_message_id", "processing_delivery_completions", ["message_id"])
    _create_outbox("delivery_outbox_events", "delivery_outbox_events", include_delivery_id=True)
    _create_inbox("delivery_inbox_messages", "delivery_inbox")
    op.add_column("deliveries", sa.Column("correlation_id", sa.String(length=255), nullable=True))


def downgrade() -> None:
    op.drop_column("deliveries", "correlation_id")
    op.drop_index("ix_delivery_inbox_consumer_name", table_name="delivery_inbox_messages")
    op.drop_table("delivery_inbox_messages")
    op.drop_index("ix_delivery_outbox_events_unpublished", table_name="delivery_outbox_events")
    op.drop_table("delivery_outbox_events")
    op.drop_index("ix_processing_completion_message_id", table_name="processing_delivery_completions")
    op.drop_table("processing_delivery_completions")
    op.drop_index("ix_processing_inbox_consumer_name", table_name="processing_inbox_messages")
    op.drop_table("processing_inbox_messages")
    op.drop_index("ix_processing_outbox_events_unpublished", table_name="processing_outbox_events")
    op.drop_table("processing_outbox_events")
