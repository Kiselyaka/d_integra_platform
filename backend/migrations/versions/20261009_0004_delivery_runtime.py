"""Create Delivery and DeliveryAttempt runtime tables for M4."""

from alembic import op
import sqlalchemy as sa


revision = "20261009_0004"
down_revision = "20261009_0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "deliveries",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("message_id", sa.String(length=36), nullable=False),
        sa.Column("route_processing_result_id", sa.String(length=36), nullable=False),
        sa.Column("route_key", sa.String(length=128), nullable=False),
        sa.Column("route_version", sa.Integer(), nullable=False),
        sa.Column("target_system", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("payload_format", sa.String(length=128), nullable=False),
        sa.Column("transformed_payload", sa.Text(), nullable=False),
        sa.Column("target_url", sa.Text(), nullable=False),
        sa.Column("http_method", sa.String(length=16), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("retry_strategy", sa.String(length=32), nullable=False),
        sa.Column("retry_initial_delay_seconds", sa.Integer(), nullable=False),
        sa.Column("retry_max_delay_seconds", sa.Integer(), nullable=False),
        sa.Column("retry_multiplier", sa.Integer(), nullable=False),
        sa.Column("retryable_errors", sa.Text(), nullable=False),
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_category", sa.String(length=64), nullable=True),
        sa.Column("last_error_code", sa.String(length=128), nullable=True),
        sa.Column("last_error_message", sa.Text(), nullable=True),
        sa.Column("dead_letter_reason", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lock_version", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("idempotency_key", sa.String(length=36), nullable=False),
        sa.UniqueConstraint("route_processing_result_id", name="uq_deliveries_route_processing_result"),
        sa.CheckConstraint("attempt_count >= 0", name="ck_deliveries_attempt_count_nonnegative"),
        sa.CheckConstraint("max_attempts >= 1", name="ck_deliveries_max_attempts_positive"),
    )
    op.create_index("ix_deliveries_message_id", "deliveries", ["message_id"])
    op.create_index("ix_deliveries_target_system_status", "deliveries", ["target_system", "status"])
    op.create_index("ix_deliveries_retry_wait_due", "deliveries", ["next_retry_at"])
    op.create_index("ix_deliveries_processing_started", "deliveries", ["processing_started_at"])
    op.create_table(
        "delivery_attempts",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("delivery_id", sa.String(length=36), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("http_status", sa.Integer(), nullable=True),
        sa.Column("error_category", sa.String(length=64), nullable=True),
        sa.Column("error_code", sa.String(length=128), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
        sa.ForeignKeyConstraint(["delivery_id"], ["deliveries.id"]),
        sa.UniqueConstraint("delivery_id", "attempt_number", name="uq_delivery_attempts_delivery_number"),
    )
    op.create_index("ix_delivery_attempts_delivery_id", "delivery_attempts", ["delivery_id"])


def downgrade() -> None:
    op.drop_index("ix_delivery_attempts_delivery_id", table_name="delivery_attempts")
    op.drop_table("delivery_attempts")
    op.drop_index("ix_deliveries_processing_started", table_name="deliveries")
    op.drop_index("ix_deliveries_retry_wait_due", table_name="deliveries")
    op.drop_index("ix_deliveries_target_system_status", table_name="deliveries")
    op.drop_index("ix_deliveries_message_id", table_name="deliveries")
    op.drop_table("deliveries")
