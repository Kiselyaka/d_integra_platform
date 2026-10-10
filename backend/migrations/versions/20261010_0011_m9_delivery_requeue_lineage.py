"""Add immutable Delivery recovery lineage for M9 manual requeue.

The old global route_processing_result uniqueness modelled one *initial*
Delivery.  Recovery executions intentionally reuse that historical snapshot,
so the uniqueness becomes partial: one initial Delivery per RPR, plus one
direct child per source Delivery.
"""

from alembic import op
import sqlalchemy as sa


revision = "20261010_0011"
down_revision = "20261010_0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    operations = op.batch_alter_table("deliveries") if op.get_bind().dialect.name == "sqlite" else None
    if operations is not None:
        with operations as batch:
            batch.add_column(sa.Column("requeued_from_delivery_id", sa.String(length=36), nullable=True))
            batch.drop_constraint("uq_deliveries_route_processing_result", type_="unique")
            batch.create_foreign_key(
                "fk_deliveries_requeued_from_delivery", "deliveries", ["requeued_from_delivery_id"], ["id"], ondelete="RESTRICT"
            )
            batch.create_unique_constraint("uq_deliveries_one_requeue_per_source", ["requeued_from_delivery_id"])
    else:
        op.add_column("deliveries", sa.Column("requeued_from_delivery_id", sa.String(length=36), nullable=True))
        op.create_foreign_key(
            "fk_deliveries_requeued_from_delivery",
            "deliveries",
            "deliveries",
            ["requeued_from_delivery_id"],
            ["id"],
            ondelete="RESTRICT",
        )
        op.drop_constraint("uq_deliveries_route_processing_result", "deliveries", type_="unique")
        op.create_unique_constraint("uq_deliveries_one_requeue_per_source", "deliveries", ["requeued_from_delivery_id"])
    op.create_index(
        "uq_deliveries_initial_route_processing_result",
        "deliveries",
        ["route_processing_result_id"],
        unique=True,
        postgresql_where=sa.text("requeued_from_delivery_id IS NULL"),
    )
    op.create_index("ix_deliveries_requeued_from_delivery_id", "deliveries", ["requeued_from_delivery_id"])
    op.create_index("ix_deliveries_created_at_id", "deliveries", ["created_at", "id"])
    op.create_index("ix_deliveries_correlation_id", "deliveries", ["correlation_id"])
    op.create_index("ix_deliveries_status_created_at_id", "deliveries", ["status", "created_at", "id"])
    op.create_index("ix_messages_received_at_id", "messages", ["received_at", "id"])
    op.create_index("ix_messages_correlation_id", "messages", ["correlation_id"])
    op.create_index("ix_messages_source_system_event_type", "messages", ["source_system", "event_type"])
    op.create_index("ix_messages_external_event_id", "messages", ["external_event_id"])


def downgrade() -> None:
    op.drop_index("ix_messages_external_event_id", table_name="messages")
    op.drop_index("ix_messages_source_system_event_type", table_name="messages")
    op.drop_index("ix_messages_correlation_id", table_name="messages")
    op.drop_index("ix_messages_received_at_id", table_name="messages")
    op.drop_index("ix_deliveries_status_created_at_id", table_name="deliveries")
    op.drop_index("ix_deliveries_correlation_id", table_name="deliveries")
    op.drop_index("ix_deliveries_created_at_id", table_name="deliveries")
    op.drop_index("ix_deliveries_requeued_from_delivery_id", table_name="deliveries")
    op.drop_index("uq_deliveries_initial_route_processing_result", table_name="deliveries")
    operations = op.batch_alter_table("deliveries") if op.get_bind().dialect.name == "sqlite" else None
    if operations is not None:
        with operations as batch:
            batch.drop_constraint("uq_deliveries_one_requeue_per_source", type_="unique")
            batch.drop_constraint("fk_deliveries_requeued_from_delivery", type_="foreignkey")
            batch.drop_column("requeued_from_delivery_id")
            batch.create_unique_constraint("uq_deliveries_route_processing_result", ["route_processing_result_id"])
    else:
        op.drop_constraint("uq_deliveries_one_requeue_per_source", "deliveries", type_="unique")
        op.create_unique_constraint("uq_deliveries_route_processing_result", "deliveries", ["route_processing_result_id"])
        op.drop_constraint("fk_deliveries_requeued_from_delivery", "deliveries", type_="foreignkey")
        op.drop_column("requeued_from_delivery_id")
