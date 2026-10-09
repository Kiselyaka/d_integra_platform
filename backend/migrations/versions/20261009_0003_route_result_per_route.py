"""Allow one processing result per matched route for a Message."""

from alembic import op


revision = "20261009_0003"
down_revision = "20261004_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # SQLite needs table recreation for an ALTER TABLE constraint.  Batch mode
    # keeps this migration executable both in production PostgreSQL and clean
    # in-memory SQLite migration tests.
    with op.batch_alter_table("route_processing_results") as batch_op:
        batch_op.create_unique_constraint(
            "uq_route_processing_results_message_route",
            ["message_id", "route_key", "route_version"],
        )


def downgrade() -> None:
    with op.batch_alter_table("route_processing_results") as batch_op:
        batch_op.drop_constraint("uq_route_processing_results_message_route", type_="unique")
