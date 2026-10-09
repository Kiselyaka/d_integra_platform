"""Allow one processing result per matched route for a Message."""

from alembic import op


revision = "20261009_0003"
down_revision = "20261004_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_route_processing_results_message_route",
        "route_processing_results",
        ["message_id", "route_key", "route_version"],
    )


def downgrade() -> None:
    op.drop_constraint(
        "uq_route_processing_results_message_route",
        "route_processing_results",
        type_="unique",
    )
