"""Create a minimal platform-owned service state table."""

from alembic import op
import sqlalchemy as sa

revision = "20260919_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "platform_service_state",
        sa.Column("service_name", sa.String(length=64), primary_key=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("CURRENT_TIMESTAMP")),
    )


def downgrade() -> None:
    op.drop_table("platform_service_state")

