"""Initial schema — tasks, attempts, effects, workflows.

Revision ID: 001
Revises: None
Create Date: 2024-01-01
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision: str = "001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # task_state enum
    # Create the enum type once, explicitly. The column below then references it
    # with create_type=False -- without that, op.create_table emits a second
    # CREATE TYPE and the migration aborts with DuplicateObjectError.
    task_state = postgresql.ENUM(
        "pending", "scheduled", "leased", "succeeded", "failed", "dead", "cancelled",
        name="task_state",
        create_type=False,
    )
    sa.Enum(
        "pending", "scheduled", "leased", "succeeded", "failed", "dead", "cancelled",
        name="task_state",
    ).create(op.get_bind(), checkfirst=True)

    # workflows table (referenced by tasks)
    op.create_table(
        "workflows",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("name", sa.Text, nullable=False),
        sa.Column("spec", JSONB, nullable=False),
        sa.Column("state", sa.Text, nullable=False, server_default="running"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    # tasks table
    op.create_table(
        "tasks",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column("queue", sa.Text, nullable=False),
        sa.Column("task_name", sa.Text, nullable=False),
        sa.Column("payload", JSONB, nullable=False),
        sa.Column("state", task_state, nullable=False, server_default="pending"),
        sa.Column("priority", sa.SmallInteger, nullable=False, server_default="0"),
        sa.Column("attempt", sa.Integer, nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer, nullable=False, server_default="5"),
        sa.Column("dedup_key", sa.Text, nullable=True),
        sa.Column("run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("workflow_id", UUID(as_uuid=True), sa.ForeignKey("workflows.id"), nullable=True),
        sa.Column("step_name", sa.Text, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )
    op.create_index(
        "tasks_dedup", "tasks", ["queue", "dedup_key"],
        unique=True, postgresql_where=sa.text("dedup_key IS NOT NULL"),
    )
    op.create_index("tasks_state_queue", "tasks", ["state", "queue", "priority", "created_at"])

    # attempts table
    op.create_table(
        "attempts",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("task_id", UUID(as_uuid=True), sa.ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("attempt_no", sa.Integer, nullable=False),
        sa.Column("worker_id", sa.Text, nullable=False),
        sa.Column("fence", sa.BigInteger, nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome", sa.Text, nullable=True),
        sa.Column("error_type", sa.Text, nullable=True),
        sa.Column("error_repr", sa.Text, nullable=True),
        sa.UniqueConstraint("task_id", "attempt_no"),
    )

    # effects table — the exactly-once-effect table
    op.create_table(
        "effects",
        sa.Column("dedup_key", sa.Text, primary_key=True),
        sa.Column("task_id", UUID(as_uuid=True), nullable=False),
        sa.Column("fence", sa.BigInteger, nullable=False),
        sa.Column("result", JSONB, nullable=True),
        sa.Column("committed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
    )


def downgrade() -> None:
    op.drop_table("effects")
    op.drop_table("attempts")
    op.drop_table("tasks")
    op.drop_table("workflows")
    sa.Enum(name="task_state").drop(op.get_bind(), checkfirst=True)
