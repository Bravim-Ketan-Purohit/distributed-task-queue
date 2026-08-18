"""SQLAlchemy table definitions — mirrors SPEC §5."""

from __future__ import annotations

import uuid

from sqlalchemy import (
    BigInteger,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    SmallInteger,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class TaskRow(Base):
    __tablename__ = "tasks"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    queue: Mapped[str] = mapped_column(Text, nullable=False)
    task_name: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(
        Enum(
            "pending", "scheduled", "leased", "succeeded", "failed", "dead", "cancelled",
            name="task_state",
            create_constraint=True,
        ),
        nullable=False,
        default="pending",
    )
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=5)
    dedup_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    run_at: Mapped[str | None] = mapped_column(DateTime(timezone=True), nullable=True)
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    step_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    attempts: Mapped[list["AttemptRow"]] = relationship(back_populates="task", cascade="all, delete-orphan")

    __table_args__ = (
        Index("tasks_dedup", "queue", "dedup_key", unique=True, postgresql_where="dedup_key IS NOT NULL"),
        Index("tasks_state_queue", "state", "queue", "priority", "created_at"),
    )


class AttemptRow(Base):
    __tablename__ = "attempts"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    # The migration declares this FK; the ORM must declare it too, or SQLAlchemy
    # cannot infer the join for TaskRow.attempts and *every* mapper fails to
    # configure -- which made every control-plane request return HTTP 500.
    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tasks.id", ondelete="CASCADE"), nullable=False
    )
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False)
    fence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    started_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    finished_at: Mapped[str | None] = mapped_column(DateTime(timezone=True), nullable=True)
    outcome: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_repr: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped[TaskRow] = relationship(back_populates="attempts")

    __table_args__ = (
        UniqueConstraint("task_id", "attempt_no"),
        {"comment": "Individual execution attempts of a task"},
    )


class EffectRow(Base):
    """The exactly-once-effect table.

    Effect + this row commit together in the same transaction, or neither commits.
    The fence comparison stops a zombie worker from committing stale work.
    """

    __tablename__ = "effects"

    dedup_key: Mapped[str] = mapped_column(Text, primary_key=True)
    task_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    fence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    result: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    committed_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class WorkflowRow(Base):
    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    spec: Mapped[dict] = mapped_column(JSONB, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False, default="running")
    created_at: Mapped[str] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
