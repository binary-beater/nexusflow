from datetime import datetime
from uuid import UUID

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JSONB_TYPE = JSONB(none_as_null=True)
TIMESTAMP_TYPE = TIMESTAMP(timezone=True)


class Base(DeclarativeBase):
    pass


class RegisteredDefinitionRecord(Base):
    __tablename__ = "registered_definitions"

    definition_id: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_name: Mapped[str] = mapped_column(Text, nullable=False)
    validated_iws: Mapped[dict] = mapped_column(JSONB_TYPE, nullable=False)
    raw_yaml: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        CheckConstraint("length(trim(workflow_name)) > 0", name="chk_definitions_name_non_empty"),
    )


class WorkflowExecutionRecord(Base):
    __tablename__ = "workflow_executions"

    workflow_execution_id: Mapped[UUID] = mapped_column(primary_key=True)
    definition_id: Mapped[UUID] = mapped_column(
        ForeignKey("registered_definitions.definition_id", ondelete="RESTRICT"),
        nullable=False,
    )
    state: Mapped[str] = mapped_column(Text, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    workflow_input: Mapped[dict] = mapped_column(JSONB_TYPE, nullable=False)
    has_output: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    workflow_output: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    failure_category: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_details: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)
    updated_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        CheckConstraint(
            "state IN ('INITIALIZING', 'RUNNING', 'FAILING', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_workflow_state",
        ),
        CheckConstraint("revision >= 1", name="chk_workflow_revision"),
        CheckConstraint(
            "(has_output = FALSE AND workflow_output IS NULL) OR (has_output = TRUE AND workflow_output IS NOT NULL)",
            name="chk_workflow_output_consistency",
        ),
        CheckConstraint(
            "state != 'SUCCEEDED' OR (has_output = TRUE)",
            name="chk_workflow_terminal_success",
        ),
        Index("idx_workflow_executions_state_created", "state", "created_at_utc", "workflow_execution_id"),
        Index("idx_workflow_executions_definition", "definition_id", "created_at_utc", "workflow_execution_id"),
    )


class TaskExecutionRecord(Base):
    __tablename__ = "task_executions"

    task_execution_id: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_execution_id: Mapped[UUID] = mapped_column(
        ForeignKey("workflow_executions.workflow_execution_id", ondelete="RESTRICT"),
        nullable=False,
    )
    task_definition_id: Mapped[str] = mapped_column(Text, nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    has_input: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stable_input: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    has_output: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    task_output: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    next_attempt_ordinal: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    retry_ready_at_utc: Mapped[datetime | None] = mapped_column(TIMESTAMP_TYPE, nullable=True)
    failure_category: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_details: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)
    updated_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        UniqueConstraint("workflow_execution_id", "task_definition_id", name="uq_task_definition_per_workflow"),
        CheckConstraint(
            "state IN ('PENDING', 'RUNNABLE', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_task_state",
        ),
        CheckConstraint("revision >= 1", name="chk_task_revision"),
        CheckConstraint("max_attempts >= 1", name="chk_task_max_attempts"),
        CheckConstraint("next_attempt_ordinal >= 1", name="chk_task_next_ordinal"),
        CheckConstraint(
            "(has_input = FALSE AND stable_input IS NULL) OR (has_input = TRUE AND stable_input IS NOT NULL)",
            name="chk_task_input_presence",
        ),
        CheckConstraint(
            "(has_output = FALSE AND task_output IS NULL) OR (has_output = TRUE AND task_output IS NOT NULL)",
            name="chk_task_output_consistency",
        ),
        CheckConstraint("state != 'SUCCEEDED' OR (has_output = TRUE)", name="chk_task_terminal_success"),
        Index("idx_task_executions_workflow_id", "workflow_execution_id"),
        Index(
            "idx_task_executions_runnable_rediscovery",
            "created_at_utc",
            "task_execution_id",
            postgresql_where=(state == "RUNNABLE"),
        ),
        Index(
            "idx_task_executions_retry_timer",
            "retry_ready_at_utc",
            "task_execution_id",
            postgresql_where=(state == "RETRY_WAIT"),
        ),
    )


class ExecutionAttemptRecord(Base):
    __tablename__ = "execution_attempts"

    attempt_id: Mapped[UUID] = mapped_column(primary_key=True)
    task_execution_id: Mapped[UUID] = mapped_column(
        ForeignKey("task_executions.task_execution_id", ondelete="RESTRICT"),
        nullable=False,
    )
    attempt_ordinal: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_session_id: Mapped[UUID] = mapped_column(nullable=False)
    state: Mapped[str] = mapped_column(Text, nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    start_deadline_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)
    execution_timeout_utc: Mapped[datetime | None] = mapped_column(TIMESTAMP_TYPE, nullable=True)
    cancellation_deadline_utc: Mapped[datetime | None] = mapped_column(TIMESTAMP_TYPE, nullable=True)
    failure_category: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_code: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    failure_details: Mapped[dict | None] = mapped_column(JSONB_TYPE, nullable=True)
    created_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)
    updated_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        UniqueConstraint("task_execution_id", "attempt_ordinal", name="uq_attempt_ordinal_per_task"),
        CheckConstraint(
            "state IN ('CLAIMED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_attempt_state",
        ),
        CheckConstraint("attempt_ordinal >= 1", name="chk_attempt_ordinal"),
        CheckConstraint("revision >= 1", name="chk_attempt_revision"),
        Index(
            "uq_single_active_attempt_per_task",
            "task_execution_id",
            unique=True,
            postgresql_where=(state.in_(["CLAIMED", "RUNNING"])),
        ),
        Index(
            "idx_execution_attempts_start_deadline",
            "start_deadline_utc",
            "attempt_id",
            postgresql_where=(state == "CLAIMED"),
        ),
        Index(
            "idx_execution_attempts_execution_timeout",
            "execution_timeout_utc",
            "attempt_id",
            postgresql_where=(state == "RUNNING") & (execution_timeout_utc.isnot(None)),
        ),
        Index(
            "idx_execution_attempts_cancel_deadline",
            "cancellation_deadline_utc",
            "attempt_id",
            postgresql_where=cancellation_deadline_utc.isnot(None),
        ),
        Index(
            "idx_execution_attempts_worker_session",
            "worker_session_id",
            postgresql_where=state.in_(["CLAIMED", "RUNNING"]),
        ),
    )


class HistoryEntryRecord(Base):
    __tablename__ = "history_entries"

    history_id: Mapped[UUID] = mapped_column(primary_key=True)
    workflow_execution_id: Mapped[UUID] = mapped_column(
        ForeignKey("workflow_executions.workflow_execution_id", ondelete="RESTRICT"),
        nullable=False,
    )
    task_execution_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("task_executions.task_execution_id", ondelete="RESTRICT"),
        nullable=True,
    )
    attempt_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("execution_attempts.attempt_id", ondelete="RESTRICT"),
        nullable=True,
    )
    event_category: Mapped[str] = mapped_column(Text, nullable=False)
    event_payload: Mapped[dict] = mapped_column(JSONB_TYPE, nullable=False)
    occurred_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        Index("idx_history_entries_pagination", "workflow_execution_id", "occurred_at_utc", "history_id"),
    )


class IdempotencyRecord(Base):
    __tablename__ = "idempotency_records"

    operation_type: Mapped[str] = mapped_column(Text, primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_fingerprint: Mapped[str] = mapped_column(Text, nullable=False)
    resource_id: Mapped[UUID] = mapped_column(nullable=False)
    created_at_utc: Mapped[datetime] = mapped_column(TIMESTAMP_TYPE, nullable=False)

    __table_args__ = (
        CheckConstraint("operation_type IN ('REGISTER_DEFINITION', 'START_EXECUTION')", name="chk_idempotency_op_type"),
        CheckConstraint("length(request_fingerprint) = 64", name="chk_idempotency_fingerprint_len"),
    )
