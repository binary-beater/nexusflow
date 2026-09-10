"""Initial schema migration according to exact LLD-02 specification.

Revision ID: 20260909_0001
Revises: None
Create Date: 2026-09-09 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "20260909_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # 1. registered_definitions
    op.create_table(
        "registered_definitions",
        sa.Column("definition_id", sa.UUID(), nullable=False),
        sa.Column("workflow_name", sa.Text(), nullable=False),
        sa.Column("validated_iws", postgresql.JSONB(none_as_null=False), nullable=False),
        sa.Column("raw_yaml", sa.Text(), nullable=True),
        sa.Column("created_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("definition_id"),
        sa.CheckConstraint(
            "length(trim(workflow_name)) > 0", name="chk_definitions_name_non_empty"
        ),
    )

    # 2. workflow_executions
    op.create_table(
        "workflow_executions",
        sa.Column("workflow_execution_id", sa.UUID(), nullable=False),
        sa.Column("definition_id", sa.UUID(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("workflow_input", postgresql.JSONB(none_as_null=False), nullable=False),
        sa.Column("has_output", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("workflow_output", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("failure_category", sa.Text(), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=True),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column("failure_details", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("created_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["definition_id"], ["registered_definitions.definition_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("workflow_execution_id"),
        sa.CheckConstraint(
            "state IN ('INITIALIZING', 'RUNNING', 'FAILING', 'CANCELLING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_workflow_state",
        ),
        sa.CheckConstraint("revision >= 1", name="chk_workflow_revision"),
        sa.CheckConstraint(
            "(has_output = FALSE AND workflow_output IS NULL) OR (has_output = TRUE AND workflow_output IS NOT NULL)",
            name="chk_workflow_output_consistency",
        ),
        sa.CheckConstraint(
            "state != 'SUCCEEDED' OR (has_output = TRUE)",
            name="chk_workflow_terminal_success",
        ),
    )
    op.create_index(
        "idx_workflow_executions_state_created",
        "workflow_executions",
        ["state", sa.text("created_at_utc DESC"), "workflow_execution_id"],
    )
    op.create_index(
        "idx_workflow_executions_definition",
        "workflow_executions",
        ["definition_id", sa.text("created_at_utc DESC"), "workflow_execution_id"],
    )

    # 3. task_executions
    op.create_table(
        "task_executions",
        sa.Column("task_execution_id", sa.UUID(), nullable=False),
        sa.Column("workflow_execution_id", sa.UUID(), nullable=False),
        sa.Column("task_definition_id", sa.Text(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("has_input", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("stable_input", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("has_output", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("task_output", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("max_attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_ordinal", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("retry_ready_at_utc", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("failure_category", sa.Text(), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=True),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column("failure_details", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("created_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.workflow_execution_id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("task_execution_id"),
        sa.UniqueConstraint(
            "workflow_execution_id", "task_definition_id", name="uq_task_definition_per_workflow"
        ),
        sa.CheckConstraint(
            "state IN ('PENDING', 'RUNNABLE', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_task_state",
        ),
        sa.CheckConstraint("revision >= 1", name="chk_task_revision"),
        sa.CheckConstraint("max_attempts >= 1", name="chk_task_max_attempts"),
        sa.CheckConstraint("next_attempt_ordinal >= 1", name="chk_task_next_ordinal"),
        sa.CheckConstraint(
            "(has_input = FALSE AND stable_input IS NULL) OR (has_input = TRUE AND stable_input IS NOT NULL)",
            name="chk_task_input_presence",
        ),
        sa.CheckConstraint(
            "state NOT IN ('RUNNABLE', 'RUNNING', 'RETRY_WAIT', 'SUCCEEDED', 'FAILED') OR (has_input = TRUE AND jsonb_typeof(stable_input) = 'object')",
            name="chk_task_stable_input_established",
        ),
        sa.CheckConstraint(
            "(has_output = FALSE AND task_output IS NULL) OR (has_output = TRUE AND task_output IS NOT NULL)",
            name="chk_task_output_consistency",
        ),
        sa.CheckConstraint(
            "state != 'SUCCEEDED' OR (has_output = TRUE)", name="chk_task_terminal_success"
        ),
        sa.CheckConstraint(
            "state != 'RETRY_WAIT' OR retry_ready_at_utc IS NOT NULL",
            name="chk_task_retry_wait_deadline",
        ),
    )
    op.create_index("idx_task_executions_workflow_id", "task_executions", ["workflow_execution_id"])
    op.create_index(
        "idx_task_executions_runnable_rediscovery",
        "task_executions",
        ["created_at_utc", "task_execution_id"],
        postgresql_where=sa.text("state = 'RUNNABLE'"),
    )
    op.create_index(
        "idx_task_executions_retry_timer",
        "task_executions",
        ["retry_ready_at_utc", "task_execution_id"],
        postgresql_where=sa.text("state = 'RETRY_WAIT'"),
    )

    # 4. execution_attempts
    op.create_table(
        "execution_attempts",
        sa.Column("attempt_id", sa.UUID(), nullable=False),
        sa.Column("task_execution_id", sa.UUID(), nullable=False),
        sa.Column("attempt_ordinal", sa.Integer(), nullable=False),
        sa.Column("worker_session_id", sa.UUID(), nullable=False),
        sa.Column("state", sa.Text(), nullable=False),
        sa.Column("revision", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("start_deadline_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("execution_timeout_utc", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("cancellation_deadline_utc", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("failure_category", sa.Text(), nullable=True),
        sa.Column("failure_code", sa.Text(), nullable=True),
        sa.Column("failure_message", sa.Text(), nullable=True),
        sa.Column("failure_details", postgresql.JSONB(none_as_null=False), nullable=True),
        sa.Column("created_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("updated_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["task_execution_id"], ["task_executions.task_execution_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("attempt_id"),
        sa.UniqueConstraint(
            "task_execution_id", "attempt_ordinal", name="uq_attempt_ordinal_per_task"
        ),
        sa.CheckConstraint(
            "state IN ('CLAIMED', 'RUNNING', 'SUCCEEDED', 'FAILED', 'CANCELLED')",
            name="chk_attempt_state",
        ),
        sa.CheckConstraint("attempt_ordinal >= 1", name="chk_attempt_ordinal"),
        sa.CheckConstraint("revision >= 1", name="chk_attempt_revision"),
    )
    op.create_index(
        "uq_single_active_attempt_per_task",
        "execution_attempts",
        ["task_execution_id"],
        unique=True,
        postgresql_where=sa.text("state IN ('CLAIMED', 'RUNNING')"),
    )
    op.create_index(
        "idx_execution_attempts_start_deadline",
        "execution_attempts",
        ["start_deadline_utc", "attempt_id"],
        postgresql_where=sa.text("state = 'CLAIMED'"),
    )
    op.create_index(
        "idx_execution_attempts_execution_timeout",
        "execution_attempts",
        ["execution_timeout_utc", "attempt_id"],
        postgresql_where=sa.text("state = 'RUNNING' AND execution_timeout_utc IS NOT NULL"),
    )
    op.create_index(
        "idx_execution_attempts_cancel_deadline",
        "execution_attempts",
        ["cancellation_deadline_utc", "attempt_id"],
        postgresql_where=sa.text("cancellation_deadline_utc IS NOT NULL"),
    )
    op.create_index(
        "idx_execution_attempts_worker_session",
        "execution_attempts",
        ["worker_session_id"],
        postgresql_where=sa.text("state IN ('CLAIMED', 'RUNNING')"),
    )

    # 5. history_entries
    op.create_table(
        "history_entries",
        sa.Column("history_id", sa.UUID(), nullable=False),
        sa.Column("workflow_execution_id", sa.UUID(), nullable=False),
        sa.Column("task_execution_id", sa.UUID(), nullable=True),
        sa.Column("attempt_id", sa.UUID(), nullable=True),
        sa.Column("event_category", sa.Text(), nullable=False),
        sa.Column("event_payload", postgresql.JSONB(none_as_null=False), nullable=False),
        sa.Column("occurred_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["workflow_execution_id"],
            ["workflow_executions.workflow_execution_id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["task_execution_id"], ["task_executions.task_execution_id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["attempt_id"], ["execution_attempts.attempt_id"], ondelete="RESTRICT"
        ),
        sa.PrimaryKeyConstraint("history_id"),
    )
    op.create_index(
        "idx_history_entries_pagination",
        "history_entries",
        ["workflow_execution_id", sa.text("occurred_at_utc ASC"), sa.text("history_id ASC")],
    )

    # 6. idempotency_records
    op.create_table(
        "idempotency_records",
        sa.Column("operation_type", sa.Text(), nullable=False),
        sa.Column("idempotency_key", sa.Text(), nullable=False),
        sa.Column("request_fingerprint", sa.Text(), nullable=False),
        sa.Column("resource_id", sa.UUID(), nullable=False),
        sa.Column("created_at_utc", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("operation_type", "idempotency_key"),
        sa.CheckConstraint(
            "operation_type IN ('REGISTER_DEFINITION', 'START_EXECUTION')",
            name="chk_idempotency_op_type",
        ),
        sa.CheckConstraint(
            "length(request_fingerprint) = 64", name="chk_idempotency_fingerprint_len"
        ),
    )


def downgrade() -> None:
    op.drop_table("idempotency_records")
    op.drop_table("history_entries")
    op.drop_table("execution_attempts")
    op.drop_table("task_executions")
    op.drop_table("workflow_executions")
    op.drop_table("registered_definitions")
