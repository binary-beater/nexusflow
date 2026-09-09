from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from datetime import datetime

from nexusflow.domain.enums import AttemptState, FailureCategory, TaskState, WorkflowState
from nexusflow.domain.identifiers import (
    AttemptId,
    DefinitionId,
    TaskDefinitionId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import JsonObject, JsonValue

# --- Output Presence (LLD-01 / LLD-02) ---


@dataclass(frozen=True, slots=True)
class OutputPresence(ABC):  # noqa: B024
    """Sum type distinguishing absent/uncommitted output from committed output."""

    pass


@dataclass(frozen=True, slots=True)
class OutputAbsent(OutputPresence):
    """Output is not yet committed (entity is in progress, failed, or cancelled)."""

    pass


@dataclass(frozen=True, slots=True)
class OutputCommitted(OutputPresence):
    """Output is durably committed. The value may be any JsonValue, including None (JSON null)."""

    value: JsonValue


# --- Failure Cause (LLD-01 / LLD-06) ---


@dataclass(frozen=True, slots=True)
class FailureCause:
    """Domain-safe normalized failure cause attached to failed attempts, tasks, and workflows."""

    category: FailureCategory
    code: str
    message: str
    details: JsonObject | None = None


# --- Core Execution Entities (LLD-01 / LLD-02) ---


@dataclass(frozen=True, slots=True)
class WorkflowExecution:
    id: WorkflowExecutionId
    definition_id: DefinitionId
    state: WorkflowState
    revision: int
    workflow_input: JsonValue
    output: OutputPresence
    failure_cause: FailureCause | None
    created_at_utc: datetime
    updated_at_utc: datetime


@dataclass(frozen=True, slots=True)
class TaskExecution:
    id: TaskExecutionId
    workflow_execution_id: WorkflowExecutionId
    task_definition_id: TaskDefinitionId
    state: TaskState
    revision: int
    stable_input: JsonValue | None
    has_input: bool
    output: OutputPresence
    max_attempts: int
    next_attempt_ordinal: int
    retry_ready_at_utc: datetime | None
    failure_cause: FailureCause | None
    created_at_utc: datetime
    updated_at_utc: datetime


@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    id: AttemptId
    task_execution_id: TaskExecutionId
    attempt_ordinal: int
    worker_session_id: WorkerSessionId
    state: AttemptState
    revision: int
    start_deadline_utc: datetime
    execution_timeout_utc: datetime | None
    cancellation_deadline_utc: datetime | None
    failure_cause: FailureCause | None
    created_at_utc: datetime
    updated_at_utc: datetime
