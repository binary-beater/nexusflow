"""Core execution domain entities and pure state transitions (LLD-01)."""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime
from enum import StrEnum
from typing import TypeVar

from nexusflow.domain.enums import AttemptState, FailureCategory, TaskState, WorkflowState
from nexusflow.domain.identifiers import (
    ActivityType,
    AttemptId,
    DefinitionId,
    TaskDefinitionId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import JsonObject, JsonValue

E = TypeVar("E")


# =====================================================================
# Output Presence Hierarchy (ADR-010, LLD-01)
# =====================================================================


@dataclass(frozen=True, slots=True)
class OutputAbsent:
    """Represents a state where output has not yet been produced or committed (SQL NULL)."""

    pass


@dataclass(frozen=True, slots=True)
class OutputCommitted:
    """Represents an authoritative output committed by a task or workflow.

    Explicitly differentiates JSON null ('null'::jsonb, where value is None)
    from uncommitted absence (OutputAbsent).
    """

    value: JsonValue


type OutputPresence = OutputAbsent | OutputCommitted


# =====================================================================
# Failure Cause (LLD-01 Section 7)
# =====================================================================


@dataclass(frozen=True, slots=True)
class FailureCause:
    category: FailureCategory
    code: str
    message: str
    details: JsonObject | None = None


# =====================================================================
# Transition Results (LLD-01 Section 9.2)
# =====================================================================


class SemanticEvent(StrEnum):
    # Workflow Events
    WORKFLOW_INITIALIZED = "WORKFLOW_INITIALIZED"
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    WORKFLOW_INIT_FAILED = "WORKFLOW_INIT_FAILED"
    WORKFLOW_FAILING_BEGUN = "WORKFLOW_FAILING_BEGUN"
    WORKFLOW_CANCELLING_BEGUN = "WORKFLOW_CANCELLING_BEGUN"
    WORKFLOW_SUCCEEDED = "WORKFLOW_SUCCEEDED"
    WORKFLOW_FAILED = "WORKFLOW_FAILED"
    WORKFLOW_CANCELLED = "WORKFLOW_CANCELLED"

    # Task Events
    TASK_INITIAL_RUNNABLE = "TASK_INITIAL_RUNNABLE"
    TASK_RETRY_RUNNABLE = "TASK_RETRY_RUNNABLE"
    TASK_RUNNING_STARTED = "TASK_RUNNING_STARTED"
    TASK_SUCCEEDED = "TASK_SUCCEEDED"
    TASK_RETRY_WAITING = "TASK_RETRY_WAITING"
    TASK_FAILED = "TASK_FAILED"
    TASK_DRAIN_CANCELLED = "TASK_DRAIN_CANCELLED"
    TASK_ATTEMPT_CANCELLED = "TASK_ATTEMPT_CANCELLED"

    # Attempt Events
    ATTEMPT_START_OBSERVED = "ATTEMPT_START_OBSERVED"
    ATTEMPT_SUCCEEDED = "ATTEMPT_SUCCEEDED"
    ATTEMPT_FAILED = "ATTEMPT_FAILED"
    ATTEMPT_CANCELLED_SETTLED = "ATTEMPT_CANCELLED_SETTLED"


@dataclass(frozen=True, slots=True)
class TransitionApplied[E]:
    entity: E
    event: SemanticEvent


@dataclass(frozen=True, slots=True)
class TransitionRejected:
    reason: str


@dataclass(frozen=True, slots=True)
class TransitionNoOp[E]:
    entity: E
    reason: str


type TransitionResult[E] = TransitionApplied[E] | TransitionRejected | TransitionNoOp[E]


# =====================================================================
# WorkflowExecution Entity (LLD-01 Section 9.3)
# =====================================================================


@dataclass(frozen=True, slots=True)
class WorkflowExecution:
    id: WorkflowExecutionId
    definition_id: DefinitionId
    state: WorkflowState
    revision: int
    workflow_input: JsonValue
    output: OutputPresence
    failure_cause: FailureCause | None = None
    created_at_utc: datetime | None = None
    updated_at_utc: datetime | None = None

    def is_terminal(self) -> bool:
        return self.state in (
            WorkflowState.SUCCEEDED,
            WorkflowState.FAILED,
            WorkflowState.CANCELLED,
        )

    def is_draining(self) -> bool:
        return self.state in (WorkflowState.FAILING, WorkflowState.CANCELLING)


def begin_running(wf: WorkflowExecution) -> TransitionResult[WorkflowExecution]:
    if wf.state != WorkflowState.INITIALIZING:
        return TransitionRejected(f"Cannot transition to RUNNING from state '{wf.state}'")
    updated = replace(wf, state=WorkflowState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_STARTED)


def complete_success(
    wf: WorkflowExecution, output_value: JsonValue
) -> TransitionResult[WorkflowExecution]:
    if wf.state != WorkflowState.RUNNING:
        return TransitionRejected(f"Cannot succeed workflow in state '{wf.state}'")
    updated = replace(
        wf,
        state=WorkflowState.SUCCEEDED,
        output=OutputCommitted(value=output_value),
        failure_cause=None,
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_SUCCEEDED)


# =====================================================================
# TaskExecution Entity (LLD-01 Section 9.4)
# =====================================================================


@dataclass(frozen=True, slots=True)
class TaskExecution:
    id: TaskExecutionId
    workflow_execution_id: WorkflowExecutionId
    task_definition_id: TaskDefinitionId
    state: TaskState
    revision: int
    stable_input: JsonObject | None  # Once established (RUNNABLE), remains immutable forever
    output: OutputPresence
    max_attempts: int
    next_attempt_ordinal: int = 1
    retry_ready_at_utc: datetime | None = None
    terminal_failure_cause: FailureCause | None = None

    def is_terminal(self) -> bool:
        return self.state in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED)


def mark_initially_runnable(
    task: TaskExecution, resolved_input: JsonObject
) -> TransitionResult[TaskExecution]:
    """The ONLY path that materializes stable_input (PENDING -> RUNNABLE)."""
    if task.state != TaskState.PENDING:
        return TransitionRejected(f"Cannot mark initially RUNNABLE from state '{task.state}'")
    updated = replace(
        task,
        state=TaskState.RUNNABLE,
        stable_input=resolved_input,
        retry_ready_at_utc=None,
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_INITIAL_RUNNABLE)


def mark_running(task: TaskExecution) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNABLE:
        return TransitionRejected(f"Cannot mark RUNNING from state '{task.state}'")
    updated = replace(task, state=TaskState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_RUNNING_STARTED)


def mark_succeeded(task: TaskExecution, output_value: JsonValue) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNING:
        return TransitionRejected(f"Cannot mark SUCCEEDED from state '{task.state}'")
    updated = replace(
        task,
        state=TaskState.SUCCEEDED,
        output=OutputCommitted(value=output_value),
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_SUCCEEDED)


# =====================================================================
# ExecutionAttempt Entity (LLD-01 Section 9.5)
# =====================================================================


@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    id: AttemptId
    task_execution_id: TaskExecutionId
    attempt_ordinal: int
    worker_session_id: WorkerSessionId
    state: AttemptState
    revision: int
    start_deadline_utc: datetime
    execution_timeout_utc: datetime | None = None
    cancellation_deadline_utc: datetime | None = None
    terminal_failure_cause: FailureCause | None = None

    def is_terminal(self) -> bool:
        return self.state in (AttemptState.SUCCEEDED, AttemptState.FAILED, AttemptState.CANCELLED)


def observe_attempt_start(
    attempt: ExecutionAttempt, now_utc: datetime
) -> TransitionResult[ExecutionAttempt]:
    if attempt.state != AttemptState.CLAIMED:
        return TransitionRejected(f"Cannot transition to RUNNING from state '{attempt.state}'")
    if now_utc > attempt.start_deadline_utc:
        return TransitionRejected(f"Start deadline expired at {attempt.start_deadline_utc}")
    updated = replace(attempt, state=AttemptState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_START_OBSERVED)


def complete_attempt_success(attempt: ExecutionAttempt) -> TransitionResult[ExecutionAttempt]:
    if attempt.state != AttemptState.RUNNING:
        return TransitionRejected(f"Cannot complete attempt in state '{attempt.state}'")
    updated = replace(attempt, state=AttemptState.SUCCEEDED)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_SUCCEEDED)


# =====================================================================
# Worker Session Domain Model (LLD-01 Section 10)
# =====================================================================


@dataclass(frozen=True, slots=True)
class WorkerSession:
    session_id: WorkerSessionId
    capabilities: frozenset[ActivityType]
    last_heartbeat_utc: datetime
    accepting_new_work: bool

    def is_live(self, now_utc: datetime, liveness_timeout_seconds: float) -> bool:
        elapsed = (now_utc - self.last_heartbeat_utc).total_seconds()
        return elapsed <= liveness_timeout_seconds

    def is_eligible_for_routing(self, now_utc: datetime, liveness_timeout_seconds: float) -> bool:
        return self.accepting_new_work and self.is_live(now_utc, liveness_timeout_seconds)
