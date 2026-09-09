"""Durable persistence transactions according to LLD-02 and Phase 2 concurrency requirements.

Authoritative concurrency architecture:
all semantic transactions enforce OCC revisions, expected lifecycle states,
and semantic predicates; narrow physical locking is used only where explicitly
required by the frozen LLD.
"""

from __future__ import annotations

from collections.abc import Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.domain.execution import OutputCommitted, WorkflowExecution
from nexusflow.domain.identifiers import (
    AttemptId,
    DefinitionId,
    IdempotencyKey,
    RequestFingerprint,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import JsonObject, thaw_json
from nexusflow.domain.spec import TaskDefinition, ValidatedWorkflowSpec
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    HistoryEntryRecord,
    IdempotencyRecord,
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)


class CommitStatus(StrEnum):
    COMMITTED = "COMMITTED"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    OCC_CONFLICT = "OCC_CONFLICT"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"


@dataclass(frozen=True, slots=True)
class CommitOutcome:
    status: CommitStatus
    message: str = ""


@asynccontextmanager
async def transactional_scope(session: AsyncSession):
    """Executes within a database transaction, using begin_nested (SAVEPOINT) if a transaction is already active."""
    if session.in_transaction():
        async with session.begin_nested():
            yield
    else:
        async with session.begin():
            yield


# =====================================================================
# 10.1 Definition Registration (LLD-02 Section 10.1)
# =====================================================================


async def commit_registered_definition(
    session: AsyncSession,
    definition_id: DefinitionId,
    workflow_name: str,
    spec: ValidatedWorkflowSpec,
    raw_yaml: str | None,
    idempotency_key: IdempotencyKey | None,
    fingerprint: RequestFingerprint | None,
    now_utc: datetime,
) -> tuple[CommitOutcome, DefinitionId]:
    """Persists a semantically validated definition in PostgreSQL."""
    bind = session.get_bind()
    is_sqlite = bind is not None and "sqlite" in bind.dialect.name
    insert_fn = sqlite_insert if is_sqlite else pg_insert

    async with transactional_scope(session):
        if idempotency_key is not None:
            if fingerprint is None:
                raise ValueError("Fingerprint cannot be None when idempotency_key is supplied")

            stmt_idem = (
                insert_fn(IdempotencyRecord)
                .values(
                    operation_type="REGISTER_DEFINITION",
                    idempotency_key=idempotency_key.value,
                    request_fingerprint=fingerprint.digest,
                    resource_id=definition_id.value,
                    created_at_utc=now_utc,
                )
                .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
            )
            res: Any = await session.execute(stmt_idem)
            if getattr(res, "rowcount", 0) == 0:
                existing = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.operation_type == "REGISTER_DEFINITION",
                        IdempotencyRecord.idempotency_key == idempotency_key.value,
                    )
                )
                if existing is not None and existing.request_fingerprint == fingerprint.digest:
                    return (
                        CommitOutcome(status=CommitStatus.COMMITTED, message="Idempotent match"),
                        DefinitionId(existing.resource_id),
                    )
                return (
                    CommitOutcome(
                        status=CommitStatus.PRECONDITION_FAILED,
                        message="Conflicting idempotency key",
                    ),
                    definition_id,
                )

        from nexusflow.definition.codec import serialize_validated_spec

        session.add(
            RegisteredDefinitionRecord(
                definition_id=definition_id.value,
                workflow_name=workflow_name,
                validated_iws=serialize_validated_spec(spec),
                raw_yaml=raw_yaml,
                created_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED), definition_id


# =====================================================================
# 10.2 Workflow Creation (LLD-02 Section 10.2)
# =====================================================================


async def commit_workflow_creation(
    session: AsyncSession,
    execution: WorkflowExecution,
    idempotency_key: IdempotencyKey | None,
    fingerprint: RequestFingerprint | None,
    now_utc: datetime,
) -> tuple[CommitOutcome, WorkflowExecutionId]:
    """Persists a new WorkflowExecution in INITIALIZING state with conflict-safe idempotency."""
    bind = session.get_bind()
    is_sqlite = bind is not None and "sqlite" in bind.dialect.name
    insert_fn = sqlite_insert if is_sqlite else pg_insert

    async with transactional_scope(session):
        if idempotency_key is not None:
            if fingerprint is None:
                raise ValueError("Fingerprint cannot be None when idempotency_key is supplied")

            stmt_idem = (
                insert_fn(IdempotencyRecord)
                .values(
                    operation_type="START_EXECUTION",
                    idempotency_key=idempotency_key.value,
                    request_fingerprint=fingerprint.digest,
                    resource_id=execution.id.value,
                    created_at_utc=now_utc,
                )
                .on_conflict_do_nothing(index_elements=["operation_type", "idempotency_key"])
            )
            res: Any = await session.execute(stmt_idem)
            if getattr(res, "rowcount", 0) == 0:
                existing = await session.scalar(
                    select(IdempotencyRecord).where(
                        IdempotencyRecord.operation_type == "START_EXECUTION",
                        IdempotencyRecord.idempotency_key == idempotency_key.value,
                    )
                )
                if existing is not None and existing.request_fingerprint == fingerprint.digest:
                    return (
                        CommitOutcome(status=CommitStatus.COMMITTED, message="Idempotent match"),
                        WorkflowExecutionId(existing.resource_id),
                    )
                return (
                    CommitOutcome(
                        status=CommitStatus.PRECONDITION_FAILED,
                        message="Conflicting idempotency key",
                    ),
                    execution.id,
                )

        # Insert WorkflowExecution in INITIALIZING state
        session.add(
            WorkflowExecutionRecord(
                workflow_execution_id=execution.id.value,
                definition_id=execution.definition_id.value,
                state="INITIALIZING",
                revision=1,
                workflow_input=thaw_json(execution.workflow_input),
                has_output=False,
                workflow_output=None,
                created_at_utc=now_utc,
                updated_at_utc=now_utc,
            )
        )
        await session.flush()

        # Insert Summarized History Entry
        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=execution.id.value,
                task_execution_id=None,
                attempt_id=None,
                event_category="WorkflowExecutionCreated",
                event_payload={"definition_id": str(execution.definition_id.value)},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED), execution.id


# =====================================================================
# 10.3 Task Population (LLD-02 Section 10.2 / LLD-04)
# =====================================================================


async def commit_task_population(
    session: AsyncSession,
    workflow_id: WorkflowExecutionId,
    task_definitions: Sequence[TaskDefinition],
    now_utc: datetime,
) -> CommitOutcome:
    """Populates TaskExecution records in PENDING state.

    Enforces OCC state check that workflow is INITIALIZING.
    """
    bind = session.get_bind()
    is_sqlite = bind is not None and "sqlite" in bind.dialect.name
    insert_fn = sqlite_insert if is_sqlite else pg_insert

    async with transactional_scope(session):
        wf_state = await session.scalar(
            select(WorkflowExecutionRecord.state).where(
                WorkflowExecutionRecord.workflow_execution_id == workflow_id.value
            )
        )
        if wf_state != "INITIALIZING":
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message=f"Workflow is not INITIALIZING (current state: {wf_state})",
            )

        for task_def in task_definitions:
            stmt = (
                insert_fn(TaskExecutionRecord)
                .values(
                    task_execution_id=uuid4(),
                    workflow_execution_id=workflow_id.value,
                    task_definition_id=task_def.id.value,
                    state="PENDING",
                    revision=1,
                    has_input=False,
                    stable_input=None,
                    has_output=False,
                    task_output=None,
                    max_attempts=task_def.max_attempts,
                    next_attempt_ordinal=1,
                    created_at_utc=now_utc,
                    updated_at_utc=now_utc,
                )
                .on_conflict_do_nothing(index_elements=["workflow_execution_id", "task_definition_id"])
            )
            await session.execute(stmt)

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=workflow_id.value,
                task_execution_id=None,
                attempt_id=None,
                event_category="TaskPopulationEstablished",
                event_payload={"task_count": len(task_definitions)},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)


# =====================================================================
# 10.4 Initialization Complete (LLD-02 Section 10.3)
# =====================================================================


async def commit_initialization_complete(
    session: AsyncSession,
    workflow_id: WorkflowExecutionId,
    expected_revision: int,
    now_utc: datetime,
) -> CommitOutcome:
    """Validates exact expected task membership and transitions INITIALIZING -> RUNNING via OCC."""
    async with transactional_scope(session):
        wf_row = (
            await session.execute(
                select(
                    WorkflowExecutionRecord.definition_id,
                    WorkflowExecutionRecord.revision,
                    WorkflowExecutionRecord.state,
                ).where(WorkflowExecutionRecord.workflow_execution_id == workflow_id.value)
            )
        ).one_or_none()

        if wf_row is None:
            return CommitOutcome(status=CommitStatus.PRECONDITION_FAILED, message="Workflow not found")
        if wf_row.state != "INITIALIZING" or wf_row.revision != expected_revision:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="Workflow revision or state conflict")

        # Retrieve exact expected task set from durable definition
        def_row = await session.scalar(
            select(RegisteredDefinitionRecord.validated_iws).where(
                RegisteredDefinitionRecord.definition_id == wf_row.definition_id
            )
        )
        if def_row is None:
            return CommitOutcome(status=CommitStatus.PRECONDITION_FAILED, message="Definition not found")

        expected_task_ids = set(def_row["tasks"].keys())

        # Retrieve actual populated task set
        actual_task_ids = set(
            await session.scalars(
                select(TaskExecutionRecord.task_definition_id).where(
                    TaskExecutionRecord.workflow_execution_id == workflow_id.value
                )
            )
        )

        if expected_task_ids != actual_task_ids:
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message=f"Task membership mismatched: expected {expected_task_ids}, got {actual_task_ids}",
            )

        # Conditional OCC update: INITIALIZING -> RUNNING
        res: Any = await session.execute(
            update(WorkflowExecutionRecord)
            .where(
                WorkflowExecutionRecord.workflow_execution_id == workflow_id.value,
                WorkflowExecutionRecord.state == "INITIALIZING",
                WorkflowExecutionRecord.revision == expected_revision,
            )
            .values(
                state="RUNNING",
                revision=WorkflowExecutionRecord.revision + 1,
                updated_at_utc=now_utc,
            )
        )
        if getattr(res, "rowcount", 0) == 0:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="OCC conflict on workflow start")

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=workflow_id.value,
                task_execution_id=None,
                attempt_id=None,
                event_category="WorkflowExecutionStarted",
                event_payload={"state": "RUNNING"},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)


# =====================================================================
# 10.5 Input Readiness (LLD-02 Section 10.5)
# =====================================================================


async def commit_task_readiness(
    session: AsyncSession,
    task_id: TaskExecutionId,
    expected_task_revision: int,
    workflow_id: WorkflowExecutionId,
    stable_input: JsonObject,
    now_utc: datetime,
) -> CommitOutcome:
    """Transitions TaskExecution PENDING -> RUNNABLE and materializes stable_input."""
    async with transactional_scope(session):
        # Verify workflow is RUNNING via OCC predicate
        wf_state = await session.scalar(
            select(WorkflowExecutionRecord.state).where(
                WorkflowExecutionRecord.workflow_execution_id == workflow_id.value
            )
        )
        if wf_state != "RUNNING":
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message=f"Workflow is not RUNNING (current state: {wf_state})",
            )

        # Conditional update: PENDING -> RUNNABLE with expected revision
        stmt = (
            update(TaskExecutionRecord)
            .where(
                TaskExecutionRecord.task_execution_id == task_id.value,
                TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                TaskExecutionRecord.state == "PENDING",
                TaskExecutionRecord.revision == expected_task_revision,
            )
            .values(
                state="RUNNABLE",
                has_input=True,
                stable_input=thaw_json(stable_input),
                revision=TaskExecutionRecord.revision + 1,
                updated_at_utc=now_utc,
            )
        )
        result: Any = await session.execute(stmt)
        if getattr(result, "rowcount", 0) == 0:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="OCC conflict on task readiness")

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=workflow_id.value,
                task_execution_id=task_id.value,
                attempt_id=None,
                event_category="TaskMarkedRunnable",
                event_payload={"task_id": str(task_id.value)},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)


# =====================================================================
# 10.7 Attempt Ownership Commit (LLD-02 Section 10.7)
# =====================================================================


async def commit_attempt_ownership(
    session: AsyncSession,
    task_id: TaskExecutionId,
    expected_task_revision: int,
    workflow_id: WorkflowExecutionId,
    worker_session_id: WorkerSessionId,
    new_attempt_id: AttemptId,
    start_deadline_utc: datetime,
    now_utc: datetime,
) -> tuple[CommitOutcome, int | None]:
    """Transitions TaskExecution RUNNABLE -> RUNNING and creates CLAIMED ExecutionAttempt.

    Linearization point for activity ownership.
    Allocates attempt ordinal atomically via RETURNING.
    """
    async with transactional_scope(session):
        # Verify workflow is RUNNING via OCC predicate
        wf_state = await session.scalar(
            select(WorkflowExecutionRecord.state).where(
                WorkflowExecutionRecord.workflow_execution_id == workflow_id.value
            )
        )
        if wf_state != "RUNNING":
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message=f"Workflow is not RUNNING (current state: {wf_state})",
            ), None

        # Verify no active attempt already exists for this task (CLAIMED or RUNNING)
        active_attempt = await session.scalar(
            select(ExecutionAttemptRecord.attempt_id).where(
                ExecutionAttemptRecord.task_execution_id == task_id.value,
                ExecutionAttemptRecord.state.in_(["CLAIMED", "RUNNING"]),
            )
        )
        if active_attempt is not None:
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message="Active attempt already exists for task",
            ), None

        # Conditionally transition Task RUNNABLE -> RUNNING and allocate ordinal
        stmt = (
            update(TaskExecutionRecord)
            .where(
                TaskExecutionRecord.task_execution_id == task_id.value,
                TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                TaskExecutionRecord.state == "RUNNABLE",
                TaskExecutionRecord.revision == expected_task_revision,
            )
            .values(
                state="RUNNING",
                revision=TaskExecutionRecord.revision + 1,
                next_attempt_ordinal=TaskExecutionRecord.next_attempt_ordinal + 1,
                updated_at_utc=now_utc,
            )
            .returning(TaskExecutionRecord.next_attempt_ordinal - 1)
        )
        res: Any = await session.execute(stmt)
        allocated_ordinal = res.scalar_one_or_none()
        if allocated_ordinal is None:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="OCC conflict claiming task"), None

        # Insert new ExecutionAttempt in CLAIMED state
        session.add(
            ExecutionAttemptRecord(
                attempt_id=new_attempt_id.value,
                task_execution_id=task_id.value,
                attempt_ordinal=allocated_ordinal,
                worker_session_id=worker_session_id.value,
                state="CLAIMED",
                revision=1,
                start_deadline_utc=start_deadline_utc,
                created_at_utc=now_utc,
                updated_at_utc=now_utc,
            )
        )
        await session.flush()

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=workflow_id.value,
                task_execution_id=task_id.value,
                attempt_id=new_attempt_id.value,
                event_category="TaskClaimedByWorker",
                event_payload={
                    "worker_session_id": str(worker_session_id.value),
                    "attempt_ordinal": allocated_ordinal,
                },
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED), allocated_ordinal


# =====================================================================
# 10.8 Execution Start (LLD-02 Section 10.8)
# =====================================================================


async def commit_worker_execution_start(
    session: AsyncSession,
    attempt_id: AttemptId,
    worker_session_id: WorkerSessionId,
    expected_attempt_revision: int,
    now_utc: datetime,
) -> CommitOutcome:
    """Transitions ExecutionAttempt CLAIMED -> RUNNING upon worker start acknowledge."""
    async with transactional_scope(session):
        stmt = (
            update(ExecutionAttemptRecord)
            .where(
                ExecutionAttemptRecord.attempt_id == attempt_id.value,
                ExecutionAttemptRecord.worker_session_id == worker_session_id.value,
                ExecutionAttemptRecord.state == "CLAIMED",
                ExecutionAttemptRecord.start_deadline_utc >= now_utc,
                ExecutionAttemptRecord.revision == expected_attempt_revision,
            )
            .values(
                state="RUNNING",
                revision=ExecutionAttemptRecord.revision + 1,
                updated_at_utc=now_utc,
            )
            .returning(ExecutionAttemptRecord.task_execution_id)
        )
        res: Any = await session.execute(stmt)
        task_id = res.scalar_one_or_none()
        if task_id is None:
            # Check if already started idempotently
            existing = await session.scalar(
                select(ExecutionAttemptRecord).where(
                    ExecutionAttemptRecord.attempt_id == attempt_id.value,
                    ExecutionAttemptRecord.worker_session_id == worker_session_id.value,
                )
            )
            if existing is not None and existing.state == "RUNNING":
                return CommitOutcome(status=CommitStatus.COMMITTED, message="Already started")
            return CommitOutcome(
                status=CommitStatus.OCC_CONFLICT,
                message="OCC conflict or expired start deadline",
            )

        wf_id = await session.scalar(
            select(TaskExecutionRecord.workflow_execution_id).where(
                TaskExecutionRecord.task_execution_id == task_id
            )
        )

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=wf_id if wf_id is not None else uuid4(),
                task_execution_id=task_id,
                attempt_id=attempt_id.value,
                event_category="AttemptExecutionStarted",
                event_payload={"state": "RUNNING"},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)


# =====================================================================
# 10.9 Task Success (LLD-02 Section 10.9)
# =====================================================================


async def commit_worker_task_success(
    session: AsyncSession,
    attempt_id: AttemptId,
    worker_session_id: WorkerSessionId,
    expected_attempt_revision: int,
    task_id: TaskExecutionId,
    expected_task_revision: int,
    output: OutputCommitted,
    now_utc: datetime,
) -> CommitOutcome:
    """Transitions ExecutionAttempt & TaskExecution RUNNING -> SUCCEEDED with output."""
    async with transactional_scope(session):
        # Transition Attempt RUNNING -> SUCCEEDED verifying task and worker session
        stmt_attempt = (
            update(ExecutionAttemptRecord)
            .where(
                ExecutionAttemptRecord.attempt_id == attempt_id.value,
                ExecutionAttemptRecord.worker_session_id == worker_session_id.value,
                ExecutionAttemptRecord.task_execution_id == task_id.value,
                ExecutionAttemptRecord.state == "RUNNING",
                ExecutionAttemptRecord.revision == expected_attempt_revision,
            )
            .values(
                state="SUCCEEDED",
                revision=ExecutionAttemptRecord.revision + 1,
                updated_at_utc=now_utc,
            )
        )
        res_attempt: Any = await session.execute(stmt_attempt)
        if getattr(res_attempt, "rowcount", 0) == 0:
            # Check for duplicate callback
            existing = await session.scalar(
                select(ExecutionAttemptRecord).where(
                    ExecutionAttemptRecord.attempt_id == attempt_id.value
                )
            )
            if existing is not None and existing.state == "SUCCEEDED":
                current_task = await session.scalar(
                    select(TaskExecutionRecord).where(
                        TaskExecutionRecord.task_execution_id == task_id.value
                    )
                )
                if current_task is not None and thaw_json(output.value) == current_task.task_output:
                    return CommitOutcome(status=CommitStatus.COMMITTED, message="Duplicate callback matches")
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="Attempt state/revision conflict")

        output_payload = thaw_json(output.value)

        # Transition Task RUNNING -> SUCCEEDED with Output
        stmt_task = (
            update(TaskExecutionRecord)
            .where(
                TaskExecutionRecord.task_execution_id == task_id.value,
                TaskExecutionRecord.state == "RUNNING",
                TaskExecutionRecord.revision == expected_task_revision,
            )
            .values(
                state="SUCCEEDED",
                has_output=True,
                task_output=output_payload,
                revision=TaskExecutionRecord.revision + 1,
                updated_at_utc=now_utc,
            )
            .returning(TaskExecutionRecord.workflow_execution_id)
        )
        res_task: Any = await session.execute(stmt_task)
        wf_id = res_task.scalar_one_or_none()
        if wf_id is None:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="Task state/revision conflict")

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=wf_id,
                task_execution_id=task_id.value,
                attempt_id=attempt_id.value,
                event_category="TaskExecutionSucceeded",
                event_payload={"has_output": True},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)


# =====================================================================
# 10.16 Workflow Success Settlement (LLD-02 Section 10.16)
# =====================================================================


async def commit_workflow_success(
    session: AsyncSession,
    workflow_id: WorkflowExecutionId,
    expected_workflow_revision: int,
    output: OutputCommitted,
    now_utc: datetime,
) -> CommitOutcome:
    """Verifies all expected tasks are SUCCEEDED and transitions WorkflowExecution to SUCCEEDED."""
    async with transactional_scope(session):
        wf_row = (
            await session.execute(
                select(
                    WorkflowExecutionRecord.definition_id,
                    WorkflowExecutionRecord.revision,
                    WorkflowExecutionRecord.state,
                ).where(WorkflowExecutionRecord.workflow_execution_id == workflow_id.value)
            )
        ).one_or_none()

        if wf_row is None:
            return CommitOutcome(status=CommitStatus.PRECONDITION_FAILED, message="Workflow not found")
        if wf_row.state != "RUNNING" or wf_row.revision != expected_workflow_revision:
            return CommitOutcome(
                status=CommitStatus.OCC_CONFLICT,
                message=f"Workflow state/revision conflict (state={wf_row.state}, rev={wf_row.revision})",
            )

        # Retrieve exact expected task set from durable definition
        def_row = await session.scalar(
            select(RegisteredDefinitionRecord.validated_iws).where(
                RegisteredDefinitionRecord.definition_id == wf_row.definition_id
            )
        )
        if def_row is None:
            return CommitOutcome(status=CommitStatus.PRECONDITION_FAILED, message="Definition not found")

        expected_task_ids = set(def_row["tasks"].keys())

        # Retrieve successful task set
        succeeded_task_ids = set(
            await session.scalars(
                select(TaskExecutionRecord.task_definition_id).where(
                    TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                    TaskExecutionRecord.state == "SUCCEEDED",
                )
            )
        )

        if expected_task_ids != succeeded_task_ids:
            return CommitOutcome(
                status=CommitStatus.PRECONDITION_FAILED,
                message=f"Not all tasks succeeded: missing {expected_task_ids - succeeded_task_ids}",
            )

        output_payload = thaw_json(output.value)

        res_update: Any = await session.execute(
            update(WorkflowExecutionRecord)
            .where(
                WorkflowExecutionRecord.workflow_execution_id == workflow_id.value,
                WorkflowExecutionRecord.state == "RUNNING",
                WorkflowExecutionRecord.revision == expected_workflow_revision,
            )
            .values(
                state="SUCCEEDED",
                has_output=True,
                workflow_output=output_payload,
                revision=WorkflowExecutionRecord.revision + 1,
                updated_at_utc=now_utc,
            )
        )
        if getattr(res_update, "rowcount", 0) == 0:
            return CommitOutcome(status=CommitStatus.OCC_CONFLICT, message="OCC conflict on workflow success")

        session.add(
            HistoryEntryRecord(
                history_id=uuid4(),
                workflow_execution_id=workflow_id.value,
                task_execution_id=None,
                attempt_id=None,
                event_category="WorkflowExecutionSucceeded",
                event_payload={"has_output": True},
                occurred_at_utc=now_utc,
            )
        )

    return CommitOutcome(status=CommitStatus.COMMITTED)
