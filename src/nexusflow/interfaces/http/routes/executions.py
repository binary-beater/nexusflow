"""Public workflow execution lifecycle and querying routes (LLD-08 Section 6)."""

import hashlib
from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.definition.codec import deserialize_validated_spec
from nexusflow.domain.enums import PublicPermission, WorkflowState
from nexusflow.domain.execution import OutputAbsent, WorkflowExecution
from nexusflow.domain.identifiers import (
    DefinitionId,
    IdempotencyKey,
    RequestFingerprint,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import freeze_json
from nexusflow.interfaces.http.dependencies import (
    RecoveryGateProtocol,
    get_db_session,
    get_recovery_gate,
    get_scheduler,
)
from nexusflow.interfaces.http.dto import (
    CreateExecutionRequestDTO,
    ExecutionResponseDTO,
    FailureCauseDTO,
    HistoryEntryResponseDTO,
    TaskExecutionResponseDTO,
)
from nexusflow.interfaces.http.errors import ApiHttpException
from nexusflow.interfaces.http.security import require_permission
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import (
    HistoryEntryRecord,
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_initialization_complete,
    commit_task_population,
    commit_workflow_creation,
)

router = APIRouter(prefix="/v1/executions", tags=["Executions"])


@router.post("", status_code=status.HTTP_201_CREATED, response_model=ExecutionResponseDTO)
async def start_execution(
    payload: CreateExecutionRequestDTO,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    gate: Annotated[RecoveryGateProtocol, Depends(get_recovery_gate)],
    scheduler: Annotated[ExecutionScheduler, Depends(get_scheduler)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.EXECUTIONS_START))],
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> ExecutionResponseDTO:
    """Starts a new WorkflowExecution or returns idempotent existing execution."""
    if not gate.allows_new_work():
        raise ApiHttpException(
            status_code=503,
            code="NOT_READY",
            message="System is recovering; cannot accept new workflow executions.",
        )

    # 1. Verify Definition exists
    def_row = await session.scalar(
        select(RegisteredDefinitionRecord).where(
            RegisteredDefinitionRecord.definition_id == payload.definition_id
        )
    )
    if def_row is None:
        raise ApiHttpException(
            status_code=404,
            code="DEFINITION_NOT_FOUND",
            message=f"Definition '{payload.definition_id}' not found.",
        )

    spec = deserialize_validated_spec(def_row.validated_iws)

    # 2. Compute fingerprint if Idempotency-Key present
    fingerprint: RequestFingerprint | None = None
    idem_key: IdempotencyKey | None = None
    if idempotency_key is not None:
        raw_fp = f"{payload.definition_id}:{payload.workflow_input}"
        digest = hashlib.sha256(raw_fp.encode("utf-8")).hexdigest()
        fingerprint = RequestFingerprint(digest=digest)
        idem_key = IdempotencyKey(value=idempotency_key)

    now_utc = datetime.now(UTC)
    new_wf_id = WorkflowExecutionId.generate()
    frozen_input = freeze_json(payload.workflow_input)

    execution_entity = WorkflowExecution(
        id=new_wf_id,
        definition_id=DefinitionId(payload.definition_id),
        state=WorkflowState.INITIALIZING,
        revision=1,
        workflow_input=frozen_input,
        output=OutputAbsent(),
        created_at_utc=now_utc,
        updated_at_utc=now_utc,
    )

    # 3. Step 1: Commit workflow creation (INITIALIZING)
    outcome, actual_wf_id = await commit_workflow_creation(
        session=session,
        execution=execution_entity,
        idempotency_key=idem_key,
        fingerprint=fingerprint,
        now_utc=now_utc,
    )

    if outcome.status == CommitStatus.PRECONDITION_FAILED:
        raise ApiHttpException(
            status_code=409,
            code="IDEMPOTENCY_CONFLICT",
            message="Idempotency key was previously used with different parameters.",
        )

    if outcome.status == CommitStatus.COMMITTED and actual_wf_id.value != new_wf_id.value:
        # Existing idempotent execution
        existing_wf = await session.scalar(
            select(WorkflowExecutionRecord).where(
                WorkflowExecutionRecord.workflow_execution_id == actual_wf_id.value
            )
        )
        if existing_wf is not None:
            return ExecutionResponseDTO(
                workflow_execution_id=existing_wf.workflow_execution_id,
                definition_id=existing_wf.definition_id,
                state=existing_wf.state,
                has_output=existing_wf.has_output,
                output=existing_wf.workflow_output,
                created_at_utc=existing_wf.created_at_utc,
                updated_at_utc=existing_wf.updated_at_utc,
            )

    # 4. Step 2: Populate tasks in PENDING
    pop_outcome = await commit_task_population(
        session=session,
        workflow_id=actual_wf_id,
        task_definitions=list(spec.tasks.values()),
        now_utc=now_utc,
    )
    if pop_outcome.status != CommitStatus.COMMITTED:
        raise ApiHttpException(
            status_code=500,
            code="INITIALIZATION_FAILED",
            message=f"Failed populating tasks: {pop_outcome.message}",
        )

    # 5. Step 3: Initialization complete (INITIALIZING -> RUNNING)
    init_outcome = await commit_initialization_complete(
        session=session,
        workflow_id=actual_wf_id,
        expected_revision=1,
        now_utc=now_utc,
    )
    if init_outcome.status != CommitStatus.COMMITTED:
        raise ApiHttpException(
            status_code=500,
            code="INITIALIZATION_FAILED",
            message=f"Failed completing initialization: {init_outcome.message}",
        )

    # Commit the initialization transaction before invoking scheduler
    await session.commit()

    # 6. Trigger scheduler transition to evaluate root tasks
    await scheduler.advance_workflow(actual_wf_id)

    # 7. Fetch authoritative workflow row
    wf_record = await session.scalar(
        select(WorkflowExecutionRecord).where(
            WorkflowExecutionRecord.workflow_execution_id == actual_wf_id.value
        )
    )
    if wf_record is None:
        raise ApiHttpException(status_code=500, code="INTERNAL_ERROR", message="Workflow record disappeared.")

    return ExecutionResponseDTO(
        workflow_execution_id=wf_record.workflow_execution_id,
        definition_id=wf_record.definition_id,
        state=wf_record.state,
        has_output=wf_record.has_output,
        output=wf_record.workflow_output,
        created_at_utc=wf_record.created_at_utc,
        updated_at_utc=wf_record.updated_at_utc,
    )


@router.get("/{workflow_id}", response_model=ExecutionResponseDTO)
async def get_execution(
    workflow_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.EXECUTIONS_READ))],
) -> ExecutionResponseDTO:
    """Retrieves workflow execution status and output."""
    record = await session.scalar(
        select(WorkflowExecutionRecord).where(
            WorkflowExecutionRecord.workflow_execution_id == workflow_id
        )
    )
    if record is None:
        raise ApiHttpException(
            status_code=404,
            code="EXECUTION_NOT_FOUND",
            message=f"Execution '{workflow_id}' not found.",
        )

    return ExecutionResponseDTO(
        workflow_execution_id=record.workflow_execution_id,
        definition_id=record.definition_id,
        state=record.state,
        has_output=record.has_output,
        output=record.workflow_output,
        created_at_utc=record.created_at_utc,
        updated_at_utc=record.updated_at_utc,
    )


@router.get("/{workflow_id}/tasks", response_model=list[TaskExecutionResponseDTO])
async def list_tasks(
    workflow_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.EXECUTIONS_READ))],
) -> list[TaskExecutionResponseDTO]:
    """Lists all task executions for a workflow."""
    records = (
        await session.execute(
            select(TaskExecutionRecord).where(
                TaskExecutionRecord.workflow_execution_id == workflow_id
            )
        )
    ).scalars().all()

    result: list[TaskExecutionResponseDTO] = []
    for r in records:
        failure: FailureCauseDTO | None = None
        if r.failure_category and r.failure_code and r.failure_message:
            failure = FailureCauseDTO(
                category=r.failure_category,
                code=r.failure_code,
                message=r.failure_message,
                details=r.failure_details,
            )
        result.append(
            TaskExecutionResponseDTO(
                task_execution_id=r.task_execution_id,
                workflow_execution_id=r.workflow_execution_id,
                task_definition_id=r.task_definition_id,
                state=r.state,
                current_attempt_ordinal=r.next_attempt_ordinal,
                has_output=r.has_output,
                output=r.task_output,
                failure_cause=failure,
                created_at_utc=r.created_at_utc,
                updated_at_utc=r.updated_at_utc,
            )
        )
    return result


@router.get("/{workflow_id}/history", response_model=list[HistoryEntryResponseDTO])
async def list_history(
    workflow_id: UUID,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _ctx: Annotated[object, Depends(require_permission(PublicPermission.EXECUTIONS_READ))],
    limit: int = Query(default=100, ge=1, le=1000),
) -> list[HistoryEntryResponseDTO]:
    """Retrieves immutable audit history for a workflow ordered by (occurred_at_utc, history_id)."""
    records = (
        await session.execute(
            select(HistoryEntryRecord)
            .where(HistoryEntryRecord.workflow_execution_id == workflow_id)
            .order_by(HistoryEntryRecord.occurred_at_utc.asc(), HistoryEntryRecord.history_id.asc())
            .limit(limit)
        )
    ).scalars().all()

    return [
        HistoryEntryResponseDTO(
            history_id=r.history_id,
            workflow_execution_id=r.workflow_execution_id,
            task_execution_id=r.task_execution_id,
            attempt_id=r.attempt_id,
            event_category=r.event_category,
            occurred_at_utc=r.occurred_at_utc,
            details=r.event_payload,
        )
        for r in records
    ]
