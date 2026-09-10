"""Internal worker protocol endpoints according to LLD-05."""

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexusflow.domain.execution import OutputCommitted
from nexusflow.domain.identifiers import (
    ActivityType,
    AttemptId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import freeze_json
from nexusflow.interfaces.http.dependencies import (
    get_db_session,
    get_scheduler,
    get_worker_registry,
)
from nexusflow.interfaces.http.dto import (
    ActivityCancelAckPayloadDTO,
    ActivityFailurePayloadDTO,
    ActivitySuccessPayloadDTO,
    TaskAssignmentPayloadDTO,
    WorkerCallbackRequestDTO,
    WorkerCallbackResponseDTO,
    WorkerHeartbeatRequestDTO,
    WorkerHeartbeatResponseDTO,
    WorkerPollRequestDTO,
    WorkerPollResponseDTO,
    WorkerRegistrationRequestDTO,
    WorkerRegistrationResponseDTO,
    WorkerStartAckRequestDTO,
    WorkerStartAckResponseDTO,
)
from nexusflow.interfaces.http.errors import ApiHttpException
from nexusflow.interfaces.http.security import require_worker_domain_auth
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_worker_execution_start,
    commit_worker_task_success,
)

router = APIRouter(prefix="/internal/v1/worker", tags=["Worker Protocol"])


@router.post(
    "/register",
    status_code=status.HTTP_200_OK,
    response_model=WorkerRegistrationResponseDTO,
    dependencies=[Depends(require_worker_domain_auth)],
)
async def register_worker(
    payload: WorkerRegistrationRequestDTO,
    registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> WorkerRegistrationResponseDTO:
    """Allocates a brand new WorkerSessionId for an authenticated worker process."""
    capabilities = frozenset(ActivityType(cap) for cap in payload.capabilities)
    record = await registry.register(
        worker_id=payload.worker_id,
        capabilities=capabilities,
        accepting_new_work=True,
    )
    return WorkerRegistrationResponseDTO(
        worker_session_id=record.session_id.value,
        status="REGISTERED",
        heartbeat_interval_seconds=5.0,
        worker_liveness_timeout_seconds=15.0,
    )


@router.post(
    "/heartbeat",
    status_code=status.HTTP_200_OK,
    response_model=WorkerHeartbeatResponseDTO,
    dependencies=[Depends(require_worker_domain_auth)],
)
async def heartbeat_worker(
    payload: WorkerHeartbeatRequestDTO,
    registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> WorkerHeartbeatResponseDTO:
    """Updates worker session liveness timestamp in memory."""
    ok = await registry.heartbeat(
        session_id=WorkerSessionId(payload.worker_session_id),
        accepting_new_work=payload.accepting_new_work,
    )
    if not ok:
        raise ApiHttpException(
            status_code=409,
            code="STALE_SESSION",
            message="Worker session expired or unknown to current control plane.",
        )
    return WorkerHeartbeatResponseDTO(status="ACCEPTED", control_plane_draining=False)


@router.post(
    "/poll",
    status_code=status.HTTP_200_OK,
    response_model=WorkerPollResponseDTO,
    dependencies=[Depends(require_worker_domain_auth)],
)
async def poll_work(
    payload: WorkerPollRequestDTO,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> WorkerPollResponseDTO:
    """Long-polls for assigned CLAIMED attempts already durably owned by this session."""
    session_id = WorkerSessionId(payload.worker_session_id)
    snap = await registry.get_snapshot(session_id)
    if snap is None or not snap.live:
        raise ApiHttpException(
            status_code=409,
            code="STALE_SESSION",
            message="Worker session expired or unknown.",
        )

    # 1. Check in-memory delivery queue hint
    queued = await registry.poll_delivery(
        session_id, timeout_seconds=min(payload.timeout_seconds, 2.0)
    )
    if queued is not None and isinstance(queued, dict):
        if queued.get("status") == "ASSIGNMENT":
            asgn = queued.get("assignment")
            if asgn:
                return WorkerPollResponseDTO(
                    status="ASSIGNMENT",
                    assignment=TaskAssignmentPayloadDTO(
                        attempt_id=UUID(asgn["attempt_id"]),
                        attempt_ordinal=asgn["attempt_ordinal"],
                        task_execution_id=UUID(asgn["task_execution_id"]),
                        workflow_execution_id=UUID(asgn["workflow_execution_id"]),
                        worker_session_id=UUID(asgn["worker_session_id"]),
                        activity_type=asgn["activity_type"],
                        stable_input=asgn["stable_input"],
                        start_deadline_utc=asgn["start_deadline_utc"],
                    ),
                )
        elif queued.get("status") == "CANCEL_COMMAND":
            canc = queued.get("cancellation")
            if canc:
                from nexusflow.interfaces.http.dto import TaskCancellationPayloadDTO

                return WorkerPollResponseDTO(
                    status="CANCEL_COMMAND",
                    cancellation=TaskCancellationPayloadDTO(
                        attempt_id=UUID(canc["attempt_id"]),
                        worker_session_id=UUID(canc["worker_session_id"]),
                        task_execution_id=UUID(canc["task_execution_id"]),
                        workflow_execution_id=UUID(canc["workflow_execution_id"]),
                        reason=canc["reason"],
                    ),
                )

    # 2. Durable fallback: check PostgreSQL for any CLAIMED attempt for this session
    attempt_row = (
        await session.execute(
            select(
                ExecutionAttemptRecord.attempt_id,
                ExecutionAttemptRecord.attempt_ordinal,
                ExecutionAttemptRecord.task_execution_id,
                ExecutionAttemptRecord.start_deadline_utc,
            )
            .where(
                ExecutionAttemptRecord.worker_session_id == payload.worker_session_id,
                ExecutionAttemptRecord.state == "CLAIMED",
            )
            .order_by(ExecutionAttemptRecord.created_at_utc.asc())
            .limit(1)
        )
    ).one_or_none()

    if attempt_row is not None:
        task_row = (
            await session.execute(
                select(
                    TaskExecutionRecord.task_definition_id,
                    TaskExecutionRecord.workflow_execution_id,
                    TaskExecutionRecord.stable_input,
                ).where(TaskExecutionRecord.task_execution_id == attempt_row.task_execution_id)
            )
        ).one_or_none()

        if task_row is not None:
            return WorkerPollResponseDTO(
                status="ASSIGNMENT",
                assignment=TaskAssignmentPayloadDTO(
                    attempt_id=attempt_row.attempt_id,
                    attempt_ordinal=attempt_row.attempt_ordinal,
                    task_execution_id=attempt_row.task_execution_id,
                    workflow_execution_id=task_row.workflow_execution_id,
                    worker_session_id=payload.worker_session_id,
                    activity_type=task_row.task_definition_id,
                    stable_input=task_row.stable_input or {},
                    start_deadline_utc=attempt_row.start_deadline_utc.isoformat(),
                ),
            )

    return WorkerPollResponseDTO(status="NO_WORK")


@router.post(
    "/start",
    status_code=status.HTTP_200_OK,
    response_model=WorkerStartAckResponseDTO,
    dependencies=[Depends(require_worker_domain_auth)],
)
async def acknowledge_start(
    payload: WorkerStartAckRequestDTO,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    registry: Annotated[WorkerRegistry, Depends(get_worker_registry)],
) -> WorkerStartAckResponseDTO:
    """Acknowledges start of activity execution; transitions CLAIMED -> RUNNING."""
    snap = await registry.get_snapshot(WorkerSessionId(payload.worker_session_id))
    if snap is None or not snap.live:
        raise ApiHttpException(
            status_code=409,
            code="STALE_SESSION",
            message="Worker session expired or unknown to current control plane.",
        )
    now_utc = datetime.now(UTC)
    outcome = await commit_worker_execution_start(
        session=session,
        attempt_id=AttemptId(payload.attempt_id),
        worker_session_id=WorkerSessionId(payload.worker_session_id),
        expected_attempt_revision=1,
        now_utc=now_utc,
    )
    if outcome.status != CommitStatus.COMMITTED:
        raise ApiHttpException(
            status_code=409,
            code="STALE_ATTEMPT",
            message=f"Could not start attempt: {outcome.message}",
        )
    return WorkerStartAckResponseDTO(
        status="ACCEPTED",
        attempt_id=payload.attempt_id,
        attempt_state="RUNNING",
    )


@router.post(
    "/callback",
    status_code=status.HTTP_200_OK,
    response_model=WorkerCallbackResponseDTO,
    dependencies=[Depends(require_worker_domain_auth)],
)
async def report_callback(
    payload: WorkerCallbackRequestDTO,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    scheduler: Annotated[ExecutionScheduler, Depends(get_scheduler)],
) -> WorkerCallbackResponseDTO:
    """Accepts activity completion callbacks from workers."""
    # 1. Fetch Attempt to find associated TaskExecution
    attempt = await session.scalar(
        select(ExecutionAttemptRecord).where(
            ExecutionAttemptRecord.attempt_id == payload.attempt_id
        )
    )
    if attempt is None:
        raise ApiHttpException(
            status_code=404,
            code="ATTEMPT_NOT_FOUND",
            message=f"Attempt '{payload.attempt_id}' not found.",
        )

    if attempt.worker_session_id != payload.worker_session_id:
        raise ApiHttpException(
            status_code=403,
            code="WRONG_SESSION",
            message="Attempt is not owned by this worker session.",
        )

    task = await session.scalar(
        select(TaskExecutionRecord).where(
            TaskExecutionRecord.task_execution_id == attempt.task_execution_id
        )
    )
    if task is None:
        raise ApiHttpException(
            status_code=500, code="INTERNAL_ERROR", message="Associated task not found."
        )

    now_utc = datetime.now(UTC)

    # Branch on outcome type
    if isinstance(payload.payload, ActivitySuccessPayloadDTO):
        try:
            frozen_output = freeze_json(payload.payload.output)
        except Exception as exc:
            raise ApiHttpException(
                status_code=422,
                code="INVALID_OUTPUT",
                message=f"Output is not valid JSON: {exc}",
            ) from exc

        outcome = await commit_worker_task_success(
            session=session,
            attempt_id=AttemptId(payload.attempt_id),
            worker_session_id=WorkerSessionId(payload.worker_session_id),
            expected_attempt_revision=attempt.revision,
            task_id=TaskExecutionId(task.task_execution_id),
            expected_task_revision=task.revision,
            output=OutputCommitted(value=frozen_output),
            now_utc=now_utc,
        )
        if outcome.status != CommitStatus.COMMITTED:
            raise ApiHttpException(
                status_code=409,
                code="STALE_ATTEMPT",
                message=f"Callback rejected: {outcome.message}",
            )
        await session.commit()
        await scheduler.advance_workflow(WorkflowExecutionId(task.workflow_execution_id))

    elif isinstance(payload.payload, ActivityFailurePayloadDTO):
        from datetime import timedelta

        from nexusflow.domain.failure import (
            WorkerFailureReport,
            classify_worker_failure,
            evaluate_retry_eligibility,
        )
        from nexusflow.persistence.transactions import (
            commit_worker_definitive_failure,
            commit_worker_failure_with_retry,
            commit_workflow_failure_direction,
        )

        wf_record = await session.scalar(
            select(WorkflowExecutionRecord).where(
                WorkflowExecutionRecord.workflow_execution_id == task.workflow_execution_id
            )
        )
        wf_state = wf_record.state if wf_record else "UNKNOWN"
        raw_report = WorkerFailureReport(
            error_code=payload.payload.error.error_type,
            error_message=payload.payload.error.message,
            details=payload.payload.error.details,
        )
        cause, is_classified_retryable = classify_worker_failure(raw_report)
        is_retryable = evaluate_retry_eligibility(
            next_attempt_ordinal=task.next_attempt_ordinal,
            max_attempts=task.max_attempts,
            is_classified_retryable=is_classified_retryable,
            workflow_state=wf_state,
        )

        if is_retryable:
            retry_ready = now_utc + timedelta(seconds=5.0)
            outcome = await commit_worker_failure_with_retry(
                session=session,
                attempt_id=AttemptId(payload.attempt_id),
                worker_session_id=WorkerSessionId(payload.worker_session_id),
                expected_attempt_revision=attempt.revision,
                task_id=TaskExecutionId(task.task_execution_id),
                expected_task_revision=task.revision,
                workflow_id=WorkflowExecutionId(task.workflow_execution_id),
                ready_at_utc=retry_ready,
                cause=cause,
                now_utc=now_utc,
            )
            if outcome.status != CommitStatus.COMMITTED:
                raise ApiHttpException(
                    status_code=409,
                    code="STALE_ATTEMPT",
                    message=f"Retry rejected: {outcome.message}",
                )
            await session.commit()
        else:
            outcome, wf_id = await commit_worker_definitive_failure(
                session=session,
                attempt_id=AttemptId(payload.attempt_id),
                worker_session_id=WorkerSessionId(payload.worker_session_id),
                expected_attempt_revision=attempt.revision,
                task_id=TaskExecutionId(task.task_execution_id),
                expected_task_revision=task.revision,
                cause=cause,
                now_utc=now_utc,
            )
            if outcome.status != CommitStatus.COMMITTED:
                raise ApiHttpException(
                    status_code=409,
                    code="STALE_ATTEMPT",
                    message=f"Definitive failure rejected: {outcome.message}",
                )
            if wf_id:
                await commit_workflow_failure_direction(
                    session, WorkflowExecutionId(wf_id), cause, now_utc
                )
                await session.commit()
                await scheduler.drain_workflow(WorkflowExecutionId(wf_id))
            else:
                await session.commit()

    elif isinstance(payload.payload, ActivityCancelAckPayloadDTO):
        from nexusflow.persistence.transactions import commit_worker_cancellation_ack

        outcome, _ = await commit_worker_cancellation_ack(
            session=session,
            attempt_id=AttemptId(payload.attempt_id),
            worker_session_id=WorkerSessionId(payload.worker_session_id),
            expected_attempt_revision=attempt.revision,
            task_id=TaskExecutionId(task.task_execution_id),
            expected_task_revision=task.revision,
            now_utc=now_utc,
        )
        if outcome.status != CommitStatus.COMMITTED:
            raise ApiHttpException(
                status_code=409,
                code="STALE_ATTEMPT",
                message=f"Cancel ACK rejected: {outcome.message}",
            )
        await session.commit()
        await scheduler.drain_workflow(WorkflowExecutionId(task.workflow_execution_id))

    return WorkerCallbackResponseDTO(
        status="ACCEPTED",
        attempt_id=payload.attempt_id,
    )
