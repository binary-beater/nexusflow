"""Transition-driven scheduler coordinating task readiness, capability matching, and ownership commits (LLD-04)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexusflow.definition.codec import deserialize_validated_spec
from nexusflow.domain.enums import FailureCategory
from nexusflow.domain.execution import OutputCommitted
from nexusflow.domain.failure import FailureCause
from nexusflow.domain.identifiers import (
    AttemptId,
    TaskDefinitionId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import freeze_json
from nexusflow.domain.readiness import (
    DependencyStateSnapshot,
    TaskReadinessSnapshot,
    evaluate_task_readiness,
    resolve_workflow_outputs,
)
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_attempt_ownership,
    commit_drain_task_cancellation,
    commit_internal_attempt_failure,
    commit_internal_cancellation_deadline,
    commit_retry_ready,
    commit_task_readiness,
    commit_workflow_cancellation,
    commit_workflow_failure,
    commit_workflow_failure_direction,
    commit_workflow_success,
)


class ExecutionScheduler:
    """Transition-driven scheduler.

    Evaluates workflow tasks upon state transitions (e.g. initialization, task completion).
    Adheres strictly to OCC and exact capability matching without background poll loops.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        worker_registry: WorkerRegistry,
        start_deadline_seconds: float = 30.0,
    ) -> None:
        self._session_factory = session_factory
        self._worker_registry = worker_registry
        self._start_deadline_seconds = start_deadline_seconds

    async def advance_workflow(self, workflow_id: WorkflowExecutionId) -> None:
        """Evaluates readiness of PENDING tasks, assigns RUNNABLE tasks to live workers,

        and commits workflow success when all tasks have completed.
        """
        async with self._session_factory() as session:
            # 1. Fetch workflow execution and its registered definition
            wf_row = (
                await session.execute(
                    select(
                        WorkflowExecutionRecord.workflow_execution_id,
                        WorkflowExecutionRecord.definition_id,
                        WorkflowExecutionRecord.state,
                        WorkflowExecutionRecord.revision,
                        WorkflowExecutionRecord.workflow_input,
                    ).where(WorkflowExecutionRecord.workflow_execution_id == workflow_id.value)
                )
            ).one_or_none()

            if wf_row is None:
                return

            if wf_row.state in ["FAILING", "CANCELLING"]:
                await self.drain_workflow(workflow_id)
                return

            if wf_row.state != "RUNNING":
                return

            def_row = (
                await session.execute(
                    select(
                        RegisteredDefinitionRecord.validated_iws,
                    ).where(RegisteredDefinitionRecord.definition_id == wf_row.definition_id)
                )
            ).scalar_one_or_none()

            if def_row is None:
                return

            spec = deserialize_validated_spec(def_row)

            # 2. Fetch all tasks for this workflow
            task_rows = (
                (
                    await session.execute(
                        select(TaskExecutionRecord).where(
                            TaskExecutionRecord.workflow_execution_id == workflow_id.value
                        )
                    )
                )
                .scalars()
                .all()
            )

        tasks_by_def_id = {row.task_definition_id: row for row in task_rows}

        # Check if all tasks have succeeded -> attempt workflow success
        all_succeeded = len(task_rows) == len(spec.tasks) and all(
            row.state == "SUCCEEDED" for row in task_rows
        )
        if all_succeeded:
            await self._try_succeed_workflow(wf_row, spec, task_rows)
            return

        # Check for due RETRY_WAIT tasks and promote to RUNNABLE
        now_utc = datetime.now(UTC)
        for task_row in task_rows:
            if (
                task_row.state == "RETRY_WAIT"
                and task_row.retry_ready_at_utc
                and task_row.retry_ready_at_utc <= now_utc
            ):
                async with self._session_factory() as session:
                    await commit_retry_ready(
                        session=session,
                        task_id=TaskExecutionId(task_row.task_execution_id),
                        expected_task_revision=task_row.revision,
                        workflow_id=workflow_id,
                        now_utc=now_utc,
                    )

        # 3. For any task in PENDING, evaluate readiness
        for task_row in task_rows:
            if task_row.state == "PENDING":
                await self._evaluate_and_promote_task(wf_row, spec, task_row, tasks_by_def_id)

        # 4. Re-fetch runnable tasks and try dispatching to eligible workers
        await self._dispatch_runnable_tasks(workflow_id, spec)

    async def _evaluate_and_promote_task(
        self,
        wf_row: Any,
        spec: Any,
        task_row: TaskExecutionRecord,
        tasks_by_def_id: dict[str, TaskExecutionRecord],
    ) -> None:
        t_def_id = TaskDefinitionId(task_row.task_definition_id)
        task_def = spec.tasks.get(t_def_id)
        if task_def is None:
            return

        # Build upstream dependency snapshots
        dep_snapshots: dict[TaskDefinitionId, DependencyStateSnapshot] = {}
        for dep_id in task_def.dependencies:
            dep_row = tasks_by_def_id.get(dep_id.value)
            if dep_row is None:
                return
            dep_snapshots[dep_id] = DependencyStateSnapshot(
                task_definition_id=dep_id,
                state=dep_row.state,
                has_output=dep_row.has_output,
                output_value=freeze_json(dep_row.task_output) if dep_row.has_output else None,
            )

        snapshot = TaskReadinessSnapshot(
            workflow_id=WorkflowExecutionId(wf_row.workflow_execution_id),
            workflow_state=wf_row.state,
            workflow_input=freeze_json(wf_row.workflow_input),
            task_id=TaskExecutionId(task_row.task_execution_id),
            task_definition_id=t_def_id,
            task_state=task_row.state,
            task_revision=task_row.revision,
            task_definition=task_def,
            upstream_dependencies=dep_snapshots,
        )

        decision = evaluate_task_readiness(snapshot)
        if decision.ready and decision.resolved_input is not None:
            now_utc = datetime.now(UTC)
            async with self._session_factory() as session:
                await commit_task_readiness(
                    session=session,
                    task_id=TaskExecutionId(task_row.task_execution_id),
                    expected_task_revision=task_row.revision,
                    workflow_id=WorkflowExecutionId(wf_row.workflow_execution_id),
                    stable_input=decision.resolved_input,
                    now_utc=now_utc,
                )

    async def _dispatch_runnable_tasks(
        self,
        workflow_id: WorkflowExecutionId,
        spec: Any,
    ) -> None:
        """Finds RUNNABLE tasks and matches them with live workers."""
        async with self._session_factory() as session:
            runnable_tasks = (
                (
                    await session.execute(
                        select(TaskExecutionRecord).where(
                            TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                            TaskExecutionRecord.state == "RUNNABLE",
                        )
                    )
                )
                .scalars()
                .all()
            )

        for task_row in runnable_tasks:
            t_def_id = TaskDefinitionId(task_row.task_definition_id)
            task_def = spec.tasks.get(t_def_id)
            if task_def is None:
                continue

            worker = await self._worker_registry.find_eligible_worker_for_activity(
                task_def.activity_type
            )
            if worker is None:
                continue

            # Linearization point: atomic ownership commit in PostgreSQL
            now_utc = datetime.now(UTC)
            start_deadline = now_utc + timedelta(seconds=self._start_deadline_seconds)
            attempt_id = AttemptId.generate()

            async with self._session_factory() as session:
                outcome, ordinal = await commit_attempt_ownership(
                    session=session,
                    task_id=TaskExecutionId(task_row.task_execution_id),
                    expected_task_revision=task_row.revision,
                    workflow_id=workflow_id,
                    worker_session_id=worker.session_id,
                    new_attempt_id=attempt_id,
                    start_deadline_utc=start_deadline,
                    now_utc=now_utc,
                )

            if outcome.status == CommitStatus.COMMITTED and ordinal is not None:
                # Observe scheduling latency: time from RUNNABLE update to CLAIMED commit
                try:
                    from nexusflow.observability.metrics import SCHEDULING_LATENCY_SECONDS

                    if task_row.updated_at_utc:
                        # Handle naive or aware timestamps cleanly
                        ref_time = (
                            task_row.updated_at_utc.replace(tzinfo=UTC)
                            if task_row.updated_at_utc.tzinfo is None
                            else task_row.updated_at_utc
                        )
                        latency = (now_utc - ref_time).total_seconds()
                        if latency >= 0:
                            SCHEDULING_LATENCY_SECONDS.observe(latency)
                except Exception:
                    pass

                # Queue delivery hint to worker long-poll
                assignment_payload = {
                    "attempt_id": str(attempt_id.value),
                    "attempt_ordinal": ordinal,
                    "task_execution_id": str(task_row.task_execution_id),
                    "workflow_execution_id": str(workflow_id.value),
                    "worker_session_id": str(worker.session_id.value),
                    "activity_type": task_def.activity_type.name,
                    "stable_input": task_row.stable_input or {},
                    "start_deadline_utc": start_deadline.isoformat(),
                }
                await self._worker_registry.queue_delivery(
                    worker.session_id,
                    {"status": "ASSIGNMENT", "assignment": assignment_payload},
                )

    async def _try_succeed_workflow(
        self,
        wf_row: Any,
        spec: Any,
        task_rows: Sequence[TaskExecutionRecord],
    ) -> None:
        task_outputs: dict[TaskDefinitionId, Any] = {}
        for r in task_rows:
            t_id = TaskDefinitionId(r.task_definition_id)
            task_outputs[t_id] = freeze_json(r.task_output) if r.has_output else None

        resolved_output = resolve_workflow_outputs(spec.output_bindings, task_outputs)
        now_utc = datetime.now(UTC)

        async with self._session_factory() as session:
            await commit_workflow_success(
                session=session,
                workflow_id=WorkflowExecutionId(wf_row.workflow_execution_id),
                expected_workflow_revision=wf_row.revision,
                output=OutputCommitted(value=resolved_output),
                now_utc=now_utc,
            )

    async def drain_workflow(self, workflow_id: WorkflowExecutionId) -> None:
        """Processes workflow drain for FAILING and CANCELLING workflows (LLD-06 Section 9).

        1. Cancels unstarted tasks (PENDING, RUNNABLE, RETRY_WAIT).
        2. Populates cancellation deadlines and queues best-effort cancellation for active attempts.
        3. If all tasks are terminal, commits terminal workflow state (FAILED or CANCELLED).
        """
        async with self._session_factory() as session:
            wf_row = (
                await session.execute(
                    select(
                        WorkflowExecutionRecord.workflow_execution_id,
                        WorkflowExecutionRecord.state,
                        WorkflowExecutionRecord.revision,
                    ).where(WorkflowExecutionRecord.workflow_execution_id == workflow_id.value)
                )
            ).one_or_none()

            if wf_row is None or wf_row.state not in ["FAILING", "CANCELLING"]:
                return

            task_rows = (
                (
                    await session.execute(
                        select(TaskExecutionRecord).where(
                            TaskExecutionRecord.workflow_execution_id == workflow_id.value
                        )
                    )
                )
                .scalars()
                .all()
            )

        now_utc = datetime.now(UTC)

        # 1. Cancel unstarted tasks
        for t in task_rows:
            if t.state in ["PENDING", "RUNNABLE", "RETRY_WAIT"]:
                async with self._session_factory() as session:
                    await commit_drain_task_cancellation(
                        session=session,
                        task_id=TaskExecutionId(t.task_execution_id),
                        expected_task_revision=t.revision,
                        now_utc=now_utc,
                    )
                    await session.commit()

        # 2. Sweep active attempts for cancellation notice / deadline
        async with self._session_factory() as session:
            active_attempts = (
                (
                    await session.execute(
                        select(ExecutionAttemptRecord)
                        .join(
                            TaskExecutionRecord,
                            ExecutionAttemptRecord.task_execution_id
                            == TaskExecutionRecord.task_execution_id,
                        )
                        .where(
                            TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                            ExecutionAttemptRecord.state.in_(["CLAIMED", "RUNNING"]),
                        )
                    )
                )
                .scalars()
                .all()
            )

        for att in active_attempts:
            # If cancellation deadline has elapsed, settle internal cancellation
            if att.cancellation_deadline_utc and att.cancellation_deadline_utc <= now_utc:
                async with self._session_factory() as session:
                    task = await session.get(TaskExecutionRecord, att.task_execution_id)
                    if task and task.state == "RUNNING":
                        await commit_internal_cancellation_deadline(
                            session=session,
                            attempt_id=AttemptId(att.attempt_id),
                            expected_attempt_revision=att.revision,
                            task_id=TaskExecutionId(att.task_execution_id),
                            expected_task_revision=task.revision,
                            now_utc=now_utc,
                        )
                        await session.commit()
            elif not att.cancellation_deadline_utc:
                # Materialize cancellation deadline (10 seconds)
                cancel_deadline = now_utc + timedelta(seconds=10.0)
                async with self._session_factory() as session:
                    await session.execute(
                        update(ExecutionAttemptRecord)
                        .where(ExecutionAttemptRecord.attempt_id == att.attempt_id)
                        .values(cancellation_deadline_utc=cancel_deadline)
                    )
                    await session.commit()

                # Queue best-effort cancellation hint to worker
                cancel_cmd = {
                    "attempt_id": str(att.attempt_id),
                    "worker_session_id": str(att.worker_session_id),
                    "task_execution_id": str(att.task_execution_id),
                    "workflow_execution_id": str(workflow_id.value),
                    "reason": f"Workflow {wf_row.state.lower()}",
                }
                await self._worker_registry.queue_delivery(
                    WorkerSessionId(att.worker_session_id),
                    {"status": "CANCEL_COMMAND", "cancellation": cancel_cmd},
                )

        # 3. Check terminalization eligibility
        async with self._session_factory() as session:
            updated_tasks = (
                (
                    await session.execute(
                        select(TaskExecutionRecord).where(
                            TaskExecutionRecord.workflow_execution_id == workflow_id.value
                        )
                    )
                )
                .scalars()
                .all()
            )

            all_terminal = all(
                t.state in ["SUCCEEDED", "FAILED", "CANCELLED"] for t in updated_tasks
            )
            if all_terminal:
                current_wf = (
                    await session.execute(
                        select(WorkflowExecutionRecord).where(
                            WorkflowExecutionRecord.workflow_execution_id == workflow_id.value
                        )
                    )
                ).scalar_one_or_none()
                if current_wf is not None:
                    if current_wf.state == "FAILING":
                        await commit_workflow_failure(
                            session=session,
                            workflow_id=workflow_id,
                            expected_workflow_revision=current_wf.revision,
                            now_utc=now_utc,
                        )
                        await session.commit()
                    elif current_wf.state == "CANCELLING":
                        await commit_workflow_cancellation(
                            session=session,
                            workflow_id=workflow_id,
                            expected_workflow_revision=current_wf.revision,
                            now_utc=now_utc,
                        )
                        await session.commit()

    async def sweep_timeouts_and_deadlines(self) -> int:
        """Periodic / defensive sweeper for expired start deadlines, execution timeouts, and cancellation deadlines."""
        now_utc = datetime.now(UTC)
        settled_count = 0

        async with self._session_factory() as session:
            # 1. Expired start deadlines on CLAIMED attempts
            expired_claims = (
                await session.execute(
                    select(ExecutionAttemptRecord, TaskExecutionRecord)
                    .join(
                        TaskExecutionRecord,
                        ExecutionAttemptRecord.task_execution_id
                        == TaskExecutionRecord.task_execution_id,
                    )
                    .where(
                        ExecutionAttemptRecord.state == "CLAIMED",
                        ExecutionAttemptRecord.start_deadline_utc <= now_utc,
                    )
                    .limit(50)
                )
            ).all()

        for att, task in expired_claims:
            cause = FailureCause(
                category=FailureCategory.TIME_BASED,
                code="START_DEADLINE_EXPIRED",
                message="Worker did not acknowledge start before start_deadline_utc",
            )
            retry_ready = now_utc + timedelta(seconds=5.0)
            async with self._session_factory() as session:
                outcome, wf_id, new_state = await commit_internal_attempt_failure(
                    session=session,
                    attempt_id=AttemptId(att.attempt_id),
                    expected_attempt_revision=att.revision,
                    task_id=TaskExecutionId(task.task_execution_id),
                    expected_task_revision=task.revision,
                    cause=cause,
                    is_retryable=True,
                    retry_ready_at_utc=retry_ready,
                    expected_lost_worker_session_id=None,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED and wf_id:
                    settled_count += 1
                    if new_state == "FAILED":
                        await commit_workflow_failure_direction(
                            session, WorkflowExecutionId(wf_id), cause, now_utc
                        )
                        await self.drain_workflow(WorkflowExecutionId(wf_id))

        async with self._session_factory() as session:
            # 2. Expired execution timeouts on RUNNING attempts
            expired_timeouts = (
                await session.execute(
                    select(ExecutionAttemptRecord, TaskExecutionRecord)
                    .join(
                        TaskExecutionRecord,
                        ExecutionAttemptRecord.task_execution_id
                        == TaskExecutionRecord.task_execution_id,
                    )
                    .where(
                        ExecutionAttemptRecord.state == "RUNNING",
                        ExecutionAttemptRecord.execution_timeout_utc.is_not(None),
                        ExecutionAttemptRecord.execution_timeout_utc <= now_utc,
                    )
                    .limit(50)
                )
            ).all()

        for att, task in expired_timeouts:
            cause = FailureCause(
                category=FailureCategory.TIME_BASED,
                code="EXECUTION_TIMEOUT",
                message="Activity execution exceeded configured execution timeout",
            )
            retry_ready = now_utc + timedelta(seconds=5.0)
            async with self._session_factory() as session:
                outcome, wf_id, new_state = await commit_internal_attempt_failure(
                    session=session,
                    attempt_id=AttemptId(att.attempt_id),
                    expected_attempt_revision=att.revision,
                    task_id=TaskExecutionId(task.task_execution_id),
                    expected_task_revision=task.revision,
                    cause=cause,
                    is_retryable=True,
                    retry_ready_at_utc=retry_ready,
                    expected_lost_worker_session_id=None,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED and wf_id:
                    settled_count += 1
                    if new_state == "FAILED":
                        await commit_workflow_failure_direction(
                            session, WorkflowExecutionId(wf_id), cause, now_utc
                        )
                        await self.drain_workflow(WorkflowExecutionId(wf_id))

        return settled_count

    async def sweep_worker_loss(self) -> int:
        """Arbitrates worker session loss for active attempts owned by non-live sessions."""
        now_utc = datetime.now(UTC)
        lost_sessions = await self._worker_registry.find_lost_sessions()
        if not lost_sessions:
            return 0

        settled = 0
        lost_ids = [s.value for s in lost_sessions]

        async with self._session_factory() as session:
            orphan_attempts = (
                await session.execute(
                    select(ExecutionAttemptRecord, TaskExecutionRecord)
                    .join(
                        TaskExecutionRecord,
                        ExecutionAttemptRecord.task_execution_id
                        == TaskExecutionRecord.task_execution_id,
                    )
                    .where(
                        ExecutionAttemptRecord.worker_session_id.in_(lost_ids),
                        ExecutionAttemptRecord.state.in_(["CLAIMED", "RUNNING"]),
                    )
                    .limit(50)
                )
            ).all()

        for att, task in orphan_attempts:
            cause = FailureCause(
                category=FailureCategory.WORKER_AVAILABILITY,
                code="WORKER_LOSS",
                message="Worker session heartbeat timed out / lost",
            )
            retry_ready = now_utc + timedelta(seconds=5.0)
            async with self._session_factory() as session:
                outcome, wf_id, new_state = await commit_internal_attempt_failure(
                    session=session,
                    attempt_id=AttemptId(att.attempt_id),
                    expected_attempt_revision=att.revision,
                    task_id=TaskExecutionId(task.task_execution_id),
                    expected_task_revision=task.revision,
                    cause=cause,
                    is_retryable=True,
                    retry_ready_at_utc=retry_ready,
                    expected_lost_worker_session_id=WorkerSessionId(att.worker_session_id),
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED and wf_id:
                    settled += 1
                    if new_state == "FAILED":
                        await commit_workflow_failure_direction(
                            session, WorkflowExecutionId(wf_id), cause, now_utc
                        )
                        await self.drain_workflow(WorkflowExecutionId(wf_id))

        return settled
