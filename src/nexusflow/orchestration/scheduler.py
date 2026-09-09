"""Transition-driven scheduler coordinating task readiness, capability matching, and ownership commits (LLD-04)."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexusflow.definition.codec import deserialize_validated_spec
from nexusflow.domain.execution import OutputCommitted
from nexusflow.domain.identifiers import (
    AttemptId,
    TaskDefinitionId,
    TaskExecutionId,
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
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_attempt_ownership,
    commit_task_readiness,
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

            if wf_row is None or wf_row.state != "RUNNING":
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
                await session.execute(
                    select(TaskExecutionRecord).where(
                        TaskExecutionRecord.workflow_execution_id == workflow_id.value
                    )
                )
            ).scalars().all()

        tasks_by_def_id = {row.task_definition_id: row for row in task_rows}

        # Check if all tasks have succeeded -> attempt workflow success
        all_succeeded = len(task_rows) == len(spec.tasks) and all(
            row.state == "SUCCEEDED" for row in task_rows
        )
        if all_succeeded:
            await self._try_succeed_workflow(wf_row, spec, task_rows)
            return

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
                await session.execute(
                    select(TaskExecutionRecord).where(
                        TaskExecutionRecord.workflow_execution_id == workflow_id.value,
                        TaskExecutionRecord.state == "RUNNABLE",
                    )
                )
            ).scalars().all()

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
