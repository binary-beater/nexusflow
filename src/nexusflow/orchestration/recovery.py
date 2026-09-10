"""NexusFlow V1 Startup Recovery & Reconciliation Engine.

Implements the 8 deterministic reconciliation phases defined in LLD-07:
1. Partial INITIALIZING repair
2. Draining workflows (FAILING & CANCELLING)
3. Active CLAIMED attempts (expired deadlines or dead pre-restart sessions)
4. Active RUNNING attempts (settle timeouts / dead sessions while allowing late callbacks to race)
5. Due RETRY_WAIT tasks (promote to RUNNABLE)
6. Durable RUNNABLE tasks (re-queue / prepare for scheduler)
7. PENDING readiness repair (resolve lost wakeups)
8. Terminalization repair (workflows where all tasks reached terminal state)
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexusflow.config.settings import NexusFlowSettings
from nexusflow.domain.enums import FailureCategory
from nexusflow.domain.execution import OutputCommitted
from nexusflow.domain.failure import FailureCause
from nexusflow.domain.identifiers import (
    AttemptId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.json_compat import freeze_json
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_internal_attempt_failure,
    commit_retry_ready,
    commit_workflow_cancellation,
    commit_workflow_failure,
    commit_workflow_failure_direction,
    commit_workflow_success,
)

logger = logging.getLogger(__name__)


class StartupRecoveryEngine:
    """Deterministic bounded recovery engine executed at application boot before accepting traffic."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        scheduler: ExecutionScheduler,
        worker_registry: WorkerRegistry,
        settings: NexusFlowSettings | None = None,
    ) -> None:
        self.session_factory = session_factory
        self.scheduler = scheduler
        self.worker_registry = worker_registry
        self.settings = settings or NexusFlowSettings()
        self.batch_size = self.settings.recovery.keyset_batch_size
        self.max_passes = self.settings.recovery.convergence_max_passes

    async def recover_system(self) -> None:
        """Run the bounded 8-phase convergence loop until a clean pass is achieved or max_passes reached."""
        logger.info(
            "startup_recovery_started: max_passes=%s, batch_size=%s",
            self.max_passes,
            self.batch_size,
        )

        for pass_idx in range(1, self.max_passes + 1):
            logger.info("startup_recovery_pass_started: pass_num=%s", pass_idx)
            mutations = 0

            async with self.session_factory() as session:
                mutations += await self._phase_1_repair_initializing(session)
                mutations += await self._phase_2_resume_draining(session)
                mutations += await self._phase_3_settle_claimed_attempts(session)
                mutations += await self._phase_4_settle_running_attempts(session)
                mutations += await self._phase_5_promote_retry_wait(session)
                mutations += await self._phase_6_rediscover_runnable(session)
                mutations += await self._phase_7_repair_pending_readiness(session)
                mutations += await self._phase_8_repair_terminalization(session)
                await session.commit()

            logger.info(
                "startup_recovery_pass_completed: pass_num=%s, mutations=%s", pass_idx, mutations
            )
            if mutations == 0:
                logger.info("startup_recovery_converged: passes=%s", pass_idx)
                return

        logger.warning("startup_recovery_max_passes_reached: max_passes=%s", self.max_passes)

    async def _phase_1_repair_initializing(self, session: AsyncSession) -> int:
        """Phase 1: Detect INITIALIZING workflows; verify task executions and advance to RUNNING if complete."""
        stmt = (
            select(WorkflowExecutionRecord)
            .where(WorkflowExecutionRecord.state == "INITIALIZING")
            .order_by(WorkflowExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        mutations = 0

        for wf in records:
            wf_id = WorkflowExecutionId(wf.workflow_execution_id)
            t_stmt = select(TaskExecutionRecord).where(
                TaskExecutionRecord.workflow_execution_id == wf.workflow_execution_id
            )
            t_res = await session.execute(t_stmt)
            tasks = t_res.scalars().all()

            if tasks:
                wf.state = "RUNNING"
                wf.revision += 1
                wf.updated_at_utc = datetime.now(UTC)
                await session.commit()
                mutations += 1
                await self.scheduler.advance_workflow(wf_id)

        return mutations

    async def _phase_2_resume_draining(self, session: AsyncSession) -> int:
        """Phase 2: Resume draining for FAILING and CANCELLING workflows."""
        stmt = (
            select(WorkflowExecutionRecord)
            .where(WorkflowExecutionRecord.state.in_(["FAILING", "CANCELLING"]))
            .order_by(WorkflowExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        mutations = 0

        for wf in records:
            wf_id = WorkflowExecutionId(wf.workflow_execution_id)
            drained = await self.scheduler.drain_workflow(wf_id)
            if drained:
                mutations += 1

        return mutations

    async def _phase_3_settle_claimed_attempts(self, session: AsyncSession) -> int:
        """Phase 3: Settle active CLAIMED attempts whose start deadline expired or pre-restart session is dead."""
        stmt = (
            select(ExecutionAttemptRecord)
            .where(ExecutionAttemptRecord.state == "CLAIMED")
            .order_by(ExecutionAttemptRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        now_utc = datetime.now(UTC)
        mutations = 0

        for att in records:
            t_rec = await session.get(TaskExecutionRecord, att.task_execution_id)
            if not t_rec:
                continue

            is_dead_session = att.worker_session_id not in self.worker_registry._sessions
            is_expired = att.start_deadline_utc <= now_utc

            if is_dead_session or is_expired:
                cause = FailureCause(
                    category=FailureCategory.WORKER_AVAILABILITY
                    if is_dead_session
                    else FailureCategory.TIME_BASED,
                    code="WORKER_LOSS" if is_dead_session else "START_DEADLINE_EXPIRED",
                    message="Claimed attempt expired or worker session lost across restart",
                )
                retry_ready = now_utc + timedelta(seconds=self.settings.retry.fixed_delay_seconds)
                outcome, wf_id, new_state = await commit_internal_attempt_failure(
                    session=session,
                    attempt_id=AttemptId(att.attempt_id),
                    expected_attempt_revision=att.revision,
                    task_id=TaskExecutionId(t_rec.task_execution_id),
                    expected_task_revision=t_rec.revision,
                    cause=cause,
                    is_retryable=True,
                    retry_ready_at_utc=retry_ready,
                    expected_lost_worker_session_id=WorkerSessionId(att.worker_session_id)
                    if is_dead_session
                    else None,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED:
                    mutations += 1
                    if wf_id and new_state == "FAILED":
                        await commit_workflow_failure_direction(
                            session, WorkflowExecutionId(wf_id), cause, now_utc
                        )
                        await self.scheduler.drain_workflow(WorkflowExecutionId(wf_id))

        return mutations

    async def _phase_4_settle_running_attempts(self, session: AsyncSession) -> int:
        """Phase 4: Settle active RUNNING attempts whose timeout expired or worker session was lost."""
        stmt = (
            select(ExecutionAttemptRecord)
            .where(ExecutionAttemptRecord.state == "RUNNING")
            .order_by(ExecutionAttemptRecord.attempt_id.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        now_utc = datetime.now(UTC)
        mutations = 0

        for att in records:
            t_rec = await session.get(TaskExecutionRecord, att.task_execution_id)
            if not t_rec:
                continue

            is_dead_session = att.worker_session_id not in self.worker_registry._sessions
            is_timeout = (
                att.execution_timeout_utc is not None and att.execution_timeout_utc <= now_utc
            )

            if is_dead_session or is_timeout:
                cause = FailureCause(
                    category=FailureCategory.TIME_BASED
                    if is_timeout
                    else FailureCategory.WORKER_AVAILABILITY,
                    code="EXECUTION_TIMEOUT" if is_timeout else "WORKER_LOSS",
                    message="Task execution timeout expired or worker session lost across restart",
                )
                retry_ready = now_utc + timedelta(seconds=self.settings.retry.fixed_delay_seconds)
                outcome, wf_id, new_state = await commit_internal_attempt_failure(
                    session=session,
                    attempt_id=AttemptId(att.attempt_id),
                    expected_attempt_revision=att.revision,
                    task_id=TaskExecutionId(t_rec.task_execution_id),
                    expected_task_revision=t_rec.revision,
                    cause=cause,
                    is_retryable=True,
                    retry_ready_at_utc=retry_ready,
                    expected_lost_worker_session_id=WorkerSessionId(att.worker_session_id)
                    if is_dead_session
                    else None,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED:
                    mutations += 1
                    if wf_id and new_state == "FAILED":
                        await commit_workflow_failure_direction(
                            session, WorkflowExecutionId(wf_id), cause, now_utc
                        )
                        await self.scheduler.drain_workflow(WorkflowExecutionId(wf_id))

        return mutations

    async def _phase_5_promote_retry_wait(self, session: AsyncSession) -> int:
        """Phase 5: Promote due RETRY_WAIT tasks to RUNNABLE."""
        now_utc = datetime.now(UTC)
        stmt = (
            select(TaskExecutionRecord)
            .where(
                TaskExecutionRecord.state == "RETRY_WAIT",
                TaskExecutionRecord.retry_ready_at_utc <= now_utc,
            )
            .order_by(TaskExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        mutations = 0

        for t_rec in records:
            outcome = await commit_retry_ready(
                session=session,
                task_id=TaskExecutionId(t_rec.task_execution_id),
                expected_task_revision=t_rec.revision,
                workflow_id=WorkflowExecutionId(t_rec.workflow_execution_id),
                now_utc=now_utc,
            )
            if outcome.status == CommitStatus.COMMITTED:
                mutations += 1
                await session.commit()
                await self.scheduler.advance_workflow(
                    WorkflowExecutionId(t_rec.workflow_execution_id)
                )

        return mutations

    async def _phase_6_rediscover_runnable(self, session: AsyncSession) -> int:
        """Phase 6: Re-queue or wake scheduler for durable RUNNABLE tasks without active attempts."""
        stmt = (
            select(TaskExecutionRecord)
            .where(
                TaskExecutionRecord.state == "RUNNABLE",
            )
            .order_by(TaskExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        for t_rec in records:
            await self.scheduler.advance_workflow(WorkflowExecutionId(t_rec.workflow_execution_id))
        return 0

    async def _phase_7_repair_pending_readiness(self, session: AsyncSession) -> int:
        """Phase 7: Settle lost wakeups for PENDING tasks whose upstream dependencies succeeded."""
        stmt = (
            select(TaskExecutionRecord)
            .where(TaskExecutionRecord.state == "PENDING")
            .order_by(TaskExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        seen_wf_ids = set()

        for t_rec in records:
            wf_id = WorkflowExecutionId(t_rec.workflow_execution_id)
            if wf_id not in seen_wf_ids:
                seen_wf_ids.add(wf_id)
                await self.scheduler.advance_workflow(wf_id)

        return 0

    async def _phase_8_repair_terminalization(self, session: AsyncSession) -> int:
        """Phase 8: Terminalize workflows where all tasks are in terminal states."""
        stmt = (
            select(WorkflowExecutionRecord)
            .where(WorkflowExecutionRecord.state.in_(["RUNNING", "FAILING", "CANCELLING"]))
            .order_by(WorkflowExecutionRecord.created_at_utc.asc())
            .limit(self.batch_size)
        )
        res = await session.execute(stmt)
        records = res.scalars().all()
        now_utc = datetime.now(UTC)
        mutations = 0

        for wf in records:
            wf_id = WorkflowExecutionId(wf.workflow_execution_id)
            t_stmt = select(TaskExecutionRecord).where(
                TaskExecutionRecord.workflow_execution_id == wf.workflow_execution_id
            )
            t_res = await session.execute(t_stmt)
            tasks = t_res.scalars().all()

            if not tasks:
                continue

            all_terminal = all(t.state in ["SUCCEEDED", "FAILED", "CANCELLED"] for t in tasks)

            if not all_terminal:
                continue

            if wf.state == "FAILING":
                failed_tasks = [t for t in tasks if t.state == "FAILED"]
                cause = FailureCause(
                    category=FailureCategory(failed_tasks[0].failure_category)
                    if (failed_tasks and failed_tasks[0].failure_category)
                    else FailureCategory.SYSTEM_PERMANENT,
                    code=failed_tasks[0].failure_code
                    if failed_tasks and failed_tasks[0].failure_code
                    else "TERMINALIZATION_FAILURE",
                    message=failed_tasks[0].failure_message
                    if failed_tasks and failed_tasks[0].failure_message
                    else "Task failed during execution",
                )
                outcome = await commit_workflow_failure(
                    session=session,
                    workflow_id=wf_id,
                    expected_workflow_revision=wf.revision,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED:
                    mutations += 1

            elif wf.state == "CANCELLING":
                outcome = await commit_workflow_cancellation(
                    session=session,
                    workflow_id=wf_id,
                    expected_workflow_revision=wf.revision,
                    now_utc=now_utc,
                )
                if outcome.status == CommitStatus.COMMITTED:
                    mutations += 1

            elif wf.state == "RUNNING":
                if any(t.state == "FAILED" for t in tasks):
                    failed_tasks = [t for t in tasks if t.state == "FAILED"]
                    cause = FailureCause(
                        category=FailureCategory(failed_tasks[0].failure_category)
                        if (failed_tasks and failed_tasks[0].failure_category)
                        else FailureCategory.SYSTEM_PERMANENT,
                        code=failed_tasks[0].failure_code
                        if failed_tasks and failed_tasks[0].failure_code
                        else "TERMINALIZATION_FAILURE",
                        message=failed_tasks[0].failure_message
                        if failed_tasks and failed_tasks[0].failure_message
                        else "Task failed during execution",
                    )
                    outcome = await commit_workflow_failure_direction(
                        session=session,
                        workflow_id=wf_id,
                        cause=cause,
                        now_utc=now_utc,
                    )
                    if outcome.status == CommitStatus.COMMITTED:
                        mutations += 1
                        await self.scheduler.drain_workflow(wf_id)
                elif all(t.state == "SUCCEEDED" for t in tasks):
                    outcome = await commit_workflow_success(
                        session=session,
                        workflow_id=wf_id,
                        expected_workflow_revision=wf.revision,
                        output=OutputCommitted(value=freeze_json({})),
                        now_utc=now_utc,
                    )
                    if outcome.status == CommitStatus.COMMITTED:
                        mutations += 1

        return mutations
