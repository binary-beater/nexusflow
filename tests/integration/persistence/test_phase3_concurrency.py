"""Integration tests for Phase 3 Concurrency and Race Conditions (LLD-02, LLD-06)."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.domain.enums import FailureCategory
from nexusflow.domain.failure import FailureCause
from nexusflow.domain.identifiers import (
    ActivityType,
    AttemptId,
    DefinitionId,
    TaskDefinitionId,
    TaskExecutionId,
    WorkerSessionId,
    WorkflowExecutionId,
)
from nexusflow.domain.spec import TaskDefinition, ValidatedWorkflowSpec
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_internal_attempt_failure,
    commit_worker_definitive_failure,
    commit_worker_task_success,
    commit_workflow_cancellation_direction,
    commit_workflow_failure_direction,
)

POSTGRES_TEST_URL = os.getenv(
    "NEXUSFLOW_DB__URL",
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow",
)


def make_spec(wf_name: str, task_name: str = "step") -> ValidatedWorkflowSpec:
    t_id = TaskDefinitionId(task_name)
    t_def = TaskDefinition(
        id=t_id,
        activity_type=ActivityType("compute.act"),
        dependencies=frozenset(),
        input_bindings={},
        max_attempts=3,
    )
    return ValidatedWorkflowSpec(
        workflow_name=wf_name,
        tasks={t_id: t_def},
        output_bindings={},
    )


@pytest.fixture
async def session_factory():
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker
    await engine.dispose()


@pytest.mark.asyncio
async def test_race_late_worker_success_vs_timeout_settlement(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Tests the race between a late worker SUCCESS callback and an engine EXECUTION_TIMEOUT sweep.

    Exactly one transaction must commit; the loser must receive OCC conflict or stale attempt.
    """
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_wf",
                validated_iws=serialize_validated_spec(make_spec("race_wf", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="RUNNING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t = TaskExecutionRecord(
            task_execution_id=t_id.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step",
            state="RUNNING",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=session_id.value,
            state="RUNNING",
            revision=1,
            start_deadline_utc=now_utc + timedelta(seconds=30),
            execution_timeout_utc=now_utc - timedelta(seconds=1),  # already expired
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        sess.add(att)
        await sess.flush()
        await sess.commit()

    barrier = asyncio.Barrier(2)

    from nexusflow.domain.execution import OutputCommitted

    async def worker_success_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_worker_task_success(
                session=sess,
                attempt_id=att_id,
                worker_session_id=session_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                output=OutputCommitted(value={"result": "ok"}),
                now_utc=datetime.now(UTC),
            )

    async def timeout_sweep_txn():
        await barrier.wait()
        cause = FailureCause(
            category=FailureCategory.TIME_BASED, code="EXECUTION_TIMEOUT", message="Timed out"
        )
        async with session_factory() as sess:
            outcome, _, _ = await commit_internal_attempt_failure(
                session=sess,
                attempt_id=att_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                cause=cause,
                is_retryable=True,
                retry_ready_at_utc=datetime.now(UTC) + timedelta(seconds=5),
                expected_lost_worker_session_id=None,
                now_utc=datetime.now(UTC),
            )
            return outcome

    results = await asyncio.gather(worker_success_txn(), timeout_sweep_txn())
    successes = [r for r in results if r.status == CommitStatus.COMMITTED]
    conflicts = [r for r in results if r.status == CommitStatus.OCC_CONFLICT]

    # Invariants: exactly one winner, exactly one loser
    assert len(successes) == 1
    assert len(conflicts) == 1


@pytest.mark.asyncio
async def test_race_workflow_cancellation_vs_task_definitive_failure(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Tests the race between public Cancel request and definitive task failure entering workflow direction.

    Narrow FOR UPDATE row lock serializes the direction:
    One enters FAILING or CANCELLING first; the second transaction sees the non-RUNNING state and fails precondition.
    """
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_wf_2",
                validated_iws=serialize_validated_spec(make_spec("race_wf_2", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="RUNNING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.commit()

    barrier = asyncio.Barrier(2)

    async def cancel_direction_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_workflow_cancellation_direction(
                session=sess,
                workflow_id=wf_id,
                now_utc=datetime.now(UTC),
            )

    async def failure_direction_txn():
        await barrier.wait()
        cause = FailureCause(
            category=FailureCategory.DOMAIN_EXECUTION, code="FATAL", message="Fatal"
        )
        async with session_factory() as sess:
            return await commit_workflow_failure_direction(
                session=sess,
                workflow_id=wf_id,
                cause=cause,
                now_utc=datetime.now(UTC),
            )

    results = await asyncio.gather(cancel_direction_txn(), failure_direction_txn())
    successes = [r for r in results if r.status == CommitStatus.COMMITTED]
    preconditions = [r for r in results if r.status == CommitStatus.PRECONDITION_FAILED]

    assert len(successes) == 1
    assert len(preconditions) == 1


@pytest.mark.asyncio
async def test_race_success_vs_worker_loss(session_factory: async_sessionmaker[AsyncSession]):
    """Tests the race between worker SUCCESS callback and internal WORKER_LOSS settlement."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_loss_wf",
                validated_iws=serialize_validated_spec(make_spec("race_loss_wf", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="RUNNING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t = TaskExecutionRecord(
            task_execution_id=t_id.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step",
            state="RUNNING",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=session_id.value,
            state="RUNNING",
            revision=1,
            start_deadline_utc=now_utc + timedelta(seconds=30),
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        sess.add(att)
        await sess.flush()
        await sess.commit()

    barrier = asyncio.Barrier(2)
    from nexusflow.domain.execution import OutputCommitted

    async def worker_success_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_worker_task_success(
                session=sess,
                attempt_id=att_id,
                worker_session_id=session_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                output=OutputCommitted(value={"result": "ok"}),
                now_utc=datetime.now(UTC),
            )

    async def worker_loss_txn():
        await barrier.wait()
        cause = FailureCause(
            category=FailureCategory.WORKER_AVAILABILITY, code="WORKER_LOSS", message="Lost"
        )
        async with session_factory() as sess:
            outcome, _, _ = await commit_internal_attempt_failure(
                session=sess,
                attempt_id=att_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                cause=cause,
                is_retryable=True,
                retry_ready_at_utc=datetime.now(UTC) + timedelta(seconds=5),
                expected_lost_worker_session_id=session_id,
                now_utc=datetime.now(UTC),
            )
            return outcome

    results = await asyncio.gather(worker_success_txn(), worker_loss_txn())
    successes = [r for r in results if r.status == CommitStatus.COMMITTED]
    conflicts = [r for r in results if r.status == CommitStatus.OCC_CONFLICT]

    assert len(successes) == 1
    assert len(conflicts) == 1


@pytest.mark.asyncio
async def test_race_success_vs_failure_callback(session_factory: async_sessionmaker[AsyncSession]):
    """Tests the race between concurrent worker success callback and worker failure callback."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_fail_wf",
                validated_iws=serialize_validated_spec(make_spec("race_fail_wf", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="RUNNING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t = TaskExecutionRecord(
            task_execution_id=t_id.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step",
            state="RUNNING",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=session_id.value,
            state="RUNNING",
            revision=1,
            start_deadline_utc=now_utc + timedelta(seconds=30),
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        sess.add(att)
        await sess.flush()
        await sess.commit()

    barrier = asyncio.Barrier(2)
    from nexusflow.domain.execution import OutputCommitted

    async def success_cb():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_worker_task_success(
                session=sess,
                attempt_id=att_id,
                worker_session_id=session_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                output=OutputCommitted(value={"result": "ok"}),
                now_utc=datetime.now(UTC),
            )

    async def fail_cb():
        await barrier.wait()
        cause = FailureCause(
            category=FailureCategory.DOMAIN_EXECUTION, code="STEP_FAILED", message="Failed"
        )
        async with session_factory() as sess:
            outcome, _ = await commit_worker_definitive_failure(
                session=sess,
                attempt_id=att_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                worker_session_id=session_id,
                cause=cause,
                now_utc=datetime.now(UTC),
            )
            return outcome

    results = await asyncio.gather(success_cb(), fail_cb())
    successes = [r for r in results if r.status == CommitStatus.COMMITTED]
    conflicts = [r for r in results if r.status == CommitStatus.OCC_CONFLICT]

    assert len(successes) == 1
    assert len(conflicts) == 1


@pytest.mark.asyncio
async def test_race_retry_ready_vs_cancellation(session_factory: async_sessionmaker[AsyncSession]):
    """Tests the race between retry-ready promotion to RUNNABLE and workflow cancellation drain."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_retry_cancel_wf",
                validated_iws=serialize_validated_spec(make_spec("race_retry_cancel_wf", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="CANCELLING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t = TaskExecutionRecord(
            task_execution_id=t_id.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step",
            state="RETRY_WAIT",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=2,
            retry_ready_at_utc=now_utc - timedelta(seconds=1),
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        await sess.commit()

    from nexusflow.persistence.transactions import (
        commit_drain_task_cancellation,
        commit_retry_ready,
    )

    barrier = asyncio.Barrier(2)

    async def retry_promote_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_retry_ready(
                session=sess,
                task_id=t_id,
                expected_task_revision=1,
                workflow_id=wf_id,
                now_utc=datetime.now(UTC),
            )

    async def drain_cancel_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_drain_task_cancellation(
                session=sess,
                task_id=t_id,
                expected_task_revision=1,
                now_utc=datetime.now(UTC),
            )

    results = await asyncio.gather(retry_promote_txn(), drain_cancel_txn())
    # In CANCELLING workflow, commit_retry_ready fails precondition (workflow is not RUNNING), while drain succeeds
    statuses = [r.status for r in results]
    assert CommitStatus.COMMITTED in statuses


@pytest.mark.asyncio
async def test_race_late_callback_vs_recovery_settlement(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Tests the race between a late callback for a historical session and startup recovery worker-loss settlement."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    historical_session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="race_recovery_cb_wf",
                validated_iws=serialize_validated_spec(make_spec("race_recovery_cb_wf", "step")),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="RUNNING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t = TaskExecutionRecord(
            task_execution_id=t_id.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step",
            state="RUNNING",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=historical_session_id.value,
            state="RUNNING",
            revision=1,
            start_deadline_utc=now_utc + timedelta(seconds=30),
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        sess.add(att)
        await sess.flush()
        await sess.commit()

    barrier = asyncio.Barrier(2)
    from nexusflow.domain.execution import OutputCommitted

    async def late_callback_txn():
        await barrier.wait()
        async with session_factory() as sess:
            return await commit_worker_task_success(
                session=sess,
                attempt_id=att_id,
                worker_session_id=historical_session_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                output=OutputCommitted(value={"recovered": True}),
                now_utc=datetime.now(UTC),
            )

    async def recovery_settlement_txn():
        await barrier.wait()
        cause = FailureCause(
            category=FailureCategory.WORKER_AVAILABILITY,
            code="WORKER_LOSS",
            message="Lost on reboot",
        )
        async with session_factory() as sess:
            outcome, _, _ = await commit_internal_attempt_failure(
                session=sess,
                attempt_id=att_id,
                expected_attempt_revision=1,
                task_id=t_id,
                expected_task_revision=1,
                cause=cause,
                is_retryable=True,
                retry_ready_at_utc=datetime.now(UTC) + timedelta(seconds=5),
                expected_lost_worker_session_id=historical_session_id,
                now_utc=datetime.now(UTC),
            )
            return outcome

    results = await asyncio.gather(late_callback_txn(), recovery_settlement_txn())
    successes = [r for r in results if r.status == CommitStatus.COMMITTED]
    conflicts = [r for r in results if r.status == CommitStatus.OCC_CONFLICT]

    # Exactly one commits, loser receives OCC conflict
    assert len(successes) == 1
    assert len(conflicts) == 1
