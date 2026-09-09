import os
from collections.abc import AsyncGenerator
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
    commit_retry_ready,
    commit_worker_definitive_failure,
    commit_worker_failure_with_retry,
    commit_workflow_cancellation,
    commit_workflow_cancellation_direction,
    commit_workflow_failure,
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
async def session() -> AsyncGenerator[AsyncSession, None]:
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    async with session_maker() as sess:
        yield sess
    await engine.dispose()


@pytest.mark.asyncio
async def test_retry_settlement_and_promotion(session: AsyncSession):
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    session_id = WorkerSessionId.generate()

    def_id = DefinitionId.generate()
    session.add(
        RegisteredDefinitionRecord(
            definition_id=def_id.value,
            workflow_name="test_wf",
            validated_iws=serialize_validated_spec(make_spec("test_wf", "test.step")),
            raw_yaml=None,
            created_at_utc=now_utc,
        )
    )
    # Seed execution
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
        task_definition_id="test.step",
        state="RUNNING",
        revision=1,
        has_input=True,
        stable_input={},
        has_output=False,
        max_attempts=3,
        next_attempt_ordinal=2,
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
    session.add(wf)
    await session.flush()
    session.add(t)
    await session.flush()
    session.add(att)
    await session.flush()
    await session.commit()

    # 1. Commit worker failure with retry
    cause = FailureCause(category=FailureCategory.DOMAIN_EXECUTION, code="TRANSIENT_ERR", message="Transient fail")
    retry_ready = now_utc + timedelta(seconds=5.0)

    outcome = await commit_worker_failure_with_retry(
        session=session,
        attempt_id=att_id,
        expected_attempt_revision=1,
        task_id=t_id,
        expected_task_revision=1,
        workflow_id=wf_id,
        worker_session_id=session_id,
        cause=cause,
        ready_at_utc=retry_ready,
        now_utc=now_utc,
    )
    assert outcome.status == CommitStatus.COMMITTED
    await session.commit()

    # Verify task state is RETRY_WAIT and next_attempt_ordinal is 2
    t_row = await session.get(TaskExecutionRecord, t_id.value)
    assert t_row is not None
    assert t_row.state == "RETRY_WAIT"
    assert t_row.next_attempt_ordinal == 2

    # 2. Promote retry when ready
    outcome_ready = await commit_retry_ready(
        session=session,
        task_id=t_id,
        expected_task_revision=t_row.revision,
        workflow_id=wf_id,
        now_utc=now_utc + timedelta(seconds=6.0),
    )
    assert outcome_ready.status == CommitStatus.COMMITTED
    t_row = await session.get(TaskExecutionRecord, t_id.value)
    assert t_row is not None
    await session.refresh(t_row)
    assert t_row.state == "RUNNABLE"


@pytest.mark.asyncio
async def test_definitive_failure_and_workflow_direction(session: AsyncSession):
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    session_id = WorkerSessionId.generate()

    def_id = DefinitionId.generate()
    session.add(
        RegisteredDefinitionRecord(
            definition_id=def_id.value,
            workflow_name="test_wf_2",
            validated_iws=serialize_validated_spec(make_spec("test_wf_2", "fatal.step")),
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
        task_definition_id="fatal.step",
        state="RUNNING",
        revision=1,
        has_input=True,
        stable_input={},
        has_output=False,
        max_attempts=1,
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
    session.add(wf)
    await session.flush()
    session.add(t)
    await session.flush()
    session.add(att)
    await session.flush()
    await session.commit()

    cause = FailureCause(category=FailureCategory.DOMAIN_EXECUTION, code="FATAL_ERR", message="Fatal fail")
    outcome, triggered_wf_id = await commit_worker_definitive_failure(
        session=session,
        attempt_id=att_id,
        expected_attempt_revision=1,
        task_id=t_id,
        expected_task_revision=1,
        worker_session_id=session_id,
        cause=cause,
        now_utc=now_utc,
    )
    assert outcome.status == CommitStatus.COMMITTED
    assert triggered_wf_id == wf_id.value
    await session.commit()

    # Direction serialization: RUNNING -> FAILING
    dir_outcome = await commit_workflow_failure_direction(
        session=session,
        workflow_id=wf_id,
        cause=cause,
        now_utc=now_utc,
    )
    assert dir_outcome.status == CommitStatus.COMMITTED
    await session.commit()

    wf_row = await session.get(WorkflowExecutionRecord, wf_id.value)
    assert wf_row is not None
    assert wf_row.state == "FAILING"

    # Terminalize workflow: FAILING -> FAILED
    term_outcome = await commit_workflow_failure(
        session=session,
        workflow_id=wf_id,
        expected_workflow_revision=wf_row.revision,
        now_utc=now_utc,
    )
    assert term_outcome.status == CommitStatus.COMMITTED
    await session.commit()

    wf_row = await session.get(WorkflowExecutionRecord, wf_id.value)
    assert wf_row is not None
    assert wf_row.state == "FAILED"


@pytest.mark.asyncio
async def test_workflow_cancellation_lifecycle(session: AsyncSession):
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    def_id = DefinitionId.generate()
    session.add(
        RegisteredDefinitionRecord(
            definition_id=def_id.value,
            workflow_name="test_wf_3",
            validated_iws=serialize_validated_spec(make_spec("test_wf_3", "step")),
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
        task_execution_id=TaskExecutionId.generate().value,
        workflow_execution_id=wf_id.value,
        task_definition_id="step",
        state="CANCELLED",
        revision=1,
        has_input=True,
        stable_input={},
        has_output=False,
        max_attempts=3,
        next_attempt_ordinal=1,
        created_at_utc=now_utc,
        updated_at_utc=now_utc,
    )
    session.add(wf)
    await session.flush()
    session.add(t)
    await session.commit()

    # Cancel direction: RUNNING -> CANCELLING
    dir_outcome = await commit_workflow_cancellation_direction(
        session=session,
        workflow_id=wf_id,
        now_utc=now_utc,
    )
    assert dir_outcome.status == CommitStatus.COMMITTED
    await session.commit()

    wf_row = await session.get(WorkflowExecutionRecord, wf_id.value)
    assert wf_row is not None
    assert wf_row.state == "CANCELLING"

    # Terminalize cancellation: CANCELLING -> CANCELLED
    term_outcome = await commit_workflow_cancellation(
        session=session,
        workflow_id=wf_id,
        expected_workflow_revision=wf_row.revision,
        now_utc=now_utc,
    )
    assert term_outcome.status == CommitStatus.COMMITTED
    await session.commit()

    wf_row = await session.get(WorkflowExecutionRecord, wf_id.value)
    assert wf_row is not None
    assert wf_row.state == "CANCELLED"
