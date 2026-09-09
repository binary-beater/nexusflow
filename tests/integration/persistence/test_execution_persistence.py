"""PostgreSQL 16 integration tests for execution persistence transactions (LLD-02)."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.domain.enums import WorkflowState
from nexusflow.domain.execution import OutputAbsent, OutputCommitted, WorkflowExecution
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
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_attempt_ownership,
    commit_initialization_complete,
    commit_task_population,
    commit_task_readiness,
    commit_worker_execution_start,
    commit_worker_task_success,
    commit_workflow_creation,
    commit_workflow_success,
)

POSTGRES_TEST_URL = os.getenv(
    "NEXUSFLOW_DB__URL",
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow",
)


@pytest.fixture
async def session_factory():
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker
    await engine.dispose()


@pytest.mark.asyncio
async def test_execution_persistence_happy_path(session_factory: async_sessionmaker[AsyncSession]):
    now_utc = datetime.now(UTC)
    def_id = DefinitionId.generate()
    t_id_1 = TaskDefinitionId("task_step_1")

    # 1. Create registered definition directly in PostgreSQL
    task_def = TaskDefinition(
        id=t_id_1,
        activity_type=ActivityType("compute.add"),
        dependencies=frozenset(),
        input_bindings={},
        max_attempts=3,
    )
    spec = ValidatedWorkflowSpec(
        workflow_name="single_step_wf",
        tasks={t_id_1: task_def},
        output_bindings={},
    )

    async with session_factory() as session:
        async with session.begin():
            session.add(
                RegisteredDefinitionRecord(
                    definition_id=def_id.value,
                    workflow_name="single_step_wf",
                    validated_iws=serialize_validated_spec(spec),
                    raw_yaml=None,
                    created_at_utc=now_utc,
                )
            )

    # 2. Commit workflow creation
    wf_id = WorkflowExecutionId.generate()
    wf_entity = WorkflowExecution(
        id=wf_id,
        definition_id=def_id,
        state=WorkflowState.INITIALIZING,
        revision=1,
        workflow_input={"num": 10},
        output=OutputAbsent(),
        created_at_utc=now_utc,
        updated_at_utc=now_utc,
    )
    async with session_factory() as session:
        outcome, actual_wf_id = await commit_workflow_creation(
            session=session,
            execution=wf_entity,
            idempotency_key=None,
            fingerprint=None,
            now_utc=now_utc,
        )
        assert outcome.status == CommitStatus.COMMITTED
        assert actual_wf_id == wf_id

    # 3. Commit task population
    async with session_factory() as session:
        pop_outcome = await commit_task_population(
            session=session,
            workflow_id=wf_id,
            task_definitions=[task_def],
            now_utc=now_utc,
        )
        assert pop_outcome.status == CommitStatus.COMMITTED

    # 4. Commit initialization complete
    async with session_factory() as session:
        init_outcome = await commit_initialization_complete(
            session=session,
            workflow_id=wf_id,
            expected_revision=1,
            now_utc=now_utc,
        )
        assert init_outcome.status == CommitStatus.COMMITTED

    # Verify workflow state in DB is RUNNING
    async with session_factory() as session:
        wf_rec = await session.scalar(
            select(WorkflowExecutionRecord).where(
                WorkflowExecutionRecord.workflow_execution_id == wf_id.value
            )
        )
        assert wf_rec is not None
        assert wf_rec.state == "RUNNING"
        assert wf_rec.revision == 2

        task_rec = await session.scalar(
            select(TaskExecutionRecord).where(
                TaskExecutionRecord.workflow_execution_id == wf_id.value
            )
        )
        assert task_rec is not None
        assert task_rec.state == "PENDING"
        task_id = TaskExecutionId(task_rec.task_execution_id)

    # 5. Commit task readiness (PENDING -> RUNNABLE)
    async with session_factory() as session:
        readiness_outcome = await commit_task_readiness(
            session=session,
            task_id=task_id,
            expected_task_revision=1,
            workflow_id=wf_id,
            stable_input={"num": 10},
            now_utc=now_utc,
        )
        assert readiness_outcome.status == CommitStatus.COMMITTED

    # 6. Commit attempt ownership (RUNNABLE -> RUNNING, creates CLAIMED attempt)
    worker_session_id = WorkerSessionId.generate()
    attempt_id = AttemptId.generate()
    start_deadline = now_utc + timedelta(seconds=30)
    async with session_factory() as session:
        claim_outcome, ordinal = await commit_attempt_ownership(
            session=session,
            task_id=task_id,
            expected_task_revision=2,
            workflow_id=wf_id,
            worker_session_id=worker_session_id,
            new_attempt_id=attempt_id,
            start_deadline_utc=start_deadline,
            now_utc=now_utc,
        )
        assert claim_outcome.status == CommitStatus.COMMITTED
        assert ordinal == 1

    # 7. Commit worker execution start (CLAIMED -> RUNNING)
    async with session_factory() as session:
        start_outcome = await commit_worker_execution_start(
            session=session,
            attempt_id=attempt_id,
            worker_session_id=worker_session_id,
            expected_attempt_revision=1,
            now_utc=now_utc,
        )
        assert start_outcome.status == CommitStatus.COMMITTED

    # 8. Commit worker task success (Attempt & Task -> SUCCEEDED)
    async with session_factory() as session:
        success_outcome = await commit_worker_task_success(
            session=session,
            attempt_id=attempt_id,
            worker_session_id=worker_session_id,
            expected_attempt_revision=2,
            task_id=task_id,
            expected_task_revision=3,
            output=OutputCommitted(value={"result": 20}),
            now_utc=now_utc,
        )
        assert success_outcome.status == CommitStatus.COMMITTED

    # 9. Commit workflow success (RUNNING -> SUCCEEDED)
    async with session_factory() as session:
        wf_success_outcome = await commit_workflow_success(
            session=session,
            workflow_id=wf_id,
            expected_workflow_revision=2,
            output=OutputCommitted(value={"final": 20}),
            now_utc=now_utc,
        )
        assert wf_success_outcome.status == CommitStatus.COMMITTED

    # Verify terminal state in DB
    async with session_factory() as session:
        final_wf = await session.scalar(
            select(WorkflowExecutionRecord).where(
                WorkflowExecutionRecord.workflow_execution_id == wf_id.value
            )
        )
        assert final_wf is not None
        assert final_wf.state == "SUCCEEDED"
        assert final_wf.has_output is True
        assert final_wf.workflow_output == {"final": 20}
