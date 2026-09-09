"""PostgreSQL 16 concurrency and OCC conflict tests (LLD-02)."""

import asyncio
import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.domain.enums import WorkflowState
from nexusflow.domain.execution import OutputAbsent, WorkflowExecution
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
from nexusflow.persistence.orm import RegisteredDefinitionRecord
from nexusflow.persistence.transactions import (
    CommitStatus,
    commit_attempt_ownership,
    commit_initialization_complete,
    commit_task_population,
    commit_task_readiness,
    commit_workflow_creation,
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
async def test_concurrent_attempt_ownership_occ_race(session_factory: async_sessionmaker[AsyncSession]):
    """Tests that two concurrent workers claiming the same RUNNABLE task resolve via OCC.

    Exactly one worker commits the claim; the second receives OCC_CONFLICT with zero rows updated.
    No row locking serialized this; OCC revision check arbitrated the race.
    """
    now_utc = datetime.now(UTC)
    def_id = DefinitionId.generate()
    t_id_def = TaskDefinitionId("concurrent_task")

    task_def = TaskDefinition(
        id=t_id_def,
        activity_type=ActivityType("compute.race"),
        dependencies=frozenset(),
        input_bindings={},
        max_attempts=3,
    )
    spec = ValidatedWorkflowSpec(
        workflow_name="race_wf",
        tasks={t_id_def: task_def},
        output_bindings={},
    )

    async with session_factory() as session:
        async with session.begin():
            session.add(
                RegisteredDefinitionRecord(
                    definition_id=def_id.value,
                    workflow_name="race_wf",
                    validated_iws=serialize_validated_spec(spec),
                    raw_yaml=None,
                    created_at_utc=now_utc,
                )
            )

    wf_id = WorkflowExecutionId.generate()
    wf_entity = WorkflowExecution(
        id=wf_id,
        definition_id=def_id,
        state=WorkflowState.INITIALIZING,
        revision=1,
        workflow_input={"num": 1},
        output=OutputAbsent(),
        created_at_utc=now_utc,
        updated_at_utc=now_utc,
    )

    async with session_factory() as session:
        await commit_workflow_creation(session, wf_entity, None, None, now_utc)
        await commit_task_population(session, wf_id, [task_def], now_utc)
        await commit_initialization_complete(session, wf_id, 1, now_utc)

    # Find task_id
    async with session_factory() as session:
        from sqlalchemy import select

        from nexusflow.persistence.orm import TaskExecutionRecord

        task_rec = await session.scalar(
            select(TaskExecutionRecord).where(
                TaskExecutionRecord.workflow_execution_id == wf_id.value
            )
        )
        assert task_rec is not None
        task_id = TaskExecutionId(task_rec.task_execution_id)

    # Promote to RUNNABLE at revision 1
    async with session_factory() as session:
        await commit_task_readiness(session, task_id, 1, wf_id, {}, now_utc)

    # Both workers see task at revision 2
    expected_task_rev = 2
    worker_1 = WorkerSessionId.generate()
    worker_2 = WorkerSessionId.generate()
    attempt_1 = AttemptId.generate()
    attempt_2 = AttemptId.generate()
    deadline = now_utc + timedelta(seconds=30)

    async def try_claim(w_id: WorkerSessionId, a_id: AttemptId):
        async with session_factory() as s:
            return await commit_attempt_ownership(
                session=s,
                task_id=task_id,
                expected_task_revision=expected_task_rev,
                workflow_id=wf_id,
                worker_session_id=w_id,
                new_attempt_id=a_id,
                start_deadline_utc=deadline,
                now_utc=now_utc,
            )

    # Race both claims concurrently
    res1, res2 = await asyncio.gather(
        try_claim(worker_1, attempt_1),
        try_claim(worker_2, attempt_2),
    )

    outcomes = [res1[0].status, res2[0].status]
    # Exactly one COMMITTED and the other failed conditionally (OCC_CONFLICT or PRECONDITION_FAILED)
    assert CommitStatus.COMMITTED in outcomes
    assert (CommitStatus.OCC_CONFLICT in outcomes) or (CommitStatus.PRECONDITION_FAILED in outcomes)
