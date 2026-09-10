"""Integration tests for StartupRecoveryEngine verifying Scenarios A through G (LLD-07)."""

import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexusflow.definition.codec import serialize_validated_spec
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
from nexusflow.orchestration.recovery import StartupRecoveryEngine
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
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
async def test_recovery_scenario_a_partial_initializing_repair(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario A: Partial INITIALIZING repair."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_a_wf", "step")

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_a_wf",
                validated_iws=serialize_validated_spec(spec),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="INITIALIZING",
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
            state="PENDING",
            revision=1,
            has_input=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        await sess.commit()

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        wf_rec = await sess.get(WorkflowExecutionRecord, wf_id.value)
        assert wf_rec is not None
        assert wf_rec.state == "RUNNING"
        assert wf_rec.revision == 2

        t_rec = await sess.get(TaskExecutionRecord, t_id.value)
        assert t_rec is not None
        assert t_rec.state == "RUNNABLE"


@pytest.mark.asyncio
async def test_recovery_scenario_b_retry_wait_survives_restart(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario B: RETRY_WAIT survives restart without changing timestamp if future."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_b_wf", "step")
    target_retry_ready = now_utc + timedelta(hours=1)

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_b_wf",
                validated_iws=serialize_validated_spec(spec),
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
            state="RETRY_WAIT",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=2,
            retry_ready_at_utc=target_retry_ready,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        await sess.commit()

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        t_rec = await sess.get(TaskExecutionRecord, t_id.value)
        assert t_rec is not None
        assert t_rec.state == "RETRY_WAIT"
        assert t_rec.retry_ready_at_utc == target_retry_ready


@pytest.mark.asyncio
async def test_recovery_scenario_c_runnable_survives_restart(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario C: RUNNABLE survives restart and is rediscovered."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_c_runnable_wf", "step")

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_c_runnable_wf",
                validated_iws=serialize_validated_spec(spec),
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
            state="RUNNABLE",
            revision=1,
            has_input=True,
            stable_input={},
            has_output=False,
            max_attempts=3,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t)
        await sess.flush()
        await sess.commit()

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        t_rec = await sess.get(TaskExecutionRecord, t_id.value)
        assert t_rec is not None
        assert t_rec.state == "RUNNABLE"


@pytest.mark.asyncio
async def test_recovery_scenario_d_claimed_restart_reconciliation(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario D: CLAIMED restart reconciliation (lost session or start-deadline expiry)."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    dead_session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_d_wf", "step")

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_d_wf",
                validated_iws=serialize_validated_spec(spec),
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
            next_attempt_ordinal=2,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=dead_session_id.value,
            state="CLAIMED",
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

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        att_rec = await sess.get(ExecutionAttemptRecord, att_id.value)
        assert att_rec is not None
        assert att_rec.state == "FAILED"
        assert att_rec.failure_code == "WORKER_LOSS"

        t_rec = await sess.get(TaskExecutionRecord, t_id.value)
        assert t_rec is not None
        assert t_rec.state == "RETRY_WAIT"
        assert t_rec.next_attempt_ordinal == 2


@pytest.mark.asyncio
async def test_recovery_scenario_e_running_restart_reconciliation(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario E: RUNNING restart reconciliation (lost session / execution timeout)."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id = TaskExecutionId.generate()
    att_id = AttemptId.generate()
    dead_session_id = WorkerSessionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_e_wf", "step")

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_e_wf",
                validated_iws=serialize_validated_spec(spec),
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
            next_attempt_ordinal=2,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        att = ExecutionAttemptRecord(
            attempt_id=att_id.value,
            task_execution_id=t_id.value,
            attempt_ordinal=1,
            worker_session_id=dead_session_id.value,
            state="RUNNING",
            revision=1,
            start_deadline_utc=now_utc + timedelta(seconds=30),
            execution_timeout_utc=now_utc + timedelta(minutes=5),
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

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        att_rec = await sess.get(ExecutionAttemptRecord, att_id.value)
        assert att_rec is not None
        assert att_rec.state == "FAILED"
        assert att_rec.failure_code == "WORKER_LOSS"

        t_rec = await sess.get(TaskExecutionRecord, t_id.value)
        assert t_rec is not None
        assert t_rec.state == "RETRY_WAIT"
        assert t_rec.next_attempt_ordinal == 2


@pytest.mark.asyncio
async def test_recovery_scenario_f_failing_restart_drain_completion(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario F: FAILING restart drain completion."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id1 = TaskExecutionId.generate()
    t_id2 = TaskExecutionId.generate()
    def_id = DefinitionId.generate()
    t_id1_def = TaskDefinitionId("step1")
    t_id2_def = TaskDefinitionId("step2")
    spec = ValidatedWorkflowSpec(
        workflow_name="scen_f_wf",
        tasks={
            t_id1_def: TaskDefinition(
                id=t_id1_def,
                activity_type=ActivityType("act1"),
                dependencies=frozenset(),
                input_bindings={},
                max_attempts=1,
            ),
            t_id2_def: TaskDefinition(
                id=t_id2_def,
                activity_type=ActivityType("act2"),
                dependencies=frozenset(),
                input_bindings={},
                max_attempts=1,
            ),
        },
        output_bindings={},
    )

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_f_wf",
                validated_iws=serialize_validated_spec(spec),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        wf = WorkflowExecutionRecord(
            workflow_execution_id=wf_id.value,
            definition_id=def_id.value,
            state="FAILING",
            revision=1,
            workflow_input={},
            has_output=False,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t1 = TaskExecutionRecord(
            task_execution_id=t_id1.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step1",
            state="FAILED",
            revision=1,
            has_input=True,
            stable_input={},
            max_attempts=1,
            next_attempt_ordinal=1,
            failure_category="SYSTEM_PERMANENT",
            failure_code="ACTIVITY_FAILED",
            failure_message="Failed definitively",
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        t2 = TaskExecutionRecord(
            task_execution_id=t_id2.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step2",
            state="PENDING",
            revision=1,
            has_input=False,
            max_attempts=1,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t1)
        sess.add(t2)
        await sess.flush()
        await sess.commit()

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        wf_rec = await sess.get(WorkflowExecutionRecord, wf_id.value)
        assert wf_rec is not None
        assert wf_rec.state == "FAILED"

        t2_rec = await sess.get(TaskExecutionRecord, t_id2.value)
        assert t2_rec is not None
        assert t2_rec.state == "CANCELLED"


@pytest.mark.asyncio
async def test_recovery_scenario_g_cancelling_restart_drain_completion(
    session_factory: async_sessionmaker[AsyncSession],
):
    """Scenario G: CANCELLING restart drain completion."""
    now_utc = datetime.now(UTC)
    wf_id = WorkflowExecutionId.generate()
    t_id1 = TaskExecutionId.generate()
    def_id = DefinitionId.generate()
    spec = make_spec("scen_g_wf", "step1")

    async with session_factory() as sess:
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="scen_g_wf",
                validated_iws=serialize_validated_spec(spec),
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
        t1 = TaskExecutionRecord(
            task_execution_id=t_id1.value,
            workflow_execution_id=wf_id.value,
            task_definition_id="step1",
            state="RUNNABLE",
            revision=1,
            has_input=True,
            stable_input={},
            max_attempts=1,
            next_attempt_ordinal=1,
            created_at_utc=now_utc,
            updated_at_utc=now_utc,
        )
        sess.add(wf)
        await sess.flush()
        sess.add(t1)
        await sess.flush()
        await sess.commit()

    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory, registry)
    engine = StartupRecoveryEngine(session_factory, scheduler, registry)

    await engine.recover_system()

    async with session_factory() as sess:
        wf_rec = await sess.get(WorkflowExecutionRecord, wf_id.value)
        assert wf_rec is not None
        assert wf_rec.state == "CANCELLED"

        t1_rec = await sess.get(TaskExecutionRecord, t_id1.value)
        assert t1_rec is not None
        assert t1_rec.state == "CANCELLED"
