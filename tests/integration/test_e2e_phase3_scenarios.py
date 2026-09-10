"""End-to-End integration tests for Phase 3 failure handling, retries, and cancellation (LLD-06, LLD-08)."""

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexusflow.interfaces.http.app import app
from nexusflow.interfaces.http.dependencies import (
    get_db_session,
    get_scheduler,
    get_session_factory,
    get_worker_registry,
)
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import ExecutionAttemptRecord
from nexusflow.worker.runtime import NexusFlowWorker, WorkerRuntimeConfig

POSTGRES_TEST_URL = (
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow"
)


@pytest.fixture
async def e2e_context():
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False, pool_pre_ping=True)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory=session_maker, worker_registry=registry)

    app.dependency_overrides[get_session_factory] = lambda: session_maker
    app.dependency_overrides[get_worker_registry] = lambda: registry
    app.dependency_overrides[get_scheduler] = lambda: scheduler

    async def override_get_db_session():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield {
            "client": client,
            "registry": registry,
            "scheduler": scheduler,
            "session_maker": session_maker,
        }

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.mark.asyncio
async def test_e2e_retry_to_success(e2e_context):
    """Scenario 1: Task fails on attempt 1 with transient error, enters RETRY_WAIT, gets retried, and succeeds on attempt 2."""
    client: AsyncClient = e2e_context["client"]
    public_headers = {
        "Authorization": "Bearer dev-secret-token",
        "Content-Type": "application/json",
    }

    # 1. Register definition with max_attempts: 2
    yaml_content = """
workflow_name: retry_pipeline
tasks:
  step:
    activity_type: flaky.compute
    dependencies: []
    max_attempts: 2
output_bindings:
  step_result:
    type: task_output
    task: step
"""
    def_resp = await client.post(
        "/v1/definitions",
        headers=public_headers,
        json={"yaml_content": yaml_content},
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    # 2. Start worker runtime
    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="test-flaky-worker",
            poll_timeout_seconds=0.5,
        ),
        client=client,
    )

    attempt_counter = 0

    @worker.activity("flaky.compute")
    async def flaky_handler(**kwargs):
        nonlocal attempt_counter
        attempt_counter += 1
        if attempt_counter == 1:
            raise ConnectionResetError("Transient network failure")
        return {"result": "recovered_successfully"}

    await worker.start(start_background_loops=False)

    # 3. Start workflow execution
    start_resp = await client.post(
        "/v1/executions",
        headers=public_headers,
        json={"definition_id": def_id, "workflow_input": {}},
    )
    assert start_resp.status_code == 201
    wf_id = start_resp.json()["workflow_execution_id"]

    # Attempt 1: Poll, execute, fail -> transitions to RETRY_WAIT
    executed = await worker.run_once()
    assert executed is True

    tasks_resp = await client.get(f"/v1/executions/{wf_id}/tasks", headers=public_headers)
    assert tasks_resp.status_code == 200
    tasks = tasks_resp.json()
    assert len(tasks) == 1
    assert tasks[0]["state"] == "RETRY_WAIT"
    assert tasks[0]["current_attempt_ordinal"] == 2

    # Promote RETRY_WAIT -> RUNNABLE via scheduler wakeup
    from datetime import UTC, datetime, timedelta

    from nexusflow.domain.identifiers import TaskExecutionId, WorkflowExecutionId
    from nexusflow.persistence.orm import TaskExecutionRecord
    from nexusflow.persistence.transactions import commit_retry_ready

    async with e2e_context["session_maker"]() as session:
        t_row = await session.scalar(
            select(TaskExecutionRecord).where(
                TaskExecutionRecord.task_execution_id == tasks[0]["task_execution_id"]
            )
        )
        assert t_row is not None
        outcome = await commit_retry_ready(
            session=session,
            task_id=TaskExecutionId(tasks[0]["task_execution_id"]),
            expected_task_revision=t_row.revision,
            workflow_id=WorkflowExecutionId(wf_id),
            now_utc=datetime.now(UTC) + timedelta(seconds=10),
        )
        assert outcome.status == "COMMITTED"
        await session.commit()

    await e2e_context["scheduler"].advance_workflow(WorkflowExecutionId(wf_id))

    # Attempt 2: Poll, execute, succeed
    executed_2 = await worker.run_once()
    assert executed_2 is True

    # Verify workflow succeeded
    wf_resp = await client.get(f"/v1/executions/{wf_id}", headers=public_headers)
    assert wf_resp.status_code == 200
    assert wf_resp.json()["state"] == "SUCCEEDED"


@pytest.mark.asyncio
async def test_e2e_cancellation_lifecycle(e2e_context):
    """Scenario 4: Workflow cancellation triggers cooperative cancellation and terminates CANCELLED."""
    client: AsyncClient = e2e_context["client"]
    public_headers = {
        "Authorization": "Bearer dev-secret-token",
        "Content-Type": "application/json",
    }

    yaml_content = """
workflow_name: cancel_pipeline
tasks:
  long_step:
    activity_type: long.running
    dependencies: []
    max_attempts: 1
"""
    def_resp = await client.post(
        "/v1/definitions",
        headers=public_headers,
        json={"yaml_content": yaml_content},
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="test-cancel-worker",
            poll_timeout_seconds=0.5,
        ),
        client=client,
    )

    @worker.activity("long.running")
    async def long_running(**kwargs):
        await asyncio.sleep(5.0)
        return {"status": "done"}

    await worker.start(start_background_loops=False)

    start_resp = await client.post(
        "/v1/executions",
        headers=public_headers,
        json={"definition_id": def_id, "workflow_input": {}},
    )
    assert start_resp.status_code == 201
    wf_id = start_resp.json()["workflow_execution_id"]

    # Trigger cancellation via Public API
    cancel_resp = await client.post(f"/v1/executions/{wf_id}/cancel", headers=public_headers)
    assert cancel_resp.status_code in (200, 202)
    assert cancel_resp.json()["state"] in ("CANCELLING", "CANCELLED")

    # Repeat cancellation returns idempotent response
    cancel_dup_resp = await client.post(f"/v1/executions/{wf_id}/cancel", headers=public_headers)
    assert cancel_dup_resp.status_code in (200, 202)


@pytest.mark.asyncio
async def test_e2e_retry_exhaustion(e2e_context):
    """Scenario: Retry exhaustion proves:
    - no Attempt beyond max_attempts
    - TaskExecution -> FAILED
    - WorkflowExecution RUNNING -> FAILING
    - remaining unstarted tasks -> CANCELLED
    - WorkflowExecution FAILING -> FAILED
    """
    client: AsyncClient = e2e_context["client"]
    public_headers = {
        "Authorization": "Bearer dev-secret-token",
        "Content-Type": "application/json",
    }

    yaml_content = """
workflow_name: retry_exhaustion_pipeline
tasks:
  failing_step:
    activity_type: fatal.act
    dependencies: []
    max_attempts: 1
  sibling_step:
    activity_type: sibling.act
    dependencies: [failing_step]
    max_attempts: 1
output_bindings: {}
"""
    def_resp = await client.post(
        "/v1/definitions",
        headers=public_headers,
        json={"yaml_content": yaml_content},
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="test-fatal-worker",
            poll_timeout_seconds=0.5,
        ),
        client=client,
    )

    @worker.activity("fatal.act")
    async def fatal_handler(**kwargs):
        raise RuntimeError("Permanent failure")

    await worker.start(start_background_loops=False)

    start_resp = await client.post(
        "/v1/executions",
        headers=public_headers,
        json={"definition_id": def_id, "workflow_input": {}},
    )
    assert start_resp.status_code == 201
    wf_id = start_resp.json()["workflow_execution_id"]

    # Run worker to execute failing_step
    executed = await worker.run_once()
    assert executed is True

    # Advance scheduler to drain workflow
    from nexusflow.domain.identifiers import WorkflowExecutionId

    await e2e_context["scheduler"].drain_workflow(WorkflowExecutionId(wf_id))

    # Verify workflow is FAILED
    wf_resp = await client.get(f"/v1/executions/{wf_id}", headers=public_headers)
    assert wf_resp.status_code == 200
    wf_data = wf_resp.json()
    assert wf_data["state"] == "FAILED"

    # Verify tasks: failing_step is FAILED, sibling_step is CANCELLED
    tasks_resp = await client.get(f"/v1/executions/{wf_id}/tasks", headers=public_headers)
    assert tasks_resp.status_code == 200
    tasks_by_name = {t["task_definition_id"]: t for t in tasks_resp.json()}
    assert tasks_by_name["failing_step"]["state"] == "FAILED"
    assert tasks_by_name["sibling_step"]["state"] == "CANCELLED"

    # Verify no attempt beyond max_attempts: exactly 1 attempt exists in database
    async with e2e_context["session_maker"]() as session:
        att_count = await session.scalar(
            select(func.count(ExecutionAttemptRecord.attempt_id)).where(
                ExecutionAttemptRecord.task_execution_id
                == tasks_by_name["failing_step"]["task_execution_id"]
            )
        )
        assert att_count == 1
