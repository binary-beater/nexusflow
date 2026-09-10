"""Complete End-to-End Workflow Execution Happy Path Test against real PostgreSQL 16 (LLD-10).

Executes a 3-stage pipeline (stage_a -> stage_b -> stage_c) using the reference Python worker runtime.
Verifies all public read APIs (/v1/executions/{id}, /tasks, /history) and process restart durability.
"""

from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexusflow.config.settings import NexusFlowSettings
from nexusflow.interfaces.http.app import app
from nexusflow.interfaces.http.dependencies import (
    get_db_session,
    get_scheduler,
    get_session_factory,
    get_worker_registry,
)
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.worker.runtime import NexusFlowWorker, WorkerRuntimeConfig


@pytest.fixture
async def e2e_context():
    settings = NexusFlowSettings()
    engine = create_async_engine(settings.database.url, echo=False)
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
async def test_e2e_three_stage_pipeline_happy_path(e2e_context):
    client: AsyncClient = e2e_context["client"]
    public_headers = {
        "Authorization": "Bearer dev-secret-token",
        "Content-Type": "application/json",
    }

    # 1. Register 3-stage definition (02_pipeline_happy_path.yaml)
    yaml_path = Path("examples/workflows/02_pipeline_happy_path.yaml")
    yaml_content = yaml_path.read_text(encoding="utf-8")

    def_resp = await client.post(
        "/v1/definitions",
        headers=public_headers,
        json={"yaml_content": yaml_content},
    )
    assert def_resp.status_code == 201, def_resp.text
    definition_id = def_resp.json()["definition_id"]

    # 2. Setup reference Python worker with activity handlers
    worker_config = WorkerRuntimeConfig(
        control_plane_url="http://test",
        worker_token="dev-worker-secret",
        worker_id="test-e2e-worker",
    )
    worker = NexusFlowWorker(config=worker_config, client=client)

    @worker.activity("pipeline.stage_a")
    async def handle_stage_a(order_id: str):
        return {"order_id": order_id, "stage_a_status": "PROCESSED", "amount": 100}

    @worker.activity("pipeline.stage_b")
    def handle_stage_b(upstream_data: dict, multiplier: int):
        return {
            "processed_amount": upstream_data["amount"] * multiplier,
            "stage_b_status": "ENRICHED",
        }

    @worker.activity("pipeline.stage_c")
    async def handle_stage_c(final_input: dict):
        return {
            "invoice_id": "INV-999",
            "total": final_input["processed_amount"],
            "status": "COMPLETED",
        }

    # Register worker with control plane (without background polling so test drives execution)
    await worker.start(start_background_loops=False)
    assert worker.session_id is not None

    # 3. Start Workflow Execution
    start_resp = await client.post(
        "/v1/executions",
        headers=public_headers,
        json={"definition_id": definition_id, "workflow_input": "ORDER-42"},
    )
    assert start_resp.status_code == 201, start_resp.text
    wf_data = start_resp.json()
    wf_id = wf_data["workflow_execution_id"]
    assert wf_data["state"] == "RUNNING"

    # 4. Worker executes stage_a
    ran_a = await worker.run_once()
    assert ran_a is True

    # Check execution after stage_a: should still be RUNNING
    status_resp = await client.get(f"/v1/executions/{wf_id}", headers=public_headers)
    assert status_resp.status_code == 200
    assert status_resp.json()["state"] == "RUNNING"

    # 5. Worker executes stage_b
    ran_b = await worker.run_once()
    assert ran_b is True

    # 6. Worker executes stage_c
    ran_c = await worker.run_once()
    assert ran_c is True

    # 7. Workflow should now be SUCCEEDED with output
    final_resp = await client.get(f"/v1/executions/{wf_id}", headers=public_headers)
    assert final_resp.status_code == 200
    final_data = final_resp.json()
    assert final_data["state"] == "SUCCEEDED"
    assert final_data["has_output"] is True
    assert final_data["output"] == {
        "pipeline_result": {
            "invoice_id": "INV-999",
            "total": 1000,
            "status": "COMPLETED",
        }
    }

    # 8. Verify /tasks API
    tasks_resp = await client.get(f"/v1/executions/{wf_id}/tasks", headers=public_headers)
    assert tasks_resp.status_code == 200
    tasks = tasks_resp.json()
    assert len(tasks) == 3
    for t in tasks:
        assert t["state"] == "SUCCEEDED"
        assert t["has_output"] is True

    # 9. Verify /history API
    history_resp = await client.get(f"/v1/executions/{wf_id}/history", headers=public_headers)
    assert history_resp.status_code == 200
    history = history_resp.json()
    assert (
        len(history) >= 8
    )  # Creation, population, started, 3x claim, 3x start, 3x success, wf_success
    categories = [h["event_category"] for h in history]
    assert "WorkflowExecutionCreated" in categories
    assert "WorkflowExecutionStarted" in categories
    assert "TaskExecutionSucceeded" in categories
    assert "WorkflowExecutionSucceeded" in categories

    # 10. Process restart durability proof:
    # Stop worker, tear down memory registry and scheduler, reload directly from PostgreSQL
    await worker.stop()

    new_registry = WorkerRegistry()
    app.dependency_overrides[get_worker_registry] = lambda: new_registry

    persisted_resp = await client.get(f"/v1/executions/{wf_id}", headers=public_headers)
    assert persisted_resp.status_code == 200
    persisted_data = persisted_resp.json()
    assert persisted_data["state"] == "SUCCEEDED"
    assert persisted_data["output"] == final_data["output"]
