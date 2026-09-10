"""Test verifying terminal execution durability across PostgreSQL container restart."""

import os
import subprocess
import time
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
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
from nexusflow.worker.runtime import NexusFlowWorker, WorkerRuntimeConfig

POSTGRES_TEST_URL = os.getenv(
    "NEXUSFLOW_DB__URL",
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow",
)


@pytest.mark.asyncio
async def test_terminal_execution_durability_across_postgres_container_restart():
    """Runs a real 3-stage execution to completion, restarts the docker postgres container,

    and verifies complete state persistence (workflow, tasks, attempts, history, output).
    """
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
    public_headers = {
        "Authorization": "Bearer dev-secret-token",
        "Content-Type": "application/json",
    }

    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # 1. Register 3-stage definition
        yaml_path = Path("examples/workflows/02_pipeline_happy_path.yaml")
        yaml_content = yaml_path.read_text(encoding="utf-8")
        def_resp = await client.post(
            "/v1/definitions",
            headers=public_headers,
            json={"yaml_content": yaml_content},
        )
        assert def_resp.status_code == 201
        definition_id = def_resp.json()["definition_id"]

        # 2. Worker setup
        worker_config = WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="restart-durability-worker",
        )
        worker = NexusFlowWorker(config=worker_config, client=client)

        @worker.activity("pipeline.stage_a")
        async def handle_a(order_id: str):
            return {"order_id": order_id, "amount": 100}

        @worker.activity("pipeline.stage_b")
        def handle_b(upstream_data: dict, multiplier: int):
            return {"processed_amount": upstream_data["amount"] * multiplier}

        @worker.activity("pipeline.stage_c")
        async def handle_c(final_input: dict):
            return {"invoice_id": "INV-100", "total": final_input["processed_amount"]}

        await worker.start(start_background_loops=False)

        # 3. Start execution
        start_resp = await client.post(
            "/v1/executions",
            headers=public_headers,
            json={"definition_id": definition_id, "workflow_input": "ORDER-999"},
        )
        assert start_resp.status_code == 201
        wf_id = start_resp.json()["workflow_execution_id"]

        # 4. Execute all 3 stages
        assert await worker.run_once() is True
        assert await worker.run_once() is True
        assert await worker.run_once() is True
        await worker.stop()

        # Capture pre-restart state
        wf_pre = (await client.get(f"/v1/executions/{wf_id}", headers=public_headers)).json()
        assert wf_pre["state"] == "SUCCEEDED"
        assert wf_pre["has_output"] is True
        assert wf_pre["output"] == {"pipeline_result": {"invoice_id": "INV-100", "total": 1000}}

        tasks_pre = (await client.get(f"/v1/executions/{wf_id}/tasks", headers=public_headers)).json()
        assert len(tasks_pre) == 3
        for t in tasks_pre:
            assert t["state"] == "SUCCEEDED"
            assert t["has_output"] is True

        history_pre = (await client.get(f"/v1/executions/{wf_id}/history", headers=public_headers)).json()
        assert len(history_pre) >= 8

        # 5. RESTART DOCKER POSTGRES CONTAINER
        import shutil
        docker_candidates = [
            shutil.which("docker"),
            os.path.expanduser(r"~\AppData\Local\Programs\DockerDesktop\resources\bin\docker.exe"),
            r"C:\Program Files\Docker\Docker\resources\bin\docker.exe",
            r"C:\Program Files\Docker\Docker\docker.exe",
            r"C:\ProgramData\DockerDesktop\version-bin\docker.exe",
            "/usr/bin/docker",
            "/usr/local/bin/docker",
        ]
        docker_exe = next((c for c in docker_candidates if c and os.path.exists(c)), "docker")
        subprocess.run([docker_exe, "restart", "nexusflow-postgres"], check=True)

        # Wait for pg_isready
        for _ in range(30):
            res = subprocess.run(
                [docker_exe, "exec", "nexusflow-postgres", "pg_isready", "-U", "nexusflow_user", "-d", "nexusflow"],
                capture_output=True,
                text=True,
            )
            if "accepting connections" in res.stdout:
                break
            time.sleep(1)

        # Invalidate old connection pool and reconnect
        await engine.dispose()
        engine_post = create_async_engine(POSTGRES_TEST_URL, echo=False, pool_pre_ping=True)
        session_maker_post = async_sessionmaker(engine_post, expire_on_commit=False)

        app.dependency_overrides[get_session_factory] = lambda: session_maker_post

        async def override_post():
            async with session_maker_post() as s:
                yield s

        app.dependency_overrides[get_db_session] = override_post

        # 6. Read back from database and assert full durability
        wf_post = (await client.get(f"/v1/executions/{wf_id}", headers=public_headers)).json()
        assert wf_post["workflow_execution_id"] == wf_id
        assert wf_post["state"] == "SUCCEEDED"
        assert wf_post["has_output"] is True
        assert wf_post["output"] == wf_pre["output"]
        assert wf_post["created_at_utc"] == wf_pre["created_at_utc"]

        tasks_post = (await client.get(f"/v1/executions/{wf_id}/tasks", headers=public_headers)).json()
        assert len(tasks_post) == 3
        for t_pre, t_post in zip(tasks_pre, tasks_post, strict=True):
            assert t_post["task_execution_id"] == t_pre["task_execution_id"]
            assert t_post["task_definition_id"] == t_pre["task_definition_id"]
            assert t_post["state"] == "SUCCEEDED"
            assert t_post["has_output"] is True
            assert t_post["output"] == t_pre["output"]

        history_post = (await client.get(f"/v1/executions/{wf_id}/history", headers=public_headers)).json()
        assert len(history_post) == len(history_pre)
        for h_pre, h_post in zip(history_pre, history_post, strict=True):
            assert h_post["history_id"] == h_pre["history_id"]
            assert h_post["event_category"] == h_pre["event_category"]

        await engine_post.dispose()

    app.dependency_overrides.clear()
