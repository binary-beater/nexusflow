import os

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexusflow.interfaces.http.app import app
from nexusflow.interfaces.http.dependencies import get_db_session

POSTGRES_TEST_URL = os.getenv(
    "NEXUSFLOW_DB__URL",
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow",
)


@pytest.fixture
async def client_with_postgres_db():
    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)

    async def override_get_db_session():
        async with session_maker() as session:
            yield session

    app.dependency_overrides[get_db_session] = override_get_db_session

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

    app.dependency_overrides.clear()
    await engine.dispose()


@pytest.mark.asyncio
async def test_health_endpoints(client_with_postgres_db: AsyncClient):
    res_healthz = await client_with_postgres_db.get("/healthz")
    assert res_healthz.status_code == 200
    assert res_healthz.json() == {"status": "ok"}

    res_readyz = await client_with_postgres_db.get("/readyz")
    assert res_readyz.status_code == 200
    assert res_readyz.json() == {"status": "ready"}


@pytest.mark.asyncio
async def test_readyz_fails_when_unready(client_with_postgres_db: AsyncClient):
    from nexusflow.interfaces.http.dependencies import (
        SimpleRecoveryGate,
        get_recovery_gate,
        get_scheduler,
    )

    class ClosedRecoveryGate(SimpleRecoveryGate):
        def allows_new_work(self) -> bool:
            return False

    # 1. Test RecoveryGate closed
    app.dependency_overrides[get_recovery_gate] = lambda: ClosedRecoveryGate()
    res = await client_with_postgres_db.get("/readyz")
    assert res.status_code == 503
    assert res.json()["code"] == "NOT_READY"

    # Reset gate
    app.dependency_overrides.pop(get_recovery_gate)

    # 2. Test Scheduler runtime unavailable
    app.dependency_overrides[get_scheduler] = lambda: None
    res = await client_with_postgres_db.get("/readyz")
    assert res.status_code == 503
    assert res.json()["code"] == "RUNTIME_UNHEALTHY"

    # Reset scheduler
    app.dependency_overrides.pop(get_scheduler)


@pytest.mark.asyncio
async def test_definition_registration_unauthorized(client_with_postgres_db: AsyncClient):
    yaml_payload = """
workflow_name: demo
tasks:
  step:
    activity_type: demo.act
    dependencies: []
    max_attempts: 1
"""
    # Missing auth header
    res = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/yaml"},
        content=yaml_payload,
    )
    assert res.status_code == 401
    assert res.json()["error"]["code"] == "UNAUTHORIZED"

    # Invalid token
    res = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={
            "Content-Type": "application/yaml",
            "Authorization": "Bearer invalid-token",
        },
        content=yaml_payload,
    )
    assert res.status_code == 401


@pytest.mark.asyncio
async def test_definition_registration_and_retrieval_success(client_with_postgres_db: AsyncClient):
    yaml_payload = """
workflow_name: demo
tasks:
  step:
    activity_type: demo.act
    dependencies: []
    max_attempts: 1
"""
    import uuid
    test_run_key = f"test-idem-{uuid.uuid4().hex[:8]}"
    headers = {
        "Content-Type": "application/yaml",
        "Authorization": "Bearer dev-secret-token",
        "Idempotency-Key": test_run_key,
    }
    # 1. Register definition
    res = await client_with_postgres_db.post(
        "/v1/definitions",
        headers=headers,
        content=yaml_payload,
    )
    assert res.status_code == 201
    data = res.json()
    assert data["workflow_name"] == "demo"
    assert "definition_id" in data
    def_id = data["definition_id"]

    # 2. Resubmit identical -> same definition_id
    res_idem = await client_with_postgres_db.post(
        "/v1/definitions",
        headers=headers,
        content=yaml_payload,
    )
    assert res_idem.status_code == 201
    assert res_idem.json()["definition_id"] == def_id

    # 3. GET definition by ID
    res_get = await client_with_postgres_db.get(
        f"/v1/definitions/{def_id}",
        headers={"Authorization": "Bearer dev-secret-token"},
    )
    assert res_get.status_code == 200
    assert res_get.json()["definition_id"] == def_id
    assert res_get.json()["workflow_name"] == "demo"


@pytest.mark.asyncio
async def test_definition_semantic_error_mapping(client_with_postgres_db: AsyncClient):
    # Cycle workflow
    yaml_cycle = """
workflow_name: cycle_wf
tasks:
  task_a:
    activity_type: test.act
    dependencies: [task_b]
    max_attempts: 1
  task_b:
    activity_type: test.act
    dependencies: [task_a]
    max_attempts: 1
"""
    headers = {
        "Content-Type": "application/yaml",
        "Authorization": "Bearer dev-secret-token",
    }
    res = await client_with_postgres_db.post(
        "/v1/definitions",
        headers=headers,
        content=yaml_cycle,
    )
    assert res.status_code == 422
    err_body = res.json()
    assert err_body["error"]["code"] == "DEFINITION_VALIDATION_FAILED"
    assert "semantic_errors" in err_body["error"]["details"]


@pytest.mark.asyncio
async def test_api_contract_matrix_scenarios(client_with_postgres_db: AsyncClient):
    # 1. Malformed YAML -> 400 MALFORMED_YAML
    res_malformed = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/yaml", "Authorization": "Bearer dev-secret-token"},
        content="workflow_name: [unclosed list",
    )
    assert res_malformed.status_code == 400
    assert res_malformed.json()["error"]["code"] == "MALFORMED_YAML"

    # 2. Structural DTO failure -> 422 VALIDATION_ERROR
    res_dto = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/yaml", "Authorization": "Bearer dev-secret-token"},
        content="workflow_name: demo\ntasks:\n  step:\n    activity_type: 12345\n    max_attempts: -1",
    )
    assert res_dto.status_code == 422
    assert res_dto.json()["error"]["code"] == "VALIDATION_ERROR"

    # 3. Unsupported media type -> 415 UNSUPPORTED_MEDIA_TYPE
    res_media = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "text/html", "Authorization": "Bearer dev-secret-token"},
        content="<b>hello</b>",
    )
    assert res_media.status_code == 415
    assert res_media.json()["error"]["code"] == "UNSUPPORTED_MEDIA_TYPE"

    # 4. Definition not found -> 404 DEFINITION_NOT_FOUND
    import uuid
    random_id = uuid.uuid4()
    res_404 = await client_with_postgres_db.get(
        f"/v1/definitions/{random_id}",
        headers={"Authorization": "Bearer dev-secret-token"},
    )
    assert res_404.status_code == 404
    assert res_404.json()["error"]["code"] == "DEFINITION_NOT_FOUND"

    # 5. Application JSON wrapper support (LLD-08 Section 5.3)
    json_payload = {
        "yaml_content": "workflow_name: json_registered\ntasks:\n  s1:\n    activity_type: test\n    max_attempts: 1\n"
    }
    import json
    res_json = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/json", "Authorization": "Bearer dev-secret-token"},
        content=json.dumps(json_payload),
    )
    assert res_json.status_code == 201
    assert res_json.json()["workflow_name"] == "json_registered"

    # 6. Idempotency conflict -> 409 IDEMPOTENCY_CONFLICT
    test_key = f"conflict-key-{uuid.uuid4().hex[:8]}"
    yaml_a = "workflow_name: original\ntasks:\n  t1:\n    activity_type: test\n    max_attempts: 1\n"
    yaml_b = "workflow_name: modified\ntasks:\n  t1:\n    activity_type: test\n    max_attempts: 1\n"
    res_init = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/yaml", "Authorization": "Bearer dev-secret-token", "Idempotency-Key": test_key},
        content=yaml_a,
    )
    assert res_init.status_code == 201

    res_conflict = await client_with_postgres_db.post(
        "/v1/definitions",
        headers={"Content-Type": "application/yaml", "Authorization": "Bearer dev-secret-token", "Idempotency-Key": test_key},
        content=yaml_b,
    )
    assert res_conflict.status_code == 409
    assert res_conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
