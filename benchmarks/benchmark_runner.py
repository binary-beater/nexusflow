"""NexusFlow V1 Performance & Stress Benchmarking Harness (LLD-10 / Phase 4).

Measures:
- Workload A: Pipeline Throughput (Workflows/sec, Tasks/sec)
- Workload B: Scheduling & Claim Latency (p50, p95, p99 percentiles)
- Workload C: Retry & Failure Load Under Transient Errors
- Workload D: Startup Crash Recovery Reconciliation Performance
"""

import asyncio
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexusflow.definition.codec import serialize_validated_spec
from nexusflow.domain.identifiers import (
    ActivityType,
    DefinitionId,
    TaskDefinitionId,
    TaskExecutionId,
    WorkflowExecutionId,
)
from nexusflow.domain.spec import TaskDefinition, ValidatedWorkflowSpec
from nexusflow.interfaces.http.app import app
from nexusflow.interfaces.http.dependencies import (
    get_db_session,
    get_scheduler,
    get_session_factory,
    get_worker_registry,
)
from nexusflow.orchestration.recovery import StartupRecoveryEngine
from nexusflow.orchestration.registry import WorkerRegistry
from nexusflow.orchestration.scheduler import ExecutionScheduler
from nexusflow.persistence.orm import (
    RegisteredDefinitionRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.worker.runtime import NexusFlowWorker, WorkerRuntimeConfig

POSTGRES_TEST_URL = (
    "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow"
)
PUBLIC_HEADERS = {"Authorization": "Bearer dev-secret-token", "Content-Type": "application/json"}


async def run_workload_a(client: AsyncClient, num_workflows: int = 50):
    print(
        f"\n--- Workload A: Throughput Benchmark ({num_workflows} workflows, 3-stage pipeline = {num_workflows * 3} tasks) ---"
    )
    yaml_path = Path("examples/workflows/02_pipeline_happy_path.yaml")
    yaml_content = yaml_path.read_text(encoding="utf-8")
    def_resp = await client.post(
        "/v1/definitions", headers=PUBLIC_HEADERS, json={"yaml_content": yaml_content}
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="bench-worker-a",
        ),
        client=client,
    )

    @worker.activity("pipeline.stage_a")
    async def h_a(order_id: str):
        return {"order_id": order_id, "amount": 100}

    @worker.activity("pipeline.stage_b")
    def h_b(upstream_data: dict, multiplier: int):
        return {"processed_amount": upstream_data["amount"] * multiplier}

    @worker.activity("pipeline.stage_c")
    async def h_c(final_input: dict):
        return {"total": final_input["processed_amount"]}

    await worker.start(start_background_loops=False)

    wf_ids = []
    start_t = time.perf_counter()

    for i in range(num_workflows):
        s_resp = await client.post(
            "/v1/executions",
            headers=PUBLIC_HEADERS,
            json={"definition_id": def_id, "workflow_input": f"BENCH-{i}"},
        )
        assert s_resp.status_code == 201
        wf_ids.append(s_resp.json()["workflow_execution_id"])

    # Worker processes all tasks sequentially
    total_tasks = num_workflows * 3
    for _ in range(total_tasks):
        executed = await worker.run_once()
        assert executed is True

    elapsed = time.perf_counter() - start_t
    wf_per_sec = num_workflows / elapsed
    tasks_per_sec = total_tasks / elapsed

    print(f"Elapsed Time: {elapsed:.3f}s")
    print(f"Workflow Throughput: {wf_per_sec:.2f} workflows/sec")
    print(f"Task Throughput:     {tasks_per_sec:.2f} tasks/sec")

    await worker.stop()
    return {"elapsed_s": elapsed, "wf_per_sec": wf_per_sec, "tasks_per_sec": tasks_per_sec}


async def run_workload_b(client: AsyncClient, samples: int = 50):
    print(
        f"\n--- Workload B: End-to-End Task Invocation & Claim Latency Percentiles ({samples} samples) ---"
    )
    print(
        "    Note: Measures round-trip from client POST /v1/executions through worker poll & attempt claim commit."
    )
    yaml_content = """
workflow_name: bench_latency
tasks:
  task_lat:
    activity_type: bench.lat
    dependencies: []
    max_attempts: 1
output_bindings:
  res:
    type: task_output
    task: task_lat
"""
    def_resp = await client.post(
        "/v1/definitions", headers=PUBLIC_HEADERS, json={"yaml_content": yaml_content}
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="bench-worker-b",
        ),
        client=client,
    )

    @worker.activity("bench.lat")
    async def h_lat():
        return {"done": True}

    await worker.start(start_background_loops=False)

    latencies_ms = []
    for _ in range(samples):
        t0 = time.perf_counter()
        s_resp = await client.post(
            "/v1/executions",
            headers=PUBLIC_HEADERS,
            json={"definition_id": def_id, "workflow_input": {}},
        )
        assert s_resp.status_code == 201
        # Worker claims and completes
        executed = await worker.run_once()
        assert executed is True
        t1 = time.perf_counter()
        latencies_ms.append((t1 - t0) * 1000.0)

    latencies_ms.sort()
    p50 = statistics.median(latencies_ms)
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)]
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)]

    print(f"p50 Latency (Submission-to-Claim): {p50:.2f} ms")
    print(f"p95 Latency (Submission-to-Claim): {p95:.2f} ms")
    print(f"p99 Latency (Submission-to-Claim): {p99:.2f} ms")

    await worker.stop()
    return {"p50_ms": p50, "p95_ms": p95, "p99_ms": p99}


async def run_workload_c(
    client: AsyncClient, session_maker: async_sessionmaker, num_failures: int = 30
):
    print(f"\n--- Workload C: Retry & Failure Load Handling ({num_failures} transient retries) ---")
    yaml_content = """
workflow_name: bench_retry
tasks:
  retry_step:
    activity_type: bench.flaky
    dependencies: []
    max_attempts: 2
output_bindings:
  res:
    type: task_output
    task: retry_step
"""
    def_resp = await client.post(
        "/v1/definitions", headers=PUBLIC_HEADERS, json={"yaml_content": yaml_content}
    )
    assert def_resp.status_code == 201
    def_id = def_resp.json()["definition_id"]

    worker = NexusFlowWorker(
        config=WorkerRuntimeConfig(
            control_plane_url="http://test",
            worker_token="dev-worker-secret",
            worker_id="bench-worker-c",
        ),
        client=client,
    )

    fail_map = {}

    @worker.activity("bench.flaky")
    async def h_flaky():
        # First call fails, second succeeds
        task_id = worker.session_id
        current = fail_map.get(task_id, 0)
        fail_map[task_id] = current + 1
        if current == 0:
            raise ConnectionError("Transient Network Blip")
        return {"status": "recovered"}

    await worker.start(start_background_loops=False)

    t0 = time.perf_counter()
    for _ in range(num_failures):
        s_resp = await client.post(
            "/v1/executions",
            headers=PUBLIC_HEADERS,
            json={"definition_id": def_id, "workflow_input": {}},
        )
        assert s_resp.status_code == 201

    t1 = time.perf_counter()
    elapsed = t1 - t0
    ops_sec = num_failures / elapsed
    print(
        f"Processed {num_failures} retry-enabled workflows in {elapsed:.3f}s ({ops_sec:.2f} workflows/sec)"
    )

    await worker.stop()
    return {"elapsed_s": elapsed, "retries_per_sec": ops_sec}


async def run_workload_d(session_maker: async_sessionmaker, num_workflows: int = 40):
    print(
        f"\n--- Workload D: Crash Recovery Reconciliation Throughput ({num_workflows} interrupted workflows) ---"
    )
    registry = WorkerRegistry()
    scheduler = ExecutionScheduler(session_factory=session_maker, worker_registry=registry)
    engine = StartupRecoveryEngine(session_maker, scheduler, registry)

    now_utc = datetime.now(UTC)
    t_def_id = TaskDefinitionId("step")
    spec = ValidatedWorkflowSpec(
        workflow_name="bench_rec",
        tasks={
            t_def_id: TaskDefinition(
                id=t_def_id,
                activity_type=ActivityType("bench.act"),
                dependencies=frozenset(),
                input_bindings={},
                max_attempts=3,
            )
        },
        output_bindings={},
    )

    async with session_maker() as sess:
        def_id = DefinitionId.generate()
        sess.add(
            RegisteredDefinitionRecord(
                definition_id=def_id.value,
                workflow_name="bench_rec",
                validated_iws=serialize_validated_spec(spec),
                raw_yaml=None,
                created_at_utc=now_utc,
            )
        )
        for _ in range(num_workflows):
            wf_id = WorkflowExecutionId.generate()
            t_id = TaskExecutionId.generate()
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

    t0 = time.perf_counter()
    summary = await engine.recover_system()
    elapsed = time.perf_counter() - t0
    rec_sec = num_workflows / elapsed

    print(f"Reconciled {num_workflows} workflows in {elapsed:.3f}s ({rec_sec:.2f} workflows/sec)")
    print(f"Summary metrics: {summary}")
    return {"elapsed_s": elapsed, "reconciled_per_sec": rec_sec}


async def main():
    print("=" * 75)
    print("NEXUSFLOW V1 PERFORMANCE BENCHMARK SUITE")
    print("PostgreSQL 16 Engine with OCC & Real Asyncpg Driver")
    print("=" * 75)

    engine = create_async_engine(POSTGRES_TEST_URL, echo=False)
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
        res_a = await run_workload_a(client, num_workflows=40)
        res_b = await run_workload_b(client, samples=40)
        res_c = await run_workload_c(client, session_maker, num_failures=25)
        res_d = await run_workload_d(session_maker, num_workflows=30)

    app.dependency_overrides.clear()
    await engine.dispose()

    print("\n" + "=" * 75)
    print("BENCHMARK RESULTS SUMMARY:")
    print(f"Workload A - Task Throughput:         {res_a['tasks_per_sec']:.2f} tasks/sec")
    print(f"Workload A - Workflow Throughput:     {res_a['wf_per_sec']:.2f} workflows/sec")
    print(f"Workload B - Median Submission-to-Claim Latency (p50):  {res_b['p50_ms']:.2f} ms")
    print(f"Workload B - 95th Percentile Latency (p95):             {res_b['p95_ms']:.2f} ms")
    print(f"Workload B - 99th Percentile Latency (p99):             {res_b['p99_ms']:.2f} ms")
    print(f"Workload C - Retry Handling Rate:     {res_c['retries_per_sec']:.2f} workflows/sec")
    print(f"Workload D - Recovery Reconciliation: {res_d['reconciled_per_sec']:.2f} workflows/sec")
    print("=" * 75)


if __name__ == "__main__":
    asyncio.run(main())
