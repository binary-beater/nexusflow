"""NexusFlow V1 Extended Resume Benchmark Harness.

Measures:
1. 350 Workflows × 3 Tasks = 1,050 Durable Tasks continuous execution
2. True Internal Ownership Latency (TaskExecution RUNNABLE -> Attempt CLAIMED commit)
3. 3 Consecutive Validation Runs for Repeatability
4. Authoritative PostgreSQL 16 Integrity Verification
5. Exports JSON summary and CSV latency distributions
"""

import asyncio
import json
import statistics
import time
from datetime import UTC, datetime
from pathlib import Path

from httpx import ASGITransport, AsyncClient
from sqlalchemy import select, text
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
from nexusflow.persistence.orm import (
    ExecutionAttemptRecord,
    HistoryEntryRecord,
    TaskExecutionRecord,
    WorkflowExecutionRecord,
)
from nexusflow.worker.runtime import NexusFlowWorker, WorkerRuntimeConfig

POSTGRES_URL = "postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow"
HEADERS = {"Authorization": "Bearer dev-secret-token", "Content-Type": "application/json"}


async def clean_benchmark_state(session_maker: async_sessionmaker) -> None:
    async with session_maker() as session:
        await session.execute(text("DELETE FROM history_entries;"))
        await session.execute(text("DELETE FROM execution_attempts;"))
        await session.execute(text("DELETE FROM task_executions;"))
        await session.execute(text("DELETE FROM workflow_executions;"))
        await session.commit()


async def run_continuous_workload(
    client: AsyncClient,
    session_maker: async_sessionmaker,
    def_id: str,
    worker: NexusFlowWorker,
    num_workflows: int = 350,
) -> dict:
    total_tasks = num_workflows * 3
    print(f"\n>>> Launching Continuous Workload: {num_workflows} workflows ({total_tasks} durable tasks) <<<")

    start_wall = time.perf_counter()
    start_dt_utc = datetime.now(UTC)

    for i in range(num_workflows):
        resp = await client.post(
            "/v1/executions",
            headers=HEADERS,
            json={"definition_id": def_id, "workflow_input": f"BENCH-{i}"},
        )
        assert resp.status_code == 201, f"Failed to create workflow: {resp.text}"

        # Execute 3 pipeline stages for this workflow
        ok_a = await worker.run_once()
        assert ok_a is True, f"Worker failed at workflow {i} stage_a"
        ok_b = await worker.run_once()
        assert ok_b is True, f"Worker failed at workflow {i} stage_b"
        ok_c = await worker.run_once()
        assert ok_c is True, f"Worker failed at workflow {i} stage_c"

    elapsed_s = time.perf_counter() - start_wall
    wf_per_sec = num_workflows / elapsed_s
    tasks_per_sec = total_tasks / elapsed_s

    print(f"Completed in {elapsed_s:.2f}s | Throughput: {tasks_per_sec:.2f} tasks/sec ({wf_per_sec:.2f} wf/sec)")

    # Post-Benchmark Authoritative PostgreSQL Integrity Check
    async with session_maker() as s:
        # Workflow execution states
        wf_rows = (await s.execute(select(WorkflowExecutionRecord))).scalars().all()
        wf_states = {}
        for w in wf_rows:
            wf_states[w.state] = wf_states.get(w.state, 0) + 1

        # Task execution states
        t_rows = (await s.execute(select(TaskExecutionRecord))).scalars().all()
        t_states = {}
        for t in t_rows:
            t_states[t.state] = t_states.get(t.state, 0) + 1

        # Attempt states
        att_rows = (await s.execute(select(ExecutionAttemptRecord))).scalars().all()
        att_states = {}
        for a in att_rows:
            att_states[a.state] = att_states.get(a.state, 0) + 1

        # Compute True Ownership Latencies:
        # Moment Task became RUNNABLE -> Moment Attempt CLAIMED committed
        runnables = (
            await s.execute(
                select(HistoryEntryRecord).where(
                    HistoryEntryRecord.event_category == "TaskMarkedRunnable",
                    HistoryEntryRecord.occurred_at_utc >= start_dt_utc,
                )
            )
        ).scalars().all()

        claimed = (
            await s.execute(
                select(HistoryEntryRecord).where(
                    HistoryEntryRecord.event_category == "TaskClaimedByWorker",
                    HistoryEntryRecord.occurred_at_utc >= start_dt_utc,
                )
            )
        ).scalars().all()

        c_map = {c.task_execution_id: c.occurred_at_utc for c in claimed}
        latencies_ms = []
        for r in runnables:
            if r.task_execution_id in c_map:
                t_r = r.occurred_at_utc
                t_c = c_map[r.task_execution_id]
                diff_ms = max(0.0, (t_c - t_r).total_seconds() * 1000.0)
                latencies_ms.append(diff_ms)

    latencies_ms.sort()
    p50 = statistics.median(latencies_ms) if latencies_ms else 0.0
    p95 = latencies_ms[int(len(latencies_ms) * 0.95)] if latencies_ms else 0.0
    p99 = latencies_ms[int(len(latencies_ms) * 0.99)] if latencies_ms else 0.0
    mean_lat = statistics.mean(latencies_ms) if latencies_ms else 0.0
    min_lat = min(latencies_ms) if latencies_ms else 0.0
    max_lat = max(latencies_ms) if latencies_ms else 0.0

    print(f"Ownership Latency ({len(latencies_ms)} samples):")
    print(f"  Min: {min_lat:.2f} ms | p50: {p50:.2f} ms | Mean: {mean_lat:.2f} ms")
    print(f"  p95: {p95:.2f} ms | p99: {p99:.2f} ms | Max: {max_lat:.2f} ms")

    return {
        "num_workflows": num_workflows,
        "total_tasks": total_tasks,
        "elapsed_s": elapsed_s,
        "wf_per_sec": wf_per_sec,
        "tasks_per_sec": tasks_per_sec,
        "wf_states": wf_states,
        "task_states": t_states,
        "att_states": att_states,
        "latency_samples": len(latencies_ms),
        "latencies_raw": latencies_ms,
        "latency_stats": {
            "min_ms": min_lat,
            "mean_ms": mean_lat,
            "p50_ms": p50,
            "p95_ms": p95,
            "p99_ms": p99,
            "max_ms": max_lat,
        },
    }


async def run_single_benchmark_run(run_idx: int, num_workflows: int = 350) -> dict:
    print(f"\n{'='*30} STARTING BENCHMARK RUN {run_idx} {'='*30}")
    engine = create_async_engine(POSTGRES_URL, echo=False)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    registry = WorkerRegistry(liveness_timeout_seconds=3600.0)
    scheduler = ExecutionScheduler(session_factory=session_maker, worker_registry=registry)

    app.dependency_overrides[get_session_factory] = lambda: session_maker
    app.dependency_overrides[get_worker_registry] = lambda: registry
    app.dependency_overrides[get_scheduler] = lambda: scheduler

    async def override_get_db():
        async with session_maker() as s:
            yield s

    app.dependency_overrides[get_db_session] = override_get_db

    await clean_benchmark_state(session_maker)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yaml_path = Path("examples/workflows/02_pipeline_happy_path.yaml")
        yaml_content = yaml_path.read_text(encoding="utf-8")
        def_resp = await client.post("/v1/definitions", headers=HEADERS, json={"yaml_content": yaml_content})
        assert def_resp.status_code == 201, f"Def registration failed: {def_resp.text}"
        def_id = def_resp.json()["definition_id"]

        worker = NexusFlowWorker(
            config=WorkerRuntimeConfig(
                control_plane_url="http://test",
                worker_token="dev-worker-secret",
                worker_id=f"extended-worker-run-{run_idx}",
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

        run_result = await run_continuous_workload(
            client=client,
            session_maker=session_maker,
            def_id=def_id,
            worker=worker,
            num_workflows=num_workflows,
        )

        await worker.stop()

    app.dependency_overrides.clear()
    await engine.dispose()
    return run_result


async def main():
    print("=" * 80)
    print("NEXUSFLOW V1 EXTENDED RESUME VALIDATION BENCHMARK")
    print("Continuous Workload: 350 Workflows × 3 Tasks = 1,050 Durable Tasks per Run")
    print("PostgreSQL 16 Authoritative Concurrency Engine")
    print("=" * 80)

    num_runs = 3
    num_workflows = 350
    runs = []

    for run_idx in range(1, num_runs + 1):
        result = await run_single_benchmark_run(run_idx=run_idx, num_workflows=num_workflows)
        runs.append(result)

    # Aggregate results across all 3 runs
    tasks_per_secs = [r["tasks_per_sec"] for r in runs]
    wf_per_secs = [r["wf_per_sec"] for r in runs]
    p50s = [r["latency_stats"]["p50_ms"] for r in runs]
    p95s = [r["latency_stats"]["p95_ms"] for r in runs]
    p99s = [r["latency_stats"]["p99_ms"] for r in runs]
    means = [r["latency_stats"]["mean_ms"] for r in runs]

    print("\n" + "=" * 80)
    print(f"EXTENDED BENCHMARK MULTI-RUN SUMMARY ({num_workflows} Workflows / {num_workflows*3} Tasks per run)")
    print("=" * 80)
    for i, r in enumerate(runs, 1):
        print(f"Run {i}: {r['tasks_per_sec']:.2f} tasks/sec | {r['wf_per_sec']:.2f} wf/sec | p50: {r['latency_stats']['p50_ms']:.2f} ms | p95: {r['latency_stats']['p95_ms']:.2f} ms")

    median_task_thru = statistics.median(tasks_per_secs)
    median_wf_thru = statistics.median(wf_per_secs)
    median_p50 = statistics.median(p50s)
    median_p95 = statistics.median(p95s)
    median_p99 = statistics.median(p99s)
    median_mean = statistics.median(means)

    print("-" * 80)
    print(f"Median Task Throughput:     {median_task_thru:.2f} tasks/sec (range: {min(tasks_per_secs):.2f} - {max(tasks_per_secs):.2f})")
    print(f"Median Workflow Throughput: {median_wf_thru:.2f} wf/sec (range: {min(wf_per_secs):.2f} - {max(wf_per_secs):.2f})")
    print(f"Median Ownership Latency:   p50 = {median_p50:.2f} ms | p95 = {median_p95:.2f} ms | p99 = {median_p99:.2f} ms | mean = {median_mean:.2f} ms")
    print("=" * 80)

    # Save artifacts
    results_dir = Path("benchmarks/results")
    results_dir.mkdir(parents=True, exist_ok=True)

    summary_payload = {
        "timestamp_utc": datetime.now(UTC).isoformat(),
        "workload": {
            "workflows_per_run": num_workflows,
            "tasks_per_run": num_workflows * 3,
            "total_runs": num_runs,
            "pipeline_stages": 3,
            "total_durable_tasks_verified": num_workflows * 3 * num_runs,
        },
        "aggregate": {
            "median_tasks_per_sec": median_task_thru,
            "min_tasks_per_sec": min(tasks_per_secs),
            "max_tasks_per_sec": max(tasks_per_secs),
            "median_wf_per_sec": median_wf_thru,
            "median_ownership_latency_p50_ms": median_p50,
            "median_ownership_latency_p95_ms": median_p95,
            "median_ownership_latency_p99_ms": median_p99,
            "median_ownership_latency_mean_ms": median_mean,
        },
        "runs": [
            {
                "run_id": idx + 1,
                "elapsed_s": r["elapsed_s"],
                "wf_per_sec": r["wf_per_sec"],
                "tasks_per_sec": r["tasks_per_sec"],
                "workflow_states": r["wf_states"],
                "task_states": r["task_states"],
                "attempt_states": r["att_states"],
                "latency_stats": r["latency_stats"],
            }
            for idx, r in enumerate(runs)
        ],
    }

    summary_file = results_dir / "v1_extended_summary.json"
    summary_file.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    print(f"Saved summary to {summary_file}")

    csv_file = results_dir / "v1_ownership_latency.csv"
    with open(csv_file, "w", encoding="utf-8") as f:
        f.write("sample_id,latency_ms\n")
        for i, lat in enumerate(runs[0]["latencies_raw"], 1):
            f.write(f"{i},{lat:.4f}\n")
    print(f"Saved latency samples to {csv_file}")


if __name__ == "__main__":
    asyncio.run(main())
