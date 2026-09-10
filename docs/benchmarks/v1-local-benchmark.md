# NexusFlow V1 — Local Performance & Stress Benchmark Report

**Date:** 2026-09-10  
**Environment:** Local Docker Engine on Windows 11  
**Database Authority:** PostgreSQL 16.15 (Alpine 15.2.0, x86_64), asyncpg driver with READ COMMITTED + OCC  
**Hardware Specifications:**
- **CPU:** 11th Gen Intel(R) Core(TM) i7-1165G7 @ 2.80GHz (4 Physical Cores, 8 Logical Processors)
- **RAM:** 16.0 GB LPDDR4x (15.56 GB usable)
- **OS:** Windows 11 Home (Build 10.0.26200)
- **Python:** 3.12.4 (uv package manager)
- **Database Engine:** PostgreSQL 16 (postgres:16-alpine container, port 5432)
- **Control Plane:** FastAPI ASGI async control plane with asyncpg connection pool (min: 5, max: 20)

---

## 1. Executive Summary

NexusFlow V1 was subjected to end-to-end performance and stress benchmarking using real PostgreSQL 16 persistence transactions. No in-memory SQLite instances were used for integration verification. All transactions traversed the full HTTP/REST API stack, FastAPI middleware, state machine validation, optimistic concurrency control (OCC) checks, and durable PostgreSQL writes with WAL fsync.

### Baseline Benchmark Workloads (Initial Audit)

| Workload | Target Characteristic | Measured Metric | Status |
| :--- | :--- | :--- | :--- |
| **Workload A** | Pipeline Throughput (40 Workflows / 120 Tasks) | **12.53 tasks/sec** (4.18 workflows/sec) | MEASURED |
| **Workload B** | Client Submission to Worker Claim Latency | **p50: 116.76 ms**, **p95: 146.47 ms**, **p99: 150.91 ms** | MEASURED |
| **Workload C** | Retry & Transient Failure Load | **18.61 workflows/sec** | MEASURED |
| **Workload D** | Recovery Reconciliation Throughput | **13.94 workflows/sec** | MEASURED |

---

## 2. Extended Resume Validation Benchmark (Continuous 350 Workflows / 1,050 Tasks)

To verify continuous execution scale and isolate true internal task-ownership latency, an extended continuous benchmark was executed across **3 independent, isolated runs** of a 3-stage linear DAG (`stage_a` -> `stage_b` -> `stage_c`):
- **Workload Spec:** 350 complete workflow executions per run (1,050 durable task executions per run, totaling 3,150 tasks).
- **Execution Model:** Full end-to-end HTTP REST API submissions, dependency evaluation, atomic ownership commits in PostgreSQL, worker execution start, user activity execution, and callback settlement.
- **Latency Definition (True Internal Ownership Latency):** Measured exact delta from the moment `TaskExecution` becomes durably `RUNNABLE` (commit of `commit_task_readiness` or initialization) to the moment durable attempt ownership commits (`commit_attempt_ownership`: `TaskExecution` `RUNNABLE` -> `RUNNING` + `ExecutionAttempt` inserted as `CLAIMED`).

### Multi-Run Empirical Results

| Run Index | Workflows Completed | Tasks Settled | Elapsed (s) | Task Throughput | Workflow Throughput | Ownership Latency (p50) | Ownership Latency (p95) | Ownership Latency (p99) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Run 1** | **350 / 350 (100%)** | **1,050 / 1,050** | 80.03s | **13.12 tasks/sec** | **4.37 wf/sec** | **11.60 ms** | **18.45 ms** | **22.39 ms** |
| **Run 2** | **350 / 350 (100%)** | **1,050 / 1,050** | 81.11s | **12.95 tasks/sec** | **4.32 wf/sec** | **12.54 ms** | **17.86 ms** | **22.75 ms** |
| **Run 3** | **350 / 350 (100%)** | **1,050 / 1,050** | 77.25s | **13.59 tasks/sec** | **4.53 wf/sec** | **11.69 ms** | **17.68 ms** | **21.48 ms** |
| **Aggregate Median** | **350 / 350 (100%)** | **1,050 / 1,050** | **80.03s** | **13.12 tasks/sec** | **4.37 wf/sec** | **11.69 ms** | **17.86 ms** | **22.39 ms** |

### Post-Benchmark Database State Integrity Audit
Following each continuous run, authoritative relational state in PostgreSQL 16 was directly queried:
- `workflow_executions`: Exactly 350 rows in `SUCCEEDED` state. 0 rows in `INITIALIZING`, `RUNNING`, `FAILING`, or `CANCELLING`.
- `task_executions`: Exactly 1,050 rows in `SUCCEEDED` state. 0 rows in `PENDING`, `RUNNABLE`, `RUNNING`, or `RETRY_WAIT`.
- `execution_attempts`: Exactly 1,050 rows in `SUCCEEDED` state. 0 rows in `CLAIMED` or `RUNNING`.
- Unexpected Errors / OCC Conflicts: **0**.
- Machine-readable evidence: Saved to `benchmarks/results/v1_extended_summary.json` and `benchmarks/results/v1_ownership_latency.csv`.

---

## 3. Resume & Portfolio Claims Audit

| Candidate Resume Claim | Audit Status | Measured Evidence & Classification |
| :--- | :--- | :--- |
| **100+ workflows executed** | **VERIFIED** | Successfully executed **350 concurrent/continuous workflows** in a single run (tested across 3 consecutive runs = 1,050 total workflows). |
| **1,000+ tasks processed** | **VERIFIED** | Successfully executed **1,050 durable tasks** per continuous run (tested across 3 consecutive runs = 3,150 total durable tasks). |
| **95%+ success rate** | **VERIFIED** (in benchmark context) | **100% of workflows (350/350)** and **100% of tasks (1,050/1,050)** succeeded without error in the deterministic local benchmark. *(Must not be claimed as generic production SLA).* |
| **Median scheduling latency < 200 ms** | **VERIFIED** | **Median true ownership latency is 11.69 ms** (p95: 17.86 ms, p99: 22.39 ms). Even end-to-end client-to-claim latency was measured at **116.76 ms** (p50). Both are strictly below 200 ms. |

---

## 4. Empirically Defensible Resume Bullets

1. **Scale & Orchestration Engine:**  
   *Built NexusFlow, a distributed workflow orchestration engine in FastAPI and PostgreSQL 16, executing 1,050+ durable tasks across 350 continuous workflows at ~13 tasks/sec in local benchmarks with 100% completion.*
2. **Distributed Systems Correctness:**  
   *Engineered durable task ownership via OCC revisions, worker-session fencing, retries with exponential backoff, cancellation drain, and crash recovery, validated across 67 integration tests.*
3. **Low Latency & Observability:**  
   *Measured p50/p95 durable task ownership latency of 11.7 ms / 17.9 ms backed by PostgreSQL ACID transactions, instrumenting execution spans with OpenTelemetry, Prometheus, Grafana, and Jaeger.*
