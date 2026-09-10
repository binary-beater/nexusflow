# NexusFlow V1

Deterministic, durable workflow orchestration engine backed by PostgreSQL 16 as the sole durable authority.

[![CI](https://github.com/binary-beater/nexusflow/actions/workflows/ci.yml/badge.svg)](https://github.com/binary-beater/nexusflow/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![PostgreSQL 16](https://img.shields.io/badge/postgresql-16-336791.svg)](https://www.postgresql.org/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

---

## 1. Overview & Core Philosophy

NexusFlow is a correctness-focused distributed workflow orchestration engine built with FastAPI and PostgreSQL.

It executes DAG-based workflows across external workers while providing durable task ownership, retries, cancellation, crash recovery, OCC-based concurrency control, and observability.

### Why This Project?
Distributed orchestration systems often suffer from subtle race conditions, split-brain state between brokers and databases, or dual-write inconsistencies. NexusFlow was built to demonstrate rigorous distributed systems correctness:
- Eliminating broker/database dual-write divergence by making PostgreSQL the single source of truth.
- Enforcing state machine invariants with explicit Optimistic Concurrency Control (OCC).
- Fencing duplicate authoritative progression via durable ownership and OCC, with deterministic recovery without event replay.

### Genuine Implemented Capabilities

| Capability | Implementation Mechanism |
| :--- | :--- |
| **DAG Workflow Definitions** | Strict AST validation, cycle detection, strongly typed JSON inputs/outputs |
| **Dependency-Aware Scheduling** | Automatic readiness evaluation (PENDING -> RUNNABLE) as parent tasks complete |
| **Distributed External Workers** | HTTP worker protocol with long-polling (/internal/v1/worker/poll) |
| **Durable Task / Attempt Ownership** | Atomic ownership commit (RUNNABLE -> RUNNING + attempt created as CLAIMED) |
| **Worker Session Fencing** | Ephemeral WorkerSessionId validation prevents stale workers from reporting results |
| **Retries & Retry Exhaustion** | Structured RETRY_WAIT state with configured retry delay and retry budget ceilings |
| **Timeouts & Worker Loss** | start_deadline_utc enforcement and worker heartbeat liveness monitors |
| **Workflow Cancellation & Draining** | Directional row locks (SELECT ... FOR UPDATE) safely drain unstarted sibling tasks |
| **PostgreSQL Crash Recovery** | Stateless StartupRecoveryEngine reconstructs and repairs state from current relational data |
| **OCC Concurrency Protection** | Strict `revision = :expected_revision` predicate prevents dirty writes |
| **Durable Audit Trail** | Append-only history_entries written in the exact same transaction as state mutations |
| **Full Observability Stack** | Native Prometheus /metrics, OpenTelemetry distributed tracing, Grafana, and Jaeger |
| **Docker Compose** | One-command orchestration bringing up Control Plane, PostgreSQL, Prometheus, Grafana, Jaeger |
| **CI Integration Tests** | GitHub Actions pipeline testing against a real PostgreSQL 16 container instance |

### Correctness Model
NexusFlow enforces strict engineering invariants:
1. **PostgreSQL as the Sole Durable Authority:** No secondary brokers or queues (no Kafka, Redis, or Celery). State transitions and ownership commits are atomic database transactions.
2. **Candidate / Offer != Ownership:** Tasks are offered to eligible workers concurrently; ownership commits **only** upon successful creation of an execution_attempts record.
3. **Attempt Creation = Ownership Commit:** Atomically transitions `task_executions` from RUNNABLE to RUNNING and inserts execution_attempts in CLAIMED state with the allocated ordinal.
4. **Expected State + Revision Predicates:** OCC updates enforce WHERE state = :expected_state AND revision = :expected_revision.
5. **First Valid Durable Commit Wins:** Concurrent claim or transition races resolve deterministically; losers receive OCC_CONFLICT and safely back off.
6. **History is an Audit Trail, Not Orchestration Authority:** Relational tables store authoritative active state; history is an immutable append-only record.
7. **Recovery Reconstructs from Durable Current State:** On startup, the recovery engine scans active rows rather than replaying historical events.

---

## 2. System Architecture

```mermaid
flowchart TD
    subgraph Clients[Client Ecosystem]
        direction TB
        CLI[Admin CLI / Curl]
        App[Upstream Services]
    end

    subgraph ControlPlane[NexusFlow Control Plane (FastAPI)]
        direction TB
        Ingestion[Definition Ingestion & AST Validator]
        Scheduler[Execution Scheduler & Dispatcher]
        WorkerReg[Worker Registry (Ephemeral)]
        Recovery[Startup Recovery Engine]
        API[Public HTTP REST API]
    end

    subgraph Persistence[PostgreSQL 16 (Authoritative Store)]
        direction TB
        DefTable[(registered_definitions)]
        WfTable[(workflow_executions)]
        TaskTable[(task_executions)]
        AttTable[(execution_attempts)]
        HistTable[(history_entries (Audit Trail))]
    end

    subgraph Workers[Worker Runtime Fleet]
        direction LR
        W1[Worker Node A]
        W2[Worker Node B]
    end

    subgraph Observability[Observability Stack]
        direction LR
        Prom[Prometheus (:9090)]
        Graf[Grafana (:3000)]
        Jaeg[Jaeger (:16686)]
    end

    CLI --> API
    App --> API
    API --> Ingestion
    API --> Scheduler
    Scheduler --> Persistence
    Recovery --> Persistence
    Workers <-->|HTTP Long-Poll & Callbacks| ControlPlane
    ControlPlane -.->|Metrics Scraping| Prom
    ControlPlane -.->|OTLP Traces| Jaeg
    Prom --> Graf
    Jaeg --> Graf
`

---

## 3. Workflow Execution & Ownership Flow

```mermaid
sequenceDiagram
    autonumber
    participant Client as Public API Client
    participant CP as Control Plane (FastAPI)
    participant DB as PostgreSQL 16
    participant W as Worker Runtime

    Client->>CP: POST /v1/executions (definition_id, input)
    CP->>DB: INSERT workflow_executions (INITIALIZING)
    CP->>DB: INSERT task_executions (PENDING)
    CP->>DB: UPDATE workflow_executions (RUNNING)
    CP-->>Client: 201 Created (workflow_execution_id)

    loop Scheduler Readiness & Dispatch Cycle
        CP->>DB: UPDATE task_executions SET state='RUNNABLE' WHERE dependencies satisfied
        Note over CP,DB: Atomic Ownership Commit in PostgreSQL
        CP->>DB: UPDATE task_executions (RUNNABLE -> RUNNING) + INSERT execution_attempts (CLAIMED)
    end

    W->>CP: POST /internal/v1/worker/poll (worker_session_id)
    CP-->>W: 200 OK (assignment: attempt_id, task_execution_id, stable_input)

    W->>CP: POST /internal/v1/worker/start (attempt_id, worker_session_id)
    CP->>DB: UPDATE execution_attempts (CLAIMED -> RUNNING) WHERE revision=1
    CP-->>W: 200 OK (status=ACCEPTED)

    loop Periodic Liveness
        W->>CP: POST /internal/v1/worker/heartbeat (worker_session_id)
        CP-->>W: 200 OK (status=ACCEPTED)
    end

    W->>W: Execute User Activity Function

    W->>CP: POST /internal/v1/worker/callback (attempt_id, outcome=SUCCESS, output)
    CP->>DB: UPDATE execution_attempts (SUCCEEDED) + UPDATE task_executions (SUCCEEDED)
    CP->>DB: Check workflow completion -> UPDATE workflow_executions (SUCCEEDED)
    CP-->>W: 200 OK
`

---

## 4. State Machines & Failure Handling

### Task Execution Lifecycle
```mermaid
stateDiagram-v2
    [*] --> PENDING: Workflow Initialized
    PENDING --> RUNNABLE: Dependencies Succeeded
    RUNNABLE --> RUNNING: Ownership Committed (Attempt Created as CLAIMED)
    RUNNING --> RETRY_WAIT: Transient Failure (Budget Remaining)
    RETRY_WAIT --> RUNNABLE: Backoff Interval Elapsed
    RUNNING --> SUCCEEDED: Activity Success Callback
    RUNNING --> FAILED: Retry Budget Exhausted / Unrecoverable Failure
    RUNNABLE --> CANCELLED: Workflow Cancelled / Sibling Drain
    PENDING --> CANCELLED: Workflow Cancelled / Sibling Drain
`

### Workflow Direction Boundaries & Drain Semantics
When an unrecoverable task failure or cancellation occurs:
1. **Pessimistic Direction Lock:** The Control Plane acquires SELECT ... FOR UPDATE exclusively on the root workflow_executions row.
2. **Transition to Draining:** The workflow enters FAILING or CANCELLING.
3. **Sibling Drain:** Unstarted sibling tasks in PENDING or RUNNABLE are immediately transitioned to CANCELLED.
4. **In-Flight Settlement:** In-flight worker attempts are allowed to complete or abort within their deadline.
5. **Terminal Settlement:** Once all tasks reach a terminal state (SUCCEEDED, FAILED, CANCELLED), the workflow transitions to FAILED or CANCELLED.

---

## 5. Performance Benchmarks

Local benchmark on an Intel Core i7-1165G7 @ 2.80GHz with PostgreSQL 16:
- **Extended Continuous Scale:** **350 workflow executions** / **1,050 durable task executions** per run (3 runs executed, 3,150 total durable tasks).
- **100% completion** (350/350 workflows, 1,050/1,050 tasks) for the deterministic benchmark workload.
- **Task Throughput:** **13.12 tasks/sec** (~4.37 workflows/sec) for a 3-stage durable workflow pipeline.
- **True Ownership Latency (RUNNABLE -> CLAIMED):** **p50 = 11.69 ms**, **p95 = 17.86 ms**, **p99 = 22.39 ms**.
- **Client Submission-to-Claim Latency:** **p50 = 116.76 ms** (p95: 146.47 ms).
- **Crash Recovery Reconciliation:** **13.94 workflows/sec** via `StartupRecoveryEngine`.

| Metric | Measured Result | Specification Context |
| :--- | :--- | :--- |
| **Continuous Task Throughput** | **13.12 tasks/sec** | 350 workflows / 1,050 durable tasks (tested across 3 consecutive runs) |
| **End-to-End Workflow Throughput** | **4.37 workflows/sec** | Complete 3-stage DAG execution & success settlement |
| **Task Ownership Latency (p50)** | **11.69 ms** | Exact internal `TaskExecution` RUNNABLE -> `Attempt` CLAIMED commit |
| **Task Ownership Latency (p95)** | **17.86 ms** | 95th percentile durable ownership latency |
| **Task Ownership Latency (p99)** | **22.39 ms** | 99th percentile durable ownership latency |
| **Client Submission to Claim Latency (p50)** | **116.76 ms** | Round-trip client submission to worker claim commit |
| **Transient Retry Handling Rate** | **18.61 workflows/sec** | Flaky error backoff & retry recovery rate |
| **Recovery Reconciliation Rate** | **13.94 workflows/sec** | Reconciles orphaned workflows via StartupRecoveryEngine |

*Detailed benchmark report, methodology, and CSV distributions available at [docs/benchmarks/v1-local-benchmark.md](docs/benchmarks/v1-local-benchmark.md).*

---

## 6. Quick Start

### 1. Start Docker Compose Stack
Starts PostgreSQL 16, Control Plane, Prometheus, Grafana, and Jaeger:
```bash
cp .env.example .env
docker compose up -d --build
`

Service endpoints:
- **NexusFlow API & OpenAPI Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Prometheus Metrics:** [http://localhost:9090](http://localhost:9090)
- **Grafana Dashboard:** [http://localhost:3000](http://localhost:3000) (User: admin, Pass: admin)
- **Jaeger Tracing:** [http://localhost:16686](http://localhost:16686)

### 2. Run Database Migrations (Local Dev)
If running outside of Docker:
```bash
uv run alembic upgrade head
`

### 3. Run Demonstration Suite (Scenarios A through E)
```bash
uv run python examples/run_demos.py
`
Outputs live trace for:
- **Scenario A:** 3-stage happy path pipeline.
- **Scenario B:** Flaky task retry and backoff recovery.
- **Scenario C:** Retry exhaustion and controlled drain.
- **Scenario D:** Cancellation lifecycle and sibling drain.
- **Scenario E:** Startup recovery & reconciliation demonstration.

### 4. Run Benchmark Suite
```bash
uv run python benchmarks/benchmark_runner.py
`

### 5. Run Verification Quality Gates
```bash
# Automated Test Suite (Tested against PostgreSQL 16)
uv run pytest -v

# Static Type Check & Linter
uv run pyright
uv run ruff check .
`

---

## 7. Repository Structure

```text
src/nexusflow/
  definition/      # AST ingestion, DAG validation, and schema codec
  domain/          # Core domain models, state enums, and identifiers
  orchestration/   # Scheduler, in-memory WorkerRegistry, and StartupRecoveryEngine
  persistence/     # SQLAlchemy ORM records and OCC persistence transactions
  interfaces/      # FastAPI HTTP REST API routes and authentication
  worker/          # External worker runtime, polling, execution, and callbacks
  observability/   # Prometheus metrics and OpenTelemetry trace setup

tests/
  unit/            # Domain model, DAG validation, and state machine tests
  integration/     # Real PostgreSQL 16 integration, recovery, and concurrency tests

examples/
  workflows/       # Declarative YAML workflow definitions
  run_demos.py     # Live multi-scenario demonstration runner

benchmarks/
  benchmark_runner.py # Reproducible benchmark harness and latency audit

docs/
  architecture/    # ADR-001 through ADR-023 and High-Level Design (HLD)
  design/          # LLD-01 through LLD-10 Low-Level Design specifications
  benchmarks/      # Measured benchmark audit report
  interview-guide.md # Systems design questions and architectural defenses
`

---

## 8. Documentation Index
- **[Interview Guide](docs/interview-guide.md):** Deep-dive interview questions and architectural defenses.
- **[Benchmark Report](docs/benchmarks/v1-local-benchmark.md):** Measured performance metrics and audit.
- **[Architecture Decision Records (ADRs)](docs/architecture/):** ADR-001 through ADR-023.
- **[Low-Level Designs (LLDs)](docs/design/):** LLD-01 through LLD-10.
- **[High-Level Design (HLD)](docs/architecture/nexusflow-v1-high-level-design.md):** System architecture overview.
