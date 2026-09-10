# NexusFlow V1

Deterministic, durable workflow orchestration engine backed by PostgreSQL 16 as the sole durable authority.

[![CI](https://github.com/binary-beater/nexusflow/actions/workflows/ci.yml/badge.svg)](https://github.com/binary-beater/nexusflow/actions/workflows/ci.yml)
[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/release/python-3120/)
[![PostgreSQL 16](https://img.shields.io/badge/postgresql-16-336791.svg)](https://www.postgresql.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 1. Overview & Core Philosophy

NexusFlow is an orchestrator designed to solve distributed task synchronization, failure recovery, and workflow state persistence with mathematical rigor and zero split-brain ambiguity.

### Key Architectural Invariants
1. **PostgreSQL is the Sole Durable Authority (ADR-001):** No secondary brokers, queues, or distributed consensus engines (no Kafka, Redis, or Celery). Everything from task claims to heartbeat renewals is an atomic database transaction.
2. **State Machine Integrity via Strict OCC (ADR-006, ADR-007):** 100% of normal execution lifecycle operations use Optimistic Concurrency Control (evision = :expected_revision).
3. **Narrow Row Locks Limited to Direction Boundaries (ADR-008):** Pessimistic SELECT ... FOR UPDATE is strictly restricted to workflow direction changes (RUNNING -> FAILING, RUNNING -> CANCELLING) to prevent races during sibling task drains.
4. **Candidate / Offer is NOT Ownership (ADR-009):** Tasks are offered to workers concurrently; ownership commits **only** upon successful creation of an execution_attempts record.
5. **Auditable History is an Immutable Append-Only Log:** History entries are created in the same transaction as state transitions, but active state is stored in concrete relational tables.
6. **Zero-Loss Crash Recovery (ADR-011):** Stateless recovery engine scans PostgreSQL upon startup or recovery cycle and reconciles incomplete workflows without event log replays.

---

## 2. System Architecture

`mermaid
flowchart TD
    subgraph Clients[Client Ecosystem]
        direction TB
        CLI[Admin CLI / Curl]
        App[Upstream Services]
    end

    subgraph ControlPlane[NexusFlow Control Plane (FastAPI)]
        direction TB
        Ingestion[Definition Ingestion & AST Validator]
        Scheduler[Execution Scheduler & Evaluator]
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
    Workers <-->|HTTP Long Poll & Heartbeats| ControlPlane
    ControlPlane -.->|Metrics Scraping| Prom
    ControlPlane -.->|OTLP Traces| Jaeg
    Prom --> Graf
    Jaeg --> Graf
`

---

## 3. Workflow Execution & Ownership Flow

`mermaid
sequenceDiagram
    autonumber
    participant Client as Public API Client
    participant CP as Control Plane
    participant DB as PostgreSQL 16
    participant W as Worker Runtime

    Client->>CP: POST /v1/executions (definition_id, input)
    CP->>DB: INSERT workflow_executions (INITIALIZING)
    CP->>DB: INSERT task_executions (PENDING)
    CP->>DB: UPDATE workflow_executions (RUNNING)
    CP-->>Client: 201 Created (workflow_execution_id)

    loop Scheduler Cycle
        CP->>DB: SELECT PENDING tasks with satisfied dependencies
        CP->>DB: UPDATE task_executions (RUNNABLE)
    end

    W->>CP: POST /v1/worker/tasks/poll (worker_session_id)
    CP-->>W: 200 OK (offer: task_id, revision)
    
    Note over W,DB: Attempt Creation IS Ownership Commit
    W->>CP: POST /v1/worker/tasks/{id}/claim
    CP->>DB: UPDATE task_executions (CLAIMED) WHERE revision=rev
    CP->>DB: INSERT execution_attempts (state=RUNNING, attempt=1)
    CP-->>W: 200 OK (attempt_id, attempt_revision)

    W->>W: Execute User Activity Function
    W->>CP: POST /v1/worker/tasks/{id}/heartbeat
    CP->>DB: UPDATE execution_attempts (last_heartbeat_utc=now)

    W->>CP: POST /v1/worker/tasks/{id}/report_success (output)
    CP->>DB: UPDATE execution_attempts (SUCCEEDED)
    CP->>DB: UPDATE task_executions (SUCCEEDED, output)
    CP->>DB: UPDATE workflow_executions (SUCCEEDED)
    CP-->>W: 200 OK
`

---

## 4. Failure, Retry & Drain Architecture

`mermaid
stateDiagram-v2
    [*] --> RUNNING: Task Started
    RUNNING --> RETRY_WAIT: Transient Failure (budget remains)
    RETRY_WAIT --> RUNNABLE: Exponential Backoff Elapses
    RUNNABLE --> RUNNING: New Attempt Claimed

    RUNNING --> FAILING: Permanent Failure / Retry Exhaustion
    note right of FAILING
      Acquires SELECT ... FOR UPDATE on owning workflow.
      Drains all unstarted siblings to CANCELLED.
    end note

    FAILING --> FAILED: In-Flight Attempts Settle
    
    RUNNING --> CANCELLING: POST /v1/executions/{id}/cancel
    CANCELLING --> CANCELLED: All Attempts Acknowledged / Deadlines Expired
`

---

## 5. Performance Benchmarks

Measured on **PostgreSQL 16 (postgres:16-alpine)** via enchmarks/benchmark_runner.py on Intel Core i7-1165G7 @ 2.80GHz:

| Metric | Measured Result | Specification Defense |
| :--- | :--- | :--- |
| **Pipeline Task Throughput** | **12.53 tasks/sec** | 100% durable commits across 3-stage pipeline |
| **End-to-End Workflow Throughput** | **4.18 workflows/sec** | Complete DAG execution & success settlement |
| **Scheduling Latency (p50 / Median)** | **116.76 ms** | Round-trip client submission to worker claim |
| **Scheduling Latency (p95)** | **146.47 ms** | Sub-150ms tail latency under OCC concurrency |
| **Scheduling Latency (p99)** | **150.91 ms** | Strict bound on tail latency |
| **Transient Retry Handling Rate** | **18.61 workflows/sec** | Flaky error backoff & retry recovery rate |
| **Crash Recovery Rate** | **13.94 workflows/sec** | Reconciles orphaned workflows on reboot |

*Detailed benchmark report and methodology available at [docs/benchmarks/v1-local-benchmark.md](file:///c:/Users/KIIT/Desktop/nexusflow/docs/benchmarks/v1-local-benchmark.md).*

---

## 6. Quick Start

### 1. Start Complete Docker Compose Stack
Starts PostgreSQL 16, Control Plane, Prometheus, Grafana, and Jaeger:
`ash
docker compose up -d
`

Service endpoints:
- **NexusFlow API & OpenAPI Docs:** [http://localhost:8000/docs](http://localhost:8000/docs)
- **Prometheus Metrics:** [http://localhost:9090](http://localhost:9090)
- **Grafana Dashboard:** [http://localhost:3000](http://localhost:3000) (User: dmin, Pass: dmin)
- **Jaeger Distributed Tracing:** [http://localhost:16686](http://localhost:16686)

### 2. Run Database Migrations
`ash
uv run alembic upgrade head
`

### 3. Run Live Demonstration Suite (Scenarios A through E)
`ash
uv run python examples/run_demos.py
`
Outputs live trace for:
- **Scenario A:** 3-stage happy path pipeline.
- **Scenario B:** Flaky task retry and backoff recovery.
- **Scenario C:** Retry exhaustion and controlled drain.
- **Scenario D:** Cancellation lifecycle and sibling drain.
- **Scenario E:** Cold crash recovery reconciliation.

### 4. Run Benchmark Suite
`ash
uv run python benchmarks/benchmark_runner.py
`

### 5. Run Verification Quality Gates
`ash
# Automated Test Suite (100% Real PostgreSQL 16)
uv run pytest -v

# Static Type Check & Linter
uv run pyright
uv run ruff check .
`

---

## 7. Documentation Index
- **[Interview Guide](file:///c:/Users/KIIT/Desktop/nexusflow/docs/interview-guide.md):** Deep-dive interview questions and architectural defenses.
- **[Benchmark Report](file:///c:/Users/KIIT/Desktop/nexusflow/docs/benchmarks/v1-local-benchmark.md):** Complete performance measurements and methodology.
- **[Architecture Decision Records (ADRs)](file:///c:/Users/KIIT/Desktop/nexusflow/docs/adr/):** ADR-001 through ADR-023.
- **[Low-Level Designs (LLDs)](file:///c:/Users/KIIT/Desktop/nexusflow/docs/design/):** LLD-01 through LLD-10.
