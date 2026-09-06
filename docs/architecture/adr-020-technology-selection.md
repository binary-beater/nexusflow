# ADR-020 — Technology Selection

* **Status**: Approved — Not Frozen
* **Last Updated**: 2026-09-06
* **Domain**: Implementation Foundation & Technology Selection
* **Criticality**: Core / Implementation Foundation
* **Relationships**:
  * **Builds On**: [ADR-001](adr-001-internal-workflow-specification.md), [ADR-002](adr-002-workflow-definition-parsing-strategy.md), [ADR-003](adr-003-canonical-workflow-graph-representation.md), [ADR-004](adr-004-workflow-validation-strategy.md), [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-006](adr-006-workflow-execution-state-machine.md), [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-009](adr-009-task-routing-strategy.md), [ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md), [ADR-011](adr-011-state-persistence-strategy.md), [ADR-012](adr-012-recovery-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), [ADR-014](adr-014-execution-history-and-audit-model.md), [ADR-015](adr-015-external-api-architecture.md), [ADR-016](adr-016-observability-architecture.md), [ADR-017](adr-017-graceful-shutdown-architecture.md), [ADR-018](adr-018-error-handling-philosophy.md), [ADR-019](adr-019-project-and-service-boundaries.md)
  * **Informs**: [ADR-021](00-architecture-decision-register.md), [ADR-022](00-architecture-decision-register.md), [ADR-023](00-architecture-decision-register.md), [ADR-024](00-architecture-decision-register.md), [ADR-025](00-architecture-decision-register.md), [ADR-026](00-architecture-decision-register.md), [ADR-027](00-architecture-decision-register.md)

---

## 1. Purpose

This Architectural Decision Record (ADR) establishes the concrete technology stack for the NexusFlow orchestration engine in Version 1 (V1). It selects the programming languages, runtime models, database engines, data-access frameworks, migration tooling, worker transport protocols, serialization formats, observability pipelines, packaging environments, and developer tooling necessary to realize the architectural models approved in [ADR-001](adr-001-internal-workflow-specification.md) through [ADR-019](adr-019-project-and-service-boundaries.md).

The central principle of this selection is:

> **Technology selection strictly serves architectural semantics.**  
> **Concrete frameworks, drivers, and libraries are chosen to faithfully implement already-approved lifecycle states, consistency groups, concurrency guards, and boundary invariants without adding unneeded infrastructure dependencies.**

---

## 2. Context

NexusFlow is an orchestration engine designed around a **Modular Monolith Control Plane with an External Distributed Worker Runtime** ([ADR-019](adr-019-project-and-service-boundaries.md)). The system invariants established across preceding ADRs impose strict operational and consistency requirements:
* **Lifecycle & State Machine Integrity**: Non-bypassable transitions for `WorkflowExecution` ([ADR-006](adr-006-workflow-execution-state-machine.md)) and `TaskExecution` / `ExecutionAttempt` ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)).
* **Sole Control-Plane Authority**: The control plane alone mutates authoritative execution state; external workers report observations and results without direct persistence access ([ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-019](adr-019-project-and-service-boundaries.md)).
* **Single Logical Authoritative Persistence Boundary**: Atomic commits across multi-entity consistency groups (attempt ownership, state settlement + output + history) without distributed transactions (2PC) ([ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), [ADR-014](adr-014-execution-history-and-audit-model.md)).
* **Optimistic Single-Winner Concurrency**: OCC revision predicates and conditional writes prevent lost updates and stale callbacks ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
* **Deterministic Current-State Recovery**: Startup and targeted reconciliation reconstruct state directly from current records without event-sourcing replay or durable scheduler cursors ([ADR-012](adr-012-recovery-strategy.md)).
* **Clean Inward Dependency Inversion**: Core domain semantics must remain completely independent of external web frameworks, database drivers, and wire transports ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **Self-Contained Local Deployment**: The entire V1 stack must run reproducibly on a standard developer workstation with zero paid cloud dependencies or mandatory external cloud services.

---

## 3. Problem Statement

Which concrete programming languages, frameworks, storage systems, protocols, and developer tools should NexusFlow select for V1 so that:
1. Every semantic guarantee and invariant established in ADR-001 through ADR-019 is faithfully implemented without distortion or compromise?
2. Multi-entity consistency groups and optimistic concurrency control (OCC) commit atomically within a single logical persistence boundary?
3. External user tasks execute across a network boundary in isolated worker runtimes, communicating via an explicit, versionable worker protocol?
4. The control plane remains testable, observable, and maintainable by a solo developer or small team, avoiding extraneous operational components (e.g., separate message brokers, distributed caches, cluster managers)?
5. The platform runs entirely in a local reference environment using Docker Compose while leaving a clean path for future High Availability ([ADR-025](00-architecture-decision-register.md)) and multi-language worker SDKs ([ADR-026](00-architecture-decision-register.md))?

---

## 4. Requirements Covered

### 4.1 Functional Requirements
* **FR-TEC-001 (Control Plane Implementation)**: Implement the V1 control plane as a single deployable unit using an asynchronous, type-safe runtime model ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **FR-TEC-002 (Authoritative Persistence Engine)**: Provide ACID transactions, relational integrity constraints, conditional row updates, and native JSON support ([ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
* **FR-TEC-003 (Worker Protocol Transport)**: Provide an explicit, technology-neutral, inspectable worker transport protocol supporting registration, heartbeats, two-phase task polling, execution-start observations, results, and best-effort cancellation ([ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-009](adr-009-task-routing-strategy.md)).
* **FR-TEC-004 (Worker Runtime)**: Provide an independently deployable worker runtime capable of executing task handlers locally and reporting observations across network boundaries ([ADR-008](adr-008-worker-coordination-and-liveness-model.md), [ADR-019](adr-019-project-and-service-boundaries.md)).
* **FR-TEC-005 (Schema Migrations)**: Provide version-controlled, reproducible database schema migrations executed outside normal application boot ([ADR-011](adr-011-state-persistence-strategy.md)).
* **FR-TEC-006 (Observability Exposition)**: Expose low-cardinality Prometheus metrics and OpenTelemetry trace contexts fail-open without degrading core orchestration ([ADR-016](adr-016-observability-architecture.md)).

### 4.2 Non-Functional Requirements
* **NFR-TEC-001 (Operational Simplicity)**: Minimize infrastructure dependencies; require zero mandatory distributed brokers or caches in V1 ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **NFR-TEC-002 (Local Reproducibility & Zero Cost)**: Run the complete reference environment via Docker Compose without paid cloud dependencies.
* **NFR-TEC-003 (Decoupled Testability)**: Domain state machines, DAG validation, and scheduling algorithms must be testable in-memory without starting live storage or network listeners ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **NFR-TEC-004 (Developer Velocity)**: Leverage modern package management, fast linting, and strict type checking to support robust solo-developer iteration.

---

## 5. Constraints

1. **No Semantic Redefinition**: Technology choices must not introduce new lifecycle states (e.g., workflow `TIMED_OUT`, attempt `CANCELLING`, task `BLOCKED`, worker `UNHEALTHY`), per-attempt leases, rate limits, quotas, concurrency caps, or expression languages.
2. **No Distributed 2PC**: Consistency groups must commit locally within the authoritative persistence boundary; cross-system distributed transactions are prohibited.
3. **No Process-Local Locks as Correctness Authority**: Thread locks or in-process mutexes must not act as the authoritative single-winner concurrency mechanism ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
4. **No Arbitrary Deserialization**: Python `pickle` or arbitrary binary serialization across network boundaries is explicitly prohibited ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)).
5. **No Database Access by Workers**: Workers must not possess database credentials or communicate directly with authoritative orchestration storage ([ADR-019](adr-019-project-and-service-boundaries.md)).
6. **No Content-Addressed Definition Identity**: `DefinitionId` must remain an opaque identifier, not a content-derived hash ([ADR-001](adr-001-internal-workflow-specification.md)).

---

## 6. Goals

* Select concrete, production-grade technologies for the control plane, persistence, worker, and observability.
* Enforce strict architectural separation between transport DTOs, persistence tables, and pure domain entities.
* Establish concrete implementations for OCC revision fencing, atomic consistency groups, and unique attempt ordinal allocation.
* Define a lightweight, inspectable, firewall-friendly HTTP/JSON pull worker protocol.
* Establish a minimal, robust reference deployment stack using Docker Compose with zero mandatory cloud services.
* Provide an explicit failure matrix mapping technology-layer faults to approved architectural error semantics.

---

## 7. Non-Goals

* **Benchmarking Claims**: Generating hypothetical latency figures or throughput benchmarks before physical implementation.
* **Detailed Test Harnesses & Fixture Architectures**: Deferred to [ADR-021](00-architecture-decision-register.md).
* **Security & Authentication Policies**: Concrete mTLS, JWT, and secret management mechanisms are deferred to [ADR-022](00-architecture-decision-register.md).
* **Numeric Capacity Limits & Buffer Sizes**: Exact connection pool limits, timeouts, and queue capacities are deferred to [ADR-023](00-architecture-decision-register.md).
* **Workflow Versioning Algorithms**: Definition migration and version routing are deferred to [ADR-024](00-architecture-decision-register.md).
* **Clustered Multi-Instance HA Coordination**: Raft consensus, leader election, and distributed scheduling are deferred to [ADR-025](00-architecture-decision-register.md).
* **Multi-Language Client SDK Architectures**: Code generation and language-specific client libraries are deferred to [ADR-026](00-architecture-decision-register.md).

---

## 8. Candidate Solutions

### 8.1 Control Plane Programming Language
* **Candidate L1 (Python 3.12 - SELECTED)**: Clean async I/O via `asyncio`, mature PostgreSQL driver (`asyncpg`), rich testing ecosystem (`pytest`), first-class OpenTelemetry instrumentation, high developer velocity for a solo engineer. CPU-heavy user tasks are physically offloaded to external workers ([ADR-019](adr-019-project-and-service-boundaries.md)), keeping the control-plane runtime focused on orchestration I/O.
* **Candidate L2 (Go)**: Strong native concurrency, low memory footprint. However, writing expressive domain models, AST manipulations, and generic DAG traversal requires boilerplate; ORM and migration ecosystem is less fluid for rapid architectural evolution.
* **Candidate L3 (Rust)**: High memory safety and performance. However, steep borrow-checker friction for complex state machines and cyclic graph representations, substantially slower development velocity for a solo developer in V1.
* **Candidate L4 (Java)**: Mature enterprise concurrency and persistence. However, heavier runtime footprint and more verbose domain boilerplate for a solo developer compared to Python.

### 8.2 Authoritative Persistence Store
* **Candidate P1 (PostgreSQL 16 - SELECTED)**: Robust ACID transactions, rich JSONB support, row-level conditional updates, relational constraints, recovery queries, and operational familiarity.
* **Candidate P2 (MySQL 8)**: Robust relational store, but historically weaker JSON functional indexing and less flexible conditional update semantics in complex workflows.
* **Candidate P3 (SQLite)**: Excellent for single-threaded file-backed operations, but database-level write locking causes high contention under concurrent worker callbacks and background scheduling loops; inadequate for a realistic multi-worker distributed system.
* **Candidate P4 (MongoDB)**: Document model maps to JSON, but cross-entity atomic multi-document transactions across workflow, task, attempt, and history collections map less naturally to relational consistency groups.

### 8.3 Ephemeral Caching & Coordination Infrastructure
* **Candidate C1 (No Redis / In-Process Memory - SELECTED)**: Ephemeral worker sessions and in-process wake-up signals reside in control-plane process memory. Reconstructible upon restart; eliminates an extra stateful dependency.
* **Candidate C2 (Redis Hybrid)**: Redis stores worker registry, cached definitions, and candidate task queues. Requires dual-state synchronization, cache invalidation, and extra operational overhead without a demonstrated V1 requirement.

### 8.4 Internal Message Broker
* **Candidate M1 (No Internal Broker - SELECTED)**: Core orchestration progression is transition-driven. The authoritative queue of work is the set of `task_executions` in state `RUNNABLE` in PostgreSQL. Eliminates cross-system delivery coordination (transactional outboxes, 2PC).
* **Candidate M2 (Apache Kafka / RabbitMQ)**: Introduces message queues between scheduling and dispatch. Requires cross-system atomicity coordination between database commits and message publication; unnecessary for V1.

### 8.5 Worker Communication Protocol
* **Candidate W1 (HTTP/JSON Pull / Long-Polling - SELECTED)**: Standard REST-oriented endpoints where workers pull work, heartbeat, and post results. Firewall/NAT friendly, curl/Postman inspectable, language-neutral, simple to implement.
* **Candidate W2 (gRPC / Protobuf Streaming)**: Compact binary format, bidirectional streaming. However, requires Protobuf compilation tooling, complex streaming error recovery, and adds debugging friction for local inspection.
* **Candidate W3 (WebSockets)**: Persistent bidirectional connection. Requires custom heartbeat framing, reconnection/backoff management, and stateful socket handling in ASGI servers.

---

## 9. Detailed Evaluation

| Subsystem Area | Candidate Options | Selected Technology | Architectural Rationale | Key Tradeoff | Governing ADRs |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Control Plane Runtime** | Python, Go, Rust, Java | **Python 3.12 (`asyncio`)** | Rapid iteration, rich DB/HTTP/OTel ecosystem, offloaded task compute | Lower raw single-thread CPU performance | ADR-019 |
| **Public API** | FastAPI, Flask, Django, Litestar | **FastAPI + Uvicorn** | Native async, OpenAPI 3.1 generation, Pydantic validation | Web framework coupling avoided via ports | ADR-015, ADR-019 |
| **Domain Models** | Pydantic, attrs, Dataclasses | **Plain Python Dataclasses** | Zero external dependencies, pure business invariants, fast | Manual mapping to persistence DTOs | ADR-001, ADR-019 |
| **YAML Parser** | PyYAML, ruamel.yaml, strictyaml | **`ruamel.yaml`** | YAML 1.2 compliance, duplicate key rejection, source location AST | Slower parsing speed than C-based PyYAML | ADR-002 |
| **Graph Algorithms** | NetworkX, Custom Sparse DAG | **Custom Sparse DAG Module** | $\mathcal{O}(V+E)$ bidirectional adjacency lists, Kahn/DFS algorithms | Hand-crafted graph traversal logic | ADR-003, ADR-004 |
| **Authoritative Persistence** | PostgreSQL, MySQL, SQLite, Mongo | **PostgreSQL 16** | Proven ACID transactions, JSONB, row OCC, wide support | Requires relational DB instance | ADR-011, ADR-013 |
| **Data Access Layer** | SQLAlchemy ORM, Core, Raw SQL | **SQLAlchemy 2.0 Async (Hybrid)** | Declarative schema mapping + explicit Core OCC queries | Learning curve of modern SQLAlchemy async | ADR-011, ADR-013 |
| **Database Driver** | asyncpg, psycopg 3, sync drivers | **`asyncpg`** | Mature async PostgreSQL support, SQLAlchemy integration | Protocol-specific edge cases | ADR-011 |
| **Database Migrations** | Alembic, Flyway, manual SQL | **Alembic** | Native SQLAlchemy migration integration, version-controlled | Requires disciplined migration workflow | ADR-011 |
| **Concurrency Control** | Redis Locks, DB Locks, OCC | **Integer Revision OCC** | Single-winner guarantee without distributed locks, zero overhead | Client retry needed on conflict | ADR-013 |
| **Transaction Isolation** | Serializable, Repeatable, Committed | **`READ COMMITTED` + OCC** | Prevents dirty reads with low lock contention; OCC ensures safety | Must write explicit conditional predicates | ADR-013 |
| **History Storage** | Cassandra, Mongo, PostgreSQL | **PostgreSQL `history_entries`** | Commits atomically with state in same transaction, append-only | Table growth requires future partitioning | ADR-014 |
| **Ephemeral Cache** | Redis, Memcached, None | **REJECTED (In-Process Memory)** | Control plane is single process in V1; eliminates failure domain | Ephemeral state lost on restart (reconstructible) | ADR-008, ADR-019 |
| **Message Broker** | Kafka, RabbitMQ, None | **REJECTED (No Broker in V1)** | Eliminates cross-system delivery coordination; DB is work truth | In-process trigger + rediscovery needed | ADR-005, ADR-019 |
| **Worker Protocol** | gRPC, WebSocket, HTTP/JSON | **HTTP/JSON Pull / Long-Polling** | Firewall/NAT friendly, inspectable, simple, multi-language | Less efficient wire format than Protobuf | ADR-008, ADR-009 |
| **Worker Execution** | Thread, Subprocess, Container | **Worker In-Process ThreadPool** | Control plane already isolated; minimal execution overhead | Fault in task code can crash worker process | ADR-008, ADR-019 |
| **Activity Registry** | Config files, Decorator | **`@activity` Decorator** | Idiomatic developer experience, clean string activity mapping | Requires explicit registration at startup | ADR-009 |
| **Metrics** | StatsD, Prometheus Client | **`prometheus-client` via `/metrics`** | Standard scrape model, low cardinality, Prometheus ecosystem | Push metrics require pushgateway (avoided) | ADR-016 |
| **Tracing** | Custom, OpenTelemetry | **OpenTelemetry SDK + Jaeger** | Vendor-neutral standard, rich automated instrumentation | Span creation overhead in hot paths | ADR-016 |
| **Packaging** | Bare metal, Docker, K8s | **Docker + Docker Compose** | Reproducible multi-container stack on any local machine | Docker daemon requirement on host | ADR-019 |
| **Package Manager** | Poetry, pip-tools, uv | **`uv`** | Fast dependency resolution, lockfiles, workspace support | Newer tool in ecosystem | ADR-019 |
| **Code Tooling** | Black/Flake8/isort/mypy | **Ruff + Pyright** | Unified formatting/linting, strict type checking | Minor syntax differences from mypy | ADR-019 |

---

## 10. Decision

### 10.1 Central Decision Statement
> **NexusFlow V1 selects Python 3.12 with FastAPI and Uvicorn for the control-plane runtime; PostgreSQL 16 with SQLAlchemy 2.0 Async and `asyncpg` for authoritative persistence under `READ COMMITTED` with explicit OCC revision guards and semantic consistency-group transactions; a custom sparse bidirectional graph representation; an HTTP/JSON pull/long-polling worker protocol; in-process ephemeral WorkerSession management; UUIDv4 for opaque identifiers; no Redis or internal message broker; and OpenTelemetry tracing, Prometheus metrics, Grafana, and Jaeger in a Docker Compose reference environment.**

### 10.2 Control Plane Runtime & Frameworks
* **Runtime**: **Python 3.12** using standard `asyncio` for multiplexed network and persistence I/O.
* **Public Ingress**: **FastAPI** running on **Uvicorn**. FastAPI routers act strictly as ingress adapters; they validate external requests and invoke Application Use Cases, never executing domain logic or calling persistence ports directly ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **DTO & Configuration Validation**: **Pydantic v2** and **`pydantic-settings`** are used strictly at boundary layers (HTTP requests/responses, worker protocol DTOs, environment configuration). Pydantic models are boundary representations, **not internal domain entities**.
* **Domain Model Representation**: Pure domain entities (`WorkflowExecution`, `TaskExecution`, `ExecutionAttempt`, `ValidatedIWS`) are implemented as standard Python `@dataclass(slots=True)` and `Enum` types, with zero dependencies on web frameworks, ORMs, or drivers.

### 10.3 Workflow Parsing & Graph Algorithms
* **YAML Ingestion**: **`ruamel.yaml`** parses external workflow definitions in V1 ([ADR-002](adr-002-workflow-definition-parsing-strategy.md)), strictly enforcing duplicate-key rejection and preserving token source locations for diagnostics. External authoring in V1 remains YAML only.
* **Canonical Graph Representation**: A custom, lightweight directed graph module implementing a **sparse bidirectional adjacency representation** ([ADR-003](adr-003-canonical-workflow-graph-representation.md)) using adjacency maps/lists:
  * Outgoing adjacency map: task $\to$ set of downstream dependent tasks.
  * Incoming adjacency map: task $\to$ set of upstream dependency tasks.
  * Space and traversal complexity: strictly $\mathcal{O}(V + E)$.
  * Validation: In-memory cycle detection and topological sorting via Kahn's algorithm and DFS ([ADR-004](adr-004-workflow-validation-strategy.md)).
  * Adjacency matrices and dense $V \times V$ representations are excluded; NetworkX is not required.
  * The graph is derived and reconstructible in memory from the `ValidatedIWS`.

### 10.4 Authoritative Persistence & Data Access
* **Storage Engine**: **PostgreSQL 16**. Authoritative source of truth for workflow definitions, execution state machines, task lifecycles, execution history, and API idempotency records.
* **Data Access Strategy**: **SQLAlchemy 2.0 Async (Hybrid Approach)** with **`asyncpg`**:
  * Declarative mapped models represent the relational database schema.
  * Multi-entity consistency groups execute inside explicit transaction blocks (`async with session.begin():`).
  * State transitions and OCC mutations execute via explicit conditional Core update queries (`update(TaskExecutionTable).where(...).values(...)`), avoiding reliance on implicit ORM dirty-tracking or hidden cascades ([ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
* **Migrations**: **Alembic** manages version-controlled DDL migrations. Migrations run as an explicit, separate pre-start command—never implicitly during application boot. Startup verifies schema compatibility against expected migration revisions and halts if incompatible.

### 10.5 Isolation & Optimistic Concurrency Control (OCC)
* **Isolation Level**: **`READ COMMITTED` (PostgreSQL Default)**.
* **OCC Revision Pattern**: Tables `workflow_executions`, `task_executions`, and `execution_attempts` include an integer column `revision INT NOT NULL DEFAULT 1`.
* **Guarded Conditional Mutations**:
  ```sql
  UPDATE task_executions
  SET state = :new_state,
      revision = revision + 1,
      updated_at = :now
  WHERE id = :task_id
    AND revision = :expected_revision
    AND state = :expected_state;
  ```
  If affected row count is `0`, the mutation conflicted or was superseded; the transaction rolls back, and the operation handles the conflict via [ADR-013](adr-013-consistency-and-concurrency-strategy.md) reread semantics.
* **Calibrated Safety**: `READ COMMITTED` is acceptable because every NexusFlow consistency group explicitly encodes semantic predicates, OCC revision guards, uniqueness constraints, and multi-entity transaction boundaries. Correctness depends on the complete consistency-group implementation, not the isolation level alone.

### 10.6 Atomic Consistency Groups
PostgreSQL transactions physically realize the multi-entity consistency groups established in [ADR-011](adr-011-state-persistence-strategy.md), [ADR-013](adr-013-consistency-and-concurrency-strategy.md), and [ADR-014](adr-014-execution-history-and-audit-model.md):
1. **Task Input Readiness**: Transition `TaskExecution` `PENDING` $\to$ `RUNNABLE` when dependencies settle.
2. **Attempt Ownership**: Atomically transition `TaskExecution` `RUNNABLE` $\to$ `RUNNING`, create `ExecutionAttempt` in `CLAIMED`, bind `WorkerSessionId`, assign attempt ordinal, and insert `HistoryEntry`.
3. **Execution-Start Observation**: Transition `ExecutionAttempt` `CLAIMED` $\to$ `RUNNING` upon worker acknowledgment.
4. **Task Success + Authoritative Output**: Atomically transition `TaskExecution` and `ExecutionAttempt` to `SUCCEEDED`, commit validated output payload, and insert `HistoryEntry`.
5. **Retry Scheduling**: Atomically transition `ExecutionAttempt` to `FAILED`, transition `TaskExecution` to `RETRY_WAIT`, compute retry delay, and insert `HistoryEntry`.
6. **Definitive Task Failure**: Atomically transition `ExecutionAttempt` and `TaskExecution` to `FAILED`, evaluate workflow failure direction, and insert `HistoryEntry`.
7. **Workflow Direction & Terminalization**: Transition `WorkflowExecution` to terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`), commit workflow output (if succeeded), and insert `HistoryEntry`.
8. **State + History Atomicity**: Every semantic transition commits alongside its corresponding `HistoryEntry` within the same database transaction.

### 10.7 Attempt Ordinal Implementation
* **Allocation Pattern**: Under the ownership consistency group, the application reads the `TaskExecution`, determines the next logical ordinal from authoritative state, verifies the guarded OCC predicate, inserts the new `ExecutionAttempt` with ordinal $N$, and commits.
* **Database Constraint**: `UNIQUE (task_execution_id, attempt_number)` enforces that no two committed attempts for the same task share an ordinal. Monotonic allocation and non-reuse are maintained by the ADR-007/ADR-013 ownership algorithm; the database constraint provides defense-in-depth. `SELECT MAX() + 1` is not used as a concurrency mechanism.

### 10.8 Entity Identifiers (UUIDv4)
* **Selection**: **UUIDv4** for all opaque resource IDs (`DefinitionId`, `WorkflowExecutionId`, `TaskExecutionId`, `AttemptId`, `HistoryEntryId`, `WorkerSessionId`).
* **Properties**: Native zero-dependency generation via Python stdlib (`uuid.uuid4()`). Identifiers are opaque reference tokens, **not security boundaries and not lifecycle ordering mechanisms** ([ADR-022](00-architecture-decision-register.md)). `DefinitionId` is an opaque UUID; human-readable workflow names remain separate metadata. Content-addressed definition identity is not used.

### 10.9 Definition, Payload, Failure, and History Storage
* **Definition Storage**: Stored in PostgreSQL `definitions` table with `validated_iws JSONB NOT NULL`. The Canonical Graph is derived in memory. `raw_yaml TEXT` may optionally be retained for diagnostics/provenance; it is non-authoritative and never reparsed during recovery.
* **Payload Storage**: Stored as PostgreSQL JSONB. Large binaries remain external URI references ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)).
  * *Committed vs. Missing Output Distinction*: The physical schema distinguishes between an uncommitted output and an intentional, committed JSON `null` (enforced via state-derived invariant where `output_payload` is valid only in `SUCCEEDED`, or an explicit `output_committed BOOLEAN`). `TaskExecution` and `WorkflowExecution` in `SUCCEEDED` are never observable without committed output.
* **Failure Metadata**: Durable failure fields store only bounded normalized semantic cause information per [ADR-018](adr-018-error-handling-philosophy.md). Raw stack traces, unbounded worker logs, and secrets are excluded from authoritative persistence.
* **History Storage**: Stored in PostgreSQL `history_entries` (append-only). In accordance with [ADR-014](adr-014-execution-history-and-audit-model.md), **no per-workflow sequence counter is introduced**. Ordering and causality are established through lifecycle transitions, revisions, attempt ordinals, timestamps, and semantic relationships. Query pagination uses `(timestamp, id)` as a stable retrieval key; this is a physical pagination mechanism, **not authoritative semantic event order**.
* **Idempotency Storage**: Stored in PostgreSQL `idempotency_records` table (`scope`, `token_hash`, `request_fingerprint`, `resource_id`). The fingerprint is derived from normalized request fields and is scoped solely to API idempotency comparison; it does not define definition identity. Duplicate requests return the original logical resource identity and status per [ADR-015](adr-015-external-api-architecture.md).

### 10.10 Coordination & Scheduling Infrastructure (No Redis / No Broker)
* **Redis Rejected for V1**: No demonstrated V1 requirement justifies an additional stateful dependency, cache invalidation surface, and recovery complexity. Reconstructible worker sessions reside in process memory; PostgreSQL provides authoritative persistence.
* **Internal Message Broker Rejected for V1**: Core orchestration is transition-driven. The authoritative queue of work is the set of `task_executions` in state `RUNNABLE` in PostgreSQL. Eliminates cross-system delivery coordination (transactional outboxes, 2PC).
* **Scheduler Wakeup & Bounded Rediscovery**:
  * In-process `asyncio.Event` wake-up provides responsiveness.
  * Startup reconciliation ([ADR-012](adr-012-recovery-strategy.md)) rediscovers durable `RUNNABLE` work after restart.
  * Targeted/defensive runtime reconciliation provides a bounded rediscovery path if an event is missed, preventing permanently stranded runnable work without requiring mandatory periodic full-table scans.
* **Timers & Deadlines**: Durable deadlines (`RETRY_WAIT` wake-up, `CLAIMED` start timeout, execution timeouts, cancellation-resolution deadlines) are persisted as `TIMESTAMPTZ` in PostgreSQL. In-process scheduling uses an async priority queue/heap. On restart, active deadlines are reconstructed from PostgreSQL ([ADR-012](adr-012-recovery-strategy.md)). Worker session heartbeat observations remain ephemeral in process memory.
* **Clock Port**: Abstract `Clock` interface provides timezone-aware UTC for durable deadlines and `time.monotonic()` for relative elapsed durations within a process lifetime. Worker wall clocks are never authoritative ([ADR-008](adr-008-worker-coordination-and-liveness-model.md)). Monotonic time does not survive restarts.

### 10.11 Worker Protocol & Execution Model
* **Protocol Transport**: **HTTP/JSON Pull / Long-Polling Worker Protocol**. Independent from public client API contracts. Exact URI paths belong to LLD.
* **Two-Phase Candidate / Ownership Semantics**:
  * *Phase 1 (Candidate / Offer)*: Worker polls with `WorkerSessionId`. Control plane verifies worker is live, `accepting_new_work == true`, exact canonical Activity Type match, Task is `RUNNABLE`. Candidate offer is ephemeral; no Attempt created; no retry budget consumed.
  * *Phase 2 (Ownership Commit)*: Immediately before dispatch, eligibility is revalidated (Task `RUNNABLE`, Workflow `RUNNING`, no active Attempt, session live, `accepting_new_work`, exact Activity Type match). Atomic commit transitions Task `RUNNABLE` $\to$ `RUNNING`, creates Attempt in `CLAIMED`, binds `WorkerSessionId`, commits ordinal and history. Only after commit does the worker receive authoritative Attempt execution context.
* **Cancellation Delivery**: Delivered via active long-poll responses, heartbeat responses, or dedicated polling operations. Correlated by `(AttemptId, WorkerSessionId)`; best-effort; bounded by cancellation-resolution deadline; does not prove remote physical execution halted.
* **Worker Runtime Language**: Default V1 worker in **Python 3.12**. Non-Python workers can implement the HTTP/JSON protocol independently ([ADR-026](00-architecture-decision-register.md)).
* **Worker Execution Model**: **Worker-local thread-pool execution for synchronous activity handlers, with native async support for async handlers**.
  * Worker process boundary isolates task execution from control-plane process memory.
  * Thread-pool execution is materially simpler for V1 and avoids per-attempt process/container machinery.
  * Threads do not provide hard task isolation; a task handler can crash its worker process.
  * Python threads do not parallelize CPU-bound bytecode due to the GIL.
  * Python cannot safely force-kill a running thread; cancellation is cooperative. If a thread ignores cancellation, the control plane logically settles under ADR-006/007/008, and any delayed result callback is fenced by Attempt authority checks ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
* **Activity Registration**: Python decorator `@activity("canonical_name")`. Canonical string is the routing identity; routing does not use class names, import paths, queues, tags, or affinity.
* **Serialization**: Strict JSON-compatible values. Arbitrary Python `pickle` is **explicitly prohibited**.

### 10.12 Observability, Packaging & Developer Tooling
* **Logging**: Standard library `logging` with structured JSON formatter outputting to `stdout`. Applicable correlation IDs (`workflow_execution_id`, `task_execution_id`, `attempt_id`, `worker_session_id`, `trace_id`) are included when available. Logs remain best-effort diagnostics.
* **Tracing**: **OpenTelemetry Python SDK** exporting via standard OTLP to **Jaeger**. Tracing is strictly fail-open.
* **Metrics**: **`prometheus-client`** exposing `/metrics` scraped by **Prometheus**. Metrics use low-cardinality labels; `activity_type` is permitted only when cardinality is bounded by configuration ([ADR-016](adr-016-observability-architecture.md), [ADR-023](00-architecture-decision-register.md)). Execution/attempt IDs are strictly excluded.
* **Visualization**: **Grafana** visualizes Prometheus metrics. **Jaeger UI** displays traces. OpenTelemetry Collector is omitted from V1 reference stack to minimize operational complexity.
* **Packaging**: **Docker & Docker Compose v2**. Reference services: `control-plane`, `worker`, `postgres`, `prometheus`, `grafana`, `jaeger`. Kubernetes is deferred (V1 does not require cluster orchestration or HA control planes).
* **Developer Tooling**: Managed via **`uv`** workspace. **Ruff** for formatting/linting, **Pyright** for static type checking, **`pytest` + `pytest-asyncio`** for testing, and **GitHub Actions** for CI.

### 10.13 Project Organization & Lifecycle Management
* **Monorepo Layout**: Logical boundaries for Core Domain, Control Plane, Worker Runtime, and Protocol Contracts. Worker runtime depends on protocol contracts and **never imports control-plane domain models, persistence adapters, or database drivers**.
* **Startup Sequence**: Load configuration $\to$ initialize persistence $\to$ verify schema compatibility $\to$ initialize ports $\to$ execute mandatory [ADR-012](adr-012-recovery-strategy.md) startup reconciliation $\to$ start invariant-critical background tasks $\to$ signal readiness. Worker presence is not required for readiness (no worker is a waiting condition, not unreadiness).
* **Background Supervision**: Invariant-critical background tasks (scheduling trigger processor, worker liveness checker, deadline evaluator) use fail-fast process termination and container restart on unhandled failure, invoking [ADR-012](adr-012-recovery-strategy.md) recovery. Best-effort diagnostic tasks (telemetry) fail-open without process termination.
* **Shutdown Mapping**: Mapped to FastAPI ASGI lifespan context. Termination signal triggers readiness withdrawal, enters ephemeral `DRAINING` state (not a domain state), stops new ownership claims, allows bounded settlement for in-flight operations, and closes database connections cleanly ([ADR-017](adr-017-graceful-shutdown-architecture.md)). Active workflows are not cancelled merely because the process shuts down.

---

## 11. Decision Rationale

1. **Alignment with Architectural Semantics**: Python 3.12, FastAPI, and SQLAlchemy 2.0 Async allow clean expression of domain ports and application use cases without leaky framework abstractions. Core state machines remain pure dataclasses, completely isolated from transport and database drivers.
2. **ACID Transactional Guarantees**: PostgreSQL 16 provides the exact primitives needed for NexusFlow's consistency groups: single-connection atomic transactions, integer revision conditional updates for OCC, relational foreign keys, and JSONB storage for bounded data-flow payloads.
3. **Elimination of Cross-System Coordination**: Rejecting Redis and internal message brokers in V1 eliminates the dual-write problem. PostgreSQL `RUNNABLE` task state acts as the single source of truth, avoiding distributed 2PC or outbox relays.
4. **Developer Velocity & Solo Maintainability**: Using Python across the control plane and default worker runtime, unified package management via `uv`, fast linting via Ruff, and strict type-checking via Pyright enables rapid iteration with minimal context switching.
5. **Inspectable, Firewall-Friendly Worker Protocol**: An HTTP/JSON pull/long-polling worker protocol allows trivial inspection via curl/Postman, traverses corporate firewalls and NATs effortlessly, and establishes an open standard for future multi-language workers ([ADR-026](00-architecture-decision-register.md)).

---

## 12. Tradeoffs

### 12.1 Benefits
* **Zero Distributed Consistency Hazards**: Consolidating authoritative state in PostgreSQL eliminates distributed locking and broker-database coordination.
* **High Development Velocity**: Modern Python tooling (`uv`, Ruff, Pyright, FastAPI) enables rapid development, debugging, and testing.
* **Complete Local Reproducibility**: The entire 6-container stack runs locally on a laptop via Docker Compose without internet access or paid services.
* **Clear Decoupling**: Ports and Adapters architecture ensures business logic can be tested in-memory in microseconds.

### 12.2 Costs & Mitigations
* **Single-Instance Control Plane Scaling**: All control-plane modules run in a single process in V1.  
  * *Mitigation*: Heavy task computation is offloaded to external workers; orchestration metadata overhead is compact. High Availability clustering is deferred to [ADR-025](00-architecture-decision-register.md).
* **Shared Database Workload**: Authoritative state mutations, definition reads, history writes, and recovery queries share one PostgreSQL instance.  
  * *Mitigation*: Proper B-tree indexing on foreign keys and compound lookup keys ensures efficient query paths. Read-side history queries can be projected to a replica in future versions ([ADR-019](adr-019-project-and-service-boundaries.md)).
* **HTTP Long-Poll Serialization Overhead**: JSON serialization is less compact than Protobuf.  
  * *Mitigation*: Control-plane metadata payloads are bounded ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)); HTTP long-polling provides superior debuggability and interoperability for V1.
* **Cooperative Thread Cancellation in Worker**: Python cannot safely force-kill worker threads.  
  * *Mitigation*: Best-effort cancellation is bounded by cancellation deadlines; late-arriving callbacks are fenced by Attempt authority guards in the control plane ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)).

---

## 13. Consequences

### 13.1 Positive Consequences
* Absolute semantic fidelity with ADR-001 through ADR-019.
* Zero distributed 2PC or distributed lock managers required.
* Highly maintainable, observable, and testable codebase for a solo developer.
* Clean separation between control plane and workers enables independent horizontal worker scaling.
* Standardized technology foundation established for downstream testing ([ADR-021](00-architecture-decision-register.md)), security ([ADR-022](00-architecture-decision-register.md)), and configuration ([ADR-023](00-architecture-decision-register.md)).

### 13.2 Negative Consequences
* High-throughput history audit queries share database resources with critical state transitions in V1.
* CPU-bound user tasks written in Python share the worker process GIL unless configured with separate worker processes.

---

## 14. Failure Modes & Boundary Behavior

| # | Failure Scenario | Technology Layer | Authoritative Impact | Semantic Handling & Recovery | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | **PostgreSQL Unavailable** | `asyncpg` pool error | Operations fail closed | Raises normalized `StorageSystemUnavailableError`; in-flight operations aborted; recovery reconciles once restored. | ADR-011, ADR-018 |
| **2** | **PostgreSQL Commit Outcome Unknown** | Connection dropped during commit | Outcome ambiguous | Application treats outcome as unknown; relies on [ADR-013](adr-013-consistency-and-concurrency-strategy.md) authoritative reread before retrying mutation. | ADR-013 |
| **3** | **OCC Update Affects 0 Rows** | SQLAlchemy conditional `UPDATE` | Mutation rejected | Transaction rolls back; handling depends on origin (reread, benign lost race, bounded retry, idempotent no-op, or public conflict). | ADR-013, ADR-018 |
| **4** | **Unique Constraint Conflict on Attempt Ordinal** | PostgreSQL `UNIQUE(task_execution_id, attempt_number)` | Attempt insert rejected | Prevents duplicate ordinals as defense-in-depth; triggers [ADR-013](adr-013-consistency-and-concurrency-strategy.md) reread to handle outcome. | ADR-007, ADR-013 |
| **5** | **Migration / Schema Incompatible** | Startup Alembic revision check | Startup aborted | Persistence adapter halts process initialization before readiness endpoint opens; raises typed configuration error. | ADR-018 |
| **6** | **FastAPI / Uvicorn Process Crashes** | Python OS Process exit | Ephemeral state lost; durable state intact | Docker restarts container; [ADR-012](adr-012-recovery-strategy.md) startup reconciliation restores active workflows and in-flight attempts from PostgreSQL. | ADR-012, ADR-017 |
| **7** | **Scheduler Asyncio Task Crashes** | Supervised `asyncio.Task` exception | Scheduling loop halted | Background task supervisor logs fatal error and terminates process to trigger container restart and clean startup recovery. | ADR-012, ADR-017 |
| **8** | **Worker Disconnects Pre-Ownership** | HTTP long-poll connection drops | No attempt created | Revalidation prevents ownership; Task remains in state `RUNNABLE`; no retry budget or ordinal consumed. | ADR-007, ADR-009 |
| **9** | **Worker Process Crashes During Task** | Remote OS Process termination | In-flight attempt active | Heartbeat misses exceed threshold; [ADR-008](adr-008-worker-coordination-and-liveness-model.md) marks `WorkerSession` lost; Attempt settles `FAILED` only if worker-loss determination wins under [ADR-013](adr-013-consistency-and-concurrency-strategy.md). | ADR-008, ADR-013 |
| **10** | **Worker Result Duplicated** | HTTP `/complete` network retransmission | Duplicate callback received | Worker Coordination verifies `(AttemptId, WorkerSessionId)` and lifecycle revision. Identical duplicate terminal submission is idempotent; conflicting duplicate rejected. | ADR-008, ADR-013 |
| **11** | **Worker Result Stale / Superseded** | Delayed HTTP callback post-timeout | Outdated callback received | Callback must match authoritative `(AttemptId, WorkerSessionId)` and be valid for current lifecycle state; stale callback rejected without modifying durable state. | ADR-008, ADR-013 |
| **12** | **Worker Protocol Times Out Before Ownership** | HTTP long-poll timeout / disconnect | Dispatch offer unacknowledged | Ownership commit aborted; task remains `RUNNABLE`; no attempt recorded. | ADR-007, ADR-009 |
| **13** | **Worker Protocol Fails After Ownership** | HTTP dispatch transmission drop | Attempt in `CLAIMED` state | `CLAIMED` start timeout timer expires; attempt settles `FAILED`; task schedules retry only if retry budget remains and workflow is `RUNNING` ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)). | ADR-007, ADR-008 |
| **14** | **In-Memory Worker Registry Lost** | Control plane process restart | Registry cleared | Surviving workers reconnect using their **existing `WorkerSessionId`**; restarted workers establish a new ID; control plane reconciles active sessions against durable attempts. | ADR-008, ADR-012 |
| **15** | **Control Plane Restarts** | Container restart | Ephemeral state reset | Executes [ADR-012](adr-012-recovery-strategy.md) startup reconciliation against PostgreSQL; active executions resumed deterministically. | ADR-012 |
| **16** | **Telemetry Exporter Unavailable** | OpenTelemetry OTLP drop | Spans dropped/buffered | Telemetry is fail-open; telemetry dropped, sampled, or buffered according to policy; core state transactions commit unimpeded. | ADR-016 |
| **17** | **Prometheus Unavailable** | Scrape failures on `/metrics` | Metric collection stalled | `/metrics` endpoint continues serving in-memory counters; control plane operates normally without degradation. | ADR-016 |
| **18** | **Grafana Unavailable** | Container offline | Dashboards inaccessible | Control plane and worker runtimes operate with zero correctness or throughput impact. | ADR-016 |
| **19** | **Jaeger / Trace Backend Unavailable** | OTLP exporter connection error | Traces uncollected | OpenTelemetry exporter drops spans fail-open; core engine unaffected. | ADR-016 |
| **20** | **Docker Container Restarts** | Container engine restart | Process killed | Standard startup reconciliation restores state; PostgreSQL volume preserves all durable truth. | ADR-012 |
| **21** | **Malformed YAML Workflow Submission** | `ruamel.yaml` parser exception | Definition intake rejected | Parser captures syntax error with line/column coordinates; returns HTTP 400 Bad Request; no DB mutation. | ADR-002, ADR-015 |
| **22** | **Malformed Worker Protocol Result** | Protocol / Pydantic validation error | Transport result rejected | Callback rejected; Task `SUCCEEDED` does NOT commit; Attempt does NOT automatically become `FAILED`; existing attempt lifecycle/timeout governs attempt. | ADR-010, ADR-018 |
| **23** | **Idempotency Storage Conflict** | PostgreSQL `UNIQUE(scope, token_hash)` | Duplicate request detected | First request commits; second request queries existing `resource_id` and returns original resource identity per [ADR-015](adr-015-external-api-architecture.md). | ADR-015 |
| **24** | **Durable Deadline Expires During Restart** | PostgreSQL `TIMESTAMPTZ` past current time | Timer expired during downtime | Startup reconciliation queries expired timers (`deadline <= now()`) and triggers appropriate expiration transitions. | ADR-007, ADR-012 |
| **25** | **Clock Drift Across Restart** | System UTC clock step | Deadline evaluation affected | Monotonic clock is valid only within one process lifetime; durable deadlines use UTC; startup reconciliation reconciles expired timers against authoritative control-plane UTC. Worker clocks ignored. | ADR-008, ADR-012 |
| **26** | **Optional Internal Trigger Lost** | `asyncio.Event` dropped on restart or error | Notification missed | Durable state in PostgreSQL remains authoritative; bounded durable rediscovery and startup reconciliation ensure runnable work is scheduled without permanent stalling. | ADR-005, ADR-012, ADR-019 |

---

## 15. Debugging

* **Transparent SQL & OCC Logging**: Explicit SQLAlchemy Core queries make generated SQL and conditional `WHERE` clauses visible in debug logs, eliminating ORM magic during debugging.
* **Correlated Diagnostic Logging**: Structured JSON logs written to `stdout` incorporate `workflow_execution_id`, `task_execution_id`, `attempt_id`, `worker_session_id`, and `trace_id` when available, enabling end-to-end trace correlation.
* **Direct Curl / Postman Worker Protocol Inspection**: Because the worker protocol uses standard HTTP/JSON pull operations, developers can inspect, mock, and debug worker registration, polling, and callback flows using standard HTTP tools.
* **Local Visual Tracing & Metrics**: Developers can inspect execution spans in Jaeger and visualize real-time scheduler metrics in Grafana locally via Docker Compose.

---

## 16. Testing Strategy

* **Pure Domain Unit Testing**: Core state machines (`WorkflowExecution`, `TaskExecution`, `ExecutionAttempt`), DAG validation algorithms, and routing logic are implemented as pure Python dataclasses and functions, testable in-memory with `pytest` in microseconds without external I/O.
* **Port Mocking**: Application Use Cases are tested against mock/fake persistence and clock ports, verifying use-case coordination independently of real databases.
* **Asynchronous Database Integration Testing**: Integration tests run against PostgreSQL instances using `pytest-asyncio` and Alembic migrations, verifying multi-entity consistency groups and OCC conditional update behaviors.
* **Worker Protocol Contract Testing**: FastAPI `TestClient` / `httpx.AsyncClient` enables full in-process integration testing of worker session handshakes, two-phase task dispatch, and result reporting.
* **Detailed Testing Architecture**: Comprehensive test suites, fixture structures, and coverage policies are formally established in [ADR-021](00-architecture-decision-register.md).

---

## 17. Operational Considerations

* **Local Reference Deployment**: The complete V1 environment deploys via Docker Compose:
  * `nexusflow-control-plane` (FastAPI + Uvicorn)
  * `nexusflow-worker-1` (Python Worker Runtime)
  * `postgres` (PostgreSQL 16)
  * `prometheus` (Metrics scraper)
  * `grafana` (Visualization)
  * `jaeger` (Tracing)
* **Authoritative State Scoping**: All authoritative orchestration state resides in PostgreSQL in the V1 reference deployment; reconstructible runtime state (worker session registry, in-process triggers, timer heap) remains ephemeral.
* **Configuration Delivery**: Managed via `pydantic-settings`. Settings are validated at boot; domain logic receives configuration through explicit constructor injection, never reading environment variables directly.
* **Health Monitoring**: Standard HTTP endpoints for container orchestration:
  * `/health/live`: Returns `200 OK` if the process and event loop are responsive.
  * `/health/ready`: Returns `200 OK` only after startup reconciliation is complete, persistence compatibility is verified, and the process is not in `DRAINING` state ([ADR-016](adr-016-observability-architecture.md), [ADR-017](adr-017-graceful-shutdown-architecture.md)). Worker presence is not required for readiness.
* **Graceful Shutdown**: Intercepts `SIGTERM` via ASGI lifespan, enters `DRAINING` state, stops establishing new attempt ownership, allows bounded settlement for active callbacks, and closes database connections cleanly ([ADR-017](adr-017-graceful-shutdown-architecture.md)).

---

## 18. Maintenance & Code Organization

* **Monorepo Workspace**: Managed using Astral's **`uv`**:
  * `packages/nexusflow-core`: Pure domain models, graph algorithms, validation, port interfaces. Zero framework dependencies.
  * `packages/nexusflow-control-plane`: FastAPI API, SQLAlchemy persistence adapters, scheduling, recovery, composition root.
  * `packages/nexusflow-worker`: Standalone worker runtime, activity decorators, HTTP long-poll client.
  * `packages/nexusflow-protocol`: Shared Pydantic DTOs and OpenAPI specifications.
* **Dependency Boundary Enforcement**: Clear package boundaries reduce accidental dependency leakage and are reinforced through Pyright, Ruff, and architectural tests. The worker runtime depends on protocol contracts and **never imports control-plane domain models, persistence adapters, or database drivers**.
* **Database Maintenance**: Schema evolution is strictly managed through Alembic migrations.

---

## 19. Future Evolution

* **High Availability & Clustering ([ADR-025](00-architecture-decision-register.md))**: The technology choices avoid obvious blockers to future HA: PostgreSQL is already the sole authoritative store, and OCC prevents lost updates across multiple writers. ADR-025 will define multi-instance authority, leader election, and distributed worker visibility.
* **Multi-Language Worker SDKs ([ADR-026](00-architecture-decision-register.md))**: The HTTP/JSON worker protocol is language-neutral. Workers in Go, TypeScript, Java, or Rust can participate by implementing the HTTP protocol without depending on Python runtimes.
* **Read-Side Query Projections ([ADR-019](adr-019-project-and-service-boundaries.md))**: Read-heavy history audit queries can be offloaded to read replicas or specialized projection stores without altering the core state machine persistence path.
* **Observability Collector**: An OpenTelemetry Collector can be introduced between the control plane and telemetry sinks if centralized sampling, filtering, or routing is required in production deployments.

---

## 20. Rejected Alternatives

1. **Redis for Ephemeral State / Caching**: Rejected for V1. Adding Redis introduces an additional stateful dependency and cache synchronization surface without a demonstrated V1 requirement. In-process memory is sufficient for worker sessions; PostgreSQL relational state serves as the authoritative work queue.
2. **Apache Kafka / RabbitMQ as Internal Broker**: Rejected for V1. Introduces a cross-system delivery coordination problem between database commits and broker publishing that NexusFlow V1 does not need. Core orchestration is transition-driven directly from PostgreSQL.
3. **gRPC for V1 Worker Protocol**: Rejected for V1. While offering high serialization performance, gRPC introduces IDL compilation tooling overhead and friction for manual HTTP debugging. HTTP/JSON pull/long-polling provides complete technology neutrality, simple curl/Postman inspection, and rapid local iteration.
4. **Subprocess / Container per Task on Worker**: Rejected for V1. The worker process boundary already physically separates task execution from the control plane. Running task handlers via a thread pool inside the worker process is materially simpler for V1 and avoids per-attempt process/container isolation machinery.
5. **OpenTelemetry Collector in Reference Stack**: Rejected for V1. Adding an OTel Collector container adds operational complexity without benefit for a local Docker Compose topology. Direct export to Prometheus and Jaeger is clean and self-contained.
6. **Kubernetes for V1**: Rejected. Introduces deployment and operational concepts not required by the V1 single-control-plane reference environment. Docker Compose completely satisfies V1 requirements.
7. **NetworkX for DAG Representation**: Rejected. Bringing in a large external graph package is unnecessary when a custom ~150-line sparse bidirectional adjacency list module provides exact $\mathcal{O}(V+E)$ traversal and zero third-party dependencies ([ADR-003](adr-003-canonical-workflow-graph-representation.md)).
8. **Python `pickle` Protocol**: Rejected. Arbitrary Python object serialization is unsafe and couples workers to Python bytecode, violating language-neutral worker protocol requirements ([ADR-010](adr-010-workflow-data-flow-and-parameter-passing.md)).

---

## 21. Decision Evolution

* **ADR-001 through ADR-018**: Established the abstract semantic architecture: IWS normalization, canonical DAG representation, validation rules, scheduling conditions, state machine lifecycles, worker coordination, routing rules, data flow bindings, persistence strategies, recovery reconciliation, concurrency fencing, audit logging, public API design, observability, graceful shutdown, and error classification.
* **ADR-019**: Established the deployable boundaries: Modular Monolith Control Plane + External Distributed Worker Runtime, single logical persistence boundary, Inverted Dependency (Ports and Adapters) architecture.
* **ADR-020 (This Record)**: Selects the concrete technology stack: Python 3.12, FastAPI, PostgreSQL 16, SQLAlchemy 2.0 Async, `asyncpg`, Alembic, HTTP/JSON pull worker protocol, Docker Compose, and OpenTelemetry.
* **Downstream ADRs**: [ADR-021](00-architecture-decision-register.md) will formalize testing strategy; [ADR-022](00-architecture-decision-register.md) security; [ADR-023](00-architecture-decision-register.md) configuration schemas; [ADR-024](00-architecture-decision-register.md) versioning; [ADR-025](00-architecture-decision-register.md) HA clustering; [ADR-026](00-architecture-decision-register.md) worker SDKs.

---

## 22. Common Misconceptions

1. **"FastAPI models are the core domain entities."**  
   *Correction*: FastAPI uses Pydantic models strictly at the HTTP transport boundary. Core domain models are pure Python dataclasses with zero framework dependencies ([ADR-019](adr-019-project-and-service-boundaries.md)).
2. **"PostgreSQL `READ COMMITTED` isolation is unsafe for distributed concurrency."**  
   *Correction*: Correctness does not depend on database isolation level alone. All state transitions use explicit conditional update queries with OCC revision checks (`WHERE id = :id AND revision = :rev AND state = :state`), ensuring single-winner semantics under `READ COMMITTED` ([ADR-013](adr-013-consistency-and-concurrency-strategy.md)).
3. **"NexusFlow needs Redis or Kafka to act as a task queue."**  
   *Correction*: The set of `task_executions` in state `RUNNABLE` in PostgreSQL is the authoritative work queue. Pre-ownership candidate dispatch is non-authoritative. No external broker is required.
4. **"UUIDv4 identifiers create security access control."**  
   *Correction*: Identifiers are opaque reference tokens, not authorization mechanisms. Access control is formally governed by [ADR-022](00-architecture-decision-register.md).
5. **"Worker threads provide hard task isolation."**  
   *Correction*: Worker-local threads separate execution from the control plane across a network boundary, but threads do not isolate tasks from each other inside the worker. Subprocess/container sandboxing is deferred.
6. **"Execution history is ordered by a global sequence number."**  
   *Correction*: [ADR-014](adr-014-execution-history-and-audit-model.md) explicitly prohibits global sequence counters. History ordering is established through lifecycle transitions, entity revisions, attempt ordinals, and timestamps.

---

## 23. Open Questions (Deferred to LLD / Downstream ADRs)

All primary architectural technology choices are resolved. The following low-level implementation details are intentionally deferred:
* **Exact Alembic Multi-Head Compatibility Algorithm**: Deferred to persistence LLD.
* **Worker Protocol Exact URI Layout & DTO Field Schemas**: Deferred to Worker Protocol Specification and LLD.
* **Telemetry Buffer Sizes & Sampling Ratios**: Deferred to [ADR-023](00-architecture-decision-register.md).
* **Detailed Test Fixtures & Harness Architectures**: Deferred to [ADR-021](00-architecture-decision-register.md).

---

## 24. Interview Discussion & Architectural Defense

* **Why select Python for an orchestration engine instead of Go or Rust?**  
  *Orchestration control planes are I/O-bound metadata managers, not raw compute engines. All CPU-heavy, long-running task execution is physically offloaded to external workers across network boundaries. Python 3.12 provides rapid domain modeling, clean async I/O via `asyncpg`, a rich database ecosystem, and high developer velocity for a solo engineer. Memory safety and concurrency in NexusFlow are governed by architectural invariants (OCC, single-winner commits, separate worker processes) rather than language-level borrow checkers.*
* **Why not use Redis for worker registry and task dispatch queues?**  
  *Adding Redis introduces dual-state architecture: state committed in PostgreSQL must be synchronized with Redis, creating dual-write hazards and cache invalidation edge cases. In V1, the set of `task_executions` in state `RUNNABLE` in PostgreSQL is the single authoritative source of work truth. Worker sessions are ephemeral and reconstructible via heartbeats in control-plane memory. Avoiding Redis eliminates an entire stateful failure surface.*
* **Why choose an HTTP/JSON pull protocol over gRPC streaming for workers?**  
  *HTTP/JSON pull/long-polling is firewall- and NAT-friendly, trivial to inspect and debug with standard tools (curl, Postman), requires no Protobuf compilation tooling in CI/CD pipelines, and establishes an open REST standard that allows external workers in any language to participate easily without specialized RPC libraries.*
* **How does the system prevent lost updates under `READ COMMITTED` isolation?**  
  *Every state-modifying query includes an explicit conditional predicate checking the expected entity revision and state (`WHERE id = :id AND revision = :expected_rev AND state = :expected_state`). If another transaction committed first, the update affects 0 rows, prompting an immediate transaction rollback and conflict handling under ADR-013.*

---

## 25. References

* [ADR-001: Internal Workflow Specification (IWS)](adr-001-internal-workflow-specification.md)
* [ADR-002: Workflow Definition Parsing Strategy](adr-002-workflow-definition-parsing-strategy.md)
* [ADR-003: Canonical Workflow Graph Representation](adr-003-canonical-workflow-graph-representation.md)
* [ADR-004: Workflow Validation Strategy](adr-004-workflow-validation-strategy.md)
* [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
* [ADR-006: Workflow Execution State Machine](adr-006-workflow-execution-state-machine.md)
* [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
* [ADR-008: Worker Coordination & Liveness Model](adr-008-worker-coordination-and-liveness-model.md)
* [ADR-009: Task Routing Strategy](adr-009-task-routing-strategy.md)
* [ADR-010: Workflow Data Flow & Parameter Passing](adr-010-workflow-data-flow-and-parameter-passing.md)
* [ADR-011: State Persistence Strategy](adr-011-state-persistence-strategy.md)
* [ADR-012: Recovery Strategy](adr-012-recovery-strategy.md)
* [ADR-013: Consistency & Concurrency Strategy](adr-013-consistency-and-concurrency-strategy.md)
* [ADR-014: Execution History & Audit Model](adr-014-execution-history-and-audit-model.md)
* [ADR-015: External API Architecture](adr-015-external-api-architecture.md)
* [ADR-016: Observability Architecture](adr-016-observability-architecture.md)
* [ADR-017: Graceful Shutdown Architecture](adr-017-graceful-shutdown-architecture.md)
* [ADR-018: Error Handling Philosophy](adr-018-error-handling-philosophy.md)
* [ADR-019: Project & Service Boundaries](adr-019-project-and-service-boundaries.md)
* [Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability Matrix

| Requirement / Invariant | Governing ADRs | ADR-020 Physical Technology Manifestation | Downstream Realization |
| :--- | :--- | :--- | :--- |
| **Control Plane Runtime** | FR-TEC-001, ADR-019 | Python 3.12, FastAPI, Uvicorn, standard library `asyncio`. | Codebase packages, Dockerfile. |
| **Authoritative Persistence** | FR-TEC-002, ADR-011 | PostgreSQL 16 with JSONB, SQLAlchemy 2.0 Async, `asyncpg`. | Persistence adapters, DDL migrations. |
| **Optimistic Concurrency (OCC)** | ADR-013 | Integer `revision` column with explicit conditional `WHERE` updates. | Persistence update queries. |
| **Atomic Consistency Groups** | FR-BND-005, ADR-011, ADR-013 | Single-connection PostgreSQL transactions (`async with session.begin():`). | Application Use Cases. |
| **Attempt Ordinal Integrity** | ADR-007, ADR-013 | OCC-guarded ordinal allocation + `UNIQUE(task_execution_id, attempt_number)`. | Database DDL, attempt claim use case. |
| **Worker Protocol & Dispatch** | FR-TEC-003, ADR-008, ADR-009 | HTTP/JSON pull/long-poll protocol with two-phase candidate/ownership coordination. | Worker Protocol Adapter, worker runtime. |
| **Worker Task Execution** | FR-TEC-004, ADR-008, ADR-019 | Worker-local thread pool execution for sync handlers; native async for async handlers. | Worker runtime dispatcher. |
| **Canonical DAG Traversal** | ADR-003, ADR-004 | Custom sparse bidirectional adjacency list module ($\mathcal{O}(V+E)$). | `nexusflow-core` graph module. |
| **Schema Migrations** | FR-TEC-005, ADR-011 | Alembic CLI migrations executed outside application boot. | Migration scripts, container pre-start. |
| **Observability Pipelines** | FR-TEC-006, ADR-016 | `prometheus-client` via `/metrics`, OpenTelemetry tracing to Jaeger, JSON logs. | Observability adapters, Compose stack. |
| **Local Deployment** | NFR-TEC-002 | Docker Compose v2 (6 containers, zero paid cloud dependencies). | `docker-compose.yml`. |

---

## 27. Decision Validation Checklist

- [x] **27 Standard Sections**: Follows the mandatory 27-section structure completely without deviation.
- [x] **Status & Criticality**: Approved — Not Frozen; Core / Implementation Foundation.
- [x] **Control Plane Stack**: Python 3.12, asyncio, FastAPI, Uvicorn, Pydantic v2 (boundary only), plain dataclass domain models.
- [x] **Domain Independence**: Core domain semantics independent of FastAPI, SQLAlchemy, `asyncpg`, and transport libraries.
- [x] **Parsing & Graph**: `ruamel.yaml` for YAML 1.2 with provenance; custom sparse bidirectional adjacency list DAG ($\mathcal{O}(V+E)$) without NetworkX.
- [x] **Authoritative Persistence**: PostgreSQL 16 with JSONB, SQLAlchemy 2.0 Async hybrid approach, `asyncpg`.
- [x] **Isolation & OCC**: `READ COMMITTED` + integer revision OCC conditional updates; single-winner safety guaranteed.
- [x] **Consistency Groups**: Atomic PostgreSQL transactions physically realize ADR-011/013/014 multi-entity groups without distributed 2PC.
- [x] **Attempt Ordinals**: OCC-guarded allocation logic; `UNIQUE(task_execution_id, attempt_number)` defense-in-depth; no `MAX()+1` concurrency mechanism.
- [x] **Entity Identifiers**: UUIDv4 for opaque IDs (`DefinitionId`, `WorkflowExecutionId`, `TaskExecutionId`, `AttemptId`, `HistoryEntryId`, `WorkerSessionId`).
- [x] **Payload Storage**: PostgreSQL JSONB; explicit distinction between committed JSON `null` and uncommitted output.
- [x] **Failure Storage**: Bounded normalized semantic causes under ADR-018; raw exceptions and stack traces excluded.
- [x] **History Integrity**: Append-only PostgreSQL table committed atomically with state; no per-workflow sequence counter; no event sourcing.
- [x] **Idempotency**: PostgreSQL idempotency records with normalized request fingerprint separate from IWS identity.
- [x] **Redis & Broker Rejection**: Redis rejected; Kafka/RabbitMQ rejected; RUNNABLE state in PostgreSQL is authoritative queue.
- [x] **Scheduler Wake-Up**: In-process `asyncio.Event` + bounded durable rediscovery; no mandatory periodic full scans.
- [x] **Timers & Clocks**: Durable PostgreSQL timestamps + in-process priority wakeup queue; abstract Clock port (UTC + monotonic).
- [x] **Worker Registry**: In-process ephemeral map; reconnecting workers reuse existing session ID; restarted workers get new session ID.
- [x] **Worker Protocol**: HTTP/JSON pull/long-polling with explicit two-phase Candidate $\to$ Ownership coordination; best-effort cancellation.
- [x] **Worker Execution**: Worker-local thread pool with cooperative cancellation limitations documented; pickle explicitly prohibited.
- [x] **Observability**: `prometheus-client` for metrics, OpenTelemetry SDK for traces, Jaeger, Prometheus, Grafana, structured JSON logs.
- [x] **Local Deployment**: Docker Compose v2 (6 containers); Kubernetes deferred.
- [x] **Developer Tooling**: `uv`, Ruff, Pyright, `pytest`, `pytest-asyncio`, GitHub Actions.
- [x] **Startup & Shutdown**: Startup reconciliation before readiness; fail-fast supervisor for invariant loops; ASGI lifespan graceful drain.
- [x] **No Performance Fiction**: Qualitative engineering reasoning only; no unmeasured benchmark numbers.
