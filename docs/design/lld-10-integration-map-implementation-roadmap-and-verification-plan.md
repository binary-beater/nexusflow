# NexusFlow V1 — LLD-10: Integration Map, Implementation Roadmap & Verification Plan

**Document Status:** Implementation-Ready / Final NexusFlow V1 Architecture Baseline  
**Authoritative References:** ADR-001 through ADR-023, NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership), LLD-05 (Worker Protocol & Worker Runtime), LLD-06 (Execution Results, Retries, Timeouts & Cancellation), LLD-07 (Recovery & Reconciliation), LLD-08 (Public API & Security), LLD-09 (Observability, Configuration & Runtime Lifecycle).  
**Downstream Phase:** Implementation Phase (Code, Tests, Docker, CI, Demos).

---

## 1. Primary Objective & Purpose

This document is the final implementation bridge for NexusFlow V1. It translates the complete, frozen architectural decisions (ADR-001 through ADR-023 and LLD-01 through LLD-09) into an executable engineering roadmap, a file-level repository structure, an implementation dependency DAG, test verification gates, and an MVP delivery plan.

### Core Implementation Directives
1. **Zero Architectural Drift:** No new lifecycle states, storage systems, message brokers, caching tiers, event sourcing models, or public API endpoints are introduced.
2. **Immediate Executability:** The file paths, module boundaries, database tables, and dependency sequences specified herein provide unambiguous instructions for AI coding agents and engineers to construct the codebase file by file without having to resolve architectural ambiguities.
3. **Correctness Before Optimization:** Invariant enforcement, transactional atomicity, optimistic concurrency control (OCC), worker fencing, and crash safety strictly precede performance tuning and synthetic caching.
4. **LLD-02 / LLD-08 Schema Authority:** LLD-10 never defines, overrides, or reinterprets physical database schemas. Implementations must follow the exact frozen column definitions, constraints, and tables in LLD-02 and LLD-08.

---

## 2. Frozen V1 Architecture Summary

| Dimension | Frozen Architectural Specification | Reference |
| :--- | :--- | :--- |
| **Language & Runtime** | Python 3.12, asyncio event loop, `uv` package and project manager | ADR-020 |
| **HTTP Framework** | FastAPI 0.110+ on ASGI (Uvicorn single worker process `--workers 1`) | ADR-015, ADR-020 |
| **Database & Driver** | PostgreSQL 16, SQLAlchemy 2.0 (Async Core/ORM), `asyncpg` driver | ADR-011, ADR-020 |
| **Schema Migrations** | Alembic out-of-band migrations; startup verifies current schema revision against HEAD | ADR-011, ADR-020 |
| **Domain Modeling** | Plain Python `@dataclass(frozen=True, slots=True)` and standard `StrEnum`; boundaries/config use Pydantic v2 | LLD-01, ADR-023 |
| **Workflow Definition** | Strict YAML parser via `ruamel.yaml` (CSafeLoader), AST node limits (1000 nodes, depth 16) | ADR-002, LLD-03 |
| **Worker Architecture** | External distributed workers communicating via HTTP/JSON pull; ephemeral in-memory registry | ADR-008, LLD-05 |
| **Process Scale ($N=1$)** | Single active control-plane instance; in-memory timer heaps and wakeup queues; no clustering | ADR-017, LLD-09 |
| **Storage Authority** | PostgreSQL relational state is the sole durable authority; control plane is memory-ephemeral | ADR-011, LLD-02 |
| **Excluded Technologies** | **No Redis, No Kafka, No RabbitMQ, No Celery, No Outbox Tables, No Leader Election, No HA Clustering** | ADR-011, ADR-019 |
| **Telemetry & Metrics** | Prometheus client (`/metrics`, low-cardinality), OpenTelemetry (operation-scoped), structured JSON logs | ADR-016, LLD-09 |
| **Observability Mode** | Fail-open; telemetry operations never participate in DB transactions or block workflow progress | ADR-016, LLD-09 |
| **Quality Tooling** | `pytest`, `pytest-asyncio`, `Hypothesis`, `Ruff` (linter/formatter), `Pyright` (type checker) | ADR-021 |

---

## 3. Final Authoritative Repository Layout

The concrete layout consolidates all module paths frozen across LLD-01 through LLD-09:

```text
nexusflow/
├── .github/
│   └── workflows/
│       └── ci.yml                      # Automated CI: lint, typecheck, unit, integration tests
├── deploy/
│   ├── grafana/
│   │   └── dashboards/
│   │       └── nexusflow.json          # Provisioned Grafana dashboard
│   └── prometheus.yml                  # Local Prometheus scrape configuration
├── docs/
│   ├── adr/                            # ADR-001 through ADR-023
│   ├── design/                         # HLD and LLD-01 through LLD-10
│   └── architecture.md                 # System architecture overview
├── examples/
│   ├── workflows/
│   │   ├── 01_linear_success.yaml      # Demo 1: Linear A -> B -> C data flow (exact LLD-03 YAML)
│   │   ├── 02_fan_out_fan_in.yaml      # Demo 2: Diamond DAG concurrency (exact LLD-03 YAML)
│   │   ├── 03_retry_policy.yaml        # Demo 3: Explicit retry exhaustion / recovery (exact LLD-03 YAML)
│   │   └── 04_cancellation.yaml        # Demo 4: Graceful workflow cancellation (exact LLD-03 YAML)
│   └── worker_agent.py                 # Reference Python worker runtime executing @activity tasks
├── migrations/
│   ├── env.py                          # Alembic async migration environment
│   ├── script.py.mako                  # Migration script template
│   └── versions/
│       └── 20260909_0001_initial_schema.py # Authoritative V1 DDL migration (exact LLD-02 schema)
├── src/
│   └── nexusflow/
│       ├── __init__.py
│       ├── config/
│       │   ├── __init__.py
│       │   ├── settings.py             # Deeply frozen Pydantic Settings models (LLD-09)
│       │   └── categories.py           # ADR-023 configuration categorization
│       ├── domain/
│       │   ├── __init__.py
│       │   ├── identifiers.py          # Strongly typed IDs (WorkflowExecutionId, etc.)
│       │   ├── enums.py                # Strict lifecycle StrEnums (WorkflowState, TaskState, etc.)
│       │   ├── spec.py                 # Validated IWS structures and AST models (LLD-03)
│       │   ├── graph.py                # Canonical DAG representation and topological algorithms
│       │   ├── model.py                # Immutable domain entities (TaskExecution, Attempt, etc.)
│       │   ├── failure.py              # FailureCategory, TerminalFailurePolicy, FailureCause
│       │   ├── security.py             # SecurityContext, PublicPermission, PrincipalType
│       │   ├── clock.py                # SystemClock & Clock protocols (LLD-01, LLD-09)
│       │   └── json_compat.py          # Canonical freeze_json and JSON value objects
│       ├── persistence/
│       │   ├── __init__.py
│       │   ├── orm.py                  # SQLAlchemy declarative table mappings (exact LLD-02)
│       │   ├── engine.py               # AsyncEngine factory and connection lifecycle
│       │   ├── contracts.py            # Low-level persistence protocol ports (LLD-01, LLD-02)
│       │   ├── occ.py                  # Optimistic concurrency control retry primitives
│       │   ├── repositories.py         # Concrete repository implementations (LLD-02)
│       │   ├── transactions.py         # Atomic multi-entity persistence transaction scripts
│       │   └── verification.py         # Startup Alembic schema revision active probe
│       ├── definition/
│       │   ├── __init__.py
│       │   ├── parser.py               # ruamel.yaml loader with AST depth/node guards
│       │   ├── normalizer.py           # AST canonical normalization (LLD-03)
│       │   ├── validator.py            # Semantic DAG, whole-value binding, and invariant validation
│       │   └── fingerprint.py          # Deterministic canonical Validated IWS fingerprint generator
│       ├── orchestration/
│       │   ├── __init__.py
│       │   ├── initialization.py       # WorkflowExecution creation and task population
│       │   ├── readiness.py            # Dependency satisfaction and whole-value binding evaluator
│       │   ├── scheduler.py            # Scheduler pass and RUNNABLE state transitions
│       │   ├── routing.py              # Activity capability matching and candidate selection
│       │   ├── ownership.py            # Authoritative attempt creation and atomic claim commit
│       │   └── terminalization.py      # Final workflow completion/failure aggregation
│       ├── settlement/
│       │   ├── __init__.py
│       │   ├── result.py               # Worker success/failure result processing (LLD-06)
│       │   ├── retries.py              # Retryability evaluation and fixed delay resolution
│       │   ├── timeouts.py             # Claim start and execution timeout sweeps
│       │   ├── liveness.py             # Worker heartbeat evaluation and loss detection
│       │   └── cancellation.py         # Explicit workflow cancellation arbitration and drain
│       ├── worker/
│       │   ├── __init__.py
│       │   ├── registry.py             # In-memory ephemeral WorkerSessionRegistry (LLD-05)
│       │   ├── protocol.py             # Worker request/response DTO schemas
│       │   ├── dispatch.py             # Poll dispatching and candidate assignment
│       │   └── transport.py            # Abstract worker cancellation notice transport
│       ├── recovery/
│       │   ├── __init__.py
│       │   ├── coordinator.py          # 8-Phase keyset startup reconciliation coordinator
│       │   ├── phases.py               # Dedicated recovery phase query/mutation handlers
│       │   └── gate.py                 # Ephemeral ProcessAdmissionPolicy / RecoveryGate
│       ├── runtime/
│       │   ├── __init__.py
│       │   ├── lifecycle.py            # ProcessLifecycle enum (SERVING, DRAINING, TERMINATING)
│       │   ├── supervisor.py           # BackgroundLoop supervisor (critical vs non-critical)
│       │   ├── bootstrap.py            # Application composition root and startup sequence
│       │   ├── shutdown.py             # SIGTERM/SIGINT signal coordinator and graceful drain
│       │   └── timers.py               # In-memory priority heap timer acceleration coordinator
│       ├── observability/
│       │   ├── __init__.py
│       │   ├── logging.py              # SanitizedJsonFormatter with field allowlisting
│       │   ├── redaction.py            # Secret, payload, and DSN scrubbing filters
│       │   ├── metrics.py              # Prometheus metrics catalog with cardinality guards
│       │   └── tracing.py              # Operation-scoped OpenTelemetry tracer factory
│       └── interfaces/
│           ├── __init__.py
│           └── http/
│               ├── __init__.py
│               ├── app.py              # FastAPI application builder and ASGI middleware
│               ├── dependencies.py     # FastAPI dependency injection providers
│               ├── errors.py           # Standardized error envelope and exception handlers
│               ├── pagination.py       # Base64url keyset cursor codec and query helpers
│               ├── routes/
│               │   ├── __init__.py
│               │   ├── definitions.py  # POST, GET /v1/definitions (LLD-08)
│               │   ├── executions.py   # POST, GET /v1/executions, cancel, tasks, history
│               │   ├── worker.py       # POST /internal/v1/worker/register, poll, ack, etc.
│               │   ├── health.py       # GET /healthz and GET /readyz (LLD-09)
│               │   └── metrics.py      # GET /metrics Prometheus scraping endpoint
│               └── dto/
│                   ├── __init__.py
│                   ├── requests.py     # Inbound Pydantic v2 request DTOs
│                   └── responses.py    # Outbound Pydantic v2 response DTOs
├── tests/
│   ├── conftest.py                     # Shared fixtures (asyncpg pool, FakeClock, clean DB)
│   ├── unit/                           # Pure in-memory unit tests (no database required)
│   ├── integration/                    # Real PostgreSQL 16 integration tests
│   ├── e2e/                            # End-to-end HTTP API and worker interaction tests
│   └── load/                           # Lightweight benchmark scripts (Locust / asyncio)
├── .env.example                        # Template environment variables for local run
├── .gitignore
├── alembic.ini                         # Alembic database configuration
├── docker-compose.yml                  # Single-node stack (PostgreSQL, Control Plane, OTel)
├── Dockerfile                          # Production-ready multi-stage Python 3.12 Dockerfile
├── pyproject.toml                      # Project metadata, dependencies, and tool configs
└── README.md                           # System overview, quick start, demo guide, portfolio
```

---

## 4. File-Level Ownership & Traceability Map

| Module Path | Responsibility | Auth LLD | Direct Dependencies | Consumed By | MVP Req? | Primary Tests |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `domain/identifiers.py` | Strongly typed entity identifiers | LLD-01 | Standard Library | Entire Codebase | **Yes** | `tests/unit/domain/test_types.py` |
| `domain/enums.py` | Authoritative lifecycle StrEnums | LLD-01 | Standard Library | Entire Codebase | **Yes** | `tests/unit/domain/test_enums.py` |
| `domain/spec.py` | Validated IWS and AST models | LLD-03 | `domain/identifiers.py` | `definition/`, `orchestration/` | **Yes** | `tests/unit/domain/test_spec.py` |
| `domain/graph.py` | Canonical DAG representation & cycle detection | LLD-03 | `domain/spec.py` | `definition/`, `orchestration/` | **Yes** | `tests/unit/domain/test_graph.py` |
| `domain/model.py` | Immutable domain entity dataclasses | LLD-01 | `domain/identifiers.py`, `enums.py` | `persistence/`, `orchestration/` | **Yes** | `tests/unit/domain/test_model.py` |
| `domain/failure.py` | Failure causes, categories, retry policies | LLD-06 | `domain/enums.py` | `settlement/`, `recovery/` | **Yes** | `tests/unit/domain/test_failure.py` |
| `domain/security.py` | SecurityContext, permissions, principal types | LLD-08 | Standard Library | `interfaces/`, `config/` | **Yes** | `tests/unit/domain/test_security.py` |
| `domain/clock.py` | SystemClock and FakeClock protocols | LLD-01, 09 | Standard Library | Entire Codebase | **Yes** | `tests/unit/runtime/test_clock.py` |
| `domain/json_compat.py` | Canonical JSON serialization and freeze helper | LLD-01 | Standard Library | `persistence/`, `definition/` | **Yes** | `tests/unit/domain/test_json.py` |
| `persistence/orm.py` | SQLAlchemy declarative tables (exact LLD-02 schema) | LLD-02 | SQLAlchemy 2.0 | `persistence/repositories.py` | **Yes** | `tests/integration/persistence/test_schema.py` |
| `persistence/engine.py` | AsyncEngine pool lifecycle and connection test | LLD-02 | `asyncpg`, `config/` | `runtime/bootstrap.py` | **Yes** | `tests/integration/persistence/test_engine.py` |
| `persistence/transactions.py` | Atomic multi-entity persistence scripts | LLD-02 | `persistence/orm.py`, `domain/` | `orchestration/`, `settlement/` | **Yes** | `tests/integration/persistence/test_tx.py` |
| `persistence/occ.py` | OCC conditional update wrappers and retry loop | LLD-02 | SQLAlchemy 2.0 | `persistence/transactions.py` | **Yes** | `tests/integration/persistence/test_occ.py` |
| `persistence/verification.py` | Active Alembic revision table probe | LLD-09 | `persistence/engine.py` | `runtime/bootstrap.py` | **Yes** | `tests/integration/runtime/test_schema_verify.py` |
| `definition/parser.py` | Safe ruamel.yaml parsing with AST guards | LLD-03 | `ruamel.yaml` | `definition/validator.py` | **Yes** | `tests/unit/definition/test_parser.py` |
| `definition/normalizer.py` | AST canonical normalization | LLD-03 | `definition/parser.py` | `definition/validator.py` | **Yes** | `tests/unit/definition/test_normalizer.py` |
| `definition/validator.py` | Semantic DAG and whole-value binding validation | LLD-03 | `domain/spec.py`, `graph.py` | `interfaces/http/routes/definitions`| **Yes** | `tests/unit/definition/test_validator.py` |
| `definition/fingerprint.py` | Deterministic SHA-256 semantic fingerprint | LLD-03, 08 | `domain/json_compat.py` | `interfaces/http/routes/definitions`| **Yes** | `tests/unit/definition/test_fingerprint.py` |
| `orchestration/initialization.py`| WorkflowExecution creation & task population | LLD-01, 04 | `persistence/transactions.py` | `interfaces/http/routes/executions` | **Yes** | `tests/integration/orchestration/test_init.py` |
| `orchestration/readiness.py` | Dependency check and whole-value input binding | LLD-04 | `domain/spec.py`, `domain/model.py` | `orchestration/scheduler.py` | **Yes** | `tests/unit/orchestration/test_readiness.py` |
| `orchestration/scheduler.py` | Transition-driven readiness sweep & queue push | LLD-04 | `persistence/transactions.py` | `runtime/supervisor.py` | **Yes** | `tests/integration/orchestration/test_scheduler.py`|
| `orchestration/routing.py` | Capability-based candidate worker selection | LLD-04 | `worker/registry.py` | `orchestration/ownership.py` | **Yes** | `tests/unit/orchestration/test_routing.py` |
| `orchestration/ownership.py` | Atomic ownership commit and Attempt creation | LLD-04 | `persistence/transactions.py` | `worker/dispatch.py` | **Yes** | `tests/integration/orchestration/test_ownership.py`|
| `orchestration/terminalization.py`| Final workflow SUCCEEDED/FAILED aggregation | LLD-06 | `persistence/transactions.py` | `settlement/result.py` | **Yes** | `tests/integration/orchestration/test_term.py` |
| `worker/registry.py` | In-memory ephemeral WorkerSessionRegistry | LLD-05 | `domain/identifiers.py` | `worker/dispatch.py`, `settlement/` | **Yes** | `tests/unit/worker/test_registry.py` |
| `worker/dispatch.py` | Worker poll matching and attempt dispatching | LLD-05 | `orchestration/ownership.py` | `interfaces/http/routes/worker.py` | **Yes** | `tests/integration/worker/test_dispatch.py` |
| `settlement/result.py` | Success/failure callback processing & OCC fence | LLD-06 | `persistence/transactions.py` | `interfaces/http/routes/worker.py` | **Yes** | `tests/integration/settlement/test_result.py` |
| `settlement/retries.py` | Retryability evaluation & fixed delay resolution | LLD-06, 09 | `domain/failure.py`, `domain/clock.py`| `settlement/result.py` | **Yes** | `tests/unit/settlement/test_retries.py` |
| `settlement/timeouts.py` | Claim start & execution timeout sweep execution | LLD-06 | `persistence/transactions.py` | `runtime/supervisor.py` | **Yes** | `tests/integration/settlement/test_timeouts.py` |
| `settlement/liveness.py` | Worker heartbeat evaluation & WORKER_LOSS settlement| LLD-05, 06 | `worker/registry.py`, `persistence/` | `runtime/supervisor.py` | **Yes** | `tests/integration/settlement/test_liveness.py` |
| `settlement/cancellation.py` | Workflow cancellation arbitration & attempt drain| LLD-06, 08 | `persistence/transactions.py` | `interfaces/http/routes/executions` | **Yes** | `tests/integration/settlement/test_cancel.py` |
| `recovery/coordinator.py` | 8-Phase keyset startup reconciliation manager | LLD-07 | `recovery/phases.py`, `recovery/gate`| `runtime/bootstrap.py` | **Yes** | `tests/integration/recovery/test_recovery.py` |
| `recovery/phases.py` | Handlers for individual keyset recovery phases | LLD-07 | `persistence/transactions.py` | `recovery/coordinator.py` | **Yes** | `tests/integration/recovery/test_phases.py` |
| `recovery/gate.py` | Ephemeral admission policy (RecoveryGate) | LLD-07 | Standard Library | `interfaces/http/dependencies.py` | **Yes** | `tests/unit/recovery/test_gate.py` |
| `config/settings.py` | Deeply frozen Pydantic Settings & cred builder | LLD-08, 09 | `pydantic_settings`, `domain/` | Entire Codebase | **Yes** | `tests/unit/config/test_settings.py` |
| `runtime/lifecycle.py` | Ephemeral ProcessLifecycle enum | LLD-09 | Standard Library | `runtime/supervisor.py`, `shutdown` | **Yes** | `tests/unit/runtime/test_lifecycle.py` |
| `runtime/supervisor.py` | BackgroundLoop supervisor (critical vs non-crit) | LLD-09 | `asyncio`, `runtime/lifecycle.py` | `runtime/bootstrap.py` | **Yes** | `tests/integration/runtime/test_supervisor.py` |
| `runtime/timers.py` | In-memory priority heap timer acceleration | LLD-09 | `heapq`, `domain/clock.py` | `runtime/supervisor.py` | **Yes** | `tests/unit/runtime/test_timers.py` |
| `runtime/shutdown.py` | Controlled graceful drain and signal coordinator | LLD-09 | `runtime/lifecycle.py`, `supervisor`| `runtime/bootstrap.py` | **Yes** | `tests/integration/runtime/test_shutdown.py` |
| `runtime/bootstrap.py` | Application composition root and boot orchestrator| LLD-09 | All subsystems | `interfaces/http/app.py` | **Yes** | `tests/integration/runtime/test_bootstrap.py` |
| `observability/logging.py`| Sanitized JSON structured log formatter | LLD-09 | `logging`, `json`, `observability/` | Entire Codebase | **Yes** | `tests/unit/observability/test_logging.py` |
| `observability/redaction.py`| Secret, payload, and DSN pre-serialization scrub | LLD-09 | Standard Library | `observability/logging.py` | **Yes** | `tests/unit/observability/test_redaction.py` |
| `observability/metrics.py`| Prometheus metrics catalog & cardinality guards | LLD-09 | `prometheus_client` | `interfaces/`, `orchestration/` | **Yes** | `tests/unit/observability/test_metrics.py` |
| `observability/tracing.py`| Operation-scoped OpenTelemetry tracer factory | LLD-09 | `opentelemetry-api` | `interfaces/`, `persistence/` | **Yes** | `tests/unit/observability/test_tracing.py` |
| `interfaces/http/app.py` | FastAPI application setup & middleware stack | LLD-08, 09 | FastAPI, `runtime/bootstrap.py` | Docker / Uvicorn entrypoint | **Yes** | `tests/e2e/test_app_boot.py` |
| `interfaces/http/errors.py` | Error envelopes, HTTP exception handlers | LLD-08 | FastAPI, Pydantic v2 | `interfaces/http/app.py` | **Yes** | `tests/unit/interfaces/test_errors.py` |
| `interfaces/http/routes/` | Public `/v1/` and internal `/internal/v1/worker/` | LLD-08 | FastAPI, Domain Use Cases | `interfaces/http/app.py` | **Yes** | `tests/e2e/test_api_routes.py` |

---

## 5. Implementation Dependency DAG

The implementation must strictly respect topological dependencies:

```text
[Layer 0: Core Domain Primitives]
├── domain/identifiers.py
├── domain/enums.py
├── domain/clock.py
├── domain/json_compat.py
└── domain/security.py
         │
         ▼
[Layer 1: Definition & Storage Foundation]
├── domain/spec.py & domain/graph.py ───────► definition/parser.py, normalizer.py, validator.py
│                                                    │
└── persistence/orm.py (Exact LLD-02 Schema)         │
         │                                           │
         ▼                                           │
[Layer 2: Persistence Engine & Atomic Transactions]  │
├── persistence/engine.py & occ.py                   │
└── persistence/transactions.py                      │
         │                                           │
         ├───────────────────────────────────────────┘
         ▼
[Layer 3: Workflow Initialization & Validation API]
├── orchestration/initialization.py
└── interfaces/http/routes/definitions.py (POST /v1/definitions)
         │
         ▼
[Layer 4: Scheduling, Routing & Ephemeral Worker Coordination]
├── orchestration/readiness.py & scheduler.py
├── worker/registry.py
└── orchestration/routing.py & ownership.py
         │
         ▼
[Layer 5: Worker Protocol & Dispatch Endpoints]
├── worker/dispatch.py
└── interfaces/http/routes/worker.py (POST /internal/v1/worker/register, poll, start_ack)
         │
         ▼
[Layer 6: Result Settlement, Retries & Timeouts]
├── settlement/result.py & retries.py
├── settlement/timeouts.py & liveness.py
└── orchestration/terminalization.py
         │
         ▼
[Layer 7: Cancellation & Crash Recovery]
├── settlement/cancellation.py
└── recovery/coordinator.py & phases.py (8-Phase Startup Keyset Recovery)
         │
         ▼
[Layer 8: Public Execution Management API]
└── interfaces/http/routes/executions.py (POST, GET /v1/executions, cancel, tasks, history)
         │
         ▼
[Layer 9: Runtime Supervision, Observability & Draining]
├── config/settings.py
├── observability/logging.py, metrics.py, tracing.py
├── runtime/supervisor.py, timers.py, shutdown.py, bootstrap.py
└── interfaces/http/app.py & health/metrics routes
         │
         ▼
[Layer 10: Portfolio Packaging & Integration Proof]
├── docker-compose.yml & Dockerfile
├── examples/worker_agent.py & workflow YAMLs
└── tests/e2e/ & Crash-Recovery Demonstrations
```

---

## 6. Database Implementation Sequence & Schema Directives

### 6.1 Strict Upstream Schema Authority
**Implementation Directive:** The control-plane persistence layer must implement each database table **exactly** from the authoritative schema frozen in LLD-02 and the API idempotency schema in LLD-08.
- **No New Columns:** LLD-10 does not define, rename, or invent physical schema columns.
- **No Invented Uniqueness:** `semantic_fingerprint` is NOT a unique column on `registered_definitions`. Definition deduplication without an `Idempotency-Key` is strictly prohibited.
- **No Sequence Number:** `history_entries` possesses NO `sequence_number` column; keyset retrieval strictly uses `(occurred_at_utc, history_id)`.

### 6.2 Implementation Order for PostgreSQL & Alembic
1. **`registered_definitions`** (LLD-02 Section 5)
2. **`workflow_executions`** (LLD-02 Section 5)
3. **`task_executions`** (LLD-02 Section 5)
4. **`execution_attempts`** (LLD-02 Section 5)
5. **`history_entries`** (LLD-02 Section 5)
6. **`idempotency_records`** (LLD-08 Section 8)
7. **Indexes & Foreign Key Constraints** (LLD-02 Section 6)
8. **Alembic Initial Migration Script** (`20260909_0001_initial_schema.py`)
9. **Active Schema Verification Probe** (LLD-09 Section 3)

---

## 7. Hard Persistence Verification Gate

Before any domain orchestration or scheduling logic is implemented, the persistence foundation must pass the **Hard Persistence Verification Gate** against a real PostgreSQL 16 container. SQLite or in-memory mocks are strictly prohibited.

```text
================================ HARD PERSISTENCE GATE ================================
[PASS] Table creation & Alembic migration upgrade to HEAD against PostgreSQL 16.
[PASS] RegisteredDefinition persistence matches exact frozen LLD-02 schema.
[PASS] API Idempotency:
       - Same operation scope + same Idempotency-Key + equivalent RequestFingerprint -> original result.
       - Same scope + same key + different RequestFingerprint -> 409 IDEMPOTENCY_CONFLICT.
       - Different Idempotency-Key or absent key -> independent registration / execution.
[PASS] WorkflowExecution INITIALIZING is durably created.
[PASS] Task population follows frozen LLD-02 consistency boundaries.
[PASS] RUNNING cannot commit until exact expected task membership is complete.
[PASS] Injected interruption during population leaves a recoverable INITIALIZING execution.
[PASS] OCC Revision Check: Winner increments revision; Loser receives OCCConflictError.
[PASS] Attempt creation strictly enforces task ownership binding (WorkerSessionId).
[PASS] Task output persistence atomically commits output, has_output=True, and history entry.
[PASS] History entry insertion occurs inside the exact state transition transaction.
[PASS] Keyset cursor pagination queries execute deterministically without row skips or duplicates.
[PASS] Unknown-commit reread helper verifies transaction outcome following connection drop.
======================================================================================
```

---

## 8. Vertical Slice Implementation Strategy

Development proceeds in nine thin, integrated vertical slices:

### Slice 1 — Definition Registration Pipeline
- **Scope:** Ingest raw YAML via `POST /v1/definitions`, validate DAG topology and whole-value bindings, compute semantic fingerprint for idempotency, persist to `registered_definitions`.
- **Acceptance:** Valid YAML returns `201 Created`; malformed YAML or cyclic graphs return `400 Bad Request` or `422 Unprocessable Entity` per LLD-08; repeated submission with the same `Idempotency-Key` returns original definition idempotently.

### Slice 2 — Workflow Creation & Task Population
- **Scope:** `POST /v1/executions` accepts `definition_id` and input; persists `WorkflowExecution` in `INITIALIZING`; populates all `TaskExecution` rows in `PENDING`; promotes workflow to `RUNNING` only after complete membership is verified.
- **Acceptance:** Full membership populated; partial population remains in `INITIALIZING` and is repaired by LLD-07; unstarted tasks visible via `GET /v1/executions/{id}/tasks`.

### Slice 3 — Scheduling to Worker Ownership
- **Scope:** Root tasks transition `PENDING -> RUNNABLE`; external worker registers via `POST /internal/v1/worker/register`; worker polls `/internal/v1/worker/poll`; router matches activity; ownership transaction atomically creates `ExecutionAttempt` in `CLAIMED`.
- **Acceptance:** Worker poll returns task assignment; candidate assignment produces zero attempts without DB commit.

### Slice 4 — Happy-Path Task Execution & Workflow Success
- **Scope:** Worker acknowledges start (`CLAIMED -> RUNNING`); executes work; submits success callback via `POST /internal/v1/worker/callback`; whole-value output persisted; downstream tasks become `RUNNABLE`; workflow transitions to `SUCCEEDED`.
- **Acceptance:** Linear DAG ($A \to B \to C$) runs to completion; outputs pass between direct dependencies; workflow output correctly populated via `WorkflowTaskOutputBinding`.

### Slice 5 — Failure Classification & Task Retries
- **Scope:** Worker submits failure callback; error classified by retry policy; attempt marked `FAILED`; task transitions to `RETRY_WAIT` with positive `retry_ready_at_utc`; sweeper promotes back to `RUNNABLE`; new attempt ordinal created.
- **Acceptance:** Flaky task fails attempt 1, waits positive delay, succeeds on attempt 2; retry exhaustion transitions task to `FAILED` and workflow to `FAILING -> FAILED`.

### Slice 6 — Deadlines & Worker Loss
- **Scope:** Timer heap tracks `start_deadline_utc` and `execution_timeout_utc`; worker liveness sweeper detects missing heartbeats; expired attempts marked `FAILED` with `WORKER_LOSS` or `EXECUTION_TIMEOUT` metadata.
- **Acceptance:** Worker abruptly killed; attempt timed out; retry or terminal failure safely triggered.

### Slice 7 — Workflow Cancellation Arbitration
- **Scope:** `POST /v1/executions/{id}/cancel` submitted; OCC first-direction-wins arbitration transitions workflow to `CANCELLING`; unstarted tasks marked `CANCELLED`; active attempts receive cancellation grace; workflow converges to `CANCELLED`.
- **Acceptance:** Cancellation rejected with 409 if terminal (`SUCCEEDED`, `FAILED`, `FAILING`); active attempts drain; idempotent re-cancellation returns 200 OK.

### Slice 8 — Startup Crash Recovery (LLD-07)
- **Scope:** Kill control plane mid-execution; restart process; RecoveryGate blocks new work admission while admitting existing-work settlement; 8-phase keyset reconciliation cleans up orphan attempts, repairs incomplete initialization, and reconciles terminal workflows; RecoveryGate opens; readiness declared.
- **Acceptance:** Control plane reboot leaves zero orphan attempts; recovered workflow converges safely according to durable state.

### Slice 9 — Runtime Observability & Draining (LLD-09)
- **Scope:** Wire structured JSON logging with secret redaction; export Prometheus metrics on `/metrics`; configure OTLP tracing; implement SIGTERM graceful drain.
- **Acceptance:** Grafana dashboard displays real metrics; Jaeger visualizes traces; logs contain zero raw tokens; SIGTERM drains in-flight callbacks.

---

## 9. Two-Day MVP Delivery Scope & Hard Constraints

Designed for a focused 20–24 hour engineering effort using AI coding assistance:

```text
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                                 NEXUSFLOW V1 MVP SCOPE                                  │
├────────────────────────────────────────┬────────────────────────────────────────────────┤
│            MUST IMPLEMENT NOW          │          POST-MVP HARDENING BACKLOG            │
├────────────────────────────────────────┼────────────────────────────────────────────────┤
│ • Complete 6-Table PostgreSQL Schema   │ • Extensive multi-worker race chaos matrix     │
│ • Validated IWS Parsing & DAG Check    │ • Property-based hypothesis graph testing      │
│ • Atomic/Consistent Task Population    │ • Deep unknown-commit reread network fuzzing   │
│ • Dependency-Driven Scheduler          │ • Large-scale payload limit benchmark (> 5MB)  │
│ • External Worker HTTP Pull & Registry │ • Prometheus metric label cardinality fuzzing  │
│ • Single-Process Worker Ownership (OCC)│ • OpenTelemetry collector disconnect chaos     │
│ • Success/Failure Callback Settlement  │ • Advanced cancellation deadline race matrices │
│ • Sequential Retries (Positive Delay)  │ • Performance tuning & PostgreSQL index bloat  │
│ • Workflow SUCCEEDED, FAILED, CANCEL   │                                                │
│ • Core 8-Phase Startup Recovery (LLD-07)│                                               │
│ • Public API (/v1) with Bearer Auth    │                                                │
│ • Structured JSON Logs (Redacted)      │                                                │
│ • Basic Prometheus Metrics & Healthz   │                                                │
│ • Docker Compose Single-Node Stack     │                                                │
│ • 4 Demo Workflows & Reference Worker  │                                                │
└────────────────────────────────────────┴────────────────────────────────────────────────┘
```

### Explicit Non-Goals & MVP Prohibitions
To ensure delivery within the time budget, the following are strictly excluded:
- **No High Availability (HA) or Leader Election:** Single control-plane process ($N=1$).
- **No Message Brokers or Queues:** No Kafka, RabbitMQ, SQS, or Redis.
- **No Dynamic Config Reload:** Configuration is deeply immutable after boot.
- **No Web Dashboard or UI:** Pure headless API and CLI curl interaction.
- **No Large Payload Storage:** Workflow payloads must fit within standard JSONB limits ($\le 1\text{ MB}$).
- **No Python Worker SDK Package:** Reference worker agent only; no separate published SDK.
- **No JSONPath / Pointer Subsystem:** Whole-value data binding only (`literal`, `workflow_input`, `task_output`).

---

## 10. Milestone Execution Breakdown

### Milestone 0: Repository Bootstrap & Tooling Verification
- **Deliverables:** `pyproject.toml`, `uv.lock`, `.env.example`, `ruff.toml`, `Dockerfile`, `docker-compose.yml`, `migrations/`.
- **Acceptance Criteria:** `uv sync` completes cleanly; `ruff check .` passes; `pyright` reports zero type errors; `docker compose up postgres` launches healthy PostgreSQL 16; `alembic upgrade head` executes with zero warnings.

### Milestone 1: Core Domain Primitives & IWS Specifications
- **Deliverables:** `domain/identifiers.py`, `domain/enums.py`, `domain/clock.py`, `domain/json_compat.py`, `domain/spec.py`, `domain/graph.py`, `domain/model.py`.
- **Acceptance Criteria:** Unit tests verify strict enum string representations; canonical JSON freezing guarantees key sorting; graph algorithms detect cycles ($A \to B \to A$) and resolve topological orderings.

### Milestone 2: Persistence Layer & Atomic Transactions
- **Deliverables:** `persistence/orm.py`, `persistence/engine.py`, `persistence/occ.py`, `persistence/transactions.py`, `persistence/verification.py`.
- **Acceptance Criteria:** Pass all requirements of the Hard Persistence Gate against real PostgreSQL 16; OCC concurrency tests prove that competing updates to the same revision produce exactly one winner and one `OCCConflictError`.

### Milestone 3: Definition Ingestion & Validation Pipeline
- **Deliverables:** `definition/parser.py`, `definition/normalizer.py`, `definition/validator.py`, `definition/fingerprint.py`, `interfaces/http/routes/definitions.py`.
- **Acceptance Criteria:** Reject YAML with unknown keys, duplicate task IDs, cyclic dependencies, or missing activity types; compute deterministic semantic fingerprints for idempotency checks; register definitions via `POST /v1/definitions`.

### Milestone 4: Execution Initialization & Task Population
- **Deliverables:** `orchestration/initialization.py`, `interfaces/http/routes/executions.py` (POST /v1/executions).
- **Acceptance Criteria:** Create `WorkflowExecution` in `INITIALIZING`; populate `TaskExecution` rows following LLD-02 transaction boundaries; promote to `RUNNING` only after complete membership is established; verify that restarting mid-population leaves workflow repairable.

### Milestone 5: Dependency Scheduling & Whole-Value Data Binding
- **Deliverables:** `orchestration/readiness.py`, `orchestration/scheduler.py`.
- **Acceptance Criteria:** Root tasks transition `PENDING -> RUNNABLE`; dependent tasks remain `PENDING` until upstream dependencies reach `SUCCEEDED`; task input bindings resolve frozen whole-value binding variants correctly (`literal`, `workflow_input`, `task_output`).

### Milestone 6: Worker Protocol & Ownership Coordination
- **Deliverables:** `worker/registry.py`, `worker/dispatch.py`, `orchestration/routing.py`, `orchestration/ownership.py`, `interfaces/http/routes/worker.py`.
- **Acceptance Criteria:** Workers register and receive `WorkerSessionId`; long-poll blocks until tasks are available; router matches capability; ownership commit atomically binds task and creates `CLAIMED` attempt; start ack flips attempt to `RUNNING`.

### Milestone 7: Result Settlement & Workflow Success
- **Deliverables:** `settlement/result.py`, `orchestration/terminalization.py`.
- **Acceptance Criteria:** Worker submits output payload; task output written atomically with transition to `SUCCEEDED`; dependent tasks scheduled; when all terminal tasks succeed, workflow outputs resolve via `WorkflowTaskOutputBinding` and state flips to `SUCCEEDED`.

### Milestone 8: Failure Handling & Retry Sweepers
- **Deliverables:** `settlement/retries.py`, `runtime/timers.py`.
- **Acceptance Criteria:** Retryable failure transitions attempt to `FAILED`, task to `RETRY_WAIT` with positive `retry_ready_at_utc`; timer sweep promotes task to `RUNNABLE`; non-retryable failure or retry exhaustion transitions workflow to `FAILING -> FAILED`.

### Milestone 9: Timeouts & Worker Liveness
- **Deliverables:** `settlement/timeouts.py`, `settlement/liveness.py`.
- **Acceptance Criteria:** Unacknowledged `CLAIMED` attempt times out via `start_deadline_utc`; long-running task times out via `execution_timeout_utc`; missing worker heartbeat triggers `WORKER_LOSS` settlement.

### Milestone 10: Workflow Cancellation Arbitration
- **Deliverables:** `settlement/cancellation.py`, `interfaces/http/routes/executions.py` (cancel route).
- **Acceptance Criteria:** `POST /v1/executions/{id}/cancel` commits `CANCELLING`; unstarted tasks immediately marked `CANCELLED`; running attempts given cancellation deadline; terminal workflows cannot be cancelled (409 Conflict).

### Milestone 11: Startup Keyset Recovery & Reconciliation
- **Deliverables:** `recovery/coordinator.py`, `recovery/phases.py`, `recovery/gate.py`.
- **Acceptance Criteria:** Control plane fast-boots with `RecoveryGate` closed to new work; executes 8 reconciliation phases across PostgreSQL keyspace; cleans up dangling attempts; admits racing late callbacks for pre-restart RUNNING attempts; opens gate and flips readiness to 200 OK.

### Milestone 12: Runtime Observability, Supervision & Shutdown
- **Deliverables:** `config/settings.py`, `runtime/supervisor.py`, `runtime/shutdown.py`, `runtime/bootstrap.py`, `observability/logging.py`, `observability/metrics.py`, `observability/tracing.py`.
- **Acceptance Criteria:** Supervisor detects critical loop crash and triggers controlled drain; non-critical telemetry loop failure fails open; logs redact secrets; Prometheus metrics expose accurate counters; SIGTERM cleanly drains process.

---

## 11. Comprehensive Test Pyramid & Verification Gates

```text
                        ┌────────────────────────┐
                        │      End-to-End &      │
                        │    Crash Recovery      │  (10 Tests - High Value)
                        │      (tests/e2e)       │
                        ├────────────────────────┴───────┐
                        │      PostgreSQL Integration    │
                        │   Concurrency & OCC Fencing    │  (25 Tests - Mission Critical)
                        │     (tests/integration)        │
                        ├────────────────────────────────┴───────────────┐
                        │              Unit Tests, AST &                │
                        │           State-Machine Invariants            │  (45 Tests - Fast & Deterministic)
                        │                 (tests/unit)                  │
                        └───────────────────────────────────────────────┘
```

### 11.1 The 32 Core MVP Blocking Tests
The following 32 tests must pass before the V1 MVP is declared complete:

1. `test_settings_deep_immutability`: Mutating nested config raises frozen exception.
2. `test_settings_raw_secret_digested`: Raw tokens converted to SHA-256 digests; plaintext discarded.
3. `test_schema_migration_head`: Active probe confirms PostgreSQL schema matches Alembic HEAD.
4. `test_yaml_parser_depth_node_limits`: Reject YAML documents exceeding 16 levels or 1000 nodes.
5. `test_validator_detects_dag_cycles`: Cyclic dependencies ($A \to B \to A$) rejected with validation error.
6. `test_validator_rejects_unknown_keys`: Unrecognized YAML fields fail validation closed.
7. `test_definition_registration_idempotency`: Same Idempotency-Key + equivalent Validated IWS returns existing definition ID; differing semantics returns 409 Conflict.
8. `test_workflow_initialization_complete_before_running`: Workflow cannot transition to `RUNNING` until complete task membership is established.
9. `test_partial_initialization_remains_recoverable`: Interrupted task population leaves recoverable `INITIALIZING` workflow.
10. `test_occ_conflict_detection`: Competing updates to same entity revision reject loser with `OCCConflictError`.
11. `test_scheduler_linear_progression`: Task B becomes `RUNNABLE` only after Task A reaches `SUCCEEDED`.
12. `test_scheduler_diamond_dependency`: Task D becomes `RUNNABLE` only after both Task B and C reach `SUCCEEDED`.
13. `test_routing_activity_capability_match`: Worker claiming activity `add` receives only `add` tasks.
14. `test_historical_session_cannot_poll`: Historical/evicted WorkerSession cannot poll for new tasks.
15. `test_historical_session_cannot_start_attempt`: Historical/evicted WorkerSession cannot acknowledge start of attempt.
16. `test_late_running_attempt_callback_uses_durable_session_fence`: Pre-restart `RUNNING` attempt callback succeeds if OCC wins, or is rejected if recovery reconciles first.
17. `test_ownership_commit_creates_single_attempt`: Task claim atomically creates attempt with ordinal 1.
18. `test_task_output_persistence_atomicity`: Task output, `has_output=True`, and history event commit atomically.
19. `test_workflow_success_terminalization`: Workflow transitions to `SUCCEEDED` with aggregated `WorkflowTaskOutputBinding`.
20. `test_retry_delay_positive_resolution`: Task retry uses positive fixed delay persisted to `retry_ready_at_utc`.
21. `test_retry_ordinal_monotonicity`: Successive attempts strictly increment attempt ordinal ($1, 2, 3$).
22. `test_retry_budget_exhaustion`: Exceeding `max_attempts` transitions task to `FAILED` and workflow to `FAILED`.
23. `test_claim_start_deadline_timeout`: Unacknowledged `CLAIMED` attempt times out with `START_DEADLINE_EXPIRED` cause.
24. `test_worker_loss_heartbeat_timeout`: Dead worker session triggers `WORKER_LOSS` attempt failure.
25. `test_cancellation_first_direction_wins`: Public cancel transitions workflow to `CANCELLING`; active attempts drain.
26. `test_cancellation_terminal_conflict`: Attempting to cancel `SUCCEEDED` or `FAILED` workflow returns 409 Conflict.
27. `test_recovery_gate_blocks_new_work`: Startup recovery blocks public mutations with 503 `NOT_READY`.
28. `test_recovery_reconciles_orphan_claimed`: Pre-restart `CLAIMED` attempt failed with `WORKER_LOSS` upon reboot.
29. `test_supervisor_critical_loop_crash`: Scheduler crash flips readiness to 503 and begins controlled drain.
30. `test_supervisor_telemetry_loop_fail_open`: Telemetry flush crash logs warning; readiness remains 200 OK.
31. `test_logging_secret_redaction`: Structured logs scrub Bearer tokens and passwords before JSON serialization.
32. `test_metrics_cardinality_prohibition`: Prometheus labels contain zero entity UUIDs or high-cardinality values.

---

## 12. Portfolio Demonstration Assets & Demo Workflows (Exact LLD-03 Syntax)

All workflow examples strictly adhere to the frozen LLD-03 grammar: mapping-keyed tasks, explicit `max_attempts`, whole-value bindings (`literal`, `workflow_input`, `task_output`), and `WorkflowTaskOutputBinding` for workflow outputs.

### 12.1 Demo Workflow 1: Linear Pipeline (`examples/workflows/01_linear_success.yaml`)
```yaml
workflow_name: demo_linear_pipeline

tasks:
  step_extract:
    activity_type: extract_tokens
    dependencies: []
    input_bindings:
      text:
        type: workflow_input
    max_attempts: 3

  step_transform:
    activity_type: transform_tokens
    dependencies:
      - step_extract
    input_bindings:
      tokens:
        type: task_output
        task: step_extract
    max_attempts: 3

  step_load:
    activity_type: persist_summary
    dependencies:
      - step_transform
    input_bindings:
      summary:
        type: task_output
        task: step_transform
    max_attempts: 3

output_bindings:
  final_summary:
    type: task_output
    task: step_load
```

### 12.2 Demo Workflow 2: Diamond Concurrency (`examples/workflows/02_fan_out_fan_in.yaml`)
```yaml
workflow_name: demo_fan_out_fan_in

tasks:
  branch_a:
    activity_type: compute_square
    dependencies: []
    input_bindings:
      val:
        type: workflow_input
    max_attempts: 2

  branch_b:
    activity_type: compute_cube
    dependencies: []
    input_bindings:
      val:
        type: workflow_input
    max_attempts: 2

  aggregate:
    activity_type: sum_results
    dependencies:
      - branch_a
      - branch_b
    input_bindings:
      res_a:
        type: task_output
        task: branch_a
      res_b:
        type: task_output
        task: branch_b
    max_attempts: 2

output_bindings:
  total:
    type: task_output
    task: aggregate
```

### 12.3 Demo Workflow 3: Transient Fault Recovery (`examples/workflows/03_retry_policy.yaml`)
```yaml
workflow_name: demo_retry_recovery

tasks:
  flaky_task:
    activity_type: transient_flaky_op
    dependencies: []
    input_bindings:
      target_failures:
        type: literal
        value: 1
    max_attempts: 3

output_bindings:
  status:
    type: task_output
    task: flaky_task
```

### 12.4 Demo Workflow 4: Controlled Cancellation (`examples/workflows/04_cancellation.yaml`)
```yaml
workflow_name: demo_cancellation

tasks:
  long_running_step:
    activity_type: sleep_activity
    dependencies: []
    input_bindings:
      duration:
        type: literal
        value: 60
    max_attempts: 1

output_bindings:
  result:
    type: task_output
    task: long_running_step
```

---

## 13. Reference Python Worker Runtime (`examples/worker_agent.py`)

A reference asynchronous worker agent demonstrating the `@activity` dispatch pattern over HTTP/JSON pull:

```python
import asyncio
import httpx
import logging
from typing import Callable, Any, Coroutine

logger = logging.getLogger("nexusflow.worker")


class ActivityRegistry:
    def __init__(self):
        self._activities: dict[str, Callable[[Any], Coroutine[Any, Any, Any]]] = {}

    def activity(self, name: str):
        def decorator(func: Callable[[Any], Coroutine[Any, Any, Any]]):
            self._activities[name] = func
            return func

        return decorator

    def get(self, name: str):
        return self._activities.get(name)

    @property
    def capabilities(self) -> list[str]:
        return list(self._activities.keys())


registry = ActivityRegistry()


@registry.activity("extract_tokens")
async def extract_tokens(inp: Any) -> Any:
    text = str(inp) if inp is not None else ""
    return text.split()


@registry.activity("transform_tokens")
async def transform_tokens(inp: Any) -> Any:
    tokens = inp if isinstance(inp, list) else []
    return f"Processed {len(tokens)} tokens successfully."


@registry.activity("persist_summary")
async def persist_summary(inp: Any) -> Any:
    logger.info("Persisting summary: %s", inp)
    return {"result_id": "doc_res_9981"}


class NexusFlowWorkerAgent:
    def __init__(self, base_url: str, token: str, worker_id: str):
        self.client = httpx.AsyncClient(
            base_url=base_url, headers={"Authorization": f"Bearer {token}"}, timeout=45.0
        )
        self.worker_id = worker_id
        self.session_id: str | None = None
        self.running = True

    async def run(self):
        # 1. Register Worker Session
        reg_resp = await self.client.post(
            "/internal/v1/worker/register",
            json={"worker_id": self.worker_id, "capabilities": registry.capabilities},
        )
        reg_resp.raise_for_status()
        self.session_id = reg_resp.json()["worker_session_id"]
        logger.info("Registered worker session: %s", self.session_id)

        # 2. Start Background Heartbeat Loop
        asyncio.create_task(self._heartbeat_loop())

        # 3. Long-Poll Work Loop
        while self.running:
            try:
                poll_resp = await self.client.post(
                    "/internal/v1/worker/poll",
                    json={"worker_session_id": self.session_id, "max_tasks": 1},
                )
                if poll_resp.status_code == 204:
                    continue

                assignment = poll_resp.json()["tasks"][0]
                await self._execute_task(assignment)
            except Exception as exc:
                logger.error("Error in worker polling loop: %s", exc)
                await asyncio.sleep(2.0)

    async def _execute_task(self, assignment: dict[str, Any]):
        attempt_id = assignment["attempt_id"]
        activity_type = assignment["activity_type"]
        task_input = assignment["task_input"]

        # Acknowledge Start
        ack_resp = await self.client.post(
            "/internal/v1/worker/start_ack",
            json={"worker_session_id": self.session_id, "attempt_id": attempt_id},
        )
        if ack_resp.status_code != 200:
            logger.warning("Start ack rejected for attempt %s; skipping execution.", attempt_id)
            return

        # Execute Activity Logic
        func = registry.get(activity_type)
        try:
            output = await func(task_input)
            await self.client.post(
                "/internal/v1/worker/callback",
                json={
                    "worker_session_id": self.session_id,
                    "attempt_id": attempt_id,
                    "status": "SUCCEEDED",
                    "output": output,
                },
            )
            logger.info("Successfully completed attempt %s", attempt_id)
        except Exception as exc:
            await self.client.post(
                "/internal/v1/worker/callback",
                json={
                    "worker_session_id": self.session_id,
                    "attempt_id": attempt_id,
                    "status": "FAILED",
                    "failure_category": "ACTIVITY_EXECUTION_ERROR",
                    "error_message": str(exc),
                },
            )

    async def _heartbeat_loop(self):
        while self.running:
            await asyncio.sleep(5.0)
            try:
                await self.client.post(
                    "/internal/v1/worker/heartbeat", json={"worker_session_id": self.session_id}
                )
            except Exception:
                pass
```

---

## 14. Portfolio Crash-Recovery Demonstration Script

This demonstration proves the correctness of the PostgreSQL-authoritative architecture by killing the control plane mid-workflow and observing convergence upon restart:

```bash
#!/usr/bin/env bash
set -euo pipefail

echo "=== [1/6] Launching Clean NexusFlow Stack ==="
docker compose down -v
docker compose up -d postgres
sleep 3
docker compose up -d nexusflow-control-plane
sleep 5

echo "=== [2/6] Registering Linear Pipeline Workflow ==="
DEF_ID=$(curl -s -X POST http://localhost:8000/v1/definitions \
  -H "Authorization: Bearer <PUBLIC_CLIENT_SECRET>" \
  -H "Content-Type: application/yaml" \
  --data-binary @examples/workflows/01_linear_success.yaml | jq -r .definition_id)
echo "Registered Definition ID: ${DEF_ID}"

echo "=== [3/6] Starting Workflow Execution ==="
EXEC_ID=$(curl -s -X POST http://localhost:8000/v1/executions \
  -H "Authorization: Bearer <PUBLIC_CLIENT_SECRET>" \
  -H "Content-Type: application/json" \
  -d "{\"definition_id\": \"${DEF_ID}\", \"workflow_input\": \"NexusFlow durable orchestration proof\"}" | jq -r .workflow_execution_id)
echo "Started Execution ID: ${EXEC_ID}"

echo "=== [4/6] Launching External Reference Worker ==="
python examples/worker_agent.py &
WORKER_PID=$!
sleep 2

echo "=== [5/6] CRASHING CONTROL PLANE MID-EXECUTION ==="
docker compose stop nexusflow-control-plane
echo "Control-plane container stopped. PostgreSQL remains authoritative."
sleep 3

echo "=== [6/6] RESTARTING CONTROL PLANE & VERIFYING CONVERGENCE ==="
docker compose start nexusflow-control-plane
sleep 5

# Poll until workflow reaches terminal state
for i in {1..30}; do
  STATE=$(curl -s -H "Authorization: Bearer <PUBLIC_CLIENT_SECRET>" http://localhost:8000/v1/executions/${EXEC_ID} | jq -r .state)
  echo "Current Execution State: ${STATE}"
  if [ "${STATE}" = "SUCCEEDED" ]; then
    echo "SUCCESS: Workflow converged to SUCCEEDED following control-plane crash recovery!"
    kill ${WORKER_PID} || true
    exit 0
  fi
  sleep 2
done

echo "FAILED: Workflow did not converge in time."
kill ${WORKER_PID} || true
exit 1
```

---

## 15. Standard Developer Tooling & Makefile

```makefile
.PHONY: install lint typecheck test test-integration up down migrate demo clean

install:
	uv sync

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run pyright

test:
	uv run pytest tests/unit -v

test-integration:
	uv run pytest tests/integration -v

up:
	docker compose up -d

down:
	docker compose down

migrate:
	uv run alembic upgrade head

demo:
	./scripts/demo_crash_recovery.sh

clean:
	rm -rf .pytest_cache .ruff_cache __pycache__
```

---

## 16. Continuous Integration (GitHub Actions)

Configured in `.github/workflows/ci.yml`:

```yaml
name: NexusFlow CI

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]

jobs:
  verify:
    runs-on: ubuntu-latest

    services:
      postgres:
        image: postgres:16-alpine
        env:
          POSTGRES_DB: nexusflow_test
          POSTGRES_USER: nexusflow_user
          POSTGRES_PASSWORD: nexusflow_password
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-timeout 5s
          --health-retries 5

    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v3
        with:
          enable-cache: true

      - name: Set up Python 3.12
        run: uv python install 3.12

      - name: Install dependencies
        run: uv sync --frozen

      - name: Run Ruff Linter
        run: uv run ruff check .

      - name: Run Pyright Type Checker
        run: uv run pyright

      - name: Run Alembic Migration
        env:
          NEXUSFLOW_DB__URL: postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow_test
        run: uv run alembic upgrade head

      - name: Run Unit Tests
        run: uv run pytest tests/unit -v

      - name: Run PostgreSQL Integration Tests
        env:
          NEXUSFLOW_DB__URL: postgresql+asyncpg://nexusflow_user:nexusflow_password@localhost:5432/nexusflow_test
        run: uv run pytest tests/integration -v

      - name: Build Docker Container
        run: docker build -t nexusflow-control-plane:latest .
```

---

## 17. Implementation-Agent Operating Guardrails

```text
======================= IMPLEMENTATION-AGENT GUARDRAILS =======================
1. ADR-001 through ADR-023 and LLD-01 through LLD-09 are authoritative and frozen.
2. NEVER introduce new workflow, task, or attempt states outside the frozen enums.
3. NEVER add Redis, Celery, RabbitMQ, Kafka, or outbox persistence tables.
4. NEVER alter the 6-table PostgreSQL schema without explicit instruction.
5. NEVER perform network I/O or sleep inside a PostgreSQL transaction block.
6. NEVER allow external workers to connect directly to PostgreSQL.
7. Attempt records MUST NOT be created during routing; only upon atomic ownership commit.
8. Candidate worker routing is strictly in-memory and ephemeral.
9. All persistence mutations MUST use Optimistic Concurrency Control (OCC).
10. Keyset pagination must use (occurred_at_utc, history_id); sequence_number is prohibited.
11. Telemetry and metrics operations MUST be fail-open and executed outside DB transactions.
12. Process lifecycle possesses exactly 3 states: SERVING, DRAINING, TERMINATING.
13. If an ambiguity arises, STOP implementation of that unit and verify upstream LLDs.
14. LLD-10 never overrides or reinterprets physical schemas defined in LLD-02 / LLD-08.
15. API request fingerprint is not Definition identity.
16. Do NOT deduplicate definitions without Idempotency-Key semantics.
17. Demo YAML must compile against exact LLD-03 grammar.
18. Do NOT invent JSONPath or JSON Pointer binding languages; use frozen whole-value bindings.
19. Current Worker Registry presence is NOT required for the frozen late RUNNING callback settlement path.
20. RUNNING workflow requires complete task membership, but LLD-10 must not invent an artificial single-transaction rule.
21. Reference Python worker is a demonstration tool, NOT an SDK.
==============================================================================
```

---

## 18. Definitive Definition of Done (DoD) for MVP

NexusFlow V1 is declared portfolio-ready and complete when all of the following conditions are verified:
1. `docker compose up` starts healthy PostgreSQL 16, NexusFlow control plane, Prometheus, Grafana, and Jaeger from a clean repository clone.
2. `alembic upgrade head` runs cleanly without warnings against the exact LLD-02 schema.
3. All 4 demo workflows register via `POST /v1/definitions` and execute successfully using `examples/worker_agent.py`.
4. The Automated Crash-Recovery Script runs to completion, demonstrating safe convergence following a mid-execution crash.
5. `GET /readyz` and `GET /healthz` accurately report probe statuses during startup, serving, drain, and fatal failure.
6. `GET /metrics` exports low-cardinality Prometheus metrics containing zero entity UUIDs.
7. Logs emit structured JSON without leaking Bearer tokens, DB passwords, or customer payloads.
8. The 32 Core MVP Blocking Tests pass with 100% success against real PostgreSQL 16.
9. `ruff check .` and `pyright` pass with zero errors.

---

## 19. Final Architecture Freeze Declaration

With the formal completion of **LLD-10**, the architecture phase of NexusFlow V1 is officially concluded:
- **ADR-001 through ADR-023**, the **NexusFlow V1 HLD**, and **LLD-01 through LLD-10** constitute the final, complete, and immutable architectural baseline.
- No further design documents (such as LLD-11 or ADR-024) will be produced.
- All subsequent engineering activities consist exclusively of implementation, testing, container packaging, benchmarking, and documentation.

---

### Classification

**LLD-10 — Implementation-Ready / Final NexusFlow V1 Architecture Baseline**
