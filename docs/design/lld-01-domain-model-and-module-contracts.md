# NexusFlow V1 — LLD-01: Domain Model & Module Contracts

---

## 1. Document Purpose

This Low-Level Design (LLD) document defines the concrete Python-level domain model, module ownership, dependency rules, domain types, state-machine interfaces, persistence port contracts, and application use-case boundaries for NexusFlow V1.

It translates the approved architectural decisions (**ADR-001 through ADR-023**) and the **NexusFlow V1 High-Level Design (HLD)** into precise, type-safe Python contracts that serve as the authoritative baseline for all subsequent LLDs (**LLD-02 through LLD-10**).

The primary objective of this document is to answer:
> **What Python objects and module contracts exist, who owns them, and how may they interact without violating the approved ADR and HLD architecture?**

---

## 2. Scope

### In-Scope
- Pure Python 3.12 domain entities, value objects, enums, and sum types.
- Strict type-level separation of `CandidateWorkflowSpec` vs. `ValidatedWorkflowSpec`.
- Whole-value data flow binding models (`Literal`, `WorkflowInput`, `TaskOutput`) for both task inputs and named workflow outputs per ADR-010.
- Deeply immutable, canonical JSON value representation eliminating mutable reference leaks and non-finite numbers.
- Explicit state machine interfaces and functional transition rules for `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`.
- Canonical graph representation (sparse bidirectional adjacency DAG).
- Pure domain services for readiness evaluation, routing compatibility, and retry eligibility.
- Application use-case contracts, input commands, and typed execution results.
- Persistence port abstractions supporting multi-entity transactional consistency groups under OCC, with explicit anti-TOCTOU semantic predicates (including distinct `commit_retry_ready` for `RETRY_WAIT $\to$ RUNNABLE`).
- Ephemeral worker session model with session continuity, reconnect semantics, and draining support.
- Logical persistence requirements handed off cleanly to LLD-02 without prewriting physical SQL DDL.
- Concrete package layout and import dependency validation rules.

### Out-of-Scope (Deferred to Subsequent LLDs)
- **LLD-02**: PostgreSQL Schema, SQLAlchemy 2.0 Async mappings, Alembic migrations, and persistence transactions.
- **LLD-03**: Definition Ingestion & Validation Pipeline (`ruamel.yaml` parser, semantic validator, DAG cycle detector).
- **LLD-04**: Scheduling, Routing & Candidate Ownership (In-process worker registry, eligibility sweeper, candidate matching).
- **LLD-05**: Worker Protocol & Worker Runtime (FastAPI worker endpoints, long-poll suspension, Python V1 worker thread pool).
- **LLD-06**: Execution Results, Retries, Timeouts & Cancellation (Callback authority, failure handling, deadline enforcement).
- **LLD-07**: Recovery & Reconciliation (Startup snapshot recovery, lost wakeup rediscovery).
- **LLD-08**: Public Management API & Security (FastAPI REST endpoints, Bearer authentication, permission guards, idempotency).
- **LLD-09**: Observability, Configuration & Runtime Lifecycle (Structured logging, Prometheus `/metrics`, OTLP tracing, graceful shutdown).
- **LLD-10**: Integration Map & Implementation Plan (End-to-end component wiring and test-driven slice schedule).

---

## 3. Design Principles & Technology Baseline

### 3.1 Principles
1. **Zero External Dependencies in Domain Core**: The domain layer (`nexusflow.domain.*`) imports only Python 3.12 standard library modules (`dataclasses`, `enum`, `typing`, `uuid`, `datetime`, `abc`, `math`, `types`). It contains zero imports of FastAPI, SQLAlchemy, asyncpg, Pydantic, Prometheus, OpenTelemetry, or HTTP libraries.
2. **Immutable State Snapshots & Pure Transitions**: Domain entities are represented as immutable dataclasses (`frozen=True, slots=True`). State transitions are executed via pure functions returning explicit transition results and newly updated snapshot instances using `dataclasses.replace`.
3. **Consistency Groups Over Entity Aggregates**: Persistence atomicity is organized around multi-entity consistency groups (e.g., Ownership Commit, Task Success + Output, Definitive Task Failure) rather than artificial repository-per-table CRUD patterns.
4. **Distinguishable Value Presence**: Uncommitted/absent data is structurally distinguishable from committed values, ensuring that committed JSON `null` is never conflated with missing output.
5. **Fail-Closed Type Safety**: Python typing (`Protocol`, `TypeAlias`, `frozenset`, `UUID`) strictly prevents semantic identifier mixups (e.g., `TaskDefinitionId` vs. `TaskExecutionId`).
6. **Anti-TOCTOU Persistence Contracts**: Persistence mutation ports require underlying SQL consistency groups to atomically revalidate semantic predicates (e.g., workflow still `RUNNING`, task still `PENDING` or `RETRY_WAIT`, worker session still matching) during commit rather than relying solely on application-layer pre-reads.

---

## 4. Package Structure & Architectural Layering

The control plane is structured into four concentric layers following strict inward dependency rules:

```
src/nexusflow/
├── domain/                  # PURE DOMAIN CORE (Zero external framework imports)
│   ├── shared/              # Opaque IDs, ActivityType, JSON types, Time
│   ├── definitions/         # Candidate/Validated IWS, TaskDefinition, Graph
│   ├── dataflow/            # Named whole-value bindings, Input/Output models
│   ├── execution/           # Workflow, Task, Attempt entities & State Machines
│   ├── failures/            # Normalized FailureCause & Category enums
│   ├── services/            # Pure domain services (Readiness, Routing, Retries)
│   └── errors.py            # Domain-specific invariants & transition exceptions
│
├── application/             # ORCHESTRATION USE CASES (Coordinates Domain & Ports)
│   ├── definitions/         # RegisterDefinition, GetDefinition, ListDefinitions
│   ├── executions/          # StartExecution, CancelExecution, ReadExecutions
│   ├── workers/             # EstablishSession, Heartbeat, Poll, Claim, Start, Result
│   ├── scheduling/          # EvaluateReadiness, RediscoverRunnable, SettleDrains
│   ├── recovery/            # StartupReconciliation
│   ├── commands.py          # Application Command/Query DTOs
│   ├── results.py           # Typed Application Return DTOs
│   └── context.py           # SecurityContext, Principal, and Permissions
│
├── ports/                   # ABSTRACT INTERFACES & CONTRACTS (Protocols)
│   ├── persistence/         # ExecutionMutationPort, ExecutionQueryPort, DefinitionPort
│   ├── workers/             # WorkerSessionRegistryPort, WorkerTransportPort
│   ├── clock.py             # Clock Protocol
│   ├── random.py            # RandomSource Protocol
│   └── telemetry.py         # TelemetryPort (Non-authoritative diagnostics)
│
├── infrastructure/          # CONCRETE ADAPTERS (Deferred to LLD-02..09)
│   ├── persistence/         # SQLAlchemy 2.0 Async, asyncpg, Unit of Work
│   ├── worker_protocol/     # FastAPI worker endpoints, long-poll waiter
│   ├── observability/       # Structured JSON logger, Prometheus, OpenTelemetry
│   └── configuration/       # Pydantic v2 Settings Loader
│
├── interfaces/              # EXTERNAL INGRESS (Deferred to LLD-08)
│   ├── public_api/          # FastAPI REST endpoints, Pydantic DTOs
│   └── worker_api/          # FastAPI Worker protocol routes
│
└── runtime/                 # PROCESS COMPOSITION & LIFECYCLE (Deferred to LLD-09)
    ├── bootstrap/           # Container initialization, N=1 assertion, recovery runner
    ├── background/          # Scheduler loop, liveness sweeper, deadline watcher
    └── shutdown/            # Process drain coordinator
```

---

## 5. Dependency Rules

The dependency graph strictly flows inward:

$$\text{Interfaces} \longrightarrow \text{Application} \longrightarrow \text{Domain Core} \longleftarrow \text{Infrastructure (implements Ports)}$$

```
+───────────────────────────────────────────────────────────────────+
|                     Interfaces / Frameworks                       |
|         (FastAPI routers, Public API DTOs, Worker DTOs)          |
+───────────────────────────────────────────────────────────────────+
                                  │
                                  ▼
+───────────────────────────────────────────────────────────────────+
|                      Application Use Cases                        |
|        (Coordinates pure domain logic with persistence ports)     |
+───────────────────────────────────────────────────────────────────+
                 │                                    │
                 ▼                                    ▼
+─────────────────────────────────+   +─────────────────────────────+
|           Domain Core           |   |       Port Contracts        |
|  (Entities, Values, Semantics)  |   |    (Protocols / ABCs)       |
+─────────────────────────────────+   +─────────────────────────────+
                 ▲                                    ▲
                 │ (Forbidden)                        │ (Implements)
+───────────────────────────────────────────────────────────────────+
|                     Infrastructure Adapters                       |
|       (SQLAlchemy, asyncpg, OTel, Alembic, Pydantic Settings)     |
+───────────────────────────────────────────────────────────────────+
```

### Prohibited Dependencies:
1. **Domain Core** must never import:
   - `fastapi`, `starlette`, or ASGI utilities.
   - `sqlalchemy`, `asyncpg`, or SQL drivers.
   - `pydantic` or `pydantic_settings`.
   - `prometheus_client` or `opentelemetry`.
   - `ports` or `infrastructure`.
2. **Application Layer** must never import:
   - `infrastructure` concrete classes directly (wired via Dependency Injection at composition root).
   - HTTP request/response objects (e.g., `fastapi.Request`).
   - Raw SQL or ORM entities.
3. **Ports** must only depend on `domain` types and Python standard library types.

---

## 6. Core Value Objects & Shared Types

All value objects are immutable, hashable, and type-safe.

```python
from __future__ import annotations
from dataclasses import dataclass
from uuid import UUID
from typing import TypeAlias, Union, Mapping, Sequence, Protocol
from types import MappingProxyType
import math

# =====================================================================
# 1. Opaque Typed Identifiers (UUIDv4 Underneath)
# =====================================================================

@dataclass(frozen=True, slots=True)
class DefinitionId:
    value: UUID
    def __str__(self) -> str: return str(self.value)

@dataclass(frozen=True, slots=True)
class WorkflowExecutionId:
    value: UUID
    def __str__(self) -> str: return str(self.value)

@dataclass(frozen=True, slots=True)
class TaskExecutionId:
    value: UUID
    def __str__(self) -> str: return str(self.value)

@dataclass(frozen=True, slots=True)
class AttemptId:
    value: UUID
    def __str__(self) -> str: return str(self.value)

@dataclass(frozen=True, slots=True)
class WorkerSessionId:
    """Runtime incarnation identity of a worker process. Non-secret correlation ID."""
    value: UUID
    def __str__(self) -> str: return str(self.value)

@dataclass(frozen=True, slots=True)
class HistoryEntryId:
    value: UUID
    def __str__(self) -> str: return str(self.value)

# =====================================================================
# 2. Local Semantic Identifiers & Canonical Types
# =====================================================================

@dataclass(frozen=True, slots=True)
class TaskDefinitionId:
    """Local semantic task identity within a workflow specification (e.g., 'validate_payment')."""
    value: str
    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 256:
            raise ValueError("TaskDefinitionId must be a non-empty string <= 256 characters.")
        if any(c in self.value for c in "\x00\r\n\t"):
            raise ValueError("TaskDefinitionId contains prohibited control characters.")
    def __str__(self) -> str: return self.value

@dataclass(frozen=True, slots=True)
class ActivityType:
    """Canonical activity string matched via exact byte-for-byte string equality."""
    name: str
    def __post_init__(self) -> None:
        if not self.name or len(self.name) > 256:
            raise ValueError("ActivityType must be a non-empty string <= 256 characters.")
        if any(c in self.name for c in "\x00\r\n\t"):
            raise ValueError("ActivityType contains prohibited control characters.")
    def __str__(self) -> str: return self.name

# =====================================================================
# 3. Application Idempotency Identifiers
# =====================================================================

@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    """Client-provided idempotency token."""
    value: str
    def __post_init__(self) -> None:
        if not self.value or len(self.value) > 256:
            raise ValueError("IdempotencyKey must be between 1 and 256 characters.")

@dataclass(frozen=True, slots=True)
class RequestFingerprint:
    """Cryptographic SHA-256 digest of normalized request payload to verify semantic equivalence."""
    digest: str
    def __post_init__(self) -> None:
        if len(self.digest) != 64:
            raise ValueError("RequestFingerprint must be a 64-character SHA-256 hex string.")

# =====================================================================
# 4. Strict Deeply Immutable JSON Domain Types & Canonicalization
# =====================================================================

JsonPrimitive: TypeAlias = Union[None, bool, int, float, str]
JsonArray: TypeAlias = tuple["JsonValue", ...]
JsonObject: TypeAlias = Mapping[str, "JsonValue"]
JsonValue: TypeAlias = Union[JsonPrimitive, JsonArray, JsonObject]

def freeze_json(value: object) -> JsonValue:
    """
    Recursively canonicalizes and deeply freezes an arbitrary JSON-compatible structure into an
    immutable representation (tuples for arrays, MappingProxyType for objects).
    Rejects NaN, Infinity, bytes, datetimes, and unsupported Python objects.
    """
    if value is None:
        return None
    elif isinstance(value, bool):
        return value
    elif isinstance(value, int):
        return value
    elif isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            raise ValueError(f"Non-finite JSON numbers (NaN, Infinity) are prohibited: {value}")
        return value
    elif isinstance(value, str):
        return value
    elif isinstance(value, (list, tuple)):
        return tuple(freeze_json(item) for item in value)
    elif isinstance(value, (dict, Mapping)):
        frozen_map = {str(k): freeze_json(v) for k, v in value.items()}
        return MappingProxyType(frozen_map)
    else:
        raise TypeError(f"Object of type '{type(value).__name__}' is not JSON-compatible: {value!r}")

# =====================================================================
# 5. Attempt Ordinal (Monotonic Positive Integer >= 1)
# =====================================================================

@dataclass(frozen=True, slots=True)
class AttemptOrdinal:
    """1-based monotonic attempt counter. Physical allocation is managed by persistence (LLD-02)."""
    value: int
    def __post_init__(self) -> None:
        if self.value < 1:
            raise ValueError(f"AttemptOrdinal must be >= 1, got {self.value}")
```

---

## 7. Output & Failure Models

### 7.1 Distinguishable Output Presence Model
To satisfy ADR-010 and eliminate ambiguity between uncommitted output and committed JSON `null`:

```python
from abc import ABC

@dataclass(frozen=True, slots=True)
class OutputPresence(ABC):
    """Sum type distinguishing absent/uncommitted output from committed output."""
    pass

@dataclass(frozen=True, slots=True)
class OutputAbsent(OutputPresence):
    """Output is not yet committed (entity is in progress, failed, or cancelled)."""
    pass

@dataclass(frozen=True, slots=True)
class OutputCommitted(OutputPresence):
    """Output is durably committed. The value may be any JsonValue, including None (JSON null)."""
    value: JsonValue
```

### 7.2 Normalized Failure Taxonomy & Worker Reporting Model
Categorizes causes per ADR-018 without leaking exceptions or tracebacks:

```python
from enum import StrEnum

class FailureCategory(StrEnum):
    CLIENT_INPUT = "CLIENT_INPUT"
    VALIDATION = "VALIDATION"
    DOMAIN_CONFLICT = "DOMAIN_CONFLICT"
    DOMAIN_EXECUTION = "DOMAIN_EXECUTION"
    TIME_BASED = "TIME_BASED"
    WORKER_AVAILABILITY = "WORKER_AVAILABILITY"
    SYSTEM_TRANSIENT = "SYSTEM_TRANSIENT"
    SYSTEM_PERMANENT = "SYSTEM_PERMANENT"
    CONCURRENCY = "CONCURRENCY"
    INTEGRITY = "INTEGRITY"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"
    SECURITY = "SECURITY"

@dataclass(frozen=True, slots=True)
class WorkerFailureReport:
    """Untrusted raw observation submitted by a worker. Worker never decides retryability."""
    error_code: str
    error_message: str
    details: JsonObject | None = None

@dataclass(frozen=True, slots=True)
class FailureCause:
    """Domain-safe normalized failure cause attached to failed attempts and tasks."""
    category: FailureCategory
    code: str
    message: str
    details: JsonObject | None = None
```

---

## 8. Definition Domain & Specification Types

### 8.1 Data Flow Binding Model (Whole-Value Only per ADR-010)
Per ADR-010, both task inputs and workflow outputs support whole-value bindings from approved sources:

```python
# --- Task Input Bindings (ADR-010 Section 10.3) ---
@dataclass(frozen=True, slots=True)
class LiteralBinding:
    """Injects a static, immutable JSON-compatible literal."""
    value: JsonValue

@dataclass(frozen=True, slots=True)
class WorkflowInputBinding:
    """Binds the entire immutable workflow execution input value."""
    pass

@dataclass(frozen=True, slots=True)
class TaskOutputBinding:
    """Binds the entire committed output of a direct upstream dependency task."""
    upstream_task_id: TaskDefinitionId

InputBinding: TypeAlias = Union[LiteralBinding, WorkflowInputBinding, TaskOutputBinding]

# --- Workflow Output Bindings (ADR-010 Section 10.9) ---
@dataclass(frozen=True, slots=True)
class WorkflowTaskOutputBinding:
    """References the authoritative whole-value output of any task defined in the workflow."""
    source_task_id: TaskDefinitionId

@dataclass(frozen=True, slots=True)
class WorkflowInputPassthroughBinding:
    """Passes through the entire immutable workflow execution input value."""
    pass

@dataclass(frozen=True, slots=True)
class WorkflowLiteralOutputBinding:
    """Binds a static literal value to a named workflow output."""
    value: JsonValue

WorkflowOutputBinding: TypeAlias = Union[
    WorkflowTaskOutputBinding,
    WorkflowInputPassthroughBinding,
    WorkflowLiteralOutputBinding
]
```

### 8.2 Task Definition
```python
@dataclass(frozen=True, slots=True)
class TaskDefinition:
    id: TaskDefinitionId
    activity_type: ActivityType
    dependencies: frozenset[TaskDefinitionId]
    input_bindings: Mapping[str, InputBinding]
    max_attempts: int

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.max_attempts}")
```

### 8.3 Candidate vs. Validated IWS Types
A `CandidateWorkflowSpec` represents unvalidated parser output. Semantic validation (ADR-004) promotes it to an immutable `ValidatedWorkflowSpec`:

```python
@dataclass(frozen=True, slots=True)
class CandidateWorkflowSpec:
    """
    Unvalidated candidate specification from YAML parser. Capable of representing invalid references,
    missing dependencies, self-loops, and cycles prior to ADR-004 semantic validation.
    """
    workflow_name: str
    tasks: Sequence[TaskDefinition]
    output_bindings: Mapping[str, WorkflowOutputBinding] | None = None

@dataclass(frozen=True, slots=True)
class ValidatedWorkflowSpec:
    """
    Immutable, semantically validated workflow specification.
    Guaranteed by ADR-004 to be an acyclic DAG with all dependencies and whole-value bindings valid.
    """
    workflow_name: str
    tasks: Mapping[TaskDefinitionId, TaskDefinition]
    output_bindings: Mapping[str, WorkflowOutputBinding]  # Empty map indicates JSON null output

    def get_task(self, task_id: TaskDefinitionId) -> TaskDefinition:
        if task_id not in self.tasks:
            raise KeyError(f"TaskDefinitionId '{task_id}' does not exist in specification.")
        return self.tasks[task_id]
```

### 8.4 Canonical Graph Representation
An immutable, bidirectional adjacency DAG constructed in $O(V+E)$:

```python
@dataclass(frozen=True, slots=True)
class CanonicalGraph:
    """Sparse bidirectional adjacency DAG derived strictly from ValidatedWorkflowSpec."""
    nodes: frozenset[TaskDefinitionId]
    dependencies: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]  # Incoming edges
    dependents: Mapping[TaskDefinitionId, frozenset[TaskDefinitionId]]    # Outgoing edges

    @classmethod
    def from_spec(cls, spec: ValidatedWorkflowSpec) -> CanonicalGraph:
        nodes = frozenset(spec.tasks.keys())
        dependencies: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}
        dependents: dict[TaskDefinitionId, set[TaskDefinitionId]] = {t: set() for t in nodes}

        for task_id, task in spec.tasks.items():
            for dep_id in task.dependencies:
                dependencies[task_id].add(dep_id)
                dependents[dep_id].add(task_id)

        return cls(
            nodes=nodes,
            dependencies={k: frozenset(v) for k, v in dependencies.items()},
            dependents={k: frozenset(v) for k, v in dependents.items()},
        )

    def get_upstream_dependencies(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependencies.get(task_id, frozenset())

    def get_downstream_dependents(self, task_id: TaskDefinitionId) -> frozenset[TaskDefinitionId]:
        return self.dependents.get(task_id, frozenset())
```

---

## 9. Execution Domain Entities & State Machines

### 9.1 Lifecycle State Enums

```python
class WorkflowState(StrEnum):
    INITIALIZING = "INITIALIZING"
    RUNNING = "RUNNING"
    FAILING = "FAILING"
    CANCELLING = "CANCELLING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class TaskState(StrEnum):
    PENDING = "PENDING"
    RUNNABLE = "RUNNABLE"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

class AttemptState(StrEnum):
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
```

### 9.2 Transition Result Types

```python
from typing import Generic, TypeVar
import dataclasses

E = TypeVar("E")

class SemanticEvent(StrEnum):
    # Workflow Events
    WORKFLOW_INITIALIZED = "WORKFLOW_INITIALIZED"
    WORKFLOW_STARTED = "WORKFLOW_STARTED"
    WORKFLOW_INIT_FAILED = "WORKFLOW_INIT_FAILED"
    WORKFLOW_FAILING_BEGUN = "WORKFLOW_FAILING_BEGUN"
    WORKFLOW_CANCELLING_BEGUN = "WORKFLOW_CANCELLING_BEGUN"
    WORKFLOW_SUCCEEDED = "WORKFLOW_SUCCEEDED"
    WORKFLOW_FAILED = "WORKFLOW_FAILED"
    WORKFLOW_CANCELLED = "WORKFLOW_CANCELLED"
    
    # Task Events
    TASK_INITIAL_RUNNABLE = "TASK_INITIAL_RUNNABLE"
    TASK_RETRY_RUNNABLE = "TASK_RETRY_RUNNABLE"
    TASK_RUNNING_STARTED = "TASK_RUNNING_STARTED"
    TASK_SUCCEEDED = "TASK_SUCCEEDED"
    TASK_RETRY_WAITING = "TASK_RETRY_WAITING"
    TASK_FAILED = "TASK_FAILED"
    TASK_DRAIN_CANCELLED = "TASK_DRAIN_CANCELLED"
    TASK_ATTEMPT_CANCELLED = "TASK_ATTEMPT_CANCELLED"

    # Attempt Events
    ATTEMPT_START_OBSERVED = "ATTEMPT_START_OBSERVED"
    ATTEMPT_SUCCEEDED = "ATTEMPT_SUCCEEDED"
    ATTEMPT_FAILED = "ATTEMPT_FAILED"
    ATTEMPT_CANCELLED_SETTLED = "ATTEMPT_CANCELLED_SETTLED"

@dataclass(frozen=True, slots=True)
class TransitionApplied(Generic[E]):
    entity: E
    event: SemanticEvent

@dataclass(frozen=True, slots=True)
class TransitionRejected:
    reason: str

@dataclass(frozen=True, slots=True)
class TransitionNoOp(Generic[E]):
    entity: E
    reason: str

TransitionResult: TypeAlias = Union[TransitionApplied[E], TransitionRejected, TransitionNoOp[E]]
```

### 9.3 WorkflowExecution Entity & Transitions

```python
from datetime import datetime

@dataclass(frozen=True, slots=True)
class WorkflowExecution:
    id: WorkflowExecutionId
    definition_id: DefinitionId
    state: WorkflowState
    revision: int
    workflow_input: JsonValue
    output: OutputPresence
    failure_cause: FailureCause | None = None
    created_at_utc: datetime | None = None
    updated_at_utc: datetime | None = None

    def is_terminal(self) -> bool:
        return self.state in (WorkflowState.SUCCEEDED, WorkflowState.FAILED, WorkflowState.CANCELLED)

    def is_draining(self) -> bool:
        return self.state in (WorkflowState.FAILING, WorkflowState.CANCELLING)

# --- Pure Workflow State Transitions ---

def begin_running(wf: WorkflowExecution) -> TransitionResult[WorkflowExecution]:
    if wf.state != WorkflowState.INITIALIZING:
        return TransitionRejected(f"Cannot transition to RUNNING from state '{wf.state}'")
    updated = dataclasses.replace(wf, state=WorkflowState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_STARTED)

def fail_initialization(wf: WorkflowExecution, cause: FailureCause) -> TransitionResult[WorkflowExecution]:
    """Definitive, unrecoverable execution-specific semantic initialization failure (ADR-006)."""
    if wf.state != WorkflowState.INITIALIZING:
        return TransitionRejected(f"Cannot fail initialization from state '{wf.state}'")
    updated = dataclasses.replace(wf, state=WorkflowState.FAILED, failure_cause=cause)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_INIT_FAILED)

def begin_failing(wf: WorkflowExecution, cause: FailureCause) -> TransitionResult[WorkflowExecution]:
    if wf.state == WorkflowState.FAILING:
        return TransitionNoOp(entity=wf, reason="Workflow already FAILING")
    if wf.state != WorkflowState.RUNNING:
        return TransitionRejected(f"Cannot begin FAILING from state '{wf.state}'")
    updated = dataclasses.replace(wf, state=WorkflowState.FAILING, failure_cause=cause)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_FAILING_BEGUN)

def begin_cancellation(wf: WorkflowExecution) -> TransitionResult[WorkflowExecution]:
    if wf.state == WorkflowState.CANCELLING:
        return TransitionNoOp(entity=wf, reason="Workflow already CANCELLING")
    if wf.state not in (WorkflowState.INITIALIZING, WorkflowState.RUNNING):
        return TransitionRejected(f"Cannot cancel workflow in state '{wf.state}'")
    updated = dataclasses.replace(wf, state=WorkflowState.CANCELLING)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_CANCELLING_BEGUN)

def complete_success(wf: WorkflowExecution, output_value: JsonValue) -> TransitionResult[WorkflowExecution]:
    if wf.state != WorkflowState.RUNNING:
        return TransitionRejected(f"Cannot succeed workflow in state '{wf.state}'")
    updated = dataclasses.replace(
        wf,
        state=WorkflowState.SUCCEEDED,
        output=OutputCommitted(value=output_value),
        failure_cause=None
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_SUCCEEDED)

def complete_failure(wf: WorkflowExecution) -> TransitionResult[WorkflowExecution]:
    """Terminal settlement after all expected tasks have reached a terminal state."""
    if wf.state != WorkflowState.FAILING:
        return TransitionRejected(f"Cannot complete failure from state '{wf.state}'; must be FAILING")
    updated = dataclasses.replace(wf, state=WorkflowState.FAILED)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_FAILED)

def complete_cancellation(wf: WorkflowExecution) -> TransitionResult[WorkflowExecution]:
    """Terminal settlement after all expected tasks have reached a terminal state."""
    if wf.state != WorkflowState.CANCELLING:
        return TransitionRejected(f"Cannot complete cancellation from state '{wf.state}'; must be CANCELLING")
    updated = dataclasses.replace(wf, state=WorkflowState.CANCELLED)
    return TransitionApplied(entity=updated, event=SemanticEvent.WORKFLOW_CANCELLED)
```

### 9.4 TaskExecution Entity & Transitions

```python
@dataclass(frozen=True, slots=True)
class TaskExecution:
    id: TaskExecutionId
    workflow_execution_id: WorkflowExecutionId
    task_definition_id: TaskDefinitionId
    state: TaskState
    revision: int
    stable_input: JsonObject | None  # Once established (RUNNABLE), remains immutable forever
    output: OutputPresence
    max_attempts: int
    retry_ready_at_utc: datetime | None = None
    terminal_failure_cause: FailureCause | None = None  # Populated ONLY on terminal FAILED

    def is_terminal(self) -> bool:
        return self.state in (TaskState.SUCCEEDED, TaskState.FAILED, TaskState.CANCELLED)

# --- Pure Task State Transitions ---

def mark_initially_runnable(task: TaskExecution, resolved_input: JsonObject) -> TransitionResult[TaskExecution]:
    """The ONLY path that materializes stable_input (PENDING -> RUNNABLE)."""
    if task.state != TaskState.PENDING:
        return TransitionRejected(f"Cannot mark initially RUNNABLE from state '{task.state}'")
    updated = dataclasses.replace(
        task,
        state=TaskState.RUNNABLE,
        stable_input=resolved_input,
        retry_ready_at_utc=None
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_INITIAL_RUNNABLE)

def mark_retry_ready(task: TaskExecution, now_utc: datetime) -> TransitionResult[TaskExecution]:
    """
    Transitions RETRY_WAIT -> RUNNABLE. Strictly preserves existing stable_input.
    Note: now_utc check in domain is advisory; authoritative verification is performed at persistence commit.
    """
    if task.state != TaskState.RETRY_WAIT:
        return TransitionRejected(f"Cannot mark retry ready from state '{task.state}'")
    if task.retry_ready_at_utc is not None and now_utc < task.retry_ready_at_utc:
        return TransitionRejected(f"Retry timer has not yet elapsed: {task.retry_ready_at_utc}")
    assert task.stable_input is not None, "Invariant fault: stable_input must be present during retry"
    updated = dataclasses.replace(
        task,
        state=TaskState.RUNNABLE,
        retry_ready_at_utc=None
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_RETRY_RUNNABLE)

def mark_running(task: TaskExecution) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNABLE:
        return TransitionRejected(f"Cannot mark RUNNING from state '{task.state}'")
    updated = dataclasses.replace(task, state=TaskState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_RUNNING_STARTED)

def mark_succeeded(task: TaskExecution, output_value: JsonValue) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNING:
        return TransitionRejected(f"Cannot mark SUCCEEDED from state '{task.state}'")
    updated = dataclasses.replace(
        task,
        state=TaskState.SUCCEEDED,
        output=OutputCommitted(value=output_value)
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_SUCCEEDED)

def schedule_retry(task: TaskExecution, ready_at_utc: datetime) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNING:
        return TransitionRejected(f"Cannot schedule retry from state '{task.state}'")
    updated = dataclasses.replace(
        task,
        state=TaskState.RETRY_WAIT,
        retry_ready_at_utc=ready_at_utc
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_RETRY_WAITING)

def mark_failed(task: TaskExecution, cause: FailureCause) -> TransitionResult[TaskExecution]:
    if task.state != TaskState.RUNNING:
        return TransitionRejected(f"Cannot mark FAILED from state '{task.state}'")
    updated = dataclasses.replace(
        task,
        state=TaskState.FAILED,
        terminal_failure_cause=cause
    )
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_FAILED)

def cancel_unstarted_task(task: TaskExecution) -> TransitionResult[TaskExecution]:
    """Settles unstarted sibling tasks to CANCELLED during workflow FAILING or CANCELLING."""
    if task.is_terminal():
        return TransitionNoOp(entity=task, reason="Task already terminal")
    if task.state not in (TaskState.PENDING, TaskState.RUNNABLE, TaskState.RETRY_WAIT):
        return TransitionRejected(f"Cannot cancel unstarted task in state '{task.state}'")
    updated = dataclasses.replace(task, state=TaskState.CANCELLED)
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_DRAIN_CANCELLED)

def settle_task_from_cancelled_attempt(task: TaskExecution) -> TransitionResult[TaskExecution]:
    """Transitions Task RUNNING -> CANCELLED upon authoritative cancellation settlement of active attempt."""
    if task.state != TaskState.RUNNING:
        return TransitionRejected(f"Cannot settle task cancellation from state '{task.state}'")
    updated = dataclasses.replace(task, state=TaskState.CANCELLED)
    return TransitionApplied(entity=updated, event=SemanticEvent.TASK_ATTEMPT_CANCELLED)
```

### 9.5 ExecutionAttempt Entity & Transitions

```python
@dataclass(frozen=True, slots=True)
class ExecutionAttempt:
    id: AttemptId
    task_execution_id: TaskExecutionId
    attempt_ordinal: AttemptOrdinal
    worker_session_id: WorkerSessionId
    state: AttemptState
    revision: int
    start_deadline_utc: datetime
    execution_timeout_utc: datetime | None = None
    cancellation_deadline_utc: datetime | None = None
    terminal_failure_cause: FailureCause | None = None

    def is_terminal(self) -> bool:
        return self.state in (AttemptState.SUCCEEDED, AttemptState.FAILED, AttemptState.CANCELLED)

# --- Pure Attempt State Transitions ---

def observe_attempt_start(attempt: ExecutionAttempt, now_utc: datetime) -> TransitionResult[ExecutionAttempt]:
    if attempt.state != AttemptState.CLAIMED:
        return TransitionRejected(f"Cannot transition to RUNNING from state '{attempt.state}'")
    if now_utc > attempt.start_deadline_utc:
        return TransitionRejected(f"Start deadline expired at {attempt.start_deadline_utc}")
    updated = dataclasses.replace(attempt, state=AttemptState.RUNNING)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_START_OBSERVED)

def complete_attempt_success(attempt: ExecutionAttempt) -> TransitionResult[ExecutionAttempt]:
    if attempt.state != AttemptState.RUNNING:
        return TransitionRejected(f"Cannot complete attempt in state '{attempt.state}'")
    updated = dataclasses.replace(attempt, state=AttemptState.SUCCEEDED)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_SUCCEEDED)

def complete_attempt_failure(attempt: ExecutionAttempt, cause: FailureCause) -> TransitionResult[ExecutionAttempt]:
    if attempt.state not in (AttemptState.CLAIMED, AttemptState.RUNNING):
        return TransitionRejected(f"Cannot fail attempt in state '{attempt.state}'")
    updated = dataclasses.replace(attempt, state=AttemptState.FAILED, terminal_failure_cause=cause)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_FAILED)

def settle_attempt_cancelled(attempt: ExecutionAttempt) -> TransitionResult[ExecutionAttempt]:
    """Authoritative logical cancellation settlement (worker ack or cancellation deadline expiry)."""
    if attempt.is_terminal():
        return TransitionNoOp(entity=attempt, reason="Attempt already terminal")
    updated = dataclasses.replace(attempt, state=AttemptState.CANCELLED)
    return TransitionApplied(entity=updated, event=SemanticEvent.ATTEMPT_CANCELLED_SETTLED)
```

---

## 10. Worker Session Domain Model

A `WorkerSession` represents ephemeral in-process coordination state ([ADR-008](../architecture/adr-008-worker-coordination-and-liveness.md)).

> [!IMPORTANT]
> **Security & Continuity Invariant:** `WorkerSessionId` is a runtime incarnation identity, **not** an authentication credential. Possession of a `WorkerSessionId` grants zero authorization on its own. Reconnecting an existing session requires establishing continuity of the surviving worker process under ADR-008, verified via session continuity proof handled at the worker-protocol/application boundary (LLD-05).

```python
@dataclass(frozen=True, slots=True)
class SessionContinuityProof:
    """Opaque verification token proving continuity of a surviving worker process."""
    token: str

@dataclass(frozen=True, slots=True)
class WorkerSession:
    session_id: WorkerSessionId
    capabilities: frozenset[ActivityType]
    last_heartbeat_utc: datetime  # Set exclusively by control-plane Clock
    accepting_new_work: bool

    def is_live(self, now_utc: datetime, liveness_timeout_seconds: float) -> bool:
        elapsed = (now_utc - self.last_heartbeat_utc).total_seconds()
        return elapsed <= liveness_timeout_seconds

    def is_eligible_for_routing(self, now_utc: datetime, liveness_timeout_seconds: float) -> bool:
        return self.accepting_new_work and self.is_live(now_utc, liveness_timeout_seconds)
```

---

## 11. Pure Domain Services

Domain services encapsulate multi-entity business rules without performing I/O or accessing databases:

### 11.1 Routing Compatibility Service ([ADR-009](../architecture/adr-009-task-routing.md))
```python
def is_worker_compatible(
    task_activity: ActivityType,
    worker_capabilities: frozenset[ActivityType]
) -> bool:
    """Exact byte-for-byte canonical string match."""
    return task_activity in worker_capabilities
```

### 11.2 Task Readiness Domain Service ([ADR-005](../architecture/adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-010](../architecture/adr-010-workflow-data-flow-and-parameter-passing.md))
```python
@dataclass(frozen=True, slots=True)
class TaskReadinessDecision(ABC):
    pass

@dataclass(frozen=True, slots=True)
class TaskReady(TaskReadinessDecision):
    resolved_input: JsonObject

@dataclass(frozen=True, slots=True)
class TaskNotReady(TaskReadinessDecision):
    reason: str

@dataclass(frozen=True, slots=True)
class TaskReadinessFault(TaskReadinessDecision):
    reason: str

def evaluate_task_readiness(
    task_def: TaskDefinition,
    workflow_state: WorkflowState,
    task_state: TaskState,
    workflow_input: JsonValue,
    upstream_tasks: Mapping[TaskDefinitionId, TaskExecution]
) -> TaskReadinessDecision:
    if workflow_state != WorkflowState.RUNNING:
        return TaskNotReady(f"Workflow state is '{workflow_state}', must be RUNNING")
    if task_state != TaskState.PENDING:
        return TaskNotReady(f"Task state is '{task_state}', must be PENDING")

    # 1. Verify upstream dependencies are terminal SUCCEEDED
    for dep_id in task_def.dependencies:
        if dep_id not in upstream_tasks:
            return TaskReadinessFault(f"Missing required upstream task '{dep_id}' in state snapshot")
        dep_task = upstream_tasks[dep_id]
        if dep_task.state != TaskState.SUCCEEDED:
            return TaskNotReady(f"Upstream task '{dep_id}' is in state '{dep_task.state}'")
        if not isinstance(dep_task.output, OutputCommitted):
            return TaskReadinessFault(f"Upstream task '{dep_id}' is SUCCEEDED but has uncommitted output")

    # 2. Resolve whole-value input bindings
    resolved_map: dict[str, JsonValue] = {}
    for param_name, binding in task_def.input_bindings.items():
        if isinstance(binding, LiteralBinding):
            resolved_map[param_name] = binding.value
        elif isinstance(binding, WorkflowInputBinding):
            resolved_map[param_name] = workflow_input
        elif isinstance(binding, TaskOutputBinding):
            upstream_task = upstream_tasks[binding.upstream_task_id]
            assert isinstance(upstream_task.output, OutputCommitted)
            resolved_map[param_name] = upstream_task.output.value

    return TaskReady(resolved_input=freeze_json(resolved_map))  # Guaranteed deeply immutable
```

### 11.3 Engine-Owned Retry Policy Decision Service ([ADR-007](../architecture/adr-007-task-execution-lifecycle-and-attempt-model.md), [ADR-018](../architecture/adr-018-error-handling-philosophy.md))
```python
@dataclass(frozen=True, slots=True)
class RetryDecision(ABC):
    pass

@dataclass(frozen=True, slots=True)
class RetryAllowed(RetryDecision):
    """Signals that a retry is semantically eligible. Ordinal allocation belongs to persistence (LLD-02)."""
    pass

@dataclass(frozen=True, slots=True)
class RetryForbidden(RetryDecision):
    reason: str

def evaluate_retry_eligibility(
    workflow_state: WorkflowState,
    used_attempts: int,
    max_attempts: int,
    is_retryable_classification: bool
) -> RetryDecision:
    """Engine-owned retry determination. Untrusted workers never control retryability."""
    if workflow_state != WorkflowState.RUNNING:
        return RetryForbidden(f"Workflow state is '{workflow_state}'; retries prohibited in non-RUNNING workflows")
    if not is_retryable_classification:
        return RetryForbidden("Failure cause is classified as non-retryable by control-plane policy")
    if used_attempts >= max_attempts:
        return RetryForbidden(f"Attempt budget exhausted ({used_attempts} >= {max_attempts})")

    return RetryAllowed()
```

### 11.4 Named Workflow Output Resolution Service ([ADR-010](../architecture/adr-010-workflow-data-flow-and-parameter-passing.md))
```python
def resolve_workflow_output(
    output_bindings: Mapping[str, WorkflowOutputBinding],
    workflow_input: JsonValue,
    tasks: Mapping[TaskDefinitionId, TaskExecution]
) -> JsonValue:
    """
    Resolves authoritative workflow output.
    - If no output bindings declared: returns explicit JSON null.
    - If named output bindings declared: returns a JSON object mapping output names to resolved whole values.
    """
    if not output_bindings:
        return None  # Evaluates to explicit JSON null

    resolved_outputs: dict[str, JsonValue] = {}
    for output_name, binding in output_bindings.items():
        if isinstance(binding, WorkflowLiteralOutputBinding):
            resolved_outputs[output_name] = binding.value
        elif isinstance(binding, WorkflowInputPassthroughBinding):
            resolved_outputs[output_name] = workflow_input
        elif isinstance(binding, WorkflowTaskOutputBinding):
            source_id = binding.source_task_id
            if source_id not in tasks:
                raise ValueError(f"Output source task '{source_id}' not found in tasks")
            source_task = tasks[source_id]
            if not isinstance(source_task.output, OutputCommitted):
                raise ValueError(f"Output source task '{source_id}' does not have committed output")
            resolved_outputs[output_name] = source_task.output.value

    return freeze_json(resolved_outputs)
```

---

## 12. Application Context & Security

Security authentication is separated from attempt execution authority ([ADR-022](../architecture/adr-022-security-architecture.md)):

```python
class PrincipalType(StrEnum):
    PUBLIC_CLIENT = "PUBLIC_CLIENT"
    WORKER = "WORKER"

class PublicPermission(StrEnum):
    DEFINITIONS_READ = "definitions:read"
    DEFINITIONS_WRITE = "definitions:write"
    EXECUTIONS_READ = "executions:read"
    EXECUTIONS_START = "executions:start"
    EXECUTIONS_CANCEL = "executions:cancel"

@dataclass(frozen=True, slots=True)
class SecurityContext:
    principal_id: str
    principal_type: PrincipalType
    permissions: frozenset[PublicPermission]

    def has_permission(self, permission: PublicPermission) -> bool:
        return permission in self.permissions
```

*(Note: Internal background tasks and recovery runners execute as trusted control-plane components without fabricating caller security contexts).*

---

## 13. Application Commands & Results

Application use cases receive typed commands and return typed DTOs:

```python
# =====================================================================
# Application Commands
# =====================================================================

@dataclass(frozen=True, slots=True)
class RegisterDefinitionCommand:
    raw_yaml: str
    idempotency_key: IdempotencyKey | None
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class StartExecutionCommand:
    definition_id: DefinitionId
    input_payload: JsonValue
    idempotency_key: IdempotencyKey | None
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class CancelExecutionCommand:
    workflow_execution_id: WorkflowExecutionId
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class EstablishWorkerSessionCommand:
    capabilities: frozenset[ActivityType]
    existing_session_id: WorkerSessionId | None  # Populated on reconnect
    continuity_proof: SessionContinuityProof | None  # Required if reconnecting
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class PollForCandidateCommand:
    worker_session_id: WorkerSessionId
    timeout_seconds: float
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class CommitOwnershipCommand:
    task_execution_id: TaskExecutionId
    worker_session_id: WorkerSessionId
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class ObserveExecutionStartCommand:
    attempt_id: AttemptId
    worker_session_id: WorkerSessionId
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class ProcessWorkerSuccessCommand:
    attempt_id: AttemptId
    worker_session_id: WorkerSessionId
    output_payload: JsonValue
    security_context: SecurityContext

@dataclass(frozen=True, slots=True)
class ProcessWorkerFailureCommand:
    attempt_id: AttemptId
    worker_session_id: WorkerSessionId
    failure_report: WorkerFailureReport  # Untrusted observation
    security_context: SecurityContext

# =====================================================================
# Application Results
# =====================================================================

@dataclass(frozen=True, slots=True)
class DefinitionRegisteredResult:
    definition_id: DefinitionId
    workflow_name: str

@dataclass(frozen=True, slots=True)
class ExecutionAcceptedResult:
    workflow_execution_id: WorkflowExecutionId
    state: WorkflowState  # Returned as INITIALIZING or RUNNING

@dataclass(frozen=True, slots=True)
class CancellationAcceptedResult:
    workflow_execution_id: WorkflowExecutionId
    state: WorkflowState

@dataclass(frozen=True, slots=True)
class WorkerSessionEstablishedResult:
    worker_session_id: WorkerSessionId
    heartbeat_interval_seconds: float

@dataclass(frozen=True, slots=True)
class CandidateOfferedResult:
    task_execution_id: TaskExecutionId
    activity_type: ActivityType

@dataclass(frozen=True, slots=True)
class OwnershipCommittedResult:
    attempt_id: AttemptId
    task_execution_id: TaskExecutionId
    attempt_ordinal: int
    task_input: JsonObject
    start_deadline_utc: datetime

@dataclass(frozen=True, slots=True)
class CallbackSuccessResult(ABC): pass

@dataclass(frozen=True, slots=True)
class CallbackCommitted(CallbackSuccessResult): pass

@dataclass(frozen=True, slots=True)
class CallbackDuplicateAcknowledged(CallbackSuccessResult):
    reason: str

@dataclass(frozen=True, slots=True)
class OperationAcknowledgedResult:
    status: str = "OK"
```

---

## 14. Persistence Port Contracts (Consistency Groups)

Persistence ports are organized around **multi-entity transactional consistency groups** with explicit anti-TOCTOU semantic predicates ([ADR-011](../architecture/adr-011-state-persistence.md), [ADR-013](../architecture/adr-013-consistency-and-concurrency.md)):

```python
from typing import Protocol

# =====================================================================
# Transaction Result Status & Definition Persistence Outcome
# =====================================================================

class CommitStatus(StrEnum):
    COMMITTED = "COMMITTED"
    OCC_CONFLICT = "OCC_CONFLICT"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"

@dataclass(frozen=True, slots=True)
class CommitOutcome:
    status: CommitStatus
    message: str | None = None

class RegistrationStatus(StrEnum):
    CREATED = "CREATED"
    IDEMPOTENT_MATCH = "IDEMPOTENT_MATCH"
    IDEMPOTENCY_CONFLICT = "IDEMPOTENCY_CONFLICT"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"

@dataclass(frozen=True, slots=True)
class RegistrationOutcome:
    status: RegistrationStatus
    definition_id: DefinitionId | None
    message: str | None = None

# =====================================================================
# Execution Mutation Port (Transactional Consistency Groups)
# =====================================================================

class ExecutionMutationPort(Protocol):
    """Encapsulates atomic state-mutation consistency groups."""

    async def commit_workflow_creation(
        self,
        execution: WorkflowExecution,
        idempotency_key: IdempotencyKey | None,
        fingerprint: RequestFingerprint | None
    ) -> CommitOutcome:
        """Group: Workflow Creation (INITIALIZING). Records WorkflowExecutionCreated history."""
        ...

    async def commit_task_population(
        self,
        workflow_id: WorkflowExecutionId,
        tasks: Sequence[TaskExecution]
    ) -> CommitOutcome:
        """Group: Task Population. Idempotently inserts PENDING tasks."""
        ...

    async def commit_initialization_complete(
        self,
        workflow_id: WorkflowExecutionId,
        expected_revision: int
    ) -> CommitOutcome:
        """
        Group: Initialization Complete (INITIALIZING -> RUNNING).
        Predicate: Workflow must be INITIALIZING and complete expected task set exists.
        """
        ...

    async def commit_task_readiness(
        self,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        workflow_id: WorkflowExecutionId,
        stable_input: JsonObject
    ) -> CommitOutcome:
        """
        Group: Input Readiness (PENDING -> RUNNABLE).
        Predicate: Workflow MUST be RUNNING; Task MUST be PENDING; expected_task_revision matches.
        """
        ...

    async def commit_retry_ready(
        self,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        workflow_id: WorkflowExecutionId,
        now_utc: datetime
    ) -> CommitOutcome:
        """
        Group: Retry Readiness (RETRY_WAIT -> RUNNABLE).
        Predicate: Workflow MUST be RUNNING; Task MUST be RETRY_WAIT; retry_ready_at_utc <= now_utc;
        preserves existing stable_input; creates no Attempt; allocates no ordinal.
        """
        ...

    async def commit_attempt_ownership(
        self,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        workflow_id: WorkflowExecutionId,
        worker_session_id: WorkerSessionId,
        new_attempt_id: AttemptId,
        start_deadline_utc: datetime
    ) -> CommitOutcome:
        """
        Group: Attempt Ownership (RUNNABLE -> RUNNING, Attempt CLAIMED).
        Predicate: Workflow MUST be RUNNING; Task MUST be RUNNABLE; no active Attempt exists.
        Persistence allocates next monotonic AttemptOrdinal.
        """
        ...

    async def commit_worker_execution_start(
        self,
        attempt_id: AttemptId,
        worker_session_id: WorkerSessionId,
        expected_attempt_revision: int
    ) -> CommitOutcome:
        """
        Group: Execution Start from Worker Callback.
        Predicate: Attempt MUST be CLAIMED; worker_session_id MUST match; deadline valid.
        """
        ...

    async def commit_worker_task_success(
        self,
        attempt_id: AttemptId,
        worker_session_id: WorkerSessionId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        output: OutputCommitted
    ) -> CommitOutcome:
        """
        Group: Task Success + Output from Worker Callback.
        Predicate: Attempt MUST be RUNNING; worker_session_id MUST match; task MUST be RUNNING.
        """
        ...

    async def commit_retry_scheduling(
        self,
        attempt_id: AttemptId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        workflow_id: WorkflowExecutionId,
        ready_at_utc: datetime,
        attempt_cause: FailureCause
    ) -> CommitOutcome:
        """
        Group: Retry Scheduling (Attempt -> FAILED, Task -> RETRY_WAIT).
        Predicate: Workflow MUST be RUNNING; Attempt and Task MUST be RUNNING.
        """
        ...

    async def commit_definitive_task_failure(
        self,
        attempt_id: AttemptId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        terminal_cause: FailureCause
    ) -> CommitOutcome:
        """Group A: Definitive Task Failure (Attempt & Task -> FAILED)."""
        ...

    async def commit_internal_attempt_timeout_or_loss(
        self,
        attempt_id: AttemptId,
        expected_attempt_revision: int,
        cause: FailureCause
    ) -> CommitOutcome:
        """Internal control-plane settlement for expired deadlines or lost worker sessions."""
        ...

    async def commit_workflow_failure_direction(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int,
        cause: FailureCause
    ) -> CommitOutcome:
        """Group B: Workflow Failure Direction (RUNNING -> FAILING under OCC). First direction wins."""
        ...

    async def commit_workflow_cancellation_direction(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int
    ) -> CommitOutcome:
        """Group: Workflow Cancellation Direction (INITIALIZING/RUNNING -> CANCELLING under OCC)."""
        ...

    async def commit_drain_task_cancellation(
        self,
        task_id: TaskExecutionId,
        expected_task_revision: int
    ) -> CommitOutcome:
        """Settles unstarted sibling task (PENDING, RUNNABLE, RETRY_WAIT) to CANCELLED during drain."""
        ...

    async def commit_workflow_success(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int,
        output: OutputCommitted
    ) -> CommitOutcome:
        """
        Group: Workflow Success Settlement.
        Predicate: Workflow MUST be RUNNING; all expected tasks MUST be SUCCEEDED.
        """
        ...

    async def commit_workflow_failure(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int
    ) -> CommitOutcome:
        """
        Group: Workflow Failure Settlement.
        Predicate: Workflow MUST be FAILING; all expected tasks MUST be terminal.
        """
        ...

    async def commit_workflow_cancellation(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int
    ) -> CommitOutcome:
        """
        Group: Workflow Cancellation Settlement.
        Predicate: Workflow MUST be CANCELLING; all expected tasks MUST be terminal.
        """
        ...
```

### 14.2 Execution Query & Definition Persistence Ports

```python
@dataclass(frozen=True, slots=True)
class PageParams:
    page_size: int
    cursor: str | None = None

@dataclass(frozen=True, slots=True)
class Page(Generic[E]):
    items: Sequence[E]
    next_cursor: str | None

class ExecutionQueryPort(Protocol):
    """Read queries for state inspection and recovery sweeps."""
    async def get_workflow(self, workflow_id: WorkflowExecutionId) -> WorkflowExecution | None: ...
    async def get_task(self, task_id: TaskExecutionId) -> TaskExecution | None: ...
    async def get_attempt(self, attempt_id: AttemptId) -> ExecutionAttempt | None: ...
    async def list_tasks_for_workflow(self, workflow_id: WorkflowExecutionId) -> Sequence[TaskExecution]: ...
    async def list_active_workflows_page(self, params: PageParams) -> Page[WorkflowExecution]: ...
    async def list_runnable_tasks_batch(self, limit: int) -> Sequence[TaskExecution]: ...
    async def list_retry_ready_tasks_batch(self, now_utc: datetime, limit: int) -> Sequence[TaskExecution]: ...
    async def list_expired_claimed_attempts_batch(self, now_utc: datetime, limit: int) -> Sequence[ExecutionAttempt]: ...

class DefinitionPersistencePort(Protocol):
    """Persistence operations for immutable registered specifications."""
    async def register_definition_if_absent(
        self,
        definition_id: DefinitionId,
        spec: ValidatedWorkflowSpec,
        idempotency_key: IdempotencyKey | None,
        fingerprint: RequestFingerprint | None
    ) -> RegistrationOutcome: ...

    async def get_definition(self, definition_id: DefinitionId) -> ValidatedWorkflowSpec | None: ...
```

---

## 15. Runtime & Infrastructure Ports

```python
class Clock(Protocol):
    """Abstract clock for deterministic time testing."""
    def now_utc(self) -> datetime:
        """Returns the current timezone-aware UTC datetime. Must raise ValueError if naive."""
        ...

class RandomSource(Protocol):
    """Abstract random provider for deterministic jitter calculation."""
    def uniform(self, a: float, b: float) -> float: ...

class WorkerSessionRegistryPort(Protocol):
    """In-process ephemeral worker session registry."""
    def register_new_session(self, capabilities: frozenset[ActivityType], now_utc: datetime) -> WorkerSessionId: ...
    def reconnect_session(self, session_id: WorkerSessionId, proof: SessionContinuityProof, now_utc: datetime) -> bool: ...
    def record_heartbeat(self, session_id: WorkerSessionId, now_utc: datetime) -> bool: ...
    def set_accepting_new_work(self, session_id: WorkerSessionId, accepting: bool) -> bool: ...
    def get_session(self, session_id: WorkerSessionId) -> WorkerSession | None: ...
    def evict_session(self, session_id: WorkerSessionId) -> None: ...
    def list_live_sessions(self, now_utc: datetime) -> Sequence[WorkerSession]: ...

class WorkerTransportPort(Protocol):
    """Abstract communication boundary for dispatching notices (e.g., best-effort cancel notices)."""
    async def send_cancellation_notice(self, worker_session_id: WorkerSessionId, attempt_id: AttemptId) -> bool: ...

class TelemetryPort(Protocol):
    """Non-authoritative, fail-open diagnostics port for structured events and span recording."""
    def record_span(self, name: str, attributes: Mapping[str, str]) -> None: ...
    def increment_counter(self, name: str, value: int = 1, labels: Mapping[str, str] | None = None) -> None: ...
```

---

## 16. State Transition Matrices

### 16.1 WorkflowExecution Transition Matrix

| Current State | Transition Function | Target State | Preconditions |
| :--- | :--- | :--- | :--- |
| `INITIALIZING` | `begin_running` | `RUNNING` | Complete expected task set established |
| `INITIALIZING` | `fail_initialization`| `FAILED` | Definitive unrecoverable semantic init failure |
| `INITIALIZING` | `begin_cancellation`| `CANCELLING` | User cancellation requested |
| `RUNNING` | `begin_failing` | `FAILING` | Definitive task failure occurred |
| `RUNNING` | `begin_cancellation`| `CANCELLING` | User cancellation requested |
| `RUNNING` | `complete_success` | `SUCCEEDED` | All tasks SUCCEEDED; output committed |
| `FAILING` | `complete_failure` | `FAILED` | All tasks in terminal state |
| `CANCELLING` | `complete_cancellation`| `CANCELLED` | All tasks in terminal state |
| **Terminal** | *Any* | *Rejected* | Terminal immutability |

### 16.2 TaskExecution Transition Matrix

| Current State | Transition Function | Target State | Preconditions |
| :--- | :--- | :--- | :--- |
| `PENDING` | `mark_initially_runnable`| `RUNNABLE` | Upstream dependencies SUCCEEDED; inputs resolved |
| `PENDING` | `cancel_unstarted_task` | `CANCELLED` | Workflow FAILING or CANCELLING |
| `RUNNABLE` | `mark_running` | `RUNNING` | Ownership committed; Attempt CLAIMED |
| `RUNNABLE` | `cancel_unstarted_task` | `CANCELLED` | Workflow FAILING or CANCELLING |
| `RUNNING` | `mark_succeeded` | `SUCCEEDED` | Authoritative attempt SUCCEEDED; output committed |
| `RUNNING` | `schedule_retry` | `RETRY_WAIT` | Attempt FAILED; retryable; budget remains; wf RUNNING |
| `RUNNING` | `mark_failed` | `FAILED` | Attempt FAILED; non-retryable or budget exhausted |
| `RUNNING` | `settle_task_from_cancelled_attempt` | `CANCELLED` | Active attempt cancelled under drain |
| `RETRY_WAIT` | `mark_retry_ready` | `RUNNABLE` | retry_ready_at_utc elapsed; wf RUNNING |
| `RETRY_WAIT` | `cancel_unstarted_task` | `CANCELLED` | Workflow FAILING or CANCELLING |
| **Terminal** | *Any* | *Rejected* | Terminal immutability |

### 16.3 ExecutionAttempt Transition Matrix

| Current State | Transition Function | Target State | Preconditions |
| :--- | :--- | :--- | :--- |
| `CLAIMED` | `observe_attempt_start` | `RUNNING` | now_utc <= start_deadline_utc |
| `CLAIMED` | `complete_attempt_failure`| `FAILED` | Start deadline expired or worker lost |
| `CLAIMED` | `settle_attempt_cancelled`| `CANCELLED` | Cancel notice acknowledged |
| `RUNNING` | `complete_attempt_success`| `SUCCEEDED` | Result callback reports success |
| `RUNNING` | `complete_attempt_failure`| `FAILED` | Result callback reports failure / timeout / worker lost |
| `RUNNING` | `settle_attempt_cancelled`| `CANCELLED` | Cancel notice acknowledged |
| **Terminal** | *Any* | *Rejected* | Terminal immutability (No direct CLAIMED $\to$ SUCCEEDED) |

---

## 17. Type Inventory

| Type Name | Module Location | Mutability | Durability | Core Invariants |
| :--- | :--- | :--- | :--- | :--- |
| `DefinitionId` | `domain.shared.ids` | Immutable | Durable | Opaque UUIDv4; hashable |
| `WorkflowExecutionId`| `domain.shared.ids` | Immutable | Durable | Opaque UUIDv4; hashable |
| `TaskExecutionId` | `domain.shared.ids` | Immutable | Durable | Opaque UUIDv4; hashable |
| `AttemptId` | `domain.shared.ids` | Immutable | Durable | Opaque UUIDv4; hashable |
| `WorkerSessionId` | `domain.shared.ids` | Immutable | Ephemeral (Session) | Opaque UUIDv4; non-secret correlation ID |
| `TaskDefinitionId` | `domain.shared.ids` | Immutable | Durable (IWS) | Definition-local string; bounded |
| `ActivityType` | `domain.shared.activity`| Immutable | Durable (IWS) | Exact byte-for-byte string; bounded |
| `OutputPresence` | `domain.dataflow.output`| Immutable | Durable | Sum type; distinguishes absent from JSON null |
| `FailureCause` | `domain.failures` | Immutable | Durable | Normalized category + code; safe details |
| `CandidateWorkflowSpec`| `domain.definitions`| Immutable | Ephemeral (Ingress)| Unvalidated parser tree |
| `ValidatedWorkflowSpec`| `domain.definitions`| Immutable | Durable | Verified acyclic DAG; immutable after validation |
| `CanonicalGraph` | `domain.definitions`| Immutable | Ephemeral (Derived)| Bidirectional adjacency DAG; $O(V+E)$ |
| `WorkflowExecution` | `domain.execution` | Immutable Snapshot | Durable | Integer revision; OCC guarded |
| `TaskExecution` | `domain.execution` | Immutable Snapshot | Durable | Stable whole-value input; OCC guarded |
| `ExecutionAttempt` | `domain.execution` | Immutable Snapshot | Durable | Bound to WorkerSessionId; monotonic ordinal |
| `WorkerSession` | `domain.workers` | Immutable Snapshot | Ephemeral | In-memory only; capability advertisement |

---

## 18. Package Dependency Matrix

| Package | Allowed Dependencies | Prohibited Dependencies |
| :--- | :--- | :--- |
| `domain.shared` | Python Standard Library | `domain.*`, `application.*`, `ports.*`, `infrastructure.*` |
| `domain.failures`| `domain.shared` | `domain.execution`, `ports.*`, Frameworks |
| `domain.dataflow`| `domain.shared` | `domain.execution`, `ports.*`, Frameworks |
| `domain.definitions`| `domain.shared`, `domain.dataflow` | `domain.execution`, `ports.*`, Frameworks |
| `domain.execution` | `domain.shared`, `domain.dataflow`, `domain.failures` | `ports.*`, `application.*`, Frameworks |
| `domain.services`| `domain.*` (Internal only) | `ports.*`, `application.*`, `infrastructure.*` |
| `ports` | `domain.*`, Python Standard Library | `application.*`, `infrastructure.*`, Frameworks |
| `application` | `domain.*`, `ports.*` | `infrastructure.*`, `fastapi`, `sqlalchemy` |
| `infrastructure`| `ports.*`, `domain.*`, External Libraries | `application.*` (Direct calls prohibited) |

---

## 19. Concrete File Layout

```
src/nexusflow/
├── domain/
│   ├── __init__.py
│   ├── errors.py
│   ├── shared/
│   │   ├── __init__.py
│   │   ├── ids.py
│   │   ├── activity.py
│   │   └── types.py
│   ├── failures/
│   │   ├── __init__.py
│   │   └── models.py
│   ├── dataflow/
│   │   ├── __init__.py
│   │   ├── bindings.py
│   │   └── output.py
│   ├── definitions/
│   │   ├── __init__.py
│   │   ├── models.py
│   │   ├── graph.py
│   │   └── validator.py
│   ├── execution/
│   │   ├── __init__.py
│   │   ├── states.py
│   │   ├── workflow.py
│   │   ├── task.py
│   │   └── attempt.py
│   └── services/
│       ├── __init__.py
│       ├── routing.py
│       ├── readiness.py
│       └── retries.py
│
├── ports/
│   ├── __init__.py
│   ├── clock.py
│   ├── random.py
│   ├── telemetry.py
│   ├── persistence/
│   │   ├── __init__.py
│   │   ├── execution_mutation.py
│   │   ├── execution_query.py
│   │   └── definition.py
│   └── workers/
│       ├── __init__.py
│       ├── session_registry.py
│       └── transport.py
│
└── application/
    ├── __init__.py
    ├── commands.py
    ├── results.py
    ├── context.py
    ├── definitions/
    ├── executions/
    ├── workers/
    ├── scheduling/
    └── recovery/
```

---

## 20. Testing Strategy

Pure domain components are unit-tested without databases, network I/O, or mock containers:
1. **State Machine Transitions**: Exhaustive matrix tests evaluating all valid transitions and asserting explicit `TransitionRejected` for invalid states.
2. **Canonical Graph Validation**: Property-based graph tests asserting acyclicity, topological sorting, and dependency resolution.
3. **Data Flow Bindings**: Isolated unit tests verifying whole-value literal, workflow input, and task output resolution, specifically asserting the distinction between uncommitted output and committed JSON `null`.
4. **Pure Services**: Deterministic tests for routing exact-string matching and retry eligibility rules using injected `FakeClock`.

---

## 21. Implementation Traps to Avoid

1. **Raw String Passing**: Never use raw `str` or `UUID` in domain APIs; use typed IDs (`WorkflowExecutionId`, `TaskDefinitionId`).
2. **Framework Leaks**: Never inherit Pydantic `BaseModel` or SQLAlchemy `DeclarativeBase` in `nexusflow.domain.*`.
3. **Pessimistic State Mutators**: Never add a generic `entity.state = new_state` mutator; state changes must pass through pure transition functions returning `TransitionResult`.
4. **Conflating JSON Null with Absent Output**: Never represent output as `JsonValue | None`. Always use the `OutputPresence` sum type (`OutputAbsent` vs. `OutputCommitted`).
5. **Generic Repository `save()`**: Avoid generic CRUD `save(entity)` APIs on persistence ports; use consistency-group methods.
6. **Worker Token Conflation**: Never confuse Bearer authentication credentials with ephemeral `WorkerSessionId` incarnation IDs.
7. **Re-resolving Input on Retry**: Never re-resolve data flow bindings during `RETRY_WAIT $\to$ RUNNABLE`; `stable_input` established at first execution is immutable forever.
8. **Trusting Worker Retry Decisions**: Never allow untrusted worker error reports to authoritatively decide `retryable = true/false`.

---

## 22. Logical Handoff to LLD-02 (PostgreSQL Schema & Persistence)

The Persistence LLD (**LLD-02**) owns the physical PostgreSQL schema, DDL, indexes, and connection pooling. LLD-01 establishes the following **logical storage and transactional requirements**:

### 22.1 Entity Storage Requirements
1. **RegisteredDefinition**: Must durably store `DefinitionId`, immutable `ValidatedWorkflowSpec`, creation timestamp, and optional idempotency/fingerprint metadata.
2. **WorkflowExecution**: Must durably store `WorkflowExecutionId`, exact `DefinitionId`, current `WorkflowState`, integer `revision`, immutable `workflow_input`, output presence flag (`has_output`), output payload (if committed), failure metadata (`category`, `code`, `message`), and creation/update timestamps.
3. **TaskExecution**: Must durably store `TaskExecutionId`, `WorkflowExecutionId`, `TaskDefinitionId`, current `TaskState`, integer `revision`, `stable_input` (once established), output presence flag (`has_output`), output payload (if committed), effective `max_attempts`, `retry_ready_at_utc` (if in `RETRY_WAIT`), terminal failure metadata (if terminal `FAILED`), and creation/update timestamps.
4. **ExecutionAttempt**: Must durably store `AttemptId`, `TaskExecutionId`, committed `AttemptOrdinal`, `WorkerSessionId`, current `AttemptState`, integer `revision`, `start_deadline_utc`, optional `execution_timeout_utc`, optional `cancellation_deadline_utc`, terminal failure metadata, and creation/update timestamps.
5. **HistoryEntry**: Must durably append semantic audit entries with `HistoryEntryId`, `WorkflowExecutionId`, optional `TaskExecutionId`, optional `AttemptId`, semantic event category, structured event payload, and UTC timestamp.

### 22.2 Invariants & Consistency Requirements
1. **Task Uniqueness**: Exactly one `TaskExecution` record exists per `(WorkflowExecutionId, TaskDefinitionId)`.
2. **Attempt Ordinal Guarantees**: Committed attempt ordinals for a `TaskExecution` are 1-based, unique, and strictly monotonically increasing; gaps are permitted (e.g., after aborted transactions), but ordinals are never reused.
3. **At-Most-One Active Attempt**: A `TaskExecution` has at most one active attempt (`CLAIMED` or `RUNNING`) at any time.
4. **Distinguishable Output Null**: The database representation must strictly differentiate between absent/uncommitted output and committed JSON `null` for both tasks and workflows.
5. **Revision & OCC Guards**: Every state update must conditionally verify the expected integer `revision` and increment it atomically.
6. **Anti-TOCTOU Consistency Groups**: Transactions implementing `ExecutionMutationPort` must revalidate workflow state (`RUNNING`), task state (`PENDING` or `RETRY_WAIT`), and worker session identity atomically within the transaction block.
7. **Atomic History Coupling**: Every state mutation must insert its corresponding `HistoryEntry` within the same database transaction.
8. **Idempotency Semantic Equivalence**: Idempotency records must support distinguishing between identical retry requests (returning original resource) and conflicting payload reuse (rejected with conflict).

---

## 23. Design Validation Checklist

- [x] **ADR Alignment**: Strictly complies with ADR-001 through ADR-023 without omission.
- [x] **Zero Framework Contamination**: Pure domain core contains zero imports of FastAPI, SQLAlchemy, asyncpg, Pydantic, Prometheus, or OpenTelemetry.
- [x] **Type Distinction**: `CandidateWorkflowSpec` and `ValidatedWorkflowSpec` are distinct types.
- [x] **Named Workflow Outputs**: Supports named workflow output mappings resolving whole values, evaluating to JSON null when empty.
- [x] **Stable Input Immutability**: `mark_initially_runnable` is the sole input-establishing transition; retry transitions strictly preserve existing stable input.
- [x] **Distinct Retry-Ready Mutation**: `commit_retry_ready` exists on `ExecutionMutationPort` and preserves stable input while atomically verifying workflow `RUNNING` and timer expiration.
- [x] **Explicit Cancellation Operations**: Differentiates unstarted task cancellation (`cancel_unstarted_task`) from active attempt settlement (`settle_task_from_cancelled_attempt`).
- [x] **Output Presence Fidelity**: `OutputPresence` sum type preserves distinction between absent data and committed JSON `null`.
- [x] **Deeply Immutable JSON Representation**: `freeze_json` canonicalizes domain payloads, rejecting non-finite numbers and eliminating caller-owned mutable references.
- [x] **Engine-Owned Retries**: Untrusted worker reports submit raw observations (`WorkerFailureReport`); control-plane policy determines retry eligibility without allocating attempt ordinals in domain logic.
- [x] **Worker Session Continuity**: Registry port supports new incarnation allocation, surviving process reconnection, and draining.
- [x] **Anti-TOCTOU Mutation Contracts**: Persistence mutation ports enforce atomic verification of workflow running, task state, and worker session identity.
- [x] **Logical LLD-02 Handoff**: Defines logical persistence requirements without prewriting physical SQL DDL.
- [x] **Cross-LLD Alignment**: LLD numbering and handoff contracts precisely match the 10 agreed LLD specifications.

---

### Classification

**LLD-01 Architecture-Ready / Approved as LLD-02 Input**
