# NexusFlow V1 — LLD-09: Observability, Configuration & Runtime Lifecycle

**Document Status:** Architecture-Ready / Approved as LLD-10 Input  
**Authoritative References:** ADR-001 (Intermediate Workflow Specification), ADR-002 (Definition Parsing), ADR-003 (Canonical Graph Representation), ADR-004 (Definition Validation), ADR-005 (Scheduler), ADR-006 (Workflow State Machine), ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-014 (History), ADR-015 (Public Management API), ADR-016 (Observability), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security Architecture), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership), LLD-05 (Worker Protocol & Worker Runtime), LLD-06 (Execution Results, Retries, Timeouts & Cancellation), LLD-07 (Recovery & Reconciliation), LLD-08 (Public API & Security).  
**Downstream Dependents:** LLD-10 (Implementation Roadmap, Verification Plan & MVP Execution).

---

## 1. Primary Objective & Architectural Scope

### 1.1 Objective Statement
This document defines the complete implementation-level runtime design for the NexusFlow V1 single-process control plane. It specifies how the control-plane process boots, validates and locks immutable configuration, establishes PostgreSQL persistence, verifies Alembic schema compatibility, orchestrates mandatory startup recovery, supervises concurrent background runtime loops, enforces process readiness and liveness, emits structured non-authoritative telemetry (logs, metrics, traces), handles operating system termination signals, coordinates graceful draining, and executes clean shutdown.

### 1.2 Non-Negotiable Core Boundaries
1. **Single Control-Plane Process ($N=1$):** V1 strictly mandates a single active control-plane instance. Ephemeral in-memory registries (Worker Registry), priority timer heaps, and dispatch queues operate within a single process space. Multi-node distributed clustering, leader election, and multiple Uvicorn workers are prohibited.
2. **PostgreSQL Current State is Sole Authority:** Memory is entirely ephemeral. If the process crashes or restarts, all orchestration state is reconstructed exclusively from PostgreSQL 16 relational records (`workflow_executions`, `task_executions`, `execution_attempts`, `registered_definitions`).
3. **Observability is Non-Authoritative:** Telemetry (Prometheus metrics, OpenTelemetry traces, structured JSON logs) is strictly diagnostic and fail-open. Emitting telemetry must never alter workflow state, participate in consistency transactions, or gate orchestration decisions.
4. **Configuration Parameterizes, Never Redefines:** Configuration specifies operational intervals, batch limits, secrets, and timeouts. It cannot alter domain invariants, state machines, or workflow semantics.

---

## 2. Process Lifecycle Architecture (ADR-017)

Process lifecycle states model the runtime operational phase of the control-plane container. They are strictly ephemeral and **must never be confused with durable workflow states**.

```text
    [ Bootstrap Phase / RecoveryGate Closed ]
                     │  (Recovery converged, critical loops running)
                     ▼
             ┌───────────────┐
             │    SERVING    │  (Readiness = True, new work accepted)
             └───────────────┘
                     │  (SIGTERM / SIGINT received OR critical loop crashes)
                     ▼
             ┌───────────────┐
             │   DRAINING    │  (Readiness = False, all new mutations blocked, bounded settlement)
             └───────────────┘
                     │  (Grace window expires or in-flight work settles)
                     ▼
             ┌───────────────┐
             │  TERMINATING  │  (Loops cancelled, DB pool disposed, clean exit)
             └───────────────┘
```

### 2.1 Process Lifecycle States vs. Workflow States
- **Process States (Ephemeral Enum):** Exactly three states defined in ADR-017:
  ```python
  from enum import StrEnum


  class ProcessLifecycle(StrEnum):
      SERVING = "SERVING"
      DRAINING = "DRAINING"
      TERMINATING = "TERMINATING"
  ```
- **Bootstrap is a Phase, Not a State:** The initial boot sequence is an unready ephemeral initialization phase during which `RecoveryGate.allows_new_work() == False` and `/readyz` returns 503. There is **no** `ProcessLifecycle.STARTUP` or `ProcessLifecycle.RECOVERING` enum member.
- **Workflow States (Durable):** `INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`.
- **No Synthetic Domain States:** The domain possesses no `STARTING`, `RECOVERING`, `STOPPED`, `DEGRADED`, or `PAUSED` states.

---

## 3. Concrete Startup Sequence & Recovery Integration

The startup sequence executes deterministically upon container boot before general operational readiness is declared:

```text
Step 1: Environment & Configuration Bootstrap
  ├── Load deployment environment variables with NEXUSFLOW_ prefix via pydantic-settings.
  ├── Validate all constraints fail-closed (min pool, timeouts, credentials).
  ├── Compute cryptographic digests (SHA-256) for raw bearer tokens immediately.
  ├── Construct deeply immutable runtime security authority (PublicCredentialConfig, token digests).
  └── Freeze configuration instance into deeply immutable models (FrozenSettingsModel).

Step 2: Structured Logging & Telemetry Initialization
  ├── Initialize JSON structured log formatter on stdout with allowlisted safe fields.
  └── Initialize OpenTelemetry TracerProvider and Prometheus MetricsRegistry.

Step 3: Persistence Connection Pool & Schema Verification
  ├── Instantiate SQLAlchemy AsyncEngine (asyncpg driver) with bounded pool limits.
  ├── Verify database connectivity via active probe: `SELECT 1`.
  └── Verify Alembic revision table against expected migration head. If mismatch: FAIL CLOSED.

Step 4: Ephemeral In-Memory State Construction
  ├── Initialize empty WorkerSessionRegistry (LLD-05).
  ├── Initialize empty TimerCoordinator priority heap.
  ├── Initialize empty SchedulerWakeupQueue (asyncio.Queue).
  └── Initialize Ephemeral RecoveryGate to CLOSED (allows_new_work=False, allows_existing_settlement=True).

Step 5: Mandatory Startup Recovery Execution (LLD-07)
  ├── Fast-boot HTTP server: GET /healthz returns 200, GET /readyz returns 503.
  ├── Public mutations blocked (503 NOT_READY); safe read-only APIs accessible.
  ├── Late result callbacks for pre-restart RUNNING attempts admitted to race OCC.
  └── Execute 8-Phase Keyset Reconciliation (Phases 1-8 to end-of-keyspace).

Step 6: Runtime Background Loop Activation & Readiness Declaration
  ├── Instantiate RuntimeSupervisor with fatal shutdown callback.
  ├── Start all background tasks (Critical: Scheduler, Sweepers, Timers, Drains; Non-Critical: Telemetry Exporter).
  ├── Verify all critical task handles are healthy and running.
  ├── Open RecoveryGate: `allows_new_work = True`.
  ├── Transition Process Lifecycle to SERVING.
  └── Readiness probe GET /readyz flips to 200 OK.
```

---

## 4. Liveness (`/healthz`) vs. Readiness (`/readyz`) Specification

| Dimension | `/healthz` (Liveness) | `/readyz` (Readiness) |
| :--- | :--- | :--- |
| **Purpose** | Indicates whether the Python event loop and HTTP server are responsive. | Indicates whether the control plane is fully reconciled and safe to orchestrate. |
| **Orchestration Authority** | None. | Gates external traffic admission (load balancer ingress). |
| **Startup Recovery Phase** | **`200 OK`** | **`503 Service Unavailable`** (`NOT_READY`) |
| **Serving Phase** | **`200 OK`** | **`200 OK`** |
| **Graceful Drain Phase** | **`200 OK`** (while alive) | **`503 Service Unavailable`** (`DRAINING`) |
| **Critical Loop Crash** | **`200 OK`** (triggers drain) | **`503 Service Unavailable`** (`RUNTIME_UNHEALTHY`) |
| **Non-Critical Loop Failure** | **`200 OK`** | **`200 OK`** (Telemetry fails open; orchestration continues) |
| **DB Outage / Disconnect** | **`200 OK`** | **`503 Service Unavailable`** (`DB_UNAVAILABLE`) |

---

## 5. Configuration Architecture & Deep Immutability (ADR-023)

Configuration is loaded once at application bootstrap using `pydantic-settings` and frozen into **deeply immutable structures**. NexusFlow V1 strictly rejects dynamic configuration reload, distributed configuration stores (Consul, etcd, Redis), and runtime feature flagging.

### 5.1 Deep Immutability Architecture & Credential Boundary Separation
To prevent runtime configuration drift, mutable collection escape hatches, or secret leakage:
1. **`FrozenSettingsModel` Base Class:** All nested settings models inherit from a common base with `frozen=True` and `extra="forbid"`.
2. **Immutable Collection Containers:** Raw mutable dictionaries and lists are prohibited as long-lived runtime authorities. Public client tokens and permissions are transformed into deeply immutable value objects using `frozenset` and `tuple`.
3. **Deployment vs. Runtime Authority Separation:**
   - *Deployment/Bootstrap Input:* Receives operator environment variables (`NEXUSFLOW_SECURITY__WORKER_DOMAIN_TOKEN`, `NEXUSFLOW_SECURITY__PUBLIC_API_TOKENS`). Raw secrets exist only transiently at configuration/bootstrap boundaries.
   - *Long-Lived Runtime Authority:* During security bootstrap, raw credentials are immediately hashed into SHA-256 cryptographic digests (`bytes`). Only these immutable digests and permission sets are retained in the frozen runtime security configuration and `SecurityContext`. Raw credentials are not retained in long-lived runtime authority after digest construction.

```python
import hashlib
import json
from dataclasses import dataclass
from pydantic import BaseModel, ConfigDict, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict
from nexusflow.domain.security import PublicPermission


class FrozenSettingsModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
    )


@dataclass(frozen=True, slots=True)
class PublicCredentialConfig:
    principal_id: str
    credential_digest: bytes
    permissions: frozenset[PublicPermission]


class DatabaseSettings(FrozenSettingsModel):
    url: PostgresDsn
    pool_min_size: int = Field(default=5, ge=1, le=50)
    pool_max_size: int = Field(default=20, ge=5, le=100)
    pool_timeout_seconds: float = Field(default=30.0, ge=1.0)
    max_overflow: int = Field(default=10, ge=0)


class HttpSettings(FrozenSettingsModel):
    host: str = "0.0.0.0"
    port: int = 8000
    metrics_port: int = 9090
    max_payload_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)  # 1 MB


class WorkerRuntimeSettings(FrozenSettingsModel):
    heartbeat_interval_seconds: float = Field(default=5.0, ge=1.0)
    heartbeat_timeout_seconds: float = Field(default=15.0, ge=3.0)
    long_poll_timeout_seconds: float = Field(default=30.0, ge=1.0, le=60.0)
    claim_start_deadline_seconds: float = Field(default=10.0, ge=1.0)


class RetrySettings(FrozenSettingsModel):
    fixed_delay_seconds: float = Field(default=10.0, gt=0.0)  # Strictly positive duration
    max_engine_occ_retries: int = Field(default=3, ge=1, le=10)


class RecoverySettings(FrozenSettingsModel):
    keyset_batch_size: int = Field(default=100, ge=10, le=1000)
    convergence_max_passes: int = Field(default=10, ge=1, le=50)


class ShutdownSettings(FrozenSettingsModel):
    grace_window_seconds: float = Field(default=30.0, ge=5.0, le=120.0)


class RuntimeSecurityAuthority(FrozenSettingsModel):
    """Immutable runtime security authority populated during bootstrap."""

    public_credentials: tuple[PublicCredentialConfig, ...] = Field(default_factory=tuple)
    worker_domain_token_digest: bytes = Field(...)


class BootstrapSecurityInput(FrozenSettingsModel):
    """Transient bootstrap schema parsing operator environment variables."""

    worker_domain_token: str = Field(...)
    public_api_tokens: str = Field(...)  # JSON string representation from deployment

    def build_runtime_authority(self) -> RuntimeSecurityAuthority:
        worker_digest = hashlib.sha256(self.worker_domain_token.encode("utf-8")).digest()
        parsed_tokens = json.loads(self.public_api_tokens)
        creds = []
        for entry in parsed_tokens:
            token_raw = entry["token"]
            token_digest = hashlib.sha256(token_raw.encode("utf-8")).digest()
            perms = frozenset(PublicPermission(p) for p in entry["permissions"])
            creds.append(
                PublicCredentialConfig(
                    principal_id=entry["principal_id"],
                    credential_digest=token_digest,
                    permissions=perms,
                )
            )
        return RuntimeSecurityAuthority(
            public_credentials=tuple(creds),
            worker_domain_token_digest=worker_digest,
        )


class ObservabilitySettings(FrozenSettingsModel):
    log_level: str = "INFO"
    otlp_endpoint: str | None = None
    enable_metrics: bool = True


class NexusFlowSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="NEXUSFLOW_", env_nested_delimiter="__", frozen=True, extra="forbid"
    )

    db: DatabaseSettings
    http: HttpSettings = Field(default_factory=HttpSettings)
    worker: WorkerRuntimeSettings = Field(default_factory=WorkerRuntimeSettings)
    retry: RetrySettings = Field(default_factory=RetrySettings)
    recovery: RecoverySettings = Field(default_factory=RecoverySettings)
    shutdown: ShutdownSettings = Field(default_factory=ShutdownSettings)
    security: RuntimeSecurityAuthority
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
```

### 5.2 The Five Architectural Configuration Categories (ADR-023)
1. **Category 1 — Architectural Invariants (Non-Configurable):** Single control-plane process ($N=1$), PostgreSQL authoritative storage, HTTP/JSON pull worker transport, no brokers, OCC-first concurrency, exact task membership terminalization, monotonic attempt ordinals.
2. **Category 2 — Definition-Semantic Values (Immutable IWS):** Task activity types, dependencies, retry budgets (`max_attempts`), input/output bindings. Defined per workflow in validated YAML; never configured globally.
3. **Category 3 — Operational Policy Resolved Durably:** Concrete operational policies whose evaluation produces durable database records.
   - *Example:* Task retry delay policy resolves an absolute UTC timestamp persisted into `task_executions.retry_ready_at_utc`. Once written, the timestamp is authoritative; restarts never recalculate the delay.
4. **Category 4 — Process Settings:** Engine pool sizing, worker liveness heartbeat cadences, defensive repair sweep intervals, long-poll timeouts, shutdown grace windows.
5. **Category 5 — Deployment Secrets:** PostgreSQL connection strings, public API token digests, worker domain token digest, OpenTelemetry OTLP collector endpoints.

### 5.3 Concrete V1 Retry Delay Policy
LLD-06 specified that retry delay resolution is operational. LLD-09 formalizes the V1 mechanism:
- **Policy:** Strictly positive fixed duration configured via `NEXUSFLOW_RETRY__FIXED_DELAY_SECONDS` (validation: `gt=0.0`, default: 10.0 seconds).
- **Semantics:** When a task failure is classified as retryable by LLD-06 engine rules:
  ```python
  retry_ready_at_utc = clock.now_utc() + timedelta(seconds=settings.retry.fixed_delay_seconds)
  ```
- **Persisted Authority:** `retry_ready_at_utc` is written atomically with the transition to `RETRY_WAIT`.
- **No Complex Math:** No exponential backoff, jitter coefficients, or definition-level delay curves in V1.
- **Disambiguation:** Task retry delay is completely distinct from database transaction OCC backoff or HTTP network retries.

---

## 6. Time & Clock Architecture

In accordance with frozen LLD-01:
```python
from datetime import datetime, timezone
from typing import Protocol


class Clock(Protocol):
    """Abstract time authority for deterministic testing."""

    def now_utc(self) -> datetime:
        """Returns timezone-aware UTC datetime. Raises ValueError if naive."""
        ...


class SystemClock:
    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
```
- **Semantic Deadlines:** All durable deadlines (`start_deadline_utc`, `execution_timeout_utc`, `retry_ready_at_utc`, `cancellation_deadline_utc`) are computed using the injected `Clock`.
- **Prohibited:** Direct calls to `datetime.now()` without UTC timezone or monotonic timers for durable business decisions.
- **Monotonic Clocks:** `time.monotonic()` is reserved strictly for measuring local in-memory durations, HTTP latencies, and long-poll timeouts.

---

## 7. Critical vs. Non-Critical Runtime Background Loops

The control plane coordinates ten asynchronous background loops supervised by the `RuntimeSupervisor`. Each loop implements the `BackgroundLoop` interface and explicitly declares whether it is correctness-critical:

```python
from typing import Protocol


class BackgroundLoop(Protocol):
    name: str
    is_critical: bool

    async def run(self) -> None: ...
```

| Background Loop Name | Owning LLD | Durable DB Source | Primary Wakeup Trigger | Fallback Cadence | Keyset Batch Size | Invoked Persistence Transaction | Critical Loop (`is_critical`) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Scheduler Wakeup Loop** | LLD-04 | `task_executions` | In-process queue / event | 500 ms | 100 | `commit_task_readiness` | **Yes** (True) |
| **Defensive PENDING Sweep**| LLD-04 | `task_executions` (PENDING) | Periodic timer | 5,000 ms | 50 | `commit_task_readiness` | **Yes** (True) |
| **RUNNABLE Rediscovery** | LLD-04 | `task_executions` (RUNNABLE) | Periodic timer / Wakeup | 2,000 ms | 100 | Ownership matching queue push | **Yes** (True) |
| **Due RETRY_WAIT Sweep** | LLD-06 | `task_executions` (RETRY_WAIT)| Priority timer heap | 1,000 ms | 50 | `commit_retry_ready` | **Yes** (True) |
| **Start Deadline Sweep** | LLD-06 | `execution_attempts` (CLAIMED)| Priority timer heap | 1,000 ms | 50 | `commit_internal_attempt_failure` | **Yes** (True) |
| **Execution Timeout Sweep**| LLD-06 | `execution_attempts` (RUNNING)| Priority timer heap | 2,000 ms | 50 | `commit_internal_attempt_failure` | **Yes** (True) |
| **Worker Liveness Sweep** | LLD-05/06 | Ephemeral `WorkerRegistry` | Periodic timer | 1,000 ms | N/A (RAM) | `commit_internal_attempt_failure` | **Yes** (True) |
| **Cancellation Deadline** | LLD-06 | `execution_attempts` (DRAIN) | Priority timer heap | 1,000 ms | 50 | `commit_internal_attempt_failure` | **Yes** (True) |
| **Workflow Drain / Term** | LLD-06 | `workflow_executions` (DRAIN) | Task terminal transition| 1,000 ms | 50 | `commit_workflow_failure / cancel`| **Yes** (True) |
| **Telemetry Exporter Flush**| LLD-09 | Non-authoritative buffer | Periodic timer | 5,000 ms | N/A | Batch OTLP / Prometheus export | **No** (False) |

---

## 8. In-Process Supervisor & Controlled Termination Architecture

### 8.1 Runtime Supervisor Classification & Critical Crash Handling
The supervisor distinguishes between **correctness-critical background loops** (`loop.is_critical is True`) and **non-critical telemetry/diagnostic loops** (`loop.is_critical is False`).

#### 8.1.1 Critical Loop Failure Semantics (`loop.is_critical is True`)
A critical loop crash represents a fundamental failure of orchestration correctness. The control plane **must not** remain alive in a zombie state claiming readiness with critical background loops dead.
1. **Fatal Runtime Signal:** When an unexpected exception escapes a critical loop (or an infinite critical loop exits normally while not stopping), the supervisor marks `RuntimeHealth` unhealthy, flips readiness to 503, and triggers a single, irrevocable fatal termination signal (`fatal_failure_event`).
2. **Process Controller Drains:** The fatal signal causes `ProcessLifecycleController` to enter `DRAINING`, closing public new-work admission immediately.
3. **No Fabricated Domain Failures:** A critical loop crash triggers **process-level termination only**. It does **not** mark active workflows or tasks `FAILED` or `CANCELLED`. Durable state remains as committed in PostgreSQL; restart recovery (LLD-07) reconciles orphan attempts safely.
4. **Intentional Cancellation Distinguished:** `asyncio.CancelledError` raised during normal graceful shutdown is expected and does **not** trigger a fatal failure signal.

#### 8.1.2 Non-Critical Loop Failure Semantics (`loop.is_critical is False`)
Per ADR-016, telemetry is strictly best-effort and fail-open.
1. **Zero Readiness Impact:** Failure of a non-critical loop (such as `TelemetryExporterFlushLoop`) must **never** trip `RuntimeHealth` unhealthy, flip readiness to 503, fire `fatal_failure_event`, or initiate process drain.
2. **Fail-Open Isolation:** The supervisor logs a sanitized warning/error diagnostic, increments a bounded loop failure counter, and may restart the non-critical loop or allow it to remain disabled until process restart. Orchestration continues completely unaffected.

```python
import asyncio
import logging
from typing import Mapping


class UnexpectedLoopExit(Exception):
    """Raised when an infinite loop terminates normally while serving."""

    pass


class UnexpectedLoopCancellation(Exception):
    """Raised when a loop is cancelled unexpectedly while serving."""

    pass


class RuntimeSupervisor:
    def __init__(
        self,
        loops: Mapping[str, "BackgroundLoop"],
        lifecycle_controller: "ProcessLifecycleController",
        logger: logging.Logger,
    ):
        self._loops = loops
        self._lifecycle = lifecycle_controller
        self._logger = logger
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._healthy = True
        self._fatal_event = asyncio.Event()

    async def start(self) -> None:
        for name, loop in self._loops.items():
            self._tasks[name] = asyncio.create_task(self._run_loop(name, loop), name=name)

    async def _run_loop(self, name: str, loop: "BackgroundLoop") -> None:
        try:
            await loop.run()

            # Infinite background loops must never terminate normally while serving
            if not self._lifecycle.is_stopping():
                exc = UnexpectedLoopExit(f"Loop '{name}' exited unexpectedly while SERVING.")
                if loop.is_critical:
                    self._handle_critical_failure(name, exc)
                else:
                    self._handle_noncritical_failure(name, exc)

        except asyncio.CancelledError:
            if self._lifecycle.is_stopping():
                self._logger.info("Background loop '%s' cancelled cleanly for shutdown.", name)
                return

            # Unexpected cancellation while supposed to be serving
            exc = UnexpectedLoopCancellation(f"Loop '{name}' cancelled unexpectedly while SERVING.")
            if loop.is_critical:
                self._handle_critical_failure(name, exc)
            else:
                self._handle_noncritical_failure(name, exc)

        except Exception as exc:
            if loop.is_critical:
                self._handle_critical_failure(name, exc)
            else:
                self._handle_noncritical_failure(name, exc)

    def _handle_critical_failure(self, name: str, exc: Exception) -> None:
        self._logger.critical(
            "Fatal failure in critical background loop '%s': %s", name, exc, exc_info=True
        )
        self._healthy = False
        if not self._fatal_event.is_set():
            self._fatal_event.set()
            # Notify process controller to begin controlled termination
            asyncio.create_task(
                self._lifecycle.initiate_controlled_drain(reason=f"CRITICAL_LOOP_CRASH:{name}")
            )

    def _handle_noncritical_failure(self, name: str, exc: Exception) -> None:
        # Non-critical loops fail open; control plane health and readiness remain intact
        self._logger.warning(
            "Non-critical background loop '%s' encountered error (fail-open): %s",
            name,
            exc,
            exc_info=True,
        )

    def is_healthy(self) -> bool:
        return self._healthy and not self._fatal_event.is_set()

    def fatal_failure_event(self) -> asyncio.Event:
        return self._fatal_event

    async def stop(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
```

### 8.2 In-Process Wakeups vs. Authoritative Persistence
- Wakeups operate strictly through `asyncio.Event` or bounded `asyncio.Queue` primitives within local process memory.
- **Wakeups are Hints Only:** If an in-memory wakeup is dropped or duplicated, system correctness is untouched because every subsystem incorporates defensive, bounded keyset sweeps.
- **No Distributed Outbox/Broker:** No Kafka, RabbitMQ, or Redis is introduced for wakeups.

### 8.3 Acceleration Timer Architecture
- Durable deadlines reside in PostgreSQL (`start_deadline_utc`, `execution_timeout_utc`, `retry_ready_at_utc`, `cancellation_deadline_utc`).
- The in-memory `TimerCoordinator` maintains a Python `heapq` of `(deadline_utc, entity_id)` tuples to accelerate wakeup without tight database polling.
- **Crash Immunity:** If the control-plane crashes, the heap is destroyed. Upon restart, LLD-07 recovery and defensive sweeps reconstruct timers directly from PostgreSQL.

---

## 9. Graceful Shutdown & Process Drain (ADR-017)

Graceful shutdown coordinates orderly process termination upon receiving `SIGTERM` or `SIGINT`, or upon a fatal critical-loop crash:

```text
OS Signal (SIGTERM / SIGINT) OR Fatal Critical Loop Crash
      │
      ▼
1. Transition Process Lifecycle: SERVING -> DRAINING
      │
      ▼
2. Readiness Fails: GET /readyz returns 503 DRAINING (load balancer detaches)
      │
      ▼
3. Block ALL New Public Mutations:
      ├── POST /v1/definitions (503 Service Unavailable)
      ├── POST /v1/executions (503 Service Unavailable)
      └── POST /v1/executions/{id}/cancel (503 Service Unavailable)
      │
      ▼
4. Stop Scheduler Ownership Allocation (Worker poll returns 204 No Content / Drain)
      │
      ▼
5. Bounded Existing-Work Settlement Window (NEXUSFLOW_SHUTDOWN__GRACE_WINDOW_SECONDS)
      ├── Admitted worker result callbacks continue to commit via PostgreSQL OCC
      ├── Already-committed ownership start/result races settle normally
      └── Ongoing database transactions are allowed to complete
      │
      ▼
6. Cancel Internal Background Loops via RuntimeSupervisor.stop()
      │
      ▼
7. Flush Local Telemetry Exporters (Best-effort flush, 2 sec timeout)
      │
      ▼
8. Dispose PostgreSQL AsyncEngine Pool (Close idle & active connections)
      │
      ▼
9. Transition Process Lifecycle: DRAINING -> TERMINATING -> Exit (0 or 1 on crash)
```

### 9.1 Shutdown Invariants
- **Public Mutations Blocked:** ADR-017 explicitly requires stopping all new external mutations. `POST /v1/executions/{id}/cancel` is a public mutation; submitting cancellation after the drain boundary is rejected with 503.
- **Existing Work May Settle:** Worker result callbacks (`POST /internal/v1/worker/callback`) for already-active attempts continue to be admitted and commit via PostgreSQL OCC during the grace window.
- **No Fabricated Failures:** Graceful shutdown **never** marks workflows `FAILED` or `CANCELLED` merely because the process is stopping.
- **No Ownership Rollback:** Active attempts owned by workers remain bound to their respective `WorkerSessionId` in PostgreSQL.
- **Crash Convergence:** If the shutdown grace window expires before all in-flight work settles, the process terminates immediately. Surviving external workers will have their attempts resolved upon restart recovery via LLD-07.

---

## 10. Observability Architecture (ADR-016)

Observability is decoupled from orchestration correctness. All telemetry operations are **fail-open** and execute outside transactional consistency boundaries.

### 10.1 Structured Logging & Leakage Prevention
To ensure secrets and customer payloads cannot leak through message strings or format arguments:
1. **Static Message Templates:** Application code **must not** interpolate variable values directly into log message strings. Code must use static event names and supply structured fields via `extra`.
2. **Strict Allowlisting:** The JSON log formatter extracts only explicitly allowlisted metadata fields, discarding raw or uninspected `__dict__` keys.
3. **Pre-Serialization Redaction:** Sensitive structured fields (`authorization`, `token`, `password`, `secret`, `raw_yaml`, `workflow_input`, `workflow_output`, `task_input`, `task_output`) are scrubbed before JSON serialization.
4. **Sanitized Exception Formatting:** Exception tracebacks are inspected and scrubbed of raw connection strings or DSN credentials before emission.

```python
import logging
import json
from datetime import datetime, timezone
from typing import Any


class SanitizedJsonFormatter(logging.Formatter):
    ALLOWED_FIELDS = {
        "timestamp",
        "level",
        "logger",
        "message",
        "event_name",
        "request_id",
        "trace_id",
        "span_id",
        "workflow_execution_id",
        "task_execution_id",
        "attempt_id",
        "duration_ms",
        "exception",
    }

    REDACTED_KEYS = {
        "authorization",
        "token",
        "password",
        "secret",
        "raw_yaml",
        "workflow_input",
        "workflow_output",
        "task_input",
        "task_output",
        "dsn",
    }

    def format(self, record: logging.LogRecord) -> str:
        # Static message format only; formatting args are sanitized
        msg = record.getMessage()
        for key in self.REDACTED_KEYS:
            if key in msg.lower():
                msg = "[MESSAGE REDACTED - CONTAINS SENSITIVE KEYWORD]"

        payload: dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": msg,
            "event_name": getattr(record, "event_name", "UNSPECIFIED"),
            "request_id": getattr(record, "request_id", None),
            "trace_id": getattr(record, "trace_id", None),
            "span_id": getattr(record, "span_id", None),
            "workflow_execution_id": getattr(record, "workflow_execution_id", None),
            "task_execution_id": getattr(record, "task_execution_id", None),
            "attempt_id": getattr(record, "attempt_id", None),
            "duration_ms": getattr(record, "duration_ms", None),
        }

        if record.exc_info:
            exc_str = self.formatException(record.exc_info)
            for key in self.REDACTED_KEYS:
                if key in exc_str.lower():
                    exc_str = "[EXCEPTION TRACEBACK REDACTED - SENSITIVE VALUE DETECTED]"
            payload["exception"] = exc_str

        # Filter to allowed fields only
        clean_payload = {
            k: v for k, v in payload.items() if k in self.ALLOWED_FIELDS and v is not None
        }
        return json.dumps(clean_payload)
```

### 10.2 Prometheus Metrics Architecture & Cardinality Guards
Prometheus metrics are exposed via `GET /metrics` on an internal management listener. To prevent memory exhaustion and time-series explosion, high-cardinality identifiers are strictly prohibited as metric labels.

#### Cardinality Policy:
- **Forbidden Metric Labels:** `workflow_execution_id`, `task_execution_id`, `attempt_id`, `worker_session_id`, `definition_id`, `request_id`, `idempotency_key`, `principal_id`.
- **Allowed Metric Labels:** `http_method`, `route_template`, `status_code`, `workflow_state`, `task_state`, `attempt_state`, `failure_category`, `operation_type`.

#### Core Metric Catalog:
```python
from prometheus_client import Counter, Histogram, Gauge

HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "nexusflow_http_request_duration_seconds",
    "HTTP request latency by route template and status",
    ["method", "route", "status_code"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)

WORKFLOW_TRANSITION_TOTAL = Counter(
    "nexusflow_workflow_transition_total", "Workflow transitions by target state", ["to_state"]
)

TASK_TRANSITION_TOTAL = Counter(
    "nexusflow_task_transition_total", "Task transitions by target state", ["to_state"]
)

ATTEMPT_SETTLEMENT_TOTAL = Counter(
    "nexusflow_attempt_settlement_total",
    "Attempt settlement counts by trigger and result",
    ["trigger", "settlement_result"],
)

ACTIVE_WORKER_SESSIONS = Gauge(
    "nexusflow_active_worker_sessions", "Current live worker sessions in ephemeral memory"
)

DATABASE_OPERATION_DURATION_SECONDS = Histogram(
    "nexusflow_db_operation_duration_seconds",
    "PostgreSQL transaction latency by operation name",
    ["operation", "status"],
    buckets=(0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0),
)

OCC_CONFLICT_TOTAL = Counter(
    "nexusflow_occ_conflict_total",
    "Optimistic concurrency control conflict retries by entity",
    ["entity_type"],
)

RECOVERY_RUN_DURATION_SECONDS = Histogram(
    "nexusflow_recovery_run_duration_seconds",
    "Duration of mandatory startup recovery sweeps",
    ["phase"],
)
```

### 10.3 OpenTelemetry Tracing Architecture
- Tracing is instrumented using the OpenTelemetry Python SDK.
- **Trace Boundaries:** Traces are operation-scoped (e.g., `POST /v1/executions`, `scheduler_evaluation_pass`, `settle_worker_callback`, `recovery_phase_4`). Long-running workflows do not maintain a single continuous distributed trace context.
- **Trace Attributes:** Safe to include high-cardinality IDs (`workflow_execution_id`, `attempt_id`) in trace span attributes for root-cause analysis, but never in Prometheus metric labels.
- **Fail-Open Exporters:** If the OTLP collector or Jaeger is unreachable, spans are dropped silently without impacting orchestration.

---

## 11. Concrete Configuration Environment Variable Reference

All configuration maps from deployment environment variables prefixed with `NEXUSFLOW_`:

| Variable Name | Category | Type | Required? | Default / Example Placeholder | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `NEXUSFLOW_DB__URL` | Deployment | DSN | **Yes** | `postgresql+asyncpg://nexusflow_user:<DB_PASSWORD>@localhost:5432/nexusflow` | PostgreSQL 16 connection string |
| `NEXUSFLOW_DB__POOL_MIN_SIZE` | Process | Int | No | `5` | Minimum connection pool size |
| `NEXUSFLOW_DB__POOL_MAX_SIZE` | Process | Int | No | `20` | Maximum connection pool size |
| `NEXUSFLOW_HTTP__PORT` | Process | Int | No | `8000` | Public API HTTP listener port |
| `NEXUSFLOW_HTTP__MAX_PAYLOAD_BYTES` | Security | Int | No | `1048576` (1 MB) | Transport body size limit |
| `NEXUSFLOW_RETRY__FIXED_DELAY_SECONDS` | Operational | Float | No | `10.0` (Must be `> 0.0`) | Fixed retry delay applied to `RETRY_WAIT` |
| `NEXUSFLOW_WORKER__HEARTBEAT_TIMEOUT_SECONDS`| Operational | Float | No | `15.0` | Inactivity threshold for `WORKER_LOSS` |
| `NEXUSFLOW_WORKER__CLAIM_START_DEADLINE_SECONDS`| Operational | Float | No | `10.0` | Start timeout for `CLAIMED` attempts |
| `NEXUSFLOW_SHUTDOWN__GRACE_WINDOW_SECONDS` | Process | Float | No | `30.0` | Maximum settlement grace before exit |
| `NEXUSFLOW_SECURITY__WORKER_DOMAIN_TOKEN` | Secret | String | **Yes** | `<WORKER_BEARER_SECRET>` | Raw high-entropy worker bearer token (hashed at boot) |
| `NEXUSFLOW_SECURITY__PUBLIC_API_TOKENS` | Secret | JSON | **Yes** | `[{"principal_id": "client-1", "token": "<PUBLIC_CLIENT_SECRET>", "permissions": ["definitions:read","definitions:write"]}]`| Public client credentials and permissions (hashed at boot) |
| `NEXUSFLOW_OBSERVABILITY__OTLP_ENDPOINT` | Deployment | String | No | `http://localhost:4317` | OpenTelemetry collector endpoint |

---

## 12. Local Portfolio Deployment (Docker Compose)

For local development and portfolio demonstration, NexusFlow V1 packages a fully operational single-node deployment:

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:16-alpine
    container_name: nexusflow-postgres
    environment:
      POSTGRES_DB: nexusflow
      POSTGRES_USER: nexusflow_user
      POSTGRES_PASSWORD: nexusflow_password
    ports:
      - "5432:5432"
    volumes:
      - pgdata:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U nexusflow_user -d nexusflow"]
      interval: 5s
      timeout: 5s
      retries: 5

  nexusflow-control-plane:
    build:
      context: .
      dockerfile: Dockerfile
    container_name: nexusflow-control-plane
    command: ["uvicorn", "nexusflow.interfaces.http.app:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      NEXUSFLOW_DB__URL: postgresql+asyncpg://nexusflow_user:nexusflow_password@postgres:5432/nexusflow
      NEXUSFLOW_SECURITY__WORKER_DOMAIN_TOKEN: "<WORKER_BEARER_SECRET>"
      NEXUSFLOW_SECURITY__PUBLIC_API_TOKENS: '[{"principal_id": "client-dev", "token": "<PUBLIC_CLIENT_SECRET>", "permissions": ["definitions:read","definitions:write","executions:read","executions:start","executions:cancel"]}]'
      NEXUSFLOW_OBSERVABILITY__OTLP_ENDPOINT: "http://jaeger:4317"
    ports:
      - "8000:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8000/healthz"]
      interval: 5s
      timeout: 5s
      retries: 3

  prometheus:
    image: prom/prometheus:v2.45.0
    container_name: nexusflow-prometheus
    volumes:
      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:10.0.0
    container_name: nexusflow-grafana
    volumes:
      - ./deploy/grafana/dashboards:/etc/grafana/provisioning/dashboards
    ports:
      - "3000:3000"

  jaeger:
    image: jaegertracing/all-in-one:1.47
    container_name: nexusflow-jaeger
    ports:
      - "16686:16686"
      - "4317:4317"

volumes:
  pgdata:
```

---

## 13. Sequence Diagrams

### 13.1 Control-Plane Boot, Recovery & Readiness Sequence
```text
Process Init           PostgreSQL 16               LLD-07 RecoveryCoordinator       RuntimeSupervisor       FastAPI / Healthz
     │                       │                                 │                            │                       │
     │ 1. Load Settings      │                                 │                            │                       │
     │ 2. Init Logger/Pool   │                                 │                            │                       │
     ├──────────────────────>│ (SELECT 1 & Alembic Check)      │                            │                       │
     │<──────────────────────┤ (Compatible)                    │                            │                       │
     │ 3. Start ASGI Server  │                                 │                            │                       │
     ├───────────────────────┴─────────────────────────────────┴────────────────────────────┴──────────────────────>│
     │                                                                                                              │ 4. GET /readyz
     │                                                                                                              ├──────────────>
     │                                                                                                              │ 503 NOT_READY
     │                                                                                                              │<──────────────
     │ 5. Reconcile Startup State                                                                                   │
     ├────────────────────────────────────────────────────────>│                                                    │
     │                       │ 6. Keyset Sweeps (Phases 1-8)   │                                                    │
     │                       │<────────────────────────────────┤                                                    │
     │                       │    Committed Mutations          │                                                    │
     │                       ├────────────────────────────────>│                                                    │
     │                       │                                 │ 7. Recovery Converged                              │
     │<────────────────────────────────────────────────────────┴────────────────────────────────────────────────────┤
     │ 8. Start Background Loops (Critical & Non-Critical)                                  │                       │
     ├─────────────────────────────────────────────────────────────────────────────────────>│                       │
     │                                                                                      │ 9. Loops Running      │
     │                                                                                      │<──────────────────────┤
     │ 10. Open Recovery Gate (allows_new_work = True)                                                              │
     │ 11. State = SERVING                                                                                          │
     │                                                                                                              │ 12. GET /readyz
     │                                                                                                              ├──────────────>
     │                                                                                                              │ 200 OK Ready
     │                                                                                                              │<──────────────
```

### 13.2 Fail-Open Telemetry Execution Flow (PostgreSQL OCC Commit)
```text
Settlement Use Case           PostgreSQL 16 (OCC)             TelemetryPort               OTLP Collector / Jaeger
        │                             │                             │                                 │
        │ 1. OCC Conditional Commit   │                             │                                 │
        │    (State & Rev Check)      │                             │                                 │
        ├────────────────────────────>│                             │                                 │
        │    COMMITTED                │                             │                                 │
        │<────────────────────────────┤                             │                                 │
        │ 2. Transaction Closes       │                             │                                 │
        │                             │                             │                                 │
        │ 3. Emit Span / Metric (Outside DB Tx)                     │                                 │
        ├──────────────────────────────────────────────────────────>│                                 │
        │                                                           │ 4. Export Span (HTTP / gRPC)    │
        │                                                           ├────────────────────────────────>│
        │                                                           │    Connection Refused / Drop    │
        │                                                           │<────────────────────────────────┤
        │                                                           │ 5. Log Diagnostic (Fail Open)   │
        │ 6. Operation Returns 200 OK                               │                                 │
        │<──────────────────────────────────────────────────────────┴─────────────────────────────────┘
```

### 13.3 Graceful Shutdown Sequence
```text
OS Signal (SIGTERM)            Process Lifecycle Controller         RuntimeSupervisor         PostgreSQL 16 Engine Pool
        │                                    │                               │                            │
        │ 1. SIGTERM                         │                               │                            │
        ├───────────────────────────────────>│                               │                            │
        │                                    │ 2. Lifecycle = DRAINING       │                            │
        │                                    │ 3. Readiness = False (503)    │                            │
        │                                    │ 4. Block ALL Public Mutations │                            │
        │                                    │ 5. Stop Ownership Allocation  │                            │
        │                                    │                               │                            │
        │                                    │ 6. Await In-Flight Settlement │                            │
        │                                    │    (Grace Window: 30s)        │                            │
        │                                    ├──────────────────────────────>│                            │
        │                                    │                               │ 7. Active Callbacks Commit │
        │                                    │                               ├───────────────────────────>│
        │                                    │                               │<───────────────────────────┤
        │                                    │ 8. Cancel Background Loops    │                            │
        │                                    │    (Supervisor.stop())        │                            │
        │                                    │                               │                            │
        │                                    │ 9. Dispose AsyncEngine Pool   │                            │
        │                                    ├───────────────────────────────┴───────────────────────────>│
        │                                    │    Pool Disposed & Closed                                  │
        │                                    │<───────────────────────────────────────────────────────────┤
        │                                    │ 10. Exit Process (0)                                       │
        │<───────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

---

## 14. Deterministic Test Strategy

### 14.1 Configuration Validation & Deep Immutability Tests (`tests/unit/config/test_settings.py`)
1. **Nested Settings Immutability:** Attempt to assign `settings.retry.fixed_delay_seconds = 99`; assert `TypeError: "RetrySettings" is frozen`.
2. **Immutable Security Permissions:** Attempt to append to `settings.security.public_credentials[0].permissions`; assert `AttributeError: frozenset has no append` or `TypeError: cannot modify frozen dataclass`.
3. **Strictly Positive Retry Delay:** Configure `NEXUSFLOW_RETRY__FIXED_DELAY_SECONDS = 0.0`; assert validation fails with `greater_than = 0.0`.
4. **Missing Required Database URL:** Bootstrap without `NEXUSFLOW_DB__URL`; assert validation error; process fails closed.
5. **No Multiprocess Deployment:** Verify deployment configuration prohibits worker count $> 1$.
6. **Raw Credential Hashing at Bootstrap:** Supply raw Bearer secrets via `NEXUSFLOW_SECURITY__WORKER_DOMAIN_TOKEN` and `NEXUSFLOW_SECURITY__PUBLIC_API_TOKENS`; assert `settings.security` contains SHA-256 byte digests and does not retain the raw token strings.

### 14.2 Time & Retry Policy Tests (`tests/unit/runtime/test_clock_retry.py`)
7. **Fixed Retry Delay Resolution:** Using `FakeClock(T0)`, trigger retryable task failure with configured delay of 15s; assert `retry_ready_at_utc == T0 + 15s`.
8. **Durable Deadline Persistence:** Verify that persisted `retry_ready_at_utc` is preserved exactly across database reconnects without recalculation.

### 14.3 Startup & Readiness Tests (`tests/integration/runtime/test_startup_lifecycle.py`)
9. **Recovery Gate Closed at Boot:** Assert `/readyz` returns 503 and public mutations return 503 `NOT_READY` during startup recovery.
10. **Readiness Probe Flips on Convergence:** Boot against database with active workflows; assert `/readyz` returns 503 until Phase 8 completes, then returns 200 OK.
11. **Schema Incompatibility Halts Boot:** Alter Alembic migration head in database; assert control plane fails closed before declaring readiness.

### 14.4 Supervisor & Loop Failure Tests (`tests/integration/runtime/test_supervisor.py`)
12. **Critical Loop Crash Triggers Controlled Shutdown:**
    - Inject unexpected exception into `SchedulerWakeupLoop` (`is_critical=True`).
    - Assert `RuntimeSupervisor.is_healthy() == False`.
    - Assert `/readyz` immediately returns 503.
    - Assert fatal failure signal emitted exactly once and `ProcessLifecycleController` initiates `DRAINING`.
    - Assert public mutations are rejected with 503.
13. **Non-Critical Telemetry Loop Failure Does NOT Fail Readiness or Trigger Shutdown:**
    - Inject unexpected exception into `TelemetryExporterFlushLoop` (`is_critical=False`).
    - Assert sanitized diagnostic warning emitted in logs.
    - Assert `RuntimeSupervisor.is_healthy() == True`.
    - Assert `/readyz` remains 200 OK.
    - Assert `fatal_failure_event` remains unset.
    - Assert `ProcessLifecycle` remains `SERVING` and orchestration continues.
14. **Normal Shutdown Cancellation Does Not Trigger Fatal Failure:**
    - Initiate graceful shutdown via `supervisor.stop()`.
    - Background loops receive `asyncio.CancelledError`.
    - Assert supervisor does not log fatal failure or fire `fatal_failure_event`.
15. **Lost Wakeup Rediscovery:** Commit tasks in `RUNNABLE` without emitting in-memory wakeup; assert defensive rediscovery sweep picks up tasks within 2 seconds.
16. **Bounded Keyset Pagination in Sweeps:** Seed 250 due retry tasks; run retry sweeper with batch size 50; assert tasks are processed in ordered pages without gaps.

### 14.5 Observability & Redaction Tests (`tests/unit/observability/test_telemetry.py`)
17. **Structured Field Redaction:** Log event with `extra={"authorization": "Bearer secret_token"}`; assert emitted JSON has `authorization: "[REDACTED]"`.
18. **Formatting Arg Leakage Prevention:** Call `logger.error("Failed auth for %s", "Bearer secret_token")`; assert formatter redacts sensitive keyword or sanitizes output string.
19. **Exception Log Sanitization:** Throw exception containing database DSN password; assert emitted JSON exception field scrubs the credential.
20. **Metrics Cardinality Guard:** Verify Prometheus metrics contain zero high-cardinality labels (`workflow_execution_id`, `attempt_id`).
21. **Fail-Open Telemetry:** Configure dead OTLP endpoint; assert database transactions and API responses succeed with 200 OK.

### 14.6 Graceful Shutdown Tests (`tests/integration/runtime/test_shutdown.py`)
22. **SIGTERM Flips Readiness:** Send `SIGTERM` to running process; assert lifecycle becomes `DRAINING` and `/readyz` immediately returns 503.
23. **All Public Mutations Blocked During Drain:** Submit `POST /v1/executions` and `POST /v1/executions/{id}/cancel` during drain; assert both rejected with 503.
24. **Existing Callbacks Admitted During Drain:** External worker submits `POST /internal/v1/worker/callback` for active attempt during drain grace window; assert accepted and committed via OCC.
25. **No Workflow State Corruption on Shutdown:** Assert no workflows are transitioned to `FAILED` or `CANCELLED` merely because shutdown completed.

---

## 15. Explicit Non-Goals & Architecture Drift Prohibitions

LLD-09 strictly excludes:
1. **Redis / In-Memory Brokers:** No Redis caching, pub/sub, or Celery backends.
2. **Distributed Queues / Brokers:** No Kafka, RabbitMQ, SQS, or outbox dispatch tables.
3. **Clustering / Leader Election:** Single control-plane process ($N=1$); no Raft, etcd, or Consul.
4. **Dynamic Configuration / Hot Reload:** Configuration is strictly immutable after process startup.
5. **Synthetic Lifecycle States:** No `RECOVERING`, `STOPPED`, `DEGRADED`, or `TIMED_OUT` states in domain models.
6. **Task Retry Backoff Curves:** Fixed operational delay only; no exponential curves or jitter in V1.
7. **Telemetry Participation in Correctness:** Zero distributed transactions or gating based on tracing/metrics health.
8. **High-Cardinality Metric Labels:** Resource IDs are strictly prohibited from Prometheus labels.
9. **Automatic Production Schema Migrations:** Alembic migrations are executed out-of-band; normal boot verifies only.
10. **Durable Worker Registry:** The worker registry is ephemeral; restarts require fresh registration.

---

## 16. Package & Module Architecture

Consistent with LLD-01 and ADR-019:

```text
src/nexusflow/
    config/
        settings.py             # Deeply frozen Pydantic Settings models
        categories.py           # ADR-023 configuration categorization

    runtime/
        lifecycle.py            # ProcessLifecycle enum (SERVING, DRAINING, TERMINATING)
        supervisor.py           # RuntimeSupervisor with critical crash detection & non-critical fail-open
        bootstrap.py            # Process composition root & startup orchestrator
        shutdown.py             # Graceful shutdown signal coordinator
        timers.py               # In-memory priority timer heap coordinator
        clock.py                # SystemClock & Clock protocols

    observability/
        logging.py              # Sanitized JSON structured log formatter
        metrics.py              # Prometheus metrics registry & cardinality guards
        tracing.py              # OpenTelemetry tracer & span factory
        redaction.py            # Payload & secret sanitization filters

    interfaces/
        http/
            system/
                health.py       # GET /healthz and GET /readyz routes
                metrics.py      # GET /metrics route
```

---

## 17. Cross-LLD Traceability Matrix

| LLD-09 Concern | Authoritative ADR | Upstream LLD Dependency | Implementation Module | Test Suite Verification |
| :--- | :--- | :--- | :--- | :--- |
| Process Lifecycle & Drain | ADR-017 | LLD-04, 05, 06 | `runtime/lifecycle.py`, `runtime/shutdown.py` | `test_shutdown.py` |
| Startup Sequence & Recovery | ADR-012, 020 | LLD-07 (`RecoveryCoordinator`)| `runtime/bootstrap.py` | `test_startup_lifecycle.py` |
| Deeply Frozen Settings | ADR-023 | LLD-01, 08 | `config/settings.py` | `test_settings.py` |
| Fixed Positive Retry Delay Policy | ADR-007, 023 | LLD-06 (`retry_ready_at_utc`)| `config/settings.py`, `runtime/timers.py` | `test_clock_retry.py` |
| Clock & Time Authority | ADR-020 | LLD-01 (`Clock` Protocol) | `runtime/clock.py` | `test_clock_retry.py` |
| Supervised Background Loops | ADR-005, 008, 017| LLD-04, 05, 06, 07 | `runtime/supervisor.py` | `test_supervisor.py` |
| Structured Logging & Redaction| ADR-016, 022 | LLD-01, 08 | `observability/logging.py`, `redaction.py`| `test_telemetry.py` |
| Prometheus Metrics | ADR-016 | LLD-04, 05, 06, 08 | `observability/metrics.py` | `test_telemetry.py` |
| OpenTelemetry Tracing | ADR-016 | LLD-01, 03, 04, 06 | `observability/tracing.py` | `test_telemetry.py` |
| Docker Compose Environment | ADR-020 | LLD-02, 08 | `docker-compose.yml` | `test_startup_lifecycle.py` |

---

### Classification

**LLD-09 — Architecture-Ready / Approved as LLD-10 Input**
