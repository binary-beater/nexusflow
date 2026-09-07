# NexusFlow V1 — LLD-04: Scheduling, Routing & Ownership

**Document Status:** Architecture-Ready / Approved as LLD-05 Input  
**Authoritative References:** ADR-003 (Canonical Graph Representation), ADR-005 (Scheduler), ADR-006 (Workflow State Machine), ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-014 (History), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline).  
**Downstream Dependents:** LLD-05 (Worker Coordination, Registration & Heartbeat Protocol), LLD-06 (Settlement, Timers & Cancellation Protocol), LLD-07 (Recovery & Reconciliation).

---

## 1. Primary Objective & Architectural Separation

### 1.1 Objective Statement
This document defines the complete implementation-level design for NexusFlow V1 task readiness evaluation, transition-driven scheduling, worker capability routing, ephemeral candidate selection, durable ownership acquisition, and scheduler rediscovery.

Specifically, it answers:
> *Exactly how does NexusFlow discover that a durable TaskExecution is eligible, resolve its immutable input, transition it to RUNNABLE, locate compatible live WorkerSessions, form ephemeral routing candidates, select a candidate, and attempt the LLD-02 durable Ownership Commit without creating duplicate authoritative Attempts?*

The architecture strictly separates the scheduling and dispatch process into distinct, non-overlapping phases:

```text
Durable Workflow / Task State (PostgreSQL 16)
        │
        ▼ (Phase 1)
Readiness Evaluation & Stable Input Resolution (Pure Domain Function)
        │
        ▼ (Phase 2)
PENDING → RUNNABLE Transition (Durable Consistency Group, LLD-02)
        │
        ▼ (Phase 3)
RUNNABLE Rediscovery & Wakeup Coordination (In-Process Hints + Keyset Scan)
        │
        ▼ (Phase 4)
Worker Registry Snapshot & Compatibility Evaluation (Pure Domain Function)
        │
        ▼ (Phase 5)
Ephemeral Routing Candidate Formation (Advisory In-Memory Tuple)
        │
        ▼ (Phase 6)
Candidate Selection (Policy Strategy)
        │
        ▼ (Phase 7)
Immediate Worker Session Revalidation & Local Lifecycle Recheck (Memory Checks)
        │
        ▼ (Phase 8)
Ownership Commit (Durable Transaction, LLD-02 commit_attempt_ownership)
        │
        ▼
Task RUNNING + ExecutionAttempt CLAIMED (WorkerSessionId Authority)
        │
        ▼ (Phase 9)
Dispatch Execution Instruction Produced (Handed off to LLD-05 Worker HTTP Transport)
```

### 1.2 Core Architectural Distinctions
The system freezes the boundary between four fundamental concepts:

1. **Eligibility / Readiness:**
   - *Question:* May this `TaskExecution` become durable `RUNNABLE`?
   - *Authority:* Durable PostgreSQL state (`workflow_executions.state == RUNNING`, `task_executions.state == PENDING`, all direct declared dependencies `SUCCEEDED` with available outputs).
2. **Routing Compatibility:**
   - *Question:* Which currently live `WorkerSession` instances could execute this `RUNNABLE` Task?
   - *Authority:* Ephemeral in-process Worker Registry (`live == True`, `accepting_new_work == True`, exact byte-for-byte `task.activity_type == capability`).
3. **Candidate Selection:**
   - *Question:* Which compatible `WorkerSession` should this specific ownership attempt target?
   - *Authority:* Ephemeral `CandidateSelector` policy (e.g., round-robin or stable hash). Advisory only; conveys zero durable rights.
4. **Ownership Commit:**
   - *Question:* Did this `TaskExecution` durably acquire one authoritative `ExecutionAttempt` bound to one `WorkerSessionId`?
   - *Authority:* The single-winner ACID database transaction in PostgreSQL 16 (`commit_attempt_ownership`).

These concepts are never merged into a single pseudo-transaction.

### 1.3 Boundary Ownership
* **LLD-04 Owns:**
  - Transition-driven readiness evaluation and dependency-satisfaction checks.
  - Stable task input resolution from immutable definitions, workflow inputs, and upstream outputs.
  - Driving the `PENDING → RUNNABLE` durable transition (LLD-02).
  - Driving the `RETRY_WAIT → RUNNABLE` timer readiness trigger (LLD-02).
  - Dual-path scheduling: Primary transition-driven in-process wakeups and secondary bounded defensive keyset rediscovery for `PENDING` readiness repair, `RUNNABLE` routing, and due `RETRY_WAIT` timers.
  - Worker Registry read-side integration and snapshotting.
  - ActivityType exact capability matching.
  - Filtering for active sessions satisfying `live == True` and `accepting_new_work == True`.
  - Ephemeral `RoutingCandidate` generation.
  - Pluggable `CandidateSelector` policy port and simple V1 deterministic implementations.
  - Immediate pre-ownership memory revalidation of targeted worker sessions.
  - Process lifecycle admission verification (`ProcessLifecycleView`) preventing new ownership during `DRAINING`.
  - Pre-generating `AttemptId` (UUIDv4) and orchestrating the LLD-02 Ownership Commit.
  - Robust reconciliation of `UNKNOWN_OUTCOME` ownership commits using persisted database facts.
  - Handling ownership OCC contention and losers cleanly without failing tasks.
  - Managing no-compatible-worker states (task remains `RUNNABLE` without synthetic errors).
  - Bounded concurrency, task coalescing, and backpressure in the control plane.
  - Scheduling telemetry, structured logging, and race verification test suites.

* **LLD-04 Does NOT Own:**
  - Worker heartbeat protocol, interval, or timeout mechanics (owned by LLD-05).
  - Worker HTTP endpoints, pull/long-polling wire protocol, or HTTP response formatting (owned by LLD-05).
  - Activity worker sandbox, thread/process execution, or SDK runtime (owned by LLD-05).
  - Worker attempt start acknowledgement wire transaction (owned by LLD-05).
  - Task completion/failure callback settlement semantics (owned by LLD-06).
  - Backoff duration computation or retry curve algorithms (owned by LLD-06).
  - Attempt start deadline or execution timeout settlement (owned by LLD-06).
  - Cancellation execution protocol or signal dispatch to workers (owned by LLD-06).
  - Crash recovery startup reconciliation or cluster leader lease management (owned by LLD-07).
  - Public REST / HTTP API ingestion endpoints (owned by LLD-08).
  - Prometheus / OpenTelemetry exporter infrastructure setup (owned by LLD-09).

---

## 2. Durable Truth vs. Ephemeral Memory Authority

NexusFlow strictly adheres to ADR-011 and ADR-013: **PostgreSQL 16 is the sole durable authority for all workflow and task execution state.** Memory is advisory, transient, and rebuildable.

### 2.1 State Authority Taxonomy
| Concept | Durable / Ephemeral | Authoritative Source | Survives Process Crash? | Description & Constraints |
| :--- | :--- | :--- | :--- | :--- |
| **Workflow State** | Durable | `workflow_executions` (DB) | Yes | Authoritative lifecycle (`RUNNING`, `FAILING`, etc.). |
| **Task State** | Durable | `task_executions` (DB) | Yes | Authoritative state (`PENDING`, `RUNNABLE`, `RUNNING`). |
| **Stable Task Input** | Durable | `task_executions.stable_input` | Yes | Immutable once committed during `PENDING → RUNNABLE`. |
| **Retry Ready Timer** | Durable | `task_executions.retry_ready_at_utc` | Yes | Target timestamp determining when `RETRY_WAIT` is due. |
| **Execution Attempt** | Durable | `execution_attempts` (DB) | Yes | Authoritative execution unit (`CLAIMED`, `RUNNING`, etc.). |
| **Attempt Ownership** | Durable | `execution_attempts.worker_session_id` | Yes | Irrevocable binding of Attempt to runtime session. |
| **Task Revision** | Durable | `task_executions.revision` | Yes | Strictly monotonic integer protecting OCC mutations. |
| **Worker Registry** | Ephemeral | In-Process Memory Cache | No | Live sessions and capabilities; rebuilt via heartbeats. |
| **Worker Liveness** | Ephemeral | In-Process Memory Cache | No | Ephemeral heartbeat tracking; not transactionally locked in DB. |
| **Routing Candidate**| Ephemeral | Ephemeral Object | No | Transient advisory pair `(TaskExecutionId, WorkerSessionId)`. |
| **Candidate Scores** | Ephemeral | Memory Calculation | No | Selector heuristics; discarded immediately after selection. |
| **Scheduler Wakeup** | Ephemeral | `asyncio.Queue` / Bus | No | Best-effort scheduling hint; loss recovered by keyset sweep. |
| **PENDING Keyset Cursor** | Ephemeral | In-Process Memory | No | Local cursor for defensive readiness repair; resets safely. |
| **RUNNABLE Keyset Cursor**| Ephemeral | In-Process Memory | No | Local cursor for defensive routing scans; resets safely. |
| **RETRY_WAIT Keyset Cursor**| Ephemeral | In-Process Memory | No | Local cursor for defensive retry sweeps; resets safely. |
| **ProcessLifecycle State**| Ephemeral | In-Process Memory | No | Local node state (`SERVING`, `DRAINING`, `TERMINATING`). |
| **Definition Graph** | Ephemeral (Cached) | In-Process Memory Cache | No | Deterministically reconstructed from `validated_iws` JSONB. |

### 2.2 Rejection of Memory Ownership
Memory holds zero execution authority:
1. An in-memory decision to route a task to a worker does **not** grant the worker permission to execute.
2. If the control-plane crashes between candidate selection and the LLD-02 database commit, the candidate evaporates. On restart, the task remains `RUNNABLE` in PostgreSQL with zero attempts, completely unaffected.
3. No network message is ever sent to a worker until after the database transaction commits `ExecutionAttempt` in state `CLAIMED`.

---

## 3. Workflow Schedulability & Lifecycle Preconditions

### 3.1 Workflow-Level Gating Invariant
Conforming to ADR-006 and LLD-02:
$$\text{New Task Scheduling Permitted} \iff \text{WorkflowExecution.state} == \text{RUNNING}$$

No task may transition to `RUNNABLE`, and no `RUNNABLE` task may acquire an `ExecutionAttempt`, unless the parent workflow is observably and locked in state `RUNNING`:

| Workflow State | May Transition PENDING $\to$ RUNNABLE? | May Commit RETRY_WAIT $\to$ RUNNABLE? | May Commit Attempt Ownership (RUNNABLE $\to$ RUNNING)? | Rationale |
| :--- | :--- | :--- | :--- | :--- |
| `INITIALIZING` | **NO** | **NO** | **NO** | Definition expansion incomplete; membership not frozen. |
| `RUNNING` | **YES** | **YES** | **YES** | Active workflow execution. |
| `FAILING` | **NO** | **NO** | **NO** | Workflow draining due to fatal task failure; only cancellations settle. |
| `CANCELLING` | **NO** | **NO** | **NO** | Workflow draining due to external cancel; active work being cancelled. |
| `SUCCEEDED` | **NO** | **NO** | **NO** | Terminal success; graph execution complete. |
| `FAILED` | **NO** | **NO** | **NO** | Terminal failure; all tasks terminal. |
| `CANCELLED` | **NO** | **NO** | **NO** | Terminal cancellation; all tasks terminal. |

### 3.2 Anti-TOCTOU Database Protection
In PostgreSQL `READ COMMITTED` isolation, reading `workflow.state == RUNNING` in application memory does not guarantee the workflow is still `RUNNING` milliseconds later.

LLD-02 enforces that all scheduling write transactions (`commit_task_readiness`, `commit_retry_ready`, and `commit_attempt_ownership`) acquire an explicit row-level exclusive lock on `workflow_executions`:
```sql
SELECT state FROM workflow_executions
WHERE workflow_execution_id = :wf_id
FOR UPDATE;
```
If the workflow has transitioned to `FAILING` or `CANCELLING`, the transaction rolls back immediately and returns `CommitStatus.PRECONDITION_FAILED`. LLD-04 never schedules new work during workflow drain.

---

## 4. Pure Task Readiness & Stable Input Resolution

### 4.1 Dependency Satisfaction Invariant (ADR-005)
A task dependency is satisfied **if and only if** the direct declared upstream `TaskExecution` has reached terminal state `SUCCEEDED`.
- Tasks in `PENDING`, `RUNNABLE`, `RETRY_WAIT`, or `RUNNING` do **not** satisfy dependencies.
- Tasks in `FAILED` or `CANCELLED` do **not** satisfy dependencies. (A definitive failure triggers workflow failure direction, causing all non-terminal tasks to be cancelled during drain).
- **Exact Dependency Authority:** The `ValidatedWorkflowSpec` and its derived `CanonicalGraph` define the authoritative direct dependency set. The scheduler never infers dependencies from whatever upstream task rows exist in the database.

### 4.2 Whole-Value Input Binding Resolution (ADR-010)
When a task's dependencies are satisfied, its input parameters are resolved into a single canonical dictionary:
$$\text{stable\_input} = \{ \text{name}_i : \text{resolve}(\text{binding}_i) \}$$

Resolution rules per binding type:
1. **`LiteralBinding(value: JsonValue)`:**
   - The resolved value is the exact, deeply frozen JSON literal defined in the specification.
2. **`WorkflowInputBinding()`:**
   - The resolved value is the entire immutable `workflow_executions.workflow_input` payload.
3. **`TaskOutputBinding(upstream_task_id)`:**
   - The resolved value is the entire authoritative `task_executions.task_output` payload of the referenced direct upstream task.
   - **Precondition:** The upstream task must be `SUCCEEDED` and have `has_output == True`.
   - **JSON Null vs. Absence:** If the upstream task completed with `task_output = 'null'::jsonb`, the resolved value is JSON `null` (`None`). This is valid output and must never be treated as missing data.

### 4.3 Input Resolution Integrity Faults (ADR-010 / ADR-018)
If an upstream dependency is `SUCCEEDED` but its output is physically missing (`has_output == False` or row unreadable), or if an input binding references an upstream task that is not a direct dependency, this is a **fatal data integrity corruption**.

The scheduler must **never** leave the downstream task silently `PENDING` forever. It must escalate the failure:
- Emit an `INTEGRITY` error log with workflow and task correlation IDs.
- Raise a structured `SchedulingIntegrityError`.
- Initiate workflow failure direction via the appropriate failure settlement pathway.

### 4.4 Stable Task Input Immutability
The resolved task input dictionary is committed durably to `task_executions.stable_input` atomically with the `PENDING → RUNNABLE` transition.
- Once committed, `stable_input` is **strictly immutable**.
- All subsequent routing attempts, worker reassignments, transient network retries, and task retries (across multiple `ExecutionAttempt` instances) receive this exact, frozen business input.
- LLD-04 **never** re-resolves input for subsequent attempts.

### 4.5 Pure Readiness Evaluator Implementation
```python
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Mapping

from nexusflow.domain.definitions.bindings import (
    LiteralBinding,
    WorkflowInputBinding,
    TaskOutputBinding,
)
from nexusflow.domain.definitions.task_definition import TaskDefinition
from nexusflow.domain.values import (
    TaskDefinitionId,
    TaskExecutionId,
    WorkflowExecutionId,
    JsonValue,
    JsonObject,
)

class ReadinessReason(StrEnum):
    READY = "READY"
    WORKFLOW_NOT_RUNNING = "WORKFLOW_NOT_RUNNING"
    TASK_NOT_PENDING = "TASK_NOT_PENDING"
    DEPENDENCIES_INCOMPLETE = "DEPENDENCIES_INCOMPLETE"
    DEPENDENCY_NOT_SUCCEEDED = "DEPENDENCY_NOT_SUCCEEDED"
    OUTPUT_NOT_AVAILABLE = "OUTPUT_NOT_AVAILABLE"
    INPUT_RESOLUTION_CORRUPT = "INPUT_RESOLUTION_CORRUPT"

@dataclass(frozen=True, slots=True)
class DependencyStateSnapshot:
    task_definition_id: TaskDefinitionId
    state: str  # "PENDING", "RUNNABLE", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"
    has_output: bool
    output_value: JsonValue | None

@dataclass(frozen=True, slots=True)
class TaskReadinessSnapshot:
    workflow_id: WorkflowExecutionId
    workflow_state: str
    workflow_input: JsonValue
    task_id: TaskExecutionId
    task_definition_id: TaskDefinitionId
    task_state: str
    task_revision: int
    task_definition: TaskDefinition
    upstream_dependencies: Mapping[TaskDefinitionId, DependencyStateSnapshot]

@dataclass(frozen=True, slots=True)
class ReadinessDecision:
    ready: bool
    resolved_input: JsonObject | None
    reason: ReadinessReason
    diagnostic_message: str = ""

def evaluate_task_readiness(snapshot: TaskReadinessSnapshot) -> ReadinessDecision:
    """
    Pure, deterministic evaluation of task eligibility and stable input resolution.
    Does NOT perform I/O, lock acquisitions, or state mutations.
    """
    if snapshot.workflow_state != "RUNNING":
        return ReadinessDecision(
            ready=False,
            resolved_input=None,
            reason=ReadinessReason.WORKFLOW_NOT_RUNNING,
            diagnostic_message=f"Workflow is in state '{snapshot.workflow_state}', not 'RUNNING'.",
        )

    if snapshot.task_state != "PENDING":
        return ReadinessDecision(
            ready=False,
            resolved_input=None,
            reason=ReadinessReason.TASK_NOT_PENDING,
            diagnostic_message=f"Task is in state '{snapshot.task_state}', not 'PENDING'.",
        )

    # Verify all declared direct dependencies are SUCCEEDED with available output
    declared_deps = snapshot.task_definition.dependencies
    for dep_id in declared_deps:
        dep_state = snapshot.upstream_dependencies.get(dep_id)
        if dep_state is None:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.DEPENDENCIES_INCOMPLETE,
                diagnostic_message=f"Direct dependency '{dep_id.value}' has no state recorded.",
            )
        if dep_state.state != "SUCCEEDED":
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.DEPENDENCY_NOT_SUCCEEDED,
                diagnostic_message=f"Dependency '{dep_id.value}' is in state '{dep_state.state}', not 'SUCCEEDED'.",
            )
        if not dep_state.has_output:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.OUTPUT_NOT_AVAILABLE,
                diagnostic_message=f"Dependency '{dep_id.value}' succeeded but authoritative output is missing.",
            )

    # Resolve named task input bindings
    resolved_map: dict[str, JsonValue] = {}
    for param_name, binding in snapshot.task_definition.input_bindings.items():
        if isinstance(binding, LiteralBinding):
            resolved_map[param_name] = binding.value
        elif isinstance(binding, WorkflowInputBinding):
            resolved_map[param_name] = snapshot.workflow_input
        elif isinstance(binding, TaskOutputBinding):
            src_dep = snapshot.upstream_dependencies.get(binding.upstream_task_id)
            if src_dep is None or not src_dep.has_output:
                return ReadinessDecision(
                    ready=False,
                    resolved_input=None,
                    reason=ReadinessReason.INPUT_RESOLUTION_CORRUPT,
                    diagnostic_message=f"TaskOutput source '{binding.upstream_task_id.value}' output unreadable.",
                )
            resolved_map[param_name] = src_dep.output_value
        else:
            return ReadinessDecision(
                ready=False,
                resolved_input=None,
                reason=ReadinessReason.INPUT_RESOLUTION_CORRUPT,
                diagnostic_message=f"Unknown binding variant '{type(binding)}'.",
            )

    return ReadinessDecision(
        ready=True,
        resolved_input=MappingProxyType(resolved_map),
        reason=ReadinessReason.READY,
        diagnostic_message="All direct dependencies succeeded and input resolved successfully.",
    )
```

---

## 5. Dual-Path Scheduling Architecture: Push-Wakeup & Defensive Keyset Sweeps

NexusFlow employs a hybrid scheduling architecture conforming to ADR-005, ADR-020, and ADR-023:
1. **Primary Mechanism:** Transition-Driven In-Process Wakeups (push-based sub-millisecond dispatch).
2. **Defensive Mechanism:** Low-Frequency Bounded Keyset Sweeps (pull-based repair for lost wakeups, process crashes, and due timers).

```
                      [ External or Internal State Mutation ]
                                         │
                                         ▼
                     [ LLD-02 Database Commit (PostgreSQL 16) ]
                                         │
                         ┌───────────────┴───────────────┐
                         │                               │
       (Push: Immediate Sub-ms Latency)   (Pull: Defensive Recovery Safety)
                         │                               │
                         ▼                               ▼
              [ SchedulerWakeup Bus ]         [ Keyset Sweeper (Periodic) ]
                         │                    - Bounded PENDING readiness repair
                         │                    - Bounded RUNNABLE routing scan
                         │                    - Bounded RETRY_WAIT timer sweep
                         │                               │
                         └───────────────┬───────────────┘
                                         ▼
                           [ Bounded Scheduler Loop ]
                                         │
                                         ▼
                      [ Task Readiness / Runnable Routing ]
```

### 5.1 Primary Mechanism: In-Process Wakeup Bus
Normal execution progress is entirely push-driven. Wakeups are strictly ephemeral in-memory signals. A dropped, duplicate, or reordered wakeup must **never** compromise system correctness.

```python
from typing import Protocol
from nexusflow.domain.values import (
    ActivityType,
    TaskExecutionId,
    WorkflowExecutionId,
)

class SchedulerWakeup(Protocol):
    """
    Ephemeral asynchronous signal bus for scheduler notifications.
    Implementations use bounded, coalescing asyncio.Queue structures.
    """
    async def notify_workflow_started(self, workflow_id: WorkflowExecutionId) -> None:
        """Emitted when WorkflowExecution transitions INITIALIZING -> RUNNING."""
        ...

    async def notify_task_succeeded(
        self,
        workflow_id: WorkflowExecutionId,
        task_id: TaskExecutionId,
    ) -> None:
        """Emitted when a task commits SUCCEEDED, making dependents eligible."""
        ...

    async def notify_task_runnable(
        self,
        task_id: TaskExecutionId,
        activity_type: ActivityType,
    ) -> None:
        """Emitted when PENDING -> RUNNABLE or RETRY_WAIT -> RUNNABLE commits."""
        ...

    async def notify_worker_capability_available(self, activity_type: ActivityType) -> None:
        """Emitted when a new worker session registers or starts accepting work."""
        ...
```

### 5.2 Primary Wakeup Handling & Propagation Rules
1. **`WorkflowStarted(workflow_id)`:**
   - Evaluates readiness for all **root tasks** (tasks with 0 dependencies) in the workflow definition. In disconnected graphs with multiple components, all roots across all components are evaluated concurrently.
2. **`TaskSucceeded(workflow_id, task_id)`:**
   - Evaluates readiness **only** for direct downstream dependents ($O(1)$ lookup via `CanonicalGraph.get_downstream_dependents`). The scheduler does not scan unrelated tasks.
3. **`TaskRunnable(task_id, activity_type)`:**
   - Triggers the routing pipeline to match the `RUNNABLE` task against live workers advertising its `ActivityType`.
4. **`WorkerCapabilityAvailable(activity_type)`:**
   - Triggers targeted routing for `RUNNABLE` tasks requiring the newly available `ActivityType`.

### 5.3 Defensive Keyset Sweeps (Secondary Repair Path)
To ensure absolute resilience against lost wakeups, network partitions, or process restarts between database commit and memory notification, the scheduler maintains three distinct bounded keyset-paginated sweeps:

```python
from datetime import datetime
from typing import Protocol, Sequence
from nexusflow.domain.values import TaskExecutionId

@dataclass(frozen=True, slots=True)
class KeysetCursor:
    sort_time: datetime
    id_val: TaskExecutionId

class DefensiveRediscoveryPort(Protocol):
    async def sweep_pending_readiness_candidates(
        self,
        cursor: KeysetCursor | None,
        batch_size: int,
    ) -> tuple[Sequence[TaskExecutionId], KeysetCursor | None]:
        """
        Defensive readiness repair: Discovers PENDING tasks in RUNNING workflows
        that may have missed a WorkflowStarted or TaskSucceeded wakeup.
        Queries:
            SELECT t.created_at_utc, t.task_execution_id
            FROM task_executions t
            JOIN workflow_executions w ON t.workflow_execution_id = w.workflow_execution_id
            WHERE t.state = 'PENDING'
              AND w.state = 'RUNNING'
              AND (t.created_at_utc, t.task_execution_id) > (:c_time, :c_id)
            ORDER BY t.created_at_utc ASC, t.task_execution_id ASC
            LIMIT :batch_size;
        """
        ...

    async def sweep_runnable_tasks(
        self,
        cursor: KeysetCursor | None,
        batch_size: int,
    ) -> tuple[Sequence[TaskExecutionId], KeysetCursor | None]:
        """
        Defensive routing repair: Discovers RUNNABLE tasks that may have missed a TaskRunnable wakeup.
        Queries:
            SELECT created_at_utc, task_execution_id FROM task_executions
            WHERE state = 'RUNNABLE'
              AND (created_at_utc, task_execution_id) > (:c_time, :c_id)
            ORDER BY created_at_utc ASC, task_execution_id ASC
            LIMIT :batch_size;
        """
        ...

    async def sweep_due_retry_tasks(
        self,
        now_utc: datetime,
        cursor: KeysetCursor | None,
        batch_size: int,
    ) -> tuple[Sequence[TaskExecutionId], KeysetCursor | None]:
        """
        Defensive retry timer repair: Discovers overdue RETRY_WAIT tasks.
        Queries:
            SELECT retry_ready_at_utc, task_execution_id FROM task_executions
            WHERE state = 'RETRY_WAIT'
              AND retry_ready_at_utc <= :now_utc
              AND (retry_ready_at_utc, task_execution_id) > (:c_time, :c_id)
            ORDER BY retry_ready_at_utc ASC, task_execution_id ASC
            LIMIT :batch_size;
        """
        ...
```
- **Defensive PENDING Sweep Contract:** Selecting a `PENDING` row does **not** mean it is ready. It merely feeds the task ID into `load_readiness_snapshot()` and `evaluate_task_readiness()`. The pure evaluator and LLD-02 `commit_task_readiness` remain the sole authorities.
- **Starvation Avoidance:** All three sweeps use composite keyset ordering `(timestamp, id)`. Cursors are ephemeral in-process variables that reset to `None` upon reaching the end of the table.

---

## 6. Worker Compatibility Matching & Ephemeral Routing

Task routing matches a durable `RUNNABLE` task with an available worker session without creating database records.

### 6.1 Worker Session Identity & Authority (ADR-008)
- **`WorkerSessionId`:** The unique UUID assigned to a specific runtime process incarnation of a worker. This is the **authoritative routing and ownership target**.
- **`WorkerId`:** A human-readable identifier (e.g., `"worker-host-prod-04"`). It is purely descriptive and holds **zero** routing or ownership authority.
- Routing candidates target `WorkerSessionId` exclusively.

### 6.2 Worker Registry Read Model
LLD-04 interacts with the Worker Registry via an immutable read snapshot:

```python
from dataclasses import dataclass
from typing import Mapping, Collection
from nexusflow.domain.values import (
    ActivityType,
    WorkerId,
    WorkerSessionId,
    TaskExecutionId,
)

@dataclass(frozen=True, slots=True)
class WorkerSessionSnapshot:
    session_id: WorkerSessionId
    worker_id: WorkerId
    capabilities: frozenset[ActivityType]
    live: bool
    accepting_new_work: bool

class WorkerRegistryView(Protocol):
    def get_session(self, session_id: WorkerSessionId) -> WorkerSessionSnapshot | None:
        ...

    def get_all_sessions(self) -> Mapping[WorkerSessionId, WorkerSessionSnapshot]:
        ...

    def get_compatible_sessions(self, activity_type: ActivityType) -> Sequence[WorkerSessionSnapshot]:
        ...
```

### 6.3 Routing Compatibility Invariants
A `WorkerSession` is compatible with a `RUNNABLE` task if and only if it satisfies all three conditions simultaneously:
1. **Liveness:** `worker.live == True` (heartbeat is currently valid in the registry).
2. **Admission Status:** `worker.accepting_new_work == True` (worker is not draining or terminating).
3. **Exact Capability Match:**
   $$\text{task.activity\_type} \in \text{worker.capabilities}$$
   Matching is strictly byte-for-byte exact. Prefix matching, wildcards, regex patterns, case-folding, and hierarchies are prohibited.

### 6.4 The Ephemeral Routing Candidate
When a compatible worker is identified, an advisory candidate is created:

```python
@dataclass(frozen=True, slots=True)
class RoutingCandidate:
    task_execution_id: TaskExecutionId
    worker_session_id: WorkerSessionId
```
**Critical Invariant (ADR-009):**
- A `RoutingCandidate` creates **NO** database attempt.
- A `RoutingCandidate` acquires **NO** database lock.
- A `RoutingCandidate` consumes **NO** attempt budget or ordinal.
- It is a transient, in-memory reference that exists only within the control-plane process.

### 6.5 Pure Compatibility Matcher
```python
def find_compatible_candidates(
    task_id: TaskExecutionId,
    required_activity: ActivityType,
    sessions: Collection[WorkerSessionSnapshot],
) -> tuple[RoutingCandidate, ...]:
    """
    Pure compatibility matching. Evaluates capabilities and availability flags.
    Returns immutable tuple of advisory RoutingCandidates.
    """
    candidates: list[RoutingCandidate] = []
    for s in sessions:
        if s.live and s.accepting_new_work and (required_activity in s.capabilities):
            candidates.append(RoutingCandidate(task_execution_id=task_id, worker_session_id=s.session_id))
    return tuple(candidates)
```

### 6.6 No Worker Available Behavior
If a `RUNNABLE` task has zero compatible, live, and accepting workers:
- The task remains in state `RUNNABLE`.
- No error is recorded, no attempt ordinal is consumed, and no history entry is written.
- There is **no mandatory routing timeout** in V1.
- The task safely waits in PostgreSQL until a compatible worker registers or begins accepting work, at which point an `ActivityTypeAvailable` wakeup re-evaluates routing.

---

## 7. Candidate Selection Policy Port

ADR-009 intentionally keeps the candidate selection algorithm pluggable. LLD-04 defines a clean policy interface:

```python
from typing import Protocol, Sequence

@dataclass(frozen=True, slots=True)
class RunnableTaskSnapshot:
    workflow_id: WorkflowExecutionId
    task_execution_id: TaskExecutionId
    task_definition_id: TaskDefinitionId
    task_revision: int
    activity_type: ActivityType
    stable_input: JsonObject
    next_attempt_ordinal: int
    max_attempts: int

class CandidateSelector(Protocol):
    """
    Strategy interface for picking a single candidate from a compatible set.
    """
    def choose(
        self,
        task: RunnableTaskSnapshot,
        candidates: Sequence[RoutingCandidate],
    ) -> RoutingCandidate | None:
        ...
```

### 7.1 Simple V1 Deterministic Round-Robin Selector
In V1, a single-control-plane process uses an in-memory round-robin pointer per `ActivityType`:

```python
class RoundRobinCandidateSelector:
    """
    Deterministic round-robin candidate selector across live sessions.
    Maintains an in-memory cursor per ActivityType.
    """
    def __init__(self) -> None:
        self._cursors: dict[str, int] = {}

    def choose(
        self,
        task: RunnableTaskSnapshot,
        candidates: Sequence[RoutingCandidate],
    ) -> RoutingCandidate | None:
        if not candidates:
            return None

        # Sort candidates deterministically by WorkerSessionId UUID string
        sorted_candidates = sorted(candidates, key=lambda c: str(c.worker_session_id.value))
        act_key = task.activity_type.name

        idx = self._cursors.get(act_key, 0) % len(sorted_candidates)
        selected = sorted_candidates[idx]

        # Advance cursor
        self._cursors[act_key] = (idx + 1) % len(sorted_candidates)
        return selected
```

---

## 8. Graceful Shutdown & Immediate Pre-Commit Revalidations

### 8.1 Process Lifecycle Admission Port (ADR-017)
To prevent new work allocation during server termination, the scheduler consumes the local process lifecycle view:

```python
class ProcessLifecycleView(Protocol):
    def accepts_new_ownership(self) -> bool:
        """Returns True only when the local process is in SERVING state."""
        ...
```

### 8.2 Two-Stage Pre-Commit Memory Verification
Worker liveness and process lifecycle exist in ephemeral memory. PostgreSQL transactions cannot lock memory states. Therefore, the scheduler performs two immediate memory checks immediately before executing the database commit:

```
Candidate Selected
        │
        ▼
[ Check 1: Worker Session Revalidation ]
        │
        ├─────────────────────────────────┐ (Worker dead or draining)
        │ (Worker confirmed live & ready) ▼
        ▼                            Discard Candidate; Task remains RUNNABLE
[ Check 2: Process Lifecycle Recheck ]
        │
        ├─────────────────────────────────┐ (Local process DRAINING)
        │ (Process SERVING)               ▼
        ▼                            Abort Ownership; Return PROCESS_DRAINING
[ Step 3: Durable DB Commit ]        Task remains RUNNABLE
        │
        ├─────────────────────────────────┐ (OCC Conflict / Predicate Failure)
        │ (COMMITTED)                     ▼
        ▼                            Re-read durable state; Zero duplicate Attempts
Task RUNNING + Attempt CLAIMED
        │
        ▼
[ Step 4: Produce Dispatch Instruction ]
(Handed to LLD-05 Worker HTTP Transport)
```

### 8.3 Unavoidable Shutdown Race & Bounded In-Flight Settling
Because process lifecycle is local state, a race can occur:
$$\text{Recheck observes SERVING} \longrightarrow \text{DRAINING begins} \longrightarrow \text{DB transaction commits}$$
- This race is unavoidable without distributed locking (which is prohibited).
- Conforming to ADR-017: **Bounded in-flight database transactions are permitted to settle.**
- Once `DRAINING` is observed by the scheduler, it initiates **zero** additional ownership attempts.
- If an ownership transaction commits during shutdown, the attempt is durably recorded in PostgreSQL as `CLAIMED`. The engine does not rollback or cancel workflows merely because the server is shutting down. On restart, LLD-07 recovery will reconcile the attempt.

---

## 9. Durable Ownership Acquisition & Unknown-Outcome Reconciliation

### 9.1 Ownership Commit Mechanics (LLD-02 Integration)
Ownership is committed via `commit_attempt_ownership` in a single ACID transaction:
1. Locks `workflow_executions` row `FOR UPDATE` and verifies `state == 'RUNNING'`.
2. Updates `task_executions`:
   - Checks `state == 'RUNNABLE'` and `revision == expected_revision`.
   - Transitions `state = 'RUNNING'`.
   - Increments `revision = revision + 1`.
   - Increments `next_attempt_ordinal = next_attempt_ordinal + 1`.
   - Durably verifies attempt budget: fails if `next_attempt_ordinal > max_attempts`.
3. Inserts a new row into `execution_attempts`:
   - `attempt_id = pre_generated_attempt_id`
   - `state = 'CLAIMED'`
   - `attempt_ordinal = allocated_ordinal`
   - `worker_session_id = target_worker_session_id`
   - `start_deadline_utc = now_utc + start_timeout`
4. Writes an audit row to `history_entries`: `event_category = 'TaskClaimedByWorker'`.

### 9.2 Robust Unknown Commit Reconciliation Protocol
When a database commit returns `UNKNOWN_OUTCOME` (due to connection reset or timeout), the scheduler executes an **operation-specific reconciliation query**:

```python
@dataclass(frozen=True, slots=True)
class ReconciledAttemptOwnership:
    is_committed: bool
    attempt_id: AttemptId
    task_execution_id: TaskExecutionId
    workflow_execution_id: WorkflowExecutionId
    worker_session_id: WorkerSessionId
    attempt_ordinal: int
    start_deadline_utc: datetime
```

**Reconciliation Invariants:**
1. **Identity Verification:** The reconciled row must match `expected_attempt_id`, `task_execution_id`, and `worker_session_id`. If an attempt exists with contradictory task or worker bindings, an `INTEGRITY` error is raised.
2. **Authoritative Durable Facts:** If committed, the `DispatchInstruction` is constructed from the persisted `start_deadline_utc` and `attempt_ordinal` loaded from the database, **never** from the caller's stale pre-commit variables.
3. **Absent Reconciliation Handling:** If reconciliation proves the Attempt row does not exist, the scheduler does not blindly assume transient failure. It re-reads the durable task state. If the task is still `RUNNABLE`, ownership may be re-evaluated under normal OCC.
4. **Worker Lost Post-Reconciliation:** If reconciliation confirms the attempt committed, but the worker session died during the network delay:
   - The attempt is **durably committed**.
   - The ownership is **never rolled back or undone**.
   - The worker transport layer will decline dispatch, and downstream start-deadline expiration (LLD-06) will settle the abandoned attempt.

### 9.3 Ownership Coordinator Implementation
```python
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from enum import StrEnum

class OwnershipOutcomeStatus(StrEnum):
    ACQUIRED = "ACQUIRED"
    NO_COMPATIBLE_WORKER = "NO_COMPATIBLE_WORKER"
    WORKER_STALE_BEFORE_COMMIT = "WORKER_STALE_BEFORE_COMMIT"
    PROCESS_DRAINING = "PROCESS_DRAINING"
    OWNERSHIP_LOST_OCC = "OWNERSHIP_LOST_OCC"
    WORKFLOW_DRAINING = "WORKFLOW_DRAINING"
    PRECONDITION_FAILED = "PRECONDITION_FAILED"
    UNKNOWN_RECONCILED_COMMITTED = "UNKNOWN_RECONCILED_COMMITTED"
    TRANSIENT_DB_FAILURE = "TRANSIENT_DB_FAILURE"

@dataclass(frozen=True, slots=True)
class DispatchInstruction:
    """
    Authoritative instruction produced only AFTER durable ownership commit.
    Delivered to LLD-05 worker HTTP communication layer.
    """
    attempt_id: AttemptId
    attempt_ordinal: int
    task_execution_id: TaskExecutionId
    workflow_execution_id: WorkflowExecutionId
    worker_session_id: WorkerSessionId
    activity_type: ActivityType
    stable_input: JsonObject
    start_deadline_utc: datetime

@dataclass(frozen=True, slots=True)
class OwnershipResult:
    status: OwnershipOutcomeStatus
    instruction: DispatchInstruction | None = None
    diagnostic_message: str = ""

class OwnershipCoordinator:
    """
    Coordinates candidate selection, immediate revalidation, lifecycle checks,
    and LLD-02 ownership commit.
    """
    def __init__(
        self,
        registry_view: WorkerRegistryView,
        selector: CandidateSelector,
        persistence_port: SchedulingPersistencePort,
        lifecycle_view: ProcessLifecycleView,
        start_timeout_seconds: float = 30.0,  # Provisional V1 default
    ) -> None:
        self._registry = registry_view
        self._selector = selector
        self._persistence = persistence_port
        self._lifecycle = lifecycle_view
        self._start_timeout = timedelta(seconds=start_timeout_seconds)

    async def try_acquire_ownership(
        self,
        task: RunnableTaskSnapshot,
        now_utc: datetime,
    ) -> OwnershipResult:
        # Check 1: Process lifecycle admission check
        if not self._lifecycle.accepts_new_ownership():
            return OwnershipResult(
                status=OwnershipOutcomeStatus.PROCESS_DRAINING,
                diagnostic_message="Local process is DRAINING; rejecting new ownership.",
            )

        # Step 2: Compatibility match against live worker registry
        compatible_sessions = self._registry.get_compatible_sessions(task.activity_type)
        candidates = find_compatible_candidates(task.task_execution_id, task.activity_type, compatible_sessions)
        if not candidates:
            return OwnershipResult(
                status=OwnershipOutcomeStatus.NO_COMPATIBLE_WORKER,
                diagnostic_message=f"Zero live workers for activity '{task.activity_type.name}'.",
            )

        # Step 3: Candidate selection via policy
        selected = self._selector.choose(task, candidates)
        if selected is None:
            return OwnershipResult(
                status=OwnershipOutcomeStatus.NO_COMPATIBLE_WORKER,
                diagnostic_message="Candidate selector returned no choice.",
            )

        # Step 4: Immediate pre-commit worker revalidation
        session_snap = self._registry.get_session(selected.worker_session_id)
        if (
            session_snap is None
            or not session_snap.live
            or not session_snap.accepting_new_work
            or (task.activity_type not in session_snap.capabilities)
        ):
            return OwnershipResult(
                status=OwnershipOutcomeStatus.WORKER_STALE_BEFORE_COMMIT,
                diagnostic_message=f"WorkerSession '{selected.worker_session_id}' became stale before commit.",
            )

        # Step 5: Immediate pre-commit process lifecycle recheck
        if not self._lifecycle.accepts_new_ownership():
            return OwnershipResult(
                status=OwnershipOutcomeStatus.PROCESS_DRAINING,
                diagnostic_message="Process transitioned to DRAINING immediately before commit.",
            )

        # Step 6: Pre-generate AttemptId application-side
        new_attempt_id = AttemptId(value=uuid4())
        start_deadline = now_utc + self._start_timeout

        # Step 7: Execute LLD-02 atomic ownership transaction
        commit_res, allocated_ordinal = await self._persistence.commit_attempt_ownership(
            task_id=task.task_execution_id,
            expected_task_revision=task.task_revision,
            workflow_id=task.workflow_id,
            worker_session_id=selected.worker_session_id,
            new_attempt_id=new_attempt_id,
            start_deadline_utc=start_deadline,
            now_utc=now_utc,
        )

        if commit_res.status == CommitStatus.COMMITTED:
            assert allocated_ordinal is not None
            instruction = DispatchInstruction(
                attempt_id=new_attempt_id,
                attempt_ordinal=allocated_ordinal,
                task_execution_id=task.task_execution_id,
                workflow_execution_id=task.workflow_id,
                worker_session_id=selected.worker_session_id,
                activity_type=task.activity_type,
                stable_input=task.stable_input,
                start_deadline_utc=start_deadline,
            )
            return OwnershipResult(status=OwnershipOutcomeStatus.ACQUIRED, instruction=instruction)

        elif commit_res.status == CommitStatus.PRECONDITION_FAILED:
            # Re-read durable workflow state to classify failure accurately
            wf_state = await self._persistence.get_workflow_state(task.workflow_id)
            if wf_state in ["FAILING", "CANCELLING"]:
                return OwnershipResult(
                    status=OwnershipOutcomeStatus.WORKFLOW_DRAINING,
                    diagnostic_message=f"Owning workflow is in drain state '{wf_state}'.",
                )
            return OwnershipResult(
                status=OwnershipOutcomeStatus.PRECONDITION_FAILED,
                diagnostic_message=f"Ownership precondition failed: {commit_res.message}",
            )

        elif commit_res.status == CommitStatus.OCC_CONFLICT:
            return OwnershipResult(
                status=OwnershipOutcomeStatus.OWNERSHIP_LOST_OCC,
                diagnostic_message="Concurrent transition modified task revision.",
            )

        elif commit_res.status == CommitStatus.UNKNOWN_OUTCOME:
            # Reconcile unknown outcome via LLD-02 pre-generated AttemptId query
            reconciled = await self._persistence.reconcile_attempt_ownership(new_attempt_id)
            if reconciled.is_committed:
                # Validate exact ownership identity
                if (
                    reconciled.task_execution_id != task.task_execution_id
                    or reconciled.worker_session_id != selected.worker_session_id
                ):
                    raise SchedulingIntegrityError(f"Reconciled attempt '{new_attempt_id}' contradicts target bindings.")

                # Reconstruct instruction strictly from durable facts
                instruction = DispatchInstruction(
                    attempt_id=new_attempt_id,
                    attempt_ordinal=reconciled.attempt_ordinal,
                    task_execution_id=reconciled.task_execution_id,
                    workflow_execution_id=reconciled.workflow_execution_id,
                    worker_session_id=reconciled.worker_session_id,
                    activity_type=task.activity_type,
                    stable_input=task.stable_input,
                    start_deadline_utc=reconciled.start_deadline_utc,
                )
                return OwnershipResult(status=OwnershipOutcomeStatus.UNKNOWN_RECONCILED_COMMITTED, instruction=instruction)
            
            # Absent attempt: Re-read durable task state
            task_row = await self._persistence.get_task_snapshot(task.task_execution_id)
            if task_row is not None and task_row.state == "RUNNABLE":
                return OwnershipResult(
                    status=OwnershipOutcomeStatus.TRANSIENT_DB_FAILURE,
                    diagnostic_message="Attempt absent; task remains RUNNABLE for re-routing.",
                )
            return OwnershipResult(status=OwnershipOutcomeStatus.TRANSIENT_DB_FAILURE)

        return OwnershipResult(status=OwnershipOutcomeStatus.TRANSIENT_DB_FAILURE)
```

---

## 10. Concurrency Races & Recovery Settlements

### 10.1 Cancellation Race vs. Readiness
- **Scenario:** A workflow cancel request is processed concurrently with a downstream task readiness check.
- **Race:** The readiness evaluator observes `workflow.state == RUNNING`. Concurrently, the cancellation handler commits `workflow.state = 'CANCELLING'`. The readiness transaction attempts `commit_task_readiness`.
- **Settlement:** The transaction locks `workflow_executions` `FOR UPDATE`, observes `state == 'CANCELLING'`, rolls back, and returns `PRECONDITION_FAILED`. The task remains `PENDING` until drain cancellation commits it as `CANCELLED`.

### 10.2 Cancellation Race vs. Ownership Commit
- **Scenario:** A worker candidate is selected while the workflow is `RUNNING`. Before the database transaction begins, a workflow cancellation or failure direction commits.
- **Settlement:** `commit_attempt_ownership` verifies `workflow.state == 'RUNNING'` under row lock. The predicate fails, the transaction aborts, and **zero attempts are created**.

### 10.3 Retry Readiness vs. Cancellation
- **Scenario:** A task in `RETRY_WAIT` reaches its `retry_ready_at_utc` deadline. Concurrently, the workflow is cancelled.
- **Settlement:** `commit_retry_ready` verifies `workflow.state == 'RUNNING'`. If the workflow is `CANCELLING` or `FAILING`, the transition to `RUNNABLE` is rejected. Drain cancellation settles the `RETRY_WAIT` task directly to `CANCELLED`.

### 10.4 Crash After Upstream Success Before Wakeup (Defensive PENDING Sweep)
- **Scenario:** Task A commits `SUCCEEDED` in PostgreSQL. The control plane crashes before posting `TaskSucceeded` to the wakeup bus. Downstream Task B remains `PENDING`.
- **Settlement:** On restart, the defensive PENDING keyset sweeper discovers Task B. The normal readiness snapshot is loaded, Task A's committed output is read, and Task B commits `PENDING → RUNNABLE`. Zero duplicate history entries are created beyond the winning transition.

### 10.5 Crash After Ownership Commit Before Dispatch
- **Scenario:** `commit_attempt_ownership` commits `RUNNABLE → RUNNING` and creates `ExecutionAttempt(state='CLAIMED')`. The control plane crashes before transmitting the dispatch instruction to the worker.
- **Settlement:** The attempt is durable in PostgreSQL with an active `start_deadline_utc`. When the deadline expires without worker start acknowledgement, LLD-06/LLD-07 recovery settles the attempt as `FAILED` (under `START_DEADLINE_EXPIRED`) and initiates a retry or workflow failure according to budget. LLD-04 never invents an unrecorded second attempt.

---

## 11. Persistence Repository & Schema Preservation

Conforming strictly to LLD-02:
- **No Schema Mutations:** LLD-04 does **not** add an `activity_type` column to `task_executions`. `task_executions` stores `task_definition_id`.
- **Activity Resolution:** The scheduler resolves `ActivityType` via the in-process `ValidatedWorkflowSpec` definition cache loaded from `registered_definitions.validated_iws`.
- **Keyset Starvation Safety:** All queries use bounded keyset pagination. Cursors are strictly ephemeral and reset cleanly when a sweep wraps.

```python
from typing import Protocol, Sequence
from nexusflow.domain.values import (
    ActivityType,
    TaskExecutionId,
    WorkflowExecutionId,
    TaskDefinitionId,
)

class SchedulingReadPort(Protocol):
    async def load_readiness_snapshot(
        self,
        workflow_id: WorkflowExecutionId,
        task_id: TaskExecutionId,
    ) -> TaskReadinessSnapshot | None:
        """Loads workflow state, workflow input, and exact direct dependency states in one query."""
        ...

    async def load_runnable_task(
        self,
        task_id: TaskExecutionId,
    ) -> RunnableTaskSnapshot | None:
        """Loads a single RUNNABLE task snapshot and resolves ActivityType from ValidatedSpec cache."""
        ...

    async def find_runnable_tasks(
        self,
        cursor: KeysetCursor | None,
        batch_size: int,
    ) -> tuple[Sequence[RunnableTaskSnapshot], KeysetCursor | None]:
        """Loads bounded RUNNABLE task snapshots using keyset pagination."""
        ...
```

---

## 12. Observability, Telemetry & Logging Boundaries

### 12.1 Metric Emission Guidelines
Conforming to ADR-016 and ADR-023:
- Metrics must be low-cardinality counters and histograms.
- High-cardinality identifiers (`WorkflowExecutionId`, `TaskExecutionId`, `AttemptId`, `WorkerSessionId`) are strictly prohibited in metric labels.
- `activity_type` is permitted as a label **only** when bounded or explicitly configured; unbounded dynamic activity types must be aggregated into an `"other"` bucket.

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `nexusflow_scheduler_readiness_eval_total` | Counter | `result` (`ready`, `not_ready`) | Total readiness evaluations performed. |
| `nexusflow_scheduler_readiness_commits_total`| Counter | `status` (`committed`, `conflict`) | Durable `PENDING -> RUNNABLE` transitions. |
| `nexusflow_scheduler_runnable_tasks` | Gauge | `activity_type` (bounded) | Current count of tasks in `RUNNABLE` state. |
| `nexusflow_scheduler_routing_candidates` | Histogram | `activity_type` (bounded) | Number of compatible workers per routing pass. |
| `nexusflow_scheduler_no_worker_total` | Counter | `activity_type` (bounded) | Routing passes finding 0 compatible workers. |
| `nexusflow_scheduler_ownership_attempts_total`| Counter| `status` (`committed`, `occ`, `stale`) | Total database ownership commit attempts. |
| `nexusflow_scheduler_rediscovery_sweeps_total`| Counter| `sweep_type` (`pending`, `runnable`, `retry`)| Total defensive keyset sweeps executed. |

### 12.2 Structured Logging & Privacy
Logs must include correlation contexts (`workflow_id`, `task_id`) but must **never** log business payload contents:
- Prohibited: `workflow_input`, `task_output`, `stable_input`, literal values, or worker authentication headers.
- Permitted: State transitions, revision numbers, worker session IDs, and error diagnostics.

---

## 13. Sequence Diagrams

### 13.1 Upstream Success & Downstream Readiness
```
WorkerSession           LLD-02 Persistence         SchedulerService          ReadinessEvaluator
      │                         │                         │                          │
      │ 1. Complete Task A      │                         │                          │
      ├────────────────────────>│                         │                          │
      │    (commit_task_success)│                         │                          │
      │    Task A: SUCCEEDED    │                         │                          │
      │                         ├────────────────────────>│                          │
      │                         │ 2. Wakeup: TaskASucceeded                          │
      │                         │    (Lookup Downstream)  │                          │
      │                         │    Task B depends on A  │                          │
      │                         │                         ├─────────────────────────>│
      │                         │                         │ 3. evaluate_readiness(B) │
      │                         │                         │    Inspect Task A output │
      │                         │                         │    Resolve Stable Input  │
      │                         │                         │<─────────────────────────┤
      │                         │                         │    ReadyDecision: READY  │
      │                         │ 4. commit_task_readiness│                          │
      │                         │<────────────────────────┤                          │
      │                         │    Status: COMMITTED    │                          │
```

### 13.2 Ephemeral Routing & Durable Ownership Acquisition
```
SchedulerService        WorkerRegistryView        CandidateSelector        PostgreSQL (LLD-02)        WorkerTransport
       │                        │                         │                         │                        │
       │ 1. Route Task X        │                         │                         │                        │
       │    (Activity: 'pay')   │                         │                         │                        │
       ├───────────────────────>│                         │                         │                        │
       │ 2. Compatible Sessions │                         │                         │                        │
       │<───────────────────────┤                         │                         │                        │
       │ 3. Form Candidates     │                         │                         │                        │
       ├─────────────────────────────────────────────────>│                         │                        │
       │ 4. choose(candidates)                            │                         │                        │
       │<─────────────────────────────────────────────────┤                         │                        │
       │    Selected: Session S1                          │                         │                        │
       │ 5. Immediate Recheck S1│                         │                         │                        │
       ├───────────────────────>│                         │                         │                        │
       │    Valid & Live        │                         │                         │                        │
       │<───────────────────────┤                         │                         │                        │
       │ 6. Lifecycle Recheck   │                         │                         │                        │
       │    (Process SERVING)   │                         │                         │                        │
       │ 7. Pre-generate AttemptId (UUIDv4)               │                         │                        │
       │ 8. commit_attempt_ownership                      │                         │                        │
       ├───────────────────────────────────────────────────────────────────────────>│                        │
       │    (Lock WF, Verify RUNNABLE, Allocate Ordinal, Insert Attempt CLAIMED)    │                        │
       │<───────────────────────────────────────────────────────────────────────────┤                        │
       │    Status: COMMITTED, Ordinal: 1                                           │                        │
       │ 9. Deliver DispatchInstruction                                             │                        │
       ├────────────────────────────────────────────────────────────────────────────────────────────────────>│
       │    (Task X, Attempt 1, Session S1, Stable Input)                                                    │
```

### 13.3 Cancellation Race During Ownership Commit
```
SchedulerService        CancellationHandler         PostgreSQL (LLD-02)
       │                         │                           │
       │ 1. Candidate Selected   │                           │
       │    (Session S1)         │                           │
       │                         │ 2. User Cancels Workflow  │
       │                         ├──────────────────────────>│
       │                         │    commit_workflow_cancel │
       │                         │    WF -> CANCELLING       │
       │                         │<──────────────────────────┤
       │                         │    Status: COMMITTED      │
       │ 3. commit_attempt_owner │                           │
       ├────────────────────────────────────────────────────>│
       │    (Lock WF FOR UPDATE)                             │
       │    (Verify WF == RUNNING -> FAILS!)                 │
       │<────────────────────────────────────────────────────┤
       │    Status: PRECONDITION_FAILED                      │
       │ 4. Discard Candidate                                │
       │    ZERO Attempts Created                            │
```

---

## 14. Inventories & Decision Reference Matrices

### 14.1 Task Readiness Evaluation Matrix
| Workflow State | Task State | Direct Dependencies | Input Resolvable? | Readiness Result | Reason Code |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `RUNNING` | `PENDING` | None (Root Task) | Yes | **READY** | `READY` |
| `RUNNING` | `PENDING` | All `SUCCEEDED` with output | Yes | **READY** | `READY` |
| `RUNNING` | `PENDING` | 1+ not terminal | Irrelevant | **NOT READY** | `DEPENDENCY_NOT_SUCCEEDED` |
| `RUNNING` | `PENDING` | 1+ `FAILED` or `CANCELLED` | Irrelevant | **NOT READY** | `DEPENDENCY_NOT_SUCCEEDED` |
| `RUNNING` | `PENDING` | 1+ `SUCCEEDED` missing output | Corrupt | **INTEGRITY FAULT** | `OUTPUT_NOT_AVAILABLE` |
| `RUNNING` | `RUNNABLE` | Irrelevant | Already Done | **NOT READY** | `TASK_NOT_PENDING` |
| `RUNNING` | `RUNNING` | Irrelevant | Already Done | **NOT READY** | `TASK_NOT_PENDING` |
| `RUNNING` | `RETRY_WAIT`| Irrelevant | Stored in DB | **NOT READY** | `TASK_NOT_PENDING` (Timer path owns) |
| `FAILING` | Any | Any | Irrelevant | **NOT READY** | `WORKFLOW_NOT_RUNNING` |
| `CANCELLING` | Any | Any | Irrelevant | **NOT READY** | `WORKFLOW_NOT_RUNNING` |

### 14.2 Worker Routing Compatibility Matrix
| Worker Live? | Accepting New Work? | ActivityType Match? | Compatible Candidate? | Action / Outcome |
| :--- | :--- | :--- | :--- | :--- |
| `True` | `True` | **Exact byte match** | **YES** | Yields `RoutingCandidate(task_id, session_id)`. |
| `True` | `True` | Case mismatch / partial | **NO** | Rejection; no fuzzy matching. |
| `True` | `False` (Draining) | Exact match | **NO** | Rejection; draining workers take no new work. |
| `False` (Heartbeat Lost)| `True` | Exact match | **NO** | Rejection; session is dead. |
| `False` | `False` | Exact match | **NO** | Rejection. |

### 14.3 Ownership Commit Outcome Matrix
| Durable Task State | Workflow State | Worker Revalidation | Commit Outcome | Attempt Created? | Next Action |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `RUNNABLE` | `RUNNING` | Live & Accepting | `COMMITTED` | **Yes (CLAIMED)** | Produce `DispatchInstruction` for worker transport. |
| `RUNNABLE` | `RUNNING` | Dead / Draining | Aborted Pre-DB | **No** | Discard candidate; task remains `RUNNABLE`. |
| `RUNNABLE` | `CANCELLING` | Live & Accepting | `PRECONDITION_FAILED`| **No** | Abort ownership; workflow is draining. |
| `RUNNING` (Won by other) | `RUNNING` | Live & Accepting | `OCC_CONFLICT` | **No** | Harmless race loss; do nothing. |
| `RUNNABLE` | `RUNNING` | Live & Accepting | `UNKNOWN_OUTCOME` | Reconciled | Query attempt table; dispatch only if confirmed committed. |

### 14.4 Wakeup Sources & Defensive Recovery Contract
| Wakeup Event Source | Trigger Context | Targeted Action | Invariant if Wakeup Dropped |
| :--- | :--- | :--- | :--- |
| `WorkflowStarted` | `INITIALIZING -> RUNNING` | Evaluate root task readiness | Bounded defensive PENDING sweeper detects un-started roots. |
| `TaskSucceeded` | Upstream task `SUCCEEDED` | Evaluate direct dependents | Bounded defensive PENDING sweeper detects eligible tasks. |
| `TaskRunnable` | `PENDING -> RUNNABLE` | Route task to compatible workers | Bounded defensive RUNNABLE sweeper detects un-claimed tasks. |
| `RetryReadyDue` | `retry_ready_at_utc <= now` | Commit `RETRY_WAIT -> RUNNABLE` | Bounded defensive RETRY_WAIT sweeper detects overdue timers. |
| `WorkerAvailable` | Worker session registered | Route pending `RUNNABLE` tasks | Normal RUNNABLE sweepers route remaining work. |

---

## 15. Concrete Package & Module Layout

Conforming strictly to LLD-01 architecture and module boundaries:

```text
src/nexusflow/
    domain/
        scheduling/
            __init__.py
            readiness.py          # Pure readiness evaluator & input resolution
            routing.py            # Pure worker capability matching
            candidates.py         # Ephemeral RoutingCandidate & snapshots
            selector.py           # CandidateSelector protocol & round-robin policy

    application/
        scheduling/
            __init__.py
            scheduler_service.py  # Coordinates wakeups, readiness, and routing
            ownership.py          # Coordinates revalidation, lifecycle checks, & ownership
            wakeups.py            # Ephemeral in-process async wakeup bus protocol
            ports.py              # SchedulingReadPort, ProcessLifecycleView, RediscoveryPort

    runtime/
        scheduler/
            __init__.py
            dispatcher.py         # Worker dispatch loop & concurrency limiter
            rediscovery.py        # Periodic keyset sweeper (PENDING, RUNNABLE, RETRY_WAIT)

    infrastructure/
        persistence/
            scheduling_repo.py    # Read-side projections (PostgreSQL 16 / SQLAlchemy)
```

---

## 16. Comprehensive Testing Strategy

### 16.1 Unit Tests (`tests/unit/scheduling/`)
1. **ActivityType Exactness (`test_routing_exactness.py`):**
   - Assert that worker with `{"email.send"}` does **not** match task requiring `"Email.Send"` or `"email.send.v1"`.
2. **Worker Admission Filter (`test_worker_filtering.py`):**
   - Assert that worker with `live=True, accepting_new_work=False` yields 0 candidates.
   - Assert that worker with `live=False, accepting_new_work=True` yields 0 candidates.
3. **No Worker Behavior (`test_no_worker.py`):**
   - Assert that when 0 workers exist, task remains `RUNNABLE`, zero attempts are created, and zero history rows are written.
4. **Candidate Advisory Status (`test_candidate_invariants.py`):**
   - Assert that generating and selecting candidates causes zero database inserts or updates.
5. **Stable Input Resolution (`test_input_resolution.py`):**
   - Verify resolution of `LiteralBinding`, `WorkflowInputBinding`, and `TaskOutputBinding`.
   - Verify that upstream output of JSON `null` resolves successfully to `None` and is not flagged as missing data.
   - Verify that an upstream task missing authoritative output raises `SchedulingIntegrityError`.
6. **Multi-Root Disconnected DAGs (`test_multi_root_readiness.py`):**
   - Create a workflow with 3 isolated components. Assert that upon `WorkflowStarted`, all 3 root tasks are identified and evaluate to `READY` independently.

### 16.2 Concurrency & Race Tests (`tests/integration/scheduling/`)
1. **Crash After Upstream Success Before Wakeup (`test_crash_after_upstream_success.py`):**
   - Task A commits `SUCCEEDED`.
   - Suppress `TaskSucceeded` wakeup. Task B remains `PENDING`.
   - Run defensive PENDING readiness sweep. Task B is discovered and evaluated.
   - Assert: Task B commits `PENDING → RUNNABLE`. Zero duplicate history rows created.
2. **Lost WorkflowStarted / Root Wakeup (`test_lost_workflow_started_wakeup.py`):**
   - Workflow transitions `INITIALIZING → RUNNING`.
   - Suppress root readiness wakeup.
   - Run defensive PENDING readiness sweep. Root task is discovered and commits `RUNNABLE`.
3. **Duplicate PENDING Repair Race (`test_duplicate_pending_repair.py`):**
   - Task B is targeted concurrently by `TaskSucceeded` wakeup and defensive PENDING sweep.
   - Both evaluate `READY`.
   - Assert: Exactly one `commit_task_readiness` wins; second receives `OCC_CONFLICT`. Exactly one history entry recorded.
4. **Duplicate Ownership Race (`test_duplicate_ownership_race.py`):**
   - Two concurrent coroutines attempt `try_acquire_ownership` on the same `RUNNABLE` task.
   - Use `asyncio.Barrier` to synchronize them immediately before the database commit.
   - **Assert:** Exactly one coroutine receives `OwnershipOutcomeStatus.ACQUIRED`; the second receives `OWNERSHIP_LOST_OCC`.
   - **Assert:** Exactly one `ExecutionAttempt` row exists in state `CLAIMED`.
5. **Pre-Commit Worker Staleness (`test_worker_staleness_race.py`):**
   - Candidate selected for Session $S_1$.
   - Memory registry updates $S_1$ to `live=False`.
   - Immediate revalidation detects staleness.
   - **Assert:** Discards candidate. Zero database transactions executed. Task remains `RUNNABLE`.
6. **Post-Commit Worker Loss (`test_worker_loss_after_commit.py`):**
   - Ownership commits successfully to Session $S_1$.
   - $S_1$ crashes immediately afterwards.
   - **Assert:** Attempt remains `CLAIMED` in PostgreSQL. Task remains `RUNNING`. The ownership is not rolled back. LLD-06 start deadline handles settlement.
7. **Graceful Shutdown Drain Transitions (`test_graceful_shutdown.py`):**
   - *Case 1 (Already Draining):* `accepts_new_ownership = False`. Assert zero DB transactions, task remains `RUNNABLE`.
   - *Case 2 (Drain Begins Before Commit):* Candidate selected; lifecycle changes to `DRAINING`; immediate pre-commit check rejects ownership; zero Attempts created.
   - *Case 3 (Transaction In Flight):* Lifecycle changes after final check while DB transaction is executing; transaction commits cleanly according to ADR-017.
8. **Unknown Commit Durable Facts Reconstruction (`test_unknown_commit_durable_facts.py`):**
   - Ownership commit succeeds but connection drops (`UNKNOWN_OUTCOME`).
   - Local caller variable `start_deadline` is intentionally corrupted.
   - Reconciliation query discovers persisted Attempt.
   - **Assert:** `DispatchInstruction` uses persisted `start_deadline_utc` and `attempt_ordinal` from database.
9. **Unknown Commit With Dead Worker (`test_unknown_commit_worker_loss.py`):**
   - Ownership commit response lost. WorkerSession drops connection.
   - Reconciliation confirms committed Attempt.
   - **Assert:** Ownership remains committed. No rollback. No second Attempt. LLD-06 start deadline owns settlement.
10. **Precondition Failure Classification (`test_precondition_failed_classification.py`):**
    - Task is no longer `RUNNABLE` while Workflow is still `RUNNING`.
    - Ownership commit fails precondition.
    - **Assert:** Evaluator returns `PRECONDITION_FAILED`, NOT `WORKFLOW_DRAINING`.

### 16.3 Property-Based Testing (`tests/property/scheduling/`)
1. **Readiness Invariant Property:**
   - Generate arbitrary random DAG topologies using `Hypothesis`.
   - **Property:** A task is readiness-eligible if and only if all direct declared dependencies are `SUCCEEDED` with required committed outputs, the task is `PENDING`, and the workflow is `RUNNING`.
2. **Routing Compatibility Property:**
   - Generate random sets of 100 workers with varying capability sets, liveness flags, and drain statuses.
   - **Property:** The candidate set returned equals precisely $\{ w \in \text{workers} \mid w.\text{live} \land w.\text{accepting} \land \text{activity} \in w.\text{capabilities} \}$.

---

## 17. Final Design Validation Checklist

- [x] **Lost Wakeup Protection**: Bounded defensive keyset sweep for `PENDING` tasks in `RUNNING` workflows recovers dropped `WorkflowStarted` and `TaskSucceeded` wakeups.
- [x] **Transition-Driven Primary**: Documented that push wakeups are primary; defensive scans exist solely for background repair.
- [x] **Accurate Wakeup Recovery Matrix**: Matrix maps `WorkflowStarted`/`TaskSucceeded` to defensive PENDING sweeps, `TaskRunnable` to RUNNABLE sweeps, and timers to RETRY_WAIT sweeps.
- [x] **Keyset Starvation Safety**: All three defensive sweeps use composite keyset ordering with ephemeral cursors that wrap on completion.
- [x] **ProcessLifecycleView Consumed**: `OwnershipCoordinator` receives `ProcessLifecycleView` and validates it both before routing and immediately before database commit.
- [x] **Fencing During DRAINING**: Emits `PROCESS_DRAINING` when shutting down; leaves `RUNNABLE` work intact without cancellation or failure.
- [x] **Unavoidable Shutdown Race Clarified**: Explains that in-flight admitted transactions settle per ADR-017 without claiming impossible distributed atomicity.
- [x] **Durable Reconciliation Facts**: Reconciles `UNKNOWN_OUTCOME` using persisted database values for `attempt_ordinal` and `start_deadline_utc`.
- [x] **Exact Ownership Identity Verified**: Verifies that reconciled attempts match target task and worker bindings.
- [x] **Absent Reconciliation Reread**: If attempt is absent, re-reads task state before deciding to retry.
- [x] **Worker Loss Post-Commit Preserved**: If worker disappears after commit or during reconciliation, committed attempt is preserved; start deadline handles settlement.
- [x] **PRECONDITION_FAILED Classified Accurately**: Re-reads workflow state to ensure only `FAILING`/`CANCELLING` workflows report `WORKFLOW_DRAINING`.
- [x] **Direct Dependencies Wording**: Property tests verify direct declared dependencies, not transitive ancestors.
- [x] **Correct LLD-07 Reference**: Labeled as "LLD-07 — Recovery & Reconciliation" without introducing control-plane HA.
- [x] **Protocol Purity**: Prohibits gRPC references; aligns with HTTP JSON pull/long-poll worker protocol (LLD-05).
- [x] **No Schema Drift**: Preserves LLD-02 schema; resolves `ActivityType` via `ValidatedWorkflowSpec` cache rather than adding durable columns.
- [x] **Bounded Metric Labels**: Explicitly bounds `activity_type` cardinality in telemetry.
- [x] **Candidate Remains Ephemeral**: Zero attempts, zero leases, and zero budget consumption during routing.
- [x] **Ownership Sole Attempt Point**: Attempt is created exclusively inside `commit_attempt_ownership`.
- [x] **Ownership Before Network**: Worker transport dispatch instruction emitted strictly after confirmed database commit.

---

### Classification

**LLD-04 — Architecture-Ready / Approved as LLD-05 Input**
