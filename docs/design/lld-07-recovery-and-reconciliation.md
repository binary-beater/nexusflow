# NexusFlow V1 — LLD-07: Recovery & Reconciliation

**Document Status:** Architecture-Ready / Approved as LLD-08 Input  
**Authoritative References:** ADR-001 (Intermediate Workflow Specification), ADR-003 (Canonical Graph Representation), ADR-004 (Definition Validation), ADR-005 (Scheduler), ADR-006 (Workflow State Machine), ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-014 (History), ADR-016 (Observability), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership), LLD-05 (Worker Protocol & Worker Runtime), LLD-06 (Execution Results, Retries, Timeouts & Cancellation).  
**Downstream Dependents:** LLD-08 (External Ingestion HTTP APIs), LLD-09 (Observability, Configuration & Runtime Lifecycle).

---

## 1. Primary Objective & Architectural Scope

### 1.1 Objective Statement
This document defines the complete implementation-level design for NexusFlow V1 control-plane startup orchestration, process-level recovery admission gating, bounded database reconciliation, and safe resumption of normal workflow progression after clean boot, process restart, or sudden system crash.

Specifically, it answers:
> *When a NexusFlow control-plane process boots or restarts with an empty in-memory state, how does it reconstruct complete, safe orchestration progress strictly from the authoritative current PostgreSQL state—repairing partially initialized workflows, reconciling lost worker sessions, rediscovering runnable work, catching up deadlines, continuing drains, and proving integrity—while admitting authoritative late-settlement callbacks to race recovery worker-loss/timeout settlement, yet preventing split-brain scheduling or new work admission before reaching verified readiness?*

### 1.2 Core Recovery Philosophy & Boundaries
In strict accordance with ADR-012:
1. **Current State is Sole Authority:** Recovery inspects current relational records in PostgreSQL 16 (`workflow_executions`, `task_executions`, `execution_attempts`, `registered_definitions`). It **never** replays `history_entries` to reconstruct state. History is an append-only audit trail, not an event-sourced event log.
2. **Immutable Definitions, Not YAML Reparsing:** Workflow structure is recovered from persisted, validated intermediate representations (`registered_definitions.validated_iws`). Recovery **never** rereads or reparses original YAML files.
3. **No Synthetic Recovery Lifecycle State:** There is no `RECOVERING` state in `WorkflowExecution`, `TaskExecution`, or `ExecutionAttempt`. Existing durable states remain valid throughout startup. Recovery is an ephemeral process-level coordination procedure.
4. **Reuse Normal Authoritative Operations:** Recovery invokes the standard consistency groups and use cases defined in LLD-02, LLD-04, and LLD-06 (e.g., `commit_task_readiness`, `commit_retry_ready`, `commit_internal_attempt_failure`, `commit_workflow_success`). No parallel "recovery-only" mutation bypasses exist.
5. **No Ephemeral Snapshot Restorations:** Process memory (Worker Registry, timer heaps, delivery queues, candidate pools) starts completely empty. Ephemeral memory is never serialized to disk across process restarts.

```text
               [ Process Boot / Restart ]
                            │
                            ▼
           [ 1. Configuration & Logging Init ]
                            │
                            ▼
           [ 2. PostgreSQL Connection & Schema Verify ]
                            │
                            ▼
           [ 3. Local Runtime & Ephemeral Queues Init ]
                            │
                            ▼
    ┌──────────────────────────────────────────────┐
    │     PROCESS-LEVEL RECOVERY GATE (CLOSED)     │
    │  NEW-WORK ADMISSION BLOCKED                  │
    │  EXISTING-WORK SETTLEMENT ALLOWED            │
    └──────────────────────────────────────────────┘
                            │
                            ▼
       [ 4. Bounded Phase-Ordered Reconciliation ]
       ├── Phase 1: Partial INITIALIZING Repair
       ├── Phase 2: Draining Workflows (FAILING / CANCELLING)
       ├── Phase 3: Active CLAIMED Attempts (Worker-Loss / Deadlines)
       ├── Phase 4: Active RUNNING Attempts (Timeouts / Worker-Loss)
       │            ▲ ── RACES ── ▼
       │            Late Result Settlement (POST /worker/callback)
       ├── Phase 5: Due RETRY_WAIT Tasks
       ├── Phase 6: Durable RUNNABLE Tasks (Rediscovery)
       ├── Phase 7: PENDING Readiness Repair (Lost Wakeups)
       └── Phase 8: Terminalization Repair
                            │
                            ▼
       [ 5. Convergence Verified & Critical Loops Start ]
                            │
                            ▼
    ┌──────────────────────────────────────────────┐
    │      PROCESS-LEVEL RECOVERY GATE (OPEN)      │
    │     (Readiness = True, Process in SERVING)   │
    │  NEW-WORK ADMISSION ALLOWED                  │
    │  EXISTING-WORK SETTLEMENT ALLOWED            │
    └──────────────────────────────────────────────┘
```

---

## 2. Process Admission Policy & Worker Protocol Fencing

### 2.1 Recovery Gate Controls New Work Admission, Not All Settlement Traffic
A critical contradiction is resolved by decoupling process admission: while startup recovery is in progress (`RecoveryGate = CLOSED`), the system must block operations that create or advance new orchestration work, but it **must not** prevent authoritative settlement of already-existing durable active work when the frozen architecture explicitly allows that settlement to race recovery.

#### Admission Policy Semantics:
```python
from typing import Protocol


class ProcessAdmissionPolicy(Protocol):
    """Process-level traffic admission policy."""

    def allows_new_work(self) -> bool:
        """Returns True if operations creating or advancing new work are admitted."""
        ...

    def allows_existing_work_settlement(self) -> bool:
        """Returns True if authoritative settlement of pre-existing work is admitted."""
        ...
```

1. **During Mandatory Startup Recovery (`RecoveryGate.is_recovery_complete() == False`):**
   - `allows_new_work() == False`
   - `allows_existing_work_settlement() == True`
2. **After Recovery Convergence (`RecoveryGate.is_recovery_complete() == True`):**
   - `allows_new_work() == True`
   - `allows_existing_work_settlement() == True`
3. **During Process Shutdown (ADR-017):**
   - Controlled by shutdown policies (new executions rejected, in-flight settlement permitted to bounded timeout).

### 2.2 Operations Blocked vs. Admitted During Recovery
| Endpoint / Operation | Category | Gate Status: CLOSED | Reason & Invariants |
| :--- | :--- | :--- | :--- |
| `POST /api/v1/definitions` | New Work | **BLOCKED (503 Not Ready)** | Cannot register new definitions during startup recovery. |
| `POST /api/v1/executions` | New Work | **BLOCKED (503 Not Ready)** | Cannot create new workflow executions during startup recovery. |
| `POST /api/v1/executions/{id}/cancel` | Public Mutation | **BLOCKED (503 Not Ready)** | Public mutation blocked during recovery; drain handles existing cancellations. |
| `POST /internal/v1/worker/poll` | Progression | **BLOCKED (503 Not Ready)** | No dispatch or task assignment before recovery reaches readiness. |
| `POST /internal/v1/worker/heartbeat` | Operational | **BLOCKED (503 / 409)** | In-memory registry is empty; historical sessions cannot heartbeat. |
| `POST /internal/v1/worker/start` (Start ACK)| Progression | **BLOCKED (409 Conflict)** | Historical sessions are dead; CLAIMED attempts cannot become RUNNING. |
| `POST /internal/v1/worker/callback` (Late Result)| Settlement | **ADMITTED** | Allowed to enter settlement pipeline for durable `RUNNING` attempts and race OCC. |

### 2.3 Late Result Callback Exception & Admission Requirements
For a durable pre-restart attempt:
```text
Attempt.state == RUNNING
Attempt.worker_session_id == S1
```
A late result callback carrying `AttemptId` and `WorkerSessionId = S1` may be admitted during recovery even though $S_1$ no longer exists in the newly booted control plane's in-memory `WorkerRegistry`.

#### Strict Admission & Fencing Requirements:
1. **Worker-Domain Bearer Authentication:** The request carries a valid Bearer credential matching the worker trust domain.
2. **Syntactic DTO Validation:** The payload is a well-formed `TaskExecutionCallbackDTO`.
3. **Durable Attempt Existence:** The database contains an `execution_attempts` row matching `attempt_id`.
4. **Exact Durable Session Fencing:** The database row has `Attempt.worker_session_id == S1`. If the callback carries a different session (e.g., newly registered $S_2$), it is rejected with `403 Forbidden` (`WRONG_SESSION`).
5. **Lifecycle Eligibility:** The durable attempt state is `RUNNING`.
6. **PostgreSQL OCC Settlement Race:** The callback enters standard LLD-06 settlement (`commit_worker_success` or `commit_worker_failure`) and contends via row-level locks against startup `WORKER_LOSS`, `EXECUTION_TIMEOUT`, or cancellation deadline settlement.

> **Crucial Invariant:** Do NOT require `RecoveryGate.allows_new_work() == True` for this callback path. This endpoint is settlement of existing authoritative work, not admission of new work.

### 2.4 Historical Operational Endpoints Remain Strictly Blocked
Previous session fencing is preserved intact:
- **No Session Resurrection:** Historical session $S_1$ cannot heartbeat, poll, or start new work after control-plane restart.
- **Pre-restart `CLAIMED` Attempts Cannot Start:** A pre-restart `CLAIMED` attempt bound to $S_1$ cannot become `RUNNING` through old $S_1$.
- **No Ownership Migration:** Brand-new session $S_2$ cannot operate or settle old $S_1$'s attempt unless durable ownership actually equals $S_2$.

---

## 3. Process Startup Lifecycle & Ephemeral Recovery Gate

### 3.1 Concrete Startup Sequence (ADR-020)
```text
1. Boot & Environment:
   - Load immutable environment configuration (Settings).
   - Initialize structured bootstrap logging.
2. Persistence Verification:
   - Establish async engine connection pool to PostgreSQL 16 via asyncpg.
   - Verify database connectivity (`SELECT 1`).
   - Verify schema compatibility against expected Alembic revision. If corrupt: FAIL CLOSED.
3. Component Initialization:
   - Initialize empty in-memory `WorkerRegistry`.
   - Initialize empty timer priority heaps.
   - Initialize empty asyncio dispatch queues.
4. Recovery Gate Initialized to CLOSED:
   - Set ephemeral `RecoveryGate.is_recovery_complete = False`.
   - `allows_new_work() -> False`
   - `allows_existing_settlement() -> True`
   - Readiness probe returns 503 Service Unavailable.
5. Mandatory Startup Reconciliation:
   - Instantiate `RecoveryCoordinator`.
   - Execute bounded, phase-ordered sweeps across all non-terminal entity classes.
   - Reconcile orphan CLAIMED attempts (worker loss / expired start deadlines).
   - Reconcile orphan RUNNING attempts (worker loss / execution timeouts) while admitting competing callbacks.
   - Loop until full convergence criterion is met.
6. Post-Recovery Activation:
   - Start background loops: liveness sweeper, timer runner, scheduler worker.
   - Open Recovery Gate: `RecoveryGate.is_recovery_complete = True`.
   - `allows_new_work() -> True`
   - `allows_existing_settlement() -> True`
   - Readiness probe returns 200 OK.
   - Process enters `SERVING` state.
```

### 3.2 Liveness vs. Readiness Distinction
- **Liveness Probe (`GET /healthz`):** Indicates the Python asyncio process is running and event loop is responsive. Returns `200 OK` shortly after socket bind.
- **Readiness Probe (`GET /readyz`):** Indicates the control plane is fully reconciled and safe to orchestrate new work.
  - Returns `503 Service Unavailable` while Recovery Gate is closed (`is_recovery_complete == False`).
  - Returns `200 OK` if and only if:
    1. PostgreSQL connection pool is healthy.
    2. Schema compatibility is verified.
    3. Mandatory startup reconciliation reached convergence (all orphan attempts processed).
    4. Critical background loops are active.
    5. Required security secrets and configuration are present.

### 3.3 Ephemeral Recovery Gate Interface
```python
from typing import Protocol


class RecoveryGate(Protocol):
    """Process-level ephemeral gate controlling operational traffic admission."""

    def is_recovery_complete(self) -> bool:
        """Returns True if mandatory startup reconciliation has converged."""
        ...

    def allows_new_work(self) -> bool:
        """Returns True only when process is in SERVING state."""
        ...

    def allows_existing_settlement(self) -> bool:
        """Returns True during both recovery and normal serving."""
        ...
```
*Note: The recovery gate resides strictly in control-plane process memory. It has no database column, uses no synthetic domain state, and conveys no domain lifecycle authority.*

---

## 4. Bounded Keyset Pagination & Safe Query Infrastructure

### 4.1 Non-Nullable Keyset Pagination for RUNNING Attempts
In PostgreSQL, SQL tuple comparisons involving `NULL` (e.g., `(execution_timeout_utc, attempt_id) > (...)`) do **not** traverse rows where `execution_timeout_utc IS NULL`. Because activity definitions may legitimately omit an execution timeout (`execution_timeout_utc = NULL`), keyseting on nullable timeout causes missing attempts during recovery.

#### Authoritative All-Active RUNNING Keyset Query:
Recovery scans all active `RUNNING` attempts using the non-null, primary key `attempt_id`:
```sql
SELECT
    attempt_id,
    task_execution_id,
    worker_session_id,
    revision,
    execution_timeout_utc
FROM execution_attempts
WHERE state = 'RUNNING'
  AND attempt_id > :cursor_attempt_id
ORDER BY attempt_id ASC
LIMIT :batch_size;
```

#### Optional Due-Timeout Keyset Query (Timer Acceleration Only):
```sql
SELECT
    attempt_id,
    task_execution_id,
    worker_session_id,
    revision,
    execution_timeout_utc
FROM execution_attempts
WHERE state = 'RUNNING'
  AND execution_timeout_utc IS NOT NULL
  AND execution_timeout_utc <= :now_utc
  AND (execution_timeout_utc, attempt_id) > (:cursor_timeout, :cursor_attempt_id)
ORDER BY execution_timeout_utc ASC, attempt_id ASC
LIMIT :batch_size;
```

### 4.2 Complete Keyset Query Shapes & Index Mapping
```sql
-- 1. Initializing Workflows Keyset Query
SELECT workflow_execution_id, definition_id, revision, created_at_utc
FROM workflow_executions
WHERE state = 'INITIALIZING'
  AND (created_at_utc, workflow_execution_id) > (:cursor_created_at, :cursor_wf_id)
ORDER BY created_at_utc ASC, workflow_execution_id ASC
LIMIT :batch_size;

-- 2. Draining Workflows Keyset Query (FAILING / CANCELLING)
SELECT workflow_execution_id, state, revision, created_at_utc
FROM workflow_executions
WHERE state IN ('FAILING', 'CANCELLING')
  AND (created_at_utc, workflow_execution_id) > (:cursor_created_at, :cursor_wf_id)
ORDER BY created_at_utc ASC, workflow_execution_id ASC
LIMIT :batch_size;

-- 3. Active CLAIMED Attempts (Index: idx_execution_attempts_start_deadline)
SELECT attempt_id, task_execution_id, worker_session_id, revision, start_deadline_utc
FROM execution_attempts
WHERE state = 'CLAIMED'
  AND (start_deadline_utc, attempt_id) > (:cursor_deadline, :cursor_attempt_id)
ORDER BY start_deadline_utc ASC, attempt_id ASC
LIMIT :batch_size;

-- 4. Due RETRY_WAIT Tasks (Index: idx_task_executions_retry_timer)
SELECT task_execution_id, workflow_execution_id, revision, retry_ready_at_utc
FROM task_executions
WHERE state = 'RETRY_WAIT'
  AND (retry_ready_at_utc, task_execution_id) > (:cursor_ready_at, :cursor_task_id)
ORDER BY retry_ready_at_utc ASC, task_execution_id ASC
LIMIT :batch_size;

-- 5. Durable RUNNABLE Tasks (Index: idx_task_executions_runnable_rediscovery)
SELECT task_execution_id, workflow_execution_id, revision, created_at_utc
FROM task_executions
WHERE state = 'RUNNABLE'
  AND (created_at_utc, task_execution_id) > (:cursor_created_at, :cursor_task_id)
ORDER BY created_at_utc ASC, task_execution_id ASC
LIMIT :batch_size;
```

---

## 5. Deterministic Recovery Ordering & Rationale

Recovery executes in eight discrete phases. The ordering is mathematically designed to eliminate cascading race conditions and prevent scheduling work on dead workflows:

```text
Phase 1: Partial INITIALIZING Repair
  Rationale: Workflows must be either fully initialized or failed before their tasks can be scheduled.
Phase 2: Draining Workflows (FAILING / CANCELLING)
  Rationale: Cancels unstarted tasks and halts retries for dying workflows before general task scheduling.
Phase 3: Active CLAIMED Attempts (Startup Worker-Loss & Expired Start Deadlines)
  Rationale: Settles orphan claim attempts whose pre-restart worker sessions are dead.
Phase 4: Active RUNNING Attempts (Startup Worker-Loss & Execution Timeouts)
  Rationale: Settles orphan running attempts whose pre-restart worker sessions are dead; races late callbacks.
Phase 5: Due RETRY_WAIT Tasks
  Rationale: Promotes due timers to RUNNABLE so the subsequent scheduler sweep sees them.
Phase 6: Durable RUNNABLE Tasks
  Rationale: Rediscovers ready tasks and queues them for scheduling once gate opens.
Phase 7: PENDING Readiness Repair
  Rationale: Resolves unblocked tasks stranded by lost in-memory wakeups.
Phase 8: Terminalization Repair
  Rationale: Catches workflows whose tasks all reached terminal states prior to or during recovery.
```

---

## 6. Phase-by-Phase Recovery Architecture & Old-Session Cancellation Cleanup

### 6.1 Phase 1: Partial INITIALIZING Workflow Repair
When the control plane crashes during workflow creation, a `WorkflowExecution` row may exist with only a partial set of `task_executions`.

#### Recovery Algorithm:
1. Query `workflow_executions` with `state == 'INITIALIZING'` in bounded keyset batches.
2. For each workflow, read `definition_id` and load `validated_iws` from `registered_definitions`.
3. Extract `expected_task_ids = set(validated_iws["tasks"].keys())`.
4. Query existing tasks: `SELECT task_definition_id FROM task_executions WHERE workflow_execution_id = :id`.
5. Compute `missing_task_ids = expected_task_ids - actual_task_ids`.
6. If `missing_task_ids` is non-empty:
   - Insert missing tasks with `state = 'PENDING', revision = 1` using `commit_task_population` (`ON CONFLICT DO NOTHING`).
7. Once all tasks exist, attempt `commit_initialization_complete`:
   - Locks `workflow_executions` row `FOR UPDATE`.
   - Re-verifies `state == 'INITIALIZING'`.
   - **Cancellation Race Check:** If a user cancelled during creation, `state` will be `CANCELLING`. The transaction aborts; initialization completion is skipped; execution routes to Phase 2 drain.
   - If still `INITIALIZING`: Transitions `INITIALIZING → RUNNING`, writes `WorkflowExecutionStarted` history entry, and commits.

### 6.2 Phase 2: Draining Workflows Reconciliation (`FAILING` & `CANCELLING`)
Workflows that were draining when the control plane crashed must resume drain progression.

#### Startup Cancellation Delivery & Convergence Invariants:
1. **No Old-Session Poll Delivery Reliance:** During startup recovery, active attempts may be bound to historical session $S_1$. Because $S_1$ cannot resume ordinary `poll` after restart, recovery **must not rely** on enqueuing a cancellation command and expecting old $S_1$ to poll it as a viable startup delivery path.
2. **Never Redirect to New Session $S_2$:** Never redirect `CANCEL(A, S1)` to a newly connected worker $S_2$. Attempt ownership remains strictly $S_1$. No ownership migration occurs.
3. **Preserve Cancellation Durability:**
   - Unstarted tasks (`PENDING`, `RUNNABLE`, `RETRY_WAIT`) are atomically transitioned to `CANCELLED` via LLD-02 `commit_drain_task_cancellation`.
   - Existing `cancellation_deadline_utc` on active attempts is strictly preserved; do not reset or extend deadlines.
   - If a missing deadline is detected on an active attempt, materialize it in accordance with frozen LLD-06 rules.
   - If `now_utc >= cancellation_deadline_utc`, settle the attempt as `FAILED` (`CANCELLATION_DEADLINE_EXPIRED`).
4. **Drain Convergence Path:** Active attempts owned by historical $S_1$ converge safely without transport delivery via:
   - Recovery worker-loss settlement (Phase 3 & Phase 4), OR
   - Expired cancellation deadline settlement, OR
   - Competing valid late result callback racing through PostgreSQL OCC.
5. **Terminalization Check:** If all declared tasks in the immutable spec are terminal (`SUCCEEDED`, `FAILED`, `CANCELLED`), execute `commit_workflow_failure` or `commit_workflow_cancellation`.

### 6.3 Phase 3: Active CLAIMED Attempt Reconciliation
Because control-plane memory was wiped at reboot, the `WorkerRegistry` is empty. Pre-restart worker session $S_1$ is dead and cannot start work.

#### Reconciliation Rules:
- **`CLAIMED` Attempts Cannot Start:** $S_1$ does not exist in memory, and $S_2$ cannot start an attempt owned by $S_1$. The attempt **must not** remain `CLAIMED` waiting for start ack.
- **Dual Eligibility & Deterministic Attempt Order:**
  ```python
  if now_utc >= attempt.start_deadline_utc:
      # Start deadline already expired
      await commit_internal_attempt_failure(
          attempt_id=attempt.id,
          trigger=InternalFailureTrigger.START_DEADLINE_EXPIRED,
          ...
      )
  else:
      # Start deadline in future, but owning session S1 is lost after restart
      await commit_internal_attempt_failure(
          attempt_id=attempt.id,
          trigger=InternalFailureTrigger.WORKER_LOSS,
          expected_lost_worker_session_id=attempt.worker_session_id,
          ...
      )
  ```
- **OCC Invariant:** Both triggers contend on the exact same attempt revision. Exactly one failure settlement commits. If retry budget remains and workflow is `RUNNING`, the task becomes `RETRY_WAIT`.

### 6.4 Phase 4: Active RUNNING Attempt Reconciliation
For `RUNNING` attempts, the worker was running activity code when the control plane restarted.

#### Reconciliation Rules:
1. **Mandatory Startup Processing:** Active `RUNNING` attempts are worker-loss candidates **during mandatory startup recovery**. Processing is **not** deferred until after readiness opens.
2. **Dual Eligibility (Timeout vs. Worker-Loss):**
   ```python
   if attempt.execution_timeout_utc is not None and now_utc >= attempt.execution_timeout_utc:
       # Execution timeout already expired
       trigger = InternalFailureTrigger.EXECUTION_TIMEOUT
   else:
       # Timeout in future or NULL, but owning session S1 is lost after restart
       trigger = InternalFailureTrigger.WORKER_LOSS
   ```
3. **Late Result Callback Race Preserved & Admitted:**
   - The recovery coordinator attempts `commit_internal_attempt_failure(trigger=trigger)`.
   - Concurrently, if a surviving external worker posts a valid callback `(AttemptId, S1, output)` to `POST /internal/v1/worker/callback`, the request is **admitted** (not rejected by the closed Recovery Gate).
   - Database OCC row locks arbitrate the winner:
     - If the callback commits first $\to$ `Attempt SUCCEEDED, Task SUCCEEDED`, and recovery worker-loss fails OCC and safely no-ops.
     - If recovery worker-loss commits first $\to$ `Attempt FAILED`, and the subsequent callback is rejected as `STALE_ATTEMPT`.

### 6.5 Phase 5: Due `RETRY_WAIT` Task Reconciliation
1. Query `task_executions` with `state == 'RETRY_WAIT'`.
2. Compare persisted `retry_ready_at_utc` against control-plane `clock.now_utc()`:
   - **If `now_utc >= retry_ready_at_utc`:** Invoke LLD-02 `commit_retry_ready`. Locks parent workflow `FOR UPDATE`. If workflow is `RUNNING`, transitions task `RETRY_WAIT → RUNNABLE` and clears `retry_ready_at_utc`. Existing `stable_input` is strictly preserved.
   - **If `now_utc < retry_ready_at_utc`:** Task remains in `RETRY_WAIT`. Compute `remaining_delay = retry_ready_at_utc - now_utc` and register with in-memory timer priority heap for accelerated wakeup.
   - **If Parent Workflow is Draining:** Task transitions to `CANCELLED` via Phase 2 drain logic.

### 6.6 Phase 6: Durable `RUNNABLE` Task Rediscovery
1. Query `task_executions` with `state == 'RUNNABLE'` in keyset batches.
2. Verify parent workflow is currently `RUNNING`.
3. Push task IDs to the internal scheduler dispatch queue.
4. **No Direct Attempt Allocation:** Recovery does **not** create attempt records. Actual assignment occurs strictly via LLD-04 Ownership Commit when compatible workers poll after the Recovery Gate opens.

### 6.7 Phase 7: PENDING Readiness Repair (Lost-Wakeup Sweep)
A crash immediately following task success or workflow start may have lost the in-memory downstream notification before dependents could be evaluated.

#### Recovery Algorithm:
1. Query tasks where `state == 'PENDING'` belonging to workflows with `state == 'RUNNING'`.
2. For each pending task, invoke pure domain service `evaluate_task_readiness`:
   - Inspect upstream dependencies declared in `ValidatedWorkflowSpec`.
   - Verify all upstream tasks are `SUCCEEDED` with committed outputs.
   - Resolve whole-value input bindings (`Literal`, `WorkflowInput`, `TaskOutput`).
3. If ready:
   - Execute LLD-02 `commit_task_readiness`.
   - Atomically transitions `PENDING → RUNNABLE`, locks `stable_input`, increments revision, and commits.
4. If not ready (upstream dependencies still pending/running): Task remains `PENDING`.

### 6.8 Phase 8: Workflow Terminalization Repair
A crash immediately following final task settlement may have left the parent workflow in `RUNNING`, `FAILING`, or `CANCELLING` even though all tasks reached terminal states.

#### Recovery Algorithm:
1. Query non-terminal workflows (`RUNNING`, `FAILING`, `CANCELLING`).
2. Compare actual terminal tasks against exact declared task set in `registered_definitions.validated_iws`:
   - **All Tasks SUCCEEDED & Workflow RUNNING:**
     - Resolve workflow outputs using `WorkflowTaskOutputBinding` (or explicit JSON `null` if none declared).
     - Invoke LLD-02 `commit_workflow_success`.
     - Atomically transitions `RUNNING → SUCCEEDED`, commits output, and appends `WorkflowExecutionSucceeded` history.
   - **All Tasks Terminal & Workflow FAILING:**
     - Invoke LLD-02 `commit_workflow_failure`.
     - Atomically transitions `FAILING → FAILED`.
   - **All Tasks Terminal & Workflow CANCELLING:**
     - Invoke LLD-02 `commit_workflow_cancellation`.
     - Atomically transitions `CANCELLING → CANCELLED`.

---

## 7. Recovery Convergence, Crash Resilience & Idempotency

### 7.1 Recovery Convergence Criterion
Mandatory startup recovery is complete if and only if:
1. All eight recovery phases have scanned through their respective keyspaces to end-of-data (`batch.count < batch_size`).
2. Every discovered actionable item has committed, lost OCC to another valid transition, or been deferred to a future deadline.
3. Every pre-restart `CLAIMED` and `RUNNING` attempt has been reconciled or participated in an authoritative settlement race.
4. Zero fatal integrity violations were encountered.

*Note: Recovery does **not** require zero `RUNNABLE` tasks in the database; `RUNNABLE` represents healthy pending work ready for scheduling once the gate opens.*

### 7.2 Repeated Recovery Idempotency
Every recovery mutation is idempotent under crash and replay:
- If the control plane crashes halfway through Phase 1, the next restart rereads `actual_task_ids` and inserts only remaining tasks.
- If the control plane crashes after transitioning `RETRY_WAIT → RUNNABLE`, the next restart observes `RUNNABLE` and pushes it to scheduler rediscovery without repeating the transition.
- If an operation commits but process crashes before logging: current database state reflects the truth; no duplicate history rows are generated.

### 7.3 Fail-Closed Integrity Violations
If recovery discovers unrecoverable corruption:
1. `TaskExecution` references non-existent `workflow_execution_id`.
2. Multiple active attempts exist for a single task (violating partial unique index).
3. A `SUCCEEDED` task or workflow lacks committed output (`has_output == FALSE` or payload missing).
4. Persisted `validated_iws` is malformed or missing required task definitions.

**Action:** The recovery coordinator logs a critical `SYSTEM_PERMANENT` / `INTEGRITY` alert. The affected workflow is isolated; the process refuses to mark readiness `True`, failing closed to prevent data corruption.

---

## 8. Sequence Diagrams

### 8.1 Control-Plane Startup & Admission Gate Decoupling
```text
Process Boot               PostgreSQL 16               RecoveryCoordinator          RecoveryGate               Public / Worker API
     │                           │                              │                        │                              │
     │ 1. Start & Init Logging   │                              │                        │                              │
     ├──────────────────────────>│                              │                        │                              │
     │ 2. Verify Schema & DB     │                              │                        │                              │
     ├──────────────────────────>│                              │                        │                              │
     │    200 OK (Compatible)    │                              │                        │                              │
     │<──────────────────────────┤                              │                        │                              │
     │ 3. Init In-Memory Queues  │                              │                        │                              │
     │ 4. Close Recovery Gate    │                              │                        │                              │
     ├──────────────────────────────────────────────────────────────────────────────────>│                              │
     │                           │                              │                        │ (allows_new_work = False     │
     │                           │                              │                        │  allows_existing_settle=True)│
     │                           │                              │                        │                              │ 5. POST /executions
     │                           │                              │                        │                              ├───────────────>
     │                           │                              │                        │                              │ 503 Not Ready
     │                           │                              │                        │                              │<───────────────
     │ 6. reconcile_startup()    │                              │                        │                              │
     ├─────────────────────────────────────────────────────────>│                        │                              │
     │                           │ 7. Bounded Phase 1-8 Sweeps  │                        │                              │
     │                           │<─────────────────────────────┤                        │                              │
     │                           │    Committed OCC Mutations   │                        │                              │
     │                           ├─────────────────────────────>│                        │                              │
     │                           │                              │ 8. Convergence Reached │                              │
     │                           │                              │ 9. Open Recovery Gate  │                              │
     │                           │                              ├───────────────────────>│                              │
     │                           │                              │                        │ (allows_new_work = True      │
     │                           │                              │                        │  allows_existing_settle=True)│
     │ 10. Start Worker Loops    │                              │                        │                              │
     │ 11. State = SERVING       │                              │                        │                              │
     ├───────────────────────────┴──────────────────────────────┴────────────────────────┼─────────────────────────────>│
     │                                                                                  │                              │ 12. GET /readyz
     │                                                                                  │                              ├───────────────>
     │                                                                                  │                              │ 200 OK Ready
     │                                                                                  │                              │<───────────────
```

### 8.2 Historical Session Operational Rejection (Start ACK, Heartbeat, Poll)
```text
Worker Process (Old S1)         RecoveryGate               PostgreSQL 16               SettlementPort (LLD-06)
        │                            │                            │                               │
        │ 1. POST /start (A1, S1)    │                            │                               │
        ├───────────────────────────>│                            │                               │
        │                            │ (allows_new_work == False, │                               │
        │                            │  S1 absent from Registry)  │                               │
        │ 2. 409 Conflict / STALE    │                            │                               │
        │<───────────────────────────┤                            │                               │
        │                            │                            │                               │
        │ 3. POST /poll (S1)         │                            │                               │
        ├───────────────────────────>│                            │                               │
        │    503 Not Ready / STALE   │                            │                               │
        │<───────────────────────────┤                            │                               │
```

### 8.3 Active RUNNING Attempt Late Callback Admitted While Gate Closed
```text
Worker Process (Old S1)         RecoveryGate / CallbackAdapter   PostgreSQL 16 (OCC)         RecoveryCoordinator
        │                                    │                            │                           │
        │ (Gate CLOSED, recovery in Phase 4) │                            │                           │
        │                                    │                            │ 1. Phase 4 Sweeper scans  │
        │                                    │                            │<──────────────────────────┤
        │                                    │                            │    Attempt A2 (S1, RUNNING)
        │                                    │                            ├──────────────────────────>│
        │                                    │                            │ 2. Trigger WORKER_LOSS    │
        │                                    │                            │    (Begins Settlement Tx) │
        │                                    │                            │<──────────────────────────┤
        │ 3. POST /callback (A2, S1, SUCCESS)│                            │                           │
        ├───────────────────────────────────>│                            │                           │
        │                                    │ (allows_existing_settle)   │                           │
        │                                    │ Exact Fencing (A2, S1) OK  │                           │
        │                                    │ 4. commit_worker_success   │                           │
        │                                    ├───────────────────────────>│                           │
        │                                    │                            │ [PostgreSQL Row Lock]     │
        │                                    │                            │ Callback locks row first: │
        │                                    │                            │ Attempt -> SUCCEEDED      │
        │                                    │                            │ Task -> SUCCEEDED         │
        │                                    │ 5. 200 OK (COMMITTED)      │                           │
        │<───────────────────────────────────┴────────────────────────────┤                           │
        │                                                                 │ 6. Worker-Loss resumes    │
        │                                                                 │    sees revision changed  │
        │                                                                 │    OCC Conflict -> No-op  │
        │                                                                 ├──────────────────────────>│
```

---

## 9. Concrete Recovery Decision Matrices

### 9.1 Process Admission Decision Matrix
| Invocation / Traffic Type | Recovery Gate: CLOSED | Recovery Gate: OPEN | Process Shutting Down |
| :--- | :--- | :--- | :--- |
| Public Workflow Creation | **Rejected (503 Service Unavailable)** | **Admitted (201 Created)** | **Rejected (503 Shutting Down)** |
| Public Definition Registration | **Rejected (503 Service Unavailable)** | **Admitted (200 OK / 201 Created)**| **Rejected (503 Shutting Down)** |
| Worker Registration (`POST /register`) | **Admitted (New Session ID $S_2$)** | **Admitted (New Session ID $S_2$)** | **Rejected (503 Shutting Down)** |
| Worker Poll (`POST /poll`) | **Rejected (503 Service Unavailable)** | **Admitted (200 OK / 204 No Cont.)**| **Rejected (204 No Content / Drain)**|
| Worker Start ACK (`POST /start`) | **Rejected (409 Stale Session)** | **Admitted (for current session)** | **Admitted (for active claims)** |
| Worker Heartbeat (`POST /heartbeat`)| **Rejected (409 Stale Session)** | **Admitted (for current session)** | **Admitted (for active session)** |
| Late Callback (`POST /callback`)| **Admitted (Races Recovery OCC)** | **Admitted (Normal Settlement)** | **Admitted (Drain Window)** |

### 9.2 CLAIMED Attempt Recovery Matrix
| Current Attempt State | Condition After Restart | Recovery Action Taken | Target Attempt State | Target Task State |
| :--- | :--- | :--- | :--- | :--- |
| `CLAIMED` | `start_deadline_utc <= now_utc` + $S_1$ absent | Trigger `START_DEADLINE_EXPIRED` (or `WORKER_LOSS`) | `FAILED` | `RETRY_WAIT` / `FAILED` |
| `CLAIMED` | `start_deadline_utc > now_utc` + $S_1$ absent | Trigger `WORKER_LOSS` (No start allowed) | `FAILED` | `RETRY_WAIT` / `FAILED` |
| `CLAIMED` | Concurrent settlement committed | Reread state; safe no-op | Unchanged | Unchanged |

### 9.3 RUNNING Attempt Recovery Matrix
| Current Attempt State | Condition After Restart | Recovery Action Taken | Target Attempt State | Target Task State |
| :--- | :--- | :--- | :--- | :--- |
| `RUNNING` | `execution_timeout_utc <= now_utc` + $S_1$ absent | Trigger `EXECUTION_TIMEOUT` (or `WORKER_LOSS`) | `FAILED` | `RETRY_WAIT` / `FAILED` |
| `RUNNING` | `execution_timeout_utc > now_utc` + $S_1$ absent | Trigger `WORKER_LOSS` during recovery | `FAILED` | `RETRY_WAIT` / `FAILED` |
| `RUNNING` | `execution_timeout_utc IS NULL` + $S_1$ absent | Trigger `WORKER_LOSS` during recovery | `FAILED` | `RETRY_WAIT` / `FAILED` |
| `RUNNING` | Late valid callback commits first | Recovery loses OCC; safe no-op | `SUCCEEDED` / `FAILED` | Settles per callback |
| `RUNNING` | Recovery worker-loss commits first | Late callback receives `STALE_ATTEMPT` | `FAILED` | `RETRY_WAIT` / `FAILED` |

### 9.4 Workflow & Task Recovery Matrix
| Entity Type | Current Durable State | Relational Condition | Recovery Action | Target State |
| :--- | :--- | :--- | :--- | :--- |
| `Workflow` | `INITIALIZING` | Missing tasks detected | Insert missing tasks via `commit_task_population` | `INITIALIZING` |
| `Workflow` | `INITIALIZING` | Exact task set exists | Execute `commit_initialization_complete` | `RUNNING` |
| `Workflow` | `INITIALIZING` | Cancelled during creation | Abort initialization; hand over to drain | `CANCELLING` |
| `Workflow` | `RUNNING` | All tasks `SUCCEEDED` | Resolve outputs; `commit_workflow_success` | `SUCCEEDED` |
| `Workflow` | `FAILING` | All tasks terminal | Execute `commit_workflow_failure` | `FAILED` |
| `Workflow` | `CANCELLING` | All tasks terminal | Execute `commit_workflow_cancellation` | `CANCELLED` |
| `Task` | `PENDING` | Upstream dependencies `SUCCEEDED` | Resolve input; `commit_task_readiness` | `RUNNABLE` |
| `Task` | `PENDING` | Upstream dependencies incomplete | Do nothing | `PENDING` |
| `Task` | `RUNNABLE` | In database | Push to scheduler queue for dispatch | `RUNNABLE` |
| `Task` | `RETRY_WAIT` | `now_utc >= retry_ready_at_utc` | Execute `commit_retry_ready` | `RUNNABLE` |
| `Task` | `RETRY_WAIT` | `now_utc < retry_ready_at_utc` | Register with in-memory timer heap | `RETRY_WAIT` |
| `Task` | Any non-terminal | Owning workflow `FAILING` / `CANCELLING`| Cancel unstarted tasks | `CANCELLED` |

---

## 10. Durable vs. Ephemeral Recovery Inventory

| Entity / Concept | Storage Location | Durability Authority | Rebuild / Handling on Reboot |
| :--- | :--- | :--- | :--- |
| `workflow_executions` | PostgreSQL 16 | **Durable Authority** | Authoritative lifecycle; survives crashes. |
| `task_executions` | PostgreSQL 16 | **Durable Authority** | Authoritative task progress and stable input. |
| `execution_attempts` | PostgreSQL 16 | **Durable Authority** | Monotonic attempt tracking; ownership binding. |
| `retry_ready_at_utc` | PostgreSQL 16 | **Durable Authority** | Read directly from DB; never recomputed. |
| `cancellation_deadline_utc` | PostgreSQL 16 | **Durable Authority** | Preserved; never extended on restart. |
| `history_entries` | PostgreSQL 16 | **Durable Audit Trail**| Not replayed; written only for new mutations. |
| `RecoveryGate` | Process Memory | Ephemeral Process Gate | Initialized `CLOSED`; `allows_new_work = True` on convergence. |
| Keyset Cursors | Process Memory | Ephemeral Progress Track | Cursors advance through pages; restart repeats safely. |
| Scheduler Wakeup Queue | Process Memory (`asyncio.Queue`)| Ephemeral Progression | Repopulated via `RUNNABLE` rediscovery sweep. |
| Timer Priority Heap | Process Memory (`heapq`) | Ephemeral Acceleration | Rehydrated from `RETRY_WAIT` & timeout DB scans. |
| Worker Registry | Process Memory | Ephemeral Tracking | Boot starts empty; workers re-register new IDs. |

---

## 11. Deterministic Testing Strategy

### 11.1 Admission Policy & Lifecycle Tests (`tests/integration/recovery/test_admission.py`)
1. **New Work Mutating Endpoints Blocked While Gate Closed:** Submit definition registration (`POST /api/v1/definitions`), workflow creation (`POST /api/v1/executions`), and public cancellation while `RecoveryGate.is_recovery_complete() == False`; assert rejected with `503 Service Unavailable / Not Ready`.
2. **Readiness Flips on Convergence:** Boot against database with active workflows; assert readiness probe returns `503 Not Ready` during recovery, then flips to `200 OK Ready` once Phase 8 converges.
3. **Database Unavailable at Boot:** Sever database connection; assert readiness returns `503 Service Unavailable`; process does not crash.
4. **Schema Incompatibility:** Alter schema version in database; assert recovery fails closed; readiness remains `False`.

### 11.2 Worker Endpoint Fencing During Recovery (`tests/integration/recovery/test_worker_fencing.py`)
5. **Historical Start ACK Blocked While Gate Closed:** Attempt $A$ is `CLAIMED` by pre-restart session $S_1$; while Recovery Gate is closed, $S_1$ sends `POST /internal/v1/worker/start`; assert rejected with `409 Conflict` (`STALE_SESSION`); attempt does not become `RUNNING`.
6. **Historical Poll Blocked While Gate Closed:** Historical $S_1$ sends `POST /internal/v1/worker/poll` during recovery; assert rejected with `503 Not Ready` / `409 Stale Session`; session is not resurrected.
7. **Historical Heartbeat Blocked:** Historical $S_1$ sends `POST /internal/v1/worker/heartbeat` during recovery; assert rejected (`409 Conflict`).
8. **New Session Cannot Start Old Attempt:** Worker registers new session $S_2$ and attempts to start Attempt $A$ (owned by $S_1$); assert rejected with `403 Forbidden` (`WRONG_SESSION`).
9. **Ownership Never Migrated:** Worker registers $S_2$; assert Attempt owned by $S_1$ is not rewritten to $S_2$.

### 11.3 Late Result Callback Admission & Race Tests (`tests/concurrency/recovery/test_late_callback_race.py`)
10. **Late Callback Admitted While Gate Closed:**
    - Recovery Gate is closed (`is_recovery_complete() == False`).
    - Attempt $A$ is `RUNNING` in PostgreSQL, owned by historical $S_1$.
    - Valid authenticated callback `(A, S1, SUCCESS)` arrives via HTTP callback adapter.
    - **Assert:** Adapter does NOT reject merely because recovery is incomplete; durable session fencing succeeds; callback enters settlement transaction.
11. **Late Callback Wins Race Against Recovery Worker-Loss (End-to-End):**
    - Recovery Gate closed; Attempt $A$ is `RUNNING` under historical $S_1$.
    - Recovery coordinator worker-loss transaction pauses at deterministic barrier after reading row.
    - Valid authenticated callback `(A, S1, SUCCESS)` enters through callback adapter and commits `commit_worker_success` via row lock.
    - Recovery worker-loss resumes; detects revision change via OCC; safely aborts/no-ops.
    - **Assert:** Attempt is `SUCCEEDED`, Task is `SUCCEEDED`, output committed.
12. **Recovery Worker-Loss Wins Race Against Late Callback (End-to-End):**
    - Recovery coordinator completes `commit_internal_attempt_failure(trigger=WORKER_LOSS)` on Attempt $A$.
    - Late callback `(A, S1, SUCCESS)` arrives afterward.
    - **Assert:** Callback is rejected with `409 Conflict` (`STALE_ATTEMPT`); Attempt remains `FAILED`.
13. **Mismatched Session in Callback Rejected During Recovery:**
    - Attempt $A$ owned by $S_1$; callback arrives carrying $S_2$.
    - **Assert:** Callback rejected with `403 Forbidden` (`WRONG_SESSION`).

### 11.4 Initializing Repair Tests (`tests/integration/recovery/test_init_repair.py`)
14. **Partial INITIALIZING Missing One Task:** Crash during creation with 2 of 3 tasks inserted; boot recovery; assert missing task inserted and workflow becomes `RUNNING`.
15. **Partial INITIALIZING Missing Multiple Tasks:** 1 of 5 tasks inserted; boot recovery; assert all 4 missing tasks populated.
16. **No Duplicate Task Rows:** Ensure `task_executions` unique constraint is respected and existing tasks are untouched.
17. **Crash Halfway Through Init Repair:** Simulate crash after inserting task 2; next recovery run inserts only task 3.
18. **Cancellation Wins During Init Repair:** User cancelled workflow during creation (`INITIALIZING → CANCELLING`); assert recovery skips transition to `RUNNING` and routes to drain.
19. **Corrupt Persisted IWS Fails Closed:** Persist invalid JSON in `validated_iws`; assert recovery fails closed and isolates workflow.

### 11.5 CLAIMED Attempt Reconciliation Tests (`tests/concurrency/recovery/test_claimed_repair.py`)
20. **CLAIMED Past Start Deadline Expired:** Start deadline in past; assert settled as `FAILED` with `START_DEADLINE_EXPIRED`.
21. **CLAIMED Future Deadline + Lost Pre-Restart Session:** Start deadline in future, but owning session $S_1$ is dead; assert recovery invokes `WORKER_LOSS` settlement during startup; attempt does not remain `CLAIMED`.

### 11.6 RUNNING Attempt Keyset & Timeout Tests (`tests/concurrency/recovery/test_running_repair.py`)
22. **RUNNING Past Execution Timeout:** Execution timeout in past; assert settled as `FAILED` with `EXECUTION_TIMEOUT`.
23. **RUNNING No-Timeout Worker-Loss Discovery:** Attempt `RUNNING` with `execution_timeout_utc = NULL`; owned by $S_1$; restart clears registry; assert recovery scan discovers attempt and executes `WORKER_LOSS` settlement.
24. **RUNNING Future-Timeout Worker-Loss Discovery:** Attempt `RUNNING` with timeout far in future; assert recovery does not defer; executes `WORKER_LOSS` settlement during startup.
25. **Nullable Timeout Keyset Pagination:** Populate `RUNNING` attempts with `NULL`, future, and expired timeouts; run paginated recovery with `batch_size = 1`; assert all attempts are visited without keyset holes.

### 11.7 Runnable Rediscovery & Pending Repair Tests (`tests/integration/recovery/test_task_repair.py`)
26. **RUNNABLE Rediscovered:** Commit tasks in `RUNNABLE`; clear memory; boot recovery; assert tasks pushed to scheduler dispatch queue.
27. **RUNNABLE Recovery Allocates No Attempts:** Ensure Phase 6 rediscovery does not insert rows into `execution_attempts`.
28. **PENDING Ready Task Repaired:** Commit workflow `RUNNING` with upstream task `SUCCEEDED`; dependent task `PENDING`; assert recovery promotes task to `RUNNABLE`.
29. **PENDING Incomplete Dependencies:** Upstream task in `RUNNING`; assert dependent task remains `PENDING`.
30. **Missing Output Integrity Fault:** Upstream task `SUCCEEDED` but `has_output == FALSE`; assert recovery raises critical integrity alert.

### 11.8 Timer & Retry Reconciliation Tests (`tests/integration/recovery/test_timer_repair.py`)
31. **Future RETRY_WAIT Preserved:** Task in `RETRY_WAIT` with deadline in future; assert remains `RETRY_WAIT` and registers in timer heap.
32. **Due RETRY_WAIT Promoted:** Task in `RETRY_WAIT` with deadline past; assert transitions `RETRY_WAIT → RUNNABLE`.
33. **Retry Deadline Never Recomputed:** Verify persisted `retry_ready_at_utc` timestamp is identical before and after recovery.
34. **RETRY_WAIT in Draining Workflow Cancelled:** Workflow in `FAILING`; task in `RETRY_WAIT`; assert task transitions to `CANCELLED`.

### 11.9 Drain & Terminalization Repair Tests (`tests/integration/recovery/test_drain_terminal_repair.py`)
35. **FAILING Unstarted Tasks Cancelled:** Unstarted tasks in `PENDING` and `RUNNABLE` become `CANCELLED`.
36. **Old-Session Cancellation Delivery Not Required:** Workflow in `CANCELLING` with active attempt owned by historical $S_1$; assert recovery converges safely via worker-loss / cancellation deadline settlement without requiring $S_1$ to poll or receive a command.
37. **New Session Never Receives Old Cancellation:** Worker registers $S_2$; assert $S_2$ poll never delivers cancellation command for attempt owned by $S_1$.
38. **CANCELLING Terminalization:** All tasks terminal; assert workflow transitions `CANCELLING → CANCELLED`.
39. **RUNNING Terminalization & Output Resolution:** All tasks `SUCCEEDED`; assert workflow transitions `RUNNING → SUCCEEDED` and resolves output.
40. **No-Output Workflow Commits JSON Null:** Completed workflow with no output bindings commits `'null'::jsonb`.
41. **Missing Membership Blocks Terminalization:** One task missing from database; assert workflow does not terminalize.
42. **Repeated Recovery Runs Idempotent:** Execute recovery 3 times consecutively on identical database state; assert zero spurious mutations or duplicate history rows.

---

## 12. Final Design Validation Checklist

- [x] **Recovery Gate Decoupled**: Distinguishes new-work admission (`allows_new_work == False`) from existing authoritative settlement (`allows_existing_settlement == True`).
- [x] **Late Callback Exception**: Valid authenticated callback for durable `RUNNING` attempt admitted during recovery to race OCC.
- [x] **Historical Session Fencing**: Pre-restart session $S_1$ cannot poll, heartbeat, or start new claims after restart; no session resurrection.
- [x] **No Ownership Migration**: Attempt ownership strictly bound to $S_1$; new session $S_2$ never inherits or operates $S_1$'s attempt.
- [x] **Old-Session Cancellation Delivery Cleanup**: Recovery does not rely on dead $S_1$ polling cancellation commands; converges via worker loss, deadlines, or late callback race.
- [x] **PostgreSQL Current State is Sole Authority**: Recovery inspects current relational records; zero history replay; zero YAML reparsing.
- [x] **No RECOVERING Lifecycle State**: Recovery is strictly an ephemeral process-level procedure; domain entity enums are untouched.
- [x] **Liveness vs. Readiness Decoupled**: Liveness probe checks process health; readiness probe gates operational traffic.
- [x] **Safe Keyset Pagination on Non-Null Keys**: RUNNING scan uses `attempt_id ASC`; eliminates nullable `execution_timeout_utc` pagination holes.
- [x] **Deterministic 8-Phase Ordering**: Phased execution prevents scheduling race conditions and respects entity dependencies.
- [x] **Partial Initialization Repaired**: Missing tasks inserted via `commit_task_population`; cancellation races properly handled.
- [x] **Pre-Restart CLAIMED Attempts Reconciled**: Historical start ACKs rejected; worker-loss / start-deadline settled during recovery.
- [x] **Pre-Restart RUNNING Attempts Reconciled**: Worker-loss settled during recovery; not deferred to post-serving.
- [x] **Retry Deadlines Never Recomputed**: Persisted `retry_ready_at_utc` timestamps are immutable across restarts.
- [x] **Exact Membership Invariant Enforced**: Terminalization requires complete match of declared tasks from immutable specification.
- [x] **History Atomicity Preserved**: Normal semantic mutations write history atomically; read-only recovery scans write zero history.
- [x] **Fail-Closed on Corruption**: Unrecoverable relational contradictions isolate workflows and fail readiness.
- [x] **No Distributed Brokers / gRPC / WebSockets**: Built strictly on Python 3.12, PostgreSQL 16, and HTTP long-polling.

---

### Classification

**LLD-07 — Architecture-Ready / Approved as LLD-08 Input**
