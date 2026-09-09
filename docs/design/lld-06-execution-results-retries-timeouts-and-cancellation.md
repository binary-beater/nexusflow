# NexusFlow V1 — LLD-06: Execution Results, Retries, Timeouts & Cancellation

**Document Status:** Architecture-Ready / Approved as LLD-07 Input  
**Authoritative References:** ADR-001 (Intermediate Workflow Specification), ADR-003 (Canonical Graph Representation), ADR-004 (Definition Validation), ADR-005 (Scheduler), ADR-006 (Workflow State Machine), ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-014 (History), ADR-016 (Observability), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership), LLD-05 (Worker Protocol & Worker Runtime).  
**Downstream Dependents:** LLD-07 (Recovery & Reconciliation), LLD-08 (External Ingestion HTTP APIs), LLD-09 (Observability, Configuration & Runtime Lifecycle).

---

## 1. Primary Objective & Architectural Scope

### 1.1 Objective Statement
This document defines the complete implementation-level design for NexusFlow V1 execution outcome settlement, deterministic retry scheduling, timeout enforcement, worker-loss arbitration, user cancellation direction, cooperative worker cancellation, drain processing for terminating workflows, authoritative terminalization, and operation-specific unknown-commit reconciliation.

Specifically, it answers:
> *Once an ExecutionAttempt is durably CLAIMED or RUNNING, how does the control plane authoritatively arbitrate between worker success callbacks, worker failure reports, execution timeouts, start deadline expirations, worker-loss detections, and user workflow cancellations, committing atomic state transitions, persisted outputs, and semantic history entries without race conditions, split-brain ownership, or premature workflow termination?*

### 1.2 Boundary Ownership & Authority
In accordance with the frozen architecture (ADR-006, ADR-007, ADR-008, ADR-011, ADR-013):
- **Authoritative Control Plane:** The control plane is the sole authority for state transitions, retry eligibility, retry budget consumption, output commitment, workflow direction, and workflow terminalization.
- **Non-Authoritative Workers:** Workers report observations only (success output, failure error metadata, cancellation observations). Worker callbacks never carry an authoritative `retryable: bool` and never mutate database state directly.
- **No Direct Attempt Creation in LLD-06:** LLD-06 never allocates or creates `ExecutionAttempt` records. When a task failure is retryable and budget remains, LLD-06 atomically transitions the active attempt to `FAILED` and the task to `RETRY_WAIT`. When the timer expires, the task becomes `RUNNABLE`. Actual attempt allocation occurs strictly through LLD-04 Ownership Commit.
- **Pure Application Outcomes:** Domain and application services in LLD-06 return typed application result objects. They never embed or return HTTP status codes (e.g., `200`, `409`, `503`). Transport adapters (LLD-05 for workers, LLD-08 for public API) own all HTTP status code mappings.

```text
       [ Worker Callback / Internal Deadline / User Cancel ]
                                 │
                                 ▼
           [ Validate Durable Authority & Session Fencing ]
                                 │
                                 ▼
            [ Execute Pure Domain Service Evaluation ]
            (Retry Eligibility, Failure Category, Output Validation)
                                 │
                                 ▼
             [ Atomic PostgreSQL 16 OCC Transaction ]
          (State Machine Transition + Output + History Entry)
                                 │
                 ┌───────────────┴───────────────┐
                 ▼                               ▼
      [ Transaction COMMITTED ]        [ OCC Conflict / Stale ]
                 │                               │
                 ▼                               ▼
       [ Post-Commit Wakeups ]         [ Return Typed Application Result ]
     (Downstream Tasks / Telemetry)    (COMMITTED, STALE, LOST_RACE, etc.)
```

---

## 2. Frozen Lifecycle Invariants & State Machine Baseline

### 2.1 Frozen State Machines
The system uses exclusively the frozen states defined in LLD-01 and LLD-02. Introducing synthetic states is strictly prohibited.

```text
WorkflowExecution States:
  INITIALIZING ──► RUNNING ──► FAILING   ──► FAILED
                     │             │
                     │             ├──► CANCELLED (Never from FAILING)
                     ├──► CANCELLING ──► CANCELLED
                     │
                     └──► SUCCEEDED

TaskExecution States:
  PENDING ──► RUNNABLE ──► RUNNING ──► SUCCEEDED
                             │
                             ├──► RETRY_WAIT ──► RUNNABLE (Loop)
                             │
                             ├──► FAILED
                             │
                             └──► CANCELLED (During drain or cancel ack)

ExecutionAttempt States:
  CLAIMED ──► RUNNING ──► SUCCEEDED
     │           │
     │           ├──► FAILED
     │           │
     └───────────┴──────► CANCELLED
```

### 2.2 Prohibited Synthetic States
The following states do **NOT** exist in NexusFlow V1:
- `TIMED_OUT`, `WORKER_LOST`, `START_DEADLINE_EXPIRED` (These are failure causes/triggers, not lifecycle states).
- `BLOCKED`, `SKIPPED` (Unstarted tasks in draining workflows become `CANCELLED`).
- `RECOVERING`, `DRAINING`, `CANCELLING` Attempt state (Attempts remain `CLAIMED` or `RUNNING` until settled `SUCCEEDED`, `FAILED`, or `CANCELLED`).
- `DISPATCHED`, `DELIVERED`, `RECEIVED`, `ABORTED`, `ERROR`.

### 2.3 Attempt Ordinal & Budget Invariants
1. **1-Based Monotonic Ordinals:** Attempt ordinals are strictly 1-based ($1, 2, 3\dots$) and monotonically increasing per `TaskExecution`.
2. **Durable Budget Invariant:** A retry is eligible if and only if:
   $$\text{task.next\_attempt\_ordinal} \le \text{task.max\_attempts}$$
   Because `next_attempt_ordinal` is incremented during LLD-04 Ownership Commit, `max_attempts` accurately represents the total allowed attempt count (initial attempt + retries).
3. **No Ordinal Leaks:** Retries of HTTP callbacks, database OCC retries, and network reconnection attempts do **not** consume task execution attempt budget. Budget is consumed exclusively when an `ExecutionAttempt` is allocated and committed.

---

## 3. Failure Taxonomy & Classification Authority

### 3.1 Failure Categories (ADR-018)
All execution failures, internal timeouts, and worker faults map into the frozen `FailureCategory` enum:

```python
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
```

### 3.2 FailureCause Model & Sanitization Boundary
The domain records failure metadata using `FailureCause`:
```python
@dataclass(frozen=True, slots=True)
class FailureCause:
    category: FailureCategory
    code: str
    message: str
    details: JsonObject | None = None  # MappingProxyType; max 32KB thawed JSON
```

**Sanitization Invariants:**
- **No Stack Traces in Durable Authority:** Raw stack traces, Python exception frames, and interpreter internals are strictly forbidden in `FailureCause.message` and `details`. They belong exclusively in application logs (LLD-09).
- **No Secrets:** Passwords, Bearer tokens, and sensitive client headers must be scrubbed before constructing `FailureCause`.
- **Bounded Diagnostic Payload:** `details` is restricted to a structured JSON object with a serialized size not exceeding 32 KB.

### 3.3 Engine Failure Classification vs. Transport Errors
- **Worker Observation Only:** The worker reports raw error metadata (`error_type`, `message`, `details`). It **never** sends an authoritative `retryable: bool`.
- **Transport Errors vs. Task Failure:** A dropped HTTP connection between the worker and NexusFlow during callback delivery is an LLD-05 transport error; the worker retries the HTTP callback. It does **not** fail the task or consume retry budget.
- **Security / Fencing Rejections:** A callback carrying an invalid session ID or unauthenticated token is rejected as a security/fencing violation (`WRONG_SESSION` / `403 Forbidden`). It does **not** mutate the task or fabricate a `FAILED` task state.

| Failure Source / Cause Code | Mapped FailureCategory | Default Engine Classification | Retry Permitted? |
| :--- | :--- | :--- | :--- |
| Worker Catchable App Exception | `DOMAIN_EXECUTION` | Configured on activity / default non-retryable | Only if classified retryable |
| Worker Network/IO Transient Error | `SYSTEM_TRANSIENT` | Transient dependency fault | Yes (if budget remains) |
| Worker Process Crash / Unresponsive | `WORKER_AVAILABILITY` | Transient worker fault | Yes (if budget remains) |
| `START_DEADLINE_EXPIRED` | `TIME_BASED` | Transient delivery / start stall | Yes (if budget remains) |
| `EXECUTION_TIMEOUT` | `TIME_BASED` | Configured task execution timeout | Yes (if budget remains) |
| `WORKER_LOSS` | `WORKER_AVAILABILITY` | Worker liveness heartbeat lost | Yes (if budget remains) |
| Output Violates JSON Spec | `VALIDATION` | Permanent serialization error | **No** (Definitive failure) |
| System / Relational Integrity Error | `INTEGRITY` | Permanent internal corruption | **No** (Definitive failure) |
| Security / Fencing Violation | `SECURITY` | Unauthorized session access | **No** (Rejected without mutation) |

---

## 4. Worker Outcome Settlement

### 4.1 Worker Success Settlement Flow
When a worker posts a `SUCCESS` payload to `/internal/v1/worker/callback`, `SettleWorkerSuccessUseCase` executes:

```text
Worker Callback: POST /internal/v1/worker/callback
  (attempt_id, worker_session_id, output_payload)
                         │
                         ▼
1. Validate JSON Value Space (freeze_json contract)
   - Supports explicit JSON null (None)
   - Rejects NaN, Infinity, non-string dict keys
                         │
                         ▼
2. Begin PostgreSQL Transaction (LLD-02 commit_worker_task_success)
   - Update execution_attempts:
       SET state = 'SUCCEEDED', revision = revision + 1
       WHERE attempt_id = :attempt_id
         AND worker_session_id = :worker_session_id
         AND state = 'RUNNING'
         AND revision = :expected_attempt_revision
   - Update task_executions:
       SET state = 'SUCCEEDED', has_output = TRUE, task_output = :output_payload, revision = revision + 1
       WHERE task_execution_id = :task_id
         AND state = 'RUNNING'
         AND revision = :expected_task_revision
   - Insert history_entries: TaskExecutionSucceeded
                         │
                         ▼
3. Commit Transaction
   - If COMMITTED: Return SettlementResult.COMMITTED
                   Trigger downstream readiness wakeup (TaskSucceeded event)
                   Trigger TryTerminalizeWorkflowUseCase
   - If OCC_CONFLICT: Check for idempotent duplicate; return IDEMPOTENT_DUPLICATE or STALE_ATTEMPT
```

#### Code Invariant (commit_worker_task_success)
The transition is committed via LLD-02's `commit_worker_task_success`. A task `SUCCEEDED` state can **never** exist without committed output. Explicit JSON `null` is stored as `'null'::jsonb` with `has_output = TRUE`.

### 4.2 Worker Failure Settlement Flow
When a worker posts a `FAILURE` payload, the control plane parses the raw `WorkerFailureReport` and applies `evaluate_retry_eligibility`:

```text
Worker Callback: POST /internal/v1/worker/callback
  (attempt_id, worker_session_id, failure_report)
                         │
                         ▼
1. Map report to domain FailureCause & determine is_retryable
                         │
                         ▼
2. Check Retry Eligibility (evaluate_retry_eligibility)
   - Workflow state == RUNNING?
   - is_retryable == True?
   - task.next_attempt_ordinal <= task.max_attempts?
                         │
         ┌───────────────┴───────────────┐
         ▼ (Retry Eligible)              ▼ (Exhausted / Non-Retryable)
3a. Settle as RETRY_WAIT            3b. Settle as Definitive FAILED
    - Resolve retry delay (policy)      - LLD-02 commit_worker_definitive_failure
    - Compute absolute ready_at_utc     - Attempt -> FAILED
    - LLD-02 commit_worker_failure_with_retry - Task -> FAILED
    - Attempt -> FAILED                 - Trigger Workflow Failure Direction
    - Task -> RETRY_WAIT                - Return SettlementResult.DEFINITIVE_FAILURE
    - Return SettlementResult.RETRY_SCHEDULED
```

---

## 5. Retry Decision, Operational Delay Policy & Ready Scheduling

### 5.1 Frozen Retry Semantics vs. Operational Delay Policy
In accordance with ADR-023 and frozen LLD-01/02/03:
- **Definition-Semantic Retry Budget:** `max_attempts` (integer $\ge 1$) on the task definition is the **only** definition-semantic retry field. No additional retry policy fields exist in the workflow YAML, AST, candidate spec, or `ValidatedWorkflowSpec`.
- **Operational Engine Delay Policy:** The delay duration before the next attempt is an operational engine policy governed by ADR-023 and configured in LLD-09. It does **not** alter the immutable workflow definition.
- **Separation of Concerns:** Operational delay policy does **not** decide whether a retry is permitted. Retry eligibility is strictly engine-owned based on failure classification, workflow state, and `max_attempts`.

```python
from typing import Protocol
from datetime import timedelta
from nexusflow.domain.values import AttemptOrdinal, FailureCategory

class RetryDelayPolicy(Protocol):
    """Operational engine policy resolving delay between execution attempts."""
    def delay_for(
        self,
        *,
        attempt_ordinal: AttemptOrdinal,
        failure_category: FailureCategory,
    ) -> timedelta:
        ...

class FixedRetryDelayPolicy:
    """Standard V1 reference operational policy."""
    def __init__(self, delay: timedelta = timedelta(seconds=5)) -> None:
        self._delay = delay

    def delay_for(
        self,
        *,
        attempt_ordinal: AttemptOrdinal,
        failure_category: FailureCategory,
    ) -> timedelta:
        return self._delay
```

### 5.2 Absolute Timestamp Durability Invariant
1. **Single Resolution:** The operational delay is resolved **once** at the moment of retryable failure settlement:
   $$\text{retry\_ready\_at\_utc} = \text{now\_utc} + \text{delay\_policy.delay\_for}(\dots)$$
2. **Atomically Persisted:** `retry_ready_at_utc` is persisted as an absolute UTC timestamp (`timestamptz`) in `task_executions` within the atomic failure transaction (`commit_worker_failure_with_retry` or `commit_internal_attempt_failure`).
3. **Never Recomputed on Restart:** Crash recovery and process restarts **never** recalculate or shift persisted retry deadlines. The persisted database timestamp is the sole authority.
4. **No `asyncio.sleep` for Durable Correctness:** In-memory timers and heaps serve purely as wakeup accelerators.

### 5.3 Retry Ready Transition (`RETRY_WAIT` $\to$ `RUNNABLE`)
When `now_utc >= retry_ready_at_utc`, the task transitions to `RUNNABLE` via LLD-02's `commit_retry_ready`:
- The transaction locks the parent workflow row `FOR UPDATE` and verifies `state == 'RUNNING'`.
- The task transitions `RETRY_WAIT → RUNNABLE` with `retry_ready_at_utc = NULL`.
- Existing `stable_input` is strictly preserved.
- LLD-04 discovers the `RUNNABLE` task and routes it to an available worker.

---

## 6. Internal Failure Triggers (Deadlines & Worker Loss)

### 6.1 Typed Internal Triggers
Internal failures are triggered by control-plane monitoring sweeps using the typed enum:
```python
class InternalFailureTrigger(str, Enum):
    START_DEADLINE_EXPIRED = "START_DEADLINE_EXPIRED"
    EXECUTION_TIMEOUT = "EXECUTION_TIMEOUT"
    WORKER_LOSS = "WORKER_LOSS"
```

### 6.2 Start Deadline Expiration
- **Preconditions:** `Attempt.state == CLAIMED` and `now_utc >= Attempt.start_deadline_utc`.
- **Semantics:** Indicates the worker failed to acknowledge execution start (`POST /internal/v1/worker/start`) within the required window.
- **Settlement:** Dispatches `commit_internal_attempt_failure(trigger=START_DEADLINE_EXPIRED)`.
- **Race Arbitration:** If the worker's start acknowledgement commits first, the attempt becomes `RUNNING` and start-deadline expiration receives zero affected rows (safe no-op). If timeout commits first, start acknowledgement is rejected with `STALE_ATTEMPT` (transport maps to `409 Conflict`).

### 6.3 Execution Timeout
- **Preconditions:** `Attempt.state == RUNNING`, `execution_timeout_utc IS NOT NULL`, and `now_utc >= Attempt.execution_timeout_utc`.
- **Semantics:** Indicates activity execution exceeded its maximum configured runtime.
- **Settlement:** Dispatches `commit_internal_attempt_failure(trigger=EXECUTION_TIMEOUT)`.
- **Race Arbitration:** If the worker's result callback commits before timeout settlement, the callback wins and sets `Attempt SUCCEEDED`. If timeout commits first, the attempt transitions to `FAILED`; a subsequent late worker callback is rejected with `STALE_ATTEMPT`.

### 6.4 Worker Loss Settlement
- **Preconditions:** `WorkerRegistry.live == False` for the session owning the attempt.
- **Fencing Predicate:** The settlement transaction re-validates `Attempt.worker_session_id == expected_lost_session` under database row lock.
- **Settlement:** Dispatches `commit_internal_attempt_failure(trigger=WORKER_LOSS)`.
- **Preserve Late Callback OCC Race:** Ephemeral registry status of `live == False` does **not** bypass durable callback arbitration. If a "lost" worker recovers and its callback reaches the database before worker-loss settlement commits, the callback commits successfully and worker-loss settlement safely aborts.

---

## 7. Workflow Direction: Failure & Cancellation

### 7.1 Workflow Failure Direction Arbitration
A definitive task failure (`TaskExecution.state == FAILED`) triggers workflow failure direction:
```text
Task FAILED ──► evaluate owning workflow state
                  │
                  ├─► If RUNNING: commit_workflow_failure_direction (RUNNING -> FAILING)
                  ├─► If FAILING: No-op (already draining)
                  ├─► If CANCELLING: No-op (cancellation takes precedence)
                  └─► If Terminal: No-op
```

**Anti-TOCTOU Serialization:** `commit_workflow_failure_direction` acquires a row lock `FOR UPDATE` on `workflow_executions`. The first committed direction wins:
- If `RUNNING → FAILING` commits first, subsequent user cancellation requests receive `CANCELLATION_CONFLICT`.
- If `RUNNING → CANCELLING` commits first, subsequent task failures settle locally as `FAILED` but do **not** alter the workflow direction.

### 7.2 User Workflow Cancellation Protocol
A user cancellation request via public API (LLD-08) executes `RequestWorkflowCancellationUseCase`:

| Current Workflow State | Action Taken | Result Returned |
| :--- | :--- | :--- |
| `INITIALIZING` | Transition `INITIALIZING → CANCELLING` | `CANCELLATION_ACCEPTED` |
| `RUNNING` | Transition `RUNNING → CANCELLING` | `CANCELLATION_ACCEPTED` |
| `CANCELLING` | No-op (Already cancelling) | `CANCELLATION_ACCEPTED` (Idempotent) |
| `CANCELLED` | No-op (Already terminal) | `CANCELLATION_ACCEPTED` (Idempotent) |
| `FAILING` | Reject cancellation | `CANCELLATION_CONFLICT` |
| `FAILED` | Reject cancellation | `CANCELLATION_CONFLICT` |
| `SUCCEEDED` | Reject cancellation | `CANCELLATION_CONFLICT` |

---

## 8. Cancellation Control Channel, Delivery & Acknowledgement Settlement

### 8.1 Cancellation Targeting & Delivery
Cancellation targets the exact tuple `(AttemptId, WorkerSessionId)`:
1. **Materialize Cancellation Deadline First:** When workflow enters `FAILING` or `CANCELLING`, the drain sweeper ensures `execution_attempts.cancellation_deadline_utc` is persisted if not already set:
   $$\text{cancellation\_deadline\_utc} = \text{now\_utc} + \text{cancellation\_grace\_seconds}$$
   This update is committed to PostgreSQL **before** transport delivery is attempted. Repeated drain passes are idempotent and do **not** overwrite an existing deadline.
2. **Control Command Generation:** LLD-06 constructs `TaskCancellationPayloadDTO`:
   ```python
   TaskCancellationPayloadDTO(
       attempt_id=attempt.id.value,
       worker_session_id=attempt.worker_session_id.value,
       task_execution_id=task.id.value,
       workflow_execution_id=workflow.id.value,
       reason="Workflow cancelling"
   )
   ```
3. **LLD-05 Transport:** Delivered via worker poll channel. No network calls inside database transactions.

### 8.2 Bounded Durable Cancellation-Control Rediscovery
The in-memory cancellation delivery queue is ephemeral. If hints are lost or the control plane restarts, cancellation delivery is recovered by querying PostgreSQL directly:
```sql
SELECT a.attempt_id, a.worker_session_id, a.task_execution_id, t.workflow_execution_id
FROM execution_attempts a
JOIN task_executions t ON a.task_execution_id = t.task_execution_id
JOIN workflow_executions w ON t.workflow_execution_id = w.workflow_execution_id
WHERE w.state IN ('FAILING', 'CANCELLING')
  AND a.state IN ('CLAIMED', 'RUNNING')
  AND a.cancellation_deadline_utc IS NOT NULL
ORDER BY a.cancellation_deadline_utc ASC, a.attempt_id ASC
LIMIT :batch_size;
```
For every discovered attempt, LLD-06 reconstructs `TaskCancellationPayloadDTO` and enqueues it for LLD-05 worker poll delivery. Repeated delivery is completely safe because workers correlate cancellation commands by `(AttemptId, WorkerSessionId)`.

### 8.3 Cancellation Acknowledgement Handling
When a worker receives a cancellation command, it reports its local observation via `POST /internal/v1/worker/callback` with `outcome_type = 'CANCEL_ACK'`:
- **`COOPERATIVELY_STOPPED`:** Control plane invokes `commit_worker_cancellation_ack`. Transitions `Attempt RUNNING/CLAIMED → CANCELLED` and `Task RUNNING → CANCELLED`.
- **`ALREADY_COMPLETED`:** Worker completed activity execution before cancellation was processed. Cancellation ack is a no-op; the result callback races through normal OCC.
- **`NOT_FOUND`:** Worker process has no record of the attempt. Control plane does **not** fabricate cancellation; settlement is left to `WORKER_LOSS` or `cancellation_deadline_utc` expiration.

### 8.4 Cancellation Resolution Deadline Sweep
If the worker does not acknowledge cancellation before `cancellation_deadline_utc`:
- The deadline sweeper executes `commit_internal_cancellation_deadline`.
- Verifies `cancellation_deadline_utc <= now_utc` and workflow is in drain state (`FAILING` or `CANCELLING`).
- Atomically transitions `Attempt → CANCELLED` and `Task → CANCELLED`.

---

## 9. Workflow Drain Processing (`FAILING` & `CANCELLING`)

### 9.1 Drain Invariants
Once a workflow enters `FAILING` or `CANCELLING`:
1. **No New Work:** LLD-04 scheduler routing excludes the workflow; no tasks become `RUNNABLE`; no new attempts are allocated.
2. **No Retries:** Failed tasks are never transitioned to `RETRY_WAIT`.
3. **Unstarted Tasks Cancelled:** Tasks in `PENDING`, `RUNNABLE`, or `RETRY_WAIT` are transitioned to `CANCELLED`.
4. **Active Outcomes Allowed:** Active attempts may legitimately settle `SUCCEEDED`, `FAILED`, or `CANCELLED` according to whichever valid commit wins the OCC race.

### 9.2 Drain Cross-State Convergence
- **Failure During CANCELLING:** If an active worker failure commits while workflow is `CANCELLING`, `Attempt → FAILED` and `Task → FAILED`. The workflow **remains `CANCELLING`** (it does not transition to `FAILING`).
- **Cancellation During FAILING:** If a cancellation ack commits while workflow is `FAILING`, `Attempt → CANCELLED` and `Task → CANCELLED`. The workflow **remains `FAILING`** (it does not transition to `CANCELLING`).

---

## 10. Workflow Terminalization & Output Materialization

### 10.1 Terminalization Eligibility: Exact Membership Invariant
A workflow may terminalize if and only if **all declared tasks** in `registered_definitions.validated_iws` exist as `task_executions` and have reached a terminal state (`SUCCEEDED`, `FAILED`, `CANCELLED`).

$$\text{terminal\_eligible} \iff \text{expected\_tasks} == \text{terminal\_tasks}$$

Relying solely on `count(non_terminal) == 0` is strictly prohibited because missing task rows could cause premature termination.

### 10.2 Terminalization Outcomes

#### 1. Workflow Success (`commit_workflow_success`):
- **Preconditions:** Workflow is `RUNNING`; exact task set matches; **every** task is `SUCCEEDED`.
- **Output Resolution:** Resolves output bindings from immutable specification using strictly `WorkflowTaskOutputBinding`.
  - If output bindings declared: Resolves values from upstream task outputs.
  - If no output bindings declared: Materializes explicit JSON `null` (`has_output = True, workflow_output = 'null'::jsonb`).
- **Atomicity:** `WorkflowExecution SUCCEEDED` can **never** exist without committed output.

#### 2. Workflow Terminal Failure (`commit_workflow_failure`):
- **Preconditions:** Workflow is `FAILING`; exact task set matches; every task is terminal (`SUCCEEDED`, `FAILED`, or `CANCELLED`).
- **Transitions:** `WorkflowExecution FAILING → FAILED`.

#### 3. Workflow Terminal Cancellation (`commit_workflow_cancellation`):
- **Preconditions:** Workflow is `CANCELLING`; exact task set matches; every task is terminal.
- **Transitions:** `WorkflowExecution CANCELLING → CANCELLED`.
- **Output:** No workflow output is produced (`has_output = False, workflow_output = NULL`).

---

## 11. Concurrency, Race Arbitration & OCC Invariants

All concurrent mutations are arbitrated via row-level locks and revision checks in PostgreSQL 16:

### 11.1 Key Concurrency Race Matrix

| Race Condition | Competing Operations | Winning Operation | Losing Operation Behavior | Invariant Enforced |
| :--- | :--- | :--- | :--- | :--- |
| **Success vs. Cancellation** | Worker Success Callback vs. Cancellation Ack Settlement | First commit to update Attempt revision | Loser receives OCC conflict (`STALE_ATTEMPT`). | Attempt never double-settles; output never written after cancel. |
| **Success vs. Execution Timeout** | Worker Success Callback vs. Timeout Sweep | First commit to update Attempt revision | If timeout wins, callback receives `STALE_ATTEMPT`. If success wins, timeout aborts. | Single terminal state per attempt. |
| **Start Ack vs. Start Deadline** | Worker Start Ack vs. Start Deadline Sweep | First commit on `CLAIMED` attempt row | If timeout wins, start ack rejected (`STALE_ATTEMPT`). | Worker must not execute expired assignment. |
| **Late Callback vs. Worker Loss** | Late Worker Success vs. Worker-Loss Sweep | First commit to update Attempt revision | If callback commits first, worker-loss aborts. If worker-loss commits, callback rejected. | Ephemeral liveness does not override durable facts. |
| **Definitive Failure vs. Cancellation** | Task Failure Direction vs. User Cancellation | First transaction to acquire `FOR UPDATE` on workflow row | Loser respects committed direction (`FAILING` or `CANCELLING`). | Workflow direction is irrevocable. |
| **Duplicate Success Callbacks** | Retried identical HTTP success callback | First commit transitions state | Second request detects matching committed output and returns `IDEMPOTENT_DUPLICATE`. | Callback retries are completely safe and idempotent. |

---

## 12. Storage Failures & Operation-Specific UNKNOWN_OUTCOME Reconciliation

### 12.1 Storage Failures: Fail-Closed Invariant
If PostgreSQL is unreachable or drops the connection during commit, the control plane **fails closed**. No in-memory state is fabricated, and no downstream wakeups are emitted.

### 12.2 Operation-Specific Proof Reconciliation
When a commit returns `UNKNOWN_OUTCOME` (e.g., dropped connection during commit), the application rereads authoritative database records. Generic existence checks are prohibited; reconciliation must prove the **specific intended durable mutation**:

```text
Commit returns UNKNOWN_OUTCOME
               │
               ▼
 Reread Authoritative Relational Facts
               │
   ┌───────────┼───────────┬───────────┐
   ▼           ▼           ▼           ▼
(Exact Match)(Lost Race)(Absent & Safe)(Mismatch)
   │           │           │           │
   ▼           ▼           ▼           ▼
COMMITTED   LOST_RACE   SAFE_RETRY  INTEGRITY_ERROR
```

1. **Worker Success Settlement:**
   - **Proof Required:** `Attempt.attempt_id == target`, `Attempt.worker_session_id == target`, `Attempt.state == SUCCEEDED`, `Task.state == SUCCEEDED`, `Task.has_output == TRUE`, and `Task.task_output == intended_output` (including JSON null equality).
   - **Classification:** If proven $\to$ `COMMITTED`. If Attempt is `FAILED` or `CANCELLED` $\to$ `LOST_RACE`. If output conflicts $\to$ `INTEGRITY_ERROR`.
2. **Retryable Worker Failure Settlement:**
   - **Proof Required:** `Attempt.state == FAILED`, `Task.state == RETRY_WAIT`, and `Task.retry_ready_at_utc == intended_deadline`.
   - **Classification:** If proven $\to$ `RETRY_SCHEDULED`. If Task is `FAILED` or `CANCELLED` $\to$ `LOST_RACE`. `Attempt FAILED` alone is **not** proof.
3. **Definitive Worker Failure Settlement:**
   - **Proof Required:** `Attempt.state == FAILED` AND `Task.state == FAILED`.
   - **Classification:** If proven $\to$ `DEFINITIVE_FAILURE`. If Task is `RETRY_WAIT` $\to$ `LOST_RACE`.
4. **Internal Failure Trigger Settlement:**
   - **Proof Required:** `Attempt.state == FAILED`, Task in intended state (`RETRY_WAIT` or `FAILED`), and `failure_code` matches trigger cause.
   - **Classification:** If proven $\to$ `COMMITTED`. If Attempt is `SUCCEEDED` $\to$ `LOST_RACE`.
5. **Workflow Failure Direction (`RUNNING → FAILING`):**
   - **Proof Required:** `Workflow.state == FAILING`.
   - **Classification:** If `Workflow.state == FAILING` $\to$ `COMMITTED`. If `Workflow.state == CANCELLING` $\to$ `LOST_RACE` (cancellation won). `CANCELLING` never proves failure direction.
6. **Workflow Cancellation Direction (`INITIALIZING/RUNNING → CANCELLING`):**
   - **Proof Required:** `Workflow.state == CANCELLING` (or `CANCELLED` with matching lineage).
   - **Classification:** If proven $\to$ `CANCELLATION_ACCEPTED`. If `Workflow.state in (FAILING, FAILED, SUCCEEDED)` $\to$ `LOST_RACE` / `CANCELLATION_CONFLICT`.
7. **Workflow Success Terminalization (`RUNNING → SUCCEEDED`):**
   - **Proof Required:** `Workflow.state == SUCCEEDED`, `has_output == TRUE`, and output equals resolved workflow output.
   - **Classification:** If proven $\to$ `COMMITTED`. If `FAILED` or `CANCELLED` $\to$ `LOST_RACE`.
8. **Workflow Failure Terminalization (`FAILING → FAILED`):**
   - **Proof Required:** `Workflow.state == FAILED`.
   - **Classification:** If proven $\to$ `COMMITTED`. `CANCELLED` or `SUCCEEDED` does not prove failure terminalization.
9. **Workflow Cancellation Terminalization (`CANCELLING → CANCELLED`):**
   - **Proof Required:** `Workflow.state == CANCELLED` and `has_output == FALSE`.
   - **Classification:** If proven $\to$ `COMMITTED`.
10. **Cancellation Acknowledgement (`CANCEL_ACK`):**
    - **Proof Required:** `Attempt.state == CANCELLED` AND `Task.state == CANCELLED`.
    - **Classification:** If proven $\to$ `COMMITTED`. If `SUCCEEDED` or `FAILED` $\to$ `LOST_RACE`.
11. **Unstarted Drain Task Cancellation:**
    - **Proof Required:** `Task.state == CANCELLED` while owning workflow is `FAILING` or `CANCELLING`.
    - **Classification:** If proven $\to$ `COMMITTED`.

---

## 13. Application Services & Component Architecture

### 13.1 Typed Application Outcomes
Domain and application services return structured result types:

```python
class SettlementStatus(StrEnum):
    COMMITTED = "COMMITTED"
    IDEMPOTENT_DUPLICATE = "IDEMPOTENT_DUPLICATE"
    STALE_ATTEMPT = "STALE_ATTEMPT"
    WRONG_SESSION = "WRONG_SESSION"
    LOST_RACE = "LOST_RACE"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    DEFINITIVE_FAILURE = "DEFINITIVE_FAILURE"
    STORAGE_UNAVAILABLE = "STORAGE_UNAVAILABLE"
    UNKNOWN_OUTCOME = "UNKNOWN_OUTCOME"
    INTEGRITY_ERROR = "INTEGRITY_ERROR"
    CANCELLATION_ACCEPTED = "CANCELLATION_ACCEPTED"
    CANCELLATION_CONFLICT = "CANCELLATION_CONFLICT"

@dataclass(frozen=True, slots=True)
class SettlementResult:
    status: SettlementStatus
    message: str | None = None
```

### 13.2 Package & Module Layout
Consistent with LLD-01 and LLD-02:

```text
src/nexusflow/
    domain/
        execution/
            state_machines.py       # Pure state transition functions
        failures/
            taxonomy.py             # FailureCategory, FailureCause
        retry/
            delay_policy.py         # RetryDelayPolicy protocol & reference policies
        services/
            retry_eligibility.py    # Engine retry evaluation
            output_resolution.py    # Named workflow output resolver

    application/
        settlement/
            worker_success.py       # SettleWorkerSuccessUseCase
            worker_failure.py       # SettleWorkerFailureUseCase
            internal_failure.py     # SettleInternalFailureUseCase
        cancellation/
            request_cancel.py       # RequestWorkflowCancellationUseCase
            process_cancel_ack.py   # ProcessCancellationAckUseCase
            drain_workflow.py       # DrainWorkflowUseCase
        timers/
            retry_deadline.py       # ProcessRetryDeadlineUseCase
            start_deadline.py       # ProcessStartDeadlineUseCase
            execution_timeout.py    # ProcessExecutionTimeoutUseCase
            cancel_deadline.py      # ProcessCancellationDeadlineUseCase
        terminalization/
            workflow_terminalize.py # TryTerminalizeWorkflowUseCase

    ports/
        persistence/
            settlement_port.py      # Persistence mutation contracts
        workers/
            control_transport.py    # Non-authoritative cancellation delivery
        clock.py                    # Clock protocol
```

### 13.3 Concrete Application Ports
```python
class SettlementPersistencePort(Protocol):
    async def commit_worker_task_success(
        self,
        attempt_id: AttemptId,
        worker_session_id: WorkerSessionId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        output: OutputCommitted,
        now_utc: datetime
    ) -> CommitOutcome: ...

    async def commit_worker_failure_with_retry(
        self,
        attempt_id: AttemptId,
        worker_session_id: WorkerSessionId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        workflow_id: WorkflowExecutionId,
        ready_at_utc: datetime,
        cause: FailureCause,
        now_utc: datetime
    ) -> CommitOutcome: ...

    async def commit_worker_definitive_failure(
        self,
        attempt_id: AttemptId,
        worker_session_id: WorkerSessionId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        cause: FailureCause,
        now_utc: datetime
    ) -> CommitOutcome: ...

    async def commit_internal_attempt_failure(
        self,
        attempt_id: AttemptId,
        expected_attempt_revision: int,
        task_id: TaskExecutionId,
        expected_task_revision: int,
        trigger: InternalFailureTrigger,
        cause: FailureCause,
        is_retryable: bool,
        retry_ready_at_utc: datetime | None,
        expected_lost_worker_session_id: WorkerSessionId | None,
        now_utc: datetime
    ) -> CommitOutcome: ...

    async def commit_workflow_success(
        self,
        workflow_id: WorkflowExecutionId,
        expected_workflow_revision: int,
        output: OutputCommitted,
        now_utc: datetime
    ) -> CommitOutcome: ...
```

---

## 14. Sequence Diagrams

### 14.1 Worker Success $\to$ Task Succeeded $\to$ Downstream Wakeup
```
Worker Process                    FastAPI Callback Route          Settlement Service            LLD-02 Database             Scheduler (LLD-04)
      │                                     │                              │                           │                           │
      │ 1. POST /callback (SUCCESS, output) │                              │                           │                           │
      ├────────────────────────────────────>│                              │                           │                           │
      │                                     │ 2. SettleWorkerSuccess       │                           │                           │
      │                                     ├─────────────────────────────>│                           │                           │
      │                                     │                              │ 3. commit_task_success    │                           │
      │                                     │                              ├──────────────────────────>│                           │
      │                                     │                              │    (Attempt SUCCEEDED,    │                           │
      │                                     │                              │     Task SUCCEEDED,       │                           │
      │                                     │                              │     TaskOutput committed, │                           │
      │                                     │                              │     HistoryEntry committed│                           │
      │                                     │                              │<──────────────────────────┤                           │
      │                                     │                              │ 4. Return COMMITTED       │                           │
      │                                     │<─────────────────────────────┤                           │                           │
      │ 5. 200 OK (ACCEPTED)                │                              │                           │                           │
      │<────────────────────────────────────┤                              │ 5. Downstream Wakeup      │                           │
      │                                     │                              ├──────────────────────────────────────────────────────>│
      │                                     │                              │    TaskSucceeded(task_id) │                           │
```

### 14.2 Worker Failure $\to$ Retry Backoff $\to$ Ready $\to$ Runnable
```
Worker Process                    FastAPI Callback Route          Settlement Service            LLD-02 Database             Scheduler (LLD-04)
      │                                     │                              │                           │                           │
      │ 1. POST /callback (FAILURE, error)  │                              │                           │                           │
      ├────────────────────────────────────>│                              │                           │                           │
      │                                     │ 2. Evaluate Retry Eligibility│                           │                           │
      │                                     │    (Budget ok, policy ok)    │                           │                           │
      │                                     │ 3. Resolve delay_for(...)    │                           │                           │
      │                                     │ 4. commit_failure_with_retry │                           │                           │
      │                                     ├─────────────────────────────>│                           │                           │
      │                                     │                              ├──────────────────────────>│                           │
      │                                     │                              │    (Attempt FAILED,       │                           │
      │                                     │                              │     Task RETRY_WAIT,      │                           │
      │                                     │                              │     retry_ready_at set)   │                           │
      │                                     │                              │<──────────────────────────┤                           │
      │ 5. 200 OK (RETRY_SCHEDULED)         │                              │                           │                           │
      │<────────────────────────────────────┤                              │                           │                           │
      │                                     │                              │                           │                           │
      │                                     │ (Time passes: now >= ready)  │                           │                           │
      │                                     │ 6. Timer Sweeper executes    │                           │                           │
      │                                     │    commit_retry_ready        │                           │                           │
      │                                     │                              ├──────────────────────────>│                           │
      │                                     │                              │    (Task RUNNABLE,        │                           │
      │                                     │                              │     retry_ready_at NULL)  │                           │
      │                                     │                              │<──────────────────────────┤                           │
      │                                     │                              │ 7. Runnable Wakeup        │                           │
      │                                     │                              ├──────────────────────────────────────────────────────>│
```

### 14.3 User Cancellation $\to$ Worker Control Command $\to$ Cancel Ack
```
Client (LLD-08)                   Cancellation Service           Worker Transport (LLD-05)     Worker Process              LLD-02 Database
      │                                     │                              │                         │                           │
      │ 1. POST /executions/{id}/cancel     │                              │                         │                           │
      ├────────────────────────────────────>│                              │                         │                           │
      │                                     │ 2. commit_cancellation_dir   │                         │                           │
      │                                     ├───────────────────────────────────────────────────────────────────────────────────>│
      │                                     │    (Workflow CANCELLING)     │                         │                           │
      │                                     │<───────────────────────────────────────────────────────────────────────────────────┤
      │ 3. 200 OK (CANCELLING)              │                              │                         │                           │
      │<────────────────────────────────────┤                              │                         │                           │
      │                                     │ 4. Enqueue CANCEL_COMMAND    │                         │                           │
      │                                     ├─────────────────────────────>│                         │                           │
      │                                     │    (attempt_id, session_id)  │                         │                           │
      │                                     │                              │ 5. Deliver on Long Poll │                           │
      │                                     │                              ├────────────────────────>│                           │
      │                                     │                              │                         │ 6. Stop local execution   │
      │                                     │                              │ 7. POST /callback       │                           │
      │                                     │                              │    (CANCEL_ACK)         │                           │
      │                                     │                              │<────────────────────────┤                           │
      │                                     │ 8. commit_cancellation_ack   │                         │                           │
      │                                     ├───────────────────────────────────────────────────────────────────────────────────>│
      │                                     │    (Attempt CANCELLED,       │                         │                           │
      │                                     │     Task CANCELLED)          │                         │                           │
      │                                     │<───────────────────────────────────────────────────────────────────────────────────┤
```

---

## 15. Concrete Transition & Race Tables

### 15.1 Worker Outcome Settlement Matrix
| Current Attempt State | Callback Outcome | Current Workflow State | Action Taken | Resulting Attempt State | Resulting Task State |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `RUNNING` | `SUCCESS` | `RUNNING` | Commit success | `SUCCEEDED` | `SUCCEEDED` |
| `RUNNING` | `SUCCESS` | `FAILING` / `CANCELLING`| Commit success (drain) | `SUCCEEDED` | `SUCCEEDED` |
| `RUNNING` | `FAILURE` (Retryable) | `RUNNING` | Commit retry wait | `FAILED` | `RETRY_WAIT` |
| `RUNNING` | `FAILURE` (Exhausted) | `RUNNING` | Commit definitive failure| `FAILED` | `FAILED` |
| `RUNNING` | `FAILURE` | `FAILING` / `CANCELLING`| Commit definitive failure| `FAILED` | `FAILED` |
| `RUNNING` | `CANCEL_ACK` | `CANCELLING` / `FAILING`| Commit cancellation ack | `CANCELLED` | `CANCELLED` |
| `CLAIMED` | Any | Any | Reject (Must start first)| Unchanged | Unchanged |
| `SUCCEEDED` | `SUCCESS` (Same) | Any | Idempotent duplicate | `SUCCEEDED` | `SUCCEEDED` |
| `SUCCEEDED` | `SUCCESS` (Diff) | Any | Reject conflict | `SUCCEEDED` | `SUCCEEDED` |
| `FAILED` / `CANCELLED` | Any | Any | Reject stale callback | Unchanged | Unchanged |

### 15.2 Internal Failure Trigger Matrix
| Trigger | Preconditions | Budget Condition | Resulting Attempt State | Resulting Task State |
| :--- | :--- | :--- | :--- | :--- |
| `START_DEADLINE_EXPIRED` | `CLAIMED`, `now >= start_deadline` | Budget Remains, WF `RUNNING` | `FAILED` | `RETRY_WAIT` |
| `START_DEADLINE_EXPIRED` | `CLAIMED`, `now >= start_deadline` | Budget Exhausted / WF Drain | `FAILED` | `FAILED` |
| `EXECUTION_TIMEOUT` | `RUNNING`, `now >= timeout` | Budget Remains, WF `RUNNING` | `FAILED` | `RETRY_WAIT` |
| `EXECUTION_TIMEOUT` | `RUNNING`, `now >= timeout` | Budget Exhausted / WF Drain | `FAILED` | `FAILED` |
| `WORKER_LOSS` | `CLAIMED`/`RUNNING`, Session Dead | Budget Remains, WF `RUNNING` | `FAILED` | `RETRY_WAIT` |
| `WORKER_LOSS` | `CLAIMED`/`RUNNING`, Session Dead | Budget Exhausted / WF Drain | `FAILED` | `FAILED` |

---

## 16. Durable vs. Ephemeral State Inventory

| State Concept | Storage Location | Durability Authority | Lifecycle & Rebuild Rules |
| :--- | :--- | :--- | :--- |
| `workflow_executions.state` | PostgreSQL 16 | **Durable Authority** | Authoritative lifecycle; survives crashes. |
| `task_executions.state` | PostgreSQL 16 | **Durable Authority** | Authoritative task progress. |
| `task_executions.task_output` | PostgreSQL 16 | **Durable Authority** | Immutable once `SUCCEEDED`. Distinct JSON `null` support. |
| `execution_attempts.state` | PostgreSQL 16 | **Durable Authority** | Attempt execution lifecycle. |
| `retry_ready_at_utc` | PostgreSQL 16 | **Durable Authority** | Absolute deadline; never recomputed on restart. |
| `start_deadline_utc` | PostgreSQL 16 | **Durable Authority** | Absolute deadline for start acknowledgement. |
| `execution_timeout_utc` | PostgreSQL 16 | **Durable Authority** | Absolute deadline for activity execution. |
| `cancellation_deadline_utc` | PostgreSQL 16 | **Durable Authority** | Absolute deadline for cancellation resolution. |
| `history_entries` | PostgreSQL 16 | **Durable Audit Trail**| Append-only audit record; never replayed for state. |
| In-Memory Timer Heap | Python Process Memory | Ephemeral Acceleration | Rebuilt from PostgreSQL index scans on startup. |
| Downstream Wakeup Queue | Python `asyncio.Queue` | Ephemeral Progression | Loss repaired by LLD-04 defensive `PENDING` scan. |
| Cancellation Control Queue | Python `asyncio.Queue` | Ephemeral Transport | Backed by long-poll database fallback queries. |
| Worker Registry Liveness | Python Process Memory | Ephemeral Observation | Rebuilt dynamically as workers register and heartbeat. |

---

## 17. Deterministic Testing Strategy

### 17.1 Success Settlement Tests (`tests/integration/settlement/test_success.py`)
1. **Atomic Success Commit:** Verify Attempt $\to$ `SUCCEEDED`, Task $\to$ `SUCCEEDED`, output JSON persisted, and `TaskExecutionSucceeded` history row committed in a single SQL transaction.
2. **JSON Null Output:** Submit success callback with `"output": null`. Assert `has_output = TRUE` and `task_output = 'null'::jsonb`.
3. **Duplicate Identical Success Callback:** Call success settlement twice with identical output. Assert first returns `COMMITTED`, second returns `IDEMPOTENT_DUPLICATE`; zero duplicate history rows.
4. **Conflicting Success Callback:** Call success settlement with different output for already-succeeded task. Assert rejected with `OCC_CONFLICT` / `STALE_ATTEMPT`.
5. **Unknown Success Commit Reconciliation:** Simulate dropped connection during success commit; execute reconciliation; assert classified `COMMITTED` if facts match, or `LOST_RACE` if Attempt is `FAILED`/`CANCELLED`.

### 17.2 Worker Failure & Retry Tests (`tests/integration/settlement/test_failure_retry.py`)
6. **Retryable Failure with Budget:** Submit failure callback when `ordinal < max_attempts` and workflow is `RUNNING`. Assert Attempt $\to$ `FAILED`, Task $\to$ `RETRY_WAIT`, `retry_ready_at_utc` populated.
7. **Retry Deadline to Runnable:** Advance FakeClock past `retry_ready_at_utc`. Execute retry deadline sweeper. Assert Task transitions `RETRY_WAIT → RUNNABLE`.
8. **Retry Budget Exhaustion:** Submit failure callback when `ordinal == max_attempts`. Assert Attempt $\to$ `FAILED`, Task $\to$ `FAILED`.
9. **Non-Retryable Failure:** Submit permanent failure. Assert Task transitions directly to `FAILED`.
10. **No Retries in FAILING:** Submit failure callback for active attempt while workflow is `FAILING`. Assert task transitions to `FAILED` even if budget remains.
11. **No Retries in CANCELLING:** Submit failure callback for active attempt while workflow is `CANCELLING`. Assert task transitions to `FAILED`; workflow remains `CANCELLING`.
12. **Unknown Retryable Failure Commit:** Simulate dropped connection during retry commit; assert reconciliation requires `Task.state == RETRY_WAIT` and matching deadline to report `RETRY_SCHEDULED`.
13. **Unknown Definitive Failure Commit:** Simulate dropped connection during definitive failure; assert reconciliation requires both `Attempt` and `Task` `FAILED`.

### 17.3 Start Deadline & Execution Timeout Tests (`tests/concurrency/settlement/test_deadlines.py`)
14. **Start Deadline Exact Boundary:** Advance clock to exact start deadline; assert trigger settles attempt `FAILED`.
15. **Start Ack Wins Start Deadline Race:** Barrier-synchronize start ack and deadline sweep; assert start ack winner commits `RUNNING`; deadline sweep aborts.
16. **Start Deadline Wins Race:** Assert deadline sweep commits `FAILED`; subsequent start ack receives `STALE_ATTEMPT`.
17. **Execution Timeout Settlement:** Advance clock past execution timeout; assert attempt settles `FAILED` and evaluates retry.
18. **Success Wins Timeout Race:** Worker success commits before timeout sweep; assert timeout sweep aborts safely.
19. **Timeout Wins Late Success Race:** Timeout commits `FAILED`; late success callback receives `STALE_ATTEMPT`.

### 17.4 Worker Loss Tests (`tests/concurrency/settlement/test_worker_loss.py`)
20. **Exact WorkerSessionId Fencing:** Trigger worker loss for session S1; assert attempts owned by session S2 are completely untouched.
21. **Late Callback Wins Worker Loss Race:** Worker registry marks session dead; worker submits success callback before loss sweeper commits; assert callback commits successfully.
22. **Worker Loss Wins Race:** Loss sweeper commits `FAILED`; subsequent late callback is rejected as stale.
23. **Stale Session Trigger Rejection:** Worker loss sweeper with mismatched session ID cannot mutate another session's attempt.

### 17.5 Workflow Cancellation Direction Tests (`tests/integration/cancellation/test_cancellation_direction.py`)
24. **INITIALIZING to CANCELLING:** User cancels workflow in `INITIALIZING`; assert transitions to `CANCELLING`.
25. **RUNNING to CANCELLING:** User cancels workflow in `RUNNING`; assert transitions to `CANCELLING`.
26. **Duplicate Cancellation in CANCELLING:** Repeat cancellation command; assert idempotent `CANCELLATION_ACCEPTED`.
27. **Duplicate Cancellation in CANCELLED:** Repeat cancellation on terminal cancelled workflow; assert idempotent `CANCELLATION_ACCEPTED`.
28. **Cancellation Conflict in FAILING:** User attempts to cancel workflow in `FAILING`; assert rejected with `CANCELLATION_CONFLICT`.
29. **Cancellation Conflict in FAILED:** User attempts to cancel `FAILED` workflow; assert rejected with `CANCELLATION_CONFLICT`.
30. **Cancellation Conflict in SUCCEEDED:** User attempts to cancel `SUCCEEDED` workflow; assert rejected with `CANCELLATION_CONFLICT`.

### 17.6 Cancellation ACK & Control Delivery Tests (`tests/integration/cancellation/test_cancel_ack.py`)
31. **Cooperative Stop ACK:** Worker sends `CANCEL_ACK` (`COOPERATIVELY_STOPPED`); assert Attempt $\to$ `CANCELLED` and Task $\to$ `CANCELLED`.
32. **Already Completed ACK:** Worker sends `CANCEL_ACK` (`ALREADY_COMPLETED`); assert no mutation; normal success callback processed.
33. **Not Found ACK:** Worker sends `NOT_FOUND`; assert control plane does not fabricate cancellation.
34. **Wrong Session Cancellation ACK:** Mismatched session sends cancel ack; assert rejected with `WRONG_SESSION`.
35. **Duplicate Cancellation ACK:** Retried cancel ack returns idempotent success without duplicate history.
36. **Cancellation Deadline Expiration:** Advance clock past `cancellation_deadline_utc`; assert internal sweep settles Attempt and Task as `CANCELLED`.
37. **Ephemeral Cancel Queue Loss:** Drop in-memory queue; assert database fallback reconstructs `TaskCancellationPayloadDTO`.
38. **Duplicate Control Delivery Harmless:** Redeliver cancellation command; assert worker handles idempotently and deadline is not reset.

### 17.7 Drain Semantics Tests (`tests/integration/drain/test_drain_processing.py`)
39. **Unstarted PENDING to CANCELLED:** Workflow enters `FAILING`; assert unstarted PENDING task becomes `CANCELLED`.
40. **Unstarted RUNNABLE to CANCELLED:** Workflow enters `CANCELLING`; assert unstarted RUNNABLE task becomes `CANCELLED`.
41. **Unstarted RETRY_WAIT to CANCELLED:** Workflow enters `FAILING`; assert RETRY_WAIT task becomes `CANCELLED`.
42. **Active CLAIMED Receives Cancel Command:** Assert draining workflow targets active CLAIMED attempt with cancel command.
43. **Active RUNNING Receives Cancel Command:** Assert draining workflow targets active RUNNING attempt with cancel command.
44. **Active Success During CANCELLING:** Active attempt completes successfully during drain; assert settles `SUCCEEDED`; workflow remains `CANCELLING`.
45. **Active Failure During CANCELLING:** Active attempt fails during drain; assert settles `FAILED`; workflow remains `CANCELLING`.
46. **Active Cancel During FAILING:** Cancel ack arrives during `FAILING`; assert settles `CANCELLED`; workflow remains `FAILING`.
47. **No New Attempt During Drain:** Assert LLD-04 routes zero tasks for workflows in `FAILING` or `CANCELLING`.

### 17.8 Direction Races & Terminalization Tests (`tests/integration/terminalization/test_terminalization.py`)
48. **Definitive Failure Wins Cancellation Race:** Race failure direction and user cancellation; assert first committed direction is irrevocable.
49. **Cancellation Wins Failure Race:** User cancellation commits first; subsequent task failure does not change direction to `FAILING`.
50. **Missing Membership Prevents SUCCEEDED:** Missing task row prevents `commit_workflow_success`.
51. **Missing Membership Prevents FAILED:** Missing task row prevents `commit_workflow_failure`.
52. **Missing Membership Prevents CANCELLED:** Missing task row prevents `commit_workflow_cancellation`.
53. **Workflow Success Output Resolution:** Resolves output bindings into `workflow_output` atomically with `SUCCEEDED`.
54. **No-Output Workflow Commits JSON Null:** Workflow with no output bindings materializes `has_output = TRUE` and `workflow_output = 'null'::jsonb`.
55. **FAILING Terminalizes Only After All Terminal:** Tasks remaining active prevents terminalization to `FAILED`.
56. **CANCELLING Terminalizes Only After All Terminal:** Tasks remaining active prevents terminalization to `CANCELLED`.

### 17.9 Unknown Commit Reconciliation Tests (`tests/integration/reconciliation/test_unknown_commits.py`)
57. **Failure Direction Intended but CANCELLING Wins:** Reconciliation asserts `Workflow.state == CANCELLING` is classified as `LOST_RACE`, not `COMMITTED`.
58. **Cancellation Direction Intended but FAILING Wins:** Reconciliation asserts `Workflow.state == FAILING` is classified as `LOST_RACE`, not `COMMITTED`.
59. **Success Intended but CANCELLED/FAILED Wins:** Reconciliation asserts classified as `LOST_RACE`, not `COMMITTED`.
60. **Retry Intended but Task FAILED:** Reconciliation asserts classified as `LOST_RACE`, not `RETRY_SCHEDULED`.
61. **Cancel ACK Intended but Attempt SUCCEEDED:** Reconciliation asserts classified as `LOST_RACE`, not `COMMITTED`.

### 17.10 Process Lifecycle Tests (`tests/integration/lifecycle/test_process_shutdown.py`)
62. **Control-Plane DRAINING Causes No Workflow Cancellation:** Control plane entering process drain does not mutate workflows to `CANCELLING`.
63. **Admitted Callbacks Finish During Process Drain:** Active callbacks complete and commit during bounded shutdown grace window.

---

## 18. Architectural Boundaries & Downstream Handoff

### 18.1 Boundary with LLD-07 (Recovery & Reconciliation)
- **LLD-06 Owns:** Standard operational settlement logic, normal-runtime deadline sweepers, drain processors, and operation-specific unknown-commit reconciliation.
- **LLD-07 Owns:** Orchestrator startup sequence, crash recovery scans, recovery readiness gates, repair of partial `INITIALIZING` executions, and startup rediscovery. LLD-07 invokes LLD-06 settlement use cases as reusable domain building blocks.

### 18.2 Boundary with LLD-08 (External Ingestion HTTP APIs)
- **LLD-06 Owns:** Application use cases, domain cancellation logic, and typed return outcomes (`SettlementResult`, `CancellationAcceptedResult`, `ConflictOutcome`).
- **LLD-08 Owns:** Public HTTP routes, authentication, FastAPI request parsing, and HTTP status code mapping (`200`, `202`, `404`, `409`).

### 18.3 Boundary with LLD-09 (Observability & Configuration)
- **LLD-06 Owns:** Triggering domain events, emitting low-cardinality telemetry signals, and consuming `RetryDelayPolicy`.
- **LLD-09 Owns:** OpenTelemetry tracer wiring, Prometheus `/metrics` scrapers, concrete delay policy configuration, and structured JSON log formatting.

---

## 19. Final Design Validation Checklist

- [x] **Exact Frozen State Machines**: Uses strictly `WorkflowExecution` (7 states), `TaskExecution` (7 states), and `ExecutionAttempt` (5 states). Zero synthetic states introduced.
- [x] **Authoritative Settlement Boundary**: Workers report observations only; control plane owns all state machine transitions, retry decisions, and terminalization.
- [x] **No Unapproved Retry Fields**: `max_attempts` is the only definition-semantic retry field. Unapproved fields (`initial_interval_seconds`, `backoff_coefficient`, `max_interval_seconds`, `jitter`) removed from definition schemas.
- [x] **Operational Retry Delay Policy**: Delay resolution is encapsulated via `RetryDelayPolicy` protocol. Absolute `retry_ready_at_utc` is persisted and never recomputed on restart.
- [x] **No False Deterministic Full Jitter**: Acknowledged random sampling nature; durability invariant focused on single resolution and atomic persistence.
- [x] **No Attempt Creation in Settlement**: LLD-06 transitions tasks to `RETRY_WAIT` and `RUNNABLE`; attempt allocation occurs exclusively in LLD-04.
- [x] **Task SUCCEEDED Never Lacks Output**: Output is committed atomically with state transition; explicit JSON `null` is supported and distinguishable.
- [x] **Operation-Specific Unknown Commit Proof**: Every major mutation defines rigorous proof predicates; generic existence checks and history-only proofs prohibited.
- [x] **Typed Internal Failure Triggers**: `START_DEADLINE_EXPIRED`, `EXECUTION_TIMEOUT`, `WORKER_LOSS` handled via typed enum without string fallthrough.
- [x] **Cancellation Protocol Targeting**: Targeted to `(AttemptId, WorkerSessionId)`; delivered via LLD-05 pull transport; bounded by `cancellation_deadline_utc`.
- [x] **Durable Cancellation Rediscovery**: Database fallback query defined to recover lost ephemeral cancellation commands.
- [x] **Drain Processing & Active Outcomes**: Unstarted tasks become `CANCELLED`; active attempts may settle `SUCCEEDED`, `FAILED`, or `CANCELLED` during drain.
- [x] **Exact Membership Invariant**: Workflow terminalization verifies exact expected task set against immutable definition; rejects partial counts.
- [x] **No HTTP Codes in Domain Outcomes**: Domain and application services return pure typed outcomes; HTTP status codes mapped exclusively at transport boundary.
- [x] **No Network in DB Transactions**: Database transactions perform pure SQL mutations; control transport and wakeups fire after commit.
- [x] **No Distributed Brokers / gRPC / WebSockets**: Conforms strictly to PostgreSQL 16 and HTTP long-polling architecture.

---

### Classification

**LLD-06 — Architecture-Ready / Approved as LLD-07 Input**
