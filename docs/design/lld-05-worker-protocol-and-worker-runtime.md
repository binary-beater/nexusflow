# NexusFlow V1 — LLD-05: Worker Protocol & Worker Runtime

**Document Status:** Architecture-Ready / Approved as LLD-06 Input  
**Authoritative References:** ADR-007 (Task Lifecycle & Attempt Model), ADR-008 (Worker Coordination & Liveness), ADR-009 (Task Routing), ADR-010 (Workflow Data Flow), ADR-011 (State Persistence), ADR-012 (Recovery), ADR-013 (Consistency & Concurrency), ADR-016 (Observability), ADR-017 (Graceful Shutdown), ADR-018 (Error Handling), ADR-019 (Project / Service Boundaries), ADR-020 (Technology Selection), ADR-021 (Testing Strategy), ADR-022 (Security), ADR-023 (Configuration), NexusFlow V1 HLD, LLD-01 (Domain Model & Module Contracts), LLD-02 (PostgreSQL Schema & Persistence Transactions), LLD-03 (Definition Ingestion & Validation Pipeline), LLD-04 (Scheduling, Routing & Ownership).  
**Downstream Dependents:** LLD-06 (Settlement, Timers & Cancellation Protocol), LLD-07 (Recovery & Reconciliation), LLD-08 (External Ingestion HTTP APIs).

---

## 1. Primary Objective & Protocol Overview

### 1.1 Objective Statement
This document defines the complete implementation-level design for NexusFlow V1 worker registration, `WorkerSession` lifecycle, heartbeat/liveness tracking, capability advertisement, task claiming/start acknowledgement, worker-side execution runtime, HTTP pull/long-poll transport, callback correlation, graceful worker drain, and control-plane/worker protocol boundaries.

Specifically, it answers:
> *Exactly how does an external worker process establish a WorkerSession, advertise exact capabilities, remain live via heartbeats, receive an already-durably-owned Attempt, acknowledge execution start, run trusted activity code, and send exactly correlated result/cancellation messages without ever becoming orchestration authority?*

The end-to-end worker protocol flow conforms strictly to the following lifecycle:

```text
Worker Process Starts
        │
        ▼ (Phase 1)
Worker Registration (POST /internal/v1/worker/register)
        │
        ▼ (Phase 2)
New WorkerSessionId Allocated & Stable Capabilities Advertised
        │
        ├─────────────────────────────────────────┐
        │ (Continuous Background Loop)            │ (Worker Poll Loop)
        ▼                                         ▼
Heartbeat / Liveness Maintenance          Long Poll for Work & Control Commands
(POST /internal/v1/worker/heartbeat)      (POST /internal/v1/worker/poll)
        │                                         │
        │                                         ▼
        │                                 Control Plane already owns Attempt:
        │                                 (Task RUNNING + Attempt CLAIMED)
        │                                         │
        │                                         ▼
        │                                 Worker receives Dispatch Payload
        │                                         │
        │                                         ▼
        │                                 Worker deduplicates by AttemptId
        │                                         │
        │                                         ▼
        │                                 Execution Start Acknowledgement
        │                                 (POST /internal/v1/worker/start)
        │                                         │
        │                                         ▼
        │                                 Attempt CLAIMED → RUNNING (Durable DB Commit)
        │                                         │
        │                                         ▼
        │                                 Execute Trusted Activity Handler
        │                                 (Async or Thread Pool)
        │                                         │
        │                                         ▼
        │                                 Result / Cancellation Callback
        │                                 (POST /internal/v1/worker/callback)
        │                                         │
        ▼                                         ▼
Worker Drain / Exit                       LLD-06 Authoritative Settlement
(accepting_new_work=False,
control polling continues
for active attempts)
```

### 1.2 Core Authority Model
NexusFlow enforces an unbridgeable authority boundary between the control plane and external workers:

1. **Control Plane Authority (Authoritative):**
   - Authoritative state machine transitions for `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`.
   - Task attempt allocation and attempt ordinal generation.
   - Authoritative attempt ownership binding: `ExecutionAttempt.worker_session_id`.
   - Evaluation of retry eligibility and backoff schedule (ADR-007, ADR-018).
   - Workflow failure and cancellation direction (ADR-006).
   - Liveness classification of worker sessions.
   - Acceptance, correlation, and fencing of worker callbacks.
2. **Worker Authority (Non-Authoritative / Execution Only):**
   - Execution of local, trusted activity code matching advertised `ActivityType` capabilities.
   - Management of local execution concurrency and thread pools.
   - Local process lifecycle (`STARTING`, `REGISTERING`, `SERVING`, `DRAINING`, `STOPPED`).
   - Observation of local execution outcomes (success return value, caught exception, cooperative cancel ack).
   - Bounded retries of HTTP callback deliveries.

**Explicit Rule:** The worker **never** decides task retry eligibility, never alters workflow direction, never creates database attempt records, and never claims unassigned work.

---

## 2. Worker Identity, Authentication & Security Context

### 2.1 Identity Invariants: `WorkerSessionId` vs. `WorkerId`
- **`WorkerSessionId` (Runtime Incarnation Authority):**
  - A cryptographically random UUID (UUIDv4) assigned by the control plane upon registration.
  - Identifies a single, continuous runtime process incarnation of a worker.
  - Authoritative target for task routing, attempt ownership, heartbeat tracking, start acknowledgement, and callback fencing.
  - A worker process restart **must always** result in a new `WorkerSessionId`.
  - **Security Invariant:** `WorkerSessionId` is an incarnation identifier; it is **NOT** a secret credential.
- **`WorkerId` (Descriptive Metadata Only):**
  - A human-readable identifier (e.g., `"worker-prod-az1-node03"`, `"payments-worker"`).
  - Purely descriptive; carries **zero** routing authority, zero ownership authority, and zero callback fencing authority.
  - Multiple `WorkerSession` instances across time or across nodes may legitimately share the same `WorkerId`.

### 2.2 Worker Domain Authentication (ADR-022)
All internal worker endpoints require a high-entropy shared secret or Bearer token transmitted via standard HTTP headers:
```http
Authorization: Bearer <worker-domain-secret>
```
- **Trust Domain Authentication:** Proves that the calling process belongs to the authorized worker trust domain.
- **Independence from Session:** The Bearer token proves *trust domain membership*; it does **not** uniquely identify an individual worker process incarnation. The `WorkerSessionId` identifies the *incarnation*.
- **Security Context:** Upon successful Bearer token verification, the control plane constructs an immutable in-process `SecurityContext`:
  ```python
  @dataclass(frozen=True, slots=True)
  class SecurityContext:
      principal: str
      principal_type: str  # "WORKER"
      authorized_domain: str
  ```
  Raw Bearer credentials are never logged, never exposed to business activity code, and never passed into domain models.

### 2.3 Explicit Security Limitation & Threat Model
Because the worker-domain credential authenticates trust-domain membership rather than an individual process instance:
- The combination of:
  ```text
  shared worker-domain credential + known WorkerSessionId + registry entry
  ```
  does **not** cryptographically prove that the caller is the exact same physical or OS process. Any caller inside the shared worker trust domain could technically present another session's ID if known.
- Therefore, session integrity in NexusFlow V1 relies strictly on:
  1. High-entropy, unguessable UUIDv4 `WorkerSessionId` generation.
  2. Mandatory TLS on all untrusted or cross-host network hops.
  3. Exact durable Attempt $\leftrightarrow$ `WorkerSessionId` fencing in PostgreSQL 16.
  4. **Strict prohibition against session resurrection:** Once a session expires, is superseded, or is lost, it cannot be resurrected or taken over.
- Do **NOT** invent a second secret, signed reconnect token, nonce exchange, or session credential.

### 2.4 Safe V1 Session Continuity Semantics
To maintain complete correctness without unprovable cryptographic claims:

#### Normal HTTP Request Continuity
Individual HTTP transport connections may disconnect and reconnect freely. A worker process may continue using its currently assigned `WorkerSessionId` across subsequent HTTP requests (heartbeat, poll, start ack, callback) if and only if:
1. That session currently exists in the control plane's in-process `WorkerRegistry`.
2. The session has not expired or been superseded (`live == True` or within active liveness threshold).
3. The worker-domain Bearer authentication succeeds.

This represents the continuation of an already-existing, live logical session over stateless HTTP. It does **not** re-register or recreate a session.

#### Expired or Lost Session
If a `WorkerSession`:
- Has expired (liveness timeout exceeded),
- Has been removed or superseded,
- Or cannot be proven to exist in the current in-process `WorkerRegistry`,

the caller **must not** resurrect it. The caller must register a **new** `WorkerSessionId`.

#### Control-Plane Restart Rule
The control-plane `WorkerRegistry` is strictly in-process and ephemeral. When the control plane restarts:
- The old `WorkerRegistry` is completely gone.
- An incoming worker process must **not** resurrect an arbitrary historical `WorkerSessionId` from client-supplied data.
- Preferred V1 behavior:
  ```text
  Control Plane Process Restarts
          │
          ▼
  Old Worker Registry is Empty
          │
          ▼
  Workers register NEW WorkerSessionIds (S2)
          │
          ▼
  Durable Attempts remain bound to their original old WorkerSessionIds (S1)
          │
          ▼
  LLD-06 Worker-Loss / Start-Deadline Recovery & LLD-07 Reconciliation resolve those Attempts
  ```
- **Prohibitions:**
  - Do **NOT** rewrite old Attempt ownership to the new session.
  - Do **NOT** migrate an Attempt between `WorkerSessionId`s.
  - Do **NOT** create replacement Attempts inside LLD-05.

#### Simplified V1 Registration Model
`WorkerRegistrationRequestDTO` does **not** support resurrecting or reconnecting historical sessions via client-supplied IDs. Registration **always** allocates and returns a brand-new `WorkerSessionId`. Existing live sessions simply continue using their established ID on ordinary authenticated operational endpoints without re-registering.

---

## 3. Ephemeral Worker Registry & Liveness Management

### 3.1 Worker Registry Data Model
The Worker Registry is an in-process, ephemeral, and rebuildable cache residing in the control-plane memory. It holds zero durable orchestration authority.

```python
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

from nexusflow.domain.values import (
    ActivityType,
    WorkerId,
    WorkerSessionId,
)


@dataclass(frozen=True, slots=True)
class WorkerSessionRecord:
    session_id: WorkerSessionId
    worker_id: WorkerId
    capabilities: frozenset[ActivityType]
    registered_at_monotonic: float
    last_heartbeat_monotonic: float
    last_heartbeat_utc: datetime
    live: bool
    accepting_new_work: bool
```

### 3.2 Registry Concurrency Model
- The control plane runs as a single asyncio process (ADR-020).
- The `WorkerRegistry` is protected via single-threaded asyncio execution and short async locks for atomic state updates.
- Readers receive deeply frozen, immutable `WorkerSessionSnapshot` views.
- **Lock Invariant:** The Worker Registry lock is **never** held across database I/O, network calls, or long CPU operations.

### 3.3 Control-Plane Time Authority & Monotonic Liveness
- **Time Authority:** Worker liveness is evaluated **exclusively** via control-plane observed time. Timestamps transmitted by workers are treated as diagnostic metadata and are ignored for liveness decisions.
- **Monotonic Clock:** Heartbeat expiration uses Python's `time.monotonic()` to guarantee immunity to NTP clock jumps and leap seconds:
  $$\text{live} \iff (\text{now\_monotonic} - \text{last\_heartbeat\_monotonic}) \le \text{worker\_liveness\_timeout}$$
  *Provisional V1 Operational Default:* `worker_liveness_timeout = 15.0s`, `heartbeat_interval = 5.0s`.

### 3.4 Session Liveness vs. Attempt Leases
- **Session-Level Heartbeat:** Heartbeats prove only that the `WorkerSession` process is alive and communicating.
- **NO Attempt Lease Renewal:** A worker heartbeat **never** extends, renews, or touches active `ExecutionAttempt` deadlines.
- Every `ExecutionAttempt` has its own durable `start_deadline_utc` and `execution_timeout_utc` managed independently in PostgreSQL.

### 3.5 Operational States: `live` vs. `accepting_new_work`
These two flags are decoupled:
1. `live == True, accepting_new_work == True`: Healthy, actively accepting new task ownership.
2. `live == True, accepting_new_work == False`: Healthy, but in graceful drain; executes existing owned attempts, but LLD-04 routing excludes it from new tasks. Control polling continues for active attempts.
3. `live == False`: Heartbeat timed out or network disconnected; session considered dead. Ephemeral delivery queues dropped.

---

## 4. Capability Advertisement & Local Activity Registry

### 4.1 Exact Capability Matching (ADR-009)
Capabilities are defined as an immutable set of exact strings:
$$\text{capabilities} = \text{frozenset}\{\text{ActivityType}_1, \text{ActivityType}_2, \dots\}$$
- **Exact Byte Identity:** Matching requires byte-for-byte exact equality (`"email.send"` $\neq$ `"Email.Send"`).
- **No Wildcards / Hierarchy:** No prefix, regex, or inheritance matching is permitted.
- **Session Stability:** A worker's capability set is fixed for the lifetime of the `WorkerSession`. Changing capabilities requires shutting down the worker and registering a new session.

### 4.2 Python Worker Activity Registry & Local Decorator API
The Python worker runtime provides a clean, trusted decorator API:

```python
import inspect
from typing import Callable, Any, Coroutine, Mapping
from nexusflow.domain.values import ActivityType, JsonValue

ActivityHandler = Callable[[Mapping[str, JsonValue]], Any | Coroutine[Any, Any, Any]]


class ActivityRegistry:
    """
    Local, trusted worker registry mapping ActivityType to local callables.
    Enforces uniqueness at startup and detects duplicate registrations.
    """

    def __init__(self) -> None:
        self._handlers: dict[ActivityType, ActivityHandler] = {}

    def register(self, name: str, handler: ActivityHandler) -> None:
        act_type = ActivityType(name)
        if act_type in self._handlers:
            raise ValueError(f"Duplicate activity registration for '{name}'.")
        if not callable(handler):
            raise TypeError(f"Handler for '{name}' must be callable.")
        self._handlers[act_type] = handler

    def get_handler(self, act_type: ActivityType) -> ActivityHandler | None:
        return self._handlers.get(act_type)

    def capabilities(self) -> frozenset[ActivityType]:
        return frozenset(self._handlers.keys())


# Global default activity registry for decorator syntax
_GLOBAL_REGISTRY = ActivityRegistry()


def activity(name: str) -> Callable[[ActivityHandler], ActivityHandler]:
    """Decorator for registering activity implementations in worker process."""

    def decorator(fn: ActivityHandler) -> ActivityHandler:
        _GLOBAL_REGISTRY.register(name, fn)
        return fn

    return decorator
```

### 4.3 Execution Boundary: Async & Sync Handlers (ADR-020)
- **Async Handlers:** Handlers defined as `async def` run directly on the worker's asyncio event loop.
- **Sync Handlers:** Standard `def` blocking handlers are dispatched to a bounded `concurrent.futures.ThreadPoolExecutor`.
- **Event Loop Protection:** Synchronous, CPU-heavy, or blocking I/O functions are **never** executed directly on the worker's main asyncio loop.

---

## 5. Work Assignment & Control Delivery: HTTP Pull Model

### 5.1 Preconditions for Work Delivery (LLD-04 Boundary)
Before any task assignment can be delivered over HTTP, LLD-04 guarantees that:
1. `TaskExecution` is durably `RUNNING`.
2. `ExecutionAttempt` is durably `CLAIMED` in PostgreSQL 16.
3. `ExecutionAttempt.worker_session_id` matches the assigned worker session.
4. `task_executions.stable_input` is locked and durably persisted.
5. `ExecutionAttempt.start_deadline_utc` is materialized.

**Core Invariant (Ownership-Before-Transport):** The control plane commits ownership in PostgreSQL *before* assignment delivery occurs. Worker drain or disconnect never rolls back committed ownership.

### 5.2 Pull Model & Long-Polling Mechanics
External workers connect to the control plane using an HTTP/JSON long-polling pull model (ADR-020):
- **Request:** The worker polls `/internal/v1/worker/poll`: *"Return work already durably owned by my `WorkerSessionId` or control commands for my active attempts."*
- **No Worker-Side Bidding/Claiming:** The worker **never** claims arbitrary `RUNNABLE` tasks. It only retrieves attempts already committed to its session by LLD-04.
- **Delivery Queues:** The control plane maintains an optional in-memory `asyncio.Queue` per active `WorkerSessionId` to unblock long-polls with sub-millisecond latency.

### 5.3 Poll Response Behavior Based on `accepting_new_work`
The control-plane long-poll handler evaluates session state when deciding what to return:

1. **`accepting_new_work == True` (Normal SERVING):**
   - May return:
     - Already-owned execution assignment (`status: "ASSIGNMENT"`).
     - Cancellation / control command for an active attempt (`status: "CANCEL_COMMAND"`).
     - Empty result on poll timeout (`status: "NO_WORK"`).

2. **`accepting_new_work == False` (Worker DRAINING):**
   - Must **NOT** return new execution assignments that would begin additional work.
   - May return:
     - Cancellation / control commands for attempts already owned by that session (`status: "CANCEL_COMMAND"`).
     - Empty result (`status: "NO_WORK"`).

#### Already-Committed Ownership vs. Drain Race
There is an inherent distributed race:
```text
LLD-04 commits Attempt A to Session S1 in PostgreSQL
        │
        ▼ (Race Window)
Worker reports accepting_new_work = False (Drain begins)
        │
        ▼
Worker long-poll arrives with accepting_new_work = False
```
- **Rule:** No new ownership is committed by LLD-04 after drain is observed.
- However, if ownership was **already committed** in PostgreSQL before `accepting_new_work=False` was registered, that Attempt remains authoritative.
- The transport may still deliver that already-committed Attempt (or if the worker cannot start it, the worker rejects or allows it to expire).
- Under no circumstances does worker drain silently roll back or delete the committed `CLAIMED` Attempt in PostgreSQL. If the worker cannot or will not start it, the Attempt is resolved via normal `start_deadline_utc` expiration or worker-loss settlement in LLD-06/LLD-07.

### 5.4 Durable Fallback & Lost Delivery Queues
The ephemeral delivery queue is an optimization, not an authoritative store. If a delivery queue drops a hint, or if the control plane restarts after an ownership commit:
- The poll handler falls back to querying PostgreSQL directly:
  ```sql
  SELECT attempt_id, task_execution_id, attempt_ordinal, start_deadline_utc
  FROM execution_attempts
  WHERE worker_session_id = :session_id
    AND state = 'CLAIMED'
  ORDER BY created_at_utc ASC
  LIMIT 1;
  ```
- This guarantees that no committed task is ever stranded by an in-memory queue loss.

### 5.5 Assignment Payload Schema
```json
{
  "attempt_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "attempt_ordinal": 1,
  "task_execution_id": "a3b2c1d0-1234-5678-9abc-def012345678",
  "workflow_execution_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "worker_session_id": "c9a0b1c2-3d4e-5f6a-7b8c-9d0e1f2a3b4c",
  "activity_type": "payment.charge",
  "stable_input": {
    "amount": 15000,
    "order_id": "ord_998877"
  },
  "start_deadline_utc": "2026-09-08T01:15:30Z"
}
```

### 5.6 Worker-Side Deduplication
Due to transient HTTP timeouts, a worker may receive the same `CLAIMED` attempt delivery twice.
- The worker runtime maintains an ephemeral `dict[AttemptId, LocalAttemptHandle]`.
- If an attempt with the same `AttemptId` is delivered while already running or starting locally, the worker ignores the duplicate delivery and does **not** launch a second concurrent execution.

---

## 6. Execution Start Acknowledgement Protocol

### 6.1 Purpose & Durable Transition
Once the worker receives an assignment and prepares its local execution context, it must formally acknowledge execution start before calling user activity code:
$$\text{AttemptState: CLAIMED} \xrightarrow[\text{Start Acknowledgement}]{\text{LLD-02 Commit}} \text{AttemptState: RUNNING}$$
- The parent `TaskExecution` remains in state `RUNNING`.
- The commit transition is performed via LLD-02's `commit_worker_execution_start`.

### 6.2 Start Ack Correlation & Fencing Rules
The control plane accepts a start acknowledgement if and only if:
1. The request carries valid worker-domain Bearer credentials.
2. The `AttemptId` exists in PostgreSQL.
3. The `ExecutionAttempt.worker_session_id` matches the caller's `WorkerSessionId`.
4. The attempt is currently in state `CLAIMED`.
5. The attempt's `start_deadline_utc >= now_utc`.
6. OCC revision checks pass.

### 6.3 Start Deadline Expiration Race
- If the worker's start acknowledgement races with the control plane's start deadline timeout:
  - Both transactions execute under row locks in PostgreSQL.
  - If `commit_internal_attempt_failure(START_DEADLINE_EXPIRED)` commits first, the attempt transitions to `FAILED`. The subsequent worker start acknowledgement fails OCC and is rejected with HTTP `409 Conflict` (`STALE_ATTEMPT`).
  - If `commit_worker_execution_start` commits first, the attempt becomes `RUNNING`. The subsequent timeout sweep observes state `RUNNING` and aborts without action.
- **Worker Reaction:** If start acknowledgement is rejected as stale or conflict, the worker **immediately aborts** execution of the local handler and releases the slot.

---

## 7. Execution Runtime & Worker-Side Lifecycle

### 7.1 Worker Process Operational States
The worker process transitions through distinct operational states:
$$\text{STARTING} \longrightarrow \text{REGISTERING} \longrightarrow \text{SERVING} \longrightarrow \text{DRAINING} \longrightarrow \text{STOPPED}$$

```text
       [ Process Starts ]
               │
               ▼
         [ STARTING ]  (Validate local ActivityRegistry, config, credentials)
               │
               ▼
        [ REGISTERING ] (POST /internal/v1/worker/register -> allocates new WorkerSessionId)
               │
               ▼
          [ SERVING ]  (accepting_new_work=True: heartbeats + work poll + execution)
               │
        (SIGTERM / Drain)
               │
               ▼
         [ DRAINING ]  (accepting_new_work=False: stop assignment admission,
               │        CONTINUE heartbeats, CONTINUE control/cancellation polling,
               │        drain active attempts for bounded grace period)
               │
               ▼
          [ STOPPED ]  (Stop control polling, terminate executors, exit process)
```

### 7.2 Graceful Worker Drain Semantics
When a worker process initiates shutdown (e.g., via `SIGTERM` or administrative drain):
1. **State Transition:** The worker moves from `SERVING` to `DRAINING`.
2. **Flag Update:** The worker sets `accepting_new_work = False` locally and in subsequent heartbeats.
3. **Stop Assignment Admission:** The worker ceases admitting new execution assignments from the poll channel.
4. **Continue Control Polling:** The worker **MUST CONTINUE** polling `/internal/v1/worker/poll` while active `ExecutionAttempt`s remain running locally. This ensures that cancellation and control commands for already-owned attempts are promptly received.
5. **Continue Heartbeats:** Heartbeats continue running to maintain process liveness so the control plane knows the worker is alive and draining.
6. **Drain Active Work:** Currently running activity handlers are permitted to execute up to a bounded `shutdown_grace_seconds` window.
7. **Grace Expiration:** If the shutdown grace window expires while synchronous threads are still executing:
   - The worker terminates the process cleanly.
   - The worker does **NOT** fabricate synthetic failure or success callbacks.
   - Authoritative resolution is left to control-plane worker-loss / execution-timeout detection in LLD-06.

### 7.3 Local Concurrency Management
- The worker maintains a semaphore limiting concurrent local activity handlers (e.g., `max_concurrency = 16`).
- When all execution slots are saturated, the worker pauses long-polling for work.
- Local concurrency limits are purely operational configuration and do not require control-plane capacity tokens.

### 7.4 Activity Error Boundary & Payload Validation
1. **Isolated Execution:** User activity code executes inside a `try/except` boundary. An unhandled exception or crash in one activity handler never terminates the worker process.
2. **Output Value Validation:** If an activity completes successfully, the worker validates that the return value conforms strictly to the JSON value space (`freeze_json` contract) before sending the success callback. If the output contains non-serializable objects (e.g., Python class instances, functions, NaN), it is mapped to a local execution failure.

---

## 8. Authoritative Result & Cancellation Callbacks

### 8.1 Result Callback Payloads
Upon completing activity execution, the worker posts a structured outcome envelope to `/internal/v1/worker/callback`:

#### Success Envelope:
```json
{
  "attempt_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "worker_session_id": "c9a0b1c2-3d4e-5f6a-7b8c-9d0e1f2a3b4c",
  "outcome_type": "SUCCESS",
  "output": {
    "confirmation_code": "TX-109283",
    "status": "APPROVED"
  }
}
```
*Note: `output: null` is fully supported as a valid JSON null return value.*

#### Failure Envelope:
```json
{
  "attempt_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
  "worker_session_id": "c9a0b1c2-3d4e-5f6a-7b8c-9d0e1f2a3b4c",
  "outcome_type": "FAILURE",
  "error": {
    "error_type": "PaymentGatewayTimeout",
    "message": "Gateway took longer than 5000ms to respond",
    "details": { "downstream_status": 504 }
  }
}
```
**Strict Invariant:** The failure payload **never** contains an authoritative `retryable: bool` field. The control plane alone classifies retryability during LLD-06 settlement.

### 8.2 Callback Correlation & Fencing Invariants
The control plane accepts a result callback **if and only if**:
$$\text{Attempt.worker\_session\_id} == \text{callback.worker\_session\_id} \land \text{Attempt.state} == \text{'RUNNING'}$$
- **Zombie Worker Rejection:** If a worker was considered lost, its attempt failed, and a retry was dispatched to another worker, any late callback from the zombie worker fails the state check (`Attempt.state != 'RUNNING'`) and is safely rejected.
- **Session Fencing:** A callback referencing an invalid, mismatched, or historical `WorkerSessionId` is rejected with `HTTP 403 Forbidden` (`WRONG_SESSION`).
- **Late Callback Arbitration (Preserve Late Callback OCC Race):** An ephemeral registry status of `live == False` does **NOT** automatically invalidate a properly correlated callback. Late callback delivery and worker-loss settlement race through durable PostgreSQL OCC:
  - If the callback commits first, the worker-loss sweep observes state `SUCCEEDED` / `FAILED` and aborts.
  - If worker-loss commits first, the attempt transitions out of `RUNNING` and the subsequent callback is safely rejected as stale.

### 8.3 Callback Delivery Retries
If the network drops while sending a result callback, the worker retries the HTTP request with bounded exponential backoff. Because control-plane settlement is idempotent under `(AttemptId, WorkerSessionId)`, duplicate result deliveries return `HTTP 200 OK` (`IDEMPOTENT_DUPLICATE`) without side effects. Retrying callbacks does **not** consume task execution retry budget.

### 8.4 Cancellation Protocol & Wire Correlation
- When a workflow is cancelled or failing, the control plane delivers a cancellation command to the assigned worker via the control poll channel.
- **Cancellation Targeting Authority:** The targeting authority is strictly `(AttemptId, WorkerSessionId)`.
- **Wire Payload:** `TaskCancellationPayloadDTO` contains:
  - `attempt_id: UUID`
  - `worker_session_id: UUID`
  - `task_execution_id: UUID`
  - `workflow_execution_id: UUID`
  - `reason: str`
- **Worker-Side Session Validation:** The worker validates that `cancellation.worker_session_id` strictly matches the `WorkerSessionId` stored in its local `AttemptHandle`. A wrong-session cancellation command is rejected locally and **must not** cancel unrelated local execution.
- **Cooperative Async Cancellation:** For async activities, the worker cancels the local `asyncio.Task` (`task.cancel()`).
- **Sync Thread Cancellation Limitation:** In standard Python, synchronous threads running in a `ThreadPoolExecutor` cannot be forcibly terminated without corrupting the interpreter state. Sync handlers must cooperatively check cancellation flags.
- **Cancellation Ack:** The worker posts a cancellation acknowledgement envelope (`CANCEL_ACK`) reporting its local observation:
  - `COOPERATIVELY_STOPPED`
  - `ALREADY_COMPLETED`
  - `NOT_FOUND`
- **Authority Boundary:** LLD-05 only delivers the control message and reports local observation. The worker **never** decides durable attempt cancellation; authoritative settlement is performed exclusively by LLD-06.

---

## 9. Concrete Internal Worker HTTP API Specification

All worker communication occurs over internal HTTP/JSON routes (distinct from public `/v1` endpoints):

### 9.1 Summary Route Table
| Method | Path | Request Body | Success Response | Error Codes | Description |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `POST` | `/internal/v1/worker/register` | `WorkerRegistrationRequestDTO` | `200 OK` (`WorkerRegistrationResponseDTO`) | `400`, `401`, `422` | Allocates new session & registers capabilities. |
| `POST` | `/internal/v1/worker/heartbeat` | `WorkerHeartbeatRequestDTO` | `200 OK` (`WorkerHeartbeatResponseDTO`) | `401`, `409` | Proves liveness; updates heartbeat timestamp and drain flag. |
| `POST` | `/internal/v1/worker/poll` | `WorkerPollRequestDTO` | `200 OK` (`WorkerPollResponseDTO`) | `401`, `409`, `422` | Pulls owned attempts (if accepting work) and control commands. |
| `POST` | `/internal/v1/worker/start` | `WorkerStartAckRequestDTO` | `200 OK` (`WorkerStartAckResponseDTO`) | `401`, `403`, `409` | Acknowledges start; transitions CLAIMED $\to$ RUNNING. |
| `POST` | `/internal/v1/worker/callback` | `WorkerCallbackRequestDTO` | `200 OK` (`WorkerCallbackResponseDTO`) | `401`, `403`, `409`, `422` | Reports success, failure, or cancellation ack. |

### 9.2 Request & Response DTOs (Pydantic v2 with `extra='forbid'`)
```python
from typing import Literal, Union, Mapping, Annotated
from pydantic import BaseModel, ConfigDict, Field
from uuid import UUID


class StrictWorkerDTO(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


# 1. Registration (Always creates a new WorkerSessionId in V1)
class WorkerRegistrationRequestDTO(StrictWorkerDTO):
    worker_id: str = Field(min_length=1, max_length=256)
    capabilities: list[str] = Field(min_length=1, max_length=1000)
    client_version: str = Field(default="1.0.0", max_length=64)


class WorkerRegistrationResponseDTO(StrictWorkerDTO):
    worker_session_id: UUID
    status: Literal["REGISTERED"]
    heartbeat_interval_seconds: float
    worker_liveness_timeout_seconds: float


# 2. Heartbeat
class WorkerHeartbeatRequestDTO(StrictWorkerDTO):
    worker_session_id: UUID
    accepting_new_work: bool = True


class WorkerHeartbeatResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED"]
    control_plane_draining: bool = False


# 3. Polling
class WorkerPollRequestDTO(StrictWorkerDTO):
    worker_session_id: UUID
    accepting_new_work: bool = True
    max_items: int = Field(default=1, ge=1, le=10)
    timeout_seconds: float = Field(default=20.0, ge=1.0, le=60.0)


class TaskAssignmentPayloadDTO(StrictWorkerDTO):
    attempt_id: UUID
    attempt_ordinal: int
    task_execution_id: UUID
    workflow_execution_id: UUID
    worker_session_id: UUID
    activity_type: str
    stable_input: dict[str, object]
    start_deadline_utc: str


class TaskCancellationPayloadDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID
    task_execution_id: UUID
    workflow_execution_id: UUID
    reason: str


class WorkerPollResponseDTO(StrictWorkerDTO):
    status: Literal["ASSIGNMENT", "CANCEL_COMMAND", "NO_WORK"]
    assignment: TaskAssignmentPayloadDTO | None = None
    cancellation: TaskCancellationPayloadDTO | None = None


# 4. Start Acknowledgement
class WorkerStartAckRequestDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID


class WorkerStartAckResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED", "IDEMPOTENT_ALREADY_RUNNING"]
    attempt_id: UUID
    attempt_state: Literal["RUNNING"]


# 5. Result Callbacks
class ActivitySuccessPayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["SUCCESS"]
    output: object  # Must be strictly JSON-compatible


class ActivityFailureDetailsDTO(StrictWorkerDTO):
    error_type: str = Field(max_length=256)
    message: str = Field(max_length=4096)
    details: dict[str, object] | None = None


class ActivityFailurePayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["FAILURE"]
    error: ActivityFailureDetailsDTO


class ActivityCancelAckPayloadDTO(StrictWorkerDTO):
    outcome_type: Literal["CANCEL_ACK"]
    observed_state: Literal["COOPERATIVELY_STOPPED", "ALREADY_COMPLETED", "NOT_FOUND"]


WorkerCallbackOutcomeDTO = Annotated[
    Union[ActivitySuccessPayloadDTO, ActivityFailurePayloadDTO, ActivityCancelAckPayloadDTO],
    Field(discriminator="outcome_type"),
]


class WorkerCallbackRequestDTO(StrictWorkerDTO):
    attempt_id: UUID
    worker_session_id: UUID
    payload: WorkerCallbackOutcomeDTO


class WorkerCallbackResponseDTO(StrictWorkerDTO):
    status: Literal["ACCEPTED", "IDEMPOTENT_DUPLICATE"]
    attempt_id: UUID
```

### 9.3 HTTP Status Code Mapping
| Status Code | Reason Code | Meaning | Client Action |
| :--- | :--- | :--- | :--- |
| `200 OK` | `ACCEPTED` / `IDEMPOTENT_*` | Successful mutation or safe duplicate replay. | Proceed normally. |
| `400 Bad Request` | `INVALID_PAYLOAD` | Malformed JSON or boundary constraint violated. | Do not retry without payload fix. |
| `401 Unauthorized`| `AUTH_FAILED` | Missing or invalid worker Bearer credential. | Fail fast; check configuration. |
| `403 Forbidden` | `WRONG_SESSION` | WorkerSession does not own targeted Attempt. | Abort attempt execution immediately. |
| `409 Conflict` | `STALE_SESSION` | WorkerSession expired or heartbeat timed out. | Re-register as a new session. |
| `409 Conflict` | `STALE_ATTEMPT` | Attempt already settled, timed out, or canceled. | Abort attempt execution immediately. |
| `422 Unprocessable`| `INVALID_OUTPUT` | Success payload violates JSON value space. | Map to activity failure callback. |
| `503 Service Unavail`| `DRAINING` | Control plane shutting down temporarily. | Bounded retry with exponential backoff. |

---

## 10. Sequence Diagrams

### 10.1 Worker Registration & Heartbeat Loop
```
Worker Process                     Control Plane Worker HTTP              Worker Registry (Memory)
      │                                       │                                       │
      │ 1. POST /register                     │                                       │
      │    (Bearer auth, capabilities)        │                                       │
      ├──────────────────────────────────────>│                                       │
      │                                       │ 2. Validate Bearer Token              │
      │                                       │ 3. Allocate NEW WorkerSessionId       │
      │                                       │ 4. Insert WorkerSessionRecord        │
      │                                       ├──────────────────────────────────────>│
      │                                       │ 5. Emit WorkerCapabilityAvailable     │
      │                                       │<──────────────────────────────────────┤
      │ 6. 200 OK (WorkerSessionId)           │                                       │
      │<──────────────────────────────────────┤                                       │
      │                                       │                                       │
      │ 7. POST /heartbeat                    │                                       │
      │    (WorkerSessionId, accepting=True)  │                                       │
      ├──────────────────────────────────────>│                                       │
      │                                       │ 8. Update last_heartbeat_monotonic    │
      │                                       ├──────────────────────────────────────>│
      │ 9. 200 OK (ACCEPTED)                  │                                       │
      │<──────────────────────────────────────┤                                       │
```

### 10.2 Assignment Delivery, Start Ack & Execution
```
Worker Process                     Worker Poll Route                   LLD-02 Database                   Activity Handler
      │                                   │                                  │                                   │
      │ 1. POST /poll (Long Poll)         │                                  │                                   │
      ├──────────────────────────────────>│                                  │                                   │
      │                                   │ (LLD-04 committed Attempt CLAIMED)│                                   │
      │                                   │ 2. Read CLAIMED Attempt          │                                   │
      │                                   ├─────────────────────────────────>│                                   │
      │                                   │<─────────────────────────────────┤                                   │
      │ 3. 200 OK (Assignment Payload)    │                                  │                                   │
      │<──────────────────────────────────┤                                  │                                   │
      │ 4. Check Local Dedup (New Attempt)│                                  │                                   │
      │ 5. POST /start                    │                                  │                                   │
      │    (AttemptId, SessionId)         │                                  │                                   │
      ├──────────────────────────────────>│                                  │                                   │
      │                                   │ 6. commit_worker_execution_start │                                   │
      │                                   │    (Attempt CLAIMED -> RUNNING)  │                                   │
      │                                   ├─────────────────────────────────>│                                   │
      │                                   │<─────────────────────────────────┤                                   │
      │ 7. 200 OK (ACCEPTED, state=RUNNING│                                  │                                   │
      │<──────────────────────────────────┤                                  │                                   │
      │ 8. Dispatch Activity Function     │                                  │                                   │
      ├─────────────────────────────────────────────────────────────────────────────────────────────────────────>│
      │                                   │                                  │ 9. Execute Business Logic         │
      │ 10. Handler Return Value (JSON)   │                                  │                                   │
      │<─────────────────────────────────────────────────────────────────────────────────────────────────────────┤
      │ 11. POST /callback (SUCCESS)      │                                  │                                   │
      ├──────────────────────────────────>│                                  │                                   │
      │                                   │ 12. commit_worker_task_success   │                                   │
      │                                   │     (LLD-06 Settlement)          │                                   │
      │                                   ├─────────────────────────────────>│                                   │
      │                                   │<─────────────────────────────────┤                                   │
      │ 13. 200 OK (ACCEPTED)             │                                  │                                   │
      │<──────────────────────────────────┤                                  │                                   │
```

### 10.3 Cooperative Cancellation Flow
```
Worker Process                     Worker Poll Route                   LLD-06 Cancellation                Activity Handler
      │                                   │                                    │                                  │
      │                                   │ (Workflow cancelling)              │                                  │
      │                                   │ 1. Enqueue Cancel Command          │                                  │
      │                                   │<───────────────────────────────────┤                                  │
      │ 2. POST /poll (Long Poll)         │                                    │                                  │
      │    (Session S1, active attempts)  │                                    │                                  │
      ├──────────────────────────────────>│                                    │                                  │
      │ 3. 200 OK (CANCEL_COMMAND, S1)    │                                    │                                  │
      │<──────────────────────────────────┤                                    │                                  │
      │ 4. Validate Session & Signal Task │                                    │                                  │
      ├──────────────────────────────────────────────────────────────────────────────────────────────────────────>│
      │                                   │                                    │ 5. Catch CancelledError / Stop   │
      │ 6. Ack Cancelled Locally          │                                    │                                  │
      │<──────────────────────────────────────────────────────────────────────────────────────────────────────────┤
      │ 7. POST /callback (CANCEL_ACK)    │                                    │                                  │
      ├──────────────────────────────────>│                                    │                                  │
      │                                   │ 8. commit_worker_cancellation_ack  │                                  │
      │                                   ├───────────────────────────────────>│                                  │
      │ 9. 200 OK (ACCEPTED)              │                                    │                                  │
      │<──────────────────────────────────┤                                    │                                  │
```

---

## 11. Reference Tables & Inventories

### 11.1 WorkerSession Lifecycle & Registry State Matrix
| Worker Event | Session Identity | Registry Status | Liveness | Durable Attempt Effect |
| :--- | :--- | :--- | :--- | :--- |
| Initial Process Start | None $\to$ Allocated UUIDv4 | `ACTIVE` | `True` | None (Eligible for routing). |
| Valid Heartbeat | Existing `WorkerSessionId` | `ACTIVE` | `True` | None (Maintains liveness). |
| Heartbeat Timeout | Existing `WorkerSessionId` | `EXPIRED` | `False` | Triggers LLD-06 worker-loss settlement. |
| Graceful Drain Start | Existing `WorkerSessionId` | `DRAINING` | `True` | Excluded from new routing; control polling continues. |
| Process Crash / Exit | Existing `WorkerSessionId` | Drops from memory | `False` | Start deadline / worker-loss timeout settles attempts. |
| Process Restart | **New UUIDv4 Required** | `ACTIVE` | `True` | Old session attempts cannot be claimed or started by new session. |

### 11.2 Callback Correlation & Fencing Matrix
| Bearer Auth Valid? | Attempt Exists in DB? | `Attempt.worker_session_id` Matches? | Current Attempt State | Control Plane Result | HTTP Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **No** | Irrelevant | Irrelevant | Irrelevant | Reject immediately. | `401 Unauthorized` |
| Yes | **No** | Irrelevant | Irrelevant | Reject as unknown. | `404 Not Found` |
| Yes | Yes | **No (Wrong Session)** | Irrelevant | Reject unauthorized session. | `403 Forbidden` |
| Yes | Yes | Yes | `RUNNING` | **Accept & Commit Settlement.**| `200 OK` (`ACCEPTED`) |
| Yes | Yes | Yes | `SUCCEEDED` (Same Output) | Idempotent duplicate. | `200 OK` (`IDEMPOTENT_DUPLICATE`) |
| Yes | Yes | Yes | `SUCCEEDED` (Different Output)| Reject conflicting duplicate. | `409 Conflict` |
| Yes | Yes | Yes | `FAILED` / `CANCELLED` | Reject stale zombie callback. | `409 Conflict` (`STALE_ATTEMPT`) |

### 11.3 Durable vs. Ephemeral Concept Taxonomy
| Concept | Storage Location | Durable Authority? | Reconstructible on Restart? |
| :--- | :--- | :--- | :--- |
| `WorkerSessionId` | PostgreSQL (in attempts) & Memory | **Yes (in committed attempts)** | Rebuilt via worker re-registration. |
| Worker Registry | Control-Plane Memory | **No** | Rebuilt dynamically as workers register. |
| Heartbeat Timestamp | Control-Plane Memory | **No** | Ephemeral monotonic tracking. |
| `accepting_new_work` | Control-Plane Memory | **No** | Tracked per session in memory. |
| Attempt Ownership | PostgreSQL (`execution_attempts`) | **YES** | Sole source of truth. |
| Delivery Queue | Control-Plane Memory (`asyncio.Queue`)| **No** | Recovered via PostgreSQL `CLAIMED` query. |
| Local `AttemptHandle`| Worker Process Memory | **No** | Ephemeral execution tracking. |
| Activity Result | Worker Process Memory | **No** | Retried over HTTP until acknowledged. |

---

## 12. Concrete Package & Module Layout

### 12.1 Control-Plane Architecture (`src/nexusflow/`)
```text
src/nexusflow/
    domain/
        workers/
            session.py            # WorkerSessionId, WorkerSessionRecord, snapshots
            registry.py           # WorkerRegistryView protocol & capability matching
            protocol_results.py   # Domain outcome types (RegistrationResult, etc.)

    application/
        workers/
            registration.py       # RegisterWorkerUseCase
            heartbeat.py          # ProcessHeartbeatUseCase
            polling.py            # PollWorkAssignmentUseCase & delivery fallback
            start_ack.py          # AcknowledgeExecutionStartUseCase
            callbacks.py          # ProcessWorkerCallbackUseCase (routes to LLD-06)
            ports.py              # WorkerAssignmentReadPort, WorkerRegistryPort

    interfaces/
        worker_http/
            routes.py             # FastAPI / ASGI route handlers for /internal/v1/worker/*
            schemas.py            # Pydantic v2 boundary DTOs (extra='forbid')
            auth.py               # Bearer token verification & SecurityContext builder

    runtime/
        workers/
            registry.py           # In-memory thread-safe WorkerRegistry implementation
            delivery.py           # Ephemeral per-session asyncio delivery queues
```

### 12.2 Python Worker Runtime Architecture (`worker/nexusflow_worker/`)
```text
worker/
    nexusflow_worker/
        __init__.py               # Public exports (@activity, WorkerRuntime)
        activity.py               # ActivityRegistry & @activity decorator
        client.py                 # Hardened HTTP client for /internal/v1/worker/*
        runtime.py                # WorkerRuntime process supervisor & event loop
        execution.py              # LocalAttemptHandle & async/sync handler executor
        callbacks.py              # Bounded callback retry dispatcher
        config.py                 # WorkerConfig (control_plane_url, token, concurrency)
```

---

## 13. Comprehensive Testing Strategy

### 13.1 Session Lifecycle & Registration Tests (`tests/unit/workers/test_session_lifecycle.py`)
1. **New WorkerSession Registration:**
   - Verify `POST /internal/v1/worker/register` returns a new, valid `WorkerSessionId` UUIDv4 and registers capabilities.
2. **Same WorkerId Across Different Sessions:**
   - Register two workers with the same descriptive `worker_id = "payments-worker"`.
   - Assert both registrations succeed and receive distinct `WorkerSessionId`s (`S1 != S2`).
3. **Worker Process Restart Creates New WorkerSessionId:**
   - Worker process restarts and calls registration.
   - Assert new session `S2` is returned. Old session `S1` attempts cannot be started or acknowledged by `S2`.
4. **Historical/Expired Session Resurrection Rejected:**
   - Worker process attempts to call endpoints using an expired or unrecorded `WorkerSessionId`.
   - Assert rejected with `409 Conflict` (`STALE_SESSION`). Historical sessions cannot be resurrected via client claims.
5. **Shared Credential Does Not Imply Same-Process Proof (Arbitrary Session Takeover Test):**
   - Worker A owns Session S1. Worker B possesses the valid shared worker-domain Bearer credential.
   - Worker B attempts to register or resume claiming S1 as its own.
   - Assert: Registration creates a new session ID regardless, and operational requests from B cannot take over or resurrect historical/lost sessions.
6. **Control-Plane Restart Test:**
   - Worker session S1 exists. Attempt A is durably `CLAIMED` by S1 in PostgreSQL.
   - Control-plane process restarts; ephemeral Worker Registry is empty.
   - Worker reconnects and registers; it receives a new session S2.
   - Assert: Attempt A remains owned by S1 in PostgreSQL; A is **not** rewritten to S2; no duplicate Attempt is created by LLD-05; LLD-06/LLD-07 recovery settles Attempt A via start-deadline or worker-loss timeout.

### 13.2 Heartbeat & Liveness Tests (`tests/unit/workers/test_heartbeat.py`)
7. **Heartbeat Updates Liveness Only:**
   - Send valid heartbeat for session S1.
   - Assert `last_heartbeat_monotonic` is updated in memory.
8. **Heartbeat Does Not Touch Attempt Deadlines:**
   - Create active Attempt A with `start_deadline_utc` and `execution_timeout_utc`.
   - Send multiple valid heartbeats for the owning session.
   - Assert PostgreSQL `execution_attempts` row is completely untouched (deadlines and revision unchanged).
9. **Expired Session Heartbeat Does Not Resurrect Session:**
   - Allow session S1 liveness timeout to expire.
   - Heartbeat arrives for S1.
   - Assert rejected with `409 Conflict` (`STALE_SESSION`); session is not resurrected.

### 13.3 Routing, Assignment & Fencing Tests (`tests/unit/workers/test_assignment.py`)
10. **Capability Exact Matching:**
    - Worker advertises `["payment.charge"]`.
    - Routing matches `"payment.charge"` exactly; rejects `"Payment.Charge"`, `"payment.*"`, and `"payment.charge.v2"`.
11. **Durable CLAIMED Assignment Fallback:**
    - Commit attempt ownership to session S1 in PostgreSQL; deliberately drop or omit the in-memory delivery queue.
    - Worker S1 polls `/internal/v1/worker/poll`.
    - Assert attempt is retrieved via PostgreSQL fallback query and returned to worker.
12. **Duplicate Assignment Delivery Deduplicated:**
    - Transport delivers the same assignment payload twice to the worker runtime.
    - Assert worker runtime deduplicates by `AttemptId` and spawns only one execution task.
13. **Correct Start Acknowledgement:**
    - Worker sends `POST /internal/v1/worker/start` with matching `(attempt_id, worker_session_id)`.
    - Assert Attempt transitions durably from `CLAIMED` to `RUNNING` in PostgreSQL.
14. **Duplicate Start ACK:**
    - Worker sends the same start ACK twice.
    - Assert second request returns `200 OK` (`IDEMPOTENT_ALREADY_RUNNING`).
15. **Wrong-Session Start ACK:**
    - Worker S2 sends start ACK for Attempt owned by S1.
    - Assert rejected with `403 Forbidden` (`WRONG_SESSION`).
16. **Start Deadline Race:**
    - Synchronize start ACK commit and start-deadline failure sweep on the same attempt.
    - Assert PostgreSQL OCC ensures exactly one commits. If timeout commits first, start ACK receives `409 Conflict` (`STALE_ATTEMPT`).

### 13.4 Callback & Settlement Boundary Tests (`tests/unit/workers/test_callbacks.py`)
17. **Success Callback:**
    - Worker sends valid success callback.
    - Assert Attempt completes and transitions to settlement in LLD-06.
18. **JSON Null Output Acceptance:**
    - Activity returns `None` / `null`. Worker posts `"output": null`.
    - Assert accepted with `200 OK` and committed as valid JSON `null`.
19. **Duplicate Callback:**
    - Worker retries exact same success callback.
    - Assert second request returns `200 OK` (`IDEMPOTENT_DUPLICATE`) with zero state mutation.
20. **Conflicting Callback:**
    - Worker sends success callback with different output for already-settled attempt.
    - Assert rejected with `409 Conflict`.
21. **Late Callback vs. Worker-Loss OCC Race:**
    - Ephemeral registry marks session as `live == False`.
    - Worker submits valid success callback before worker-loss sweep commits.
    - Assert callback is **not** pre-rejected based on ephemeral memory; callback wins OCC and commits success in PostgreSQL.
22. **Callback Response Loss Retry:**
    - Worker executes handler, sends callback, but HTTP connection drops before response is received.
    - Worker retries callback; assert control plane safely returns idempotent acceptance.

### 13.5 Worker Drain & Control Channel Tests (`tests/integration/workers/test_drain.py`)
23. **Worker Drain Remains Live:**
    - Worker enters `DRAINING` with `accepting_new_work = False`.
    - Assert worker continues sending heartbeats and remains `live == True` in registry.
24. **Worker Drain Receives No New Ownership:**
    - Worker is `DRAINING`. Scheduler evaluates runnable tasks.
    - Assert LLD-04 routing excludes the draining worker from new task assignments.
25. **Worker Drain Still Receives Cancellation/Control Commands:**
    - Worker has active Attempt A and is `DRAINING` (`accepting_new_work = False`).
    - Control plane enqueues cancellation command for Attempt A.
    - Assert worker's poll request receives `CANCEL_COMMAND`; worker cooperatively stops handler and posts `CANCEL_ACK`.
26. **Already-Committed Ownership During Drain Remains Authoritative:**
    - LLD-04 commits Attempt A to session S1 in PostgreSQL.
    - Before assignment delivery, worker S1 begins draining (`accepting_new_work = False`).
    - Assert Attempt A ownership in PostgreSQL is **not** rolled back; Attempt A remains `CLAIMED` by S1.
27. **Wrong-Session Cancellation Rejected:**
    - Worker executes Attempt A under Session S1.
    - Cancellation command arrives carrying Session S2.
    - Assert worker validates session mismatch, rejects cancellation command, and does not cancel Attempt A.
28. **Sync Cancellation Limitation:**
    - Sync handler in thread pool executes blocking I/O without checking cancel flag.
    - Shutdown grace expires.
    - Assert worker process exits cleanly without thread corruption; control plane resolves attempt via worker-loss/timeout settlement.

---

## 14. Final Design Validation Checklist

- [x] **WorkerSessionId Authoritative Runtime Incarnation**: Verified across all models, endpoints, and fencing rules; `WorkerId` is descriptive only.
- [x] **Token Authentication vs. Session Identity**: Bearer token authenticates trust domain; `WorkerSessionId` identifies incarnation.
- [x] **No Second Credential / Secret**: Session continuity uses authenticated caller domain and registry validation without extra tokens.
- [x] **Unguessable Session IDs**: High-entropy UUIDv4 protects against session tampering within the trust domain.
- [x] **No Historical Session Resurrection**: Registration always allocates a new `WorkerSessionId`; dead sessions cannot be resurrected.
- [x] **Control Plane Restart Safety**: Ephemeral registry loss leads to new worker sessions; durable attempts remain bound to original sessions and are settled via LLD-06/LLD-07.
- [x] **Split Assignment Admission vs Control Polling**: Draining workers halt new task admission (`accepting_new_work=False`) but continue control polling for active attempts.
- [x] **Already-Committed Ownership Preserved**: Drain does not roll back committed `CLAIMED` attempts in PostgreSQL.
- [x] **Cancellation Wire Correlation**: `TaskCancellationPayloadDTO` carries `(AttemptId, WorkerSessionId)`; worker validates session before acting.
- [x] **Worker Never Decides Retryability**: Worker reports local error metadata only; LLD-06 alone evaluates retry eligibility.
- [x] **JSON Null Supported**: Explicitly handles JSON `null` outputs without missing-output ambiguity.
- [x] **Sync Work Off Event Loop**: Sync activity handlers run in bounded thread pools; event loop remains unblocked.
- [x] **HTTP JSON Pull Model Only**: Strictly pull/long-poll; prohibits gRPC, WebSockets, or distributed brokers.
- [x] **Late Callback OCC Arbitration**: Ephemeral `live=False` does not block late callbacks; PostgreSQL OCC arbitrates settlement races.

---

### Classification

**LLD-05 — Architecture-Ready / Approved as LLD-06 Input**
