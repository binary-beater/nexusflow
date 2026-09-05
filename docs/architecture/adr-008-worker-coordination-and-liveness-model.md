# ADR-008 — Worker Coordination & Liveness Model

*   **Status**: Approved
*   **Last Updated**: 2026-09-05
*   **Deciders**: Project Owner
*   **Domain**: Execution (Worker Coordination, Liveness & Execution Ownership)
*   **Criticality**: Core
*   **Relationships**:
    *   **Depends On**: [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
    *   **Enables**: [ADR-009: Task Routing Strategy](00-architecture-decision-register.md#L204), [ADR-017: Graceful Shutdown Strategy](00-architecture-decision-register.md#L214)
    *   **Related To**: [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md), [ADR-006: Workflow Execution State Machine](adr-006-workflow-execution-state-machine.md), [ADR-011: State Persistence Strategy](00-architecture-decision-register.md#L210), [ADR-012: Recovery Strategy](00-architecture-decision-register.md#L211), [ADR-013: Consistency & Concurrency Strategy](00-architecture-decision-register.md#L212), [ADR-014: History & Audit Model](00-architecture-decision-register.md#L213), [ADR-016: Observability Architecture](00-architecture-decision-register.md#L221), [ADR-020: Technology Selection Strategy](00-architecture-decision-register.md#L223), [ADR-022: Security Model (Version 1)](00-architecture-decision-register.md#L225)

---

## 1. Purpose
This document defines the authoritative worker coordination and liveness architecture for NexusFlow. It establishes how remote worker processes establish authoritative execution ownership of an `ExecutionAttempt`, how execution start is authoritatively observed, how worker runtime presence is monitored, how worker disappearance is detected and translated into attempt failures, how cancellation intent is communicated, how uncooperative attempts are logically revoked, how results are correlated, and how crash recovery reconciles active attempts without creating cascading false worker losses.

---

## 2. Context
NexusFlow establishes the top-level execution lifecycle via [ADR-006](adr-006-workflow-execution-state-machine.md) and governs task scheduling and dependency satisfaction via [ADR-005](adr-005-workflow-task-scheduling-and-dispatch-architecture.md). [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) defines the two-tier domain separation between logical `TaskExecution` and physical `ExecutionAttempt` records, establishing that:
1. An `ExecutionAttempt` is created **strictly when** worker execution ownership is authoritatively established, progressing the attempt into `CLAIMED`.
2. An attempt transitions from `CLAIMED` to `RUNNING` when worker execution-start is authoritatively observed.
3. If worker coordination determines that an assigned worker or attempt is lost or unusable, the attempt transitions to `FAILED` with a worker-loss-related cause.
4. Active attempts receive best-effort cancellation intent during workflow `FAILING` or `CANCELLING`, but logical settlement must not wait indefinitely for remote worker acknowledgement.
5. Stale callbacks from superseded or revoked attempts must be strictly fenced from mutating active task state.

ADR-008 sits directly beneath ADR-007, supplying the concrete worker coordination mechanisms that fulfill these lifecycle contracts while remaining independent of specific transport protocols, database technologies, or network framing.

---

## 3. Problem Statement
How should NexusFlow coordinate external worker processes, track runtime liveness, grant authoritative execution ownership, deliver cancellation signals, and fence stale results across network partitions and orchestrator restarts, without leaking transport details into domain logic or requiring fragile distributed consensus in V1?

---

## 4. Requirements Covered
*   **Capability 5**: Worker Coordination & Liveness Management.
*   **System Invariant (KT-8)**: "At most one authoritative active ExecutionAttempt may exist for a TaskExecution."
*   **System Invariant (KT-8)**: "A task execution attempt must be bound to exactly one worker runtime incarnation."
*   **System Invariant (KT-8)**: "Logical settlement is authoritative; physical remote worker shutdown may lag."
*   **System Invariant (KT-8)**: "Stale or superseded attempt results must never mutate active task state."
*   **Core Principle**: Correctness Over Performance.
*   **Core Principle**: Explicit Behaviour Over Implicit Behaviour.
*   **Core Principle**: Technology Independence Before Implementation.
*   **Core Principle**: Recovery as a First-Class Capability.
*   **Core Principle**: Clear Ownership and Separation of Responsibilities.

---

## 5. Constraints
*   **Transport Independence**: Coordination abstractions must support both push-based and pull-based worker architectures without embedding queue-, HTTP-, or RPC-specific constructs.
*   **ADR-007 Contract Preservation**: Must preserve ADR-007's 5-state attempt model (`CLAIMED`, `RUNNING`, `SUCCEEDED`, `FAILED`, `CANCELLED`) without introducing new attempt lifecycle states.
*   **Global Fail-Fast Compatibility**: Must support [ADR-006](adr-006-workflow-execution-state-machine.md) by guaranteeing that uncooperative workers cannot indefinitely block workflow drain convergence during `FAILING` or `CANCELLING`.
*   **Single-Developer V1 Scope**: Avoid complex distributed consensus algorithms, dynamic leader election, multi-master cluster fencing, or distributed lease-renewal meshes.
*   **Untrusted Networks & Unsynchronized Clocks**: Worker wall-clock timestamps cannot be assumed synchronized with the orchestrator; the control plane must remain authoritative for time-based coordination decisions.

---

## 6. Goals
*   Define a runtime incarnation identity model centered on `WorkerSessionId` to isolate worker process restarts from stale messages.
*   Establish an ephemeral, reconstructible Worker Registry that tracks active worker sessions without becoming a source of stale persistent state.
*   Ensure that the `ExecutionAttempt ↔ WorkerSessionId` ownership association is durably recoverable under [ADR-011](00-architecture-decision-register.md#L210).
*   Implement worker-session-level liveness monitoring via periodic liveness assertions, rejecting complex per-attempt renewable leases in V1.
*   Define an abstract three-phase execution ownership handshake (`Candidate/Offer` $\to$ `Ownership Commit` $\to$ `Execution Start`) compatible with both push and pull paradigms.
*   Enforce a finite Execution-Start Deadline on `CLAIMED` attempts to prevent unstarted tasks from remaining unresolved indefinitely.
*   Enforce a finite Cancellation-Resolution Deadline on active attempts to guarantee workflow drain convergence.
*   Define crash recovery reconciliation rules that prevent orchestrator restarts from falsely declaring surviving workers lost.

---

## 7. Non-Goals
*   Selecting physical transport frameworks (e.g., gRPC, HTTP/REST, WebSocket, Redis, RabbitMQ, Kafka) (deferred to [ADR-020](00-architecture-decision-register.md#L223)).
*   Designing task routing algorithms, worker pool selection policies, or queue assignment rules (deferred to [ADR-009](00-architecture-decision-register.md#L204)).
*   Designing physical database tables, column types, or SQL schemas for worker associations (deferred to [ADR-011](00-architecture-decision-register.md#L210)).
*   Designing recovery scanning loops, boot-up reconciliation sweeps, or engine restart state machines (deferred to [ADR-012](00-architecture-decision-register.md#L211)).
*   Selecting atomic concurrency control mechanisms (e.g., CAS, row locking, transaction isolation) (deferred to [ADR-013](00-architecture-decision-register.md#L212)).
*   Designing history and audit event schemas (deferred to [ADR-014](00-architecture-decision-register.md#L213)).
*   Designing OpenTelemetry metrics, Prometheus counters, or dashboard visualizers (deferred to [ADR-016](00-architecture-decision-register.md#L221)).
*   Designing worker process shutdown hooks or SIGTERM/SIGKILL handlers (deferred to [ADR-017](00-architecture-decision-register.md#L214)).
*   Selecting worker cryptographic authentication, mTLS, or RBAC token schemas (deferred to [ADR-022](00-architecture-decision-register.md#L225)).
*   Guaranteeing exactly-once physical task execution across arbitrary distributed network partitions.

---

## 8. Candidate Solutions

### Alternative A: Stateless Workers / No Registry
Workers do not register or send liveness signals; they simply pull tasks and submit results anonymously.
*   *Mechanism*: Tasks are dispatched into a queue; worker identity is absent or ephemeral per message.
*   *Pros*: Minimal control-plane state.
*   *Cons*: Impossible to detect worker crashes before full task timeout; cannot deliver targeted cancellation signals; cannot determine whether an assigned worker is still alive; breaks crash recovery reconciliation.

### Alternative B: Ephemeral Worker Registry + Worker-Session Liveness (Selected Architecture)
Workers register a runtime incarnation identity (`WorkerSessionId`) in an ephemeral registry, emit periodic session-level liveness assertions, and bind to attempts via an abstract three-phase handshake.
*   *Mechanism*: Liveness is evaluated at the worker session level against control-plane time authority. Finite deadlines bound pre-execution (`CLAIMED`) and cancellation resolution. Attempt-to-session ownership is durably persisted.
*   *Pros*: Eliminates stale worker incarnation confusion; provides robust crash detection without high-frequency per-attempt lease traffic; supports both push and pull; guarantees workflow convergence via finite deadlines; clean V1 implementation.
*   *Cons*: Does not detect internal thread hangs in tasks that lack configured execution timeouts while the worker process remains live.

### Alternative C: Renewable Per-Attempt Leases
Each running attempt requires the worker to periodically renew a time-bounded lease with the orchestrator.
*   *Mechanism*: If an attempt lease is not renewed before its deadline, the orchestrator revokes authority and retries.
*   *Pros*: Directly binds liveness to individual task progress.
*   *Cons*: Creates substantial coordination traffic scaling with concurrent tasks; transient orchestrator network jitter risks expiring leases across hundreds of healthy tasks simultaneously; high protocol complexity.

### Alternative D: Long-Lived Transport Connection as Authority
Liveness and attempt ownership exist strictly for the duration of a continuous TCP/WebSocket connection.
*   *Mechanism*: Socket disconnect instantly triggers worker-loss failure.
*   *Pros*: Instantaneous disconnect detection.
*   *Cons*: Highly brittle under transient network blips; tightly couples orchestration logic to stateful socket runtimes; prevents graceful orchestrator restarts without aborting all in-flight work.

---

## 9. Detailed Evaluation

| Evaluation Criteria | Alt A: Stateless / No Registry | Alt B: Ephemeral Registry + Session Liveness (Selected) | Alt C: Per-Attempt Renewable Leases | Alt D: Connection-Bound Authority |
| :--- | :--- | :--- | :--- | :--- |
| **Transport Independence** | High | **High (Push, pull, queue, RPC)** | Moderate | Low (Requires persistent socket) |
| **Partition & Jitter Resilience** | Low | **High (Configurable expiry & grace)** | Low (Transient jitter aborts tasks) | Very Low (Disconnect = abort) |
| **Coordination Overhead** | Very Low | **Low (1 heartbeat per worker process)** | High ($N$ concurrent tasks $\times$ renewal) | Moderate |
| **Cancellation Convergence**| Poor | **High (Finite revocation deadline)** | Moderate | Moderate |
| **Recovery Compatibility** | Poor | **High (Surviving session reassertion)**| Complex | Poor |
| **V1 Implementation Fit** | Poor (Brittle) | **Optimal (Clean, defensible, scalable)** | Poor (Over-engineered) | Poor (Transport coupled) |

---

## 10. Decision

NexusFlow adopts **Alternative B: Ephemeral Worker Registry with Worker-Session Liveness, Three-Phase Ownership Handshake, and Finite Coordination Deadlines**.

### 10.1 Authoritative Worker Identity Model
1. **`WorkerSessionId` (Authoritative Runtime Incarnation Identity)**:
   - A sufficiently unique identifier generated by the worker runtime process upon startup.
   - Represents exactly one continuous execution of the worker process.
   - Reused across reconnects by the *same surviving* process.
   - Regenerated as a *new* identifier if the worker process restarts.
   - All correctness-critical coordination (claims, liveness, cancellation, results) binds strictly to `WorkerSessionId`.
2. **`WorkerId` (Optional Descriptive / Logical Name)**:
   - An optional, descriptive identifier (e.g., `worker-pool-us-east-42`).
   - Used solely for operational logging, human inspection, and routing hints.
   - Carries **zero correctness authority**; cannot authorize claims, heartbeats, or result submissions.

### 10.2 Ephemeral / Reconstructible Worker Registry
- The control plane maintains an **ephemeral, reconstructible Worker Registry** containing active worker sessions, their advertised capabilities, current liveness observations, and new-work eligibility.
- The registry is operational coordination state and does **not** act as durable correctness authority. Its physical representation is deferred.
- The registry is reconstructed dynamically: surviving worker sessions reassert their presence via registration and liveness signals after control-plane restarts.
- **Durable Association Invariant**: While the registry catalog is ephemeral, the authoritative assignment between an active `ExecutionAttempt` and its `WorkerSessionId` is **durable** and must be recoverable under [ADR-011](00-architecture-decision-register.md#L210).

### 10.3 Capabilities & Worker Eligibility
- **Activity Type Capabilities**: Each worker session advertises its supported Activity Types. Capabilities remain stable for the lifetime of that session in V1. ADR-008 exposes this metadata; [ADR-009](00-architecture-decision-register.md#L204) consumes it for routing.
- **Liveness vs. Eligibility**:
  - `live`: The worker session is currently observed as present according to control-plane liveness policy.
  - `accepting_new_work`: A boolean coordination flag indicating whether the session is eligible to receive new task execution ownership.
  - A worker may be `live = true` and `accepting_new_work = false` during graceful draining/shutdown ([ADR-017](00-architecture-decision-register.md#L214)).
- **Capacity**: ADR-008 defers complex concurrency caps, slot quotas, and active attempt counters to ADR-009.

### 10.4 Worker-Session Liveness Model
- **Session-Level Heartbeat**: Workers periodically emit a lightweight liveness assertion containing `WorkerSessionId`. Heartbeats do not require enumerating active attempt IDs.
- **Rejection of Per-Attempt Leases and Progress Heartbeats**: Per-attempt renewable leases and progress heartbeats are explicitly rejected for V1 to prevent protocol bloat and fragile mass-expiry cascades.
- **Time Authority**: The control plane is authoritative for all coordination timing decisions. Worker wall-clock timestamps carry zero authority. monotonic elapsed-time evaluation is preferred within an orchestrator process lifetime; deadlines surviving restart are persisted as durable semantic representations ([ADR-011](00-architecture-decision-register.md#L210)).
- **Liveness Expiry**: A worker session is marked lost only after exceeding a configurable liveness timeout. The policy must tolerate transient communication jitter; a single missed observation must never be equated with worker death.

### 10.5 Abstract Three-Phase Execution Ownership Handshake
ADR-008 standardizes an abstract handshake supporting both push and pull transport models:

```
┌─────────────────────────────────────────────────────────────────────────┐
│ Phase 1: Candidate / Offer                                              │
│ - TaskExecution is RUNNABLE (ADR-005).                                  │
│ - Candidate worker session identified (orchestrator offer or pull poll).│
│ - No ExecutionAttempt exists; no worker holds execution ownership.      │
└────────────────────────────────────┬────────────────────────────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Phase 2: Ownership Commit (Atomic Boundary)                             │
│ - Exactly one WorkerSessionId is granted authoritative ownership.       │
│ - ExecutionAttempt created in CLAIMED state (ADR-007).                  │
│ - TaskExecution transitions to RUNNING (ADR-007).                       │
└────────────────────────────────────┬────────────────────────────────────┘
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│ Phase 3: Execution Start                                                │
│ - Worker begins task execution and emits execution-start signal.        │
│ - Control plane authoritatively observes start.                         │
│ - ExecutionAttempt transitions from CLAIMED to RUNNING (ADR-007).       │
└─────────────────────────────────────────────────────────────────────────┘
```

- **Pre-Ownership Failure**: Any failure prior to Ownership Commit (worker rejection, transport drop, routing miss) leaves the task in `RUNNABLE`. Zero attempts are created, and zero retry budget is consumed.
- **Ownership Exclusivity**: Exactly one `WorkerSessionId` may hold authoritative ownership of an attempt. Atomic single-winner enforcement is owned by [ADR-013](00-architecture-decision-register.md#L212).

### 10.6 Coordination Deadlines
1. **Execution-Start Deadline (`CLAIMED` State)**:
   - A finite semantic deadline bounds the `CLAIMED` state.
   - If execution-start is not authoritatively observed before this deadline elapses (and no other terminal outcome commits), the attempt is authoritatively resolved as `FAILED` with a start-failure/timeout-related cause.
   - Protects the system from attempts remaining stuck in `CLAIMED` if a worker crashes or stalls immediately after claiming ownership.
2. **Cancellation-Resolution Deadline (`RUNNING` / `CLAIMED` States)**:
   - When a workflow enters `FAILING` or `CANCELLING`, best-effort cancellation intent is directed to the specific `(AttemptId, WorkerSessionId)`.
   - A finite semantic cancellation-resolution deadline bounds the wait for cooperative worker resolution.
   - If the attempt does not achieve terminal settlement before the deadline elapses, the control plane **authoritatively revokes its execution authority** and transitions the attempt directly to `CANCELLED`.
   - Prevents an uncooperative, partitioned, or hung worker from indefinitely blocking workflow drain convergence.

### 10.7 Worker Loss & Partition Semantics
- When a worker session exceeds its liveness expiry boundary, the control plane declares the session lost.
- All active attempts bound to that session are authoritatively resolved as `FAILED` with a worker-loss-related cause (unless an already-racing terminal transition such as cancellation commits first).
- [ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md) determines whether the task enters `RETRY_WAIT` or fails terminally.
- If a partitioned worker continues executing physically in the background, its authority has been revoked; any eventual completion result it attempts to submit is strictly **fenced** and rejected.

### 10.8 Reconnect vs. Restart Semantics
- **Worker Reconnect**: A surviving worker process reasserts its existing `WorkerSessionId`. If its attempt authority was not revoked during the disconnection, its active attempts continue validly. If authority was already revoked, late results are fenced.
- **Worker Restart**: A restarted worker process generates a **new** `WorkerSessionId`. It cannot inherit or report results for attempts owned by the previous session. Old attempts are resolved via normal liveness or recovery procedures.

### 10.9 Result Correlation & Fencing
- Every worker result must identify the exact `(AttemptId, WorkerSessionId)`.
- A result from a worker session or attempt whose authority has been revoked (due to worker loss, cancellation deadline, or superseding retry) **cannot mutate** attempt or task state, nor satisfy dependencies.
- Duplicate identical terminal submissions are lifecycle-idempotent ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)).

### 10.10 Recovery Contract
- On orchestrator restart, active `ExecutionAttempt` records with their bound `WorkerSessionId` values are recovered from durable storage ([ADR-011](00-architecture-decision-register.md#L210)).
- Worker absence immediately following orchestrator restart does **not** by itself prove worker loss.
- Recovery must provide a bounded reconciliation opportunity allowing surviving workers to reassert active sessions before declaring them lost ([ADR-012](00-architecture-decision-register.md#L211)).
- Attempts remain in their legal ADR-007 states during reconciliation (no synthetic `RECONCILING` state).

---

## 11. Decision Rationale

1. **Incarnation Isolation via `WorkerSessionId`**:
   Binding attempt authority to a transient runtime incarnation identity prevents restarted worker processes from inadvertently claiming ownership or submitting results for stale attempts left behind by previous process instances.
2. **Resilience Over Lease Fragility**:
   Rejecting per-attempt renewable leases avoids massive coordination traffic and eliminates the risk of cascading false-positive task retries caused by momentary orchestrator latency. Session-level liveness combined with independent task execution timeouts provides a far more resilient architecture.
3. **Guaranteed Drain Convergence via Finite Deadlines**:
   Enforcing finite execution-start and cancellation-resolution deadlines ensures that uncooperative, partitioned, or deadlocked remote runtimes cannot freeze the orchestrator's state machine, satisfying ADR-006's global convergence guarantees.
4. **Transport Neutrality via Three-Phase Handshake**:
   Decoupling candidate selection, ownership commitment, and execution start provides a unified semantic interface that operates identically across HTTP long-polling, direct RPC, or message brokers.

---

## 12. Tradeoffs

*   **What is Gained**:
    *   Immunity against stale worker incarnation confusion and zombie result corruption.
    *   Low coordination overhead (one lightweight heartbeat per worker process).
    *   Guaranteed workflow termination convergence under uncooperative worker behavior.
    *   Safe crash recovery without mass worker loss cascades.
    *   Strict transport independence.
*   **What is Sacrificed**:
    *   Detection of internal thread deadlocks in tasks that lack configured execution timeouts (accepted for V1; bounded by timeouts when configured).
    *   Physical single execution: Partitioned workers may continue computing physically after authority is revoked (accepted; results are fenced at the control-plane boundary).
    *   Immediate worker loss detection: Heartbeat intervals require multi-cycle expiry buffers to prevent false positives from transient network jitter.

---

## 13. Consequences

### 13.1 Upstream & Downstream Impact
*   **ADR-005 (Scheduler)**: Hands runnable work to the dispatch boundary; waits for ADR-008 to commit ownership before the task moves to `RUNNING`.
*   **ADR-006 (Workflow State Machine)**: Relies on ADR-008's cancellation-resolution deadline to ensure active work settles and workflows can terminate cleanly during `FAILING` and `CANCELLING`.
*   **ADR-007 (Task Lifecycle)**: Consumes ownership commitment to create `ExecutionAttempt` in `CLAIMED`, execution-start observation to move to `RUNNING`, and worker-loss failure causes to trigger retries.
*   **ADR-009 (Routing)**: Consumes worker session capabilities (Activity Types) and `accepting_new_work` eligibility to route tasks.
*   **ADR-011 (Persistence)**: Must persist `ExecutionAttempt.worker_session_id` and durable semantic deadline representations.
*   **ADR-012 (Recovery)**: Implements the recovery reconciliation procedure and reasserts surviving worker sessions.
*   **ADR-013 (Concurrency)**: Enforces single-winner atomicity for Ownership Commit (Phase 2).
*   **ADR-017 (Graceful Shutdown)**: Uses `accepting_new_work = false` to drain workers cleanly.
*   **ADR-022 (Security)**: Authenticates worker connections before assigning `WorkerSessionId` authority.

---

## 14. Failure Modes

| Failure Mode | Direct Consequence | Architectural Mitigation | Owning ADR |
| :--- | :--- | :--- | :--- |
| **Worker Dies Before Ownership Commit** | Candidate worker drops connection during offer. | Task remains `RUNNABLE`; no attempt created; retry budget unaffected. | ADR-008, ADR-007 |
| **Worker Dies in `CLAIMED` Pre-Start** | Worker crashes after claim without starting task. | Execution-Start Deadline expires $\implies$ Attempt `FAILED(cause=START_TIMEOUT)`. | ADR-008, ADR-007 |
| **Worker Dies While `RUNNING`** | Worker process terminates mid-execution. | Missed heartbeats exceed liveness expiry $\implies$ Attempt `FAILED(cause=WORKER_LOST)`. | ADR-008, ADR-007 |
| **Temporary Network Partition** | Heartbeats temporarily blocked, then resume. | If reconnected within expiry, execution continues; if expired, authority is revoked. | ADR-008, ADR-013 |
| **Worker Restarts with Same Name** | Worker process restarts with original `WorkerId`. | New `WorkerSessionId` generated; cannot claim or report for old attempts; old attempts fenced. | ADR-008, ADR-007 |
| **Two Workers Race to Claim Task** | Multiple workers poll/accept same runnable task. | Atomic Ownership Commit ensures single winner; losing worker claim rejected. | ADR-008, ADR-013 |
| **Duplicate Execution-Start Signal** | Network retransmits execution-start observation. | Idempotent transition handling; subsequent start signals acknowledged with no state change. | ADR-008, ADR-007 |
| **Duplicate / Delayed Result Delivery** | Worker completion message retransmitted or lagged. | Idempotent acknowledgement; terminal attempt state is immutable. | ADR-008, ADR-007 |
| **Worker Ignores Cancellation** | Worker hangs in native code or ignores cancel. | Cancellation-Resolution Deadline expires $\implies$ Authority revoked $\implies$ Attempt `CANCELLED`. | ADR-008, ADR-006 |
| **Orchestrator Crashes While Worker Runs**| Control plane reboots during active task execution. | Recovery grace window allows surviving worker to reassert session; execution continues. | ADR-008, ADR-012 |
| **Physical Execution After Revocation** | Partitioned worker completes after timeout/retry. | Late result strictly fenced by `(AttemptId, WorkerSessionId)` validation; state unaffected. | ADR-008, ADR-007 |
| **Malformed Worker Message** | Worker emits invalid payload or corrupted wire frame. | Message rejected without state mutation; infrastructure failure not converted to task failure. | ADR-008, ADR-018 |

---

## 15. Debugging

Diagnostic observability is maintained by correlating:
* `worker_session_id`: Authoritative runtime process incarnation.
* `worker_id`: Optional descriptive logical worker name.
* `attempt_id`: UUID for the specific execution try.
* `task_execution_id`: Logical task identifier.
* `workflow_execution_id`: Parent workflow execution identifier.
* `is_live`: Current liveness status in the ephemeral registry.
* `accepting_new_work`: Eligibility flag for new task assignments.
* `last_liveness_observed_at`: Monotonic/control-plane receipt timestamp of last heartbeat.
* `execution_start_deadline`: Expiration boundary for `CLAIMED` attempts.
* `cancellation_deadline`: Expiration boundary for cancellation-directed attempts.

---

## 16. Testing

The following unit, integration, and simulation test suites must be developed to validate ADR-008:
*   **Session Incarnation Tests**: Verify that worker restart produces a new `WorkerSessionId` and that old session credentials cannot mutate state.
*   **Surviving Reconnect Tests**: Verify that a surviving worker process reconnecting with its original `WorkerSessionId` preserves its active attempts.
*   **Three-Phase Handshake Tests**: Assert that transport failures during Phase 1 leave tasks in `RUNNABLE` with zero attempts created.
*   **Ownership Exclusivity Tests**: Simulate concurrent worker claims for the same task and assert that exactly one winner commits.
*   **Execution-Start Deadline Tests**: Simulate a worker claiming an attempt and stalling; assert that the attempt resolves to `FAILED` when the deadline expires.
*   **Liveness Expiry & Worker Loss Tests**: Simulate missed heartbeats exceeding liveness timeout; assert that active attempts transition to `FAILED(cause=WORKER_LOST)`.
*   **Cancellation-Resolution Deadline Tests**: Simulate an uncooperative worker ignoring cancellation; assert that the attempt resolves to `CANCELLED` when the deadline expires.
*   **Stale Result Fencing Tests**: Simulate a partitioned worker submitting a completion after its authority was revoked; assert that the callback is rejected.
*   **Recovery Grace Simulation Tests**: Simulate orchestrator restart with surviving workers reconnecting within the grace period; assert that active attempts are not prematurely failed.

---

## 17. Operational Considerations
*   **Transient Worker Presence**: Operators should understand that the Worker Registry is dynamically reconstructed; missing workers after a prolonged outage will simply drop off after the liveness expiry.
*   **Tuning Expiry Buffers**: Setting liveness expiry too aggressively risks false-positive worker loss during transient network spikes, causing unnecessary retries. Setting it too conservatively delays failure detection and recovery.
*   **Cancellation Grace Tuning**: The cancellation resolution deadline trades fast workflow termination against allowing running tasks a reasonable window to complete or shut down cleanly.
*   **Zero Wall-Clock Dependence**: Monitoring and operational dashboards should display durations relative to control-plane time rather than relying on worker system clocks.

---

## 18. Maintenance
*   **Decoupled Protocol Evolution**: Future changes to transport protocols (e.g., migrating from REST polling to gRPC streams) must preserve the abstract three-phase handshake and session identity model.
*   **Extending Worker Metadata**: Future additions to worker metadata (e.g., GPU memory, rack locality, CPU architectures) must be introduced as supplementary capability attributes without modifying core liveness semantics.

---

## 19. Future Evolution
*   **V2 Fine-Grained Worker Capacity Models**: Future versions can introduce explicit slot quotas, dynamic load metrics, and concurrent attempt counters in coordination with [ADR-009](00-architecture-decision-register.md#L204).
*   **V2 Progress Heartbeats**: For extremely long-running tasks (e.g., multi-hour batch jobs), optional user-driven task progress heartbeats can be introduced to provide fine-grained progress tracking.
*   **V2 Distributed Orchestrator Coordination**: Multi-orchestrator high availability ([ADR-025](00-architecture-decision-register.md#L232)) can introduce distributed consensus for ownership commits and shared liveness monitoring across multiple control-plane nodes.

---

## 20. Rejected Alternatives

*   **Worker Authority Based on `WorkerId` Only**: Rejected because process restarts reusing the same static name create zombie identity overlap, allowing stale messages from dead processes to corrupt active state.
*   **Durable Worker Registry as Active Truth**: Rejected because active worker presence is inherently transient; relying on database records for liveness risks accumulating stale "ghost workers" across crashes.
*   **Per-Attempt Renewable Leases**: Rejected for V1 because the bi-directional traffic overhead and risk of false-positive mass retries during control-plane hiccups outweigh the benefits for a single-developer architecture.
*   **Mandatory Per-Attempt Progress Heartbeats**: Rejected for V1 because task execution timeouts already bound duration, and requiring progress signals adds substantial client complexity without guaranteeing hung-thread detection.
*   **Socket Connection State as Sole Authority**: Rejected because it binds the orchestration model to persistent stateful connections and causes instant false aborts during transient network reconnects.
*   **`ExecutionAttempt.CANCELLING` State**: Rejected in ADR-007 and preserved here; cancellation deadlines are tracked as coordination context, avoiding state-machine bloat.
*   **Worker Wall-Clock as Time Authority**: Rejected because distributed clock skew creates non-deterministic timeout and liveness bugs.
*   **Guaranteed Physical Single Execution**: Rejected because network partitions make physical remote execution guarantees impossible; logical fencing at the control-plane boundary is the only mathematically sound guarantee.

---

## 21. Decision Evolution
*   *2026-09-05 (Architectural Review & Refinement)*:
    *   Adopted `WorkerSessionId` as authoritative runtime incarnation identity; relegated `WorkerId` to optional descriptive role.
    *   Selected ephemeral/reconstructible Worker Registry; mandated durable persistence for `ExecutionAttempt ↔ WorkerSessionId` association.
    *   Defined abstract Three-Phase Ownership Handshake (`Candidate/Offer` $\to$ `Ownership Commit` $\to$ `Execution Start`).
    *   Rejected per-attempt leases and per-attempt progress heartbeats in favor of worker-session heartbeats and independent execution timeouts.
    *   Established finite Execution-Start Deadlines for `CLAIMED` and Cancellation-Resolution Deadlines for drain convergence.
    *   Defined control-plane time authority and recovery reconciliation grace concepts.
    *   Removed transport leakage (HTTP, gRPC, Redis, Kafka) and database mechanism leakage (CAS, row locks).

---

## 22. Common Misconceptions

*   **Misconception: "A worker heartbeat proves that a specific task thread is actively making progress."**
    *   *Reality*: Worker heartbeats prove that the worker *process runtime* is alive and communicating. Progress of a specific task thread is bounded by its configured execution timeout ([ADR-007](adr-007-task-execution-lifecycle-and-attempt-model.md)).
*   **Misconception: "NexusFlow guarantees that a cancelled worker stops executing immediately."**
    *   *Reality*: Cancellation intent is delivered best-effort. If a remote worker ignores the signal or hangs in native code, the orchestrator revokes authority after the cancellation deadline and resolves the attempt as `CANCELLED`. The physical worker may continue executing in isolation, but its results are strictly fenced.
*   **Misconception: "A single missed heartbeat indicates worker death."**
    *   *Reality*: Transient network congestion frequently delays heartbeats. Liveness expiry requires an evaluation boundary tolerating transient jitter before declaring a session lost.
*   **Misconception: "WorkerSessionId is an authentication mechanism."**
    *   *Reality*: `WorkerSessionId` provides incarnation uniqueness and correlation; it does not provide cryptographic trust or access control ([ADR-022](00-architecture-decision-register.md#L225)).

---

## 23. Open Questions
*   **Non-blocking downstream configuration decisions**:
    *   Specific numeric values for heartbeat intervals and liveness timeout multipliers (Configuration / [ADR-020](00-architecture-decision-register.md#L223)).
    *   Specific numeric durations for execution-start deadlines and cancellation-resolution deadlines (Configuration / [ADR-020](00-architecture-decision-register.md#L223)).
    *   Selection of wire protocols and transport mechanisms (push vs. pull vs. message broker) ([ADR-020](00-architecture-decision-register.md#L223)).
    *   Selection of worker capacity and concurrency scheduling algorithms ([ADR-009](00-architecture-decision-register.md#L204)).

---

## 24. Interview Discussion

*   **Q: Why use WorkerSessionId instead of a static WorkerId?**
    *   *A*: In distributed systems, worker processes crash and reboot. If a worker named `worker-1` crashes while executing Attempt A, and a new process boots up under the same name `worker-1`, any delayed or retransmitted messages from the old process could be accepted as valid by the orchestrator. By generating a unique `WorkerSessionId` per process incarnation, the orchestrator unambiguously distinguishes between the dead incarnation and the new incarnation, fencing out zombie messages.
*   **Q: Why choose worker-session heartbeats instead of renewable per-attempt leases?**
    *   *A*: In an engine running thousands of concurrent tasks, per-attempt leases create high coordination traffic scaling with task count ($N \times \text{heartbeats}$). If the orchestrator experiences a momentary database stall or network hiccup, all active attempt leases could expire simultaneously, triggering a catastrophic cascading failure where thousands of healthy tasks are aborted and retried. A single session-level heartbeat decoupled from task duration scales with worker node count, provides robust process crash detection, and avoids mass-retry storms.
*   **Q: How does NexusFlow prevent uncooperative workers from blocking workflow cancellation?**
    *   *A*: NexusFlow enforces a finite Cancellation-Resolution Deadline. When a workflow cancels or fails fast, cancellation intent is emitted to active workers. If a worker fails to report a terminal outcome within the deadline, the orchestrator authoritatively revokes the attempt's execution authority and marks it `CANCELLED`. Logical settlement completes, and the workflow terminates cleanly. Any late completion submitted by the worker is fenced and rejected.
*   **Q: What happens during a network partition?**
    *   *A*: If a worker is partitioned from the orchestrator, its heartbeats will stop arriving. Once the liveness timeout expires, the orchestrator authoritatively declares the session lost and marks its active attempts as `FAILED(cause=WORKER_LOST)`. If retry budget remains, the orchestrator creates a new attempt on a different worker. If the partitioned worker continues executing and eventually submits a result, the orchestrator rejects the callback because the attempt authority was revoked.

---

## 25. References
*   [ADR-005: Workflow Task Scheduling & Dispatch Architecture](adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
*   [ADR-006: Workflow Execution State Machine](adr-006-workflow-execution-state-machine.md)
*   [ADR-007: Task Execution Lifecycle & Attempt Model](adr-007-task-execution-lifecycle-and-attempt-model.md)
*   [00-Architecture Decision Register](00-architecture-decision-register.md)

---

## 26. Traceability

| Artifact / Requirement | Addressed in ADR-008 |
| :--- | :--- |
| **Capability 5** | Fully addressed via worker identity model, reconstructible registry, liveness monitoring, and three-phase claim handshake. |
| **ADR-005 Scheduler** | Defines ownership handshake establishing the transition from `RUNNABLE` to `RUNNING`. |
| **ADR-006 State Machine** | Provides cancellation delivery and finite cancellation-resolution deadlines ensuring drain convergence. |
| **ADR-007 Task Lifecycle** | Supplies ownership commitment for attempt creation, execution-start observation, and worker-loss failure causes. |
| **ADR-009 Routing** | Exposes worker capabilities (Activity Types) and `accepting_new_work` eligibility. |
| **ADR-011 Persistence** | Defines semantic requirement for durable `ExecutionAttempt ↔ WorkerSessionId` association. |
| **ADR-012 Recovery** | Defines recovery reconciliation contract preventing premature worker loss on reboot. |
| **ADR-013 Concurrency** | Establishes single-winner requirements for Phase 2 Ownership Commit and result fencing. |

---

## 27. Decision Validation Checklist

*   [x] **WorkerSessionId is correctness authority?** Yes (Section 10.1).
*   [x] **WorkerId is optional/descriptive only?** Yes (Section 10.1).
*   [x] **No UUIDv4/crypto generation commitment?** Yes, defined as sufficiently unique (Section 10.1).
*   [x] **Worker Registry ephemeral/reconstructible?** Yes (Section 10.2).
*   [x] **No mandatory in-memory implementation?** Yes, representation deferred (Section 10.2).
*   [x] **Attempt ↔ WorkerSession association durably recoverable?** Yes (Section 10.2).
*   [x] **Activity Type capabilities exposed?** Yes (Section 10.3).
*   [x] **live distinct from accepting_new_work?** Yes (Section 10.3).
*   [x] **No mandatory concurrency_limit?** Yes, deferred to ADR-009 (Section 10.3).
*   [x] **Session-level liveness only?** Yes (Section 10.4).
*   [x] **No per-Attempt heartbeat?** Yes, explicitly rejected (Section 10.4).
*   [x] **No per-Attempt renewable lease?** Yes, explicitly rejected (Section 10.4).
*   [x] **One missed heartbeat does not equal death?** Yes, jitter buffer required (Section 10.4).
*   [x] **Worker clock not authoritative?** Yes, control plane authoritative (Section 10.4).
*   [x] **Durable deadlines compatible with restart?** Yes (Section 10.4).
*   [x] **Three-phase ownership semantics?** Yes (`Candidate` $\to$ `Commit` $\to$ `Start`, Section 10.5).
*   [x] **Attempt created only at Ownership Commit?** Yes (Section 10.5).
*   [x] **Pre-ownership failure creates no Attempt?** Yes (Section 10.5).
*   [x] **CLAIMED distinct from RUNNING?** Yes (Section 10.5).
*   [x] **Finite execution-start deadline?** Yes (Section 10.6).
*   [x] **Worker loss resolves active Attempt semantically?** Yes (Section 10.7).
*   [x] **Reconnect same session only for surviving process?** Yes (Section 10.8).
*   [x] **Restart creates new session?** Yes (Section 10.8).
*   [x] **Finite cancellation-resolution deadline?** Yes (Section 10.6).
*   [x] **No Attempt CANCELLING state?** Yes (Section 10.6, 20).
*   [x] **Success/failure/cancel race remains single-winner?** Yes (Section 10.6, 14).
*   [x] **Stale/revoked results fenced?** Yes (Section 10.9).
*   [x] **No result wire schema frozen?** Yes (Section 10.9).
*   [x] **Recovery grace is semantic, not procedural?** Yes (Section 10.10).
*   [x] **No synthetic reconciliation state?** Yes (Section 10.10).
*   [x] **Routing boundary preserved?** Yes (Section 13.1).
*   [x] **Persistence/recovery/concurrency boundaries preserved?** Yes (Section 13.1).
*   [x] **WorkerSessionId not authentication?** Yes (Section 22).
*   [x] **No exactly-once physical execution claim?** Yes, explicitly disclaimed (Section 7, 12).
*   [x] **No fake benchmarks/production evidence?** Yes, verified throughout.
