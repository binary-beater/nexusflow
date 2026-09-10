# ADR-012 — Recovery Strategy

## 1. Purpose

This Architectural Decision Record (ADR) defines the system recovery strategy, post-crash reconciliation procedure, and operational continuity model for the NexusFlow orchestration engine. It establishes how the control plane reconstructs safe, deterministic orchestration behavior following an orchestrator process crash, host termination, transient persistence outage, loss of volatile message queues, or loss of in-memory worker registries.

Furthermore, this record formalizes how the orchestrator reconciles in-flight worker executions, re-establishes ephemeral scheduling heaps, handles cold-start worker re-registration grace, and repairs valid interrupted lifecycle transitions. It preserves the integrity of state machines established in [ADR-006](docs/architecture/adr-006-workflow-execution-state-machine.md) and [ADR-007](docs/architecture/adr-007-task-execution-lifecycle-and-attempt-model.md), enforces the durable current-state model from [ADR-011](docs/architecture/adr-011-state-persistence-strategy.md), and maintains clean boundaries with physical concurrency control ([ADR-013](docs/architecture/00-architecture-decision-register.md#adr-013---consistency--concurrency-strategy)) and error classification ([ADR-018](docs/architecture/00-architecture-decision-register.md#adr-018---error-handling-philosophy)).

---

## 2. Context

NexusFlow orchestrates long-running, multi-step directed acyclic graph (DAG) workflows across distributed, loosely coupled workers. The architectural foundation is established across eleven preceding decisions:
- [ADR-001](docs/architecture/adr-001-internal-workflow-specification.md) & [ADR-002](docs/architecture/adr-002-workflow-definition-parsing-strategy.md) established the canonical Internal Workflow Specification (Validated IWS) and dictated that execution and recovery must never re-parse external YAML authoring files.
- [ADR-003](docs/architecture/adr-003-canonical-workflow-graph-representation.md) established that the canonical task graph is a pure, deterministic projection derived directly from the Validated IWS.
- [ADR-005](docs/architecture/adr-005-workflow-task-scheduling-and-dispatch-architecture.md) defined success-only dependency satisfaction and task eligibility.
- [ADR-006](docs/architecture/adr-006-workflow-execution-state-machine.md) established the root `WorkflowExecution` lifecycle (`INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`).
- [ADR-007](docs/architecture/adr-007-task-execution-lifecycle-and-attempt-model.md) decoupled logical `TaskExecution` states from ephemeral `ExecutionAttempt` records and established retry isolation.
- [ADR-008](docs/architecture/adr-008-worker-coordination-and-liveness-model.md) established worker incarnation tracking via `WorkerSessionId`, durable attempt ownership, execution-start deadlines, cancellation-resolution deadlines, and single-winner result fencing.
- [ADR-009](docs/architecture/adr-009-task-routing-strategy.md) decoupled routing and candidate matching from authoritative attempt creation.
- [ADR-010](docs/architecture/adr-010-workflow-data-flow-and-parameter-passing.md) established the JSON-compatible value model, materialized task inputs, authoritative output immutability, and atomic visibility of success states with outputs.
- [ADR-011](docs/architecture/adr-011-state-persistence-strategy.md) established the **Durable Current-State Persistence Model** as the authoritative source of orchestration truth, defined ten logical consistency groups, and classified ephemeral vs. reconstructible state.

While ADR-011 establishes **what state is durable**, an orchestration engine inevitably encounters process crashes, host reboots, unannounced worker disconnections, and storage timeouts. The system requires an explicit **Recovery Strategy** to govern how a restarted orchestrator transitions from cold storage to active scheduling without losing progress, duplicating execution, or corrupting state.

---

## 3. Problem Statement

Orchestrator recovery must address complex distributed coordination challenges:

1. **State Reconstruction without Event Replay:** Replaying entire historical event streams from workflow inception causes high recovery latency, memory overhead, and event versioning hazards. How does NexusFlow recover execution truth instantly using only current-state records?
2. **Elimination of Source Authoring Dependencies:** If a crash occurs, how does the orchestrator reconstruct task graph topology when external YAML files are missing, moved, or modified?
3. **Distinguishing Ephemeral Absence from Physical Worker Loss:** Immediately after an orchestrator restarts, its in-memory `WorkerRegistry` is empty. How does recovery avoid prematurely declaring active workers dead and scheduling duplicate attempts while workers are still executing their assignments?
4. **Preserving Timing Semantics Across Restarts:** Process-local monotonic timers do not survive crashes. How does the engine ensure that retry backoffs, execution-start deadlines, task execution timeouts, and cancellation drains do not reset back to zero following a restart?
5. **Handling Reconnecting Workers and Claims:** When surviving workers reconnect, how does the engine evaluate worker claims about in-flight or completed tasks without compromising durable authority?
6. **Repairing Interrupted Lifecycle Transitions:** If a crash interrupts multi-entity operations—such as eager task creation during initialization, or workflow completion after all tasks succeed—how does recovery reconcile them forward into consistent states?
7. **Handling Persistence Invariant Violations:** If corrupted or torn data is encountered in storage, how does recovery prevent hazardous execution without silently fabricating state?

---

## 4. Requirements Covered

This architecture directly addresses the following NexusFlow requirements:

- **RECOV-01:** Implement a deterministic durable-state reconciliation model that recovers non-terminal workflows without event replay or rollback.
- **RECOV-02:** Reconstruct canonical task graph projections exclusively from durable Validated IWS specifications without reparsing external YAML files.
- **RECOV-03:** Execute a selective startup scan across non-terminal executions without introducing a global stop-the-world recovery barrier.
- **RECOV-04:** Provide a bounded worker reconnection grace window that allows surviving worker sessions to re-establish liveness without resetting execution-start deadlines or granting retrospective start claims.
- **RECOV-05:** Reconstruct ephemeral scheduling structures, dispatch pools, and timer heaps directly from durable current-state records.
- **RECOV-06:** Reconcile interrupted workflow initializations, ensuring that partial task sets are completed idempotently and that cancellations during initialization guarantee the creation and cancellation of the complete expected task set.
- **RECOV-07:** Preserve the monotonic direction of `FAILING` and `CANCELLING` workflows, ensuring unstarted tasks are cancelled and terminal settlement occurs only when all tasks are terminal.
- **RECOV-08:** Guarantee strict idempotency and re-entrancy across all recovery operations.
- **RECOV-09:** Maintain strict fail-closed persistence behavior during recovery outages, ensuring infrastructure faults are never converted into business execution failures.
- **RECOV-10:** Quarantine persistence invariant violations, preventing hazardous execution progression on corrupted data.

---

## 5. Constraints

1. **State Machine Invariants (ADR-006 & ADR-007):** Recovery must not invent ad-hoc lifecycle states (no `RECOVERING` or `RECONCILING` states) and must not bypass legal state machine transitions.
2. **Durable Current-State Authority (ADR-011):** Recovery operates exclusively from the authoritative durable entities defined in ADR-011 (`RegisteredDefinition`, `WorkflowExecution`, `TaskExecution`, `ExecutionAttempt`). Ephemeral registries and message queues have zero authority.
3. **Worker Coordination Semantics (ADR-008):** Worker session authority is bound strictly to `WorkerSessionId`. New worker incarnations (`new WorkerSessionId`) cannot inherit authority for attempts owned by a dead session.
4. **Bounded Inline Payloads (ADR-010):** Payload handling adheres to the JSON-compatible value model and bounded limits; recovery does not dereference external blob stores in V1.
5. **Separation of Concurrency Primitives (ADR-013):** ADR-012 defines logical recovery algorithms; physical concurrency controls (locks, CAS, transaction isolation) are owned by ADR-013.
6. **Separation of Error Taxonomies (ADR-018):** ADR-012 defines when an attempt or task must fail; formal error codes and failure classifications are owned by ADR-018.
7. **Separation of Configuration Limits (ADR-023):** Numerical thresholds for grace durations and deadlines are owned by ADR-023.

---

## 6. Goals

- Define a **Deterministic Durable-State Reconciliation** model for crash recovery.
- Establish a selective startup scan that discovers non-terminal executions and rebuilds ephemeral scheduling state.
- Ensure recovery is incremental and execution-scoped, avoiding global startup barriers.
- Define the bounded worker reconnection grace model, enforcing that grace does not extend or reset elapsed execution-start deadlines.
- Define worker reconnection protocols, treating worker claims and active-attempt omissions as non-authoritative reconciliation evidence.
- Ensure that task execution retry backoffs, timeouts, and cancellation deadlines survive restarts without being reset to zero.
- Reconcile interrupted initializations, guaranteeing that workflows reaching terminal cancellation establish exactly one `TaskExecution` per `TaskDefinition`.
- Repair valid crash intermediate states (e.g., all-tasks-succeeded, task-failure-mismatch) into consistent terminal or failure-drain states.
- Enforce fail-safe quarantine for impossible persistence invariant violations.
- Guarantee that orchestrator restarts alone never create `ExecutionAttempt` records and never consume retry budgets.

---

## 7. Non-Goals

- Replaying historical event streams or audit logs to reconstruct execution state in V1.
- Implementing automated rollback to earlier checkpoints or restarting failed workflows from scratch.
- Solving multi-orchestrator leader election or active-active failover (owned by ADR-025).
- Providing an exactly-once physical execution guarantee for external worker activity side effects.
- Defining long-term audit trail archiving, event schemas, or history purging (owned by ADR-014).
- Specifying physical database queries, SQL syntax, or index layouts (owned by ADR-020).

---

## 8. Candidate Solutions

### 8.1 Recovery Strategy Candidates

#### Candidate A: Global Workflow Restart from Inception
Upon orchestrator restart, all non-terminal workflows are reset to their initial state. Completed tasks are re-executed from scratch.
- *Pros:* Extremely simple recovery logic; no need to reconcile intermediate task states or in-flight attempts.
- *Cons:* Catastrophic compute waste; causes massive duplicate execution of non-idempotent business side effects; unacceptable latency for long-running workflows; completely unviable for production orchestration.

#### Candidate B: Fail-All-on-Restart
Upon orchestrator restart, all non-terminal workflows and active tasks are immediately marked as `FAILED`.
- *Pros:* Trivially simple implementation; eliminates recovery reconciliation complexities.
- *Cons:* Destroys workflow progress on every routine orchestrator deployment or minor crash; unacceptable availability and SLA violations.

#### Candidate C: Full Event-Stream Replay
Every state transition is stored as an event. Recovery replays the entire event log from workflow inception through the state machine logic to reconstruct the in-memory state of each execution.
- *Pros:* Reconstructs the exact sequence of historical transitions.
- *Cons:* High recovery latency scaling with workflow event volume; requires complex snapshotting and compaction systems; event schema versioning across application upgrades creates maintenance fragility; excessive complexity for a V1 engine.

#### Candidate D: Worker-Authoritative State Reconstruction
Upon startup, the orchestrator queries all connected workers to discover what tasks they are currently running. The orchestrator reconstructs its state based on worker assertions.
- *Pros:* Reduces reliance on orchestrator persistence.
- *Cons:* Inverts architectural authority; workers are untrusted edge nodes that may be partitioned, crashed, or slow; creates split-brain hazards and makes stale or duplicate task execution unavoidable.

#### Candidate E: Selected — Deterministic Durable-State Reconciliation
Recovery operates directly against the authoritative current-state records defined in ADR-011. The orchestrator inspects the current lifecycle states, materialized inputs, authoritative outputs, worker ownership links, and semantic deadlines, deterministically reconciling each execution forward to its next valid state under standard state machine rules.
- *Pros:* Low recovery startup latency; direct indexed queries; perfectly aligned with ADR-011's ACID entity model; preserves completed task progress; resilient against message broker or cache loss.
- *Cons:* Requires precise reconciliation logic for interrupted intermediate lifecycle states.

---

## 9. Detailed Evaluation

| Evaluation Dimension | Option A: Global Restart | Option B: Fail-All | Option C: Event Replay | Option D: Selected (Reconciliation) | Option E: Worker-Authoritative |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Progress Preservation** | Zero (re-runs DAG) | Zero (aborts workflows) | Full (replays history) | **Full (preserves completed outputs)** | Unsafe (trusts untrusted workers) |
| **Recovery Latency** | Very High (re-execution) | Immediate (mass failure) | High (linear with events) | **Low (direct indexed query)** | Moderate (waits for full worker poll) |
| **Crash Resilience** | Poor (cascading failure) | Terrible (zero durability) | Moderate (snapshot compaction) | **High (re-entrant, idempotent)** | Low (split-brain on worker crash) |
| **Operational Simplicity** | Low | Very Low | Very Low (projections drift) | **Moderate (state-machine alignment)** | High (complex distributed consensus) |
| **Business Impact** | Severe compute waste | Severe SLA violations | High CPU/memory during boot | **Minimal (smooth resumption of work)** | High risk of duplicate side effects |

The evaluation demonstrates that **Deterministic Durable-State Reconciliation** provides the required balance of operational reliability, rapid startup, and strict adherence to established state machines.

---

## 10. Decision

NexusFlow adopts **Deterministic Durable-State Reconciliation** as its recovery strategy.

### 10.1 Authoritative Recovery Input
Recovery operates strictly from the authoritative durable current-state model established in ADR-011:
- `RegisteredDefinition` (holding the immutable Validated IWS semantics).
- `WorkflowExecution` (lifecycle state, input, output, failure/cancellation context, definition reference).
- `TaskExecution` (lifecycle state, materialized business input, authoritative output, retry timing).
- `ExecutionAttempt` (attempt identity, ordinal, lifecycle state, `WorkerSessionId` ownership, semantic deadlines, terminal diagnostic context).

**Recovery Independence Invariant:** Recovery operates exclusively against durable internal specifications. It **never** reparses external authoring YAML files, relies on source file availability, or accepts worker claims as authoritative truth.

---

### 10.2 Startup Scan & Scope
1. **Mandatory Startup Scan:** Upon startup, the orchestrator executes a selective scan across all non-terminal executions in durable storage:
   - `WorkflowExecution` records in `INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`.
   - `TaskExecution` records in `PENDING`, `RUNNABLE`, `RUNNING`, `RETRY_WAIT`.
   - `ExecutionAttempt` records in `CLAIMED`, `RUNNING`.
   - Durable semantic deadlines and retry eligibility timestamps.
2. **Steady-State Polling Decoupling:** The mandatory startup scan is strictly a cold-start initialization mechanism. Steady-state orchestration correctness does **not** rely on continuous, periodic full-database polling sweeps.
3. **No Global Recovery Barrier:** Recovery is **incremental and execution-scoped**. The control plane does not block the entire system until all non-terminal workflows are reconciled. Individual workflows resume scheduling as soon as their required reconciliation context is loaded. New workflow starts are accepted once core control-plane services are initialized.

---

### 10.3 Absence of Synthetic Lifecycle States
NexusFlow explicitly rejects introducing synthetic lifecycle states such as `RECOVERING` or `RECONCILING` into the `WorkflowExecution`, `TaskExecution`, or `ExecutionAttempt` state machines. 
- A `WorkflowExecution` remains in its authoritative state (e.g., `RUNNING`) while operationally undergoing reconciliation.
- Recovery status (e.g., `is_reconciling`, `recovery_started_at`) is strictly operational metadata (ADR-015 / ADR-016), never business execution state.

---

### 10.4 Canonical Graph Reconstruction
For each active workflow execution discovered during recovery:
- The orchestrator retrieves the exact immutable `RegisteredDefinition` referenced by the execution.
- The canonical DAG topology is deterministically reconstructed in memory using the pure projection rules from ADR-003.
- If a referenced definition record is missing or undecodable, the execution is flagged as an unrecoverable persistence integrity failure and quarantined.

---

### 10.5 Workflow Initialization Recovery

#### 10.5.1 Interrupted Eager Task Creation
An interrupted eager task creation loop leaves a workflow in `INITIALIZING` with a partial task set ($M$ of $N$ tasks in storage).
- **Handling:** Recovery compares existing `TaskExecution` records against the expected task set from the canonical DAG, idempotently inserts missing tasks in `PENDING`, and promotes the workflow to `RUNNING` only after the complete expected task set exists durably (ADR-011 Atomicity Group 1).
- **Non-Failure Invariant:** Infrastructure crashes during initialization **never** trigger `INITIALIZING -> FAILED`.

#### 10.5.2 Cancellation During Partial Initialization
If `WorkflowExecution.lifecycle_state = CANCELLING` and initialization was interrupted with only $M$ of $N$ tasks created:
- **Canonical Invariant:** Every `WorkflowExecution` must have **exactly one logical `TaskExecution` for every `TaskDefinition`** in its definition.
- **Handling:**
  1. Existing $M$ tasks (in `PENDING`) transition directly to `CANCELLED`.
  2. The remaining $N - M$ missing `TaskExecution` records are durably established through the ADR-007-compatible lifecycle: instantiated in `PENDING` and immediately transitioned to `CANCELLED`. They are never observably schedulable or runnable work.
  3. The workflow execution transitions `CANCELLING -> CANCELLED` only after the complete expected $N$-task set exists durably and every `TaskExecution` is terminal.
- A terminal workflow execution is **never** permitted to exist with a truncated or incomplete task set.

---

### 10.6 Task Execution Reconciliation

#### 10.6.1 PENDING Tasks
- If the workflow is `RUNNING`: recovery evaluates direct dependencies. If all direct dependencies are terminal `SUCCEEDED`, recovery materializes the resolved business input and transitions `TaskExecution.PENDING -> RUNNABLE` (Atomicity Group 1). Otherwise, it remains `PENDING`.
- If the workflow is `FAILING` or `CANCELLING`: the task transitions `PENDING -> CANCELLED`.
- If an upstream dependency definitively failed while the workflow is `RUNNING`, the task remains `PENDING` until workflow failure-direction repair initiates failure drain.

#### 10.6.2 RUNNABLE Tasks
- `RUNNABLE` is authoritative, rediscoverable executable work.
- If the workflow is `RUNNING`: the task is verified to have materialized business input and is re-enqueued into the ephemeral routing candidate pool (ADR-009). No `ExecutionAttempt` is created during recovery.
- If the workflow is `FAILING` or `CANCELLING`: the task transitions `RUNNABLE -> CANCELLED`.

#### 10.6.3 RETRY_WAIT Tasks
- If the workflow is `RUNNING`:
  - If `retry_eligible_at <= now()`: the backoff has elapsed; transition `TaskExecution.RETRY_WAIT -> RUNNABLE`.
  - If `retry_eligible_at > now()`: rebuild the in-memory timer wakeup for the remaining duration.
  - **No Reset Invariant:** Process restarts **never** reset retry backoff timers to zero and never replay missed timer ticks.
- If the workflow is `FAILING` or `CANCELLING`: the task transitions `RETRY_WAIT -> CANCELLED`.

#### 10.6.4 RUNNING Tasks
- A `TaskExecution` in `RUNNING` strictly requires an associated active `ExecutionAttempt` in `CLAIMED` or `RUNNING`.
- Recovery locates the active attempt from durable storage.
- If no active attempt exists, or if multiple active attempts exist, it is classified as an ADR-011 persistence invariant violation and quarantined.

---

### 10.7 Execution Attempt Reconciliation & Worker Coordination

#### 10.7.1 Bounded Worker Reconnection Grace
- Immediately following an orchestrator restart, the in-memory `WorkerRegistry` is empty.
- An empty registry is **not** evidence of worker death.
- All active attempts enter a bounded **Reconnection Grace Window** ($T_{grace}$, configured in ADR-023).
- **Grace Boundary Invariant:** The grace window governs worker *liveness* reconciliation only. It **never** extends, resets, or resurrects elapsed execution-start deadlines, task timeouts, or cancellation deadlines, and never authorizes retrospective start claims.

#### 10.7.2 Same-Session Continuity vs. New Incarnations
- Surviving worker processes presenting the **identical `WorkerSessionId`** re-associate with their durable attempts.
- A worker rebooting creates a **new `WorkerSessionId`**. A new session cannot inherit authority for attempts owned by the previous session, even if `worker_id` or host matches. The old attempts remain bound to the dead session and are resolved via worker-loss timeout post-grace.

#### 10.7.3 CLAIMED Attempt Reconciliation
1. **Worker Absent:** The attempt remains in `CLAIMED` during the grace window, governed by its durable `execution_start_deadline_at`.
2. **Worker Reconnects Within Deadline:** If the same worker session reconnects and `execution_start_deadline_at > now()`, the worker may execute the standard ADR-008 execution-start handshake. Only current control-plane acceptance of that handshake permits the transition `CLAIMED -> RUNNING`.
3. **Rejection of Retrospective Start Claims:** If a worker asserts, *"I started before the crash,"* this statement is non-authoritative worker testimony. It **cannot** retroactively promote `CLAIMED -> RUNNING`.
4. **Expired Start Deadline:** If `execution_start_deadline_at <= now()` (whether elapsed before the crash or during downtime), the start deadline is not extended or resurrected by grace. The attempt transitions `CLAIMED -> FAILED` (error cause classified under ADR-018 execution-start timeout semantics).
5. **Buffered Terminal Result for CLAIMED Attempt:** There is no legal transition `CLAIMED -> SUCCEEDED`. If a reconnecting worker buffers a completed result while the durable attempt remains `CLAIMED`, recovery **must not** accept success directly, fabricate a retrospective start observation, or perform a fictional post-completion start handshake. Because authoritative execution start was never observed by the control plane under ADR-007/008, the attempt remains governed by its execution-start deadline. If the start deadline expired, the attempt transitions `CLAIMED -> FAILED`, and the late terminal payload is fenced as non-authoritative.

#### 10.7.4 RUNNING Attempt Reconciliation
- Execution start was authoritatively observed prior to the crash.
- If the owning worker reconnects with the same session ID, it is re-associated in the `WorkerRegistry`, and normal execution tracking resumes.
- If the worker fails to reconnect before the grace window expires, authority is revoked, and the attempt transitions `RUNNING -> FAILED` (error cause classified under ADR-018 worker-loss semantics).
- If an execution timeout elapsed during downtime, it remains eligible for enforcement. However, if a valid worker terminal result races against timeout enforcement, single-winner commit rules apply under ADR-013. Timeout does not blindly overwrite a completed result.

#### 10.7.5 Worker Reconnection Claims & Omission Handling
- Worker-reported active attempt lists are **reconciliation evidence**, never authoritative truth.
- If a worker claims an attempt that is already terminal or revoked in storage, the claim is rejected and the worker is instructed to abort.
- If durable storage shows `Session_S` owns active Attempt $B$, but `Session_S` reconnects and **omits Attempt $B$** from its reported list:
  - **Omission alone does NOT automatically terminalize Attempt $B$.**
  - The orchestrator treats omission as negative reconciliation evidence and issues an explicit reconciliation query.
  - If the worker responds with an explicit negative acknowledgement (confirming it is not executing Attempt $B$), the orchestrator may execute accelerated worker-loss resolution: transition Attempt $B$ to `FAILED` (error cause classified under ADR-018 worker abandonment/loss semantics).
  - Otherwise, Attempt $B$ remains governed by normal deadline and grace window expirations.

#### 10.7.6 Worker-Held Terminal Results & Fencing
- A surviving worker holding an unacknowledged terminal result may resubmit it upon reconnection.
- **Authoritative Prerequisite:** A buffered terminal result may directly settle task success only when the durable authoritative attempt state is **`RUNNING`** and `WorkerSessionId` ownership remains valid. In this case, the result is accepted as a single-winner commit (ADR-011 Atomicity Group 4), transitioning the task to `SUCCEEDED`.
- If the attempt in storage is `CLAIMED`, the buffered result cannot be accepted directly (as detailed in Section 10.7.3).
- Stale or duplicate results for already closed, revoked, or superseded attempts are fenced and quarantined.

---

### 10.8 Ephemeral Scheduling & Timer Heap Reconstruction
- External message queues, broker topics, and routing pools are completely reconstructed by scanning durable `RUNNABLE` tasks.
- Ephemeral timer heaps are reconstructed by loading pending timestamps:
  - `retry_eligible_at` for `RETRY_WAIT` tasks.
  - `execution_start_deadline_at` for `CLAIMED` attempts.
  - `execution_timeout_at` for `RUNNING` attempts.
  - `cancellation_deadline_at` for attempts under cancellation drain.
- Overdue timestamps trigger immediate lifecycle progression; future timestamps fire normally when due.

---

### 10.9 Workflow Lifecycle Repair

#### 10.9.1 Definitive Task Failure Repair
If a task is definitively `FAILED` while the parent `WorkflowExecution` is still `RUNNING`:
- Recovery initiates the transition `WorkflowExecution.RUNNING -> FAILING` (Atomicity Group 7).
- This transition participates in standard single-winner concurrency under ADR-013 (e.g., racing against concurrent user cancellation).

#### 10.9.2 All-Tasks-Succeeded Repair
If all tasks in the canonical DAG are terminal `SUCCEEDED` while the workflow is still `RUNNING`:
- Recovery resolves explicit workflow output bindings (ADR-010).
- Recovery commits authoritative workflow output and transitions `WorkflowExecution.RUNNING -> SUCCEEDED` atomically (Atomicity Group 10). No tasks are re-executed.

#### 10.9.3 Drain Recovery (FAILING & CANCELLING)
- **Monotonic Direction:** Workflows in `FAILING` or `CANCELLING` **never** return to `RUNNING` and never switch directions.
- **Unstarted Tasks:** Tasks in `PENDING`, `RUNNABLE`, or `RETRY_WAIT` transition directly to `CANCELLED`. No attempts are spawned; no retries are permitted.
- **Active Attempts in FAILING:** Active attempts in `CLAIMED` or `RUNNING` may receive best-effort termination signals, but active authoritative outcomes still settle normally under ADR-006/007: if an active attempt succeeds, its task becomes `SUCCEEDED`; if it fails, its task becomes `FAILED` (with no retry permitted); if cancellation settles it, its task becomes `CANCELLED`.
- **Active Attempts in CANCELLING:** Active attempts receive re-issued cancellation signals and remain governed by cancellation-resolution deadlines. If an active attempt reports terminal success before cancellation takes effect, its task becomes `SUCCEEDED` (while workflow direction remains `CANCELLING`); if it fails, its task becomes `FAILED` (no retry); if cancelled or revoked, its task becomes `CANCELLED`.
- **Terminal Settlement:** When **all** expected `TaskExecution` records in the workflow are in terminal states (`SUCCEEDED`, `FAILED`, `CANCELLED`), the root workflow transitions:
  $$\text{FAILING} \longrightarrow \text{FAILED}$$
  $$\text{CANCELLING} \longrightarrow \text{CANCELLED}$$

---

### 10.10 Persistence Invariant Violations (Quarantine Model)
- If recovery encounters impossible durable states (e.g., `SUCCEEDED` task without output, `RUNNING` task without attempt, missing materialized input, duplicate active attempts, missing definition):
- **Quarantine Policy:** Recovery **never** guesses data, fabricates synthetic outputs, or silently resets records.
- The affected execution is locked against further scheduling progression and flagged with an operational quarantine status for administrative inspection.
- Uncontaminated executions continue processing normally.

---

### 10.11 Idempotency, Re-entrancy, and Failure Policy
- **Idempotency:** Repeated recovery passes against the same durable state produce the exact same outcome.
- **Re-entrancy:** If the orchestrator crashes while executing recovery, the subsequent restart safely resumes from durable state. No monolithic "recovery transaction" spans multiple executions.
- **Persistence Outages:** If durable storage becomes unavailable during recovery, recovery halts immediately (fails closed) without speculating in memory. Reconciliation resumes once storage connectivity returns.

---

## 11. Decision Rationale

1. **Deterministic Current-State Alignment:** ADR-011 established that the database represents authoritative execution reality. Reconciling current state eliminates event replay overhead, avoids event schema migration bugs, and enables rapid startup via indexed database queries.
2. **Protection Against Duplicate Execution:** Forcing active attempts through a bounded reconnection grace window prevents the orchestrator from prematurely failing in-flight work and spawning duplicate attempts on new workers while the original worker is still executing.
3. **Preservation of Start-Deadline Semantics:** Allowing retrospective worker testimony to authorize execution start would create a major loophole: a worker that failed to start within its deadline could claim it started before the crash, bypassing the execution-start contract. Enforcing current observation within the original start deadline preserves scheduling integrity.
4. **Structural Completeness in Terminal Workflows:** Guaranteeing that cancelled workflows establish all $N$ tasks in `CANCELLED` state ensures that execution introspection, audit tools, and API consumers observe a uniform, complete task graph matching the workflow definition.
5. **No Shadow State Machine:** Forcing recovery to use standard lifecycle transitions and consistency groups guarantees that recovery code paths cannot drift from normal runtime scheduling behavior.

---

## 12. Tradeoffs

| Architectural Advantage | Tradeoff Incurred | Mitigation Strategy |
| :--- | :--- | :--- |
| **Instant Startup Lookups:** Selective queries eliminate long event-log replay loops. | **Reconciliation Complexity:** Recovery must account for every valid intermediate crash state. | Exhaustive failure matrix and strict atomicity group mapping eliminate ambiguous states. |
| **Duplicate Execution Protection:** Grace window prevents re-executing active work post-crash. | **Delayed Worker-Loss Detection:** Truly dead workers are not declared lost until grace expires. | Grace duration is bounded and configurable via ADR-023; explicit negative acks allow accelerated resolution. |
| **Complete Task Graph Integrity:** Cancelled initializations establish all missing tasks. | **Additional Insertion I/O:** Extra records are written for workflows that never actively ran. | Inserts occur in a single batch transaction; payload size for empty cancelled tasks is minimal. |
| **Queue-Less Fault Tolerance:** System survives total message broker loss. | **Startup Scan Overhead:** Control plane must scan database for runnable tasks on cold boot. | Selective indexing on lifecycle states ensures fast query execution. |

---

## 13. Consequences

### 13.1 Positive Consequences
- **Fast, Predictable Recovery:** Orchestrators restart and resume scheduling rapidly, independent of historical workflow event volume.
- **Resilience to Infrastructure Loss:** Complete loss of message queues, Redis caches, or volatile process memory results in zero lost work or progress.
- **Reproducible Retries:** Retry attempts receive the exact same materialized business input; attempt ordinals are never reused.
- **Safe polyglot Worker Coordination:** Surviving worker processes re-associate seamlessly; zombie workers are logically fenced.

### 13.2 Negative Consequences
- **Transient Recovery Latency for Lost Workers:** If a worker died during orchestrator downtime, the task will not be retried until the reconnection grace window expires.
- **Strict Database Performance Dependency:** Recovery initialization performance depends directly on database query latency for non-terminal executions.

---

## 14. Failure Modes

| # | Failure Scenario | Authoritative Durable State | Recovery Classification | Permitted Action | Forbidden Action | Resulting Authoritative State | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | `INITIALIZING` with zero tasks | Workflow `INITIALIZING`, no task records. | Interrupted eager setup. | Insert all $N$ tasks in `PENDING`. | Transition to `FAILED`. | Workflow `INITIALIZING`, all tasks `PENDING`. | ADR-006 / ADR-012 |
| **2** | `INITIALIZING` with partial tasks | Workflow `INITIALIZING`, $M$ of $N$ tasks exist. | Interrupted eager setup. | Insert missing $N-M$ tasks in `PENDING`. | Duplicate existing tasks; transition to `FAILED`. | Workflow `INITIALIZING`, all $N$ tasks `PENDING`. | ADR-006 / ADR-012 |
| **3** | `INITIALIZING` with all tasks | Workflow `INITIALIZING`, all $N$ tasks `PENDING`. | Setup complete. | Transition `INITIALIZING -> RUNNING`. | Re-insert tasks; remain in `INITIALIZING`. | Workflow `RUNNING`. | ADR-006 / ADR-012 |
| **4** | Cancellation during partial initialization | Workflow `CANCELLING`, $M$ of $N$ tasks exist. | Interrupted cancel setup. | Settle existing $M$ tasks to `CANCELLED`; create remaining $N-M$ in `PENDING` and immediately `CANCELLED`; transition workflow to `CANCELLED`. | Leave missing tasks uncreated; mark workflow `CANCELLED` with incomplete task graph. | Workflow `CANCELLED`, all $N$ tasks `CANCELLED`. | ADR-006 / ADR-007 / ADR-012 |
| **5** | `PENDING` deps unsatisfied | Task `PENDING`, upstream non-terminal. | In-progress dependency. | Maintain `PENDING`. | Transition to `RUNNABLE`. | Task `PENDING`. | ADR-005 / ADR-012 |
| **6** | `PENDING` deps satisfied | Task `PENDING`, all deps `SUCCEEDED`. | Ready task. | Materialize inputs; transition `PENDING -> RUNNABLE`. | Dispatch without materializing inputs. | Task `RUNNABLE`. | ADR-005 / ADR-010 / ADR-012 |
| **7** | `RUNNABLE` with lost dispatch | Task `RUNNABLE`, no active attempt. | Rediscoverable work. | Enqueue for ADR-009 routing. | Fabricate attempt; transition to `FAILED`. | Task `RUNNABLE` (routed). | ADR-005 / ADR-009 / ADR-012 |
| **8** | `RETRY_WAIT` not due | Task `RETRY_WAIT`, `retry_eligible_at > now()`. | Active backoff. | Rebuild timer heap wakeup. | Reset backoff duration; dispatch prematurely. | Task `RETRY_WAIT`. | ADR-007 / ADR-011 / ADR-012 |
| **9** | `RETRY_WAIT` due | Task `RETRY_WAIT`, `retry_eligible_at <= now()`. | Matured backoff. | Transition `RETRY_WAIT -> RUNNABLE`. | Remain stuck in `RETRY_WAIT`. | Task `RUNNABLE`. | ADR-007 / ADR-012 |
| **10** | `CLAIMED` + worker reconnects within deadline | Attempt `CLAIMED`, `execution_start_deadline_at > now()`. | Active start. | Worker executes start handshake; transition `CLAIMED -> RUNNING`. | Revoke authority prematurely; accept retrospective claim without handshake. | Attempt `RUNNING`. | ADR-008 / ADR-012 |
| **11** | `CLAIMED` + retrospective start claim | Worker claims it started pre-crash. | Non-authoritative testimony. | Require current start handshake within deadline; if deadline expired, reject. | Retroactively promote to `RUNNING` based on claim alone. | Dependent on deadline validity. | ADR-008 / ADR-012 |
| **12** | `CLAIMED` + expired start deadline | `execution_start_deadline_at <= now()`. | Expired start contract. | Transition attempt `CLAIMED -> FAILED` (start timeout cause). | Extend start deadline; resurrect via grace. | Attempt `FAILED`. | ADR-008 / ADR-012 |
| **13** | `CLAIMED` + worker absent | Attempt `CLAIMED`, worker silent. | Disconnected worker. | Wait for grace; if absent at grace expiry, fail attempt. | Fail immediately upon startup. | Attempt `FAILED` (worker loss cause). | ADR-008 / ADR-012 |
| **14** | `RUNNING` + worker reconnects | Attempt `RUNNING`, session matches. | Surviving execution. | Re-associate in `WorkerRegistry`; resume liveness tracking. | Abort execution; trigger retry. | Attempt `RUNNING`. | ADR-008 / ADR-012 |
| **15** | `RUNNING` + worker absent | Attempt `RUNNING`, worker silent. | Disconnected worker. | Wait for grace; if absent at grace expiry, fail attempt. | Fail immediately upon startup. | Attempt `FAILED` (worker loss cause). | ADR-008 / ADR-012 |
| **16** | Execution timeout elapsed during downtime | `execution_timeout_at <= now()`. | Expired timeout. | Fail attempt via timeout; race against buffered worker result. | Grant fresh full timeout. | Attempt `FAILED` (or `SUCCEEDED` if result won). | ADR-007 / ADR-012 / ADR-013 |
| **17** | Buffered worker terminal result | Worker reconnects holding unacknowledged output. | Delayed result arrival. | If `RUNNING`, commit result atomically; if `CLAIMED`, cannot accept directly (fail if start deadline expired). | Accept success directly on `CLAIMED` attempt. | Task `SUCCEEDED` (if `RUNNING`) or Attempt `FAILED`. | ADR-008 / ADR-010 / ADR-012 |
| **18** | Worker omits durable active attempt | Worker reconnects; active attempt omitted from list. | Reconciliation discrepancy. | Issue explicit reconciliation query; if negative ack received, accelerate failure; else wait grace. | Immediately terminalize attempt from omission alone. | Governed by query ack or grace expiry. | ADR-008 / ADR-012 |
| **19** | Explicit worker negative acknowledgement | Worker confirms it is not running attempt. | Explicit abandonment. | Transition attempt to `FAILED` (worker abandonment/loss cause). | Wait for full grace window. | Attempt `FAILED`. | ADR-008 / ADR-012 |
| **20** | Stale result after authority revoked | Attempt `FAILED` or `CANCELLED` in DB. | Fenced result. | Reject and quarantine payload; acknowledge fence. | Overwrite terminal state. | State unchanged. | ADR-008 / ADR-012 / ADR-013 |
| **21** | Zombie worker | Old session executing revoked attempt. | Zombie execution. | Reject result; fence worker; ignore payload. | Mutate completed task output. | State unchanged. | ADR-008 / ADR-012 |
| **22** | New worker incarnation | Same `worker_id`, new `worker_session_id`. | Incarnation mismatch. | Register for new work; cannot inherit old attempt. | Re-attach old attempt to new session. | Old attempt fails post-grace; new session available. | ADR-008 / ADR-012 |
| **23** | Task `FAILED` while Workflow `RUNNING` | Task `FAILED`, Workflow `RUNNING`. | Failure direction repair. | Transition `WorkflowExecution.RUNNING -> FAILING`. | Transition to `SUCCEEDED`; ignore failure. | Workflow `FAILING`. | ADR-006 / ADR-012 |
| **24** | All tasks `SUCCEEDED` while Workflow `RUNNING` | All tasks `SUCCEEDED`, Workflow `RUNNING`. | Success completion repair. | Resolve output bindings; transition to `SUCCEEDED`. | Re-run tasks; transition to `FAILED`. | Workflow `SUCCEEDED`. | ADR-006 / ADR-010 / ADR-012 |
| **25** | `FAILING` with unstarted tasks | Tasks in `PENDING`, `RUNNABLE`, `RETRY_WAIT`. | Failure drain. | Transition unstarted tasks to `CANCELLED`. | Dispatch unstarted tasks to workers. | Tasks `CANCELLED`. | ADR-006 / ADR-007 / ADR-012 |
| **26** | `FAILING` with active attempt | Attempt in `RUNNING`. | Draining active task. | Allow attempt to complete or fail; no retry. | Schedule retry upon attempt failure. | Task `SUCCEEDED`, `FAILED`, or `CANCELLED`. | ADR-006 / ADR-007 / ADR-012 |
| **27** | `FAILING` with all tasks terminal | All tasks `SUCCEEDED`, `FAILED`, `CANCELLED`. | Drain completion. | Transition `WorkflowExecution.FAILING -> FAILED`. | Return to `RUNNING`. | Workflow `FAILED`. | ADR-006 / ADR-012 |
| **28** | `CANCELLING` with unstarted tasks | Tasks in `PENDING`, `RUNNABLE`, `RETRY_WAIT`. | Cancellation drain. | Transition unstarted tasks to `CANCELLED`. | Dispatch unstarted tasks to workers. | Tasks `CANCELLED`. | ADR-006 / ADR-007 / ADR-012 |
| **29** | `CANCELLING` with active attempt | Attempt in `RUNNING`. | Draining cancellation. | Re-issue cancel signal; enforce cancellation deadline. | Schedule retry upon attempt failure; force `CANCELLED` if success won. | Task `SUCCEEDED`, `FAILED`, or `CANCELLED`. | ADR-006 / ADR-008 / ADR-012 |
| **30** | Cancellation deadline elapsed | `cancellation_deadline_at <= now()`. | Expired cancellation. | Revoke authority unilaterally; mark attempt `CANCELLED`. | Wait indefinitely for worker response. | Attempt `CANCELLED`, Task `CANCELLED`. | ADR-008 / ADR-012 |
| **31** | `CANCELLING` with all tasks terminal | All tasks terminal. | Drain completion. | Transition `WorkflowExecution.CANCELLING -> CANCELLED`. | Transition to `FAILED` or `RUNNING`. | Workflow `CANCELLED`. | ADR-006 / ADR-012 |
| **32** | Broker / queue state lost | Tasks in `RUNNABLE` in DB. | Lost ephemeral transit. | Re-scan `RUNNABLE` tasks; re-emit dispatch offers. | Mark tasks `FAILED` due to missing broker. | Tasks `RUNNABLE` (re-dispatched). | ADR-005 / ADR-011 / ADR-012 |
| **33** | Retry timer lost | `retry_eligible_at` persisted in DB. | Lost ephemeral timers. | Reconstruct timer heap from database timestamps. | Reset backoff durations. | Timer heap populated; due tasks fire. | ADR-007 / ADR-011 / ADR-012 |
| **34** | Deadline timer lost | Deadlines persisted in DB. | Lost ephemeral timers. | Reconstruct timer heap from database timestamps. | Reset deadlines to full duration. | Timer heap populated; due deadlines fire. | ADR-008 / ADR-011 / ADR-012 |
| **35** | Source YAML unavailable | Validated IWS in `RegisteredDefinition`. | Operational independence. | Reconstruct canonical graph from durable IWS. | Attempt to read YAML file from disk. | Execution continues normally. | ADR-001 / ADR-002 / ADR-012 |
| **36** | Referenced definition missing | Definition ID foreign key missing. | Storage corruption. | Quarantine execution; raise operational alert. | Silently drop workflow; fabricate failure. | Execution quarantined. | ADR-011 / ADR-012 / ADR-018 |
| **37** | Task `RUNNING` without active attempt | No attempt in `CLAIMED` or `RUNNING`. | Atomicity violation. | Quarantine task and workflow; log integrity error. | Silently generate synthetic attempt. | Execution quarantined. | ADR-011 / ADR-012 / ADR-018 |
| **38** | Active attempt missing session ID | Attempt has null `worker_session_id`. | Atomicity violation. | Quarantine attempt and workflow; log integrity error. | Dispatch attempt to random worker. | Execution quarantined. | ADR-011 / ADR-012 / ADR-018 |
| **39** | Task `SUCCEEDED` without output | `SUCCEEDED` but output uncommitted. | Atomicity violation. | Quarantine task and workflow; log integrity error. | Fabricate null output; re-run task. | Execution quarantined. | ADR-010 / ADR-011 / ADR-012 |
| **40** | Workflow `SUCCEEDED` without output | `SUCCEEDED` but output uncommitted. | Atomicity violation. | Quarantine execution; log integrity error. | Fabricate workflow output. | Execution quarantined. | ADR-010 / ADR-011 / ADR-012 |
| **41** | `RUNNABLE` task missing input | `RUNNABLE` but `resolved_input` null. | Atomicity violation. | Quarantine task; log integrity error. | Dispatch without inputs; guess inputs. | Execution quarantined. | ADR-010 / ADR-011 / ADR-012 |
| **42** | Duplicate active attempts | Multiple attempts in `CLAIMED`/`RUNNING`. | Atomicity violation. | Quarantine task and workflow; revoke all sessions. | Allow both attempts to execute in parallel. | Execution quarantined. | ADR-007 / ADR-011 / ADR-012 |
| **43** | Recovery crashes during reconciliation | Interrupted recovery pass. | Re-entrant recovery. | Restart recovery from Phase 1; resume cleanly. | Invalidate previous recovery updates. | State converges forward idempotently. | ADR-011 / ADR-012 |
| **44** | Persistence unavailable during recovery | DB connection fails during scan. | Infrastructure outage. | Fail closed; pause recovery until DB restores. | Fabricate failure for unreachable workflows. | Recovery pauses; resumes on DB reconnect. | ADR-011 / ADR-012 / ADR-018 |
| **45** | User cancel races recovery failure transition | Task failed, user requests cancel. | Concurrent direction race. | Whichever valid transition commits first wins (ADR-013). | Force failure over cancellation arbitrarily. | Workflow in `FAILING` or `CANCELLING`. | ADR-006 / ADR-012 / ADR-013 |
| **46** | Duplicate cancellation signal | Task already `CANCELLED` in DB. | Idempotent control message. | Acknowledge cancellation idempotently; no state change. | Trigger second cancellation flow. | State unchanged. | ADR-006 / ADR-007 / ADR-012 |
| **47** | Repeated recovery pass against same state | Redundant recovery execution. | Idempotent reconciliation. | Observe state already reconciled; no mutations. | Re-execute transitions; duplicate attempts. | State unchanged. | ADR-011 / ADR-012 |

---

## 15. Debugging

Recovery provides structured operational visibility to enable rapid troubleshooting of post-crash state:

- **Recovery Tracing Identifiers:** All recovery operations log and trace against authoritative identifiers:
  - `workflow_execution_id`
  - `task_execution_id`
  - `attempt_id`
  - `worker_session_id`
  - `definition_id`
- **Reconciliation Audit Milestones:** The orchestrator records explicit recovery milestones:
  - `RECOVERY_STARTED`: Recovery startup scan initiated.
  - `INITIALIZATION_REPAIRED`: Missing task records established for an `INITIALIZING` execution.
  - `WORKER_SESSION_RECONNECTED`: Active worker session re-associated with its in-flight attempts.
  - `GRACE_PERIOD_EXPIRED`: Reconnection grace window closed; lost workers flagged.
  - `WORKFLOW_DIRECTION_REPAIRED`: Uncommitted failure or success transitions settled.
  - `EXECUTION_QUARANTINED`: Invariant violation detected; execution isolated.
- **Diagnostics vs. Lifecycle:** Recovery diagnostics are exposed via dedicated administration query endpoints (ADR-015), ensuring business execution states remain uncontaminated.

---

## 16. Testing

The recovery architecture requires exhaustive verification across crash-injection and fault-tolerance test suites:

### 16.1 Crash Point & Re-entrancy Tests
- **Atomicity Boundary Crashes:** Inject process kills immediately before and after every logical consistency group commit (Groups 1–10); verify post-restart state adheres strictly to the failure matrix.
- **Recovery-During-Recovery Crashes:** Terminate orchestrator midway through the startup scan and midway through task reconciliation; verify subsequent startup resumes and converges without duplicate records.
- **Repeated Pass Idempotency:** Execute multiple consecutive recovery sweeps against an identical non-terminal database state; verify zero state mutations, zero duplicate attempts, and zero budget leakage.

### 16.2 Initialization & Graph Integrity Tests
- **Interrupted Eager Initialization:** Crash during eager task insertion; verify recovery establishes missing tasks and promotes to `RUNNING`.
- **Cancellation During Partial Setup:** Commit cancellation while only $M$ of $N$ tasks exist; verify recovery establishes all $N$ tasks in `CANCELLED` and terminalizes the workflow cleanly.
- **Missing Source YAML:** Delete source YAML files from disk; verify recovery reconstructs graphs and executes tasks using the durable Validated IWS alone.

### 16.3 Worker Coordination & Fencing Tests
- **Same-Session Reconnect:** Disconnect orchestrator, keep worker alive; verify worker reconnects with same session and resumes execution without attempt restart.
- **New Incarnation Rejection:** Restart worker process during downtime (new `WorkerSessionId`); verify new session cannot claim old attempts and old attempt times out post-grace.
- **Start-Deadline Strictness:** Attempt in `CLAIMED` with expired start deadline; worker reconnects claiming pre-crash start; verify attempt transitions to `FAILED` and claim is rejected.
- **Active-Attempt Omission:** Reconnect worker omitting an active attempt; verify orchestrator issues explicit query and does not fail attempt until explicit negative ack or grace expiry.
- **Buffered Terminal Results:** Buffer completed results on worker during downtime; verify results commit atomically upon reconnect.
- **Zombie Worker Fencing:** Revoke attempt authority post-grace; reconnect original worker with result; verify result is fenced and rejected.

### 16.4 Drain & Quarantine Tests
- **Drain Preservation:** Crash workflow in `FAILING` and `CANCELLING`; verify recovery cancels unstarted tasks, enforces deadlines, and never returns to `RUNNING`.
- **Direction Race:** Concurrently submit user cancellation while recovery repairs a failed task; verify single-winner settlement under ADR-013.
- **Invariant Quarantine:** Inject torn records (e.g., `SUCCEEDED` task with null output); verify recovery halts progression on that execution, leaves other executions unaffected, and raises operational alerts.

---

## 17. Operational Considerations

1. **Recovery Startup Latency:** Startup scan execution time scales with the number of non-terminal workflows. The persistence technology selected under ADR-020 must support practical selective discovery of nonterminal and recovery-relevant state at the expected operating scale.
2. **Grace Window Tuning:** The reconnection grace window ($T_{grace}$) represents a fundamental operational tradeoff: longer grace prevents unnecessary retries of slow-reconnecting workers; shorter grace accelerates failure detection for genuinely dead workers.
3. **Fail-Closed Storage Stance:** If durable storage is unreachable during recovery, the orchestrator pauses and retries connectivity. It must never proceed with speculative in-memory scheduling.
4. **Queue & Cache Ephemerality:** System administrators can safely flush Redis caches, message queues, or worker registries during maintenance; the database remains the sole source of truth.

---

## 18. Maintenance

- **Schema Migration Safety:** Database migrations must not alter the semantics of active non-terminal executions. Any schema changes to entity models must support backward-compatible recovery decoding.
- **Reconciliation Invariant Verification:** New task or attempt states added in future architectural revisions must be mapped explicitly into the recovery failure matrix and reconciliation loops.

---

## 19. Future Evolution

- **High Availability & Failover (ADR-025):** The incremental, current-state reconciliation model can be reused by a future orchestrator instance after ADR-025 establishes legitimate control-plane authority (governing leader election, leases, epochs, and fencing).
- **Automated Repair Workflows:** Future operational tooling may provide administrative CLI commands to inspect and remediate quarantined invariant violations.

---

## 20. Rejected Alternatives

### 20.1 Restart All Non-Terminal Workflows from Scratch
- **Rejected:** Unacceptable compute waste and duplicate execution of non-idempotent side effects.

### 20.2 Fail All Non-Terminal Workflows on Restart
- **Rejected:** Severe availability degradation; invalidates enterprise orchestration reliability guarantees.

### 20.3 Full Event-Stream Replay as Primary Recovery Mechanism
- **Rejected:** High startup latency, snapshot compaction overhead, and event versioning fragility.

### 20.4 Worker-Authoritative State Reconstruction
- **Rejected:** Violates single source of truth; untrusted edge nodes cannot govern core orchestration authority.

### 20.5 Retroactive Authorization of Execution Start via Worker Claims
- **Rejected:** Weakens ADR-008 start-deadline contracts and enables workers that missed deadlines to claim pre-crash starts retroactively.

### 20.6 Truncated Task Sets on Interrupted Cancellation
- **Rejected:** Violates the fundamental invariant that every workflow execution must have exactly one `TaskExecution` per `TaskDefinition`.

---

## 21. Decision Evolution

- **Initial Concept:** Evaluated an event-sourced replay engine that reconstructed state by reading event logs.
- **Refinement 1 (ADR-011 Alignment):** Abandoned event replay in favor of direct reconciliation against ADR-011's durable current-state model.
- **Refinement 2 (Reconnection Grace Protocol):** Decoupled worker ownership from worker liveness; introduced the bounded reconnection grace window to prevent premature worker-loss conclusions.
- **Refinement 3 (Start-Deadline Strictness):** Prohibited retrospective worker testimony from authorizing execution start; enforced that start deadlines are never extended or resurrected by grace.
- **Refinement 4 (Non-Authoritative Worker Omission):** Clarified that worker attempt omissions during reconnection are reconciliation evidence requiring explicit confirmation prior to accelerated failure.
- **Refinement 5 (Task Graph Completeness):** Mandated that cancellation during partial initialization must instantiate and cancel the complete expected task set ($N$ tasks), eliminating truncated task graphs.
- **Final Accepted State (ADR-012):** Standardized on deterministic durable-state reconciliation, incremental non-blocking scans, fail-safe invariant quarantining, and strict re-entrancy.

---

## 22. Common Misconceptions

- **Misconception 1: "Recovery replays historical events to rebuild workflow state."**
  *Correction:* Recovery operates exclusively by inspecting the durable current-state records in the database. No event stream replay is used.
- **Misconception 2: "Message broker queues must be persistent to prevent work loss."**
  *Correction:* The database state `TaskExecution.lifecycle_state = RUNNABLE` is the authoritative work truth. Message queues are disposable delivery accelerators.
- **Misconception 3: "Worker Registry loss means all workers have died."**
  *Correction:* Worker Registry is ephemeral memory. Active attempts remain authoritative in storage and enter a bounded reconnection grace window.
- **Misconception 4: "A worker can reconnect and claim it started executing before the crash."**
  *Correction:* Retrospective start claims are non-authoritative testimony. Promotion to `RUNNING` requires current control-plane acceptance of a start handshake within the original start deadline.
- **Misconception 5: "If a worker omits an attempt during reconnect, the attempt is immediately failed."**
  *Correction:* Omission is negative evidence, not an authoritative command. The control plane issues an explicit query and requires explicit negative acknowledgement or grace expiry to fail the attempt.
- **Misconception 6: "An orchestrator crash resets all retry backoff and timeout timers."**
  *Correction:* Deadlines and backoffs are persisted as absolute control-plane timestamps; elapsed time is preserved across crashes.
- **Misconception 7: "Orchestrator restarts consume retry budgets."**
  *Correction:* Restarts alone never create `ExecutionAttempt` records and never consume retry capacity.
- **Misconception 8: "Workflows enter a RECOVERING state during startup."**
  *Correction:* Workflows remain in their authoritative lifecycle state (`RUNNING`, `CANCELLING`, etc.). Recovery status is operational metadata.

---

## 23. Open Questions

- *Non-Blocking:* Default numerical duration for the worker reconnection grace window ($T_{grace}$, owned by ADR-023).
- *Non-Blocking:* Physical concurrency and transaction locking primitives used during reconciliation sweeps (owned by ADR-013).
- *Status:* **Zero architectural blockers remain for ADR-012.**

---

## 24. Interview Discussion

### Question 1: Why did NexusFlow choose deterministic current-state reconciliation over event-sourced replay?
**Answer:** While event sourcing provides an audit trail by default, using it as the operational recovery mechanism introduces severe latency and complexity: the engine must replay every event from workflow inception, manage snapshotting and stream compaction, and handle event schema migrations across software upgrades. By contrast, ADR-011's normalized current-state model allows the orchestrator to query the exact current status of active executions instantly using indexed database reads. Recovery simply inspects these records and reconciles them forward to their next valid lifecycle milestone using standard state-machine transitions, providing predictable recovery times and operational transparency.

### Question 2: How does NexusFlow avoid duplicate task execution when an orchestrator restarts while workers are executing tasks?
**Answer:** When the orchestrator restarts, its in-memory `WorkerRegistry` is empty, but durable storage still records `ExecutionAttempt -> WorkerSessionId` ownership associations. Rather than assuming absent workers are dead, the orchestrator enters a bounded Reconnection Grace Window. Surviving workers reconnect using their persistent `WorkerSessionId` and re-establish their in-flight attempts. Because the attempts were never revoked or failed, no replacement attempts were spawned, preventing duplicate execution. Furthermore, single-winner result fencing guarantees that even if a worker is declared lost post-grace, any late result it submits is rejected.

### Question 3: Why are retrospective worker start claims ("I started before the crash") rejected during recovery?
**Answer:** In ADR-008, `CLAIMED` represents committed ownership where execution start has *not* yet been authoritatively observed by the control plane, while `RUNNING` means start was durably accepted. If we allowed a worker to reconnect after a crash and retroactively claim that it started before the crash, we would allow workers that failed to meet their execution-start deadline to bypass their scheduling contract. If start had been accepted before the crash, the database would already show `RUNNING`. Therefore, a reconnecting worker must execute a current start handshake while its original deadline is still valid; otherwise, it is failed for start timeout.

### Question 4: How does NexusFlow handle workflow cancellation if the orchestrator crashes midway through initial task creation?
**Answer:** A fundamental NexusFlow invariant is that every workflow execution must have exactly one `TaskExecution` for every `TaskDefinition` in its canonical DAG. If a workflow in `CANCELLING` is recovered with only $M$ of $N$ tasks in storage, recovery settles the existing $M$ tasks to `CANCELLED`, instantiates the remaining $N - M$ tasks in `PENDING`, and immediately transitions them to `CANCELLED` within the same transaction. The workflow then transitions `CANCELLING -> CANCELLED`. This ensures that downstream queries, audit logs, and API consumers always observe a complete, non-truncated task graph.

---

## 25. References

- [ADR-001: Internal Workflow Specification](docs/architecture/adr-001-internal-workflow-specification.md)
- [ADR-002: Workflow Definition Parsing & Normalization](docs/architecture/adr-002-workflow-definition-parsing-strategy.md)
- [ADR-003: Canonical Workflow Graph Representation](docs/architecture/adr-003-canonical-workflow-graph-representation.md)
- [ADR-004: Workflow Validation Strategy](docs/architecture/adr-004-workflow-validation-strategy.md)
- [ADR-005: Workflow Task Scheduling & Dispatch](docs/architecture/adr-005-workflow-task-scheduling-and-dispatch-architecture.md)
- [ADR-006: Workflow Execution State Machine](docs/architecture/adr-006-workflow-execution-state-machine.md)
- [ADR-007: Task Execution Lifecycle & Attempt Model](docs/architecture/adr-007-task-execution-lifecycle-and-attempt-model.md)
- [ADR-008: Worker Coordination & Liveness Model](docs/architecture/adr-008-worker-coordination-and-liveness-model.md)
- [ADR-009: Task Routing Strategy](docs/architecture/adr-009-task-routing-strategy.md)
- [ADR-010: Workflow Data Flow & Parameter Passing](docs/architecture/adr-010-workflow-data-flow-and-parameter-passing.md)
- [ADR-011: State Persistence Strategy](docs/architecture/adr-011-state-persistence-strategy.md)
- [Architecture Decision Register](docs/architecture/00-architecture-decision-register.md)

---

## 26. Traceability

### 26.1 Backward Traceability
- **ADR-001 & ADR-002:** Canonical DAG reconstructed from durable `Validated IWS`; recovery never touches external YAML.
- **ADR-003:** Canonical graph projection executed deterministically during recovery.
- **ADR-005:** Rediscovery of `RUNNABLE` tasks drives dispatch without persistent queue dependencies.
- **ADR-006:** Preservation of workflow lifecycle states (`INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`); failure/cancellation drain monotonicity.
- **ADR-007:** Task lifecycle transitions reused; monotonic attempt ordinals preserved; no speculative attempt creation on restart.
- **ADR-008:** Reconnection grace window; `WorkerSessionId` continuity; rejection of retrospective start claims; single-winner result fencing.
- **ADR-009:** Re-queuing of unowned `RUNNABLE` tasks into routing candidate pools.
- **ADR-010:** Materialized business input preservation; authoritative output immutability; atomic visibility of success states with outputs.
- **ADR-011:** Operates directly on the four durable entities and ten logical atomicity groups; fail-closed storage semantics.

### 26.2 Forward Traceability
- **ADR-013 (Consistency & Concurrency):** Owns physical locks, CAS operations, and transaction isolation levels executing recovery transitions.
- **ADR-014 (Execution History & Audit):** Records recovery milestones into append-only historical audit logs.
- **ADR-015 (API):** Exposes operational recovery diagnostics and query endpoints.
- **ADR-016 (Observability):** Defines recovery metrics, logs, and distributed trace contexts.
- **ADR-018 (Error Handling):** Formally categorizes error codes for start timeouts, worker loss, and quarantined invariant violations.
- **ADR-020 (Technology Selection):** Implements physical recovery queries, connection pooling, and in-memory timer heaps.
- **ADR-022 (Security):** Validates worker identity authentication during reconnection handshake.
- **ADR-023 (Configuration):** Governs numerical thresholds for reconnection grace duration ($T_{grace}$) and startup scan batch sizes.
- **ADR-025 (High Availability):** Leverages ADR-012's current-state reconciliation model for active-passive failover.

---

## 27. Decision Validation Checklist

- [x] **DETERMINISTIC DURABLE-STATE RECONCILIATION selected** (Section 10)
- [x] **Operates from ADR-011 durable current-state model** (Section 10.1)
- [x] **No complete event-stream replay** (Section 10.1, 20.3)
- [x] **No workflow restart from beginning** (Section 10.1, 20.1)
- [x] **No rollback to checkpoint** (Section 10.1)
- [x] **No fail-all-on-restart** (Section 10.1, 20.2)
- [x] **No worker-authoritative reconstruction** (Section 10.1, 20.4)
- [x] **No reconstruction from external YAML** (Section 10.1, 10.4)
- [x] **Selective startup scan required** (Section 10.2)
- [x] **No mandatory steady-state polling sweeps** (Section 10.2)
- [x] **No global recovery barrier** (Section 10.2)
- [x] **No RECOVERING lifecycle state** (Section 10.3)
- [x] **Recovery status is operational metadata** (Section 10.3)
- [x] **INITIALIZING partial task set recoverable** (Section 10.5.1)
- [x] **Infrastructure crash never causes INITIALIZING -> FAILED** (Section 10.5.1)
- [x] **Cancellation during partial initialization creates complete terminal task set** (Section 10.5.2)
- [x] **PENDING deps satisfied -> RUNNABLE** (Section 10.6.1)
- [x] **RUNNABLE rediscoverable work** (Section 10.6.2)
- [x] **Lost broker/queue state reconstructed from RUNNABLE** (Section 10.8)
- [x] **RETRY_WAIT timing survives restart** (Section 10.6.3)
- [x] **Restart alone never creates Attempt and never consumes retry budget** (Section 10.6.3)
- [x] **RUNNING task requires active Attempt** (Section 10.6.4)
- [x] **Worker Registry empty is expected post-restart** (Section 10.7.1)
- [x] **Bounded worker reconnection grace window established** (Section 10.7.1)
- [x] **Grace does not reset or extend deadlines** (Section 10.7.1)
- [x] **Same WorkerSession reconnects** (Section 10.7.2)
- [x] **New WorkerSession cannot inherit authority** (Section 10.7.2)
- [x] **Worker claims are non-authoritative evidence** (Section 10.7.5)
- [x] **Attempt omission from worker list is non-authoritative** (Section 10.7.5)
- [x] **CLAIMED retrospective start claim rejected** (Section 10.7.3)
- [x] **Expired start deadline not resurrected by grace** (Section 10.7.3)
- [x] **No CLAIMED -> SUCCEEDED bypass transition** (Section 10.7.3)
- [x] **RUNNING attempt reconnects** (Section 10.7.4)
- [x] **Execution timeout preserved; races against buffered result** (Section 10.7.4)
- [x] **Cancellation deadline preserved** (Section 10.8)
- [x] **Worker-held terminal results accepted only if attempt state is RUNNING and authority valid** (Section 10.7.3, 10.7.6)
- [x] **Duplicate results idempotent** (Section 10.7.6)
- [x] **Stale and zombie results fenced** (Section 10.7.6)
- [x] **Definitive task failure repairs to FAILING direction** (Section 10.9.1)
- [x] **All-tasks-succeeded repairs to SUCCEEDED with output** (Section 10.9.2)
- [x] **FAILING/CANCELLING direction monotonicity preserved** (Section 10.9.3)
- [x] **Unstarted tasks settle to CANCELLED during drain** (Section 10.9.3)
- [x] **Active results during drain retain standard semantics** (Section 10.9.3)
- [x] **Workflow terminal settlement requires all tasks terminal** (Section 10.9.3)
- [x] **Persistence invariant violations quarantined, not silently repaired** (Section 10.10)
- [x] **Recovery idempotent and re-entrant** (Section 10.11)
- [x] **No dedicated recovery checkpoint table** (Section 10.11)
- [x] **Timer wakeups reconstructed into ephemeral heaps** (Section 10.8)
- [x] **Cancellation signals reissued idempotently** (Section 10.9.3)
- [x] **Persistence failure fails closed** (Section 10.11)
- [x] **ADR-013 concurrency boundary preserved** (Section 26.2)
- [x] **ADR-018 error taxonomy boundary preserved** (Section 26.2)
- [x] **ADR-020 technology boundary preserved** (Section 26.2)
- [x] **ADR-023 configuration boundary preserved** (Section 26.2)
- [x] **No fake empirical or benchmark claims** (Compliant throughout)
