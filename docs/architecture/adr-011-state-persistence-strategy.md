# ADR-011 — State Persistence Strategy

## 1. Purpose

This Architectural Decision Record (ADR) defines the state persistence strategy and durable orchestration truth model for the NexusFlow orchestration engine. It establishes how workflow definitions, workflow executions, task executions, execution attempts, materialized business inputs, authoritative outputs, worker ownership associations, and restart-safe semantic timing state are durably persisted to survive orchestrator crashes, process restarts, node migrations, and worker disconnects.

Furthermore, this record formalizes the structural distinction between authoritative durable state, derived reconstructible state, and ephemeral operational state. It establishes ten logical atomicity groups governing consistent state transitions and enforces a strict fail-closed persistence policy while keeping physical database selection (ADR-020), recovery algorithms (ADR-012), physical concurrency mechanisms (ADR-013), and long-term history/audit retention (ADR-014) cleanly decoupled.

---

## 2. Context

NexusFlow orchestrates distributed multi-task workflows structured as directed acyclic graphs. The preceding architectural decisions have established the foundations of specification, execution, coordination, and data flow:
- [ADR-001](docs/architecture/adr-001-internal-workflow-specification.md) & [ADR-002](docs/architecture/adr-002-workflow-definition-parsing-strategy.md) established the canonical Internal Workflow Specification (Validated IWS) and dictated that runtime execution and recovery must never re-parse source authoring YAML.
- [ADR-003](docs/architecture/adr-003-canonical-workflow-graph-representation.md) & [ADR-004](docs/architecture/adr-004-workflow-validation-strategy.md) defined the immutable canonical DAG representation and deep two-phase validation.
- [ADR-005](docs/architecture/adr-005-workflow-task-scheduling-and-dispatch-architecture.md) established success-only dependency satisfaction and task dispatch eligibility.
- [ADR-006](docs/architecture/adr-006-workflow-execution-state-machine.md) defined the root `WorkflowExecution` state machine (`INITIALIZING`, `RUNNING`, `CANCELLING`, `FAILING`, `SUCCEEDED`, `FAILED`, `CANCELLED`).
- [ADR-007](docs/architecture/adr-007-task-execution-lifecycle-and-attempt-model.md) decoupled the logical `TaskExecution` lifecycle from the physical, ephemeral `ExecutionAttempt` lifecycle, dictating monotonic attempt ordinals and strict retry isolation.
- [ADR-008](docs/architecture/adr-008-worker-coordination-and-liveness-model.md) defined worker session ownership, worker incarnation tracking, execution-start deadlines, cancellation-resolution deadlines, and result fencing.
- [ADR-009](docs/architecture/adr-009-task-routing-strategy.md) established two-stage routing where routing decisions are non-authoritative and decoupled from attempt creation.
- [ADR-010](docs/architecture/adr-010-workflow-data-flow-and-parameter-passing.md) established the JSON-compatible logical value model, whole-value input bindings, stable logical task inputs, authoritative output immutability, and atomic visibility of terminal success states with their outputs.

While upstream ADRs define how state machines transition and how data flows, an orchestration engine requires a concrete strategy for persisting this state durably. Without a disciplined persistence architecture, systems suffer from split-brain execution, corrupted state machines caused by partial writes, lost work during restarts, unrecoverable worker sessions, and blurred boundaries between volatile caches and durable truth. ADR-011 formalizes the persistence model that underpins NexusFlow's durability guarantees.

---

## 3. Problem Statement

To provide crash-resilient orchestration, NexusFlow must resolve several core persistence challenges:

1. **Authoritative Orchestration Truth:** What constitutes the durable ground truth of an execution? How does the engine distinguish authoritative data from rebuildable operational caches and transient network states?
2. **Current State vs. Event Replay:** Should the engine rely on event sourcing (replaying event logs from workflow inception to reconstruct state) or a current-state snapshot model? How can recovery startup latency and operational complexity be kept manageable for a V1 architecture?
3. **Execution Entity Modeling:** How should workflow definitions, workflow executions, task executions, and retry attempts be represented logically so that concurrent task updates do not serialize on a single bottleneck?
4. **Retry Durability and Identity Preservation:** How are attempt ordinals, retry limits, and historical attempt records preserved across crashes so that retries are strictly reproducible and attempt numbers are never reused?
5. **Worker Ownership vs. Worker Liveness:** How does the orchestrator persist worker session authority without falsely treating disconnected or dead workers as live after a restart?
6. **Task Input and Output Durability:** How and when are resolved task inputs persisted, and how does persistence guarantee that task and workflow success states are never observable without their authoritative outputs?
7. **Restart-Safe Semantic Deadlines:** How are execution-start deadlines, task timeouts, retry backoffs, and cancellation drains persisted so that process crashes do not blindly reset timer windows?
8. **Crash Consistency and Partial Writes:** If the orchestrator crashes midway through workflow initialization or task completion, what durable states are permitted, and how does the engine prevent impossible lifecycle states?

---

## 4. Requirements Covered

This architecture addresses the following requirements:

- **PERSIST-01:** Establish a durable current-state persistence model as the authoritative source of orchestration truth, surviving orchestrator crashes and restarts.
- **PERSIST-02:** Durably store immutable Validated IWS semantics, ensuring workflow execution recovery never depends on external YAML authoring files.
- **PERSIST-03:** Represent workflow executions, task executions, and execution attempts as logically distinct, independently addressable durable entities.
- **PERSIST-04:** Guarantee that `ExecutionAttempt` records are never overwritten or reused, maintaining monotonic, unique attempt ordinals.
- **PERSIST-05:** Durably persist `ExecutionAttempt -> WorkerSessionId` ownership associations while treating worker liveness observations as strictly ephemeral.
- **PERSIST-06:** Materialize resolved business task inputs upon entering `RUNNABLE`, ensuring identical business inputs across all retry attempts.
- **PERSIST-07:** Enforce atomic visibility between terminal success transitions and authoritative output commitments for both tasks and workflows.
- **PERSIST-08:** Distinguish semantically between an uncommitted/missing output and an authoritative output whose logical value is `null`.
- **PERSIST-09:** Persist semantic timing facts ensuring execution-start deadlines, cancellation drains, timeouts, and retry backoffs survive restarts without being reset to zero.
- **PERSIST-10:** Define logical atomicity groups that prevent partially committed, impossible lifecycle states.
- **PERSIST-11:** Enforce a fail-closed persistence policy, ensuring storage failures are never converted into business execution failures.

---

## 5. Constraints

1. **State Machine Invariants (ADR-006 & ADR-007):** Persistence must strictly mirror the authoritative lifecycle states of `WorkflowExecution` (`INITIALIZING`, `RUNNING`, `CANCELLING`, `FAILING`, `SUCCEEDED`, `FAILED`, `CANCELLED`) and `TaskExecution` (`PENDING`, `RUNNABLE`, `RUNNING`, `RETRY_WAIT`, `SUCCEEDED`, `FAILED`, `CANCELLED`). There is no `DISPATCHED` state.
2. **Deterministic Graph Reconstruction (ADR-003):** The canonical task graph is a pure projection of the Validated IWS; recovery must not introduce external graph-parsing dependencies.
3. **Bounded Inline Payloads (ADR-010):** Payloads conform to the JSON-compatible logical value model and bounded size thresholds; the orchestrator is not a distributed blob engine in V1.
4. **Technology Neutrality:** ADR-011 defines logical entity relationships, consistency groups, and storage capability requirements. It must not select specific database vendors (PostgreSQL, SQLite), ORM frameworks, or table schemas (owned by ADR-020).
5. **Separation of History and Audit (ADR-014):** ADR-011 defines operational persistence required for execution correctness and recovery. Long-term event streams, audit logs, and compliance retention belong to ADR-014.
6. **Separation of Recovery Logic (ADR-012):** ADR-011 defines what state survives and what queries are supported. The recovery reconciliation algorithm belongs to ADR-012.
7. **Separation of Concurrency Control (ADR-013):** ADR-011 defines logical consistency groups. Concurrency primitives (optimistic locking, row locks, CAS) belong to ADR-013.

---

## 6. Goals

- Define a **Durable Current-State Persistence Model** as the authoritative source of orchestration truth.
- Define four primary durable domain entities: `RegisteredDefinition`, `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`.
- Classify all orchestration data into Authoritative Durable State, Derived/Reconstructible State, and Ephemeral Operational State.
- Materialize stable business inputs into `TaskExecution` upon transitioning to `RUNNABLE`.
- Persist `ExecutionAttempt -> WorkerSessionId` ownership associations durably while keeping worker liveness ephemeral.
- Establish restart-safe semantic timing representations that survive process restarts without resetting timer windows.
- Define ten logical atomicity groups governing consistent state transitions and payload visibility.
- Establish that `TaskExecution.state = RUNNABLE` in durable storage is the authoritative work discovery mechanism, eliminating mandatory durable queues or outbox tables for pre-ownership dispatch in V1.
- Establish a strict fail-closed persistence policy.

---

## 7. Non-Goals

- Selecting physical database engines (PostgreSQL, MySQL, SQLite, DynamoDB) or specific ORM libraries in this ADR.
- Freezing physical DDL schemas, table names, SQL constraints, or column types.
- Implementing full event sourcing or requiring event replay to reconstruct current execution state.
- Requiring a dedicated transactional outbox table or persistent message queue for pre-ownership task routing in V1.
- Guaranteeing survival against arbitrary physical hardware destruction or disaster-recovery site replication in V1.
- Defining long-term audit trail schemas, historical event projections, or log purging schedules (owned by ADR-014).
- Specifying the operational crash recovery reconciliation algorithm or worker grace period loop (owned by ADR-012).

---

## 8. Candidate Solutions

### 8.1 State Authority Models

#### Candidate A: In-Memory Authority with Periodic Disk Snapshots
The orchestrator maintains all execution state in process memory. State is asynchronously checkpointed to disk periodically or on major milestones.
- *Pros:* Extremely fast write path; zero database contention during execution.
- *Cons:* Catastrophic data loss window between checkpoints; non-deterministic state post-crash; impossible to coordinate multiple workers safely; violates fundamental enterprise orchestration requirements.

#### Candidate B: Full Event Sourcing as Sole Source of Truth
Every state change is an immutable event appended to an append-only event log. Current state is reconstructed by replaying the event stream from workflow inception.
- *Pros:* Complete audit trail by default; enables historical time-travel debugging.
- *Cons:* High operational complexity; requires snapshot compaction to prevent unbounded replay times; event schema evolution and versioning across software updates is notoriously difficult; unnecessary complexity for a V1 single-developer engine.

#### Candidate C: Selected — Normalized Durable Current-State Model with Separate Audit History
Authoritative state is stored in normalized, independently addressable domain records representing current execution truth. State transitions are committed synchronously to crash-safe storage. Append-only event history is maintained separately (ADR-014) as a non-authoritative audit projection.
- *Pros:* Immediate, low-latency state lookups for schedulers and recovery; standard query capabilities; simple operational model; robust isolation across concurrent tasks.
- *Cons:* Requires disciplined transaction boundaries to avoid partial update anomalies.

---

### 8.2 Execution Entity Representation

#### Candidate A: Monolithic Workflow Document Aggregate (Single Blob)
The entire workflow execution tree—including root state, all tasks, all retry attempts, inputs, and outputs—is serialized and stored as a single document/blob per workflow.
- *Pros:* Simple data model; atomic reads of entire workflow state in a single query.
- *Cons:* Massive write amplification; parallel tasks executing concurrently on different workers serialize on document-level lock contention; updating a single attempt heartbeat or task state rewrites the entire workflow payload; high risk of write collisions.

#### Candidate B: Selected — Normalized Independently Addressable Entities
State is split into four distinct logical entities: `RegisteredDefinition`, `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`.
- *Pros:* Independent updates for concurrent tasks; fine-grained concurrency control; efficient recovery queries for active attempts and runnable tasks; scales cleanly with wide DAG topologies.
- *Cons:* Requires managing relational integrity and multi-entity consistency groups.

---

## 9. Detailed Evaluation

| Evaluation Dimension | Option A: In-Memory + Snapshots | Option B: Monolithic Workflow Blob | Option C: Full Event Sourcing | Option D: Selected (Normalized Current-State) |
| :--- | :--- | :--- | :--- | :--- |
| **Crash Recovery Simplicity** | Poor (data loss window) | Moderate (deserialize root blob) | Low (replay event log stream) | **High (direct indexed state queries)** |
| **Concurrency Isolation** | Poor (in-memory lock) | Terrible (write-lock entire workflow) | Moderate (optimistic stream append) | **High (task-level record isolation)** |
| **Operational Simplicity** | Moderate | High initially, fragile at scale | Very Low (projections, schema drift) | **High (standard queryable records)** |
| **Query Flexibility** | None | Low (requires document parsing) | Low (requires custom projections) | **High (native selective queries)** |
| **State Drift Risk** | High | Low | Very Low | **Very Low (atomic consistency groups)** |

The evaluation demonstrates that the **Normalized Durable Current-State Model** provides the optimal balance of transactional integrity, operational simplicity, concurrent task scalability, and recovery efficiency for NexusFlow.

---

## 10. Decision

NexusFlow establishes the following state persistence architecture:

### 10.1 Central Decision: Durable Current-State Persistence Model
NexusFlow adopts a **Durable Current-State Persistence Model** as the authoritative source of orchestration truth. 
- Authoritative execution state is stored as logically distinct, independently addressable records representing the current lifecycle reality.
- Full event replay is **not** required to determine execution state, schedule tasks, or perform crash recovery.
- All state transitions and payload commitments are synchronously recorded in crash-safe storage before external actions are initiated.

```
+-------------------------------------------------------------------------------+
|                      AUTHORITATIVE DURABLE STATE                              |
|  - RegisteredDefinition (Exact immutable Validated IWS semantics)             |
|  - WorkflowExecution (Lifecycle state, Input, Output, Terminal context)       |
|  - TaskExecution (Lifecycle state, Materialized Input, Authoritative Output)  |
|  - ExecutionAttempt (Attempt identity/ordinal, Lifecycle, Worker ownership,   |
|                      Restart-safe semantic timing state, Terminal context)     |
+-------------------------------------------------------------------------------+
                                      |
                           (Derives & Reconstructs)
                                      v
+-------------------------------------------------------------------------------+
|                      DERIVED / RECONSTRUCTIBLE STATE                          |
|  - Task redispatch eligibility (RUNNABLE task discovery)                      |
|  - Upstream dependency satisfaction conditions                                |
|  - Current active attempt identity per TaskExecution                          |
|  - Progress aggregations / task completion counts                             |
|  - Canonical DAG topology (Reconstructed from Validated IWS)                  |
+-------------------------------------------------------------------------------+
                                      |
                            (Disposable Runtime)
                                      v
+-------------------------------------------------------------------------------+
|                       EPHEMERAL OPERATIONAL STATE                             |
|  - Worker Registry (Heartbeat timestamps, Socket connections, Live endpoints) |
|  - Ephemeral routing candidate matches & transient dispatch offers            |
|  - In-memory scheduler wakeup timers & tick loops                             |
|  - Volatile cache layers & transient delivery broker envelopes                |
+-------------------------------------------------------------------------------+
```

---

### 10.2 Classification of State

NexusFlow strictly categorizes all operational data into three tiers:

1. **Authoritative Durable State:**
   - The absolute source of truth. If data in this category is not durably committed, the transition semantically did not happen.
   - Includes: `RegisteredDefinition` semantics, `WorkflowExecution` state/inputs/outputs, `TaskExecution` state/inputs/outputs, `ExecutionAttempt` state/ordinals/ownership/deadlines, and terminal failure context.
2. **Derived / Reconstructible State:**
   - Operational projections deterministically recomputed from authoritative durable state.
   - Includes: `RUNNABLE` task candidate sets, dependency satisfaction counters, progress summaries, current active attempt pointers, and in-memory canonical DAG representations.
   - Derived state **must never** become an independent source of truth. If cached or persisted for performance, it remains fully rebuildable.
3. **Ephemeral Operational State:**
   - Transient runtime tracking data valid only for the active lifetime of a specific process instance, network socket, or connection.
   - Includes: `WorkerRegistry` membership, heartbeat timestamps, live connection pools, routing candidate queues, and in-memory scheduler tick loops.
   - Ephemeral state has **zero correctness authority**. Loss of ephemeral state never compromises workflow execution integrity.

---

### 10.3 Durable Entity Model

Authoritative state is structured across four primary logical entities:

1. **`RegisteredDefinition`**:
   - Stores the exact, normalized, immutable Validated IWS semantics (ADR-001).
   - Identified by an unambiguous, immutable definition reference.
   - **Recovery Invariant:** Execution recovery must **never** depend on external authoring YAML files or source file accessibility.
2. **`WorkflowExecution`**:
   - Uniquely identified root execution record bound permanently to a specific `RegisteredDefinition`.
   - Persists authoritative lifecycle state strictly adhering to ADR-006:
     $$\text{INITIALIZING}, \text{RUNNING}, \text{CANCELLING}, \text{FAILING}, \text{SUCCEEDED}, \text{FAILED}, \text{CANCELLED}$$
   - Holds immutable logical workflow input, authoritative workflow output upon success, and terminal failure/cancellation context.
3. **`TaskExecution`**:
   - Represents a logical task within a workflow execution, bound to a specific `TaskDefinition`.
   - Persists authoritative lifecycle state strictly adhering to ADR-007:
     $$\text{PENDING}, \text{RUNNABLE}, \text{RUNNING}, \text{RETRY_WAIT}, \text{SUCCEEDED}, \text{FAILED}, \text{CANCELLED}$$
   - **No Dispatched State:** There is no intermediate `DISPATCHED` state.
   - Holds materially persisted resolved business input, authoritative task output upon success, and restart-safe retry timing state.
4. **`ExecutionAttempt`**:
   - Represents a distinct, physical execution try under a `TaskExecution`.
   - Persists authoritative lifecycle state strictly adhering to ADR-007 and ADR-008:
     $$\text{CLAIMED}, \text{RUNNING}, \text{SUCCEEDED}, \text{FAILED}, \text{CANCELLED}$$
   - Holds an immutable, 1-based monotonic attempt ordinal.
   - Holds the durable `WorkerSessionId` ownership association.
   - Holds restart-safe semantic timing facts (execution-start deadlines, cancellation-resolution deadlines, execution timeouts) and terminal failure diagnostic context.

---

### 10.4 Canonical Graph Recovery

The canonical workflow graph is a deterministic projection of the Validated IWS (ADR-003). 
- In V1, the canonical DAG topology is deterministically reconstructed from the durable Validated IWS upon workflow initialization and during crash recovery.
- Implementations may cache the graph representation in memory or persist it as a rebuildable projection, provided recovery never invokes external YAML parsers.

---

### 10.5 Attempt Immutability, Ordinals, and Retention

- **Non-Reuse Rule:** Every retry creates a **new, distinct** `ExecutionAttempt` record. Earlier attempt records are never overwritten, reused, or replaced.
- **Ordinal Monotonicity:** Attempt ordinals are 1-based, strictly monotonic integers unique within the parent `TaskExecution`. Once committed, an attempt ordinal is permanently consumed and never reallocated.
- **Current Authoritative Attempt:** At most **one** authoritative active attempt may exist for a `TaskExecution` at any time. Persistence must allow this active attempt to be determined unambiguously after restart (via explicit reference or lifecycle query). Highest ordinal identifies the latest created attempt, but active authority is strictly governed by lifecycle state and fencing validation (ADR-007 / ADR-008).
- **Retry Budget Durability:** Retry budget consumption is authoritatively governed by the existence of committed `ExecutionAttempt` records. Any persisted retry counter is a rebuildable projection.
- **Retention Scope:** All `ExecutionAttempt` records required for correctness, retry validation, fencing, and crash recovery must remain durably accessible for at least the active lifetime of the workflow execution. Long-term audit retention and purging belong to ADR-014.

---

### 10.6 Worker Ownership vs. Ephemeral Worker Liveness

- **Authoritative Link:** The association `ExecutionAttempt -> WorkerSessionId` is durable truth, committed when an attempt transitions to `CLAIMED` during Ownership Commit (ADR-008). An active attempt must always have a recoverable worker session identity.
- **Ephemeral Liveness:** The `WorkerRegistry`, heartbeat timestamps, active socket connections, and worker polling pools are strictly ephemeral. Persisting a `WorkerSessionId` on an attempt does **not** imply the worker is currently alive post-restart. Liveness is re-evaluated dynamically post-restart by ADR-008 and ADR-012.

---

### 10.7 Task Input Materialization and Consistency

- **V1 Decision:** The resolved logical business input dictionary for a `TaskExecution` is **materially persisted** when the task transitions from `PENDING` to `RUNNABLE`.
- **Rationale:** Materialization guarantees deterministic, identical business inputs across all subsequent retry attempts without re-evaluating upstream dependencies or definition bindings, simplifies worker dispatch queries, and insulates running tasks from schema evolution.
- **Atomicity Invariant:** A `TaskExecution` must **never** become observably `RUNNABLE` while its materialized input is missing or uncommitted.

---

### 10.8 Task and Workflow Output Durability and Atomicity

- **Task Output:** Authoritative `TaskExecution` output is correctness-critical. A task must **never** become observably `SUCCEEDED` while its authoritative output is missing, uncommitted, or unreadable. Failed, cancelled, or fenced attempt results are strictly quarantined and never committed as authoritative task outputs.
- **Workflow Output:** A workflow must **never** become observably `SUCCEEDED` while its authoritative workflow output is missing, unresolved, or uncommitted. Workflows with no declared output bindings commit an authoritative output of `null` (ADR-010).

---

### 10.9 Null vs. Missing Output Semantics

Persistence must unambiguously distinguish between two distinct states:
1. **Uncommitted / Missing Output:** The task or workflow has not completed successfully (e.g., in progress, failed, cancelled).
2. **Authoritative `null` Output:** The task or workflow has successfully completed with an explicit logical value of `null` (void task return or empty workflow output).

Implementations may satisfy this via presence markers, tagged wrappers, or state/payload invariants; the distinction must be semantically unambiguous.

---

### 10.10 Restart-Safe Semantic Timing Model

To ensure process restarts do not compromise time-based scheduling or fencing:
- **Semantic Timing Facts:** Deadlines and backoffs are persisted as absolute control-plane deadline instants OR equivalent durable timing facts (e.g., authoritative start instant + configured duration) sufficient to reconstruct that instant post-crash.
- **Timer Re-evaluation Rule:** Orchestrator restarts must **not** blindly reset timer windows to their full original duration. Elapsed time intent must be preserved across crashes.
- **Applicability:**
  - `RETRY_WAIT`: Persists the next-eligibility timing condition; restarts preserve elapsed backoff.
  - `CLAIMED`: Persists the execution-start deadline; evaluated post-restart against recovery grace rules (ADR-008 / ADR-012).
  - `RUNNING`: Persists execution timeout timing where configured.
  - Cancellation Drain: Persists cancellation-resolution deadlines, ensuring unreachable workers cannot delay workflow cancellation indefinitely.

---

### 10.11 Staged Initialization and Partial Creation

Workflow creation follows a staged durable model:
1. `WorkflowExecution` is durably established in `INITIALIZING`.
2. Exactly one `TaskExecution` per `TaskDefinition` is eagerly established in `PENDING`.
3. `WorkflowExecution` transitions from `INITIALIZING` to `RUNNING` only after the complete expected task set exists durably.

**Partial Initialization Semantics:**
- An `INITIALIZING` workflow coexisting with a partial set of `TaskExecution` records post-crash is a **valid, recoverable intermediate state**, not data corruption.
- Infrastructure failures, storage outages, and crashes during task creation **must never** trigger `INITIALIZING -> FAILED`. The execution remains in `INITIALIZING` to be completed by ADR-012 recovery.
- Only definitive, unrecoverable execution-specific semantic validation failures (as circumscribed by ADR-006) permit a direct transition `INITIALIZING -> FAILED`. There is no `INITIALIZING -> FAILING` transition.

---

### 10.12 Work Rediscovery: RUNNABLE as Durable Work Truth

- In V1, **no separate durable queue entity or transactional outbox is required for correctness.**
- The state `TaskExecution.lifecycle_state = RUNNABLE` in durable storage is the authoritative work truth.
- Following an orchestrator crash or message broker outage, recovery discovers unowned work by querying `RUNNABLE` tasks. External message queues or brokers serve as non-authoritative delivery accelerators.

---

### 10.13 Logical Consistency Groups

ADR-011 defines ten logical consistency groups representing interdependent state facts that must become durably consistent together (physical concurrency and transaction enforcement mechanisms belong to ADR-013):

1. **Task Input Readiness:**
   Materialized `TaskExecution` input + Transition `PENDING -> RUNNABLE`.
2. **Task Ownership Commit:**
   Transition `TaskExecution.RUNNABLE -> RUNNING` + Creation of `ExecutionAttempt` in `CLAIMED` + Durable `WorkerSessionId` assignment + Monotonic attempt ordinal consumption.
3. **Execution Start Observation:**
   Transition `ExecutionAttempt.CLAIMED -> RUNNING` + Cessation of execution-start deadline governance.
4. **Task Success & Output Commit:**
   Authoritative `ExecutionAttempt.SUCCEEDED` + `TaskExecution.SUCCEEDED` + Authoritative task output commitment.
5. **Retry Scheduling:**
   Authoritative `ExecutionAttempt.FAILED` + `TaskExecution.RETRY_WAIT` + Durable next-retry timing state.
6. **Definitive Task Failure:**
   Authoritative `ExecutionAttempt.FAILED` + `TaskExecution.FAILED` (when retry budget is exhausted or failure is non-retryable).
7. **Workflow Failure Direction Commit:**
   Single-winner transition `WorkflowExecution.RUNNING -> FAILING` (locking terminal direction against conflicting cancellation).
8. **Workflow Cancellation Direction Commit:**
   Single-winner transition `WorkflowExecution.RUNNING -> CANCELLING` or `WorkflowExecution.INITIALIZING -> CANCELLING` (locking terminal direction against conflicting failure).
9. **Initialization Terminal Failure Commit:**
   Direct single-winner transition `WorkflowExecution.INITIALIZING -> FAILED` (strictly reserved for definitive semantic initialization failures; never infrastructure failure).
10. **Workflow Success & Output Commit:**
    Authoritative workflow output commitment + Transition `WorkflowExecution.RUNNING -> SUCCEEDED`.

---

### 10.14 Persistence Failure and Crash-Commit Semantics

- **Fail-Closed Principle:** If an authoritative persistence commit fails, the semantic transition **did not occur**. Speculative in-memory state must not be treated as committed truth.
- **No Fabricated Failures:** Storage outages, database connection drops, and commit failures must **never** be converted into business task failures or workflow failures.
- **Crash Commit:** Any state transition durably committed prior to a crash is authoritative upon restart. Any uncommitted in-flight operation is treated as never having occurred.

---

### 10.15 Recovery Query Requirements

The persistence layer must support practical, selective discovery of:
- Nonterminal `WorkflowExecution` records (`INITIALIZING`, `RUNNING`, `CANCELLING`, `FAILING`).
- `TaskExecution` records in `RUNNABLE` state.
- `TaskExecution` records in `RETRY_WAIT` whose next-retry timing condition is due.
- Active `ExecutionAttempt` records (`CLAIMED`, `RUNNING`).
- Attempts associated with a specific `WorkerSessionId`.
- Attempts with pending or elapsed semantic deadlines.

---

### 10.16 Referential and Uniqueness Semantics

- **Referential Integrity:** Logical parent-child relationships must be preserved (`WorkflowExecution -> RegisteredDefinition`, `TaskExecution -> WorkflowExecution`, `ExecutionAttempt -> TaskExecution`). Orphaned entities are prohibited.
- **Uniqueness Invariants:**
  - Exactly one `TaskExecution` exists per `(WorkflowExecution, TaskDefinition)`.
  - `ExecutionAttempt` ordinals are strictly unique per `TaskExecution`.
  - Execution identifiers are globally unambiguous.

---

### 10.17 Required Storage Capabilities

ADR-011 constrains downstream technology selection (ADR-020) to storage systems capable of:
- Crash-safe durability for committed writes.
- Multi-record atomic consistency for the defined logical atomicity groups.
- Conditional updates or uniqueness enforcement.
- Selective query indexing over lifecycle states and foreign associations.
- Storage of bounded logical JSON values.
- Schema evolution support without silent reinterpretation of active states.

---

## 11. Decision Rationale

1. **Rejection of Event Sourcing in Favor of Current State:** Event sourcing requires complex snapshotting, stream compaction, event schema evolution frameworks, and replay loops. For a single-developer V1 architecture, a normalized current-state model provides immediate, indexed queryability, standard transaction semantics, transparent debugging, and rapid crash recovery without event replay overhead.
2. **Normalized Entities vs. Monolithic Blob:** In distributed workflows with wide parallel branches, multiple workers execute independent tasks concurrently. A monolithic workflow document forces continuous write-lock contention across concurrent tasks and causes massive write amplification. Normalized entities isolate task writes, enabling concurrent progress and fine-grained recovery queries.
3. **Materialized Task Inputs:** Materializing the resolved business input dictionary when entering `RUNNABLE` decouples downstream attempts from upstream task records. Worker dispatch queries read a single local record rather than performing recursive upstream graph joins. Retries (Attempts $1..N$) consume the exact same pre-materialized input, guaranteeing retry reproducibility even if workflow definitions evolve.
4. **Authoritative Work in RUNNABLE State:** Equating the durable `RUNNABLE` database state with work truth eliminates the need to build and synchronize an independent persistent queue or transactional outbox in V1. If the message broker or orchestrator crashes, recovery queries discover `RUNNABLE` tasks and re-dispatch them, ensuring zero work loss with minimal architectural complexity.
5. **Separation of Worker Ownership from Liveness:** Treating worker session ownership as durable truth while keeping worker liveness ephemeral allows the orchestrator to fence stale worker results post-crash without falsely assuming that dead workers are still connected.

---

## 12. Tradeoffs

| Architectural Advantage | Tradeoff Incurred | Mitigation Strategy |
| :--- | :--- | :--- |
| **Instant Recovery Lookups:** Current-state records allow immediate discovery of runnable work and active attempts. | **Separated Audit Stream:** Historical timelines and audit trails are not automatically captured in the current-state tables. | ADR-014 introduces a dedicated, append-only history projection decoupled from the operational state path. |
| **Task-Level Concurrency:** Normalized entities allow concurrent tasks to commit independently without root-document contention. | **Multi-Entity Transactions:** Operations spanning tasks and attempts require atomic consistency groups. | ADR-013 establishes physical concurrency and transaction primitives satisfying the ten atomicity groups. |
| **Retry Parameter Stability:** Materializing task input provides guaranteed reproducible retries. | **Storage Duplication:** Inputs derived from workflow input or upstream outputs are duplicated into the task record. | Payloads are strictly bounded (ADR-010); storage overhead is predictable and manageable. |
| **Queue-Less Scheduling:** Treating `RUNNABLE` as work truth eliminates persistent broker dependencies. | **Recovery Polling:** Schedulers and recovery engines must query the database for runnable work post-crash. | Selective indexing on lifecycle states ensures recovery queries are highly efficient. |

---

## 13. Consequences

### 13.1 Positive Consequences
- **Instant Crash Recovery:** A restarted orchestrator immediately queries active executions and runnable tasks without replaying historical event logs.
- **Robust Multi-Task Concurrency:** Concurrent tasks in parallel DAG branches update their own attempt and task records independently without lock collisions.
- **Zero Lost Work on Broker Failure:** Wiping out message brokers, Redis caches, or in-memory queues loses zero workflow progress; all unowned work is discovered from `RUNNABLE` state.
- **Deterministic Retries:** Worker retries execute against identical, pre-materialized business inputs, eliminating parameter drift.
- **Safe Single-Winner Settling:** Persisted worker session IDs and attempt ordinals allow stale or duplicate results to be fenced cleanly post-crash.

### 13.2 Negative Consequences
- **Storage Amplification:** Materializing task inputs creates redundant copies of data when multiple downstream tasks consume the same workflow input or upstream output.
- **Strict Transaction Requirements:** Storage engines selected under ADR-020 must support multi-record atomic consistency to satisfy the ten atomicity groups.

---

## 14. Failure Modes

| # | Failure Scenario | Durable Authoritative Truth | Allowed Persisted State | Forbidden Invariant Violation | ADR-012 Recovery Determination | Owning ADR |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **1** | Crash before `WorkflowExecution` committed | No execution record exists. | Execution record absent. | Orphaned or partial workflow record. | Recognizes execution does not exist; client receives error or resubmits. | ADR-011 / ADR-015 |
| **2** | Crash during eager task instantiation | Workflow is `INITIALIZING`; subset of tasks exist. | `INITIALIZING` with partial task records. | Workflow marked `RUNNING` with missing tasks; or marked `FAILED`. | Detects incomplete task set against Validated IWS; completes instantiation and promotes. | ADR-011 / ADR-012 |
| **3** | Duplicate workflow initialization attempt | First committed `INITIALIZING` record is authority. | Single unique execution record. | Duplicate workflow execution records. | Ignores duplicate initialization request. | ADR-011 / ADR-013 |
| **4** | Duplicate `TaskExecution` creation race | Exactly one task per definition invariant. | Exactly one task record. | Multiple task records for same definition in one workflow. | Reject duplicate insertion via uniqueness invariant. | ADR-011 / ADR-013 |
| **5** | Crash after `RUNNABLE` commit before dispatch | Task is `RUNNABLE`; input materialized. | Task in `RUNNABLE`. | Task marked `RUNNING` without attempt; or input uncommitted. | Discovers `RUNNABLE` task and initiates dispatch. | ADR-005 / ADR-011 / ADR-012 |
| **6** | Concurrent Ownership Commit attempts | Single-winner commit wins (ADR-008). | Exactly one attempt in `CLAIMED`. | Multiple active attempts for same task. | Winning worker receives claim; loser rejected. | ADR-008 / ADR-013 |
| **7** | Crash during Ownership Commit | Incomplete commit fails closed. | Task remains `RUNNABLE`; no attempt created. | Attempt exists without worker session; or task marked `RUNNING` with no attempt. | Task redispatched to eligible worker. | ADR-008 / ADR-011 / ADR-013 |
| **8** | Attempt `CLAIMED` but worker never starts | Attempt in `CLAIMED` with valid start deadline. | Attempt in `CLAIMED`. | Indefinite stall in `CLAIMED`. | Evaluates attempt against start deadline and reconciliation grace. | ADR-008 / ADR-012 |
| **9** | Crash after `CLAIMED` committed | Attempt in `CLAIMED` with session ownership. | Attempt in `CLAIMED`. | Lost ownership association; attempt ordinal reused. | Evaluates attempt under recovery grace period. | ADR-008 / ADR-012 |
| **10** | Crash after Execution Start observed | Attempt in `RUNNING`. | Attempt in `RUNNING`. | Attempt reverts to `CLAIMED`; or start deadline remains active. | Evaluates task execution timeout (if configured). | ADR-008 / ADR-012 |
| **11** | Retry ordinal allocation race | Ordinal is strictly monotonic and unique. | Sequential ordinals ($1, 2, \dots$). | Duplicate ordinal allocated to two attempts. | Atomic allocation ensures distinct monotonic ordinals. | ADR-007 / ADR-013 |
| **12** | Duplicate Attempt establishment | Only one active attempt permitted. | Single active attempt record. | Multiple attempts in `CLAIMED` or `RUNNING`. | Enforces single-winner active attempt constraint. | ADR-007 / ADR-013 |
| **13** | Worker success reported but output commit fails | Output uncommitted; transition fails closed. | Attempt and task remain `RUNNING`. | Task marked `SUCCEEDED` with uncommitted output. | Re-evaluates attempt; worker may retransmit or time out. | ADR-010 / ADR-011 |
| **14** | Crash after Task output committed before scheduler wakeup | Task is `SUCCEEDED` with authoritative output. | Task `SUCCEEDED` with output. | Output committed but task `RUNNING`; or output missing. | Scheduler discovers completed task and advances downstream dependencies. | ADR-010 / ADR-011 / ADR-012 |
| **15** | Crash after Task success before downstream progression | Task is `SUCCEEDED`; downstream `PENDING`. | Consistent state in DB. | Downstream tasks marked `RUNNABLE` with uncommitted inputs. | Evaluates downstream dependency readiness. | ADR-005 / ADR-012 |
| **16** | Task success without output attempted | Forbidden invariant violation. | Blocked by atomicity group 4. | `SUCCEEDED` task with missing output. | Commit fails closed; task remains `RUNNING`. | ADR-010 / ADR-011 |
| **17** | Stale worker reports success post-crash | Attempt in DB is already superseded or closed. | Result rejected; output quarantined. | Stale result overwrites authoritative output. | Maintains single-winner fencing; idempotent rejection. | ADR-008 / ADR-013 |
| **18** | Workflow enters `FAILING` and crashes | Workflow is `FAILING` in DB. | Workflow in `FAILING`. | Workflow reverts to `RUNNING` or transitions to `CANCELLING`. | Resumes failure drain of active tasks. | ADR-006 / ADR-012 |
| **19** | Workflow enters `CANCELLING` and crashes | Workflow is `CANCELLING` in DB. | Workflow in `CANCELLING`. | Workflow reverts to `RUNNING` or transitions to `FAILING`. | Resumes cancellation drain of active tasks. | ADR-006 / ADR-012 |
| **20** | Cancellation timing pending during restart | Cancellation deadline persisted in DB. | Attempt retains deadline. | Cancellation timer reset back to full duration. | Re-evaluates cancellation deadline against current control-plane time. | ADR-008 / ADR-011 / ADR-012 |
| **21** | Retry wait active during restart | Task in `RETRY_WAIT` with next-eligibility time. | Task in `RETRY_WAIT`. | Retry backoff timer reset back to full duration. | Evaluates eligibility time; promotes to `RUNNABLE` when due. | ADR-007 / ADR-011 / ADR-012 |
| **22** | Workflow output commit fails | Output uncommitted; transition fails closed. | Workflow remains in `RUNNING`. | Workflow marked `SUCCEEDED` without output. | Re-attempts workflow terminal transaction. | ADR-010 / ADR-011 |
| **23** | Workflow success without output attempted | Forbidden invariant violation. | Blocked by atomicity group 10. | `SUCCEEDED` workflow with missing output. | Commit fails closed; workflow remains in `RUNNING`. | ADR-010 / ADR-011 |
| **24** | Durable store temporarily unavailable | Fail closed; in-memory advancement blocked. | DB state unchanged. | Speculative state treated as truth; tasks marked failed. | Operations retry once storage connectivity returns. | ADR-011 / ADR-018 |
| **25** | Delivery queue / broker completely lost | Database contains all `RUNNABLE` tasks. | Authoritative state intact. | Lost workflow progress or dropped tasks. | Re-populates dispatch notifications from `RUNNABLE` records. | ADR-011 / ADR-012 |
| **26** | Worker Registry lost on crash | Registry empty; durable attempts retain session ID. | Empty registry; active attempts in DB. | Dead workers assumed active. | Enters bounded recovery grace to allow live workers to re-register. | ADR-008 / ADR-012 |
| **27** | External authoring YAML unavailable | `RegisteredDefinition` contains Validated IWS. | Normalized IWS intact. | Workflow unable to execute due to missing YAML. | Execution and recovery proceed without external files. | ADR-001 / ADR-002 / ADR-011 |
| **28** | Referenced definition unavailable in storage | Corrupt persistence state. | Invariant violation. | Workflow executing with unknown definition. | Execution cannot proceed; raises critical infrastructure error. | ADR-011 / ADR-018 |
| **29** | Materialized task input missing while `RUNNABLE` | Forbidden invariant violation. | Blocked by atomicity group 1. | `RUNNABLE` task with missing input. | Commit fails closed; task remains `PENDING`. | ADR-010 / ADR-011 |
| **30** | Explicit `null` task output | Output committed as `null` with presence marker. | Task `SUCCEEDED` with explicit `null`. | Task treated as incomplete or output missing. | Downstream tasks bind `null` whole-value. | ADR-010 / ADR-011 |
| **31** | Explicit `null` workflow output | Output committed as `null` with presence marker. | Workflow `SUCCEEDED` with explicit `null`. | Workflow output marked missing. | API returns `output: null`. | ADR-010 / ADR-011 / ADR-015 |
| **32** | Conflicting duplicate result reported | First committed result is authoritative. | Original output intact. | Conflicting output overwrites committed output. | Rejects duplicate result; fences reporting attempt. | ADR-008 / ADR-010 / ADR-013 |
| **33** | Repeated terminal transition request | Terminal states are immutable. | State unchanged. | Terminal state re-evaluated or altered. | Idempotent acknowledgment; no state change. | ADR-006 / ADR-007 / ADR-013 |
| **34** | Initialization infrastructure failure | Storage error during eager task instantiation. | Workflow remains `INITIALIZING`. | Workflow marked `FAILED` or `FAILING`. | Recovery completes task establishment; no business failure. | ADR-006 / ADR-011 / ADR-012 |
| **35** | Definitive initialization semantic failure | Unrecoverable execution-specific semantic error. | Direct transition to `FAILED`. | Workflow transitions to `FAILING` or `RUNNING`. | Execution terminated as `FAILED` (ADR-006). | ADR-006 / ADR-011 |

---

## 15. Debugging

NexusFlow provides diagnostic visibility into persisted execution state without requiring heavy runtime payload dumping:

- **Entity Key Correlations:** All database records must correlate via unambiguous foreign associations:
  - `workflow_execution_id`
  - `task_execution_id`
  - `attempt_id`
  - `attempt_number`
  - `worker_session_id`
  - `definition_id`
- **Inspection Capabilities:** Operators can inspect:
  1. Authoritative workflow and task lifecycle states.
  2. The complete history of all `ExecutionAttempt` records for a task, including attempt ordinals and failure context.
  3. The active `WorkerSessionId` assigned to any in-flight attempt.
  4. The materialized business input dictionary for any `RUNNABLE` or executing task.
  5. The authoritative output payload and commitment status for completed tasks and workflows.
  6. Elapsed vs. remaining semantic deadlines (`deadline_at` vs. control-plane UTC time).
  7. Initialization completeness by comparing persisted `TaskExecution` counts against the `RegisteredDefinition` task count.

---

## 16. Testing

The persistence strategy requires comprehensive verification across integration and crash-injection test suites:

### 16.1 Crash and Restart Testing
- **Lifecycle Transition Restart:** Inject process kills immediately before and after every state transition across `WorkflowExecution`, `TaskExecution`, and `ExecutionAttempt`; verify authoritative state is strictly preserved post-restart.
- **Partial Initialization Recovery:** Terminate orchestrator midway through eager task creation; verify workflow remains in `INITIALIZING` and ADR-012 completes task creation without error.
- **Timing Preservation:** Verify that restart while in `RETRY_WAIT`, `CLAIMED`, or cancellation drain does not reset timer windows to zero.
- **Worker Registry Loss:** Wipe in-memory worker registry while active attempts exist; verify durable attempt ownership persists and enables graceful worker re-registration.
- **Broker / Delivery Queue Wipe:** Flush message delivery queues while tasks are `RUNNABLE`; verify recovery discovers all runnable work and re-initiates dispatch.
- **External File Absence:** Delete source YAML files after workflow registration; verify execution and recovery execute cleanly using the durable Validated IWS.

### 16.2 Invariant and Atomicity Testing
- **Input / RUNNABLE Consistency:** Verify that a task cannot be observed in `RUNNABLE` without its materialized input dictionary present.
- **Output / Success Consistency:** Verify that queries never observe a `SUCCEEDED` task or workflow whose output is uncommitted or unreadable.
- **Null vs. Missing Output:** Verify that explicit `null` outputs are cleanly distinguishable from uncommitted outputs in test queries.
- **Monotonic Attempt Ordinals:** Concurrently trigger retries; verify that attempt ordinals are strictly sequential, unique, and never reused.
- **Worker Ownership Integrity:** Verify that an attempt cannot transition to `CLAIMED` without a valid `worker_session_id`.
- **Direction Lock:** Verify that once `RUNNING -> FAILING` or `RUNNING -> CANCELLING` is committed, subsequent conflicting direction requests are rejected.
- **Initialization Failure Bounds:** Inject database outages during initialization; verify workflow **never** transitions to `FAILED` or `FAILING`. Verify that only definitive semantic initialization failures trigger `INITIALIZING -> FAILED`.

---

## 17. Operational Considerations

1. **Persistence on Critical Path:** Authoritative state advancement depends synchronously on database write performance. Storage latency directly impacts task dispatch and result settling throughput.
2. **Fail-Closed Operational Stance:** If durable storage becomes unavailable, the control plane stops advancing execution state. Schedulers must not speculate or advance state in memory during a storage outage.
3. **Database Indexing for Recovery:** Fast recovery depends on indexed queries across lifecycle states (`WHERE lifecycle_state = 'RUNNABLE'`). Database configurations must ensure these queries execute efficiently.
4. **Historical Payload Management:** While inline payloads are bounded (ADR-010), high-throughput workflow engines accumulate substantial storage volume over time. Historical archiving and data pruning policies must be established under ADR-014.
5. **Schema Migrations:** Changes to entity representations must support forward and backward compatibility, ensuring in-flight workflows are not corrupted during rolling orchestrator upgrades.

---

## 18. Maintenance

- **Adding New Entity Attributes:** New operational metadata fields must be nullable or provide backward-compatible defaults.
- **Lifecycle State Stability:** Lifecycle enums must be stored using stable representations immune to re-ordering.
- **Decoupled Audit Maintenance:** Maintainers can optimize or prune historical audit logs (ADR-014) without risking the integrity of active execution state.

---

## 19. Future Evolution

- **High Availability (ADR-025):** The normalized durable current-state model naturally supports active-passive or multi-orchestrator topologies, as authority resides in the shared durable database rather than process memory.
- **Workflow Versioning (ADR-024):** Immutable `RegisteredDefinition` records provide the foundation for workflow definition versioning and in-flight migration strategies.
- **Payload Offloading:** If future requirements permit larger payloads, the logical entity model can incorporate payload reference pointers without modifying the core state machine.

---

## 20. Rejected Alternatives

### 20.1 In-Memory State Authority with Periodic Snapshots
- **Rejected:** Unacceptable data loss window upon unexpected crash; violates enterprise durability requirements; cannot coordinate distributed workers safely.

### 20.2 Monolithic Workflow Document Aggregate (Single Blob)
- **Rejected:** Severe write-lock contention across concurrent tasks in parallel DAG branches; massive write amplification on minor attempt updates; poor queryability for active attempts and runnable tasks.

### 20.3 Full Event Sourcing as Sole Source of Truth
- **Rejected:** Excessive implementation and operational complexity for V1; requires snapshotting and event replay to reconstruct state; complicates schema evolution.

### 20.4 Persistent Message Broker as State Authority
- **Rejected:** Message brokers are optimized for transient message transit, not complex multi-record state queries, referential integrity, or historical consistency. Losing broker state must not corrupt orchestration truth.

### 20.5 Mandatory Transactional Outbox for Pre-Ownership Dispatch
- **Rejected:** Redundant for pre-ownership dispatch in V1 because `TaskExecution.lifecycle_state = RUNNABLE` in durable storage is already sufficient to rediscover work post-crash.

### 20.6 Resetting Timers to Full Duration on Restart
- **Rejected:** Allows repeated crashes to extend retry backoffs, task timeouts, and cancellation drains indefinitely.

---

## 21. Decision Evolution

- **Initial Concept:** Evaluated an event-sourced architecture with an append-only event store and in-memory workflow projections.
- **Refinement 1 (Single-Developer Feasibility):** Rejected full event sourcing due to snapshot compaction and replay complexity; selected a normalized current-state model.
- **Refinement 2 (Retry Parameter Stability):** Decided to materially persist resolved task inputs upon entering `RUNNABLE` to guarantee identical business inputs across all retry attempts.
- **Refinement 3 (Worker Coordination Alignment):** Decoupled durable worker session ownership from ephemeral worker liveness in accordance with ADR-008.
- **Refinement 4 (State Machine Invariant Alignment):** Removed the non-existent `DISPATCHED` task state; corrected workflow direction atomicity to prohibit `INITIALIZING -> FAILING` and restrict `INITIALIZING -> FAILED` strictly to definitive semantic initialization failures (ADR-006).
- **Final Accepted State (ADR-011):** Formulated ten logical consistency groups, strict null vs. missing output semantics, restart-safe deadline representations, and technology-neutral storage capability requirements.

---

## 22. Common Misconceptions

- **Misconception 1: "Message brokers or Redis queues are the source of truth for runnable tasks."**
  *Correction:* Message brokers and queues are non-authoritative delivery accelerators. The database state `TaskExecution.lifecycle_state = RUNNABLE` is the sole authoritative work truth.
- **Misconception 2: "A runnable task requires a separate durable queue record."**
  *Correction:* Marking the task `RUNNABLE` in durable storage is completely sufficient to rediscover the work during recovery.
- **Misconception 3: "Storing a WorkerSessionId implies the worker is currently alive."**
  *Correction:* Persisted worker ownership merely records who was granted execution authority. Worker liveness is strictly ephemeral and re-evaluated dynamically post-crash.
- **Misconception 4: "The highest attempt number is automatically the authoritative active attempt."**
  *Correction:* Highest ordinal indicates the latest attempt created. Active authority is strictly governed by lifecycle state and fencing validation.
- **Misconception 5: "A database outage during execution means the task or workflow has failed."**
  *Correction:* Infrastructure outages cause operations to fail closed. They never trigger business failure transitions.
- **Misconception 6: "Workflow authoring YAML is required to recover a workflow after a crash."**
  *Correction:* Recovery operates exclusively against the durable, immutable Validated IWS stored in `RegisteredDefinition`.
- **Misconception 7: "Event sourcing is mandatory for building a reliable workflow engine."**
  *Correction:* A normalized current-state model backed by ACID transactions provides robust durability, deterministic crash recovery, and instant state queryability without event replay overhead.
- **Misconception 8: "A null output means the task output is missing."**
  *Correction:* Successful void tasks commit an authoritative output of `null`, which is semantically distinct from an uncommitted output.
- **Misconception 9: "Orchestrator restarts can simply reset all timers back to zero."**
  *Correction:* Restarts must preserve elapsed time intent using restart-safe control-plane timing facts.

---

## 23. Open Questions

- *Non-Blocking:* Final physical database vendor and driver selection (PostgreSQL, SQLite; owned by ADR-020).
- *Non-Blocking:* Physical concurrency control mechanism (optimistic locking vs. row-level locking; owned by ADR-013).
- *Status:* **Zero architectural blockers remain for ADR-011.**

---

## 24. Interview Discussion

### Question 1: Why did NexusFlow choose a normalized current-state model over full event sourcing?
**Answer:** While event sourcing provides an audit trail by default, it introduces significant operational complexity: replaying long event streams to reconstruct state, managing snapshotting and compaction, and navigating event schema versioning across application upgrades. For a high-performance, single-developer V1 architecture, a normalized current-state model backed by crash-safe storage provides immediate, indexed state queries for schedulers and recovery loops, transparent relational debugging, and clean task-level isolation without event replay latency. Audit and history requirements are cleanly separated into ADR-014 as an append-only projection.

### Question 2: Why are resolved task inputs materialized upon entering RUNNABLE rather than reconstructed on demand from upstream task outputs?
**Answer:** Materializing resolved task inputs when a task becomes `RUNNABLE` provides three major reliability benefits: First, it guarantees absolute retry parameter stability—every retry attempt reads the exact same pre-materialized payload directly, preventing parameter drift. Second, it simplifies worker dispatch, allowing the scheduler to read a single local record rather than executing recursive joins across upstream task outputs. Third, it insulates active executions from definition changes or upstream output compaction. In a system with bounded payloads, the minimal storage overhead is heavily outweighed by these reliability gains.

### Question 3: How does NexusFlow survive message broker outages without losing scheduled work?
**Answer:** NexusFlow does not treat message brokers or delivery queues as sources of truth. The authoritative indicator of executable work is `TaskExecution.lifecycle_state = RUNNABLE` in the durable database. If a message broker crashes, loses messages, or desynchronizes, zero workflow progress is lost. The recovery engine queries the database for `RUNNABLE` tasks and re-initiates dispatch notifications, treating the broker strictly as a non-authoritative transport accelerator.

### Question 4: How are timeouts and deadlines handled across orchestrator restarts without resetting timers?
**Answer:** NexusFlow persists deadlines as absolute control-plane timing facts (such as absolute deadline instants or start timestamps plus configured durations) rather than relying on volatile, process-local monotonic timers. When the orchestrator restarts, recovery evaluates pending deadlines against current control-plane time, ensuring that elapsed wait time is preserved and preventing repeated crashes from indefinitely delaying timeouts or cancellation drains.

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
- [Architecture Decision Register](docs/architecture/00-architecture-decision-register.md)

---

## 26. Traceability

### 26.1 Backward Traceability
- **ADR-001 & ADR-002:** `RegisteredDefinition` durably stores the exact Validated IWS semantics; recovery never reparses external YAML.
- **ADR-003:** Canonical graph topology is deterministically reconstructible from durable Validated IWS semantics.
- **ADR-005:** `TaskExecution.lifecycle_state = RUNNABLE` serves as the authoritative work discovery mechanism.
- **ADR-006:** `WorkflowExecution` persists authoritative root states (`INITIALIZING`, `RUNNING`, `FAILING`, `CANCELLING`, `SUCCEEDED`, `FAILED`, `CANCELLED`); atomicity groups enforce legal transitions.
- **ADR-007:** Decoupled `TaskExecution` and `ExecutionAttempt` lifecycles; monotonic attempt ordinals; non-reusable attempt records; no `DISPATCHED` state.
- **ADR-008:** Durable `ExecutionAttempt -> WorkerSessionId` ownership; restart-safe execution-start and cancellation deadlines; ephemeral worker liveness.
- **ADR-009:** Task routing remains non-authoritative; no attempt created during routing.
- **ADR-010:** Materialized task inputs; authoritative task/workflow output durability; atomic visibility of success states; unambiguous null vs. missing output semantics.

### 26.2 Forward Traceability
- **ADR-012 (Recovery Strategy):** Owns startup reconciliation loops, worker grace periods, and timeout evaluation using persisted current-state records and timing facts.
- **ADR-013 (Consistency & Concurrency):** Owns physical locking, isolation levels, optimistic concurrency control, and atomic transaction mechanisms enforcing the ten logical consistency groups.
- **ADR-014 (Execution History & Audit):** Owns historical event stream projections, timeline schemas, and compliance archiving.
- **ADR-015 (API):** Defines external API query envelopes for workflow and task execution state.
- **ADR-016 (Observability):** Defines metrics, tracing, and structured logging of database transaction latencies and recovery scan operations.
- **ADR-018 (Error Handling):** Formally categorizes error taxonomies stored in terminal diagnostic context fields.
- **ADR-020 (Technology Selection):** Selects physical database engine, ORM libraries, and connection pooling satisfying the required storage capabilities.
- **ADR-023 (Configuration):** Governs numerical thresholds for bounded payload sizes and default timeout durations.
- **ADR-024 (Workflow Versioning):** Governs versioning schemas built on immutable `RegisteredDefinition` records.
- **ADR-025 (High Availability):** Governs multi-orchestrator coordination sharing durable database authority.

---

## 27. Decision Validation Checklist

- [x] **Durable Current-State model selected** (Section 10.1)
- [x] **RegisteredDefinition durable** (Section 10.3)
- [x] **Exact Validated IWS recoverable** (Section 10.3)
- [x] **No recovery dependency on YAML** (Section 10.3)
- [x] **WorkflowExecution durable** (Section 10.3)
- [x] **TaskExecution durable** (Section 10.3)
- [x] **ExecutionAttempt durable** (Section 10.3)
- [x] **No DISPATCHED TaskExecution state** (Section 10.3)
- [x] **Task lifecycle remains ADR-007-owned** (Section 10.3)
- [x] **Attempt lifecycle remains ADR-007/008-owned** (Section 10.3)
- [x] **One TaskExecution per TaskDefinition per WorkflowExecution** (Section 10.16)
- [x] **Attempt identities never reused** (Section 10.5)
- [x] **Attempt ordinals monotonic and never reused once committed** (Section 10.5)
- [x] **Active Attempt authority recoverable unambiguously** (Section 10.5)
- [x] **Highest ordinal alone does not define authority** (Section 10.5)
- [x] **WorkerSession ownership durable** (Section 10.6)
- [x] **WorkerRegistry liveness non-authoritative** (Section 10.6)
- [x] **Task input materially persisted** (Section 10.7)
- [x] **RUNNABLE cannot be visible without input** (Section 10.7)
- [x] **Retries receive same business input** (Section 10.7)
- [x] **Task output durable** (Section 10.8)
- [x] **Task success cannot appear without output** (Section 10.8)
- [x] **Workflow output durable** (Section 10.8)
- [x] **Workflow success cannot appear without output** (Section 10.8)
- [x] **Committed null distinguished from missing** (Section 10.9)
- [x] **Restart-safe retry timing** (Section 10.10)
- [x] **Restart-safe CLAIMED timing** (Section 10.10)
- [x] **Restart-safe execution timeout** (Section 10.10)
- [x] **Restart-safe cancellation timing** (Section 10.10)
- [x] **Restart does not blindly reset timers** (Section 10.10)
- [x] **Partial INITIALIZING state recoverable** (Section 10.11)
- [x] **Infrastructure failures do not cause INITIALIZING -> FAILED** (Section 10.11)
- [x] **INITIALIZING -> FAILING prohibited** (Section 10.11, 10.13)
- [x] **INITIALIZING -> FAILED only semantic unrecoverable initialization failure** (Section 10.11, 10.13)
- [x] **RUNNING -> FAILING allowed** (Section 10.13)
- [x] **INITIALIZING/RUNNING -> CANCELLING allowed** (Section 10.13)
- [x] **RUNNING -> SUCCEEDED + workflow output** (Section 10.13)
- [x] **No mandatory durable queue** (Section 10.12)
- [x] **No mandatory pre-ownership outbox** (Section 10.12)
- [x] **Broker/cache non-authoritative** (Section 10.2, 10.12)
- [x] **Derived counters non-authoritative** (Section 10.2)
- [x] **Ten logical consistency groups documented** (Section 10.13)
- [x] **ADR-013 owns enforcement** (Section 10.13, 26.2)
- [x] **ADR-012 owns recovery mechanics** (Section 26.2)
- [x] **ADR-014 owns long-term history** (Section 26.2)
- [x] **Technology remains unselected** (Section 10.17)
- [x] **No fake benchmarks or performance claims** (Compliant throughout)
- [x] **No fake crash-test results** (Section 16)
